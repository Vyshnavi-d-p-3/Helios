# Revised Cursor Prompt: Raft Bootstrap (replaces v1 Step 3.1's bootstrap section)

## Why the v1 prompt produced broken code

v1 said "set up raft.Config with sane defaults" and "Bootstrap(peers []string) error:
bootstrap cluster with initial peers." Cursor, given that prompt, generates code
that calls `raft.BootstrapCluster()` with a single-voter configuration (just self).
The other nodes have no way to join — they sit forever in candidate state with no
visible voters.

The right pattern depends on whether you're running a fresh cluster or restarting
existing nodes. Real systems handle this with a small bootstrap protocol.

## The corrected prompt

Paste this into Cursor as a replacement for v1 Step 3.1's RaftNode prompt.

```
Implement internal/cluster/raft_node.go — a hashicorp/raft wrapper with correct
multi-node bootstrap.

Peer addresses are passed as "name=host:port" pairs (e.g.,
"helios-1=helios-1:7000,helios-2=helios-2:7000,helios-3=helios-3:7000").
This format gives each node a stable Raft ID independent of its address.

RaftNode struct fields:
- raft     *raft.Raft
- fsm      raft.FSM
- transport raft.Transport
- store    BoltStore (log + stable, single bbolt file)
- snaps    *raft.FileSnapshotStore
- nodeID   string

NewRaftNode(cfg RaftConfig, fsm raft.FSM) (*RaftNode, error):
1. Open BoltStore at cfg.DataDir/raft.db. This is BOTH log store and stable store —
   raft-boltdb supports both interfaces from a single bbolt instance.
2. Create FileSnapshotStore at cfg.DataDir/snapshots, retain 3 snapshots.
3. Create TCPTransport bound to cfg.RaftBindAddr. Advertise address must be
   reachable by other nodes (use cfg.RaftAdvertiseAddr if set, else BindAddr).
4. Build raft.Config:
     LocalID:           raft.ServerID(cfg.NodeID)
     HeartbeatTimeout:  1 * time.Second
     ElectionTimeout:   2 * time.Second  // must be > 2x HeartbeatTimeout
     CommitTimeout:     500 * time.Millisecond
     MaxAppendEntries:  64
     BatchApplyCh:      true   // CRITICAL for throughput — see comment below
     SnapshotInterval:  120 * time.Second
     SnapshotThreshold: 8192   // log entries before snapshot
5. raft.NewRaft(cfg, fsm, store, store, snaps, transport)

Bootstrap behaviour (the part v1 got wrong):
   if cfg.Bootstrap is true:
     Build raft.Configuration with ALL peers as voters from the start:
       servers := []raft.Server{
         {ID: "helios-1", Address: "helios-1:7000", Suffrage: raft.Voter},
         {ID: "helios-2", Address: "helios-2:7000", Suffrage: raft.Voter},
         {ID: "helios-3", Address: "helios-3:7000", Suffrage: raft.Voter},
       }
     Call raft.HasExistingState(store, store, snaps).
     If NO existing state, call raft.BootstrapCluster(servers).
     If existing state, log "skipping bootstrap, existing state found" and continue.

Why bootstrap-with-full-config is the right pattern:
- The single-voter alternative requires a separate AddVoter step, which fails when
  the cluster can't reach a quorum yet, and which v1's docker-compose has no way
  to invoke.
- With full-config bootstrap, when helios-1 starts it knows about helios-2 and 3
  even before they're up. It will fail elections (no quorum) until they join, then
  succeed. helios-2 and 3 don't bootstrap (their Bootstrap=false); they connect
  to the Raft addresses they know and discover the leader via election.

IsLeader() bool: r.raft.State() == raft.Leader
LeaderAddr() string: addr, _ := r.raft.LeaderWithID(); return string(addr)

Apply(payload []byte, timeout time.Duration) error:
  if !IsLeader() { return ErrNotLeader }
  f := r.raft.Apply(payload, timeout)
  if err := f.Error(); err != nil { return err }
  resp := f.Response()
  if e, ok := resp.(error); ok && e != nil { return e }
  return nil

Shutdown(): r.raft.Shutdown().Error(), close store and transport.

Tests in cluster_test.go:
- TestRaftNode_SingleNode: bootstrap=true with 1 voter, verify it becomes leader
  within 5 seconds.
- TestRaftNode_ThreeNode_InProcess: spin up 3 RaftNodes in-process with different
  ports, all configured with the same peer set, helios-1 bootstrap=true, others
  false. Verify exactly one becomes leader within 5 seconds.
- TestRaftNode_LeaderFailover: kill the leader, verify a new leader within 5s.
- TestRaftNode_Restart: leader writes 100 entries, kill it, start it back up,
  verify HasExistingState=true and we don't re-bootstrap.

Use raft's testing helpers (raft.NewInmemTransport, raft.NewInmemStore) for
fast tests where appropriate, but keep at least one test that uses the real
TCPTransport on loopback to catch transport-related bugs.
```

## Note on `BatchApplyCh: true`

This setting is the difference between 5 K samples/sec and 500 K samples/sec.
With `BatchApplyCh: false` (the default), every `raft.Apply()` call serialises
through a single channel send + FSM Apply call. With it set to true, hashicorp/raft
batches up to `MaxAppendEntries` log entries into a single FSM `ApplyBatch`.

For Helios, this means the FSM should implement `raft.BatchingFSM`:

```go
type HeliosFSM struct { engine *storage.Engine }

func (f *HeliosFSM) ApplyBatch(logs []*raft.Log) []interface{} {
    // Decode all log payloads into a single Sample batch
    var batch []storage.Sample
    for _, log := range logs {
        cmd, err := decodeFSMCommand(log.Data)
        if err != nil { /* ... */ }
        batch = append(batch, cmd.Samples...)
    }
    // Single Engine.Write call for the whole batch — amortises WAL fsync,
    // memtable lock acquisitions, etc.
    if err := f.engine.Write(batch); err != nil {
        // Return the same error for every log entry in the batch
        out := make([]interface{}, len(logs))
        for i := range out { out[i] = err }
        return out
    }
    return make([]interface{}, len(logs)) // nil for each
}

func (f *HeliosFSM) Apply(log *raft.Log) interface{} {
    // Required by raft.FSM but rarely called when BatchingFSM is implemented.
    return f.ApplyBatch([]*raft.Log{log})[0]
}
```

This single change is responsible for ~100x of the achievable throughput. v1
omitted it.
