package cluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

type Config struct {
	NodeID string
	// RaftDir holds raft log/stable/snapshot files. If empty, filepath.Join(DataDir,"raft") is used.
	DataDir   string
	RaftDir   string
	BindAddr  string
	Advertise string
	// HTTPAdvertise is the host:port used to forward follower writes to the leader (must match peers' HTTP).
	HTTPAdvertise string
	Bootstrap     bool
	Peers         []string

	SnapshotInterval time.Duration
	BatchSize        int
	BatchWait        time.Duration
}

type Node struct {
	raft           *raft.Raft
	fsm            *HeliosFSM
	cfg            Config
	bw             *batchWriter
	peerHTTPLookup map[string]string // raftAddr -> httpAddr
}

func NewNode(cfg Config, eng *engine.Engine) (*Node, error) {
	raftDir := cfg.RaftDir
	if raftDir == "" {
		raftDir = filepath.Join(cfg.DataDir, "raft")
	}
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.db"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", cfg.Advertise)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	if cfg.SnapshotInterval > 0 {
		rc.SnapshotInterval = cfg.SnapshotInterval
	}
	rc.SnapshotThreshold = 8192

	fsm := NewFSM(eng)
	r, err := raft.NewRaft(rc, fsm, logStore, stableStore, snaps, transport)
	if err != nil {
		return nil, err
	}
	n := &Node{
		raft:           r,
		fsm:            fsm,
		cfg:            cfg,
		peerHTTPLookup: buildPeerHTTPLookup(cfg),
	}
	if cfg.Bootstrap {
		if err := bootstrapCluster(r, cfg); err != nil {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}
	n.bw = newBatchWriter(r, 5*time.Second, cfg.BatchSize, cfg.BatchWait)
	return n, nil
}

func (n *Node) IsLeader() bool {
	if n == nil || n.raft == nil {
		return false
	}
	return n.raft.State() == raft.Leader
}
func (n *Node) Leader() string {
	if n == nil || n.raft == nil {
		return ""
	}
	return string(n.raft.Leader())
}
func (n *Node) State() string {
	if n == nil || n.raft == nil {
		return "unknown"
	}
	return n.raft.State().String()
}

func (n *Node) Apply(payload []byte) error {
	return n.bw.Submit(payload)
}

// Replicate encodes samples and submits them to Raft (leader only — callers should forward first).
func (n *Node) Replicate(samples []storage.Sample) error {
	payload, err := encodeBatch(samples)
	if err != nil {
		return err
	}
	return n.Apply(payload)
}

func (n *Node) Shutdown() error {
	return n.raft.Shutdown().Error()
}

func (n *Node) AddVoter(id, addr string) error {
	return n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second).Error()
}

func (n *Node) leaderHTTPAddr(raftAddr string) string {
	if v, ok := n.peerHTTPLookup[raftAddr]; ok {
		return v
	}
	return ""
}

func buildPeerHTTPLookup(cfg Config) map[string]string {
	m := map[string]string{}
	if cfg.Advertise != "" && cfg.HTTPAdvertise != "" {
		m[cfg.Advertise] = cfg.HTTPAdvertise
	}
	for _, raw := range cfg.Peers {
		p := parsePeer(strings.TrimSpace(raw))
		if p.RaftAddr != "" && p.HTTPAddr != "" {
			m[p.RaftAddr] = p.HTTPAddr
		}
	}
	return m
}
