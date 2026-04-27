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
)

type Config struct {
	NodeID    string
	DataDir   string
	BindAddr  string
	Advertise string
	Bootstrap bool
	Peers     []string

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
	raftDir := filepath.Join(cfg.DataDir, "raft")
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

func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }
func (n *Node) Leader() string { return string(n.raft.Leader()) }
func (n *Node) State() string  { return n.raft.State().String() }

func (n *Node) Apply(payload []byte) error {
	return n.bw.Submit(payload)
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
	// include self if peers omit it
	if cfg.Advertise != "" && cfg.BindAddr != "" {
		selfHost := strings.Split(cfg.Advertise, ":")[0]
		httpPort := strings.TrimPrefix(cfg.BindAddr, ":")
		if httpPort != "" {
			m[cfg.Advertise] = fmt.Sprintf("%s:%s", selfHost, httpPort)
		}
	}
	for _, raw := range cfg.Peers {
		p := parsePeer(strings.TrimSpace(raw))
		if p.RaftAddr != "" && p.HTTPAddr != "" {
			m[p.RaftAddr] = p.HTTPAddr
		}
	}
	return m
}
