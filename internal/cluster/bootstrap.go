package cluster

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// bootstrapCluster runs only on the seed node on first startup.
// Peer format: "node1@10.0.0.1:7000:8080" (id@raft:http)
func bootstrapCluster(r *raft.Raft, cfg Config) error {
	fut := r.GetConfiguration()
	if err := fut.Error(); err == nil && len(fut.Configuration().Servers) > 0 {
		return nil
	}

	servers := []raft.Server{{
		Suffrage: raft.Voter,
		ID:       raft.ServerID(cfg.NodeID),
		Address:  raft.ServerAddress(cfg.Advertise),
	}}
	for _, peer := range cfg.Peers {
		p := parsePeer(strings.TrimSpace(peer))
		if p.ID == "" || p.ID == cfg.NodeID || p.RaftAddr == "" {
			continue
		}
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(p.ID),
			Address:  raft.ServerAddress(p.RaftAddr),
		})
	}
	f := r.BootstrapCluster(raft.Configuration{Servers: servers})
	if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

type peerInfo struct {
	ID       string
	RaftAddr string
	HTTPAddr string
}

func parsePeer(s string) peerInfo {
	// id@raft:port:http_port
	parts := strings.SplitN(s, "@", 2)
	if len(parts) != 2 {
		return peerInfo{}
	}
	id := strings.TrimSpace(parts[0])
	addrParts := strings.Split(parts[1], ":")
	if len(addrParts) < 2 {
		return peerInfo{}
	}
	host := strings.TrimSpace(addrParts[0])
	raftPort := strings.TrimSpace(addrParts[1])
	p := peerInfo{
		ID:       id,
		RaftAddr: fmt.Sprintf("%s:%s", host, raftPort),
	}
	if len(addrParts) >= 3 {
		p.HTTPAddr = fmt.Sprintf("%s:%s", host, strings.TrimSpace(addrParts[2]))
	}
	return p
}
