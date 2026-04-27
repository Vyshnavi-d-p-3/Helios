package cluster

import (
	"encoding/json"
	"io"
	"net/http"
)

type joinRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// LocalID returns this node's Raft server ID (same as process -node-id).
func (n *Node) LocalID() string {
	return n.cfg.NodeID
}

// LeaderInfoHandler exposes leader address and Raft role for operators.
func (n *Node) LeaderInfoHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"node_id":    n.LocalID(),
			"leader":     n.Leader(),
			"state":      n.State(),
			"raft_local": n.cfg.Advertise,
		})
	}
}

// JoinHandler lets the leader add a voter at runtime (dynamic membership).
func (n *Node) JoinHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !n.IsLeader() {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		var req joinRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.ID == "" || req.Address == "" {
			http.Error(w, "id and address required", http.StatusBadRequest)
			return
		}
		if err := n.AddVoter(req.ID, req.Address); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
