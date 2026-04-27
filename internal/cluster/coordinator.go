package cluster

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

// LeaderForwardingHandler forwards writes to the current leader when this node
// is not the leader. On the leader, it executes localHandler directly.
func (n *Node) LeaderForwardingHandler(localHandler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if n.IsLeader() {
			localHandler(w, r)
			return
		}
		leader := n.Leader()
		if leader == "" {
			http.Error(w, "no leader available", http.StatusServiceUnavailable)
			return
		}
		httpAddr := n.leaderHTTPAddr(leader)
		if httpAddr == "" {
			http.Error(w, "leader http address unknown", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		targetURL := fmt.Sprintf("http://%s%s", httpAddr, r.URL.RequestURI())
		req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		req.Header = r.Header.Clone()
		req.Header.Set("X-Helios-Forwarded-By", n.cfg.NodeID)
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}
}
