package cluster

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLeaderInfoHandler_NoRaftInstance(t *testing.T) {
	n := &Node{cfg: Config{NodeID: "n1", Advertise: "n1:7000"}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cluster/leader", nil)
	n.LeaderInfoHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{`"node_id":"n1"`, `"leader":""`, `"state":"unknown"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body=%q missing %s", body, want)
		}
	}
}

func TestJoinHandler_MethodAndLeaderChecks(t *testing.T) {
	n := &Node{cfg: Config{NodeID: "n1"}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cluster/join", nil)
	n.JoinHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status=%d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/cluster/join", strings.NewReader(`{"id":"n2","address":"n2:7000"}`))
	n.JoinHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("follower status=%d", rr.Code)
	}
}
