package cluster

import "testing"

func TestParsePeer(t *testing.T) {
	p := parsePeer("node2@helios-2:7000:8080")
	if p.ID != "node2" {
		t.Fatalf("id = %q", p.ID)
	}
	if p.RaftAddr != "helios-2:7000" {
		t.Fatalf("raft = %q", p.RaftAddr)
	}
	if p.HTTPAddr != "helios-2:8080" {
		t.Fatalf("http = %q", p.HTTPAddr)
	}
}

func TestBuildPeerHTTPLookup(t *testing.T) {
	cfg := Config{
		Peers: []string{
			"node1@helios-1:7000:8080",
			"node2@helios-2:7000:8081",
		},
	}
	m := buildPeerHTTPLookup(cfg)
	if m["helios-1:7000"] != "helios-1:8080" {
		t.Fatalf("missing helios-1 mapping: %#v", m)
	}
	if m["helios-2:7000"] != "helios-2:8081" {
		t.Fatalf("missing helios-2 mapping: %#v", m)
	}
}
