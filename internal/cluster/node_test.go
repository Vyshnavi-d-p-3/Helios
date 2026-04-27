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
		Advertise:     "helios-1:7000",
		HTTPAdvertise: "helios-1:9090",
		Peers: []string{
			"node2@helios-2:7000:8081",
		},
	}
	m := buildPeerHTTPLookup(cfg)
	if m["helios-1:7000"] != "helios-1:9090" {
		t.Fatalf("missing self mapping: %#v", m)
	}
	if m["helios-2:7000"] != "helios-2:8081" {
		t.Fatalf("missing helios-2 mapping: %#v", m)
	}
}

func TestParsePeerEqualsForm(t *testing.T) {
	p := parsePeer("helios-2=helios-2:7000")
	if p.ID != "helios-2" || p.RaftAddr != "helios-2:7000" || p.HTTPAddr != "helios-2:8080" {
		t.Fatalf("got %#v", p)
	}
}
