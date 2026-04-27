package cluster

import "testing"

func TestRaftBindAdvertise(t *testing.T) {
	b, a, err := RaftBindAdvertise(":7000")
	if err != nil || b != ":7000" || a != "127.0.0.1:7000" {
		t.Fatalf("got %q %q err=%v", b, a, err)
	}
	b, a, err = RaftBindAdvertise("node:7000")
	if err != nil || b != ":7000" || a != "node:7000" {
		t.Fatalf("got %q %q err=%v", b, a, err)
	}
}

func TestHTTPAdvertise(t *testing.T) {
	v, err := HTTPAdvertise(":8080", "helios-1:7000")
	if err != nil || v != "helios-1:8080" {
		t.Fatalf("got %q err=%v", v, err)
	}
	v, err = HTTPAdvertise("0.0.0.0:8080", "10.0.0.5:7000")
	if err != nil || v != "10.0.0.5:8080" {
		t.Fatalf("got %q err=%v", v, err)
	}
}
