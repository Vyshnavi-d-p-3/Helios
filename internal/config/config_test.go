package config

import (
	"flag"
	"testing"
)

func TestParseArgs_freshFlagSet(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	cfg, err := ParseArgs(fs, []string{
		"-peers", "a,b,c",
		"-node-id", "n1",
		"-http-addr", ":18080",
		"-bootstrap=true",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v; peers=%#v bootstrap=%v", err, cfg.Peers, cfg.Bootstrap)
	}
	if cfg.NodeID != "n1" || cfg.HTTPAddr != ":18080" {
		t.Fatalf("unexpected cfg %+v", cfg)
	}
	if !cfg.Bootstrap || len(cfg.Peers) != 3 {
		t.Fatalf("bootstrap/peers: %+v", cfg)
	}
}
