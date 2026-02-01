// Command helios is the Helios TSDB process entry point.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
)

// Version is set at build time (e.g. -ldflags "-X main.Version=0.1.0").
var Version = "dev"

func main() {
	log.SetFlags(0)
	log.SetPrefix("helios: ")

	cfg, err := config.Parse()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("config: %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		log.Fatalf("data dir: %v", err)
	}

	eng, err := engine.Open(cfg)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}
	defer eng.Close()

	log.Printf("helios %s node=%s http=%s mem_points=%d next_wal_seq=%d",
		Version, cfg.NodeID, cfg.HTTPAddr, eng.MemLen(), eng.NextWALSeq())
	log.Printf("storage data_dir=%s (WAL + memtable; HTTP/query next)", cfg.DataDir)
	fmt.Fprintln(os.Stdout, "Helios: signal SIGINT or SIGTERM to stop.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Print("shutting down")
}
