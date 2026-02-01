// Command helios is the Helios TSDB process entry point.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/api"
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

	h := &api.Handler{Eng: eng, Version: Version}
	mux := api.NewServeMux(h)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	go func() {
		log.Printf("http listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	log.Printf("helios %s node=%s mem_points=%d next_wal_seq=%d",
		Version, cfg.NodeID, eng.MemLen(), eng.NextWALSeq())
	log.Printf("read: GET /api/v1/query /api/v1/query_range  write: POST /api/v1/write  data_dir=%s", cfg.DataDir)
	fmt.Fprintln(os.Stdout, "Helios: Ctrl+C to stop.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Print("shutting down http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
}
