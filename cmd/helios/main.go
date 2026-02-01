// Command helios is the Helios TSDB process entry point.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // register default pprof on http.DefaultServeMux; see withPprof
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/api"
	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
)

// withPprof serves net/http/pprof (heap, profile, etc.) and delegates other paths
// to next. pprof is registered on http.DefaultServeMux by the import side effect.
// Do not expose this service untrusted; restrict the listen address or a reverse proxy in production.
func withPprof(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/debug/pprof") {
			http.DefaultServeMux.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// runRetentionInitialDelay runs one EnforceRetention after a short delay (cancel on shutdown).
func runRetentionInitialDelay(ctx context.Context, eng *engine.Engine) {
	t := time.NewTimer(10 * time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
		n, err := eng.EnforceRetention()
		if err != nil {
			log.Printf("retention (initial): %v", err)
			return
		}
		if n > 0 {
			log.Printf("retention (initial): removed %d sstable file(s)", n)
		}
	}
}

// runRetentionBackground calls EnforceRetention on a timer until ctx is done.
func runRetentionBackground(ctx context.Context, eng *engine.Engine, cfg config.Config) {
	t := time.NewTicker(cfg.RetentionGCTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := eng.EnforceRetention()
			if err != nil {
				log.Printf("retention gc: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("retention gc: removed %d sstable file(s)", n)
			}
		}
	}
}

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

	runCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	if cfg.RetentionPeriod > 0 && cfg.RetentionGCTickInterval > 0 {
		go runRetentionBackground(runCtx, eng, cfg)
	}
	// One shot shortly after start so we do not wait a full tick before the first GC.
	if cfg.RetentionPeriod > 0 {
		go runRetentionInitialDelay(runCtx, eng)
	}

	h := &api.Handler{Eng: eng, Version: Version}
	mux := api.NewServeMux(h)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: withPprof(mux)}

	go func() {
		log.Printf("http listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	log.Printf("helios %s node=%s mem_points=%d next_wal_seq=%d",
		Version, cfg.NodeID, eng.MemLen(), eng.NextWALSeq())
	if cfg.RetentionPeriod > 0 && cfg.RetentionGCTickInterval > 0 {
		log.Printf("retention: background GC every %s (max SST age %s)", cfg.RetentionGCTickInterval, cfg.RetentionPeriod)
	}
	log.Printf("probes: GET /-/healthy /-/ready  pprof: GET /debug/pprof/  read: GET /api/v1/query /api/v1/query_range  write: POST /api/v1/write  flush: POST /api/v1/flush  POST /api/v1/retention  GET /metrics  data_dir=%s", cfg.DataDir)
	fmt.Fprintln(os.Stdout, "Helios: Ctrl+C to stop.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	stopBackground()
	log.Print("shutting down http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
}
