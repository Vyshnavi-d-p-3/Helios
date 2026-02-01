// Package config provides typed configuration for Helios nodes.
//
// Configuration sources, in priority order:
//  1. Command-line flags
//  2. HELIOS_* environment variables
//  3. Defaults (DefaultConfig)
//
// v2 fix: configuration is now testable. v1 called flag.Parse() against the
// global flag.CommandLine, which has already been parsed by `go test` and
// can't be re-parsed. v2 takes an explicit FlagSet and []string args, with
// Parse() as a thin wrapper that uses os.Args[1:] for production.
package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds all configuration for a Helios node.
type Config struct {
	NodeID string

	HTTPAddr string
	GRPCAddr string
	RaftAddr string

	Peers      []string
	ShardCount int
	Bootstrap  bool

	DataDir         string
	MemtableMaxSize int64
	WALSyncMode     string
	WALSyncInterval time.Duration

	L0CompactionThreshold int
	MaxLevels             int

	// Retention: drop SSTables whose MaxTime is older than this.
	// 0 disables retention (all data kept indefinitely).
	RetentionPeriod time.Duration

	// Cardinality cap: per-metric series limit. Writes to a metric that
	// would exceed this are rejected with a clear error. 0 = unlimited
	// (NOT recommended in production — this is the single most common
	// cause of TSDB OOMs).
	MaxSeriesPerMetric int

	AnomalyAlpha     float64
	AnomalyThreshold float64
	AnomalyWarmup    int
	AnomalyMaxSeries int

	LogLevel string
}

// DefaultConfig returns production-ready defaults.
func DefaultConfig() Config {
	return Config{
		NodeID:                "helios-1",
		HTTPAddr:              ":8080",
		GRPCAddr:              ":9090",
		RaftAddr:              ":7000",
		ShardCount:            1, // single-shard by default; explicit opt-in to multi-shard
		Bootstrap:             false,
		DataDir:               "./data",
		MemtableMaxSize:       64 * 1024 * 1024,
		WALSyncMode:           "batch",
		WALSyncInterval:       10 * time.Millisecond,
		L0CompactionThreshold: 4,
		MaxLevels:             4,
		RetentionPeriod:       30 * 24 * time.Hour, // 30 days
		MaxSeriesPerMetric:    100_000,
		AnomalyAlpha:          0.3,
		AnomalyThreshold:      3.0,
		AnomalyWarmup:         10,
		AnomalyMaxSeries:      1_000_000,
		LogLevel:              "info",
	}
}

// ParseArgs builds a Config from the given flag set and arguments.
// This is the testable seam — pass a fresh FlagSet to avoid global state.
//
// Resolution order: defaults → env → flags. Flags override env, env overrides
// defaults. List values (peers) accept comma-separated input.
//
// package flag stops at the first non-flag argument. For bool flags, prefer
// -bootstrap=true (or -bootstrap=false) when more flags follow; a bare
// -bootstrap makes the very next token the first "non-flag", so values like
// "true" or later "-peers" are not applied as flags.
func ParseArgs(fs *flag.FlagSet, args []string) (Config, error) {
	cfg := DefaultConfig()

	// Apply environment variables to the defaults BEFORE binding flags so
	// that flag defaults reflect the env-overridden values. This makes
	// `--help` show the values that would actually be used.
	cfg.applyEnv()

	var peersStr string
	fs.StringVar(&cfg.NodeID, "node-id", cfg.NodeID, "Unique node identifier")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP API listen address")
	fs.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	fs.StringVar(&cfg.RaftAddr, "raft-addr", cfg.RaftAddr, "Raft transport listen address")
	fs.IntVar(&cfg.ShardCount, "shard-count", cfg.ShardCount, "Number of shards (1 = single-shard, replicated)")
	fs.BoolVar(&cfg.Bootstrap, "bootstrap", cfg.Bootstrap, "Bootstrap a new Raft cluster")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Data directory")
	fs.Int64Var(&cfg.MemtableMaxSize, "memtable-max-size", cfg.MemtableMaxSize, "Max memtable size in bytes")
	fs.StringVar(&cfg.WALSyncMode, "wal-sync-mode", cfg.WALSyncMode, "WAL sync mode: immediate|batch")
	fs.DurationVar(&cfg.WALSyncInterval, "wal-sync-interval", cfg.WALSyncInterval, "Batch sync interval")
	fs.IntVar(&cfg.L0CompactionThreshold, "l0-compaction-threshold", cfg.L0CompactionThreshold, "L0 SSTable count trigger")
	fs.IntVar(&cfg.MaxLevels, "max-levels", cfg.MaxLevels, "Maximum SSTable levels")
	fs.DurationVar(&cfg.RetentionPeriod, "retention-period", cfg.RetentionPeriod, "Drop data older than this; 0 = unlimited")
	fs.IntVar(&cfg.MaxSeriesPerMetric, "max-series-per-metric", cfg.MaxSeriesPerMetric, "Per-metric cardinality cap")
	fs.Float64Var(&cfg.AnomalyAlpha, "anomaly-alpha", cfg.AnomalyAlpha, "EWMA smoothing factor (0,1)")
	fs.Float64Var(&cfg.AnomalyThreshold, "anomaly-threshold", cfg.AnomalyThreshold, "Anomaly z-score threshold")
	fs.IntVar(&cfg.AnomalyWarmup, "anomaly-warmup", cfg.AnomalyWarmup, "Samples before detection activates")
	fs.IntVar(&cfg.AnomalyMaxSeries, "anomaly-max-series", cfg.AnomalyMaxSeries, "Anomaly registry cardinality cap")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug|info|warn|error")
	fs.StringVar(&peersStr, "peers", strings.Join(cfg.Peers, ","), "Comma-separated Raft peers")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if peersStr != "" {
		cfg.Peers = splitCSV(peersStr)
	}
	return cfg, nil
}

// Parse is the production entrypoint: a fresh FlagSet over os.Args[1:].
// Tests should call ParseArgs directly with their own FlagSet.
func Parse() (Config, error) {
	fs := flag.NewFlagSet("helios", flag.ContinueOnError)
	return ParseArgs(fs, os.Args[1:])
}

// applyEnv reads HELIOS_* environment variables and overrides matching fields.
// Unknown env vars are silently ignored (forwards-compatible).
func (c *Config) applyEnv() {
	getEnv := func(key string, set func(string)) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			set(v)
		}
	}

	getEnv("HELIOS_NODE_ID", func(v string) { c.NodeID = v })
	getEnv("HELIOS_HTTP_ADDR", func(v string) { c.HTTPAddr = v })
	getEnv("HELIOS_GRPC_ADDR", func(v string) { c.GRPCAddr = v })
	getEnv("HELIOS_RAFT_ADDR", func(v string) { c.RaftAddr = v })
	getEnv("HELIOS_DATA_DIR", func(v string) { c.DataDir = v })
	getEnv("HELIOS_LOG_LEVEL", func(v string) { c.LogLevel = v })
	getEnv("HELIOS_PEERS", func(v string) { c.Peers = splitCSV(v) })

	// Avoid pulling in strconv noise here — the flag pass will re-parse
	// these from string defaults if they're flag-overridable. For env-only
	// numeric values, add explicit parsers as the need arises.
	if v, ok := os.LookupEnv("HELIOS_BOOTSTRAP"); ok {
		c.Bootstrap = v == "true" || v == "1"
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Validate returns the first internal-consistency error, or nil.
func (c Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node-id is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	if c.ShardCount < 1 {
		return fmt.Errorf("shard-count must be >= 1, got %d", c.ShardCount)
	}
	if c.MemtableMaxSize < 1<<20 {
		return fmt.Errorf("memtable-max-size must be >= 1MiB, got %d", c.MemtableMaxSize)
	}
	if c.AnomalyAlpha <= 0 || c.AnomalyAlpha >= 1 {
		return fmt.Errorf("anomaly-alpha must be in (0,1), got %f", c.AnomalyAlpha)
	}
	if c.AnomalyThreshold <= 0 {
		return fmt.Errorf("anomaly-threshold must be > 0, got %f", c.AnomalyThreshold)
	}
	if c.WALSyncMode != "immediate" && c.WALSyncMode != "batch" {
		return fmt.Errorf("wal-sync-mode must be 'immediate' or 'batch', got %q", c.WALSyncMode)
	}
	if c.Bootstrap && len(c.Peers) == 0 {
		return fmt.Errorf("bootstrap=true requires at least one peer (the node itself)")
	}
	return nil
}
