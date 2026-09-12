package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/secacy/tide-artisan/internal/gateway"
)

func TestWorkerSettings(t *testing.T) {
	cfg, err := readWorkerSettings("")
	if err != nil || cfg.MaxSessions != 100 || len(cfg.Workers) != 1 {
		t.Fatalf("defaults: %+v %v", cfg, err)
	}
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"multi", `{"max_sessions":8,"worker_policy":"least_reserved_ratio","workers":[{"id":"a","address":"localhost:50051","capacity":2},{"id":"b","address":"localhost:50052","capacity":6}]}`, true},
		{"single", `{"max_sessions":1,"workers":[{"id":"a","address":"localhost:50051","capacity":1}]}`, true},
		{"missing policy", `{"max_sessions":2,"workers":[{"id":"a","address":"a:1","capacity":1},{"id":"b","address":"b:1","capacity":1}]}`, false},
		{"duplicate address", `{"max_sessions":2,"worker_policy":"round_robin","workers":[{"id":"a","address":"a:1","capacity":1},{"id":"b","address":"a:1","capacity":1}]}`, false},
		{"unknown field", `{"max_session":2}`, false},
		{"zero", `{"max_sessions":0,"workers":[{"id":"a","address":"a:1","capacity":1}]}`, false},
		{"trailing", `{} {}`, false},
		{"empty", `null`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "gateway.json")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := readWorkerSettings(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
			if !tc.valid {
				return
			}
			pool, cleanup, err := openWorkerPool(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			lease, err := pool.TryAcquire()
			if err != nil {
				t.Fatal(err)
			}
			lease.Release()
			if cfg.Policy != gateway.RoundRobin && cfg.Policy != gateway.LeastReservedRatio {
				t.Fatal("invalid policy")
			}
		})
	}
}
