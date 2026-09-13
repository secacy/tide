package gateway

import (
	"context"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
	"testing"
	"time"
)

func TestGatewayStreamingConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want Config
	}{
		{
			name: "defaults",
			cfg:  Config{MaxSessions: 1},
			want: Config{ProcessingTimeout: 3 * time.Second, EndTimeout: 5 * time.Second, MaxUnprocessedChunks: 4096, MaxSessions: 1, MaxMessageBytes: 1024 * 1024,
				AudioQueueMaxBytes: 64_000, AudioQueueMaxChunks: 128, ResultWriteTimeout: 2 * time.Second},
		},
		{
			name: "explicit",
			cfg: Config{ProcessingTimeout: time.Second, EndTimeout: 2 * time.Second, MaxUnprocessedChunks: 42, MaxSessions: 2, MaxMessageBytes: 4096,
				AudioQueueMaxBytes: 8000, AudioQueueMaxChunks: 8, ResultWriteTimeout: 250 * time.Millisecond},
			want: Config{ProcessingTimeout: time.Second, EndTimeout: 2 * time.Second, MaxUnprocessedChunks: 42, MaxSessions: 2, MaxMessageBytes: 4096,
				AudioQueueMaxBytes: 8000, AudioQueueMaxChunks: 8, ResultWriteTimeout: 250 * time.Millisecond},
		},
		{
			name: "partial override",
			cfg:  Config{MaxSessions: 1, AudioQueueMaxChunks: 1},
			want: Config{ProcessingTimeout: 3 * time.Second, EndTimeout: 5 * time.Second, MaxUnprocessedChunks: 4096, MaxSessions: 1, MaxMessageBytes: 1024 * 1024,
				AudioQueueMaxBytes: 64_000, AudioQueueMaxChunks: 1, ResultWriteTimeout: 2 * time.Second},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := &unusedGatewayWorker{}
			g, err := New(context.Background(), worker, nil, tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			tc.want.Heartbeat = wsheartbeat.Config{Interval: 2 * time.Second, Timeout: 3 * time.Second}
			if g.cfg != tc.want {
				t.Fatalf("normalized config = %+v, want %+v", g.cfg, tc.want)
			}
			if worker.calls.Load() != 0 {
				t.Fatal("configuration opened a Worker stream")
			}
		})
	}
}

func TestGatewayRejectsNegativeStreamingConfig(t *testing.T) {
	for _, cfg := range []Config{
		{MaxSessions: 1, AudioQueueMaxBytes: -1},
		{MaxSessions: 1, AudioQueueMaxChunks: -1},
		{MaxSessions: 1, ResultWriteTimeout: -time.Nanosecond},
		{MaxSessions: 1, ProcessingTimeout: -time.Nanosecond},
		{MaxSessions: 1, EndTimeout: -time.Nanosecond},
		{MaxSessions: 1, MaxUnprocessedChunks: -1},
		{MaxSessions: 1, Heartbeat: wsheartbeat.Config{Interval: -1}},
		{MaxSessions: 1, Heartbeat: wsheartbeat.Config{Timeout: -1}},
	} {
		g, err := New(context.Background(), &unusedGatewayWorker{}, nil, cfg)
		if g != nil || err == nil {
			t.Fatalf("New(%+v) = (%v, %v), want configuration error", cfg, g, err)
		}
	}
}
