package gateway

import (
	"context"
	"testing"
	"time"
)

func TestV2EntryConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*V2Config)
		limit  int64
	}{
		{"handshakes", func(c *V2Config) { c.MaxHandshakes = -1 }, 128},
		{"entry_timeout", func(c *V2Config) { c.EntryTimeout = -1 }, 128},
		{"resume_window", func(c *V2Config) { c.ResumeWindow = -1 }, 128},
		{"retention", func(c *V2Config) { c.ResultRetentionTimeout = -1 }, 128},
		{"status_timeout", func(c *V2Config) { c.WorkerStatusTimeout = -1 }, 128},
		{"audio_chunks", func(c *V2Config) { c.MaxAudioChunks = -1 }, 128},
		{"results", func(c *V2Config) { c.MaxResults = -1 }, 128},
		{"short_message", func(c *V2Config) {}, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := V2Config{}
			tc.change(&c)
			if _, err := New(context.Background(), &v2EntrySelector{}, Config{V2: &c, MaxMessageBytes: tc.limit}); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	t.Run("copy_defaults_and_shared_tracker", func(t *testing.T) {
		c := V2Config{MaxHandshakes: 3, EntryTimeout: time.Second}
		g, err := New(context.Background(), &v2EntrySelector{}, Config{MaxSessions: 2, V2: &c})
		if err != nil {
			t.Fatal(err)
		}
		c.MaxHandshakes = 999
		if g.v2 == &c || g.v2.MaxHandshakes != 3 || g.v2.EntryTimeout != time.Second || g.v2.MaxAudioBytes != 1<<20 || g.v2.MaxAudioChunks != 256 || g.gate.tracker != g.tracker || g.cfg.V2 != nil {
			t.Fatal("configuration is aliased or defaults are missing")
		}
	})
}
