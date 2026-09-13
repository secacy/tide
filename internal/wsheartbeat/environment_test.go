package wsheartbeat

import (
	"testing"
	"time"
)

func TestFromEnvironment(t *testing.T) {
	for _, tc := range []struct {
		interval, timeout string
		valid             bool
		want              time.Duration
	}{
		{"", "", true, 2 * time.Second},
		{"5s", "500ms", true, 5 * time.Second},
		{"2", "3s", false, 0},
		{"2s", "invalid", false, 0},
		{"-1s", "3s", false, 0},
	} {
		cfg, err := FromEnvironment(func(key string) string {
			if key == "TIDE_HEARTBEAT_INTERVAL" {
				return tc.interval
			}
			return tc.timeout
		})
		if (err == nil) != tc.valid || (tc.valid && cfg.Interval != tc.want) {
			t.Fatalf("%+v: %+v %v", tc, cfg, err)
		}
	}
}
