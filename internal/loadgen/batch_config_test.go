package loadgen_test

import (
	"math"
	"testing"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// TestBatchConfigValidateTotalBoundary 仅检查数值边界，不为极大计划分配会话。
// 乘法边界两侧相邻的合法 PCM 长度，必须分别被接受和拒绝。
func TestBatchConfigValidateTotalBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sessions int
		bytes    int64
		valid    bool
	}{
		{"one_max_aligned", 1, math.MaxInt64 - 1, true},
		{"two_below_limit", 2, math.MaxInt64/2 - 1, true},
		{"two_above_limit", 2, math.MaxInt64/2 + 1, false},
		{"zero_bytes_before_division", 1, 0, false},
		{"negative_bytes_before_division", 1, -2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadgen.BatchConfig{Sessions: tc.sessions, Session: sessionConfig("ws://unresolvable.invalid/asr")}
			cfg.Session.AudioBytes = tc.bytes
			before := cfg
			if err := cfg.Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate = %v, want valid=%v", err, tc.valid)
			}
			if cfg != before {
				t.Fatal("Validate changed configuration")
			}
		})
	}
}
