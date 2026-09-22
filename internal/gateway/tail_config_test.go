package gateway

import (
	"context"
	"testing"
	"time"
)

// TestTailTimeoutConfig 验证默认预算、自定义预算和负值拒绝。
// 配置字段存在不代表默认值已经生效；这里检查 New 处理后的实际配置。
func TestTailTimeoutConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{"default", 0, 15 * time.Second},
		{"explicit", 200 * time.Millisecond, 200 * time.Millisecond},
		{"negative", -time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New(context.Background(), &recordingWorker{}, Config{TailTimeout: tc.configured})
			if tc.configured < 0 {
				if err == nil || g != nil {
					t.Fatalf("negative timeout: gateway=%v err=%v", g, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if g.cfg.TailTimeout != tc.want {
				t.Fatalf("tail timeout=%v want=%v", g.cfg.TailTimeout, tc.want)
			}
		})
	}
}
