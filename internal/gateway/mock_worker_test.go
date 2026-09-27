package gateway

import (
	"testing"

	"github.com/secacy/tide-artisan/internal/mockasr"
)

// mustMockWorker 为链路测试构造 Mock；配置错误应直接使测试失败。
func mustMockWorker(t *testing.T, cfg mockasr.Config) *mockasr.Worker {
	t.Helper()
	w, err := mockasr.New(cfg)
	if err != nil {
		t.Fatalf("create mock Worker: %v", err)
	}
	return w
}
