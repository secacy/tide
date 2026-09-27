package mockasr

import "testing"

// mustWorker 用于预期合法的测试配置；构造失败时终止当前测试。
func mustWorker(t *testing.T, cfg Config) *Worker {
	t.Helper()
	w, err := New(cfg)
	if err != nil {
		t.Fatalf("create mock Worker: %v", err)
	}
	return w
}
