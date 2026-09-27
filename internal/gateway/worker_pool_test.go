package gateway

import (
	"testing"

	"github.com/secacy/tide-artisan/internal/workerpool"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// singleWorkerPool 为原有单后端测试组装固定选择器，保持旧测试的会话/故障语义。
func singleWorkerPool(t *testing.T, client asrv1.ASRServiceClient) *workerpool.RoundRobin {
	t.Helper()
	pool, err := workerpool.NewRoundRobin([]workerpool.Worker{{ID: "test-worker", Client: client}})
	if err != nil {
		t.Fatal(err)
	}
	return pool
}
