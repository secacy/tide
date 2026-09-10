package gateway

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// TestExperimentSendStallDisconnect 记录停滞发送下的断开感知行为。
// 这是显式启用的诊断实验，不断言“必须复现缺陷”，可供后续方案使用同一负载比较。
func TestExperimentSendStallDisconnect(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("set TIDE_RUN_EXPERIMENTS=1 to run EXP-001")
	}
	entered, rpcCanceled := make(chan struct{}), make(chan struct{})
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return &controlledWorkerStream{
			ctx: ctx,
			send: func(*asrv1.StreamingRecognizeRequest) error {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			},
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				<-ctx.Done()
				close(rpcCanceled)
				return nil, ctx.Err()
			},
		}, nil
	}}
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	awaitGatewaySignal(t, h.ctx, entered, "Send entered stalled operation")
	if err := conn.CloseNow(); err != nil {
		t.Fatal(err)
	}
	const window = 100 * time.Millisecond
	started := time.Now()
	timer := time.NewTimer(window)
	defer timer.Stop()
	completed := false
	select {
	case <-h.finished:
		completed = true
	case <-timer.C:
	case <-h.ctx.Done():
		t.Fatal("experiment exceeded harness deadline")
	}
	observedMS := float64(time.Since(started)) / float64(time.Millisecond)
	canceled := false
	select {
	case <-rpcCanceled:
		canceled = true
	default:
	}
	registered := len(h.gateway.registry.snapshot())
	cleanupStarted := time.Now()
	h.gateway.Abort()
	if err := h.gateway.Wait(h.ctx); err != nil {
		t.Fatalf("experiment cleanup failed: %v", err)
	}
	if !completed {
		h.waitHandlers(t, 1)
	}
	awaitGatewaySignal(t, h.ctx, rpcCanceled, "RPC canceled by disconnect or final cleanup")
	// JSON 字段定义与 EXP-001 对应，输出可以作为原始实验记录保存。
	result, err := json.Marshal(map[string]any{
		"experiment":                  "EXP-001",
		"window_ms":                   window.Milliseconds(),
		"observation_ms":              observedMS,
		"handler_completed_in_window": completed,
		"rpc_canceled_at_observation": canceled,
		"registered_at_observation":   registered,
		"abort_cleanup_ms":            float64(time.Since(cleanupStarted)) / float64(time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", result)
}
