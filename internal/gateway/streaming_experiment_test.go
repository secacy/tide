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
	rpcCancelAt := make(chan time.Time, 1)
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
				rpcCancelAt <- time.Now()
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
	triggerStarted := time.Now()
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
	var handlerMS, rpcMS *float64
	if completed {
		value := float64(time.Since(triggerStarted)) / float64(time.Millisecond)
		handlerMS = &value
	}
	canceled := false
	select {
	case <-rpcCanceled:
		canceled = true
		value := float64((<-rpcCancelAt).Sub(triggerStarted)) / float64(time.Millisecond)
		rpcMS = &value
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
		"rpc_cancel_ms":               rpcMS,
		"handler_done_ms":             handlerMS,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", result)
}

// TestExperimentAudioQueueOverload 记录满队列触发失败与注销的边界。
// 第一块停在 Send，第二块占满队列，第三块必须显式失败。
func TestExperimentAudioQueueOverload(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("set TIDE_RUN_EXPERIMENTS=1 to run EXP-001 queue-full extension")
	}
	for _, limit := range []string{"bytes", "chunks"} {
		t.Run(limit, func(t *testing.T) {
			cfg := Config{MaxSessions: 1, AudioQueueMaxBytes: 2, AudioQueueMaxChunks: 10}
			if limit == "chunks" {
				cfg.AudioQueueMaxBytes, cfg.AudioQueueMaxChunks = 100, 1
			}
			entered, canceled := make(chan struct{}), make(chan struct{})
			h := newGatewayHarnessWithConfig(t, stalledSendWorker(entered, canceled), cfg)
			conn := h.mustDial(t)
			h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
			h.write(t, conn, websocket.MessageBinary, []byte{1, 0})
			awaitGatewaySignal(t, h.ctx, entered, "first audio in flight")
			h.write(t, conn, websocket.MessageBinary, []byte{2, 0})
			started := time.Now()
			h.write(t, conn, websocket.MessageBinary, []byte{3, 0})
			awaitGatewaySignal(t, h.ctx, canceled, "overload canceled RPC")
			cancelMS := float64(time.Since(started)) / float64(time.Millisecond)
			beforeClose := len(h.gateway.registry.snapshot())
			_, _, closeErr := conn.Read(h.ctx)
			closeCode := websocket.CloseStatus(closeErr)
			h.waitHandlers(t, 1)
			data, err := json.Marshal(map[string]any{
				"experiment": "EXP-001-queue-full", "limit": limit,
				"max_bytes": cfg.AudioQueueMaxBytes, "max_chunks": cfg.AudioQueueMaxChunks,
				"rpc_cancel_ms": cancelMS, "handler_done_ms": float64(time.Since(started)) / float64(time.Millisecond),
				"registered_before_close_ack": beforeClose, "registered_after_cleanup": len(h.gateway.registry.snapshot()),
				"close_code": closeCode,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("EXPERIMENT_RESULT %s", data)
		})
	}
}
