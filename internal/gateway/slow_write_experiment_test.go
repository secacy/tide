package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestExperimentSlowWebSocketWrite 区分写入停滞时的三种取消/终止来源。
// 它只记录观察结果，不把某个候选架构或基线缺陷作为测试的通过条件。
func TestExperimentSlowWebSocketWrite(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("set TIDE_RUN_EXPERIMENTS=1 to run EXP-002")
	}
	for _, trigger := range []string{"client_close", "gateway_abort", "worker_error_ready"} {
		t.Run(trigger, func(t *testing.T) {
			ctx, cancelTest := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelTest()
			audioReceived, terminalReady := make(chan struct{}), make(chan struct{})
			rpcCanceled := make(chan time.Time, 1)
			var recvCalls atomic.Int32
			worker := &controlledWorkerClient{open: func(rpcCtx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				// 独立观察取消，不依赖 download 再次调用 Recv。
				go func() {
					<-rpcCtx.Done()
					rpcCanceled <- time.Now()
				}()
				return &controlledWorkerStream{
					ctx: rpcCtx,
					send: func(*asrv1.StreamingRecognizeRequest) error {
						close(audioReceived) // 此实验仅发送一个音频块。
						return nil
					},
					recv: func() (*asrv1.StreamingRecognizeResponse, error) {
						if recvCalls.Add(1) == 1 {
							select {
							case <-audioReceived:
								return &asrv1.StreamingRecognizeResponse{
									SegmentId: "1", Text: strings.Repeat("x", 8192),
								}, nil
							case <-rpcCtx.Done():
								return nil, rpcCtx.Err()
							}
						}
						select {
						case <-terminalReady:
							return nil, status.Error(codes.Unavailable, "injected terminal Worker error")
						case <-rpcCtx.Done():
							return nil, rpcCtx.Err()
						}
					},
					closeSend: func() error { return nil },
				}, nil
			}}
			appCtx, cancelApp := context.WithCancel(context.Background())
			defer cancelApp()
			g, err := New(appCtx, worker, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{MaxSessions: 1})
			if err != nil {
				t.Fatal(err)
			}
			server, peer := net.Pipe()
			finished := make(chan time.Time, 1)
			handlerObserved, rpcObserved := false, false
			t.Cleanup(func() {
				g.Abort()
				_ = peer.Close()
				_ = server.Close()
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := g.Wait(cleanupCtx); err != nil {
					t.Errorf("experiment left registered sessions: %v", err)
				}
				if !handlerObserved {
					select {
					case <-finished:
					case <-cleanupCtx.Done():
						t.Error("experiment handler did not exit")
					}
				}
			})
			writer := &hijackTestWriter{
				ResponseRecorder: httptest.NewRecorder(),
				hijack: func() (net.Conn, *bufio.ReadWriter, error) {
					return server, bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server)), nil
				},
			}
			go func() {
				g.ServeHTTP(writer, websocketTestRequest())
				finished <- time.Now() // 所有 handler defer 已执行。
			}()
			deadline, _ := ctx.Deadline()
			_ = peer.SetDeadline(deadline)
			writeRawClientFrame(t, peer, 1, []byte(`{"type":"start","version":"v1"}`))
			writeRawClientFrame(t, peer, 2, []byte{0, 0})
			// net.Pipe 没有传输缓冲，只读取帧头前两字节，剩余结果必然阻塞。
			header := make([]byte, 2)
			if _, err := io.ReadFull(peer, header); err != nil {
				t.Fatal(err)
			}
			if header[0]&0xf != 1 {
				t.Fatalf("expected a text result frame, got %v", header)
			}
			const window = 100 * time.Millisecond
			started := time.Now()
			timer := time.NewTimer(window)
			defer timer.Stop()
			switch trigger {
			case "client_close":
				_ = peer.Close()
			case "gateway_abort":
				g.Abort()
			case "worker_error_ready":
				close(terminalReady)
			}
			var rpcMS, handlerMS *float64
		observe:
			for !handlerObserved || !rpcObserved {
				select {
				case at := <-rpcCanceled:
					rpcObserved = true
					value := float64(at.Sub(started)) / float64(time.Millisecond)
					rpcMS = &value
				case at := <-finished:
					handlerObserved = true
					value := float64(at.Sub(started)) / float64(time.Millisecond)
					handlerMS = &value
				case <-timer.C:
					break observe
				case <-ctx.Done():
					t.Fatal("experiment exceeded outer deadline")
				}
			}
			result := map[string]any{
				"experiment": "EXP-002", "trigger": trigger, "window_ms": window.Milliseconds(),
				"observation_ms": float64(time.Since(started)) / float64(time.Millisecond),
				"rpc_cancel_ms":  rpcMS, "handler_done_ms": handlerMS,
				"registered_at_observation": len(g.registry.snapshot()),
				"recv_calls_at_observation": recvCalls.Load(),
			}
			// 观测后的兜底动作不计入前面的 cancellation/handler 时间。
			g.Abort()
			if err := g.Wait(ctx); err != nil {
				t.Fatalf("final cleanup failed: %v", err)
			}
			if !handlerObserved {
				select {
				case <-finished:
					handlerObserved = true
				case <-ctx.Done():
					t.Fatal("handler did not finish after Abort")
				}
			}
			if !rpcObserved {
				select {
				case <-rpcCanceled:
				case <-ctx.Done():
					t.Fatal("RPC observer did not exit")
				}
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("EXPERIMENT_RESULT %s", encoded)
		})
	}
}
