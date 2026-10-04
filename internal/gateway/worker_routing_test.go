package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/workerpool"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGatewayWorkerRouting 验证顺序会话及同时保持的多会话固定后端、三块音频与尾部结果。
func TestGatewayWorkerRouting(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		name, n := "sequential", 6
		if overlap {
			name, n = "overlapping", 12
		}
		t.Run(name, func(t *testing.T) {
			workers, backends := newRoutingBackends(t)
			f := newRoutingFixture(t, workers, Config{})
			ctx := routingContext(t)
			conns := make([]*websocket.Conn, n)
			for i := range n {
				id, worker := fmt.Sprintf("session-%d", i), workers[i%2].ID
				conn := f.dial(t, ctx, id)
				conns[i] = conn
				routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
				routingWrite(t, ctx, conn, websocket.MessageBinary, id+":first")
				routingResult(t, ctx, conn, id, worker, "first", false)
				if !overlap {
					finishRoutingSession(t, ctx, conn, id, worker)
					f.waitHandlers(t, ctx, 1)
					f.assertActive(t, 0)
				}
			}
			if overlap {
				f.assertActive(t, n)
				// 会话都已建立后并行传输剩余音频；无需依赖调度恰好造成交叠。
				t.Run("finish", func(t *testing.T) {
					for i := range n {
						t.Run(fmt.Sprint(i), func(t *testing.T) {
							t.Parallel()
							finishRoutingSession(t, ctx, conns[i], fmt.Sprintf("session-%d", i), workers[i%2].ID)
						})
					}
				})
				f.waitHandlers(t, ctx, n)
				f.assertActive(t, 0)
			}
			checkRoutingExits(t, ctx, backends, n/2)
			if next := f.pool.Pick(); next.ID != "A" {
				t.Fatalf("unexpected selection count: next=%s", next.ID)
			}
			t.Logf("sessions=%d Worker A=%d B=%d, three audio chunks and final per session", n, backends[0].started.Load(), backends[1].started.Load())
		})
	}
}

// TestGatewayRoutingBeforeStart 不合规/未开始的连接不得消耗轮询位置或建立后端流。
func TestGatewayRoutingBeforeStart(t *testing.T) {
	for _, mode := range []string{"invalid_start", "start_timeout", "disconnect_before_start", "shutdown_before_start", "stopped_admission", "failed_upgrade"} {
		t.Run(mode, func(t *testing.T) {
			a, b := &recordingWorker{}, &recordingWorker{}
			f := newRoutingFixture(t, []workerpool.Worker{{ID: "A", Client: a}, {ID: "B", Client: b}}, Config{StartTimeout: 100 * time.Millisecond})
			ctx := routingContext(t)
			runRoutingBeforeStart(t, ctx, f, mode)
			if a.called.Load() || b.called.Load() {
				t.Fatal("Worker called before legal start")
			}
			if next := f.pool.Pick(); next.ID != "A" {
				t.Fatalf("pre-start exit advanced selection to %s", next.ID)
			}
		})
	}
}

// TestGatewayRoutingAdmissionRejected 满额拒绝不推进选择，持有会话正常完成后名额可复用。
func TestGatewayRoutingAdmissionRejected(t *testing.T) {
	workers, backends := newRoutingBackends(t)
	f := newRoutingFixture(t, workers, Config{MaxSessions: 1})
	ctx := routingContext(t)
	first := f.dial(t, ctx, "first")
	routingWrite(t, ctx, first, websocket.MessageText, `{"type":"start","version":"v1"}`)
	routingWrite(t, ctx, first, websocket.MessageBinary, "first:first")
	routingResult(t, ctx, first, "first", "A", "first", false)
	conn, response, err := websocket.Dial(ctx, f.url+"/rejected", nil)
	if conn != nil {
		conn.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %v %v", response, err)
	}
	f.waitHandlers(t, ctx, 1)
	f.assertActive(t, 1)
	finishRoutingSession(t, ctx, first, "first", "A")
	f.waitHandlers(t, ctx, 1)
	f.assertActive(t, 0)
	second := f.dial(t, ctx, "second")
	routingWrite(t, ctx, second, websocket.MessageText, `{"type":"start","version":"v1"}`)
	routingWrite(t, ctx, second, websocket.MessageBinary, "second:first")
	routingResult(t, ctx, second, "second", "B", "first", false)
	finishRoutingSession(t, ctx, second, "second", "B")
	f.waitHandlers(t, ctx, 1)
	f.assertActive(t, 0)
	checkRoutingExits(t, ctx, backends, 1)
}

// routingOpenFailure 在客户端建流处确定性失败，保留 RPC context 供清理断言。
type routingOpenFailure struct {
	calls    atomic.Int64
	contexts chan context.Context
	cause    error
}

func (c *routingOpenFailure) StreamingRecognize(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	c.calls.Add(1)
	c.contexts <- ctx
	return nil, c.cause
}

// TestGatewayRoutingOpenFailure 验证失败不重试/回退，下一会话正常轮到 B，之后再次轮到 A。
func TestGatewayRoutingOpenFailure(t *testing.T) {
	failure := &routingOpenFailure{contexts: make(chan context.Context, 3), cause: status.Error(codes.Unavailable, "injected open failure")}
	backend := &routingBackend{id: "B", exits: make(chan error, 3)}
	f := newRoutingFixture(t, []workerpool.Worker{{ID: "A", Client: failure}, {ID: "B", Client: newBaselineTCPWorkerClient(t, backend)}}, Config{MaxSessions: 1})
	ctx := routingContext(t)
	for i := range 3 {
		id := fmt.Sprintf("failure-%d", i)
		conn := f.dial(t, ctx, id)
		routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
		if i == 1 {
			routingWrite(t, ctx, conn, websocket.MessageBinary, id+":first")
			routingResult(t, ctx, conn, id, "B", "first", false)
			finishRoutingSession(t, ctx, conn, id, "B")
		} else {
			_, _, err := conn.Read(ctx)
			if websocket.CloseStatus(err) != websocket.StatusInternalError {
				t.Fatalf("expected 1011: %v", err)
			}
		}
		f.waitHandlers(t, ctx, 1)
		f.assertActive(t, 0)
		if i != 1 {
			select {
			case rpcCtx := <-failure.contexts:
				if !errors.Is(rpcCtx.Err(), context.Canceled) {
					t.Errorf("failed RPC context not canceled: %v", rpcCtx.Err())
				}
			case <-ctx.Done():
				t.Fatal("failed Worker not called")
			}
		}
		if i == 0 && backend.started.Load() != 0 {
			t.Fatal("failed stream retried another Worker")
		}
	}
	if failure.calls.Load() != 2 {
		t.Errorf("failed Worker called %d times, want 2", failure.calls.Load())
	}
	checkRoutingExits(t, ctx, []*routingBackend{backend}, 1)
}

// TestSessionRoutingErrorIdentity 直接观测 run 返回值，检查 Worker ID 和原始错误未丢失。
func TestSessionRoutingErrorIdentity(t *testing.T) {
	cause := status.Error(codes.Unavailable, "identity failure")
	client := &routingOpenFailure{contexts: make(chan context.Context, 1), cause: cause}
	pool, err := workerpool.NewRoundRobin([]workerpool.Worker{{ID: "backend-A", Client: client}})
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		id  string
		err error
	}
	done := make(chan outcome, 1)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		defer ws.CloseNow()
		s := newSession(ws, pool, time.Second, time.Second, time.Second, time.Second, time.Second, 0)
		err = s.run(parent)
		done <- outcome{id: s.workerID, err: err}
	}))
	defer server.Close()
	ctx := routingContext(t)
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
	_, _, err = conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusInternalError {
		t.Fatalf("expected 1011: %v", err)
	}
	select {
	case got := <-done:
		if got.id != "backend-A" || !errors.Is(got.err, cause) || !strings.Contains(got.err.Error(), "backend-A") {
			t.Fatalf("lost worker identity or cause: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("session did not finish")
	}
}

// TestGatewayRoutingCancellation 验证选中不同后端的活动会话在断连/停服后清理，
// 单客户端断开不影响另一后端上的健康会话；本测试不依赖关闭共享 gRPC 连接解除阻塞。
func TestGatewayRoutingCancellation(t *testing.T) {
	for _, mode := range []string{"client_disconnect", "service_shutdown"} {
		t.Run(mode, func(t *testing.T) {
			workers, backends := newRoutingBackends(t)
			f := newRoutingFixture(t, workers, Config{})
			ctx := routingContext(t)
			conns := make([]*websocket.Conn, 2)
			for i := range 2 {
				id := fmt.Sprintf("cancel-%d", i)
				conns[i] = f.dial(t, ctx, id)
				routingWrite(t, ctx, conns[i], websocket.MessageText, `{"type":"start","version":"v1"}`)
				routingWrite(t, ctx, conns[i], websocket.MessageBinary, id+":first")
				routingResult(t, ctx, conns[i], id, workers[i].ID, "first", false)
			}
			f.assertActive(t, 2)
			if mode == "client_disconnect" {
				conns[0].CloseNow()
				f.waitHandlers(t, ctx, 1)
				f.assertActive(t, 1)
				select {
				case err := <-backends[0].exits:
					if status.Code(err) != codes.Canceled {
						t.Errorf("disconnected RPC exit=%v", err)
					}
				case <-ctx.Done():
					t.Fatal("disconnected Worker did not exit")
				}
				finishRoutingSession(t, ctx, conns[1], "cancel-1", "B")
				f.waitHandlers(t, ctx, 1)
				checkRoutingExits(t, ctx, backends[1:], 1)
			} else {
				f.g.StopAccepting()
				f.cancel()
				for _, conn := range conns {
					_, _, err := conn.Read(ctx)
					if websocket.CloseStatus(err) != websocket.StatusGoingAway {
						t.Errorf("shutdown close=%v", err)
					}
				}
				f.waitHandlers(t, ctx, 2)
				if err := f.g.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				for _, w := range backends {
					select {
					case err := <-w.exits:
						if status.Code(err) != codes.Canceled {
							t.Errorf("shutdown RPC exit=%v", err)
						}
					case <-ctx.Done():
						t.Fatal("Worker did not exit on shutdown")
					}
				}
			}
			f.assertActive(t, 0)
			for _, w := range backends {
				if w.started.Load() != 1 {
					t.Errorf("Worker %s streams=%d want=1", w.id, w.started.Load())
				}
			}
			if next := f.pool.Pick(); next.ID != "A" {
				t.Fatalf("cleanup changed selection: next=%s", next.ID)
			}
		})
	}
}
