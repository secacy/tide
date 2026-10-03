package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// constructorOnly 类型的 Pick 一旦被调用就失败，用于验证构造校验没有消耗选择。
type constructorOnlyValue struct{}

func (constructorOnlyValue) Pick() workerpool.Worker { panic("constructor must not select") }

type constructorOnlyFunc func() workerpool.Worker

func (f constructorOnlyFunc) Pick() workerpool.Worker { return f() }

type constructorOnlyMap map[string]int

func (constructorOnlyMap) Pick() workerpool.Worker { panic("constructor must not select") }

type constructorOnlySlice []int

func (constructorOnlySlice) Pick() workerpool.Worker { panic("constructor must not select") }

type constructorOnlyChan chan int

func (constructorOnlyChan) Pick() workerpool.Worker { panic("constructor must not select") }

// countedWorkerSelector 观测调用次数，实际选择交给生产策略；计数不是容量指标。
type countedWorkerSelector struct {
	selector WorkerSelector
	calls    atomic.Int64
}

func (s *countedWorkerSelector) Pick() workerpool.Worker {
	s.calls.Add(1)
	return s.selector.Pick()
}
func newCountedWeightedSelector(t *testing.T, workers []workerpool.Worker) *countedWorkerSelector {
	t.Helper()
	selector, err := workerpool.NewWeightedRoundRobin([]workerpool.WeightedWorker{
		{Worker: workers[0], Weight: 2}, {Worker: workers[1], Weight: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &countedWorkerSelector{selector: selector}
}

// TestGatewaySelectorNilContract 从公开 New 入口检查 nil 动态值及非指针实现，不直接测试反射分支。
func TestGatewaySelectorNilContract(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selector WorkerSelector
		invalid  bool
	}{
		{"nil_interface", nil, true},
		{"nil_round_robin", (*workerpool.RoundRobin)(nil), true},
		{"nil_weighted_round_robin", (*workerpool.WeightedRoundRobin)(nil), true},
		{"nil_custom_pointer", (*constructorOnlyValue)(nil), true},
		{"nil_func", constructorOnlyFunc(nil), true},
		{"nil_map", constructorOnlyMap(nil), true},
		{"nil_slice", constructorOnlySlice(nil), true},
		{"nil_chan", constructorOnlyChan(nil), true},
		{"value", constructorOnlyValue{}, false},
		{"pointer", &constructorOnlyValue{}, false},
		{"func", constructorOnlyFunc(func() workerpool.Worker { panic("constructor must not select") }), false},
		{"empty_map", constructorOnlyMap{}, false},
		{"empty_slice", constructorOnlySlice{}, false},
		{"chan", make(constructorOnlyChan), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New(context.Background(), tc.selector, Config{})
			if tc.invalid {
				if err == nil || g != nil {
					t.Fatalf("nil selector accepted: gateway=%v err=%v", g, err)
				}
			} else if err != nil || g == nil {
				t.Fatalf("non-nil selector rejected: gateway=%v err=%v", g, err)
			}
		})
	}
}

// TestGatewayWeightedRouting 检查合法 start 后只选一次，以及交叠会话的多块音频、尾部和清理。
func TestGatewayWeightedRouting(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		name := "sequential"
		if overlap {
			name = "overlapping"
		}
		t.Run(name, func(t *testing.T) {
			workers, backends := newRoutingBackends(t)
			selector := newCountedWeightedSelector(t, workers)
			f := newSelectorRoutingFixture(t, selector, Config{})
			if selector.calls.Load() != 0 {
				t.Fatal("constructor consumed selection")
			}
			ctx := routingContext(t)
			const n = 12
			conns := make([]*websocket.Conn, n)
			for i := range n {
				id := fmt.Sprintf("weighted-%d", i)
				worker := string("ABA"[i%3])
				conn := f.dial(t, ctx, id)
				conns[i] = conn
				routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
				routingWrite(t, ctx, conn, websocket.MessageBinary, id+":first")
				routingResult(t, ctx, conn, id, worker, "first", false)
				if got := selector.calls.Load(); got != int64(i+1) {
					t.Fatalf("Pick calls=%d want=%d", got, i+1)
				}
				if !overlap {
					finishRoutingSession(t, ctx, conn, id, worker)
					f.waitHandlers(t, ctx, 1)
					f.assertActive(t, 0)
				}
			}
			if overlap {
				f.assertActive(t, n)
				t.Run("finish", func(t *testing.T) {
					for i := range n {
						t.Run(fmt.Sprint(i), func(t *testing.T) {
							t.Parallel()
							finishRoutingSession(t, ctx, conns[i], fmt.Sprintf("weighted-%d", i), string("ABA"[i%3]))
						})
					}
				})
				f.waitHandlers(t, ctx, n)
				f.assertActive(t, 0)
			}
			if got := selector.calls.Load(); got != n {
				t.Fatalf("audio/end/cleanup consumed selection: calls=%d", got)
			}
			for i, backend := range backends {
				want := 8
				if i == 1 {
					want = 4
				}
				checkRoutingExits(t, ctx, []*routingBackend{backend}, want)
			}
			t.Logf("sessions=12 selections=%d streams A=%d B=%d; three chunks and final remain bound", selector.calls.Load(), backends[0].started.Load(), backends[1].started.Load())
		})
	}
}

// TestGatewayWeightedBeforeStart 不合法或未开始的连接不得消耗选择，即使存在可用加权后端。
func TestGatewayWeightedBeforeStart(t *testing.T) {
	for _, mode := range []string{"invalid_start", "start_timeout", "disconnect", "service_shutdown", "stopped_admission", "failed_upgrade"} {
		t.Run(mode, func(t *testing.T) {
			a, b := &recordingWorker{}, &recordingWorker{}
			selector := newCountedWeightedSelector(t, []workerpool.Worker{{ID: "A", Client: a}, {ID: "B", Client: b}})
			f := newSelectorRoutingFixture(t, selector, Config{StartTimeout: 100 * time.Millisecond})
			ctx := routingContext(t)
			switch mode {
			case "stopped_admission":
				f.g.StopAccepting()
				conn, response, err := websocket.Dial(ctx, f.url, nil)
				if conn != nil {
					conn.CloseNow()
				}
				if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("expected 503: %v %v", response, err)
				}
			case "failed_upgrade":
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http"+strings.TrimPrefix(f.url, "ws"), nil)
				if err != nil {
					t.Fatal(err)
				}
				response, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode == http.StatusSwitchingProtocols {
					t.Fatal("unexpected upgrade")
				}
			default:
				conn := f.dial(t, ctx, mode)
				switch mode {
				case "invalid_start":
					routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"end"}`)
				case "disconnect":
					conn.CloseNow()
				case "service_shutdown":
					f.g.StopAccepting()
					f.cancel()
				}
				if mode != "disconnect" {
					_, _, err := conn.Read(ctx)
					if err == nil || ctx.Err() != nil {
						t.Fatalf("server did not close independently: %v", err)
					}
				}
			}
			f.waitHandlers(t, ctx, 1)
			f.assertActive(t, 0)
			if selector.calls.Load() != 0 || a.called.Load() || b.called.Load() {
				t.Fatal("pre-start exit consumed selection or opened RPC")
			}
		})
	}
}

// TestGatewayWeightedOpenFailure 验证建流失败不重选、不回退，失败流取消后会话名额可复用。
func TestGatewayWeightedOpenFailure(t *testing.T) {
	failure := &routingOpenFailure{contexts: make(chan context.Context, 6), cause: status.Error(codes.Unavailable, "weighted open failure")}
	backend := &routingBackend{id: "B", exits: make(chan error, 6)}
	selector := newCountedWeightedSelector(t, []workerpool.Worker{{ID: "A", Client: failure}, {ID: "B", Client: newBaselineTCPWorkerClient(t, backend)}})
	f := newSelectorRoutingFixture(t, selector, Config{MaxSessions: 1})
	ctx := routingContext(t)
	for i, worker := range "ABAABA" {
		id := fmt.Sprintf("weighted-failure-%d", i)
		conn := f.dial(t, ctx, id)
		routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
		if worker == 'B' {
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
		if selector.calls.Load() != int64(i+1) {
			t.Fatal("failed open retried or other operation consumed selection")
		}
		if worker == 'A' {
			select {
			case rpcCtx := <-failure.contexts:
				if !errors.Is(rpcCtx.Err(), context.Canceled) {
					t.Fatalf("failed RPC not canceled: %v", rpcCtx.Err())
				}
			case <-ctx.Done():
				t.Fatal("failed backend was not called")
			}
		}
	}
	if failure.calls.Load() != 4 {
		t.Fatalf("failed backend calls=%d want=4", failure.calls.Load())
	}
	checkRoutingExits(t, ctx, []*routingBackend{backend}, 2)
}
