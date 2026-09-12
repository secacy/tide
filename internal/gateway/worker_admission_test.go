package gateway

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// 接入上限分别来自 Gateway 与 Worker；准备阶段已占名额，拒绝不创建 RPC。
func TestGatewayIndependentWorkerAndSessionLimits(t *testing.T) {
	for _, limits := range [][2]int{{1, 3}, {3, 1}} {
		t.Run(string(rune('0'+limits[0])), func(t *testing.T) {
			worker := &unusedGatewayWorker{}
			pool, err := NewWorkerPool([]WorkerConfig{{ID: "a", Client: worker, Capacity: limits[1]}}, RoundRobin)
			if err != nil {
				t.Fatal(err)
			}
			h := newGatewayHarnessWithPool(t, pool, Config{MaxSessions: limits[0]})
			conn := h.mustDial(t)
			if got := pool.Snapshot()[0].Reserved; got != 1 {
				t.Fatal(got)
			}
			h.expectHTTPStatus(t, http.StatusServiceUnavailable)
			h.waitHandlers(t, 1)
			if got := pool.Snapshot()[0].Reserved; got != 1 {
				t.Fatal("rejected request altered existing lease", got)
			}
			if worker.calls.Load() != 0 {
				t.Fatal("Worker opened before Start")
			}
			conn.CloseNow()
			h.waitHandlers(t, 1)
			if pool.Snapshot()[0].Reserved != 0 {
				t.Fatal("disconnect leaked lease")
			}
		})
	}
}

func TestGatewayWorkerLeaseFailurePaths(t *testing.T) {
	worker := &controlledWorkerClient{open: func(context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return nil, errors.New("injected open failure")
	}}
	h := newGatewayHarness(t, worker, 1)
	// 升级失败两次，证明回收后可再次预留。
	for range 2 {
		h.expectHTTPStatus(t, http.StatusUpgradeRequired)
		h.waitHandlers(t, 1)
		if h.gateway.pool.Snapshot()[0].Reserved != 0 {
			t.Fatal("upgrade failure leaked")
		}
	}
	for _, valid := range []bool{false, true} {
		conn := h.mustDial(t)
		start := `{"type":"start","version":"invalid"}`
		code := websocket.StatusPolicyViolation
		if valid {
			start = `{"type":"start","version":"v1"}`
			code = websocket.StatusInternalError
		}
		h.write(t, conn, websocket.MessageText, []byte(start))
		h.expectClose(t, conn, code)
		h.waitHandlers(t, 1)
		if h.gateway.pool.Snapshot()[0].Reserved != 0 {
			t.Fatal("failed Start/open leaked")
		}
	}
}

// 停止 Worker 新预留不会撤销准备中的会话；结果来自该会话最初绑定的 Worker。
func TestGatewayPinnedWorkerSurvivesStopAccepting(t *testing.T) {
	for _, policy := range []WorkerSelectionPolicy{RoundRobin, LeastReservedRatio} {
		t.Run(string(policy), func(t *testing.T) {
			var configs []WorkerConfig
			for _, id := range []string{"a", "b"} {
				configs = append(configs, WorkerConfig{ID: id, Capacity: 1, Client: startGatewayWorker(t, mockasr.New(mockasr.Config{FinalText: id}))})
			}
			pool, err := NewWorkerPool(configs, policy)
			if err != nil {
				t.Fatal(err)
			}
			h := newGatewayHarnessWithPool(t, pool, Config{MaxSessions: 3})
			a, b := h.mustDial(t), h.mustDial(t)
			if err := pool.StopAccepting("a"); err != nil {
				t.Fatal(err)
			}
			h.expectHTTPStatus(t, http.StatusServiceUnavailable)
			h.waitHandlers(t, 1)
			complete := func(conn *websocket.Conn, id string) {
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
				h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
				h.expectResult(t, conn, id, true)
				h.expectClose(t, conn, websocket.StatusNormalClosure)
				h.waitHandlers(t, 1)
			}
			complete(a, "a")
			// A 虽已释放，仍停止接入；B 尚占用，不能误接纳。
			h.expectHTTPStatus(t, http.StatusServiceUnavailable)
			h.waitHandlers(t, 1)
			complete(b, "b")
			complete(h.mustDial(t), "b")
			h.gateway.StopAccepting()
			if err := h.gateway.Wait(h.ctx); err != nil {
				t.Fatal(err)
			}
			for _, s := range pool.Snapshot() {
				if s.Reserved != 0 {
					t.Fatal(s)
				}
			}
		})
	}
}
