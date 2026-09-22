package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resultWriteReport 的阻塞起点来自底层 net.Conn.Write，并非整个 WebSocket Write 起点。
// Gateway.ServeHTTP 不公开 run 错误；此处记录自主清理，错误分类由独立集成测试验证。
type resultWriteReport struct {
	Outcome                 string          `json:"outcome"`
	BudgetMS                float64         `json:"budget_ms"`
	AtDetection             stallIOSnapshot `json:"at_detection"`
	AfterCleanup            stallIOSnapshot `json:"after_cleanup"`
	WorkerSends             stallIOSnapshot `json:"worker_sends"`
	PendingWriteToWorkerMS  *float64        `json:"pending_write_to_worker_ms"`
	PendingWriteToHandlerMS *float64        `json:"pending_write_to_handler_ms"`
	OriginToHandlerMS       *float64        `json:"origin_to_handler_ms"`
	WorkerError             string          `json:"worker_error,omitempty"`
	FallbackCancel          bool            `json:"fallback_cancel"`
	ActiveAfterCleanup      int             `json:"active_after_cleanup"`
	ReentryAccepted         bool            `json:"reentry_accepted"`
	FinalActive             int             `json:"final_active"`
}

// TestResultWriteSlowReader 验证真实 TCP 下无读取客户端不会长期占位。
func TestResultWriteSlowReader(t *testing.T) { runResultWriteExperiment(t) }

// TestResultWriteTimeoutExperiment 显式记录三轮时使用 -count=3，负载沿用旧基线。
func TestResultWriteTimeoutExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_RESULT_WRITE_TIMEOUT_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_RESULT_WRITE_TIMEOUT_EXPERIMENT=1")
	}
	runResultWriteExperiment(t)
}

func runResultWriteExperiment(t *testing.T) {
	t.Helper()
	const budget = 200 * time.Millisecond
	r := resultWriteReport{Outcome: "incomplete", BudgetMS: baselineMS(budget), ActiveAfterCleanup: -1, FinalActive: -1}
	defer func() {
		b, err := json.Marshal(r)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("RESULT_WRITE_JSON %s", b)
	}()
	w := &slowReaderWorker{exit: make(chan tailWorkerExit, 1)}
	workerClient := newBaselineTCPWorkerClient(t, w)
	appCtx, stop := context.WithCancel(context.Background())
	g, err := New(appCtx, workerClient, Config{MaxSessions: 1, InputIdleTimeout: 30 * time.Second, ResultWriteTimeout: budget})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	mainDone, reentryDone := make(chan time.Time, 1), make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/held" {
			defer func() { mainDone <- time.Now() }()
		} else {
			defer func() { reentryDone <- struct{}{} }()
		}
		g.ServeHTTP(rw, req)
	}))
	probe := &stallIOProbe{}
	enabled := &atomic.Bool{}
	server.Listener = &slowReaderListener{Listener: server.Listener, probe: probe, enabled: enabled}
	server.Start()
	var conn, reentry *websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		stop()
		if conn != nil {
			_ = conn.CloseNow()
		}
		if reentry != nil {
			_ = reentry.CloseNow()
		}
		server.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err = websocket.Dial(ctx, url+"/held", nil)
	if err != nil {
		t.Fatal(err)
	}
	origin := time.Now()
	enabled.Store(true)
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	if err = conn.Write(ctx, websocket.MessageBinary, []byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	// 不读取响应，不发送 end；测试没有主动关闭主客户端或取消服务来促成退出。
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
detect:
	for {
		select {
		case <-ticker.C:
			snapshot := probe.snapshot(origin)
			if snapshot.Started > snapshot.Returned && snapshot.PendingMS >= 50 {
				r.AtDetection = snapshot
				break detect
			}
		case <-mainDone:
			r.Outcome = "blocking_not_observed"
			t.Fatal("handler exited before persistent TCP Write observed")
		case <-ctx.Done():
			r.FallbackCancel = true
			stop()
			t.Fatal("blocking discovery timed out")
		}
	}
	ms := func(v float64) *float64 { return &v }
	select {
	case exit := <-w.exit:
		r.PendingWriteToWorkerMS = ms(baselineMS(exit.at.Sub(origin)) - r.AtDetection.PendingSinceMS)
		if exit.err == nil {
			t.Fatal("worker unexpectedly succeeded")
		}
		r.WorkerError = exit.err.Error()
		if status.Code(exit.err) != codes.Canceled {
			t.Fatalf("worker exit=%v", exit.err)
		}
	case <-ctx.Done():
		r.FallbackCancel = true
		stop()
		t.Fatal("worker did not exit")
	}
	select {
	case at := <-mainDone:
		r.OriginToHandlerMS = ms(baselineMS(at.Sub(origin)))
		r.PendingWriteToHandlerMS = ms(*r.OriginToHandlerMS - r.AtDetection.PendingSinceMS)
	case <-ctx.Done():
		r.FallbackCancel = true
		stop()
		t.Fatal("handler did not exit")
	}
	r.AfterCleanup = probe.snapshot(origin)
	r.WorkerSends = w.sends.snapshot(origin)
	if r.AfterCleanup.Started != r.AtDetection.Started || r.AfterCleanup.Returned != r.AfterCleanup.Started || r.AfterCleanup.Error == "" {
		t.Fatalf("observed Write not the failed final write: before=%+v after=%+v", r.AtDetection, r.AfterCleanup)
	}
	r.ActiveAfterCleanup = admissionActive(g.tracker)
	if r.ActiveAfterCleanup != 0 {
		t.Fatalf("active=%d", r.ActiveAfterCleanup)
	}
	enabled.Store(false) // 后续名额复用握手不计入主连接写入探针。
	reentry, _, err = websocket.Dial(ctx, url+"/reentry", nil)
	if err != nil {
		t.Fatalf("reentry failed: %v", err)
	}
	r.ReentryAccepted = true
	if admissionActive(g.tracker) != 1 {
		t.Fatal("reentry not counted")
	}
	_ = reentry.CloseNow() // 只验证重新接入，不创建第二个 ASR stream。
	select {
	case <-reentryDone:
	case <-ctx.Done():
		t.Fatal("reentry did not clean up")
	}
	r.FinalActive = admissionActive(g.tracker)
	if r.FinalActive != 0 {
		t.Fatalf("final active=%d", r.FinalActive)
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if appCtx.Err() != nil {
		t.Fatal("success depended on external cancellation")
	}
	r.Outcome = "autonomous_cleanup"
}
