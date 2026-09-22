package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// tailWorkerExit 保存 Worker 方法实际返回的时刻；通过 channel 交接，避免并发读写。
type tailWorkerExit struct {
	at  time.Time
	err error
}

type tailObservedWorker struct {
	*normalEndWorker
	exit chan tailWorkerExit
}

func (w *tailObservedWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	err := w.normalEndWorker.StreamingRecognize(s)
	w.exit <- tailWorkerExit{time.Now(), err}
	return err
}

// tailIntegrationReport 的时间均相对测试观察到 Worker EOF 通知的时刻。
// EOF 通知与协调者启动 timer 不是同一事件；这些数据不能用来推导精确计时误差。
type tailIntegrationReport struct {
	Case                 string   `json:"case"`
	Outcome              string   `json:"outcome"`
	BudgetMS             float64  `json:"budget_ms"`
	BeforeEndWaitMS      float64  `json:"before_end_wait_ms"`
	TailDelayMS          float64  `json:"tail_delay_ms"`
	CloseCode            int      `json:"close_code"`
	CloseReason          string   `json:"close_reason"`
	EOFFromEndWriteMS    *float64 `json:"eof_from_end_write_ms"`
	WorkerExitFromEOFMS  *float64 `json:"worker_exit_from_eof_ms"`
	ClientCloseFromEOFMS *float64 `json:"client_close_from_eof_ms"`
	HandlerExitFromEOFMS *float64 `json:"handler_exit_from_eof_ms"`
	WorkerCanceled       bool     `json:"worker_canceled"`
	FallbackCancel       bool     `json:"fallback_cancel"`
	ActiveAfterCleanup   int      `json:"active_after_cleanup"`
	ReentryAccepted      bool     `json:"reentry_accepted"`
	FinalActive          int      `json:"final_active"`
}

type tailIntegrationCase struct {
	name                         string
	budget, beforeEnd, tailDelay time.Duration
	stall                        bool
}

// TestTailSessionIntegration 覆盖完整链路：长于预算的输入阶段、合法尾部和尾部永久等待。
func TestTailSessionIntegration(t *testing.T) {
	for _, tc := range []tailIntegrationCase{
		{"before_end_exceeds_budget", 200 * time.Millisecond, 300 * time.Millisecond, 50 * time.Millisecond, false},
		{"normal_delayed_tail", time.Second, 0, 500 * time.Millisecond, false},
		{"stalled_tail", 200 * time.Millisecond, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) { runTailIntegration(t, tc) })
	}
}

// TestTailTimeoutExperiment 与改造前使用相同的 EOF 停滞 Worker，测试预算设置为 200ms。
func TestTailTimeoutExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_TAIL_TIMEOUT_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_TAIL_TIMEOUT_EXPERIMENT=1")
	}
	runTailIntegration(t, tailIntegrationCase{name: "stalled_tail", budget: 200 * time.Millisecond, stall: true})
}

func runTailIntegration(t *testing.T, tc tailIntegrationCase) {
	t.Helper()
	r := tailIntegrationReport{Case: tc.name, Outcome: "incomplete", BudgetMS: baselineMS(tc.budget), BeforeEndWaitMS: baselineMS(tc.beforeEnd), TailDelayMS: baselineMS(tc.tailDelay), ActiveAfterCleanup: -1, FinalActive: -1}
	defer func() {
		b, err := json.Marshal(r)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("TAIL_TIMEOUT_JSON %s", b)
	}()
	w := &tailObservedWorker{normalEndWorker: &normalEndWorker{inputEnded: make(chan struct{}), releaseTail: make(chan struct{}), finished: make(chan error, 1)}, exit: make(chan tailWorkerExit, 1)}
	workerClient := newBaselineTCPWorkerClient(t, w)
	appCtx, stop := context.WithCancel(context.Background())
	// 输入阶段等待超过尾部预算时，另给足输入空闲预算，避免混淆两种原因。
	idle := 100 * time.Millisecond
	if tc.beforeEnd > 0 {
		idle = 2 * time.Second
	}
	g, err := New(appCtx, workerClient, Config{MaxSessions: 1, InputIdleTimeout: idle, WorkerSendTimeout: 100 * time.Millisecond, TailTimeout: tc.budget})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	mainDone, reentryDone := make(chan time.Time, 1), make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/held" {
			defer func() { mainDone <- time.Now() }()
		} else {
			defer func() { reentryDone <- time.Now() }()
		}
		g.ServeHTTP(rw, req)
	}))
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
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	if err = conn.Write(ctx, websocket.MessageBinary, []byte("audio-1")); err != nil {
		t.Fatal(err)
	}
	assertRecognitionResult(t, ctx, conn, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "segment-1", Text: "第一段定稿", IsFinal: true})
	if tc.beforeEnd > 0 {
		timer := time.NewTimer(tc.beforeEnd)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
	}
	if err = conn.Write(ctx, websocket.MessageBinary, []byte("audio-2")); err != nil {
		t.Fatal(err)
	}
	assertRecognitionResult(t, ctx, conn, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "segment-2", Text: "第二段中间结果"})
	endAt := time.Now()
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.inputEnded:
	case <-ctx.Done():
		t.Fatal("worker did not see EOF")
	}
	eofAt := time.Now()
	ms := func(d time.Duration) *float64 { v := baselineMS(d); return &v }
	r.EOFFromEndWriteMS = ms(eofAt.Sub(endAt))
	// 接收者持续读取；正常尾部的延迟通过可取消的单次回调放行，不暂停客户端读取。
	if !tc.stall {
		timer := time.AfterFunc(tc.tailDelay, func() { close(w.releaseTail) })
		defer timer.Stop()
		for _, expected := range []wsprotocol.ResultMessage{
			{Type: wsprotocol.MessageTypeResult, SegmentID: "segment-2", Text: "第二段定稿", IsFinal: true},
			{Type: wsprotocol.MessageTypeResult, SegmentID: "segment-3", Text: "尾部补充结果", IsFinal: true},
		} {
			assertRecognitionResult(t, ctx, conn, expected)
		}
	}
	_, _, err = conn.Read(ctx)
	r.ClientCloseFromEOFMS = ms(time.Since(eofAt))
	r.CloseCode = int(websocket.CloseStatus(err))
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		r.CloseReason = closeErr.Reason
	}
	if ctx.Err() != nil {
		r.FallbackCancel = true
		stop()
		t.Fatalf("experiment timeout: %v", err)
	}
	if tc.stall {
		if r.CloseCode != int(websocket.StatusInternalError) || r.CloseReason != "tail timeout" {
			t.Fatalf("expected tail timeout, got %v", err)
		}
	} else if r.CloseCode != int(websocket.StatusNormalClosure) {
		t.Fatalf("expected normal closure: %v", err)
	}
	select {
	case exit := <-w.exit:
		r.WorkerExitFromEOFMS = ms(exit.at.Sub(eofAt))
		r.WorkerCanceled = errors.Is(exit.err, context.Canceled)
		if tc.stall && !r.WorkerCanceled {
			t.Fatalf("worker was not canceled: %v", exit.err)
		}
		if !tc.stall && exit.err != nil {
			t.Fatalf("normal worker exit: %v", exit.err)
		}
	case <-ctx.Done():
		r.FallbackCancel = true
		stop()
		t.Fatal("worker did not exit")
	}
	select {
	case at := <-mainDone:
		r.HandlerExitFromEOFMS = ms(at.Sub(eofAt))
	case <-ctx.Done():
		r.FallbackCancel = true
		stop()
		t.Fatal("handler did not exit")
	}
	r.ActiveAfterCleanup = admissionActive(g.tracker)
	if r.ActiveAfterCleanup != 0 {
		t.Fatalf("active after cleanup=%d", r.ActiveAfterCleanup)
	}
	reentry, _, err = websocket.Dial(ctx, url+"/reentry", nil)
	if err != nil {
		t.Fatalf("released slot not reusable: %v", err)
	}
	r.ReentryAccepted = true
	if admissionActive(g.tracker) != 1 {
		t.Fatal("reentry not counted")
	}
	// 此处只验证名额复用；新连接不发送 start，不创建第二条 Worker RPC。
	_ = reentry.CloseNow()
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
		t.Fatal("completion depended on service cancellation")
	}
	r.Outcome = "completed"
	if tc.stall {
		r.Outcome = "tail_timeout"
	}
}
