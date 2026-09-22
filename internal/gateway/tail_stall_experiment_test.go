package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// tailStallReport 记录有限观察窗口，不把观察结束解释为系统已自主退出。
type tailStallReport struct {
	Outcome                string            `json:"outcome"`
	ObservationMS          float64           `json:"observation_ms"`
	ActiveAfterObservation int               `json:"active_after_observation"`
	RejectedAdmission      *admissionAttempt `json:"rejected_admission"`
	FallbackCancel         bool              `json:"fallback_cancel"`
	CloseCode              int               `json:"close_code"`
	WorkerCanceled         bool              `json:"worker_canceled"`
	CancelToHandlerMS      *float64          `json:"cancel_to_handler_ms"`
	FinalActive            int               `json:"final_active"`
}

// TestTailStallBaselineExperiment 在尚未接入尾部期限的版本记录对照。
// Worker 已读取 EOF 但不返回尾部，客户端保持连接且持续读取；500ms 后才执行外部取消兜底。
// 后续改造对照须另设测试预算，不能仅凭本测试的有限观察窗口判定是否有超时保护。
func TestTailStallBaselineExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_TAIL_STALL_BASELINE") != "1" {
		t.Skip("set TIDE_RUN_TAIL_STALL_BASELINE=1")
	}
	report := tailStallReport{Outcome: "incomplete", ActiveAfterObservation: -1, FinalActive: -1}
	defer func() {
		data, err := json.Marshal(report)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("TAIL_BASELINE_JSON %s", data)
	}()
	w := &normalEndWorker{inputEnded: make(chan struct{}), releaseTail: make(chan struct{}), finished: make(chan error, 1)}
	workerClient := newBaselineTCPWorkerClient(t, w)
	appCtx, stop := context.WithCancel(context.Background())
	g, err := New(appCtx, workerClient, Config{MaxSessions: 1, InputIdleTimeout: 100 * time.Millisecond, WorkerSendTimeout: 100 * time.Millisecond})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	handlerDone := make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/held" {
			defer func() { handlerDone <- time.Now() }()
		}
		g.ServeHTTP(rw, r)
	}))
	var conn *websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		stop()
		if conn != nil {
			_ = conn.CloseNow()
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
	for i := 1; i <= 2; i++ {
		if err = conn.Write(ctx, websocket.MessageBinary, []byte(fmt.Sprintf("audio-%d", i))); err != nil {
			t.Fatal(err)
		}
		if _, _, err = conn.Read(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.inputEnded:
	case <-ctx.Done():
		t.Fatal("worker did not observe EOF")
	}
	observedEOF := time.Now() // 测试观察到通知的时刻，不冒充协调者的计时起点。
	readDone := make(chan error, 1)
	go func() { _, _, err := conn.Read(ctx); readDone <- err }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			select {
			case <-readDone:
			case <-time.After(time.Second):
				t.Error("receiver did not exit")
			}
		}
	}()
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case err := <-readDone:
		joined = true
		t.Fatalf("client ended during observation: %v", err)
	case err := <-w.finished:
		t.Fatalf("worker exited during observation: %v", err)
	case <-handlerDone:
		t.Fatal("handler ended during observation")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	report.ObservationMS = baselineMS(time.Since(observedEOF))
	report.ActiveAfterObservation = admissionActive(g.tracker)
	if report.ActiveAfterObservation != 1 {
		t.Fatalf("active=%d", report.ActiveAfterObservation)
	}
	probe := dialAdmission(ctx, url+"/probe", http.DefaultClient, 1)
	report.RejectedAdmission = &probe
	if probe.conn != nil {
		_ = probe.conn.CloseNow()
	}
	if probe.Outcome != "capacity_rejected" {
		t.Fatalf("expected full Gateway: %+v", probe)
	}
	// 从此处开始才允许兜底；不能将随后退出记为尾部期限生效。
	report.FallbackCancel = true
	canceledAt := time.Now()
	stop()
	select {
	case err := <-readDone:
		joined = true
		report.CloseCode = int(websocket.CloseStatus(err))
		if websocket.CloseStatus(err) != websocket.StatusGoingAway {
			t.Fatalf("fallback close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("client did not observe shutdown")
	}
	select {
	case err := <-w.finished:
		report.WorkerCanceled = errors.Is(err, context.Canceled)
		if !report.WorkerCanceled {
			t.Fatalf("worker exit: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("worker did not exit")
	}
	select {
	case at := <-handlerDone:
		elapsed := baselineMS(at.Sub(canceledAt))
		report.CancelToHandlerMS = &elapsed
	case <-ctx.Done():
		t.Fatal("handler did not exit")
	}
	report.FinalActive = admissionActive(g.tracker)
	if report.FinalActive != 0 {
		t.Fatalf("final active=%d", report.FinalActive)
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	report.Outcome = "required_external_cancel"
}
