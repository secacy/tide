package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// admissionWorker 验证每条流的一块 PCM，并统计真实 RPC 数量和退出结果。
type admissionWorker struct {
	asrv1.UnimplementedASRServiceServer
	worker  *mockasr.Worker
	started atomic.Int64
	exits   chan baselineWorkerExit
}

func (w *admissionWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	w.started.Add(1)
	observed := &baselineStream{BidiStreamingServer: s}
	err := w.worker.StreamingRecognize(observed)
	w.exits <- baselineWorkerExit{observed.chunks, observed.bytes, err}
	return err
}

// admissionAttempt 保留成功与失败握手的耗时、状态、原因；计时包含本机建连。
type admissionAttempt struct {
	Index     int     `json:"index"`
	Outcome   string  `json:"outcome"`
	Status    int     `json:"status"`
	ElapsedMS float64 `json:"elapsed_ms"`
	Error     string  `json:"error,omitempty"`
	Body      string  `json:"body,omitempty"`
	conn      *websocket.Conn
}

type admissionRound struct {
	Round                   int                `json:"round"`
	Outcome                 string             `json:"outcome"`
	Attempts                []admissionAttempt `json:"attempts"`
	Accepted                int                `json:"accepted"`
	Rejected                int                `json:"rejected"`
	OtherErrors             int                `json:"other_errors"`
	MaxObservedActive       int                `json:"max_observed_active"`
	HeldActive              int                `json:"held_active"`
	WorkerStartsBeforeStart int64              `json:"worker_starts_before_start"`
	Completed               int                `json:"completed"`
	ActiveAfterCleanup      int                `json:"active_after_cleanup"`
	Reentry                 *admissionAttempt  `json:"reentry"`
	ReentryCompleted        bool               `json:"reentry_completed"`
	FinalActive             int                `json:"final_active"`
}

// dialAdmission 使用真实 WebSocket 握手，HTTP 503 只有容量原因才计入容量拒绝。
func dialAdmission(ctx context.Context, url string, client *http.Client, index int) admissionAttempt {
	row := admissionAttempt{Index: index, Outcome: "other_error"}
	began := time.Now()
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: client})
	row.ElapsedMS = baselineMS(time.Since(began))
	if resp != nil {
		row.Status = resp.StatusCode
		if err != nil && resp.Body != nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			row.Body = string(body)
			if readErr != nil {
				row.Error = readErr.Error()
			}
		}
	}
	if err != nil {
		row.Error += err.Error()
		if row.Status == http.StatusServiceUnavailable && strings.TrimSpace(row.Body) == "session limit exceeded" {
			row.Outcome = "capacity_rejected"
		}
		return row
	}
	row.Outcome, row.conn = "accepted", conn
	return row
}

// finishAdmission 在已建立连接上完成 start/audio/end，并验证唯一 final 和正常关闭。
func finishAdmission(ctx context.Context, conn *websocket.Conn) error {
	for _, msg := range []struct {
		kind websocket.MessageType
		data []byte
	}{
		{websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)},
		{websocket.MessageBinary, make([]byte, audio.ChunkBytesDefault)},
		{websocket.MessageText, []byte(`{"type":"end"}`)},
	} {
		if err := conn.Write(ctx, msg.kind, msg.data); err != nil {
			return err
		}
	}
	r := receiveBaseline(ctx, conn)
	if r.err != nil {
		return r.err
	}
	if r.finals != 1 || r.partials != 0 {
		return fmt.Errorf("finals=%d partials=%d", r.finals, r.partials)
	}
	return nil
}

// TestAdmissionRealConnections 保留一轮快速真实链路回归。
func TestAdmissionRealConnections(t *testing.T) { runAdmissionRounds(t, 1) }

// TestSessionAdmissionExperiment 显式启用五轮量化验收，共 100 次突发接入加 5 次重新接入。
func TestSessionAdmissionExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_SESSION_ADMISSION_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_SESSION_ADMISSION_EXPERIMENT=1")
	}
	runAdmissionRounds(t, 5)
}

func runAdmissionRounds(t *testing.T, rounds int) {
	t.Helper()
	const limit, attempts = 4, 20
	w := &admissionWorker{worker: mockasr.New(mockasr.Config{FinalText: "final"}), exits: make(chan baselineWorkerExit, rounds*(limit+1))}
	workerClient := newBaselineTCPWorkerClient(t, w)
	appCtx, stop := context.WithCancel(context.Background())
	g, err := New(appCtx, workerClient, Config{MaxSessions: limit})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	handlers := make(chan struct{}, rounds*(attempts+1)+1)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer func() { handlers <- struct{}{} }()
		g.ServeHTTP(rw, r)
	}))
	transport := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: transport}
	var conns []*websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		stop()
		for _, conn := range conns {
			_ = conn.CloseNow()
		}
		transport.CloseIdleConnections()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := g.Wait(ctx); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	waitHandlers := func(n int) {
		for range n {
			select {
			case <-handlers:
			case <-ctx.Done():
				t.Fatal("handlers did not finish")
			}
		}
	}
	waitWorker := func() {
		select {
		case exit := <-w.exits:
			if exit.err != nil || exit.chunks != 1 || exit.bytes != audio.ChunkBytesDefault {
				t.Fatalf("worker exit: %+v", exit)
			}
		case <-ctx.Done():
			t.Fatal("worker did not finish")
		}
	}
	for round := 1; round <= rounds; round++ {
		// 每轮记录用独立闭包保证失败时也输出，成功样本不依赖服务取消。
		func() {
			r := admissionRound{Round: round, Outcome: "incomplete", ActiveAfterCleanup: -1, FinalActive: -1}
			defer func() {
				b, err := json.Marshal(r)
				if err != nil {
					t.Error(err)
					return
				}
				t.Logf("ADMISSION_JSON %s", b)
			}()
			before := w.started.Load()
			start := make(chan struct{})
			results := make(chan admissionAttempt, attempts)
			var wg sync.WaitGroup
			for i := range attempts {
				wg.Add(1)
				go func() { defer wg.Done(); <-start; results <- dialAdmission(ctx, url, client, i) }()
			}
			close(start)
			var accepted []*websocket.Conn
			for range attempts {
				row := <-results // Dial 使用有期限的 ctx；所有发送者均会汇报。
				r.Attempts = append(r.Attempts, row)
				r.MaxObservedActive = max(r.MaxObservedActive, admissionActive(g.tracker))
				switch row.Outcome {
				case "accepted":
					r.Accepted++
					accepted = append(accepted, row.conn)
					conns = append(conns, row.conn)
				case "capacity_rejected":
					r.Rejected++
				default:
					r.OtherErrors++
				}
			}
			wg.Wait()
			r.HeldActive, r.WorkerStartsBeforeStart = admissionActive(g.tracker), w.started.Load()-before
			if r.Accepted != limit || r.Rejected != attempts-limit || r.OtherErrors != 0 || r.HeldActive != limit || r.MaxObservedActive > limit || r.WorkerStartsBeforeStart != 0 {
				t.Fatalf("admission counts: accepted=%d rejected=%d other=%d active=%d prestart RPCs=%d", r.Accepted, r.Rejected, r.OtherErrors, r.HeldActive, r.WorkerStartsBeforeStart)
			}
			for _, conn := range accepted {
				if err := finishAdmission(ctx, conn); err != nil {
					t.Fatal(err)
				}
				waitWorker()
				r.Completed++
			}
			waitHandlers(attempts)
			r.ActiveAfterCleanup = admissionActive(g.tracker)
			if r.ActiveAfterCleanup != 0 {
				t.Fatalf("active after batch=%d", r.ActiveAfterCleanup)
			}
			reentry := dialAdmission(ctx, url, client, attempts)
			r.Reentry = &reentry
			if reentry.conn != nil {
				conns = append(conns, reentry.conn)
			}
			if reentry.Outcome != "accepted" {
				t.Fatalf("reentry: %+v", reentry)
			}
			if err := finishAdmission(ctx, reentry.conn); err != nil {
				t.Fatal(err)
			}
			waitWorker()
			waitHandlers(1)
			r.ReentryCompleted = true
			r.FinalActive = admissionActive(g.tracker)
			if r.FinalActive != 0 {
				t.Fatalf("active after reentry=%d", r.FinalActive)
			}
			r.Outcome = "completed"
		}()
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if appCtx.Err() != nil {
		t.Fatal("success depended on service cancellation")
	}
}

// TestAdmissionTailKeepsSlot 让 Worker 等待测试放行尾部，验证 end 不会提前归还名额。
func TestAdmissionTailKeepsSlot(t *testing.T) {
	w := &normalEndWorker{inputEnded: make(chan struct{}), releaseTail: make(chan struct{}), finished: make(chan error, 1)}
	client := newBaselineTCPWorkerClient(t, w)
	appCtx, stop := context.WithCancel(context.Background())
	g, err := New(appCtx, client, Config{MaxSessions: 1})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	done := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer func() { done <- struct{}{} }()
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
	conn, _, err = websocket.Dial(ctx, url, nil)
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
		t.Fatal("worker did not see end")
	}
	row := dialAdmission(ctx, url, http.DefaultClient, 1)
	if row.conn != nil {
		_ = row.conn.CloseNow()
	}
	if row.Outcome != "capacity_rejected" || admissionActive(g.tracker) != 1 {
		t.Fatalf("tail released slot: outcome=%s active=%d", row.Outcome, admissionActive(g.tracker))
	}
	close(w.releaseTail)
	for range 2 {
		if _, _, err = conn.Read(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err = conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-w.finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("worker did not exit")
	}
	for range 2 {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("handler did not exit")
		}
	}
	if admissionActive(g.tracker) != 0 {
		t.Fatal("slot not returned after tail cleanup")
	}
}
