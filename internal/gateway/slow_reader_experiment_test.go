package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// slowReaderWorker 至多发送 64 条、每条 256KiB 文本，再等待取消。
// 这是有总量上限的加速故障负载，不模拟真实临床转录输出频率。
type slowReaderWorker struct {
	asrv1.UnimplementedASRServiceServer
	sends stallIOProbe
	exit  chan tailWorkerExit
}

func (w *slowReaderWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) (err error) {
	defer func() { w.exit <- tailWorkerExit{time.Now(), err} }()
	if _, err = s.Recv(); err != nil {
		return err
	}
	text := strings.Repeat("x", 256*1024)
	for range 64 {
		w.sends.begin()
		err = s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "1", Text: text})
		w.sends.end(err)
		if err != nil {
			return err
		}
	}
	<-s.Context().Done()
	return status.FromContextError(s.Context().Err()).Err()
}

// slowReaderConn 只记录真实 net.Conn.Write 的边界，不制造延迟、不调整 TCP 缓冲。
// 一个 WebSocket Write 可能对应多次底层 Write，计数不能当作结果条数。
type slowReaderConn struct {
	net.Conn
	probe   *stallIOProbe
	enabled *atomic.Bool
}

func (c *slowReaderConn) Write(p []byte) (int, error) {
	if !c.enabled.Load() {
		return c.Conn.Write(p)
	}
	c.probe.begin()
	n, err := c.Conn.Write(p)
	c.probe.end(err)
	return n, err
}

type slowReaderListener struct {
	net.Listener
	probe   *stallIOProbe
	enabled *atomic.Bool
}

func (l *slowReaderListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &slowReaderConn{Conn: c, probe: l.probe, enabled: l.enabled}, nil
}

// slowReaderReport 区分发现阻塞时与追加观察后，失败也保留报告。
type slowReaderReport struct {
	Outcome                string          `json:"outcome"`
	AtDetection            stallIOSnapshot `json:"at_detection"`
	AfterObservation       stallIOSnapshot `json:"after_observation"`
	WorkerSends            stallIOSnapshot `json:"worker_sends"`
	ActiveAfterObservation int             `json:"active_after_observation"`
	FallbackCancel         bool            `json:"fallback_cancel"`
	CancelToWorkerMS       *float64        `json:"cancel_to_worker_ms"`
	CancelToHandlerMS      *float64        `json:"cancel_to_handler_ms"`
	WorkerError            string          `json:"worker_error,omitempty"`
	FinalActive            int             `json:"final_active"`
}

// TestSlowReaderBaselineExperiment 显式运行：不读客户端使真实输出缓冲耗尽。
// 连续 300ms 的同一次 TCP Write 未返回后，再观察 500ms；默认日常回归跳过。
func TestSlowReaderBaselineExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_SLOW_READER_BASELINE") != "1" {
		t.Skip("set TIDE_RUN_SLOW_READER_BASELINE=1")
	}
	r := slowReaderReport{Outcome: "incomplete", ActiveAfterObservation: -1, FinalActive: -1}
	defer func() {
		b, err := json.Marshal(r)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("SLOW_READER_JSON %s", b)
	}()
	w := &slowReaderWorker{exit: make(chan tailWorkerExit, 1)}
	workerClient := newBaselineTCPWorkerClient(t, w)
	appCtx, stop := context.WithCancel(context.Background())
	g, err := New(appCtx, workerClient, Config{MaxSessions: 1, InputIdleTimeout: 30 * time.Second})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	handlerDone := make(chan time.Time, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		defer func() { handlerDone <- time.Now() }()
		g.ServeHTTP(rw, req)
	}))
	probe := &stallIOProbe{}
	enabled := &atomic.Bool{}
	server.Listener = &slowReaderListener{Listener: server.Listener, probe: probe, enabled: enabled}
	server.Start()
	var conn *websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		stop()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	origin := time.Now()
	enabled.Store(true) // 已完成 HTTP 握手；后续只记录服务器 WebSocket 底层写入。
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	if err = conn.Write(ctx, websocket.MessageBinary, []byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	// 客户端从这里开始不调用 Read，也不发送 end；输入期限足以覆盖观察与兜底。
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	discovery := time.NewTimer(4 * time.Second)
	defer discovery.Stop()
detect:
	for {
		select {
		case <-ticker.C:
			snapshot := probe.snapshot(origin)
			if snapshot.Started > snapshot.Returned && snapshot.PendingMS >= 300 {
				r.AtDetection = snapshot
				break detect
			}
		case <-discovery.C:
			r.AtDetection = probe.snapshot(origin)
			r.Outcome = "blocking_not_reproduced"
			t.Fatal("no persistent TCP Write observed within 4s")
		case at := <-handlerDone:
			t.Fatalf("handler exited during discovery at %v", at.Sub(origin))
		case exit := <-w.exit:
			t.Fatalf("worker exited during discovery: %v", exit.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	observe := time.NewTimer(500 * time.Millisecond)
	defer observe.Stop()
	select {
	case <-observe.C:
	case <-handlerDone:
		t.Fatal("handler exited during observation")
	case exit := <-w.exit:
		t.Fatalf("worker exited during observation: %v", exit.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.AfterObservation = probe.snapshot(origin)
	r.WorkerSends = w.sends.snapshot(origin)
	r.ActiveAfterObservation = admissionActive(g.tracker)
	if r.AfterObservation.Started != r.AtDetection.Started || r.AfterObservation.Returned != r.AtDetection.Returned || r.AfterObservation.PendingMS < 800 || r.ActiveAfterObservation != 1 {
		t.Fatalf("blocking changed: before=%+v after=%+v active=%d", r.AtDetection, r.AfterObservation, r.ActiveAfterObservation)
	}
	r.FallbackCancel = true
	canceledAt := time.Now()
	stop()
	ms := func(d time.Duration) *float64 { v := baselineMS(d); return &v }
	select {
	case exit := <-w.exit:
		r.CancelToWorkerMS = ms(exit.at.Sub(canceledAt))
		if exit.err == nil {
			t.Fatal("stalled worker exited without error")
		}
		r.WorkerError = exit.err.Error()
	case <-ctx.Done():
		t.Fatal("worker did not cancel")
	}
	select {
	case at := <-handlerDone:
		r.CancelToHandlerMS = ms(at.Sub(canceledAt))
	case <-ctx.Done():
		t.Fatal("handler did not exit after external cancellation")
	}
	r.FinalActive = admissionActive(g.tracker)
	if r.FinalActive != 0 {
		t.Fatal(fmt.Sprintf("final active=%d", r.FinalActive))
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	r.Outcome = "required_external_cancel"
}
