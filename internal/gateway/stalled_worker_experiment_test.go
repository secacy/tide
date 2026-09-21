package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stallIOSnapshot 是某一方向的调用计数和当前等待；成功返回不等于音频已被识别。
type stallIOSnapshot struct {
	Started        int     `json:"started"`
	Returned       int     `json:"returned"`
	Succeeded      int     `json:"succeeded"`
	PendingMS      float64 `json:"pending_ms"`
	PendingSinceMS float64 `json:"pending_since_ms"`
	LastReturnMS   float64 `json:"last_return_ms"`
	Error          string  `json:"error,omitempty"`
}

// stallIOProbe 只用短锁记录 I/O 边界，不在锁内执行网络调用。
// 每个探针对应一个串行发送者；snapshot 由实验协调者并发读取。
type stallIOProbe struct {
	mu                           sync.Mutex
	started, returned, succeeded int
	pending, lastReturn          time.Time
	err                          string
}

func (p *stallIOProbe) begin() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started++
	p.pending = time.Now()
}

func (p *stallIOProbe) end(err error) {
	at := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.returned++
	p.lastReturn = at
	p.pending = time.Time{}
	if err == nil {
		p.succeeded++
	} else {
		p.err = err.Error()
	}
}

func (p *stallIOProbe) snapshot(origin time.Time) stallIOSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := stallIOSnapshot{Started: p.started, Returned: p.returned, Succeeded: p.succeeded, Error: p.err}
	if !p.pending.IsZero() {
		s.PendingMS = baselineMS(time.Since(p.pending))
		s.PendingSinceMS = baselineMS(p.pending.Sub(origin))
	}
	if !p.lastReturn.IsZero() {
		s.LastReturnMS = baselineMS(p.lastReturn.Sub(origin))
	}
	return s
}

// stallClient 包装 Gateway 使用的真实 gRPC client，直接观察 Send 而不从 WebSocket 推测。
type stallClient struct {
	asrv1.ASRServiceClient
	probe         *stallIOProbe
	rpcCtx        context.Context // 仅在 rpcDone 关闭之后由实验线程读取。
	rpcDone       chan struct{}
	rpcCanceledAt time.Time // 回调观察到取消的时刻，不是精确的取消函数调用时刻。
}

func (c *stallClient) StreamingRecognize(ctx context.Context, options ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	c.rpcCtx = ctx
	context.AfterFunc(ctx, func() { c.rpcCanceledAt = time.Now(); close(c.rpcDone) })
	stream, err := c.ASRServiceClient.StreamingRecognize(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &stallClientStream{BidiStreamingClient: stream, probe: c.probe}, nil
}

type stallClientStream struct {
	grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	probe *stallIOProbe
}

func (s *stallClientStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	s.probe.begin()
	err := s.BidiStreamingClient.Send(req)
	s.probe.end(err)
	return err
}

// stallWorker 保存退出时刻与实际读取量。done 关闭之后才允许读取这些字段。
type stallWorker struct {
	asrv1.UnimplementedASRServiceServer
	worker *mockasr.Worker
	done   chan struct{}
	at     time.Time
	exit   baselineWorkerExit
}

func (w *stallWorker) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	s := &baselineStream{BidiStreamingServer: stream}
	err := w.worker.StreamingRecognize(s)
	w.at = time.Now()
	w.exit = baselineWorkerExit{chunks: s.chunks, bytes: s.bytes, err: err}
	close(w.done)
	return err
}

// stallReport 记录故障前、断开观察期末和取消后的独立结果。
// Fallback 表示实验主动清理，不能把此后的退出记成客户端断开自动恢复。
type stallReport struct {
	Case                         string           `json:"case"`
	Outcome                      string           `json:"outcome"`
	Error                        string           `json:"error,omitempty"`
	TriggerMS                    float64          `json:"trigger_ms"`
	BeforeSend                   stallIOSnapshot  `json:"before_send"`
	BeforeWrite                  stallIOSnapshot  `json:"before_write"`
	DisconnectObservationMS      float64          `json:"disconnect_observation_ms"`
	SendAfterDisconnect          *stallIOSnapshot `json:"send_after_disconnect,omitempty"`
	ActiveAfterDisconnect        int              `json:"active_after_disconnect"`
	WorkerExitedAfterDisconnect  bool             `json:"worker_exited_after_disconnect"`
	HandlerExitedAfterDisconnect bool             `json:"handler_exited_after_disconnect"`
	Fallback                     bool             `json:"fallback_cancel"`
	CancelMS                     float64          `json:"cancel_ms"`
	SendAfter                    stallIOSnapshot  `json:"send_after"`
	WriteAfter                   stallIOSnapshot  `json:"write_after"`
	WorkerExitMS                 float64          `json:"worker_exit_ms"`
	HandlerExitMS                float64          `json:"handler_exit_ms"`
	WorkerChunks                 int              `json:"worker_chunks"`
	WorkerBytes                  int              `json:"worker_bytes"`
	ClientCloseCode              int              `json:"client_close_code"`
	FinalActive                  int              `json:"final_active"`
	FinalOpen                    int64            `json:"final_open_connections"`
	WorkerSendTimeoutMS          float64          `json:"worker_send_timeout_ms"`
	RPCCancelObservedMS          float64          `json:"rpc_cancel_observed_ms"`
	RPCCause                     string           `json:"rpc_cause"`
}

// TestStalledWorkerExperiment 是有总输入与时间上限的诊断实验，默认跳过。
// “发现断开后未退出”是诊断结论，不能通过测试兜底取消掩盖它。
func TestStalledWorkerExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_STALLED_WORKER_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_STALLED_WORKER_EXPERIMENT=1 for the bounded stall experiment")
	}
	t.Logf("environment go=%s os=%s arch=%s logical_cpu=%d GOMAXPROCS=%d transport=loopback-TCP same-process", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	for _, mode := range []string{"service_cancel", "client_disconnect"} {
		t.Run(mode, func(t *testing.T) { runStalledWorker(t, mode, false) })
	}
}

// TestWorkerSendTimeoutExperiment 复用原停读负载，要求无需测试取消也能结束会话。
// timeout_connected 保持客户端连接；client_disconnect 在相同阻塞条件下先断开。
func TestWorkerSendTimeoutExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_WORKER_SEND_TIMEOUT_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_WORKER_SEND_TIMEOUT_EXPERIMENT=1 to verify automatic timeout cleanup")
	}
	t.Logf("environment go=%s os=%s arch=%s logical_cpu=%d GOMAXPROCS=%d transport=loopback-TCP same-process", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	for _, mode := range []string{"timeout_connected", "client_disconnect"} {
		t.Run(mode, func(t *testing.T) { runStalledWorker(t, mode, true) })
	}
}

// runStalledWorker 先确认 5 块双向传输，再加速发送直到两个方向都持续等待一秒。
// 故障观测最多 15 秒、总输入最多 5000 块；断开后的自主退出单独观察两秒。
func runStalledWorker(t *testing.T, mode string, requireTimeout bool) {
	t.Helper()
	origin := time.Now()
	report := stallReport{Case: mode, Outcome: "incomplete"}
	defer func() {
		data, err := json.Marshal(report)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("STALL_JSON %s", data)
	}()
	fail := func(err error) { report.Outcome = "failed"; report.Error = err.Error(); t.Fatal(err) }
	worker := &stallWorker{worker: mockasr.New(mockasr.Config{StallAfterChunks: 5, PartialEvery: 500 * time.Millisecond, PartialTexts: []string{"ready"}}), done: make(chan struct{})}
	sendProbe, writeProbe := &stallIOProbe{}, &stallIOProbe{}
	client := &stallClient{ASRServiceClient: newBaselineTCPWorkerClient(t, worker), probe: sendProbe, rpcDone: make(chan struct{})}
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	g, err := New(sessionCtx, client, Config{})
	if err != nil {
		cancelSessions()
		fail(err)
	}
	report.WorkerSendTimeoutMS = baselineMS(g.cfg.WorkerSendTimeout)
	handlerDone := make(chan struct{})
	var handlerAt time.Time // 由 handler 写入，关闭 handlerDone 后读取。
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { handlerAt = time.Now(); close(handlerDone) }()
		g.ServeHTTP(w, r)
	}))
	listener := &cleanupListener{Listener: server.Listener}
	server.Listener = listener
	server.Start()
	ctx, cancelClient := context.WithTimeout(context.Background(), 35*time.Second)
	var conn *websocket.Conn
	var senderDone, receiverDone chan struct{}
	t.Cleanup(func() {
		g.StopAccepting()
		cancelSessions()
		cancelClient()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
		cleanupCtx, stop := context.WithTimeout(context.Background(), 12*time.Second)
		defer stop()
		if err := g.Wait(cleanupCtx); err != nil {
			t.Errorf("cleanup wait: %v", err)
		}
		for _, done := range []chan struct{}{senderDone, receiverDone} {
			if done != nil {
				select {
				case <-done:
				case <-cleanupCtx.Done():
					t.Error("experiment goroutine did not exit")
				}
			}
		}
	})
	conn, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		fail(err)
	}
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		fail(err)
	}
	writeChunk := func(i int) error {
		data := bytes.Repeat([]byte{byte(i)}, audio.ChunkBytesDefault)
		writeProbe.begin()
		err := conn.Write(ctx, websocket.MessageBinary, data)
		writeProbe.end(err)
		return err
	}
	for i := 0; i < 5; i++ {
		if err := writeChunk(i); err != nil {
			fail(err)
		}
	}
	_, data, err := conn.Read(ctx)
	if err != nil {
		fail(err)
	}
	var ready wsprotocol.ResultMessage
	if err = json.Unmarshal(data, &ready); err != nil {
		fail(err)
	}
	if ready.Type != wsprotocol.MessageTypeResult || ready.Text != "ready" || ready.IsFinal {
		fail(fmt.Errorf("unexpected ready result: %+v", ready))
	}
	receiverDone = make(chan struct{})
	var readErr error // 只在 receiverDone 关闭后读取。
	go func() { defer close(receiverDone); _, _, readErr = conn.Read(ctx) }()
	senderDone = make(chan struct{})
	go func() {
		defer close(senderDone)
		for i := 5; i < 5000; i++ {
			if writeChunk(i) != nil {
				return
			}
		}
	}()
	observeUntil := time.Now().Add(15 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		report.BeforeSend = sendProbe.snapshot(origin)
		report.BeforeWrite = writeProbe.snapshot(origin)
		if report.BeforeSend.PendingMS >= 1000 && report.BeforeWrite.PendingMS >= 1000 {
			break
		}
		if time.Now().After(observeUntil) {
			report.Outcome = "not_reproduced_within_limits"
			if requireTimeout {
				fail(fmt.Errorf("required blocked-send precondition was not reproduced"))
			}
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			fail(ctx.Err())
		}
	}
	report.TriggerMS = baselineMS(time.Since(origin))
	if mode == "client_disconnect" {
		_ = conn.CloseNow()
		observationStart := time.Now()
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-handlerDone:
		case <-timer.C:
		}
		timer.Stop()
		report.DisconnectObservationMS = baselineMS(time.Since(observationStart))
		snapshot := sendProbe.snapshot(origin)
		report.SendAfterDisconnect = &snapshot
		report.ActiveAfterDisconnect = stallActive(g)
		report.WorkerExitedAfterDisconnect = stallClosed(worker.done)
		report.HandlerExitedAfterDisconnect = stallClosed(handlerDone)
		report.Fallback = !report.WorkerExitedAfterDisconnect || !report.HandlerExitedAfterDisconnect
		if sessionCtx.Err() != nil {
			fail(fmt.Errorf("service canceled before disconnect observation ended"))
		}
	}
	g.StopAccepting()
	if mode == "service_cancel" || report.Fallback {
		report.CancelMS = baselineMS(time.Since(origin))
		cancelSessions()
	}
	finishCtx, stop := context.WithTimeout(context.Background(), 12*time.Second)
	defer stop()
	for _, done := range []<-chan struct{}{worker.done, handlerDone, senderDone, receiverDone, client.rpcDone} {
		select {
		case <-done:
		case <-finishCtx.Done():
			fail(fmt.Errorf("exit not observed: %w", finishCtx.Err()))
		}
	}
	if err := g.Wait(finishCtx); err != nil {
		fail(err)
	}
	report.SendAfter = sendProbe.snapshot(origin)
	report.WriteAfter = writeProbe.snapshot(origin)
	report.WorkerExitMS = baselineMS(worker.at.Sub(origin))
	report.HandlerExitMS = baselineMS(handlerAt.Sub(origin))
	report.WorkerChunks, report.WorkerBytes = worker.exit.chunks, worker.exit.bytes
	report.ClientCloseCode = int(websocket.CloseStatus(readErr))
	report.FinalActive = stallActive(g)
	report.FinalOpen = listener.open.Load()
	report.RPCCancelObservedMS = baselineMS(client.rpcCanceledAt.Sub(origin))
	if cause := context.Cause(client.rpcCtx); cause != nil {
		report.RPCCause = cause.Error()
	}
	if worker.exit.chunks != 5 || worker.exit.bytes != 16000 || status.Code(worker.exit.err) != codes.Canceled {
		fail(fmt.Errorf("unexpected Worker exit: %+v", worker.exit))
	}
	if report.FinalActive != 0 || report.FinalOpen != 0 || report.SendAfter.Started != report.SendAfter.Returned {
		fail(fmt.Errorf("incomplete cleanup: %+v", report))
	}
	if readErr == nil {
		fail(fmt.Errorf("unexpected result after Worker should have stalled"))
	}
	if mode == "service_cancel" && websocket.CloseStatus(readErr) != websocket.StatusGoingAway {
		fail(fmt.Errorf("expected service close 1001, got %v", readErr))
	}
	if requireTimeout {
		if report.Fallback || report.CancelMS != 0 || sessionCtx.Err() != nil || !errors.Is(context.Cause(client.rpcCtx), ErrWorkerSendTimeout) {
			fail(fmt.Errorf("did not clean up through autonomous send timeout: fallback=%v cause=%v", report.Fallback, context.Cause(client.rpcCtx)))
		}
		if mode == "timeout_connected" {
			var closeErr websocket.CloseError
			if !errors.As(readErr, &closeErr) || closeErr.Code != websocket.StatusInternalError || closeErr.Reason != "worker send timeout" {
				fail(fmt.Errorf("unexpected timeout close: %v", readErr))
			}
		}
	}
	report.Outcome = "observed_and_cleaned"
}

// stallActive 读取包含清理阶段在内的活跃会话数。
func stallActive(g *Gateway) int {
	g.tracker.mu.Lock()
	defer g.tracker.mu.Unlock()
	return g.tracker.active
}

// stallClosed 非阻塞观察只关闭一次的完成信号，不消费事件。
func stallClosed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
