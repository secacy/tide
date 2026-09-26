package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const protectionTimeout = 75 * time.Millisecond

var protectionModes = []string{"normal", "backlog", "send_timeout", "tail_timeout", "write_timeout"}

// protectionExit 由 Worker 退出时交接，防止测试并发读取 Worker 局部计数。
type protectionExit struct {
	mode   string
	chunks int
	err    error
}

// protectionWorker 在真实 TCP gRPC 上运行；故障模式由测试客户端 metadata 传入。
// 每次至多处理两块输入，不保存音频历史或每轮增长的结果缓存。
type protectionWorker struct {
	asrv1.UnimplementedASRServiceServer
	active atomic.Int64
	exits  chan protectionExit
}

func (w *protectionWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) (err error) {
	md, _ := metadata.FromIncomingContext(s.Context())
	modes := md.Get("cleanup-mode")
	if len(modes) != 1 {
		return status.Error(codes.InvalidArgument, "missing test mode")
	}
	mode := modes[0]
	chunks := 0
	w.active.Add(1)
	defer func() { w.active.Add(-1); w.exits <- protectionExit{mode, chunks, err} }()
	req, err := s.Recv()
	if err != nil {
		return err
	}
	chunks++
	if len(req.Data) != 3200 {
		return status.Error(codes.InvalidArgument, "unexpected test audio length")
	}
	if mode != "backlog" {
		if err := s.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 3200}}); err != nil {
			return err
		}
	}
	if err := s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "1", Text: "ready"}); err != nil {
		return err
	}
	_, err = s.Recv()
	if err == nil {
		chunks++
		return status.Error(codes.InvalidArgument, "unexpected second audio at Worker")
	}
	if mode == "tail_timeout" && errors.Is(err, io.EOF) {
		<-s.Context().Done()
		return status.FromContextError(s.Context().Err()).Err()
	}
	if mode == "normal" && errors.Is(err, io.EOF) {
		return s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "1", Text: "final", IsFinal: true})
	}
	return err
}

// protectionClient 只在发送超时模式阻塞 API Send，底层仍是真实 gRPC stream。
// 此门闩验证取消/计时器的反复清理，不模拟 TCP 流控阈值或性能。
type protectionClient struct {
	asrv1.ASRServiceClient
	mode        atomic.Int64
	activeSends atomic.Int64
}

func (c *protectionClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	mode := protectionModes[c.mode.Load()]
	ctx = metadata.AppendToOutgoingContext(ctx, "cleanup-mode", mode)
	s, err := c.ASRServiceClient.StreamingRecognize(ctx, opts...)
	if err != nil {
		return nil, err
	}
	if mode != "send_timeout" {
		return s, nil
	}
	return &protectionSendStream{BidiStreamingClient: s, ctx: ctx, active: &c.activeSends}, nil
}

type protectionSendStream struct {
	grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	ctx    context.Context
	active *atomic.Int64
}

func (s *protectionSendStream) Send(*asrv1.StreamingRecognizeRequest) error {
	s.active.Add(1)
	defer s.active.Add(-1)
	<-s.ctx.Done()
	return io.EOF
}

// protectionListener 为每次连接创建独立写门闩。Dial 返回后再启用，握手不受影响。
// 外层 cleanupListener 统计底层 TCP Close，避免仅凭 handler 返回推断关闭。
type protectionListener struct {
	net.Listener
	gates chan *resultWriteGate
}

func (l *protectionListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	gate := &resultWriteGate{started: make(chan struct{}), closed: make(chan struct{})}
	l.gates <- gate
	return &resultWriteGateConn{Conn: c, gate: gate}, nil
}

// protectionResourceSample 在每轮所有退出通知到齐、客户端关闭并 GC 后采集。
// 堆与 goroutine 包含客户端/Worker/测试及运行时，不能当作纯网关单会话资源。
type protectionResourceSample struct {
	Round          int    `json:"round"`
	Settled        bool   `json:"settled"`
	Goroutines     int    `json:"goroutines"`
	HeapAlloc      uint64 `json:"heap_alloc"`
	HeapObjects    uint64 `json:"heap_objects"`
	HeapInuse      uint64 `json:"heap_inuse"`
	ActiveSessions int    `json:"active_sessions"`
	OpenWS         int64  `json:"open_ws"`
	OpenGRPC       int64  `json:"open_grpc"`
	ActiveWorkers  int64  `json:"active_workers"`
	ActiveSends    int64  `json:"active_sends"`
}
type protectionCleanupReport struct {
	Group            string                     `json:"group"`
	Rounds           int                        `json:"rounds"`
	WarmupRounds     int                        `json:"warmup_rounds"`
	SessionsPerRound int                        `json:"sessions_per_round"`
	Validated        bool                       `json:"validated"`
	Outcomes         map[string]int             `json:"outcomes"`
	Samples          []protectionResourceSample `json:"samples"`
}

// TestProtectionCleanupProbe 覆盖两组各两轮，避免日常回归运行正式长轮次。
func TestProtectionCleanupProbe(t *testing.T) { runProtectionCleanup(t, 2) }

// TestProtectionCleanupExperiment 每组两轮预热、二十轮测量，每轮五个串行会话。
// go test -count=3 重复整套实验；组内及组间共用同一 Gateway 和 gRPC ClientConn。
func TestProtectionCleanupExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_PROTECTION_CLEANUP") != "1" {
		t.Skip("set TIDE_RUN_PROTECTION_CLEANUP=1")
	}
	runProtectionCleanup(t, 20)
}

func runProtectionCleanup(t *testing.T, rounds int) {
	t.Helper()
	worker := &protectionWorker{exits: make(chan protectionExit, 1)}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcListener := &cleanupListener{Listener: raw}
	grpcServer := grpc.NewServer()
	asrv1.RegisterASRServiceServer(grpcServer, worker)
	grpcDone := make(chan struct{})
	go func() { defer close(grpcDone); _ = grpcServer.Serve(grpcListener) }()
	conn, err := grpc.NewClient(raw.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Stop()
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = raw.Close()
		select {
		case <-grpcDone:
		case <-time.After(time.Second):
			t.Error("gRPC server cleanup timeout")
		}
		if open := grpcListener.open.Load(); open != 0 {
			t.Errorf("gRPC connections remain after fixture shutdown: %d", open)
		}
	}()
	client := &protectionClient{ASRServiceClient: asrv1.NewASRServiceClient(conn)}
	appCtx, stop := context.WithCancel(context.Background())
	defer stop()
	g, err := New(appCtx, client, Config{MaxSessions: 1, MaxPendingAudioBytes: 3200, WorkerSendTimeout: protectionTimeout, TailTimeout: protectionTimeout, ResultWriteTimeout: protectionTimeout})
	if err != nil {
		t.Fatal(err)
	}
	handlers := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { handlers <- struct{}{} }()
		g.ServeHTTP(w, r)
	}))
	wsListener := &cleanupListener{Listener: server.Listener}
	gateListener := &protectionListener{Listener: wsListener, gates: make(chan *resultWriteGate, 1)}
	server.Listener = gateListener
	server.Start()
	defer func() { stop(); g.StopAccepting(); server.Close() }()
	accepted := int64(0)
	sample := func(round int, settled bool) protectionResourceSample {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return protectionResourceSample{round, settled, runtime.NumGoroutine(), m.HeapAlloc, m.HeapObjects, m.HeapInuse, admissionActive(g.tracker), wsListener.open.Load(), grpcListener.open.Load(), worker.active.Load(), client.activeSends.Load()}
	}
	for _, group := range []string{"normal_control", "mixed_faults"} {
		report := protectionCleanupReport{Group: group, Rounds: rounds, WarmupRounds: 2, SessionsPerRound: 5, Outcomes: make(map[string]int), Samples: make([]protectionResourceSample, 0, rounds+2)}
		// 在基线前填好键，避免首次测量时才扩充报告对象。
		report.Outcomes["normal"] = 0
		if group == "mixed_faults" {
			for _, mode := range protectionModes {
				report.Outcomes[mode] = 0
			}
		}
		// 独立函数保证失败也输出已收集数据，且不保留每条会话对象影响堆趋势。
		func() {
			defer func() {
				b, err := json.Marshal(report)
				if err != nil {
					t.Error(err)
					return
				}
				t.Logf("PROTECTION_CLEANUP_JSON %s", b)
			}()
			for round := -1; round <= rounds; round++ {
				for i := 0; i < 5; i++ {
					mode := 0
					if group == "mixed_faults" {
						mode = i
					}
					client.mode.Store(int64(mode))
					runProtectionCleanupSession(t, server.URL, protectionModes[mode], gateListener.gates, handlers, worker.exits)
					accepted++
					assertCleanupCounts(t, g, wsListener, accepted)
					if worker.active.Load() != 0 || client.activeSends.Load() != 0 || appCtx.Err() != nil {
						t.Fatal("operations remain or application was canceled")
					}
					if round > 0 {
						report.Outcomes[protectionModes[mode]]++
					}
				}
				// -1/0 为两轮预热；第 0 轮结束后建立资源基线。
				if round >= 0 {
					r := sample(round, false)
					if r.OpenGRPC != 1 {
						t.Fatalf("persistent gRPC transport count=%d, want 1", r.OpenGRPC)
					}
					report.Samples = append(report.Samples, r)
				}
			}
			// 清理断言已由通知和计数完成；250ms 只用于另记运行时后台任务沉降后样本。
			timer := time.NewTimer(250 * time.Millisecond)
			<-timer.C
			final := sample(rounds, true)
			report.Samples = append(report.Samples, final)
			baseline := report.Samples[0].Goroutines
			if final.Goroutines > baseline+8 {
				var stacks bytes.Buffer
				_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
				t.Fatalf("goroutines failed to settle: baseline=%d final=%d\n%s", baseline, final.Goroutines, stacks.String())
			}
			if final.ActiveSessions != 0 || final.OpenWS != 0 || final.ActiveWorkers != 0 || final.ActiveSends != 0 || final.OpenGRPC != 1 {
				t.Fatalf("unexpected final resources: %+v", final)
			}
			report.Validated = true
		}()
	}
	g.StopAccepting()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

// runProtectionCleanupSession 返回前确认退出和原因；失败清理使用 defer，
// 成功断言之前不主动关闭客户端或取消应用，避免辅助清理掩盖业务泄漏。
func runProtectionCleanupSession(t *testing.T, url, mode string, gates <-chan *resultWriteGate, handlers <-chan struct{}, exits <-chan protectionExit) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(url, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	var gate *resultWriteGate
	select {
	case gate = <-gates:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if mode == "write_timeout" {
		gate.enabled.Store(true)
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	if mode != "send_timeout" && mode != "write_timeout" {
		assertRecognitionResult(t, ctx, conn, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: "ready"})
		if mode == "backlog" {
			if err := conn.Write(ctx, websocket.MessageBinary, []byte{0, 0}); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if mode == "normal" {
		assertRecognitionResult(t, ctx, conn, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: "final", IsFinal: true})
	}
	if mode == "write_timeout" {
		select {
		case <-gate.started:
		case <-ctx.Done():
			t.Fatal("result Write was not blocked")
		}
	}
	_, _, readErr := conn.Read(ctx)
	if ctx.Err() != nil {
		t.Fatalf("%s required experiment fallback: %v", mode, readErr)
	}
	var closeErr websocket.CloseError
	if mode == "write_timeout" {
		if readErr == nil || websocket.CloseStatus(readErr) == websocket.StatusNormalClosure {
			t.Fatalf("blocked write did not close: %v", readErr)
		}
	} else {
		code, reason := websocket.StatusInternalError, map[string]string{"backlog": "audio backlog exceeded", "send_timeout": "worker send timeout", "tail_timeout": "tail timeout"}[mode]
		if mode == "normal" {
			code, reason = websocket.StatusNormalClosure, "completed"
		}
		if !errors.As(readErr, &closeErr) || closeErr.Code != code || closeErr.Reason != reason {
			t.Fatalf("%s close=%v, want %v %q", mode, readErr, code, reason)
		}
	}
	select {
	case exit := <-exits:
		chunks := 1
		if mode == "send_timeout" {
			chunks = 0
		}
		if exit.mode != mode || exit.chunks != chunks {
			t.Fatalf("unexpected Worker exit: %+v", exit)
		}
		if mode == "normal" {
			if exit.err != nil {
				t.Fatal(exit.err)
			}
		} else if status.Code(exit.err) != codes.Canceled {
			t.Fatalf("%s Worker error=%v", mode, exit.err)
		}
	case <-ctx.Done():
		t.Fatalf("%s Worker did not exit", mode)
	}
	select {
	case <-handlers:
	case <-ctx.Done():
		t.Fatalf("%s handler did not exit", mode)
	}
}
