package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// baselineWrite 保存相对首块计划时刻的毫秒数；失败写入也保留记录。
type baselineWrite struct {
	Index      int     `json:"index"`
	PlannedMS  float64 `json:"planned_ms"`
	StartedMS  float64 `json:"started_ms"`
	ReturnedMS float64 `json:"returned_ms"`
	Error      string  `json:"error,omitempty"`
}

// baselineReport 同时保存成功与失败实验；零值时间在没有对应事件时不代表成功。
// Outcome 和 Error 必须与指标一起读取；Writes 可用于重新计算写入耗时与发送落后量。
type baselineReport struct {
	Case          string          `json:"case"`
	Outcome       string          `json:"outcome"`
	Error         string          `json:"error,omitempty"`
	ProcessingMS  float64         `json:"processing_ms"`
	AudioSendMS   float64         `json:"audio_send_ms"`
	MaxWriteMS    float64         `json:"max_write_ms"`
	MaxLagMS      float64         `json:"max_lag_ms"`
	EndStartedMS  float64         `json:"end_started_ms"`
	EndReturnedMS float64         `json:"end_returned_ms"`
	FinalMS       float64         `json:"final_ms"`
	TailMS        float64         `json:"tail_ms"`
	CloseMS       float64         `json:"close_ms"`
	WorkerChunks  int             `json:"worker_chunks"`
	WorkerBytes   int             `json:"worker_bytes"`
	Writes        []baselineWrite `json:"writes"`
}

// baselineWorkerExit 由识别方法退出时一次性发送，避免测量线程并发读取 Worker 计数。
type baselineWorkerExit struct {
	chunks, bytes int
	err           error
}

// baselineWorker 在不修改 Mock 的前提下，验证它实际读取的每块音频内容与顺序。
type baselineWorker struct {
	asrv1.UnimplementedASRServiceServer
	worker *mockasr.Worker
	done   chan baselineWorkerExit
}

func (w *baselineWorker) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	observed := &baselineStream{BidiStreamingServer: stream}
	err := w.worker.StreamingRecognize(observed)
	w.done <- baselineWorkerExit{chunks: observed.chunks, bytes: observed.bytes, err: err}
	return err
}

// baselineStream 只在 Worker 的串行 Recv 路径中使用。数据通过校验后才交给 Mock。
type baselineStream struct {
	grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	chunks, bytes int
}

func (s *baselineStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	req, err := s.BidiStreamingServer.Recv()
	if err != nil {
		return nil, err
	}
	want := bytes.Repeat([]byte{byte(s.chunks)}, audio.ChunkBytesDefault)
	if !bytes.Equal(req.GetData(), want) {
		return nil, fmt.Errorf("audio mismatch at chunk %d", s.chunks)
	}
	s.chunks++
	s.bytes += len(req.GetData())
	return req, nil
}

// baselineReception 仅由接收 goroutine 构建，在读取结束后交给实验主线程。
type baselineReception struct {
	finalAt, closedAt time.Time
	finals, partials  int
	err               error
}

// TestSlowWorkerBaselineExperiment 是显式启用的真实时间实验，不拖慢日常回归。
// 两种速度各运行一个单会话；用 go test -count=3 获得三组原始记录。
func TestSlowWorkerBaselineExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_SLOW_WORKER_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_SLOW_WORKER_EXPERIMENT=1 to run the real-time baseline")
	}
	t.Logf("environment go=%s os=%s arch=%s logical_cpu=%d GOMAXPROCS=%d transport=loopback-TCP same-process", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	for _, tc := range []struct {
		name  string
		delay time.Duration
	}{
		{"normal", 50 * time.Millisecond}, {"slow", 200 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) { runSlowWorkerBaseline(t, tc.name, tc.delay) })
	}
}

// runSlowWorkerBaseline 固定发送 100 块 PCM，并持续并发读取结果。
// 60 秒只限制实验本身；不会把实验超时解释成 Gateway 已实现处理超时。
func runSlowWorkerBaseline(t *testing.T, name string, delay time.Duration) {
	t.Helper()
	report := baselineReport{Case: name, Outcome: "incomplete", ProcessingMS: baselineMS(delay), Writes: make([]baselineWrite, 0, 100)}
	defer func() {
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Errorf("encode experiment report: %v", err)
			return
		}
		t.Logf("BASELINE_JSON %s", encoded)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	fail := func(err error) {
		report.Outcome = "failed"
		if ctx.Err() != nil {
			report.Outcome = "experiment_timeout"
		}
		report.Error = err.Error()
		t.Fatal(err)
	}
	worker := &baselineWorker{
		worker: mockasr.New(mockasr.Config{ProcessingDelay: delay, PartialEvery: 500 * time.Millisecond,
			ResponseDelay: 0, PartialTexts: []string{"p1", "p2", "p3"}, FinalText: "final"}),
		done: make(chan baselineWorkerExit, 1),
	}
	client := newBaselineTCPWorkerClient(t, worker)
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	g, err := New(sessionCtx, client, Config{})
	if err != nil {
		cancelSessions()
		fail(err)
	}
	server := httptest.NewServer(g)
	var conn *websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		cancelSessions()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
		waitCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := g.Wait(waitCtx); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	conn, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		fail(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		fail(err)
	}
	received := make(chan baselineReception, 1)
	go func() { received <- receiveBaseline(ctx, conn) }()
	// 确保失败路径也等待接收 goroutine 退出，避免测试工具自身残留。
	defer func() {
		cancel()
		if received != nil {
			select {
			case <-received:
			case <-time.After(5 * time.Second):
				t.Error("experiment receiver did not exit")
			}
		}
	}()
	start := time.Now()
	for i := 0; i < 100; i++ {
		chunk := bytes.Repeat([]byte{byte(i)}, audio.ChunkBytesDefault)
		planned := start.Add(time.Duration(i) * 100 * time.Millisecond)
		if wait := time.Until(planned); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				fail(ctx.Err())
			}
		}
		began := time.Now()
		err := conn.Write(ctx, websocket.MessageBinary, chunk)
		ended := time.Now()
		row := baselineWrite{Index: i, PlannedMS: baselineMS(planned.Sub(start)), StartedMS: baselineMS(began.Sub(start)), ReturnedMS: baselineMS(ended.Sub(start))}
		if err != nil {
			row.Error = err.Error()
		}
		report.Writes = append(report.Writes, row)
		report.MaxWriteMS = max(report.MaxWriteMS, baselineMS(ended.Sub(began)))
		report.MaxLagMS = max(report.MaxLagMS, baselineMS(began.Sub(planned)))
		if err != nil {
			fail(err)
		}
		report.AudioSendMS = row.ReturnedMS
	}
	report.EndStartedMS = baselineMS(time.Since(start))
	err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`))
	report.EndReturnedMS = baselineMS(time.Since(start))
	if err != nil {
		fail(err)
	}
	var result baselineReception
	select {
	case result = <-received:
		received = nil
	case <-ctx.Done():
		fail(ctx.Err())
	}
	if !result.finalAt.IsZero() {
		report.FinalMS = baselineMS(result.finalAt.Sub(start))
		report.TailMS = report.FinalMS - report.EndStartedMS
	}
	if !result.closedAt.IsZero() {
		report.CloseMS = baselineMS(result.closedAt.Sub(start))
	}
	if result.err != nil {
		fail(result.err)
	}
	if result.finals != 1 || result.partials != 3 {
		fail(fmt.Errorf("results: final=%d partial=%d", result.finals, result.partials))
	}
	select {
	case exit := <-worker.done:
		report.WorkerChunks, report.WorkerBytes = exit.chunks, exit.bytes
		if exit.err != nil {
			fail(exit.err)
		}
		if exit.chunks != 100 || exit.bytes != 100*audio.ChunkBytesDefault {
			fail(fmt.Errorf("worker received chunks=%d bytes=%d", exit.chunks, exit.bytes))
		}
	case <-ctx.Done():
		fail(ctx.Err())
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		fail(err)
	}
	report.Outcome = "completed"
}

// receiveBaseline 验证全部结果和正常关闭；final 到达时立即取时间，避免日志影响计时。
func receiveBaseline(ctx context.Context, conn *websocket.Conn) (result baselineReception) {
	for {
		kind, data, err := conn.Read(ctx)
		at := time.Now()
		if err != nil {
			result.closedAt = at
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				result.err = err
			}
			return result
		}
		var msg wsprotocol.ResultMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			result.err = err
			return result
		}
		if kind != websocket.MessageText || msg.Type != wsprotocol.MessageTypeResult || msg.SegmentID != "1" {
			result.err = fmt.Errorf("unexpected result: kind=%v message=%+v", kind, msg)
			return result
		}
		if msg.IsFinal {
			result.finalAt = at
			result.finals++
			if msg.Text != "final" {
				result.err = fmt.Errorf("unexpected final %q", msg.Text)
				return result
			}
		} else {
			result.partials++
			if result.finals != 0 || msg.Text != fmt.Sprintf("p%d", result.partials) {
				result.err = fmt.Errorf("unexpected partial %q", msg.Text)
				return result
			}
		}
	}
}

// baselineMS 保留毫秒小数，不提前截断短写入的测量值。
func baselineMS(duration time.Duration) float64 { return float64(duration) / float64(time.Millisecond) }

// newBaselineTCPWorkerClient 使用本机 TCP，避免把 bufconn 的固定缓冲行为当成真实网络现象。
// Client、Gateway、Worker 仍在同一进程，结果只代表本次本机实验环境。
func newBaselineTCPWorkerClient(t *testing.T, worker asrv1.ASRServiceServer) asrv1.ASRServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	asrv1.RegisterASRServiceServer(server, worker)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("gRPC server did not stop")
		}
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return asrv1.NewASRServiceClient(conn)
}
