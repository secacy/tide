package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
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
)

// pausePartial 将可控 Mock 的第 k 条结果对应到第 5*k 块音频。
// WaitMS 包含发送落后、传输和处理等待，不是纯推理耗时。
type pausePartial struct {
	Index      int     `json:"index"`
	PlannedMS  float64 `json:"planned_ms"`
	ReceivedMS float64 `json:"received_ms"`
	WaitMS     float64 `json:"wait_ms"`
}

// pauseReport 保留逐块与逐结果记录；缺失的恢复/完成时刻使用 nil。
type pauseReport struct {
	Case                  string          `json:"case"`
	Outcome               string          `json:"outcome"`
	Error                 string          `json:"error,omitempty"`
	PauseMS               float64         `json:"pause_ms"`
	Writes                []baselineWrite `json:"writes"`
	Sends                 []baselineWrite `json:"sends"`
	Partials              []pausePartial  `json:"partials"`
	BoundaryMS            *float64        `json:"boundary_ms"`
	ResumeMS              *float64        `json:"resume_ms"`
	RecoveryConfirmedMS   *float64        `json:"recovery_confirmed_ms"`
	RecoveryAfterResumeMS *float64        `json:"recovery_after_resume_ms"`
	EndMS                 *float64        `json:"end_ms"`
	FinalMS               *float64        `json:"final_ms"`
	CloseMS               *float64        `json:"close_ms"`
	CloseCode             int             `json:"close_code"`
	WorkerChunks          int             `json:"worker_chunks"`
	WorkerBytes           int             `json:"worker_bytes"`
	MaxWaitMS             float64         `json:"max_wait_ms"`
	MaxPartialGapMS       float64         `json:"max_partial_gap_ms"`
}

// pauseSendProbe 记录真实 gRPC Send 的起止，锁不跨越网络调用。
type pauseSendProbe struct {
	mu     sync.Mutex
	origin time.Time
	rows   []baselineWrite
}

type pauseClient struct {
	asrv1.ASRServiceClient
	probe *pauseSendProbe
}

func (c *pauseClient) StreamingRecognize(ctx context.Context, options ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	s, err := c.ASRServiceClient.StreamingRecognize(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &pauseClientStream{BidiStreamingClient: s, probe: c.probe}, nil
}

type pauseClientStream struct {
	grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	probe *pauseSendProbe
}

func (s *pauseClientStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	began := time.Now()
	err := s.BidiStreamingClient.Send(req)
	ended := time.Now()
	p := s.probe
	p.mu.Lock()
	defer p.mu.Unlock()
	row := baselineWrite{Index: len(p.rows), PlannedMS: float64(len(p.rows)) * 100,
		StartedMS: baselineMS(began.Sub(p.origin)), ReturnedMS: baselineMS(ended.Sub(p.origin))}
	if err != nil {
		row.Error = err.Error()
	}
	p.rows = append(p.rows, row)
	return err
}

type pauseWorkerExit struct {
	baselineWorkerExit
	boundary, resume time.Time
}

type pauseWorker struct {
	asrv1.UnimplementedASRServiceServer
	worker *mockasr.Worker
	done   chan pauseWorkerExit
}

func (w *pauseWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	observed := &pauseServerStream{baselineStream: &baselineStream{BidiStreamingServer: s}}
	err := w.worker.StreamingRecognize(observed)
	w.done <- pauseWorkerExit{baselineWorkerExit{observed.chunks, observed.bytes, err}, observed.boundary, observed.resume}
	return err
}

// pauseServerStream 记录第 20 块对应 partial 发送返回、下一次 Recv 调用的时刻。
// 这是暂停两侧可观察的边界，包含少量函数/调度开销，不冒充内部计时器起止。
type pauseServerStream struct {
	*baselineStream
	boundary, resume time.Time
}

func (s *pauseServerStream) Send(resp *asrv1.StreamingRecognizeResponse) error {
	err := s.BidiStreamingServer.Send(resp)
	if err == nil && !resp.GetIsFinal() && resp.GetText() == "p4" {
		s.boundary = time.Now()
	}
	return err
}

func (s *pauseServerStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	if s.chunks == 20 && s.resume.IsZero() {
		s.resume = time.Now()
	}
	return s.baselineStream.Recv()
}

type pauseReception struct {
	rows              []pausePartial
	final, closed     time.Time
	finals, closeCode int
	err               error
}

// receivePause 只在结果读循环内写记录，通过 channel 交给实验主线程。
func receivePause(ctx context.Context, conn *websocket.Conn, origin time.Time) (r pauseReception) {
	for {
		kind, data, err := conn.Read(ctx)
		at := time.Now()
		if err != nil {
			r.closed, r.closeCode = at, int(websocket.CloseStatus(err))
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				r.err = err
			}
			return r
		}
		var msg wsprotocol.ResultMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			r.err = err
			return r
		}
		if kind != websocket.MessageText || msg.Type != wsprotocol.MessageTypeResult || msg.SegmentID != "1" {
			r.err = fmt.Errorf("unexpected result: %+v", msg)
			return r
		}
		if msg.IsFinal {
			r.final, r.finals = at, r.finals+1
			if msg.Text != "final" {
				r.err = fmt.Errorf("unexpected final %q", msg.Text)
				return r
			}
		} else {
			k := len(r.rows) + 1
			if r.finals != 0 || msg.Text != fmt.Sprintf("p%d", k) {
				r.err = fmt.Errorf("unexpected partial %q", msg.Text)
				return r
			}
			planned, received := float64(5*k-1)*100, baselineMS(at.Sub(origin))
			r.rows = append(r.rows, pausePartial{k, planned, received, received - planned})
		}
	}
}

// TestWorkerPauseExperiment 使用本机 TCP 和墙上时间，日常回归默认跳过。
func TestWorkerPauseExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_WORKER_PAUSE_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_WORKER_PAUSE_EXPERIMENT=1")
	}
	for _, tc := range []struct {
		name  string
		pause time.Duration
	}{
		{"no_pause", 0}, {"pause_500ms", 500 * time.Millisecond}, {"pause_3s", 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) { runWorkerPause(t, tc.name, tc.pause) })
	}
}

func runWorkerPause(t *testing.T, name string, pause time.Duration) {
	t.Helper()
	report := pauseReport{Case: name, Outcome: "incomplete", PauseMS: baselineMS(pause)}
	defer func() {
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Errorf("encode pause report: %v", err)
			return
		}
		t.Logf("PAUSE_JSON %s", encoded)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	fail := func(err error) {
		report.Outcome, report.Error = "failed", err.Error()
		if ctx.Err() != nil {
			report.Outcome = "experiment_timeout"
		}
		t.Fatal(err)
	}
	texts := make([]string, 20)
	for i := range texts {
		texts[i] = fmt.Sprintf("p%d", i+1)
	}
	w := &pauseWorker{worker: mockasr.New(mockasr.Config{ProcessingDelay: 50 * time.Millisecond,
		PartialEvery: 500 * time.Millisecond, PartialTexts: texts, FinalText: "final",
		PauseAfterChunks: 20, PauseDuration: pause}), done: make(chan pauseWorkerExit, 1)}
	probe := &pauseSendProbe{}
	client := &pauseClient{ASRServiceClient: newBaselineTCPWorkerClient(t, w), probe: probe}
	appCtx, stop := context.WithCancel(context.Background())
	g, err := New(appCtx, client, Config{WorkerSendTimeout: 2 * time.Second})
	if err != nil {
		stop()
		fail(err)
	}
	server := httptest.NewServer(g)
	var conn *websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		stop()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
		waitCtx, end := context.WithTimeout(context.Background(), 5*time.Second)
		defer end()
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
	origin := time.Now()
	probe.mu.Lock()
	probe.origin = origin // 此时尚未发送音频，第一条 Write 前发布。
	probe.mu.Unlock()
	received := make(chan pauseReception, 1)
	go func() { received <- receivePause(ctx, conn, origin) }()
	var reception pauseReception
	joined := false
	// 失败路径也取消并回收读取者，保留已观测的结果与 Send 样本。
	defer func() {
		cancel()
		if !joined {
			select {
			case reception = <-received:
			case <-time.After(5 * time.Second):
				t.Error("receiver did not exit")
			}
		}
		report.Partials = reception.rows
		report.CloseCode = reception.closeCode
		probe.mu.Lock()
		report.Sends = append([]baselineWrite(nil), probe.rows...)
		probe.mu.Unlock()
	}()
	ms := func(at time.Time) *float64 {
		if at.IsZero() {
			return nil
		}
		v := baselineMS(at.Sub(origin))
		return &v
	}
	for i := 0; i < 100; i++ {
		planned := origin.Add(time.Duration(i) * 100 * time.Millisecond)
		if d := time.Until(planned); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				fail(ctx.Err())
			}
		}
		began := time.Now()
		err := conn.Write(ctx, websocket.MessageBinary, bytes.Repeat([]byte{byte(i)}, audio.ChunkBytesDefault))
		row := baselineWrite{Index: i, PlannedMS: float64(i) * 100, StartedMS: baselineMS(began.Sub(origin)), ReturnedMS: baselineMS(time.Since(origin))}
		if err != nil {
			row.Error = err.Error()
		}
		report.Writes = append(report.Writes, row)
		if err != nil {
			fail(err)
		}
	}
	report.EndMS = ms(time.Now())
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
		fail(err)
	}
	select {
	case reception = <-received:
		joined = true
	case <-ctx.Done():
		fail(ctx.Err())
	}
	report.FinalMS, report.CloseMS = ms(reception.final), ms(reception.closed)
	if reception.err != nil {
		fail(reception.err)
	}
	if reception.finals != 1 || len(reception.rows) != 20 {
		fail(fmt.Errorf("finals=%d partials=%d", reception.finals, len(reception.rows)))
	}
	select {
	case exit := <-w.done:
		report.WorkerChunks, report.WorkerBytes = exit.chunks, exit.bytes
		report.BoundaryMS, report.ResumeMS = ms(exit.boundary), ms(exit.resume)
		if exit.err != nil {
			fail(exit.err)
		}
		if exit.chunks != 100 || exit.bytes != 100*audio.ChunkBytesDefault {
			fail(fmt.Errorf("worker chunks=%d bytes=%d", exit.chunks, exit.bytes))
		}
	case <-ctx.Done():
		fail(ctx.Err())
	}
	streak := 0
	for i, row := range reception.rows {
		report.MaxWaitMS = max(report.MaxWaitMS, row.WaitMS)
		if i > 0 {
			report.MaxPartialGapMS = max(report.MaxPartialGapMS, row.ReceivedMS-reception.rows[i-1].ReceivedMS)
		}
		if pause > 0 && row.Index > 4 && report.RecoveryConfirmedMS == nil {
			if row.WaitMS <= 200 {
				streak++
			} else {
				streak = 0
			}
			if streak == 3 && report.ResumeMS != nil {
				confirmed, elapsed := row.ReceivedMS, row.ReceivedMS-*report.ResumeMS
				report.RecoveryConfirmedMS, report.RecoveryAfterResumeMS = &confirmed, &elapsed
			}
		}
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		fail(err)
	}
	report.Outcome = "completed"
}
