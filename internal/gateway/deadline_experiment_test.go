//go:build tide_load

package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// EXP-004 uses temporary overlays and experimental progress envelopes. Neither
// the envelopes nor the cancellation close code constitute a production API.
var deadlineOverlayEnabled bool
var deadlineObserver atomic.Pointer[deadlineTrace]

// deadlineTrace belongs to one trial/session. Arrival history is retained only
// for this bounded experiment, not proposed as a production progress tracker.
// Queue hooks acquire q.mu then mu; no path acquires these in reverse order.
type deadlineTrace struct {
	mu                                              sync.Mutex
	mode, scenario                                  string
	epoch                                           time.Time
	arrivals                                        []time.Time
	dequeued, acknowledged                          int
	queuePeak                                       int
	sendStarted, ended, triggered, observed, breach time.Time
	reason                                          string
	cancel                                          context.CancelCauseFunc
	oldestPeak, queueAgePeak, sendPeak              time.Duration
	latency                                         loadHistogram
	sent, results                                   int
	final                                           bool
	processed                                       atomic.Int64 // Ground truth for reporting ONLY, never drives policy.
	workerDone                                      chan struct{}
}

func deadlinePush(q *audioQueue) {
	if m := deadlineObserver.Load(); m != nil {
		m.mu.Lock()
		m.arrivals = append(m.arrivals, time.Now())
		m.queuePeak = max(m.queuePeak, q.bytes)
		m.mu.Unlock()
	}
}
func deadlinePop() {
	if m := deadlineObserver.Load(); m != nil {
		m.mu.Lock()
		m.dequeued++
		m.mu.Unlock()
	}
}
func deadlineEnd() {
	if m := deadlineObserver.Load(); m != nil {
		m.mu.Lock()
		if m.ended.IsZero() {
			m.ended = time.Now()
		}
		m.mu.Unlock()
	}
}

// deadlineWatch is joined by run's defer. It observes every 10 ms even if no
// new audio/result arrives. Cancel outside the metric lock to avoid lock/I/O coupling.
func deadlineWatch(s *session) func() {
	m := deadlineObserver.Load()
	if m == nil {
		return func() {}
	}
	m.mu.Lock()
	m.cancel = s.cancel
	m.mu.Unlock()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-s.ctx.Done():
				return
			case now := <-ticker.C:
				m.mu.Lock()
				oldest, queued, sending := time.Duration(0), time.Duration(0), time.Duration(0)
				if m.acknowledged < len(m.arrivals) {
					oldest = now.Sub(m.arrivals[m.acknowledged])
				}
				if m.dequeued < len(m.arrivals) {
					queued = now.Sub(m.arrivals[m.dequeued])
				}
				if !m.sendStarted.IsZero() {
					sending = now.Sub(m.sendStarted)
				}
				m.oldestPeak = max(m.oldestPeak, oldest)
				m.queueAgePeak = max(m.queueAgePeak, queued)
				if oldest >= 3*time.Second && m.breach.IsZero() {
					m.breach = now
				}
				reason := ""
				if m.mode == "local" {
					if queued >= 3*time.Second {
						reason = "queue_age"
					}
					if sending >= 3*time.Second {
						reason = "send_stall"
					}
				}
				if m.mode == "progress" && oldest >= 3*time.Second {
					reason = "unprocessed_age"
				}
				if m.mode != "baseline" && !m.ended.IsZero() && now.Sub(m.ended) >= 5*time.Second {
					reason = "end_timeout"
				}
				if reason != "" && m.reason == "" {
					m.reason, m.triggered = reason, now
				}
				m.mu.Unlock()
				if reason != "" {
					s.cancel(fmt.Errorf("EXP-004: %s", reason))
					return
				}
			}
		}
	}()
	return func() { close(stop); <-done }
}

// TestExperimentDeadline compares policies, rather than asserting that every
// candidate meets the target. Negative evidence is a valid experiment result.
func TestExperimentDeadline(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("explicit experiment opt-in required")
	}
	if !deadlineOverlayEnabled {
		t.Fatal("use scripts/run_deadline_experiment.py")
	}
	for _, scenario := range []string{"normal", "jitter", "slow", "silence", "batched_progress", "stalled_one", "end_hang"} {
		for _, mode := range []string{"baseline", "local", "progress"} {
			t.Run(scenario+"/"+mode, func(t *testing.T) { runDeadlineTrial(t, scenario, mode) })
		}
	}
}

func runDeadlineTrial(t *testing.T, scenario, mode string) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Second)
	defer cancel()
	m := &deadlineTrace{mode: mode, scenario: scenario, workerDone: make(chan struct{})}
	deadlineObserver.Store(m)
	t.Cleanup(func() { deadlineObserver.Store(nil) })
	h := newGatewayHarness(t, startDeadlineWorker(t, m), 1)
	h.ctx = ctx
	conn := h.mustDial(t)
	m.epoch = time.Now() // Established before Start can start Worker or watcher work.
	sendCtx, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	sentDone := make(chan int, 1)
	chunks := 300 // 6 seconds, including silence (actual zero PCM except metadata).
	if scenario == "slow" {
		chunks = 600
	}
	if scenario == "stalled_one" {
		chunks = 1
	}
	if scenario == "end_hang" {
		chunks = 50
	}
	go func() {
		sent := 0
		defer func() { sentDone <- sent }()
		if conn.Write(sendCtx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)) != nil {
			return
		}
		data := make([]byte, 640)
		for i := 0; i < chunks; i++ {
			if loadWait(sendCtx, time.Until(m.epoch.Add(time.Duration(i)*20*time.Millisecond))) != nil {
				return
			}
			binary.LittleEndian.PutUint64(data, uint64(i))
			binary.LittleEndian.PutUint64(data[8:], uint64(time.Since(m.epoch)))
			if conn.Write(sendCtx, websocket.MessageBinary, data) != nil {
				return
			}
			sent++
		}
		if scenario != "stalled_one" {
			_ = conn.Write(sendCtx, websocket.MessageText, []byte(`{"type":"end"}`))
		}
	}()
	// Finite observation windows are reported as censored, never as policy exits.
	window := 14 * time.Second
	if scenario == "stalled_one" {
		window = 6 * time.Second
	}
	if scenario == "end_hang" {
		window = 8 * time.Second
	}
	observation := time.AfterFunc(window, func() {
		m.mu.Lock()
		if m.reason == "" {
			m.reason, m.observed = "observation_end", time.Now()
		}
		abort := m.cancel
		m.mu.Unlock()
		if abort != nil {
			abort(errors.New("EXP-004 observation window ended"))
		}
	})
	defer observation.Stop()
	nextResult := 0
	var clientErr string
	code := websocket.StatusCode(-1)
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			code = websocket.CloseStatus(err)
			break
		}
		var response wsprotocol.ResultMessage
		if err := json.Unmarshal(data, &response); err != nil {
			clientErr = err.Error()
			break
		}
		if response.IsFinal {
			m.final = true
			continue
		}
		seq, err := strconv.Atoi(response.SegmentID)
		if err != nil || seq != nextResult {
			clientErr = "result order mismatch"
			break
		}
		stamp, err := strconv.ParseInt(response.Text, 10, 64)
		if err != nil {
			clientErr = "invalid result timestamp"
			break
		}
		m.latency.add(time.Since(m.epoch) - time.Duration(stamp))
		nextResult++
	}
	cancelSend()
	m.sent = <-sentDone
	if clientErr != "" {
		_ = conn.CloseNow()
	}
	h.waitHandlers(t, 1)
	select {
	case <-m.workerDone:
	case <-ctx.Done():
		t.Fatal("Worker did not exit")
	}
	elapsed := time.Since(m.epoch)
	m.mu.Lock()
	defer m.mu.Unlock()
	outcome := m.reason
	if outcome == "" {
		if code == websocket.StatusNormalClosure {
			outcome = "completed"
		} else if code == websocket.StatusTryAgainLater {
			outcome = "queue_full"
		} else {
			outcome = "unexpected_close"
		}
	}
	atMS := func(at time.Time) any {
		if at.IsZero() {
			return nil
		}
		return float64(at.Sub(m.epoch)) / float64(time.Millisecond)
	}
	endMS, cleanupMS := any(nil), any(nil)
	if !m.ended.IsZero() {
		endMS = float64(m.epoch.Add(elapsed).Sub(m.ended)) / float64(time.Millisecond)
	}
	if !m.triggered.IsZero() {
		cleanupMS = float64(m.epoch.Add(elapsed).Sub(m.triggered)) / float64(time.Millisecond)
	}
	result := map[string]any{
		"experiment": "EXP-004", "scenario": scenario, "policy": mode,
		"elapsed_ms": float64(elapsed) / float64(time.Millisecond), "outcome": outcome, "close_code": int(code),
		"trigger_ms": atMS(m.triggered), "first_age_breach_ms": atMS(m.breach), "observation_end_ms": atMS(m.observed),
		"end_received_ms": atMS(m.ended), "end_to_cleanup_ms": endMS, "trigger_to_cleanup_ms": cleanupMS,
		"sent": m.sent, "admitted": len(m.arrivals), "dequeued": m.dequeued, "processed": m.processed.Load(),
		"acknowledged": m.acknowledged, "results": nextResult, "final": m.final,
		"unacknowledged_tail": len(m.arrivals) - m.acknowledged, "queue_peak_bytes": m.queuePeak,
		"oldest_unacknowledged_peak_ms": float64(m.oldestPeak) / float64(time.Millisecond),
		"queue_age_peak_ms":             float64(m.queueAgePeak) / float64(time.Millisecond),
		"send_max_ms":                   float64(m.sendPeak) / float64(time.Millisecond), "result_latency": m.latency.summary(),
		"registered_after_cleanup": len(h.gateway.registry.snapshot()), "client_error": clientErr,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", encoded)
	if clientErr != "" || outcome == "unexpected_close" || ctx.Err() != nil {
		t.Errorf("driver failure: %s, %s, %v", clientErr, outcome, ctx.Err())
	}
	if outcome == "completed" && (!m.final || m.sent != chunks || m.acknowledged != chunks || (scenario != "silence" && nextResult != chunks)) {
		t.Error("incomplete normal completion")
	}
	if len(h.gateway.registry.snapshot()) != 0 {
		t.Error("registry not empty")
	}
}

// Worker acknowledgements mean processing finished, including silence. Batching
// advances a contiguous watermark once per second; a received byte is never an ack.
type deadlineWorker struct {
	asrv1.UnimplementedASRServiceServer
	m *deadlineTrace
}

func (w *deadlineWorker) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	defer close(w.m.workerDone)
	count := 0
	ack := func() error {
		return stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "__exp_progress", Text: strconv.Itoa(count)})
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if w.m.scenario == "end_hang" {
				<-stream.Context().Done()
				return stream.Context().Err()
			}
			if err := ack(); err != nil {
				return err
			}
			return stream.Send(&asrv1.StreamingRecognizeResponse{Text: "final", IsFinal: true})
		}
		if err != nil {
			return err
		}
		if len(req.Data) != 640 || int(binary.LittleEndian.Uint64(req.Data)) != count {
			return errors.New("audio order mismatch")
		}
		if w.m.scenario == "stalled_one" {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		delay := 5 * time.Millisecond
		if w.m.scenario == "slow" {
			delay = 50 * time.Millisecond
		}
		if w.m.scenario == "jitter" && count == 50 {
			delay = 500 * time.Millisecond
		}
		if err := loadWait(stream.Context(), delay); err != nil {
			return err
		}
		count++
		w.m.processed.Store(int64(count))
		if w.m.scenario != "batched_progress" || count%50 == 0 {
			if err := ack(); err != nil {
				return err
			}
		}
		if w.m.scenario != "silence" {
			if err := stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: strconv.Itoa(count - 1), Text: strconv.FormatUint(binary.LittleEndian.Uint64(req.Data[8:]), 10)}); err != nil {
				return err
			}
		}
	}
}

func startDeadlineWorker(t *testing.T, m *deadlineTrace) asrv1.ASRServiceClient {
	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer(grpc.InitialWindowSize(64*1024), grpc.InitialConnWindowSize(1024*1024))
	asrv1.RegisterASRServiceServer(server, &deadlineWorker{m: m})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///deadline-worker", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithInitialWindowSize(64*1024), grpc.WithInitialConnWindowSize(1024*1024), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &deadlineClient{ASRServiceClient: asrv1.NewASRServiceClient(conn), m: m}
}

type deadlineClient struct {
	asrv1.ASRServiceClient
	m *deadlineTrace
}

func (c *deadlineClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	stream, err := c.ASRServiceClient.StreamingRecognize(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &deadlineStream{BidiStreamingClient: stream, m: c.m}, nil
}

// Progress is carried over real gRPC and consumed by this experimental adapter;
// it is not fabricated from the Worker's shared in-process ground-truth counter.
type deadlineStream struct {
	grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	m *deadlineTrace
}

func (s *deadlineStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	started := time.Now()
	s.m.mu.Lock()
	s.m.sendStarted = started
	s.m.mu.Unlock()
	err := s.BidiStreamingClient.Send(req)
	s.m.mu.Lock()
	s.m.sendPeak = max(s.m.sendPeak, time.Since(started))
	s.m.sendStarted = time.Time{}
	s.m.mu.Unlock()
	return err
}
func (s *deadlineStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	for {
		response, err := s.BidiStreamingClient.Recv()
		if err != nil {
			return nil, err
		}
		if response.SegmentId != "__exp_progress" {
			return response, nil
		}
		ack, err := strconv.Atoi(response.Text)
		s.m.mu.Lock()
		if err != nil || ack < s.m.acknowledged || ack > len(s.m.arrivals) {
			s.m.mu.Unlock()
			return nil, errors.New("invalid experimental progress")
		}
		s.m.acknowledged = ack
		s.m.mu.Unlock()
	}
}
