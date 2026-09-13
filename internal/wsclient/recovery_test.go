package wsclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// The harness exercises the actual client, WebSocket Gateway and gRPC boundary.
// Worker fault injection models immutable independent segments, not ASR quality.
func recoveryHarness(t *testing.T, worker asrv1.ASRServiceServer, recovery RecoveryConfig, realtime bool) *Client {
	t.Helper()
	c, _ := recoveryHarnessWithGateway(t, worker, recovery, realtime)
	return c
}

func recoveryHarnessWithGateway(t *testing.T, worker asrv1.ASRServiceServer, recovery RecoveryConfig, realtime bool) (*Client, *gateway.Gateway) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	asrv1.RegisterASRServiceServer(server, worker)
	go func() { _ = server.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///recovery", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	g, err := gateway.New(t.Context(), asrv1.NewASRServiceClient(cc), slog.New(slog.NewTextHandler(io.Discard, nil)), gateway.Config{MaxSessions: 1, Heartbeat: wsheartbeat.Config{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(g)
	t.Cleanup(func() {
		g.StopAccepting()
		waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := g.Wait(waitCtx); err != nil {
			g.Abort()
			t.Errorf("Gateway did not release session: %v", err)
		}
		hs.Close()
		_ = cc.Close()
		server.Stop()
		_ = lis.Close()
	})
	client, err := New(Config{URL: "ws" + strings.TrimPrefix(hs.URL, "http"), ChunkBytes: 320, Realtime: realtime, Recovery: recovery, Heartbeat: wsheartbeat.Config{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	return client, g
}

type recoveryFaultWorker struct {
	asrv1.UnimplementedASRServiceServer
	mode     string
	attempts atomic.Int32
	active   atomic.Int32
	mu       sync.Mutex
	starts   []wsprotocol.StartMessage
}

func (w *recoveryFaultWorker) RecoverableRecognize(s asrv1.ASRService_RecoverableRecognizeServer) error {
	attempt := w.attempts.Add(1)
	w.active.Add(1)
	defer w.active.Add(-1)
	first, err := s.Recv()
	if err != nil {
		return err
	}
	start := first.RecoveryStart
	if start == nil {
		return status.Error(codes.InvalidArgument, "missing Start")
	}
	w.mu.Lock()
	w.starts = append(w.starts, wsprotocol.StartMessage{SessionID: start.SessionId, AttemptID: start.AttemptId, FromSample: start.FromSample})
	w.mu.Unlock()
	if w.mode == "unsupported" {
		return status.Error(codes.Unimplemented, "no recovery")
	}
	if err := s.Send(&asrv1.StreamingRecognizeResponse{Ready: true}); err != nil {
		return err
	}
	from, next := start.FromSample, start.FromSample
	var seq uint64
	checkpoint := func() error {
		if from == next {
			return nil
		}
		cp := &asrv1.RecoveryCheckpoint{FromSample: from, ThroughSample: next, Text: fmt.Sprintf("[%d,%d)", from, next)}
		if w.mode == "invalid" {
			cp.ThroughSample++
		}
		err := s.Send(&asrv1.StreamingRecognizeResponse{Checkpoint: cp})
		from = next
		return err
	}
	for {
		req, err := s.Recv()
		if errors.Is(err, io.EOF) {
			if w.mode == "persistent_tail" || (w.mode == "fail_after_end" && attempt == 1) {
				return status.Error(codes.Unavailable, "lost final closure")
			}
			if w.mode == "eof_without_checkpoint" {
				return nil
			}
			return checkpoint()
		}
		if err != nil {
			return err
		}
		if req.StartSample != next || req.AudioSeq != seq+1 {
			return status.Error(codes.InvalidArgument, "wrong replay position or seq")
		}
		for i := 0; i < len(req.Data); i += 2 {
			if binary.LittleEndian.Uint16(req.Data[i:]) != uint16(next+uint64(i/2)) {
				return status.Error(codes.InvalidArgument, "changed PCM")
			}
		}
		next += uint64(len(req.Data) / 2)
		seq++
		if err := s.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: seq}}); err != nil {
			return err
		}
		if attempt == 1 && seq == 3 && w.mode == "fail_before_checkpoint" {
			return status.Error(codes.Unavailable, "processed but checkpoint lost")
		}
		if w.mode == "persistent_tail" || w.mode == "never_checkpoint" || w.mode == "eof_without_checkpoint" || (attempt == 1 && w.mode == "no_checkpoint_first") {
			continue
		}
		if err := checkpoint(); err != nil {
			return err
		}
		if attempt == 1 && seq == 3 && w.mode == "fail_after_checkpoint" {
			return status.Error(codes.Unavailable, "checkpoint then connection failure")
		}
	}
}
func testPCM(blocks int) []byte {
	data := make([]byte, blocks*320)
	for i := 0; i < len(data); i += 2 {
		binary.LittleEndian.PutUint16(data[i:], uint16(i/2))
	}
	return data
}
func requireCoverage(t *testing.T, r Transcript) {
	t.Helper()
	type span struct{ from, to uint64 }
	spans := make([]span, 0, len(r.Segments)+len(r.Gaps))
	for _, s := range r.Segments {
		spans = append(spans, span{s.FromSample, s.ThroughSample})
	}
	for _, g := range r.Gaps {
		spans = append(spans, span{g.FromSample, g.ThroughSample})
	}
	// Sort small reports without depending on result and gap interleaving.
	for i := 1; i < len(spans); i++ {
		for j := i; j > 0 && spans[j].from < spans[j-1].from; j-- {
			spans[j], spans[j-1] = spans[j-1], spans[j]
		}
	}
	var through uint64
	for _, s := range spans {
		if s.from != through || s.to <= s.from {
			t.Fatalf("coverage hole/overlap: %+v", r)
		}
		through = s.to
	}
	if through != r.CapturedThrough {
		t.Fatalf("coverage ends at %d, captured %d", through, r.CapturedThrough)
	}
}
func TestRecoverableEndToEnd(t *testing.T) {
	for _, mode := range []string{"normal", "fail_before_checkpoint", "fail_after_checkpoint", "fail_after_end"} {
		t.Run(mode, func(t *testing.T) {
			w := &recoveryFaultWorker{mode: mode}
			c := recoveryHarness(t, w, RecoveryConfig{RetryDelay: time.Millisecond, Budget: time.Second}, false)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(10))})
			if err != nil || !r.Complete || !r.Ended || len(r.Gaps) != 0 || r.CapturedThrough != 1600 {
				t.Fatalf("report=%+v err=%v", r, err)
			}
			requireCoverage(t, r)
			if mode == "normal" && r.Attempts != 1 || mode != "normal" && r.Attempts < 2 {
				t.Fatalf("attempts=%d", r.Attempts)
			}
			w.mu.Lock()
			defer w.mu.Unlock()
			for i, s := range w.starts {
				if s.SessionID != r.SessionID {
					t.Fatal("logical session changed")
				}
				if i > 0 && s.AttemptID == w.starts[i-1].AttemptID {
					t.Fatal("attempt identity reused")
				}
			}
			if mode == "fail_before_checkpoint" && w.starts[1].FromSample != 320 {
				t.Fatalf("replay must use applied checkpoint, starts=%+v", w.starts)
			}
			if mode == "fail_after_checkpoint" && w.starts[1].FromSample != 480 {
				t.Fatalf("replayed committed audio: %+v", w.starts)
			}
			if mode == "fail_after_end" && w.starts[1].FromSample != 1600 {
				t.Fatalf("empty tail restart incorrect: %+v", w.starts)
			}
		})
	}
}
func TestRecoverableActualMock(t *testing.T) {
	c := recoveryHarness(t, mockasr.New(mockasr.Config{PartialEvery: 20 * time.Millisecond}), RecoveryConfig{}, true)
	r, err := c.RunRecoverable(t.Context(), ReaderSource{bytes.NewReader(testPCM(8))})
	if err != nil || !r.Complete || len(r.Segments) != 4 {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	requireCoverage(t, r)
}
func TestRecoverableOverflowKeepsGap(t *testing.T) {
	w := &recoveryFaultWorker{mode: "no_checkpoint_first"}
	c := recoveryHarness(t, w, RecoveryConfig{BufferDuration: 40 * time.Millisecond, Budget: time.Second, RetryDelay: time.Millisecond}, true)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(35))})
	if !errors.Is(err, ErrIncomplete) || r.Complete || !r.Ended || len(r.Gaps) != 1 || r.Gaps[0].Reason != "buffer_exhausted" || r.Attempts < 2 {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	if r.CachePeakBytes > 1280 {
		t.Fatal("cache exceeded PCM budget")
	}
	requireCoverage(t, r)
}
func TestRecoverableEndTailBudget(t *testing.T) {
	w := &recoveryFaultWorker{mode: "persistent_tail"}
	c := recoveryHarness(t, w, RecoveryConfig{Budget: 150 * time.Millisecond, RetryDelay: 10 * time.Millisecond}, false)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(5))})
	if !errors.Is(err, ErrRecoveryBudget) || !r.Ended || r.Complete || r.Attempts < 2 || time.Since(start) > time.Second {
		t.Fatalf("report=%+v err=%v elapsed=%v", r, err, time.Since(start))
	}
	requireCoverage(t, r)
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, s := range w.starts {
		if s.FromSample != 0 {
			t.Fatalf("End tail skipped: %+v", w.starts)
		}
	}
}
func TestRecoverableUnsupported(t *testing.T) {
	c := recoveryHarness(t, &recoveryFaultWorker{mode: "unsupported"}, RecoveryConfig{}, false)
	r, err := c.RunRecoverable(t.Context(), ReaderSource{bytes.NewReader(testPCM(1))})
	if err == nil || r.Attempts != 1 || r.Complete {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	requireCoverage(t, r)
}

// A context-aware live source proves capture and retry lifetime are independent.
type waitingSource struct{ entered, exited chan struct{} }

func (s waitingSource) Read(ctx context.Context, p []byte) (int, error) {
	close(s.entered)
	defer close(s.exited)
	<-ctx.Done()
	return 0, ctx.Err()
}
func TestRecoverableAdmissionBudgetCancelsCapture(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { attempts.Add(1); http.Error(w, "full", 503) }))
	defer server.Close()
	c, _ := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Recovery: RecoveryConfig{Budget: 100 * time.Millisecond, RetryDelay: 10 * time.Millisecond}})
	source := waitingSource{make(chan struct{}), make(chan struct{})}
	start := time.Now()
	r, err := c.RunRecoverable(t.Context(), source)
	if !errors.Is(err, ErrRecoveryBudget) || time.Since(start) > time.Second || r.Attempts < 2 {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	select {
	case <-source.exited:
	default:
		t.Fatal("capture not joined")
	}
	n := attempts.Load()
	time.Sleep(30 * time.Millisecond)
	if attempts.Load() != n {
		t.Fatal("retries survived logical session")
	}
}

// A protocol peer isolates client validation from the Gateway's own checks.
func TestRecoverableRejectsInvalidAndIgnoresDuplicate(t *testing.T) {
	for _, mode := range []string{"duplicate", "hole", "ahead", "identity", "old_attempt", "before_ready"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				_, data, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				var start wsprotocol.StartMessage
				_ = json.Unmarshal(data, &start)
				send := func(msg wsprotocol.RecoveryMessage) error {
					b, _ := json.Marshal(msg)
					return conn.Write(r.Context(), websocket.MessageText, b)
				}
				msg := wsprotocol.RecoveryMessage{Type: wsprotocol.MessageTypeReady, SessionID: start.SessionID, AttemptID: start.AttemptID}
				if mode == "before_ready" {
					msg.Type = wsprotocol.MessageTypeCheckpoint
					msg.ThroughSample = 1
				}
				if send(msg) != nil {
					return
				}
				typ, _, err := conn.Read(r.Context())
				if err != nil || typ != websocket.MessageBinary {
					return
				}
				msg.Type = wsprotocol.MessageTypeCheckpoint
				msg.ThroughSample = 160
				switch mode {
				case "hole":
					msg.FromSample = 1
				case "ahead":
					msg.ThroughSample = 161
				case "identity":
					msg.SessionID = "wrong-session"
				}
				if mode == "old_attempt" {
					old := msg
					old.AttemptID = "old"
					old.ThroughSample = 999
					if send(old) != nil {
						return
					}
				}
				if send(msg) != nil {
					return
				}
				if mode == "duplicate" {
					if send(msg) != nil {
						return
					}
				}
				for {
					typ, _, err = conn.Read(r.Context())
					if err != nil {
						return
					}
					if typ == websocket.MessageText {
						_ = conn.Close(websocket.StatusNormalClosure, "done")
						return
					}
				}
			}))
			defer server.Close()
			c, _ := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), ChunkBytes: 320, Heartbeat: wsheartbeat.Config{Disabled: true}})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(1))})
			if mode == "duplicate" || mode == "old_attempt" {
				if err != nil || len(r.Segments) != 1 || !r.Complete {
					t.Fatalf("report=%+v err=%v", r, err)
				}
			} else if !errors.Is(err, ErrRecoveryProtocol) || r.Attempts != 1 {
				t.Fatalf("report=%+v err=%v", r, err)
			}
		})
	}
}
func TestPCMCacheCheckpointInsideBlock(t *testing.T) {
	b := pcmCache{maxBytes: 640, maxChunks: 2}
	if !b.append(capturedFrame{data: testPCM(1)}) || !b.append(capturedFrame{from: 160, data: testPCM(1)}) {
		t.Fatal("append")
	}
	if b.append(capturedFrame{from: 320, data: []byte{0, 0}}) {
		t.Fatal("overflow accepted")
	}
	b.drop(80)
	f, ok := b.at(80)
	if !ok || len(f.data) != 160 || b.bytes != 480 {
		t.Fatalf("cache=%+v frame=%+v", b, f)
	}
	b.drop(320)
	if b.bytes != 0 || len(b.frames) != 0 {
		t.Fatal("committed PCM retained")
	}
}

func TestRecoverableFallbackDoesNotRenewBudget(t *testing.T) {
	w := &recoveryFaultWorker{mode: "never_checkpoint"}
	c := recoveryHarness(t, w, RecoveryConfig{BufferDuration: 40 * time.Millisecond, Budget: 180 * time.Millisecond, RetryDelay: time.Millisecond}, true)
	start := time.Now()
	r, err := c.RunRecoverable(t.Context(), ReaderSource{bytes.NewReader(testPCM(1000))})
	if !errors.Is(err, ErrRecoveryBudget) || r.Ended || r.Complete || r.Attempts < 3 || time.Since(start) > time.Second || len(r.Gaps) == 0 {
		t.Fatalf("report=%+v err=%v elapsed=%v", r, err, time.Since(start))
	}
	requireCoverage(t, r)
}
func TestRecoverableWorkerContractViolations(t *testing.T) {
	for _, mode := range []string{"invalid", "eof_without_checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			c := recoveryHarness(t, &recoveryFaultWorker{mode: mode}, RecoveryConfig{Budget: time.Second}, false)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(3))})
			if err == nil || r.Complete || r.Attempts != 1 || websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Fatalf("report=%+v err=%v", r, err)
			}
			requireCoverage(t, r)
		})
	}
}
func TestRecoverableReadyTimeout(t *testing.T) {
	var active atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	c, _ := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Heartbeat: wsheartbeat.Config{Disabled: true}, Recovery: RecoveryConfig{ReadyTimeout: 30 * time.Millisecond, Budget: 120 * time.Millisecond, RetryDelay: time.Millisecond}})
	start := time.Now()
	r, err := c.RunRecoverable(t.Context(), ReaderSource{bytes.NewReader(nil)})
	if !errors.Is(err, ErrRecoveryBudget) || r.Attempts < 2 || time.Since(start) > time.Second {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	deadline := time.Now().Add(time.Second)
	for active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 0 {
		t.Fatal("attempt reader retained")
	}
}
func TestRecoverableEndedOverflowDoesNotSkip(t *testing.T) {
	w := &recoveryFaultWorker{mode: "never_checkpoint"}
	// An unpaced finite source reaches End during initial admission. Retained
	// history cannot fit, and the client must not report a shortened success.
	c := recoveryHarness(t, w, RecoveryConfig{BufferDuration: 20 * time.Millisecond, Budget: 100 * time.Millisecond, RetryDelay: 20 * time.Millisecond}, false)
	r, err := c.RunRecoverable(t.Context(), ReaderSource{bytes.NewReader(testPCM(20))})
	if err == nil || r.Complete || len(r.Gaps) == 0 {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	requireCoverage(t, r)
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, s := range w.starts {
		if s.FromSample != 0 {
			t.Fatalf("started latest after input End: %+v", w.starts)
		}
	}
}
func TestRecoverableInputFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "full", 503) }))
	defer server.Close()
	for _, data := range [][]byte{{1}, {0, 0, 1}} {
		c, _ := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http")})
		r, err := c.RunRecoverable(t.Context(), ReaderSource{bytes.NewReader(data)})
		if err == nil || !strings.Contains(err.Error(), "unaligned PCM") || r.Complete {
			t.Fatalf("report=%+v err=%v", r, err)
		}
	}
	c, _ := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Recovery: RecoveryConfig{BufferDuration: time.Millisecond}})
	if _, err := c.RunRecoverable(t.Context(), ReaderSource{bytes.NewReader(nil)}); err == nil {
		t.Fatal("oversized chunk accepted")
	}
}

// The first relay discards a complete downstream checkpoint before the client
// applies it and severs both actual WebSocket connections. The next attempt goes
// through the same Gateway's ordinary admission and RPC creation paths.
func checkpointLossRelay(t *testing.T, target string) (string, *atomic.Int32) {
	t.Helper()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nth := attempts.Add(1)
		up, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer up.CloseNow()
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		down, _, err := websocket.Dial(ctx, target, nil)
		if err != nil {
			return
		}
		defer down.CloseNow()
		senderDone := make(chan struct{})
		go func() {
			defer close(senderDone)
			for {
				typ, data, err := up.Read(ctx)
				if err != nil {
					cancel()
					_ = down.CloseNow()
					return
				}
				if down.Write(ctx, typ, data) != nil {
					cancel()
					_ = up.CloseNow()
					return
				}
			}
		}()
		defer func() { cancel(); _ = up.CloseNow(); _ = down.CloseNow(); <-senderDone }()
		checkpoints := 0
		for {
			typ, data, err := down.Read(ctx)
			if err != nil {
				if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
					_ = up.Close(websocket.StatusNormalClosure, "completed")
				}
				return
			}
			var msg wsprotocol.RecoveryMessage
			_ = json.Unmarshal(data, &msg)
			if msg.Type == wsprotocol.MessageTypeCheckpoint {
				checkpoints++
			}
			if nth == 1 && checkpoints == 2 {
				return
			}
			if up.Write(ctx, typ, data) != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http"), &attempts
}
func TestRecoverableLostWebSocketCheckpoint(t *testing.T) {
	w := &recoveryFaultWorker{mode: "normal"}
	c := recoveryHarness(t, w, RecoveryConfig{RetryDelay: time.Millisecond, Budget: time.Second}, false)
	c.cfg.URL, _ = checkpointLossRelay(t, c.cfg.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(10))})
	if err != nil || !r.Complete || r.Attempts < 2 {
		t.Fatalf("report=%+v err=%v", r, err)
	}
	requireCoverage(t, r)
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.starts) < 2 || w.starts[1].FromSample != 160 {
		t.Fatalf("replayed from Worker progress instead of client-applied result: %+v", w.starts)
	}
}

func TestRecoverableNormalCloseDuringPing(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		ping := make(chan struct{})
		var once sync.Once
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OnPingReceived: func(context.Context, []byte) bool { once.Do(func() { close(ping) }); return false }})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var start wsprotocol.StartMessage
		_ = json.Unmarshal(data, &start)
		msg := wsprotocol.RecoveryMessage{Type: wsprotocol.MessageTypeReady, SessionID: start.SessionID, AttemptID: start.AttemptID}
		send := func() error { b, _ := json.Marshal(msg); return conn.Write(r.Context(), websocket.MessageText, b) }
		if send() != nil {
			return
		}
		done := make(chan struct{})
		defer func() { _ = conn.CloseNow(); <-done }()
		go func() {
			defer close(done)
			for {
				typ, data, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				if typ == websocket.MessageText {
					var end wsprotocol.EndMessage
					_ = json.Unmarshal(data, &end)
					// Reader must keep processing control frames while the closer waits.
					go func() {
						select {
						case <-ping:
						case <-r.Context().Done():
							return
						}
						msg.Type = wsprotocol.MessageTypeCheckpoint
						msg.ThroughSample = end.ThroughSample
						if send() == nil {
							_ = conn.Close(websocket.StatusNormalClosure, "done")
						}
					}()
				}
			}
		}()
		// done closes only after the normal close has unblocked Reader.
		<-done
	}))
	defer server.Close()
	c, _ := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), ChunkBytes: 320, Heartbeat: wsheartbeat.Config{Interval: 5 * time.Millisecond, Timeout: 100 * time.Millisecond}})
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(1))})
		cancel()
		if err != nil || !r.Complete || r.Attempts != 1 {
			t.Fatalf("normal close misclassified: %+v %v", r, err)
		}
	}
	if attempts.Load() != 5 {
		t.Fatal("healthy close caused retry")
	}
}
