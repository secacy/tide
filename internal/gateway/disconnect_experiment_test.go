//go:build tide_disconnect

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// EXP-010 uses a bounded TCP relay, not packet loss emulation. Pausing forwarding
// preserves byte order and suppresses FIN in that direction until release. The
// OS socket buffers still exist; no conclusion about WAN/TCP retransmission follows.
type disconnectGate struct {
	mu        sync.Mutex
	wake      chan struct{}
	paused    bool
	forwarded atomic.Int64
	held      atomic.Int64 // Bytes already read by this relay but waiting to be forwarded.
	blocked   atomic.Int64
}

func newDisconnectGate() *disconnectGate { return &disconnectGate{wake: make(chan struct{})} }

func (g *disconnectGate) set(paused bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused != paused {
		g.paused = paused
		close(g.wake)
		g.wake = make(chan struct{})
	}
}

func (g *disconnectGate) wait(ctx context.Context, bytes int) error {
	g.held.Store(int64(bytes))
	defer g.held.Store(0)
	counted := false
	for {
		g.mu.Lock()
		paused, wake := g.paused, g.wake
		g.mu.Unlock()
		if !paused {
			return nil
		}
		if !counted {
			g.blocked.Add(1)
			counted = true
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// At most 32 KiB per direction is held in the relay. EOF is forwarded as a
// half-close only after the gate opens, so a black hole does not manufacture an
// immediate disconnection at the other endpoint when the first detector closes.
func disconnectCopy(ctx context.Context, dst, src *net.TCPConn, gate *disconnectGate) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if gate.wait(ctx, n) != nil {
			return
		}
		if n > 0 {
			written, writeErr := dst.Write(buf[:n])
			gate.forwarded.Add(int64(written))
			if writeErr != nil {
				return
			}
		}
		if err != nil {
			_ = dst.CloseWrite()
			return
		}
	}
}

// A source-level overlay adds only a decision observation, after the production
// coordinator returns. It never changes the selected result or any deadline.
var disconnectOverlayEnabled bool
var disconnectObservers sync.Map // *session -> *disconnectTrace

func disconnectDecision(s *session, result sessionResult) {
	if v, ok := disconnectObservers.Load(s); ok {
		v.(*disconnectTrace).mark("gateway_decision", fmt.Sprintf("kind=%d error=%v", result.kind, result.err))
	}
}

type disconnectEvent struct {
	MS     float64 `json:"ms"`
	Detail string  `json:"detail,omitempty"`
}

type disconnectTrace struct {
	mu     sync.Mutex
	epoch  time.Time
	events map[string]disconnectEvent
	pings  map[string]int
}

func (r *disconnectTrace) mark(name, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.events[name]; !exists {
		r.events[name] = disconnectEvent{float64(time.Since(r.epoch)) / float64(time.Millisecond), detail}
	}
}

func (r *disconnectTrace) ping(side string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pings[side]++
}

func (r *disconnectTrace) snapshot() (map[string]disconnectEvent, map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	events, pings := make(map[string]disconnectEvent), make(map[string]int)
	for k, v := range r.events {
		events[k] = v
	}
	for k, v := range r.pings {
		pings[k] = v
	}
	return events, pings
}

// Candidate watchdog: fixed-period Ping independent of application traffic,
// one outstanding Ping per endpoint, 3s response/write budget. Reader must keep
// running. On failure this experiment invokes the existing local abort/close;
// integrating it into production lifecycle is a later decision.
func disconnectHeartbeat(ctx context.Context, stop <-chan struct{}, conn *websocket.Conn, side string, trace *disconnectTrace, fail func(), interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case <-stop:
				return
			default:
			}
			probe, cancel := context.WithTimeout(ctx, timeout)
			err := conn.Ping(probe)
			cancel()
			if ctx.Err() != nil {
				return
			} // Planned teardown is not a failed probe.
			if err != nil {
				trace.mark(side+"_heartbeat_failure", err.Error())
				fail()
				return
			}
			trace.ping(side)
		}
	}
}

type disconnectClient struct {
	asrv1.ASRServiceClient
	onOpen  func(context.Context)
	stall   bool
	trace   *disconnectTrace
	entered chan struct{}
}

func (c *disconnectClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	c.onOpen(ctx)
	stream, err := c.ASRServiceClient.StreamingRecognize(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &disconnectStream{BidiStreamingClient: stream, owner: c, ctx: ctx}, nil
}

type disconnectStream struct {
	grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	owner *disconnectClient
	ctx   context.Context
}

func (s *disconnectStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	if s.owner.stall && req.AudioSeq == 2 {
		s.owner.trace.mark("send_stall", "second Send waits for RPC cancellation; test adapter")
		close(s.owner.entered)
		<-s.ctx.Done()
		s.owner.trace.mark("send_exit", s.ctx.Err().Error())
		return s.ctx.Err()
	}
	return s.BidiStreamingClient.Send(req)
}

// TestExperimentDisconnect is opt-in. Baseline and candidate run the same fault
// at the same phase; a 6s observation window is recorded BEFORE forced cleanup.
func TestExperimentDisconnect(t *testing.T) {
	if os.Getenv("TIDE_RUN_DISCONNECT_EXPERIMENTS") != "1" {
		t.Skip("explicit experiment opt-in required")
	}
	if !disconnectOverlayEnabled {
		t.Fatal("decision observation overlay is required")
	}
	for _, scenario := range []string{"idle", "acked_idle", "blackhole", "blackhole_late", "upstream_blackhole", "downstream_blackhole", "pause", "send_stall", "send_stall_isolated"} {
		for _, mode := range []string{"baseline", "heartbeat"} {
			t.Run(scenario+"_"+mode, func(t *testing.T) { runDisconnect(t, scenario, mode) })
		}
	}
}

func runDisconnect(t *testing.T, scenario, mode string) {
	runDisconnectTimed(t, scenario, mode, "EXP-010", 2*time.Second, 6*time.Second)
}

// runDisconnectTimed shares the exact fault and cleanup paths across interval
// candidates. The legacy EXP-010 entry keeps its original timing and record names.
func runDisconnectTimed(t *testing.T, scenario, mode, experiment string, interval, observation time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), max(25*time.Second, interval+observation+15*time.Second))
	defer cancel()
	trace := &disconnectTrace{epoch: time.Now(), events: make(map[string]disconnectEvent), pings: make(map[string]int)}
	started, workerDone, rpcCanceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
		close(started)
		defer func() { trace.mark("worker_exit", ""); close(workerDone) }()
		for {
			req, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return stream.Send(&asrv1.StreamingRecognizeResponse{Text: "final", IsFinal: true})
			}
			if err != nil {
				return err
			}
			if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: req.AudioSeq}}); err != nil {
				return err
			}
			if err := stream.Send(&asrv1.StreamingRecognizeResponse{Text: "ack"}); err != nil {
				return err
			}
		}
	}})
	wrapped := &disconnectClient{ASRServiceClient: worker, stall: strings.HasPrefix(scenario, "send_stall"), trace: trace, entered: make(chan struct{})}
	bound := make(chan *session, 1)
	var g *Gateway
	wrapped.onOpen = func(rpcCtx context.Context) {
		s := g.registry.snapshot()[0] // Same handler has already assigned s.ws before this call.
		disconnectObservers.Store(s, trace)
		bound <- s
		context.AfterFunc(rpcCtx, func() { trace.mark("rpc_cancel", "context cancellation observed"); close(rpcCanceled) })
	}
	cfg := Config{MaxSessions: 1}
	if scenario == "send_stall_isolated" {
		cfg.ProcessingTimeout = 12 * time.Second
	}
	var err error
	g, err = New(ctx, wrapped, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	listen := func() *net.TCPListener {
		t.Helper()
		l, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	backend, front := listen(), listen()
	finished, serverDone, relayDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.ServeHTTP(w, r)
		trace.mark("handler_exit", "registry and lease already released")
		close(finished)
	})}
	go func() { defer close(serverDone); _ = server.Serve(backend) }()
	up, down := newDisconnectGate(), newDisconnectGate()
	relayCtx, stopRelay := context.WithCancel(ctx)
	go func() {
		defer close(relayDone)
		client, err := front.AcceptTCP()
		if err != nil {
			return
		}
		defer client.Close()
		remote, err := net.DialTCP("tcp4", nil, backend.Addr().(*net.TCPAddr))
		if err != nil {
			return
		}
		defer remote.Close()
		stop := context.AfterFunc(relayCtx, func() { _ = client.Close(); _ = remote.Close() })
		defer stop()
		var wg sync.WaitGroup
		wg.Go(func() { disconnectCopy(relayCtx, remote, client, up) })
		wg.Go(func() { disconnectCopy(relayCtx, client, remote, down) })
		wg.Wait()
	}()
	t.Cleanup(func() {
		stopRelay()
		_ = front.Close()
		g.Abort()
		_ = server.Close()
		<-relayDone
		<-serverDone
	})
	conn, _, err := websocket.Dial(ctx, "ws://"+front.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	readDone, results := make(chan struct{}), make(chan string, 4)
	go func() {
		defer close(readDone)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				trace.mark("client_read_exit", err.Error())
				return
			}
			var result struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(data, &result); err != nil {
				trace.mark("client_decode_error", err.Error())
				return
			}
			results <- result.Text
		}
	}()
	write := func(kind websocket.MessageType, data string) {
		t.Helper()
		if err := conn.Write(ctx, kind, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	write(websocket.MessageText, `{"type":"start","version":"v1"}`)
	awaitGatewaySignal(t, ctx, started, "Worker RPC started")
	var s *session
	select {
	case s = <-bound:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer disconnectObservers.Delete(s)
	if scenario != "idle" {
		write(websocket.MessageBinary, "\x01\x00")
		select {
		case text := <-results:
			if text != "ack" {
				t.Fatalf("unexpected warmup result %q", text)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		trace.mark("audio_confirmed", "progress precedes ack on the ordered response stream")
	}
	// Both modes have exactly the same one-off warmup probes; baseline has no
	// ongoing probes. Completion also establishes that both Readers are running.
	for _, c := range []*websocket.Conn{conn, s.ws} {
		probe, done := context.WithTimeout(ctx, time.Second)
		err := c.Ping(probe)
		done()
		if err != nil {
			t.Fatal(err)
		}
	}
	trace.mark("observation_start", "")
	// Stop future probes without canceling a healthy in-flight control-frame
	// Write: canceling its Context can close the underlying WebSocket. The last
	// Ping keeps its own finite budget; the experiment joins it before End.
	hbStop := make(chan struct{})
	var hbStopOnce sync.Once
	stopHB := func() { hbStopOnce.Do(func() { close(hbStop) }) }
	var heartbeatWG sync.WaitGroup
	defer func() { stopHB(); heartbeatWG.Wait() }()
	if mode == "heartbeat" {
		heartbeatWG.Go(func() {
			disconnectHeartbeat(ctx, hbStop, conn, "client", trace, func() { _ = conn.CloseNow() }, interval, 3*time.Second)
		})
		heartbeatWG.Go(func() {
			disconnectHeartbeat(s.ctx, hbStop, s.ws, "gateway", trace, s.abort, interval, 3*time.Second)
		})
	}
	healthy := scenario == "idle" || scenario == "acked_idle" || scenario == "pause"
	phase := 200 * time.Millisecond
	if scenario == "blackhole_late" {
		phase = 1200 * time.Millisecond
	}
	if scenario == "pause" {
		phase = interval - 200*time.Millisecond
	}
	if !healthy || scenario == "pause" {
		select {
		case <-time.After(phase):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if wrapped.stall {
			write(websocket.MessageBinary, "\x02\x00")
			awaitGatewaySignal(t, ctx, wrapped.entered, "Send blocked before fault")
		}
		if scenario != "downstream_blackhole" {
			up.set(true)
		}
		if scenario != "upstream_blackhole" {
			down.set(true)
		}
		trace.mark("fault", "relay forwarding disabled; connections remain open")
		if scenario == "pause" {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			up.set(false)
			down.set(false)
			trace.mark("resumed", "relay preserves buffered bytes and order")
		}
	}
	// Snapshot precedes the watchdog. EXP-010 uses 6s; EXP-011 extends the
	// window to cover its 8s candidate and at least two healthy 5s probes.
	select {
	case <-time.After(observation):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	trace.mark("observation_end", "before End, Abort or relay teardown")
	before, pingCounts := trace.snapshot()
	registeredBefore := len(g.registry.snapshot())
	reservedBefore := g.pool.Snapshot()[0].Reserved
	upBytes, downBytes := up.forwarded.Load(), down.forwarded.Load()
	upBlocked, downBlocked := up.blocked.Load(), down.blocked.Load()
	stopHB()
	heartbeatWG.Wait()
	controlSurvived := healthy
	for _, name := range []string{"gateway_decision", "client_read_exit", "gateway_heartbeat_failure", "client_heartbeat_failure"} {
		if _, ok := before[name]; ok {
			controlSurvived = false
		}
	}
	if controlSurvived {
		write(websocket.MessageText, `{"type":"end"}`)
	} else {
		trace.mark("watchdog_cleanup", "unblock relay and Abort only AFTER observation snapshot")
		up.set(false)
		down.set(false)
		s.abort()
		_ = conn.CloseNow()
	}
	awaitGatewaySignal(t, ctx, finished, "handler exit")
	awaitGatewaySignal(t, ctx, rpcCanceled, "RPC cancellation observation")
	awaitGatewaySignal(t, ctx, workerDone, "Worker exit")
	awaitGatewaySignal(t, ctx, readDone, "client Reader exit")
	if controlSurvived {
		gotFinal := false
		for len(results) > 0 {
			if <-results == "final" {
				gotFinal = true
			}
		}
		if !gotFinal {
			t.Fatal("control lost final result")
		}
	}
	registeredAfter, reservedAfter := len(g.registry.snapshot()), g.pool.Snapshot()[0].Reserved
	if registeredAfter != 0 || reservedAfter != 0 {
		t.Fatal("leaked Session or Worker reservation")
	}
	stopRelay()
	_ = front.Close()
	awaitGatewaySignal(t, ctx, relayDone, "relay goroutines exit")
	after, _ := trace.snapshot()
	caseName := scenario + "_" + mode
	if experiment == "EXP-011" {
		caseName = fmt.Sprintf("%s_i%d", scenario, interval.Milliseconds())
	}
	record := map[string]any{
		"experiment": experiment, "case": caseName, "scenario": scenario, "mode": mode,
		"interval_ms": interval.Milliseconds(), "ping_timeout_ms": 3000, "detection_target_ms": (interval + 3*time.Second).Milliseconds(),
		"observation_ms":        observation.Milliseconds(),
		"processing_timeout_ms": g.cfg.ProcessingTimeout.Milliseconds(), "fault_phase_ms": phase.Milliseconds(),
		"events_before_cleanup": before, "events": after, "successful_pings": pingCounts,
		"registered_before_cleanup": registeredBefore, "reserved_before_cleanup": reservedBefore,
		"registered_after_cleanup": registeredAfter, "reserved_after_cleanup": reservedAfter,
		"upstream_forwarded_bytes": upBytes, "downstream_forwarded_bytes": downBytes,
		"upstream_blocked_reads": upBlocked, "downstream_blocked_reads": downBlocked,
		"elapsed_ms": float64(time.Since(trace.epoch)) / float64(time.Millisecond),
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", encoded)
}

// Check the pause primitive without real sleeps or network access: it must hold
// data until released, and cancellation must wake it without a gate reopening.
func TestDisconnectGate(t *testing.T) {
	for _, cancelInstead := range []bool{false, true} {
		gate := newDisconnectGate()
		gate.set(true)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- gate.wait(ctx, 7) }()
		deadline := time.After(time.Second)
		for gate.blocked.Load() == 0 {
			select {
			case <-deadline:
				t.Fatal("gate not reached")
			default:
				time.Sleep(time.Millisecond)
			}
		}
		select {
		case <-done:
			t.Fatal("paused gate did not hold")
		default:
		}
		if gate.held.Load() != 7 {
			t.Fatal("missing bounded pending bytes")
		}
		if cancelInstead {
			cancel()
		} else {
			gate.set(false)
		}
		select {
		case err := <-done:
			if cancelInstead != errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected gate exit %v", err)
			}
		case <-deadline:
			t.Fatal("gate did not wake")
		}
		cancel()
		if gate.held.Load() != 0 {
			t.Fatal("gate retained held bytes")
		}
	}
}

// disconnectWriteSignal exposes the start of a net.Pipe Write. Without a peer
// Read that Write cannot complete, giving a deterministic in-flight Ping window.
type disconnectWriteSignal struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *disconnectWriteSignal) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

// Reproduce the candidate's shutdown failure independently of scheduler luck:
// canceling Ping while its frame is being written closes the WebSocket. Stopping
// future probes and joining the current one preserves a healthy connection.
func TestDisconnectHeartbeatStopDuringPing(t *testing.T) {
	for _, mode := range []string{"cancel_inflight", "stop_future"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			s, peer, _ := newDirectSession(t, ctx, &unusedGatewayWorker{})
			transport := s.transport.(*readSignalConn)
			entered := make(chan struct{})
			// No I/O goroutine has started; safely add observation to the test pipe.
			transport.Conn = &disconnectWriteSignal{Conn: transport.Conn, entered: entered}
			readerDone := make(chan error, 1)
			go func() {
				_, data, err := s.ws.Read(ctx)
				if err == nil && string(data) != "still open" {
					err = fmt.Errorf("unexpected payload %q", data)
				}
				readerDone <- err
			}()
			if mode == "cancel_inflight" {
				probe, cancelProbe := context.WithCancel(ctx)
				pingDone := make(chan error, 1)
				go func() { pingDone <- s.ws.Ping(probe) }()
				awaitGatewaySignal(t, ctx, entered, "Ping frame Write entered")
				cancelProbe()
				select {
				case err := <-pingDone:
					if err == nil {
						t.Fatal("canceled Ping succeeded")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case err := <-readerDone:
					if err == nil {
						t.Fatal("canceled Write left Reader alive")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if err := s.ws.Write(ctx, websocket.MessageText, []byte("End")); err == nil {
					t.Fatal("canceled control Write did not close connection")
				}
			} else {
				trace := &disconnectTrace{epoch: time.Now(), events: make(map[string]disconnectEvent), pings: make(map[string]int)}
				stop, done := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(done)
					disconnectHeartbeat(ctx, stop, s.ws, "gateway", trace, s.abort, time.Millisecond, time.Second)
				}()
				awaitGatewaySignal(t, ctx, entered, "candidate Ping frame Write entered")
				close(stop)
				opcode, payload := readRawServerFrame(t, peer)
				if opcode != 9 {
					t.Fatalf("want Ping, got %d", opcode)
				}
				writeRawClientFrame(t, peer, 10, payload)
				awaitGatewaySignal(t, ctx, done, "candidate joined without canceling active Ping")
				writeRawClientFrame(t, peer, 1, []byte("still open"))
				select {
				case err := <-readerDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				events, pings := trace.snapshot()
				if len(events) != 0 || pings["gateway"] != 1 {
					t.Fatalf("unexpected heartbeat outcome: %v %v", events, pings)
				}
			}
		})
	}
}
