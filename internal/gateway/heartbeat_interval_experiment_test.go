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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// TestExperimentHeartbeatInterval compares detection at the same absolute fault
// phases. Pause controls instead straddle each candidate's first probe so both
// actually exercise a delayed Pong. No audio replay is implemented by this test.
func TestExperimentHeartbeatInterval(t *testing.T) {
	if os.Getenv("TIDE_RUN_HEARTBEAT_EXPERIMENTS") != "1" {
		t.Skip("explicit interval experiment opt-in required")
	}
	if !disconnectOverlayEnabled {
		t.Fatal("decision observation overlay required")
	}
	for _, scenario := range []string{"blackhole", "blackhole_late", "upstream_blackhole", "downstream_blackhole", "pause", "acked_idle"} {
		for _, interval := range []time.Duration{2 * time.Second, 5 * time.Second} {
			t.Run(fmt.Sprintf("%s_i%d", scenario, interval.Milliseconds()), func(t *testing.T) {
				window := 9 * time.Second
				if scenario == "pause" || scenario == "acked_idle" {
					window = 11 * time.Second
				}
				runDisconnectTimed(t, scenario, "heartbeat", "EXP-011", interval, window)
			})
		}
	}
}

// intervalWire counts bytes at the Gateway's WS transport boundary. Headers
// from TCP/IP are excluded; both client-masked and server-unmasked WS frames are
// included. Counter snapshots occur after setup and before closing handshakes.
type intervalWire struct {
	up, down atomic.Int64
}

type intervalCountConn struct {
	net.Conn
	wire *intervalWire
}

func (c *intervalCountConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.wire.up.Add(int64(n))
	return n, err
}

func (c *intervalCountConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.wire.down.Add(int64(n))
	return n, err
}

type intervalCountListener struct {
	net.Listener
	wire *intervalWire
}

func (l *intervalCountListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &intervalCountConn{Conn: c, wire: l.wire}, nil
}

// intervalWorkerClient binds an observation to each sequentially admitted
// Session while its handler is opening the RPC. The production client is shared.
type intervalWorkerClient struct {
	asrv1.ASRServiceClient
	bind func()
}

func (c *intervalWorkerClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	c.bind()
	return c.ASRServiceClient.StreamingRecognize(ctx, opts...)
}

// intervalPeer owns one experiment client's Read loop and heartbeat observation.
// A retained active Session is not an assertion of compute capacity: no audio is
// sent during this idle control-path workload.
type intervalPeer struct {
	conn      *websocket.Conn
	session   *session
	trace     *disconnectTrace
	done      chan struct{}
	final     atomic.Bool
	closeCode atomic.Int64
}

// intervalCPUSeconds is process-wide user + system CPU time, not Gateway-only
// time. The test process contains clients, Gateway, gRPC Worker and observers.
func intervalCPUSeconds(t *testing.T) float64 {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

// TestExperimentHeartbeatOverhead includes a no-heartbeat control. All endpoints
// start probes together to expose a repeatable timer burst; this is not a claim
// about a randomly staggered deployment. 21s avoids stopping at a 2s/5s tick.
func TestExperimentHeartbeatOverhead(t *testing.T) {
	if os.Getenv("TIDE_RUN_HEARTBEAT_EXPERIMENTS") != "1" {
		t.Skip("explicit overhead experiment opt-in required")
	}
	counts := []int{64, 256}
	window := 21 * time.Second
	if value := os.Getenv("TIDE_HEARTBEAT_CONNECTIONS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 1024 {
			t.Fatal("invalid connection override")
		}
		counts = []int{n}
	}
	if value := os.Getenv("TIDE_HEARTBEAT_WINDOW_MS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			t.Fatal("invalid observation override")
		}
		window = time.Duration(n) * time.Millisecond
	}
	for _, n := range counts {
		for _, interval := range []time.Duration{0, 2 * time.Second, 5 * time.Second} {
			t.Run(fmt.Sprintf("n%d_i%d", n, interval.Milliseconds()), func(t *testing.T) { runHeartbeatOverhead(t, n, interval, window) })
		}
	}
}

func runHeartbeatOverhead(t *testing.T, n int, interval, window time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), window+45*time.Second)
	defer cancel()
	var activeRPC atomic.Int64
	started := make(chan struct{}, n)
	var workerWG sync.WaitGroup
	worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
		workerWG.Add(1)
		defer workerWG.Done()
		activeRPC.Add(1)
		defer activeRPC.Add(-1)
		started <- struct{}{}
		_, err := stream.Recv()
		if !errors.Is(err, io.EOF) {
			return err
		}
		return stream.Send(&asrv1.StreamingRecognizeResponse{Text: "final", IsFinal: true})
	}})
	bound := make(chan *session, 1)
	seen := make(map[*session]bool)
	var seenMu sync.Mutex
	var g *Gateway
	wrapper := &intervalWorkerClient{ASRServiceClient: worker, bind: func() {
		seenMu.Lock()
		defer seenMu.Unlock()
		for _, s := range g.registry.snapshot() {
			if !seen[s] {
				seen[s] = true
				bound <- s
				return
			}
		}
	}}
	var err error
	g, err = New(ctx, wrapper, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{Heartbeat: wsheartbeat.Config{Disabled: true}, MaxSessions: n})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wire := &intervalWire{}
	var handlerWG sync.WaitGroup
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerWG.Add(1)
		defer handlerWG.Done()
		g.ServeHTTP(w, r)
	})}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		_ = server.Serve(&intervalCountListener{Listener: listener, wire: wire})
	}()
	t.Cleanup(func() { g.Abort(); _ = server.Close(); _ = listener.Close(); <-serverDone })
	var readerWG sync.WaitGroup
	peers := make([]*intervalPeer, 0, n)
	for range n {
		conn, _, err := websocket.Dial(ctx, "ws://"+listener.Addr().String(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.CloseNow() })
		p := &intervalPeer{conn: conn, done: make(chan struct{}), trace: &disconnectTrace{epoch: time.Now(), events: make(map[string]disconnectEvent), pings: make(map[string]int)}}
		readerWG.Go(func() {
			defer close(p.done)
			for {
				_, data, err := conn.Read(ctx)
				if err != nil {
					p.closeCode.Store(int64(websocket.CloseStatus(err)))
					return
				}
				var result struct {
					Text  string `json:"text"`
					Final bool   `json:"isFinal"`
				}
				if json.Unmarshal(data, &result) == nil && result.Text == "final" && result.Final {
					p.final.Store(true)
				}
			}
		})
		if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
			t.Fatal(err)
		}
		select {
		case p.session = <-bound:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		awaitGatewaySignal(t, ctx, started, "idle Worker RPC established")
		// One-off warmup, identical in all groups and outside measurement.
		for _, c := range []*websocket.Conn{conn, p.session.ws} {
			probe, stop := context.WithTimeout(ctx, 3*time.Second)
			err := c.Ping(probe)
			stop()
			if err != nil {
				t.Fatal(err)
			}
		}
		peers = append(peers, p)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	baseGoroutines := runtime.NumGoroutine()
	stopHB, startHB := make(chan struct{}), make(chan struct{})
	var stopOnce sync.Once
	stopProbes := func() { stopOnce.Do(func() { close(stopHB) }) }
	var heartbeats sync.WaitGroup
	defer func() { stopProbes(); heartbeats.Wait() }()
	if interval > 0 {
		for _, p := range peers {
			heartbeats.Go(func() {
				select {
				case <-startHB:
				case <-stopHB:
					return
				}
				disconnectHeartbeat(ctx, stopHB, p.conn, "client", p.trace, func() { _ = p.conn.CloseNow() }, interval, 3*time.Second)
			})
			heartbeats.Go(func() {
				select {
				case <-startHB:
				case <-stopHB:
					return
				}
				disconnectHeartbeat(p.session.ctx, stopHB, p.session.ws, "gateway", p.trace, p.session.abort, interval, 3*time.Second)
			})
		}
	}
	upBefore, downBefore := wire.up.Load(), wire.down.Load()
	cpuBefore := intervalCPUSeconds(t)
	start := time.Now()
	close(startHB)
	select {
	case <-time.After(window):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	elapsed := time.Since(start)
	cpu := intervalCPUSeconds(t) - cpuBefore
	upBytes, downBytes := wire.up.Load()-upBefore, wire.down.Load()-downBefore
	runtime.ReadMemStats(&after)
	liveGoroutines := runtime.NumGoroutine()
	registered, reserved, rpcs := len(g.registry.snapshot()), g.pool.Snapshot()[0].Reserved, activeRPC.Load()
	pingCounts := map[string]int{"client": 0, "gateway": 0}
	heartbeatFailures, earlyExit := 0, 0
	for _, p := range peers {
		events, pings := p.trace.snapshot()
		heartbeatFailures += len(events)
		for k, v := range pings {
			pingCounts[k] += v
		}
		select {
		case <-p.done:
			earlyExit++
		default:
		}
	}
	stopProbes()
	heartbeats.Wait()
	// End and closing traffic are outside the observation. Preserve premature
	// failures as observations instead of replacing them with successful probes.
	for _, p := range peers {
		select {
		case <-p.done:
			continue
		default:
		}
		if err := p.conn.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
			_ = p.conn.CloseNow()
		}
	}
	for _, p := range peers {
		awaitGatewaySignal(t, ctx, p.done, "client close after idle workload")
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	handlerWG.Wait()
	readerWG.Wait()
	workerWG.Wait()
	if len(g.registry.snapshot()) != 0 || g.pool.Snapshot()[0].Reserved != 0 || activeRPC.Load() != 0 {
		t.Fatal("idle workload retained resources")
	}
	normal := 0
	for _, p := range peers {
		if p.final.Load() && p.closeCode.Load() == int64(websocket.StatusNormalClosure) {
			normal++
		}
	}
	record := map[string]any{
		"experiment": "EXP-011", "kind": "overhead", "case": fmt.Sprintf("n%d_i%d", n, interval.Milliseconds()),
		"connections": n, "interval_ms": interval.Milliseconds(), "ping_timeout_ms": 3000, "window_ms": window.Milliseconds(), "observed_ms": float64(elapsed) / float64(time.Millisecond),
		"cpu_seconds": cpu, "cpu_mean_cores": cpu / elapsed.Seconds(), "upstream_bytes": upBytes, "downstream_bytes": downBytes,
		"successful_pings": pingCounts, "heartbeat_failures": heartbeatFailures, "early_client_exits": earlyExit, "normal_complete": normal,
		"registered_at_window_end": registered, "reserved_at_window_end": reserved, "active_rpc_at_window_end": rpcs,
		"goroutines_before_probes": baseGoroutines, "goroutines_at_window_end": liveGoroutines,
		"heap_before": before.HeapAlloc, "heap_at_window_end": after.HeapAlloc, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "gc_cycles": after.NumGC - before.NumGC,
		"registered_after": len(g.registry.snapshot()), "reserved_after": g.pool.Snapshot()[0].Reserved, "active_rpc_after": activeRPC.Load(),
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", encoded)
}
