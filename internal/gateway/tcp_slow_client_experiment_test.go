//go:build tide_tcp

package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// tcpTrace records real socket Write calls and RPC cancellation independently of
// Gateway decision logic. The mutex is never held across socket I/O.
type tcpTrace struct {
	mu                                     sync.Mutex
	epoch, writing, canceled, workerExited time.Time
	writeBytes                             int
	maxWrite                               time.Duration
	rpcCanceled, workerDone                chan struct{}
}

type tracedTCPConn struct {
	net.Conn
	trace *tcpTrace
}

func (c *tracedTCPConn) Write(p []byte) (int, error) {
	started := time.Now()
	c.trace.mu.Lock()
	c.trace.writing = started
	c.trace.writeBytes = len(p)
	c.trace.mu.Unlock()
	n, err := c.Conn.Write(p)
	c.trace.mu.Lock()
	c.trace.maxWrite = max(c.trace.maxWrite, time.Since(started))
	c.trace.writing = time.Time{}
	c.trace.mu.Unlock()
	return n, err
}

type tracedTCPListener struct {
	net.Listener
	trace *tcpTrace
}

func (l *tracedTCPListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tcp := conn.(*net.TCPConn)
	if err := tcp.SetWriteBuffer(4096); err != nil {
		_ = tcp.Close()
		return nil, err
	}
	return &tracedTCPConn{Conn: tcp, trace: l.trace}, nil
}

type tcpCancelClient struct {
	asrv1.ASRServiceClient
	trace *tcpTrace
}

func (c *tcpCancelClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	context.AfterFunc(ctx, func() {
		c.trace.mu.Lock()
		c.trace.canceled = time.Now()
		c.trace.mu.Unlock()
		close(c.trace.rpcCanceled)
	})
	return c.ASRServiceClient.StreamingRecognize(ctx, opts...)
}

type tcpLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *tcpLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *tcpLogBuffer) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

// TestExperimentTCPSlowClient runs only when explicitly opted in. TCP is loopback;
// the Worker transport remains bufconn so the downstream client is the variable.
func TestExperimentTCPSlowClient(t *testing.T) {
	if os.Getenv("TIDE_RUN_TCP_EXPERIMENTS") != "1" {
		t.Skip("explicit loopback TCP experiment opt-in required")
	}
	for _, name := range []string{"write_timeout", "processing_timeout", "end_timeout", "client_reset", "abort", "resume"} {
		t.Run(name, func(t *testing.T) { runTCPSlowClient(t, name) })
	}
}

func runTCPSlowClient(t *testing.T, name string) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	trace := &tcpTrace{rpcCanceled: make(chan struct{}), workerDone: make(chan struct{})}
	worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
		defer func() { trace.mu.Lock(); trace.workerExited = time.Now(); trace.mu.Unlock(); close(trace.workerDone) }()
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if name != "processing_timeout" {
			if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: req.AudioSeq}}); err != nil {
				return err
			}
		}
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{Text: strings.Repeat("x", 1024*1024)}); err != nil {
			return err
		}
		if name == "processing_timeout" {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		if _, err := stream.Recv(); err != io.EOF {
			return fmt.Errorf("expected half-close: %v", err)
		}
		return stream.Send(&asrv1.StreamingRecognizeResponse{Text: "final", IsFinal: true})
	}})
	cfg := Config{MaxSessions: 1}
	if name == "processing_timeout" || name == "end_timeout" || name == "abort" {
		cfg.ResultWriteTimeout = 8 * time.Second
	}
	logs := &tcpLogBuffer{}
	g, err := New(ctx, &tcpCancelClient{ASRServiceClient: worker, trace: trace}, slog.New(slog.NewJSONHandler(logs, nil)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	finished, serverDone := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(finished); g.ServeHTTP(w, r) })}
	go func() {
		defer close(serverDone)
		_ = server.Serve(&tracedTCPListener{Listener: listener, trace: trace})
	}()
	t.Cleanup(func() { g.Abort(); _ = server.Close(); _ = listener.Close(); <-serverDone })
	peer, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if err := peer.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	_ = peer.SetDeadline(deadline)
	_, err = fmt.Fprintf(peer, "GET /v1/asr HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(peer)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status %d", response.StatusCode)
	}
	trace.epoch = time.Now()
	writeRawClientFrame(t, peer, 1, []byte(`{"type":"start","version":"v1"}`))
	writeRawClientFrame(t, peer, 2, []byte{1, 0})
	if name != "processing_timeout" {
		writeRawClientFrame(t, peer, 1, []byte(`{"type":"end"}`))
	}
	prefix := make([]byte, 2)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		t.Fatal(err)
	}
	// A frame prefix alone is insufficient: require a real socket Write of at
	// least 4 KiB to remain in progress for 100 ms before injecting the action.
	blocked := time.Time{}
	check := time.NewTicker(10 * time.Millisecond)
	defer check.Stop()
	for blocked.IsZero() {
		select {
		case <-check.C:
			trace.mu.Lock()
			if !trace.writing.IsZero() && trace.writeBytes >= 4096 && time.Since(trace.writing) >= 100*time.Millisecond {
				blocked = time.Now()
			}
			trace.mu.Unlock()
		case <-trace.rpcCanceled:
			t.Fatal("RPC exited before socket blockage was established")
		case <-ctx.Done():
			t.Fatal("no actual blocked socket Write observed")
		}
	}
	actionAt := time.Time{}
	frames, bytesRead, closeCode := 0, 0, 0
	switch name {
	case "client_reset":
		actionAt = time.Now()
		_ = peer.SetLinger(0)
		_ = peer.Close()
	case "abort":
		actionAt = time.Now()
		g.Abort()
	case "resume":
		time.Sleep(400 * time.Millisecond) // Total paused interval is approximately 500 ms.
		actionAt = time.Now()
		source := io.MultiReader(bytes.NewReader(prefix), reader)
		for {
			opcode, payload, err := readTCPFrame(source)
			if err != nil {
				t.Fatal(err)
			}
			frames++
			bytesRead += len(payload)
			if opcode == 8 {
				if len(payload) < 2 {
					t.Fatal("missing close code")
				}
				closeCode = int(binary.BigEndian.Uint16(payload))
				writeRawClientFrame(t, peer, 8, payload)
				break
			}
		}
	}
	awaitGatewaySignal(t, ctx, trace.rpcCanceled, "RPC canceled")
	awaitGatewaySignal(t, ctx, finished, "Gateway handler exited")
	awaitGatewaySignal(t, ctx, trace.workerDone, "Worker handler exited")
	ended := time.Now()
	trace.mu.Lock()
	canceledAt, workerAt, maxWrite := trace.canceled, trace.workerExited, trace.maxWrite
	trace.mu.Unlock()
	cause := ""
	for _, line := range strings.Split(logs.text(), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) == nil && entry["msg"] == "session run ended" {
			if value, ok := entry["error"].(string); ok {
				cause = value
			}
		}
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	var actionMS, actionToCancel any
	if !actionAt.IsZero() {
		actionMS = ms(actionAt.Sub(trace.epoch))
		actionToCancel = ms(canceledAt.Sub(actionAt))
	}
	result := map[string]any{
		"experiment": "EXP-005", "case": name, "elapsed_ms": ms(ended.Sub(trace.epoch)), "socket_block_confirmed_ms": ms(blocked.Sub(trace.epoch)),
		"rpc_cancel_ms": ms(canceledAt.Sub(trace.epoch)), "rpc_cancel_to_cleanup_ms": ms(ended.Sub(canceledAt)), "worker_exit_ms": ms(workerAt.Sub(trace.epoch)),
		"action_ms": actionMS, "action_to_rpc_cancel_ms": actionToCancel, "socket_write_max_ms": ms(maxWrite),
		"result_write_timeout_ms": g.cfg.ResultWriteTimeout.Milliseconds(), "processing_timeout_ms": g.cfg.ProcessingTimeout.Milliseconds(), "end_timeout_ms": g.cfg.EndTimeout.Milliseconds(),
		"session_error": cause, "frames_read": frames, "payload_bytes_read": bytesRead, "observed_close_code": closeCode,
		"registered_after_cleanup": len(g.registry.snapshot()), "server_requested_send_buffer": 4096, "client_requested_receive_buffer": 1024, "result_text_bytes": 1024 * 1024,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", encoded)
	if len(g.registry.snapshot()) != 0 {
		t.Error("Session retained after handler exit")
	}
	if name == "resume" && (closeCode != 1000 || cause != "" || bytesRead < 1024*1024) {
		t.Errorf("resume did not complete: code=%d error=%s", closeCode, cause)
	}
	// Other failure causes are evidence, not hard-coded expected timing outcomes.
}

// readTCPFrame consumes unmasked server frames, including long and continuation
// frames. It bounds allocations; control frames are returned to the caller.
func readTCPFrame(reader io.Reader) (byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	if header[1]&0x80 != 0 {
		return 0, nil, fmt.Errorf("server frame unexpectedly masked")
	}
	size := uint64(header[1] & 127)
	if size == 126 {
		var b [2]byte
		if _, err := io.ReadFull(reader, b[:]); err != nil {
			return 0, nil, err
		}
		size = uint64(binary.BigEndian.Uint16(b[:]))
	}
	if size == 127 {
		var b [8]byte
		if _, err := io.ReadFull(reader, b[:]); err != nil {
			return 0, nil, err
		}
		size = binary.BigEndian.Uint64(b[:])
	}
	if size > 2*1024*1024 {
		return 0, nil, fmt.Errorf("unexpected frame size %d", size)
	}
	payload := make([]byte, int(size))
	_, err := io.ReadFull(reader, payload)
	return header[0] & 15, payload, err
}
