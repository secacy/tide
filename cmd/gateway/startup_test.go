package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// startupBackend 标记后端身份并回显音频，统计真实 gRPC 连接和 stream。
// ConnEnd 在 Gateway 进程仍存活、测试服务端未停止时用于验证客户端主动关闭连接。
type startupBackend struct {
	asrv1.UnimplementedASRServiceServer
	id                      string
	opened, closed, streams atomic.Int64
	exits                   chan error
}

func (w *startupBackend) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (w *startupBackend) HandleRPC(context.Context, stats.RPCStats)                       {}
func (w *startupBackend) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (w *startupBackend) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		w.opened.Add(1)
	case *stats.ConnEnd:
		w.closed.Add(1)
	}
}
func (w *startupBackend) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) (err error) {
	w.streams.Add(1)
	defer func() { w.exits <- err }()
	var processed uint64
	for {
		req, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: w.id, Text: w.id + "/tail", IsFinal: true})
		}
		if err != nil {
			return err
		}
		processed += uint64(len(req.GetData()))
		if err := s.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: processed}}); err != nil {
			return err
		}
		if err := s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: w.id, Text: w.id + "/" + string(req.GetData())}); err != nil {
			return err
		}
	}
}
func startStartupBackend(t *testing.T, id string) (string, *startupBackend) {
	t.Helper()
	w := &startupBackend{id: id, exits: make(chan error, 16)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.StatsHandler(w))
	asrv1.RegisterASRServiceServer(server, w)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		listener.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("test backend did not stop")
		}
	})
	return listener.Addr().String(), w
}

// startupLogSignal 等待本次 run 自己完成监听，避免把其他进程的 healthz 当作启动成功。
// 测试串行替换标准 logger 的 writer，清理 run 后恢复原 writer。
type startupLogSignal struct {
	writer io.Writer
	ready  chan struct{}
	once   sync.Once
}

func (w *startupLogSignal) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if strings.Contains(string(p), "websocket gateway listening on ") {
		w.once.Do(func() { close(w.ready) })
	}
	return n, err
}

func startupWrite(t *testing.T, ctx context.Context, conn *websocket.Conn, kind websocket.MessageType, data string) {
	t.Helper()
	if err := conn.Write(ctx, kind, []byte(data)); err != nil {
		t.Fatal(err)
	}
}
func startupResult(t *testing.T, ctx context.Context, conn *websocket.Conn, worker, text string, final bool) {
	t.Helper()
	kind, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	want := wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: worker, Text: worker + "/" + text, IsFinal: final}
	if kind != websocket.MessageText || got != want {
		t.Fatalf("result=%+v kind=%v want=%+v", got, kind, want)
	}
}
func startupExit(t *testing.T, ctx context.Context, w *startupBackend, want codes.Code) {
	t.Helper()
	select {
	case err := <-w.exits:
		if status.Code(err) != want {
			t.Fatalf("Worker %s exit=%v want=%v", w.id, err, want)
		}
	case <-ctx.Done():
		t.Fatalf("Worker %s did not exit", w.id)
	}
}

// TestGatewayMultiWorkerStartup 调用真实 parseGatewayConfig -> run -> WebSocket -> gRPC 链路。
// HTTP 地址目前固定 :8080；端口被占用时明确跳过，不关闭或使用用户已有服务。
// 后端始终保持运行，先断言 ClientConn 关闭，再执行测试服务端清理。
func TestGatewayMultiWorkerStartup(t *testing.T) {
	reservation, err := net.Listen("tcp", gatewayAddr)
	if err != nil {
		t.Skipf("startup integration requires free %s: %v", gatewayAddr, err)
	}
	reservation.Close()
	addrA, a := startStartupBackend(t, "A")
	addrB, b := startStartupBackend(t, "B")
	cfg, err := parseGatewayConfig([]string{"-workers= " + addrB + " , " + addrA + " ", "-max-pending-audio-bytes=32000"})
	if err != nil {
		t.Fatal(err)
	}
	previous := log.Writer()
	signal := &startupLogSignal{writer: previous, ready: make(chan struct{})}
	log.SetOutput(signal)
	t.Cleanup(func() { log.SetOutput(previous) })
	serviceCtx, stop := context.WithCancel(context.Background())
	finished := make(chan struct{})
	var runErr error
	go func() { defer close(finished); runErr = run(serviceCtx, cfg) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("Gateway run did not stop")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	select {
	case <-signal.ready:
	case <-finished:
		t.Fatalf("startup failed: %v", runErr)
	case <-ctx.Done():
		t.Fatal("startup timed out")
	}
	// NewClient 本身不建连，监听就绪不表示后端已连接。
	if a.opened.Load() != 0 || b.opened.Load() != 0 {
		t.Fatal("backend connected before first RPC")
	}
	backends := []*startupBackend{b, a}
	var held []*websocket.Conn
	for i := range 6 {
		w := backends[i%2]
		conn, _, err := websocket.Dial(ctx, "ws://127.0.0.1:8080/v1/asr", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.CloseNow() })
		startupWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
		data := fmt.Sprintf("session-%d:first", i)
		startupWrite(t, ctx, conn, websocket.MessageBinary, data)
		startupResult(t, ctx, conn, w.id, data, false)
		if i >= 4 {
			held = append(held, conn)
			continue
		}
		data = fmt.Sprintf("session-%d:second", i)
		startupWrite(t, ctx, conn, websocket.MessageBinary, data)
		startupResult(t, ctx, conn, w.id, data, false)
		startupWrite(t, ctx, conn, websocket.MessageText, `{"type":"end"}`)
		startupResult(t, ctx, conn, w.id, "tail", true)
		_, _, err = conn.Read(ctx)
		if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
			t.Fatalf("normal close: %v", err)
		}
		startupExit(t, ctx, w, codes.OK)
	}
	for _, w := range backends {
		if w.streams.Load() != 3 || w.opened.Load() != 1 || w.closed.Load() != 0 {
			t.Fatalf("Worker %s streams=%d opened=%d closed=%d", w.id, w.streams.Load(), w.opened.Load(), w.closed.Load())
		}
	}
	stop()
	for _, conn := range held {
		_, _, err := conn.Read(ctx)
		if websocket.CloseStatus(err) != websocket.StatusGoingAway {
			t.Fatalf("shutdown close: %v", err)
		}
	}
	select {
	case <-finished:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-ctx.Done():
		t.Fatal("Gateway did not return after stop")
	}
	for _, w := range backends {
		startupExit(t, ctx, w, codes.Canceled)
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for a.closed.Load() != 1 || b.closed.Load() != 1 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("ClientConn cleanup incomplete: A=%d/%d B=%d/%d", a.closed.Load(), a.opened.Load(), b.closed.Load(), b.opened.Load())
		}
	}
	t.Log("routing=B,A,B,A,B,A; normal=4 canceled=2; connections per Worker opened=1 closed=1 before backend shutdown")
}

// TestGatewayStartupCLI 使用真实可执行程序检查帮助和配置错误退出，不启动健康探测。
func TestGatewayStartupCLI(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "gateway")
	buildCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, tc := range []struct {
		name string
		args []string
		code int
		hint string
	}{
		{"help", []string{"-h"}, 0, "-workers"},
		{"long_help", []string{"-help"}, 0, "-workers"},
		{"empty", []string{"-workers="}, 1, "worker address must not be empty"},
		{"duplicate", []string{"-workers=localhost:50051,localhost:50051"}, 1, "duplicate worker address"},
		{"signed_port", []string{"-workers=localhost:+50051"}, 1, "invalid worker address"},
		{"trailing_empty", []string{"-workers=localhost:50051,"}, 1, "worker address must not be empty"},
		{"unknown", []string{"-worker=localhost:50051"}, 1, "flag provided but not defined"},
		{"budget_rejected_after_parse", []string{"-workers=localhost:50052,localhost:50051", "-max-pending-audio-bytes=-1"}, 1, "create gateway"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, binary, tc.args...).CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("command did not exit: %v\n%s", ctx.Err(), output)
			}
			code := 0
			if err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				code = exit.ExitCode()
			}
			if code != tc.code || !strings.Contains(string(output), tc.hint) {
				t.Fatalf("code=%d want=%d output=%s", code, tc.code, output)
			}
			if strings.Contains(string(output), "websocket gateway listening") {
				t.Fatalf("invalid/help command started HTTP: %s", output)
			}
			if tc.name == "budget_rejected_after_parse" {
				if !strings.Contains(string(output), "strategy=round_robin") || !strings.Contains(string(output), "[localhost:50052 localhost:50051]") || strings.Contains(string(output), "!BADKEY") {
					t.Fatalf("configuration log lost order or has invalid slog fields: %s", output)
				}
			} else if strings.Contains(string(output), "gateway backend configuration") {
				t.Fatalf("logged backend configuration before successful parse: %s", output)
			}
		})
	}
}
