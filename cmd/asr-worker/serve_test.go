package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// lifecycleListener 记录监听器已关闭，供测试同步停止接入，而非依赖 sleep 猜测。
type lifecycleListener struct {
	net.Listener
	closed     chan struct{}
	once       sync.Once
	accepted   chan struct{}
	acceptOnce sync.Once
}

func (l *lifecycleListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.accepted) })
	return l.Listener.Accept()
}

func (l *lifecycleListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func newLifecycleListener(t *testing.T) *lifecycleListener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w := &lifecycleListener{Listener: l, closed: make(chan struct{}), accepted: make(chan struct{})}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func awaitLifecycle(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("lifecycle event did not complete")
	}
}

func lifecycleResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(8 * time.Second):
		t.Fatal("lifecycle task did not finish")
		return nil
	}
}

// TestRunListenerRollback 让两个服务争用同一实际端口，HTTP 必须失败且释放先取得的 gRPC 端口。
func TestRunListenerRollback(t *testing.T) {
	for _, mode := range []string{"conflict", "invalid_http_address", "already_canceled"} {
		t.Run(mode, func(t *testing.T) {
			probe := newLifecycleListener(t)
			address := probe.Addr().String()
			if err := probe.Close(); err != nil {
				t.Fatal(err)
			}
			cfg := expectedWorkerConfig()
			cfg.ListenAddr, cfg.DebugListenAddr = address, address
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "invalid_http_address" {
				cfg.DebugListenAddr = "invalid"
			}
			if mode == "already_canceled" {
				cancel()
			}
			err := run(ctx, cfg)
			if mode == "already_canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("run=%v", err)
				}
			} else {
				var networkErr *net.OpError
				if !errors.As(err, &networkErr) || !strings.Contains(err.Error(), "debug HTTP") {
					t.Fatalf("lost HTTP bind failure: %v", err)
				}
			}
			rebound, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatalf("gRPC listener leaked: %v", err)
			}
			rebound.Close()
		})
	}
}

// acceptFailureListener 用确定的失败原因触发 Serve 返回，不修改生产代码。
type acceptFailureListener struct {
	*lifecycleListener
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
}

func (l *acceptFailureListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.entered) })
	select {
	case <-l.release:
	case <-l.closed:
	}
	return nil, l.err
}

// TestServeWorkerFailureStopsPeer 真实 Accept 错误及未协调的 Stop/Close 都必须触发另一服务清理。
func TestServeWorkerFailureStopsPeer(t *testing.T) {
	for _, mode := range []string{"grpc_accept_error", "http_accept_error", "grpc_unexpected_stop", "http_unexpected_close", "pre_canceled"} {
		t.Run(mode, func(t *testing.T) {
			gl, hl := newLifecycleListener(t), newLifecycleListener(t)
			gs, hs := grpc.NewServer(), &http.Server{Handler: http.NotFoundHandler()}
			defer gs.Stop()
			defer hs.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := errors.New("injected accept failure")
			failed := &acceptFailureListener{lifecycleListener: gl, entered: make(chan struct{}), release: make(chan struct{}), err: cause}
			var grpcL, httpL net.Listener = gl, hl
			if mode == "grpc_accept_error" {
				grpcL = failed
			}
			if mode == "http_accept_error" {
				failed.lifecycleListener = hl
				httpL = failed
			}
			if mode == "pre_canceled" {
				cancel()
			}
			if mode == "grpc_unexpected_stop" {
				gs.Stop()
			}
			if mode == "http_unexpected_close" {
				hs.Close()
			}
			done := make(chan error, 1)
			go func() { done <- serveWorker(ctx, gs, grpcL, hs, httpL) }()
			if strings.HasSuffix(mode, "accept_error") {
				awaitLifecycle(t, failed.entered)
				if mode == "grpc_accept_error" {
					awaitLifecycle(t, hl.accepted)
				} else {
					awaitLifecycle(t, gl.accepted)
				}
				close(failed.release)
			}
			err := lifecycleResult(t, done)
			switch {
			case mode == "pre_canceled":
				if err != nil {
					t.Fatal(err)
				}
			case strings.HasSuffix(mode, "accept_error"):
				if !errors.Is(err, cause) {
					t.Fatalf("lost cause: %v", err)
				}
			default:
				if err == nil || !strings.Contains(err.Error(), "unexpectedly") {
					t.Fatalf("unexpected stop accepted: %v", err)
				}
			}
			// Serve 在启动前发现服务器已关闭时未必接管监听器，run 的 defer 承担兜底。
			// 两个正常启动且发生 Accept 失败的场景必须完成双方监听清理。
			if strings.HasSuffix(mode, "accept_error") {
				awaitLifecycle(t, gl.closed)
				awaitLifecycle(t, hl.closed)
			}
		})
	}
}

// shutdownRPC 用真实 gRPC stream 控制正常完成或等待 RPC context 取消。
type shutdownRPC struct {
	asrv1.UnimplementedASRServiceServer
	entered, release, exited chan struct{}
}

func (s *shutdownRPC) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	close(s.entered)
	defer close(s.exited)
	select {
	case <-s.release:
		return stream.Send(&asrv1.StreamingRecognizeResponse{Text: "tail", IsFinal: true})
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

// TestShutdownWorkerServers 同时保持一条 RPC 和一个 HTTP 请求，验证共享收尾窗口和强制关闭。
// 通过关闭监听器的同步点释放短请求；期限用小值注入，不修改生产五秒配置。
func TestShutdownWorkerServers(t *testing.T) {
	for _, mode := range []string{"natural", "force_grpc", "force_http", "force_both"} {
		t.Run(mode, func(t *testing.T) {
			gl, hl := newLifecycleListener(t), newLifecycleListener(t)
			backend := &shutdownRPC{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
			gs := grpc.NewServer()
			asrv1.RegisterASRServiceServer(gs, backend)
			defer gs.Stop()
			grpcDone := make(chan error, 1)
			go func() { grpcDone <- gs.Serve(gl) }()
			httpEntered, httpRelease, httpExited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			hs := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(httpEntered)
				defer close(httpExited)
				select {
				case <-httpRelease:
					_, _ = io.WriteString(w, "tail")
				case <-r.Context().Done():
				}
			})}
			defer hs.Close()
			httpDone := make(chan error, 1)
			go func() { httpDone <- hs.Serve(hl) }()
			cc, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := asrv1.NewASRServiceClient(cc).StreamingRecognize(ctx, grpc.WaitForReady(true))
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			awaitLifecycle(t, backend.entered)
			clientDone := make(chan error, 1)
			go func() {
				client := &http.Client{Timeout: 5 * time.Second}
				defer client.CloseIdleConnections()
				resp, err := client.Get("http://" + hl.Addr().String())
				if err == nil {
					body, readErr := io.ReadAll(resp.Body)
					resp.Body.Close()
					err = readErr
					if err == nil && string(body) != "tail" {
						err = errors.New("HTTP tail missing")
					}
				}
				clientDone <- err
			}()
			awaitLifecycle(t, httpEntered)
			budget := 80 * time.Millisecond
			if mode == "natural" {
				budget = 2 * time.Second
			}
			stopped := make(chan error, 1)
			go func() { stopped <- shutdownWorkerServers(gs, hs, budget) }()
			awaitLifecycle(t, gl.closed)
			awaitLifecycle(t, hl.closed)
			forceGRPC := mode == "force_grpc" || mode == "force_both"
			forceHTTP := mode == "force_http" || mode == "force_both"
			if !forceGRPC {
				close(backend.release)
			}
			if !forceHTTP {
				close(httpRelease)
			}
			err = lifecycleResult(t, stopped)
			if mode == "natural" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost deadline: %v", err)
			}
			if forceGRPC && !strings.Contains(err.Error(), "gRPC") {
				t.Fatalf("missing gRPC failure: %v", err)
			}
			if forceHTTP && !strings.Contains(err.Error(), "HTTP") {
				t.Fatalf("missing HTTP failure: %v", err)
			}
			awaitLifecycle(t, backend.exited)
			awaitLifecycle(t, httpExited)
			if err := lifecycleResult(t, grpcDone); err != nil {
				t.Fatalf("gRPC serve: %v", err)
			}
			if err := lifecycleResult(t, httpDone); !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("HTTP serve: %v", err)
			}
			if err := lifecycleResult(t, clientDone); (err != nil) != forceHTTP {
				t.Fatalf("HTTP client=%v force=%v", err, forceHTTP)
			}
			result, err := stream.Recv()
			if forceGRPC {
				if err == nil || errors.Is(err, io.EOF) {
					t.Fatalf("interrupted RPC reported completion: %v %v", result, err)
				}
			} else {
				if err != nil || !result.GetIsFinal() || result.GetText() != "tail" {
					t.Fatalf("tail lost: %v %v", result, err)
				}
				if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("RPC end: %v", err)
				}
			}
		})
	}
}

// waitProcessState 从真实 HTTP 查询等待 Worker 占用和等待计数；失败或未采集不当作零。
func waitProcessState(t *testing.T, address string, wantInUse, wantWaiting int) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		// 使用正式客户端查询真实 Worker 路由，协议错误和网络错误都不伪造零计数。
		value, err := loadgen.FetchWorkerSnapshot(context.Background(), client, "http://"+address+"/debug/worker")
		if err == nil && value.ProcessingLimitEnabled && value.Processing.Limit == 1 && value.Processing.InUse == wantInUse && value.Processing.Waiting == wantWaiting {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("Worker did not report in_use=%d waiting=%d", wantInUse, wantWaiting)
		case <-tick.C:
		}
	}
}

// verifyWorkerProcessLifecycle 复用一次真实二进制构建，验证默认/双服务信号、忙碌快照和五秒强制退出。
func verifyWorkerProcessLifecycle(t *testing.T, binary string) {
	for _, tc := range []struct {
		name        string
		debug, busy bool
		signal      os.Signal
	}{
		{"grpc_term", false, false, syscall.SIGTERM},
		{"debug_term", true, false, syscall.SIGTERM},
		{"debug_interrupt", true, false, os.Interrupt},
		{"busy_term", true, true, syscall.SIGTERM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"-processing-concurrency=1", "-response-delay=0s"}
			if tc.debug {
				args = append(args, "-debug-listen=127.0.0.1:0")
			}
			if tc.busy {
				args = append(args, "-processing-delay=1h")
			}
			p := startWorkerProcess(t, binary, args...)
			if (p.debugAddress != "") != tc.debug {
				t.Fatalf("debug address missing/unexpected: %s", p.startLog)
			}
			if tc.debug {
				waitProcessState(t, p.debugAddress, 0, 0)
			}
			if !tc.busy {
				verifyWorkerRPC(t, p.address)
			}
			var stream asrv1.ASRService_StreamingRecognizeClient
			if tc.busy {
				cc, err := grpc.NewClient(p.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				defer cc.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
				defer cancel()
				stream, err = asrv1.NewASRServiceClient(cc).StreamingRecognize(ctx, grpc.WaitForReady(true))
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 3200)}); err != nil {
					t.Fatal(err)
				}
				waitProcessState(t, p.debugAddress, 1, 0)
			}
			if err := p.process.Signal(tc.signal); err != nil {
				t.Fatal(err)
			}
			select {
			case <-p.done:
			case <-time.After(9 * time.Second):
				t.Fatal("signal did not stop Worker")
			}
			if tc.busy {
				var exit *exec.ExitError
				if !errors.As(p.err, &exit) || exit.ExitCode() != 1 {
					t.Fatalf("forced exit=%v", p.err)
				}
				if _, err := stream.Recv(); err == nil || errors.Is(err, io.EOF) {
					t.Fatalf("forced stream reported normal completion: %v", err)
				}
			} else if p.err != nil {
				t.Fatalf("normal signal exit: %v", p.err)
			}
			for _, address := range []string{p.address, p.debugAddress} {
				if address == "" {
					continue
				}
				conn, err := net.DialTimeout("tcp", address, time.Second)
				if err == nil {
					conn.Close()
					t.Fatalf("port still accepts after exit: %s", address)
				}
			}
		})
	}
}

// TestShutdownMockWorkerReleasesSlots 通过真实双向 RPC 构造一个处理者和一个等待者，
// 强制停止后确认 RPC handler 已退出，名额池计数归零。
func TestShutdownMockWorkerReleasesSlots(t *testing.T) {
	worker, err := mockasr.New(mockasr.Config{ProcessingConcurrency: 1, ProcessingDelay: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	gl, hl := newLifecycleListener(t), newLifecycleListener(t)
	gs := grpc.NewServer()
	asrv1.RegisterASRServiceServer(gs, worker)
	defer gs.Stop()
	hs := &http.Server{Handler: routes(worker)}
	defer hs.Close()
	gDone, hDone := make(chan error, 1), make(chan error, 1)
	go func() { gDone <- gs.Serve(gl) }()
	go func() { hDone <- hs.Serve(hl) }()
	cc, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var streams []asrv1.ASRService_StreamingRecognizeClient
	for range 2 {
		stream, err := asrv1.NewASRServiceClient(cc).StreamingRecognize(ctx, grpc.WaitForReady(true))
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 3200)}); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream)
	}
	waitProcessState(t, hl.Addr().String(), 1, 1)
	if err := shutdownWorkerServers(gs, hs, 50*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected forced shutdown: %v", err)
	}
	if err := lifecycleResult(t, gDone); err != nil {
		t.Fatal(err)
	}
	if err := lifecycleResult(t, hDone); !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
	for _, stream := range streams {
		if _, err := stream.Recv(); err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("interrupted audio reported completed: %v", err)
		}
	}
	got, enabled := worker.ProcessingSnapshot()
	if !enabled || got != (mockasr.ProcessingSnapshot{Limit: 1}) {
		t.Fatalf("slots not released: %+v enabled=%v", got, enabled)
	}
}
