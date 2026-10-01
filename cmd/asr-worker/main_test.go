package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestRunStartupErrors 验证配置错误先于监听错误，并保留可识别的原始错误。
func TestRunStartupErrors(t *testing.T) {
	t.Run("config_before_listen", func(t *testing.T) {
		cfg := expectedWorkerConfig()
		cfg.ListenAddr = "not-a-tcp-address"
		cfg.Mock.ProcessingConcurrency = -1
		err := run(cfg)
		if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "create mock ASR worker") {
			t.Fatalf("configuration was not rejected before listen: %v", err)
		}
	})
	t.Run("occupied_address", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		cfg := expectedWorkerConfig()
		cfg.ListenAddr = listener.Addr().String()
		err = run(cfg)
		var networkErr *net.OpError
		if !errors.As(err, &networkErr) || !strings.Contains(err.Error(), "listen on "+cfg.ListenAddr) {
			t.Fatalf("lost listen context or cause: %v", err)
		}
	})
}

// TestWorkerExecutable 编译真实入口，检查退出语义与双实例 RPC；不使用 go run 包装进程。
// 构建产物和进程日志均位于测试临时目录，每个运行中的子进程在测试结束时取消并回收。
func TestWorkerExecutable(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "asr-worker")
	buildCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, tc := range []struct {
		name string
		args []string
		code int
		hint string
	}{
		{"help", []string{"-h"}, 0, "Usage of asr-worker"},
		{"long_help", []string{"-help"}, 0, "Usage of asr-worker"},
		{"unknown", []string{"-unknown=1"}, 1, "flag provided but not defined"},
		{"negative_processing", []string{"-processing-delay=-1ms"}, 1, "processing-delay must be >= 0"},
		{"negative_response", []string{"-response-delay=-1ms"}, 1, "response-delay must be >= 0"},
		{"blank_listen", []string{"-listen= "}, 1, "missing listen address"},
		{"negative_concurrency_before_listen", []string{"-listen=invalid", "-processing-concurrency=-1"}, 1, "create mock ASR worker"},
		{"malformed_address", []string{"-listen=invalid"}, 1, "listen on invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, tc.args...)
			output, err := cmd.CombinedOutput()
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
				t.Fatalf("exit=%d want=%d output=%s", code, tc.code, output)
			}
			if strings.Contains(string(output), "mock ASR worker started") {
				t.Fatalf("unexpected startup: %s", output)
			}
			if tc.code == 0 && strings.Contains(string(output), "level=ERROR") {
				t.Fatalf("help logged as failure: %s", output)
			}
			if tc.name == "help" || tc.name == "long_help" {
				// 复用真实命令帮助检查，确认新增参数说明包含协议、关闭方式和本机示例。
				for _, hint := range []string{"-debug-listen", "HTTP", "empty disables", "127.0.0.1:50081"} {
					if !strings.Contains(string(output), hint) {
						t.Errorf("help missing %q: %s", hint, output)
					}
				}
			}
		})
	}
	t.Run("two_workers", func(t *testing.T) {
		first := startWorkerProcess(t, binary, "-processing-concurrency=1", "-processing-delay=10ms", "-response-delay=0s")
		second := startWorkerProcess(t, binary, "-processing-concurrency=2", "-processing-delay=20ms", "-response-delay=5ms")
		if first.address == second.address {
			t.Fatal("Workers share the same listening address")
		}
		for i, p := range []*workerProcess{first, second} {
			for _, field := range [][]string{
				{"processing_concurrency=1", "processing_delay=10ms", "response_delay=0s"},
				{"processing_concurrency=2", "processing_delay=20ms", "response_delay=5ms"},
			}[i] {
				if !strings.Contains(p.startLog, field) {
					t.Errorf("missing %s in startup log: %s", field, p.startLog)
				}
			}
			verifyWorkerRPC(t, p.address)
			select {
			case <-p.done:
				t.Fatalf("Worker exited during RPC: %v", p.err)
			default:
			}
		}
	})
}

// workerProcess 保存测试进程与实际监听地址；err 仅在 done 关闭后读取。
type workerProcess struct {
	address, startLog string
	done              chan struct{}
	err               error
}

// startWorkerProcess 请求系统分配独立端口，从真实启动日志提取地址。
// 成功绑定后仍由 verifyWorkerRPC 验证实际服务可用，日志不代替 RPC 检查。
func startWorkerProcess(t *testing.T, binary string, args ...string) *workerProcess {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "worker.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, binary, append([]string{"-listen=127.0.0.1:0"}, args...)...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		cancel()
		logFile.Close()
		t.Fatal(err)
	}
	p := &workerProcess{done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("Worker process not reaped after cancellation")
		}
		if err := logFile.Close(); err != nil {
			t.Error(err)
		}
	})
	addressPattern := regexp.MustCompile(`address=(127\.0\.0\.1:\d+)`)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, "mock ASR worker started") {
				match := addressPattern.FindStringSubmatch(line)
				if len(match) == 2 {
					p.address = match[1]
					p.startLog = line
					return p
				}
			}
		}
		select {
		case <-p.done:
			t.Fatalf("Worker exited before startup: %v\n%s", p.err, data)
		case <-deadline.C:
			t.Fatalf("startup timeout: %s", data)
		case <-ticker.C:
		}
	}
}

// verifyWorkerRPC 发送两块共一秒静音 PCM，半关闭输入后读取到 EOF。
// 核对独立累计进度、partial 顺序与完整 final，避免仅以端口开放判断启动成功。
func verifyWorkerRPC(t *testing.T, address string) {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := asrv1.NewASRServiceClient(conn).StreamingRecognize(ctx, grpc.WaitForReady(true))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 16000)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var responses []*asrv1.StreamingRecognizeResponse
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		responses = append(responses, response)
	}
	if len(responses) != 5 {
		t.Fatalf("responses=%v, want progress/partial/progress/partial/final", responses)
	}
	for i, want := range []uint64{16000, 32000} {
		p := responses[i*2]
		if p.GetProgress() == nil || p.GetProgress().GetProcessedAudioBytes() != want || p.GetText() != "" {
			t.Errorf("progress %d: %v", i, p)
		}
		text := responses[i*2+1]
		if text.GetProgress() != nil || text.GetIsFinal() || text.GetText() != []string{"今", "今天"}[i] {
			t.Errorf("partial %d: %v", i, text)
		}
	}
	final := responses[4]
	if final.GetProgress() != nil || !final.GetIsFinal() || final.GetText() != "今天天气不错" {
		t.Errorf("final=%v", final)
	}
}
