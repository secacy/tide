package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"github.com/secacy/tide-artisan/workers/sherpa/internal/native"
	"google.golang.org/grpc"
)

type config struct {
	listen            string        // 实际监听前仍由 net.Listen 校验地址。
	model             native.Config // 仅参数值；解析不加载模型或访问文件。
	maxAudioBytes     int           // 整场原始输入上限，默认 30 秒。
	segmentAudioBytes int           // 0 使用单段模式，正偶数显式启用分段。
	observeSegments   bool          // 显式实验观察；记录流边界和成功提交的样本摘要。
}

// parseConfig 解析独立 Worker 参数；帮助输出不会创建原生对象。
func parseConfig(args []string, output io.Writer) (config, error) {
	cfg := config{}
	fs := flag.NewFlagSet("sherpa-asr-worker", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&cfg.listen, "listen", "127.0.0.1:50051", "gRPC TCP listen address")
	fs.StringVar(&cfg.model.ModelDir, "model-dir", "", "required fixed int8 Zipformer asset directory")
	fs.IntVar(&cfg.model.Threads, "threads", 2, "native CPU threads (1..16)")
	fs.IntVar(&cfg.maxAudioBytes, "max-audio-bytes", 960000, "whole-session PCM byte limit (positive even integer)")
	fs.IntVar(&cfg.segmentAudioBytes, "segment-audio-bytes", 0, "PCM bytes per segment; 0 disables segmentation")
	fs.BoolVar(&cfg.observeSegments, "observe-segments", false, "log segment lifecycle and submitted sample digests (requires segmentation)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 || strings.TrimSpace(cfg.listen) == "" || strings.TrimSpace(cfg.model.ModelDir) == "" || cfg.model.Threads < 1 || cfg.model.Threads > 16 || cfg.maxAudioBytes <= 0 || cfg.maxAudioBytes%2 != 0 || cfg.segmentAudioBytes < 0 || cfg.segmentAudioBytes%2 != 0 || cfg.segmentAudioBytes > cfg.maxAudioBytes || (cfg.observeSegments && cfg.segmentAudioBytes == 0) {
		return config{}, errors.New("require model-dir, nonempty listen, threads 1..16, positive even max-audio-bytes and even segment-audio-bytes in [0,max]; no positional arguments")
	}
	return cfg, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg, err := parseConfig(os.Args[1:], os.Stdout)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = run(ctx, cfg, logger)
	stop() // os.Exit 不执行 defer，先显式注销信号。
	if err != nil {
		logger.Error("sherpa worker exited", "error", err)
		os.Exit(1)
	}
}

// run 先检查资源，再监听；模型由每个取得名额的 RPC 延迟创建。
// 返回前等待 handler 清理，ctx 取消只负责触发停服。
func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	if err := ctx.Err(); err != nil {
		return nil
	}
	w, limits, err := configuredWorker(cfg, logger)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(4096), grpc.WaitForHandlers(true))
	asrv1.RegisterASRServiceServer(server, w)
	if err := ctx.Err(); err != nil {
		return nil
	}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	defer listener.Close()
	logger.Info("listening", "address", listener.Addr().String(), "model_loaded", false,
		"backend", "sherpa-onnx-go-macos@v1.13.8", "threads", cfg.model.Threads,
		"max_audio_bytes", limits.MaxAudioBytes, "max_chunk_bytes", limits.MaxChunkBytes, "max_text_bytes", limits.MaxTextBytes,
		"segment_audio_bytes", cfg.segmentAudioBytes, "observe_segments", cfg.observeSegments, "progress_available", false)
	return serve(ctx, server, listener, 5*time.Second)
}

// serve 同时观察 Serve 和外部停止；timeout 只限制自然收尾阶段。
// server 必须使用 WaitForHandlers(true)，Stop 才等待实际 handler 返回。
func serve(ctx context.Context, server *grpc.Server, listener net.Listener, timeout time.Duration) error {
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		server.Stop()
		if err == nil {
			return errors.New("gRPC server stopped unexpectedly")
		}
		return fmt.Errorf("serve gRPC: %w", err)
	case <-ctx.Done():
	}
	graceful := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(graceful)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var shutdownErr error
	select {
	case <-graceful:
	case <-timer.C:
		select {
		case <-graceful:
		default:
			shutdownErr = errors.New("graceful shutdown expired; RPCs canceled")
			// 可以等待不可中断的原生调用；不承诺 timeout 内退出进程。
			server.Stop()
			<-graceful
		}
	}
	serveErr := <-served
	if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("serve gRPC: %w", serveErr))
	}
	return shutdownErr
}
