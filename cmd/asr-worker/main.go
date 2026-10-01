package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := parseWorkerConfig(os.Args[1:])
	if err != nil {
		// 用户主动查看帮助，属于正常退出。
		if errors.Is(err, flag.ErrHelp) {
			return
		}

		slog.Error("failed to parse worker config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	err = run(ctx, cfg)

	// 必须在 os.Exit 之前显式注销信号通知；
	// os.Exit 不会执行 defer。
	stopping := ctx.Err() != nil
	stop()

	if err != nil {
		// 信号可能恰好发生在 run 真正启动服务之前。
		// 这种情况下 run 会直接返回 context.Canceled，
		// 仍然属于正常的停止请求。
		if stopping && errors.Is(err, context.Canceled) {
			return
		}

		slog.Error("mock ASR worker exited", "error", err)
		os.Exit(1)
	}
}

// run 根据配置创建同一个 Worker，取得全部监听器后运行服务。
// ctx 非 nil，表示外部停止请求；本函数不注册信号、不退出进程。
func run(ctx context.Context, cfg workerConfig) error {
	// 已经收到停止请求时，不再创建 Worker 或占用任何监听端口。
	if err := ctx.Err(); err != nil {
		return err
	}

	worker, err := mockasr.New(cfg.Mock)
	if err != nil {
		return fmt.Errorf("create mock ASR worker: %w", err)
	}

	// 先取得 gRPC 监听器。
	grpcListener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen for gRPC on %s: %w", cfg.ListenAddr, err)
	}
	defer grpcListener.Close()

	// HTTP 显式启用时，再取得 HTTP 监听器。
	// 此时若绑定失败，上面的 defer 会释放 gRPC 端口，
	// 因而不会启动一半的服务。
	var debugListener net.Listener
	if cfg.DebugListenAddr != "" {
		debugListener, err = net.Listen("tcp", cfg.DebugListenAddr)
		if err != nil {
			return fmt.Errorf("listen for debug HTTP on %s: %w", cfg.DebugListenAddr, err)
		}
		defer debugListener.Close()
	}

	grpcServer := grpc.NewServer()

	// gRPC 和 HTTP 必须使用同一个 Worker 实例。
	asrv1.RegisterASRServiceServer(grpcServer, worker)

	var debugServer *http.Server
	if debugListener != nil {
		debugServer = &http.Server{
			Handler:           routes(worker),
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      5 * time.Second,
			IdleTimeout:       30 * time.Second,
		}
	}

	debugAddress := ""
	if debugListener != nil {
		debugAddress = debugListener.Addr().String()
	}

	slog.Info(
		"mock ASR worker started",
		"address", grpcListener.Addr().String(),
		"debug_enabled", debugServer != nil,
		"debug_address", debugAddress,
		"processing_concurrency", cfg.Mock.ProcessingConcurrency,
		"processing_delay", cfg.Mock.ProcessingDelay,
		"response_delay", cfg.Mock.ResponseDelay,
	)

	return serveWorker(
		ctx,
		grpcServer,
		grpcListener,
		debugServer,
		debugListener,
	)
}
