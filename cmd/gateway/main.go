package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	gatewayAddr     = ":8080"           // WebSocket Gateway 对外监听地址
	workerAddr      = "localhost:50051" // gRPC Mock Worker 地址
	shutdownTimeout = 5 * time.Second   // HTTP Server 优雅关闭的最长等待时间
)

func main() {
	// logger 统一当前进程的日志格式、输出位置和服务标识。
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("service", "tide-gateway")

	// SIGINT 对应 Ctrl+C，SIGTERM 通常用于容器或进程管理器停止服务。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	err := run(ctx, logger)
	// os.Exit 不执行 defer，因此在决定退出码前释放信号通知资源。
	stop()
	if err != nil {
		logger.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

// run 组装应用依赖，并向各组件传入从非 nil 基础日志器派生的日志器。
func run(ctx context.Context, logger *slog.Logger) error {
	grpcConn, err := grpc.NewClient(workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create worker grpc client: %w", err)
	}
	defer grpcConn.Close()

	workerClient := asrv1.NewASRServiceClient(grpcConn)
	// 组件字段在依赖组装时绑定，会话字段由 Gateway 在接入时绑定。
	gatewayLogger := logger.With("component", "gateway")
	httpLogger := logger.With("component", "http")

	wsGateway, err := gateway.New(ctx, workerClient, gatewayLogger, gateway.Config{
		MaxMessageBytes: 1024 * 1024,
		MaxSessions:     100,
	})
	if err != nil {
		return fmt.Errorf("create gateway: %w", err)
	}

	server := &http.Server{
		Addr:              gatewayAddr,
		Handler:           routes(wsGateway),
		ReadHeaderTimeout: 5 * time.Second,
		// 将 HTTP 标准库内部错误接入同一个结构化日志输出。
		ErrorLog: slog.NewLogLogger(httpLogger.Handler(), slog.LevelError),
	}

	return serve(ctx, server, httpLogger)
}

// routes 负责声明 Gateway 暴露的 HTTP 接口。
func routes(wsGateway http.Handler) http.Handler {
	mux := http.NewServeMux()

	// 一个 WebSocket Connection 对应一个音频 Session，并在 Gateway 内部进一步对应一个 gRPC bidi stream
	mux.Handle("/v1/asr", wsGateway)

	// 简单的进程存活检查
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return mux
}

// serve 管理 HTTP Server 的完整生命周期。
// logger 必须非 nil，用于记录 HTTP 服务启动和关闭事件。
//
// 它同时处理两个事件：
//  1. HTTP Server 自身发生异常；
//  2. 应用 Context 被取消，需要优雅关闭。
func serve(ctx context.Context, server *http.Server, logger *slog.Logger) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.Addr, err)
	}

	logger.Info("http server listening", "address", listener.Addr().String())

	group, groupCtx := errgroup.WithContext(ctx)

	// HTTP Server 主循环
	group.Go(func() error {
		err := server.Serve(listener)
		// Shutdown 会让 Serve 返回 http.ErrServerClosed。这是预期的正常退出，不应该作为错误返回。
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	})

	// 等待应用退出
	group.Go(func() error {
		<-groupCtx.Done()
		logger.Info("http server shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		// HTTP 服务关闭不代表所有 WebSocket 会话已经完成清理。
		logger.Info("http server shutdown completed")
		return nil
	})

	return group.Wait()
}
