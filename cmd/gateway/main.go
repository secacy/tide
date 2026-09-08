package main

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	// SIGINT 对应 Ctrl+C，SIGTERM 通常用于容器或进程管理器停止服务。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

// run 只负责应用依赖的组装。
func run(ctx context.Context) error {
	grpcConn, err := grpc.NewClient(workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create worker grpc client: %w", err)
	}
	defer grpcConn.Close()

	workerClient := asrv1.NewASRServiceClient(grpcConn)

	wsGateway, err := gateway.New(ctx, workerClient, gateway.Config{
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
	}

	return serve(ctx, server)
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
//
// 它同时处理两个事件：
//  1. HTTP Server 自身发生异常；
//  2. 应用 Context 被取消，需要优雅关闭。
func serve(ctx context.Context, server *http.Server) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.Addr, err)
	}

	log.Printf("websocket gateway listening on %s", server.Addr)

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
		log.Printf("shutting down websocket gateway")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		return nil
	})

	return group.Wait()
}
