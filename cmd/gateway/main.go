package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	gatewayAddr     = ":8080"          // WebSocket Gateway 对外监听地址
	shutdownTimeout = 15 * time.Second // HTTP Server 优雅关闭和已有会话退出的等待时间上限
)

func main() {
	cfg, err := parseGatewayConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("gateway backend configuration",
		"strategy", "round_robin",
		"workers", cfg.WorkerAddrs,
	)

	// SIGINT 对应 Ctrl+C，SIGTERM 通常用于容器或进程管理器停止服务。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

// run 根据启动配置组装后端客户端、选择器和 Gateway，并协调退出。
// 所有后端客户端由本函数创建和关闭，会话只拥有自己的 RPC stream。
func run(ctx context.Context, cfg gatewayConfig) error {
	var conns []*grpc.ClientConn
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()

	workers := make([]workerpool.Worker, 0, len(cfg.WorkerAddrs))

	for _, addr := range cfg.WorkerAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("create worker grpc client %q: %w", addr, err)
		}

		// 必须在成功创建后立即登记，确保后续任一步骤失败时都能关闭。
		conns = append(conns, conn)

		workers = append(workers, workerpool.Worker{
			ID:     addr,
			Client: asrv1.NewASRServiceClient(conn),
		})
	}

	pool, err := workerpool.NewRoundRobin(workers)
	if err != nil {
		return fmt.Errorf("create worker pool: %w", err)
	}

	sessionCtx, cancelSessions := context.WithCancel(context.Background()) // sessionCtx 管理本 Gateway 所有会话的停止通知
	defer cancelSessions()
	wsGateway, err := gateway.New(sessionCtx, pool, cfg.Gateway)
	if err != nil {
		return fmt.Errorf("create gateway: %w", err)
	}

	server := &http.Server{
		Addr:              gatewayAddr,
		Handler:           routes(wsGateway),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return serve(ctx, server, wsGateway, cancelSessions)
}

// routes 负责声明 Gateway 暴露的 HTTP 接口。
func routes(wsGateway *gateway.Gateway) http.Handler {
	mux := http.NewServeMux()

	// 一个 WebSocket Connection 对应一个音频 Session，并在 Gateway 内部进一步对应一个 gRPC bidi stream
	mux.Handle("/v1/asr", wsGateway)

	// 查询 Gateway 当前会话状态，不占用会话名额。
	mux.HandleFunc("GET /debug/gateway", gatewaySnapshotHandler(wsGateway))

	// 简单的进程存活检查
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return mux
}

// serve 运行 HTTP 服务，并协调服务退出时的清理。
//
// 它同时处理两个事件：
//  1. HTTP Server 自身发生异常；
//  2. 应用 Context 被取消，需要优雅关闭。
//
// ctx 表示外部退出请求；HTTP 服务异常也会触发关闭流程。
// wsGateway 用于停止接入和等待会话退出。
// cancelSessions 用于通知已有会话停止。
//
// HTTP 关闭或会话等待失败时返回错误，不能报告清理成功。
func serve(ctx context.Context, server *http.Server, wsGateway *gateway.Gateway, cancelSessions context.CancelFunc) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.Addr, err)
	}

	log.Printf("websocket gateway listening on %s", server.Addr)

	group, groupCtx := errgroup.WithContext(ctx)

	// cleanupErr 仅由关闭 goroutine 写入。
	// 外层在 group.Wait 返回后读取，避免并发读写。
	var cleanupErr error

	// HTTP Server 主循环：真实错误通过返回值触发 groupCtx 取消。
	group.Go(func() error {
		err := server.Serve(listener)
		// Shutdown 会让 Serve 返回 http.ErrServerClosed。这是预期的正常退出，不应该作为错误返回。
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	})

	// 收到停止通知后，执行关闭并收集清理错误
	group.Go(func() error {
		<-groupCtx.Done()
		var errs []error
		log.Printf("shutting down websocket gateway")

		// 停止接纳新会话
		wsGateway.StopAccepting()
		// 向所有继承 sessionCtx 的会话发送停止通知
		cancelSessions()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		// 关闭 HTTP 服务
		if err := server.Shutdown(shutdownCtx); err != nil { // server.Serve 由 server.Shutdown() 来停止服务。
			errs = append(errs, fmt.Errorf("shutdown HTTP server: %w", err))
			if err := server.Close(); err != nil { // 关闭剩余普通 HTTP 连接
				errs = append(errs, fmt.Errorf("close HTTP server: %w", err))
			}
		}
		// 等待会话完成清理
		if err := wsGateway.Wait(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("wait WebSocket sessions: %w", err))
		}
		cleanupErr = errors.Join(errs...)
		return nil
	})

	serveErr := group.Wait()

	// 返回运行和清理结果
	return errors.Join(serveErr, cleanupErr)
}
