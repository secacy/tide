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
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
)

const (
	gatewayAddr    = ":8080"           // WebSocket Gateway 对外监听地址
	workerAddr     = "localhost:50051" // gRPC Mock Worker 地址
	drainTimeout   = 5 * time.Second   // HTTP 关闭和会话自然排空共享的时间预算
	cleanupTimeout = 2 * time.Second   // 强制停止后的清理等待预算
)

func main() {
	// logger 统一当前进程的日志格式、输出位置和服务标识。
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("service", "tide-gateway")

	// SIGINT 对应 Ctrl+C，SIGTERM 通常用于容器或进程管理器停止服务。
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	err := run(stopCtx, logger)
	// os.Exit 不执行 defer，因此在决定退出码前释放信号通知资源。
	stop()
	if err != nil {
		logger.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

// run 组装应用依赖；stopCtx 只触发关闭，logger 必须非 nil。
// serve 完成关闭编排后，才释放会话 Context 和共享 gRPC 连接。
func run(stopCtx context.Context, logger *slog.Logger) error {
	heartbeat, err := wsheartbeat.FromEnvironment(os.Getenv)
	if err != nil {
		return fmt.Errorf("read heartbeat configuration: %w", err)
	}
	settings, err := readWorkerSettings(os.Getenv("TIDE_GATEWAY_CONFIG"))
	if err != nil {
		return fmt.Errorf("read Worker configuration: %w", err)
	}
	pool, closeWorkers, err := openWorkerPool(settings)
	if err != nil {
		return fmt.Errorf("create Worker pool: %w", err)
	}
	defer closeWorkers()
	logger.Info("worker pool configured", "policy", settings.Policy, "workers", pool.Snapshot(), "max_sessions", settings.MaxSessions)
	logger.Info("heartbeat configured", "interval", heartbeat.Interval, "timeout", heartbeat.Timeout)

	// 组件字段在依赖组装时绑定，会话字段由 Gateway 在接入时绑定。
	gatewayLogger := logger.With("component", "gateway")
	httpLogger := logger.With("component", "http")

	// sessionCtx 控制会话生命周期；退出信号只触发关闭编排，不直接取消正在自然排空的会话。
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	defer cancelSessions()

	wsGateway, err := gateway.NewWithPool(sessionCtx, pool, gatewayLogger, gateway.Config{
		Heartbeat:       heartbeat,
		MaxMessageBytes: 1024 * 1024,
		MaxSessions:     settings.MaxSessions,
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

	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.Addr, err)
	}
	defer listener.Close() // 覆盖 serve 启动前校验失败等退出路径。

	return serve(stopCtx, server, listener, wsGateway, httpLogger, shutdownConfig{
		DrainTimeout:   drainTimeout,
		CleanupTimeout: cleanupTimeout,
	})
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

// serve 运行 HTTP 服务，收到 stopCtx 取消或 Serve 返回后执行关闭编排。
//
// listener 已由调用方创建，交给 HTTP Server 使用。
// 返回前收集 Serve 的结果，并完成关闭编排。
// stopCtx 仅用于触发关闭，不作为会话或排空等待的 Context。
// 所有依赖必须非 nil；配置无效时不启动服务，Listener 由调用方清理。
func serve(stopCtx context.Context, server *http.Server, listener net.Listener, sessions gatewayLifecycle, logger *slog.Logger, cfg shutdownConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	logger.Info("http server listening", "address", listener.Addr().String())
	// 缓冲允许 Serve 在协调者执行关闭流程时提交结果。
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- server.Serve(listener)
	}()
	var (
		serveErr      error
		serveResultIn bool
	)

	select {
	case <-stopCtx.Done():
		logger.Info("gateway shutdown started", "trigger", "stop_context")
	case serveErr = <-serveErrCh:
		serveResultIn = true
		logger.Info("gateway shutdown started", "trigger", "serve_returned", "serve_error", serveErr)
	}

	// 无论是谁触发退出，都只执行一次完整关闭编排。
	shutdownErr := shutdownServer(server, sessions, logger, cfg)
	// 收取唯一的 Serve 结果，避免监听异常被关闭结果覆盖。
	if !serveResultIn {
		serveErr = <-serveErrCh
	}

	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}

	var errs []error

	if serveErr != nil {
		errs = append(errs, fmt.Errorf("serve HTTP: %w", serveErr))
	}

	if shutdownErr != nil {
		errs = append(errs, fmt.Errorf("shutdown gateway: %w", shutdownErr))
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}

	logger.Info("gateway stopped")
	return nil
}
