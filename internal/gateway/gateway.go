package gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// Config 描述 WebSocket Gateway 配置。
type Config struct {
	ProcessingTimeout    time.Duration // 最早未确认处理音频的等待预算，默认 3 秒。
	EndTimeout           time.Duration // 从收到合法 End 起的完成预算，默认 5 秒。
	MaxUnprocessedChunks int           // 未确认音频时间记录上限，默认 4096。
	MaxMessageBytes      int64         // Gateway 允许接收的单个 WebSocket Message 上限, 这个值只是安全限制
	MaxSessions          int           // Gateway 同时管理的最大会话数

	AudioQueueMaxBytes  int           // 限制队列内等待发送的音频总字节数，不包含正在发送的音频。
	AudioQueueMaxChunks int           // 限制队列内的音频块数量，防止大量小消息产生过多管理开销。
	ResultWriteTimeout  time.Duration // 限制单次转录结果写入 WebSocket 的等待时间。
}

// Gateway 把 WebSocket 音频流桥接到 gRPC Worker。
type Gateway struct {
	ctx      context.Context
	pool     *WorkerPool
	cfg      Config
	registry *sessionRegistry // 保存本 Gateway 已接纳、尚未完成清理的会话。每个请求通过 register 和 unregister 获取、归还名额。
	logger   *slog.Logger     // 基础日志器；请求通过 With 派生会话日志器，不修改此字段。
}

// New 是单 Worker 兼容入口，为它配置与 MaxSessions 相同的配额。
//
// ctx 应该是应用级生命周期 Context。
// 服务关闭时取消 ctx，可以同时结束所有正在运行的 WebSocket session。
// logger 为 nil 时使用 slog.Default。cfg.MaxSessions 必须大于零。
// 音频队列容量和结果写入期限为零时使用实验初值，负数视为配置错误。
func New(ctx context.Context, worker asrv1.ASRServiceClient, logger *slog.Logger, cfg Config) (*Gateway, error) {
	if cfg.MaxSessions <= 0 {
		return nil, errInvalidMaxSessions
	}
	pool, err := NewWorkerPool([]WorkerConfig{{ID: "default", Client: worker, Capacity: cfg.MaxSessions}}, RoundRobin)
	if err != nil {
		return nil, err
	}
	return NewWithPool(ctx, pool, logger, cfg)
}

// NewWithPool 为单 Gateway 绑定固定 Worker 池，保留独立的 Gateway 会话上限。
// 每个 Gateway 使用自己的本地账本；多个 Gateway 对同一后端的配额不会自动协调。
// 共享客户端由应用组装层管理，应在 Gateway 排空之后关闭。
func NewWithPool(ctx context.Context, pool *WorkerPool, logger *slog.Logger, cfg Config) (*Gateway, error) {
	if ctx == nil {
		return nil, fmt.Errorf("gateway context is nil")
	}
	if pool == nil || len(pool.workers) == 0 {
		return nil, fmt.Errorf("worker pool is not initialized")
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 1024 * 1024 // 1 MiB
	}
	if cfg.AudioQueueMaxBytes < 0 || cfg.AudioQueueMaxChunks < 0 || cfg.ResultWriteTimeout < 0 || cfg.ProcessingTimeout < 0 || cfg.EndTimeout < 0 || cfg.MaxUnprocessedChunks < 0 {
		return nil, fmt.Errorf("audio queue limits and result write timeout must not be negative")
	}
	// 以下是可调的实验初值，不代表已验证的容量或延迟保证。
	if cfg.AudioQueueMaxBytes == 0 {
		cfg.AudioQueueMaxBytes = 64_000 // 当前 PCM 格式约 2 秒音频。
	}
	if cfg.AudioQueueMaxChunks == 0 {
		cfg.AudioQueueMaxChunks = 128
	}
	if cfg.ResultWriteTimeout == 0 {
		cfg.ResultWriteTimeout = 2 * time.Second
	}
	if cfg.ProcessingTimeout == 0 {
		cfg.ProcessingTimeout = 3 * time.Second
	}
	if cfg.EndTimeout == 0 {
		cfg.EndTimeout = 5 * time.Second
	}
	if cfg.MaxUnprocessedChunks == 0 {
		cfg.MaxUnprocessedChunks = 4096
	}
	registry, err := newSessionRegistry(cfg.MaxSessions)
	if err != nil {
		return nil, fmt.Errorf("create session registry: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Gateway{
		ctx:      ctx,
		pool:     pool,
		cfg:      cfg,
		registry: registry,
		logger:   logger,
	}, nil
}

// ServeHTTP 管理一次客户端接入及其登记生命周期。
//
// 在 WebSocket 升级前创建并登记 Session。
// 登记失败时返回 HTTP 错误，不进行升级。
// 登记成功后，所有退出路径都必须最终注销会话。
//
// 升级成功后，将连接交给 Session 运行。
// Session 返回后，先完成连接兜底清理，再注销并释放名额。
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sessionID := rand.Text()
	s := newSession(g.ctx, sessionID, nil)
	// 使用局部日志器关联当前接入过程，避免并发请求串用会话字段。
	sessionLogger := g.logger.With("session_id", s.id)
	if err := g.registry.register(s); err != nil {
		s.cancel(nil)
		if errors.Is(err, errSessionLimit) || errors.Is(err, errRegistryStopping) {
			sessionLogger.Info("session admission rejected", "error", err)
		} else {
			sessionLogger.Error("session registration failed", "error", err)
		}
		writeRegisterError(w, err)
		return
	}
	var lease *WorkerLease
	defer func() {
		// 后登记的连接清理 defer 先执行；Worker 预留必须先于 Session 注销归还。
		s.cancel(nil)
		lease.Release()
		g.registry.unregister(s)
	}()

	if s.ctx.Err() != nil {
		http.Error(w, "gateway stopping", http.StatusServiceUnavailable)
		return
	}
	var err error
	lease, err = g.pool.TryAcquire()
	if err != nil {
		sessionLogger.Info("worker admission rejected", "error", err)
		http.Error(w, "worker capacity unavailable", http.StatusServiceUnavailable)
		return
	}
	s.worker = lease.Client() // run 前只绑定一次；Abort 不读写 worker 字段。
	sessionLogger = sessionLogger.With("worker_id", lease.WorkerID())
	if s.ctx.Err() != nil {
		http.Error(w, "gateway stopping", http.StatusServiceUnavailable)
		return
	}

	conn, err := websocket.Accept(wrapSessionResponseWriter(w, s), r, nil)
	if err != nil {
		sessionLogger.Debug("websocket upgrade failed", "error", err)
		return
	}
	// 正常情况下 session.run 已经完成 WebSocket close handshake，
	// 此处只是资源兜底。
	defer conn.CloseNow()

	conn.SetReadLimit(g.cfg.MaxMessageBytes)
	s.ws = conn
	sessionLogger.Info("session connected")

	// run 返回后，外层 defer 还会继续清理连接并注销会话。
	// 当前返回值也包含客户端断开等情况，暂不将所有错误归类为服务故障。
	if err := s.run(g.cfg); err != nil {
		sessionLogger.Info("session run ended", "error", err)
	} else {
		sessionLogger.Info("session run ended")
	}
}

// writeRegisterError 将登记失败映射为升级前的 HTTP 错误响应。
func writeRegisterError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errSessionLimit):
		http.Error(w, "session capacity reached", http.StatusServiceUnavailable)

	case errors.Is(err, errRegistryStopping):
		http.Error(w, "gateway stopping", http.StatusServiceUnavailable)

	case errors.Is(err, errInvalidSession):
		http.Error(w, "internal server error", http.StatusInternalServerError)

	case errors.Is(err, errDuplicateSession):
		http.Error(w, "internal server error", http.StatusInternalServerError)

	default:
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
