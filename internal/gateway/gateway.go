package gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// Config 描述 WebSocket Gateway 配置。
type Config struct {
	MaxMessageBytes int64 // Gateway 允许接收的单个 WebSocket Message 上限, 这个值只是安全限制
	MaxSessions     int   // Gateway 同时管理的最大会话数
}

// Gateway 把 WebSocket 音频流桥接到 gRPC Worker。
type Gateway struct {
	ctx      context.Context
	worker   asrv1.ASRServiceClient
	cfg      Config
	registry *sessionRegistry // 保存本 Gateway 已接纳、尚未完成清理的会话。每个请求通过 register 和 unregister 获取、归还名额。
}

// New 创建一个 Gateway，并初始化会话注册表.
//
// ctx 应该是应用级生命周期 Context。
// 服务关闭时取消 ctx，可以同时结束所有正在运行的 WebSocket session。
func New(ctx context.Context, worker asrv1.ASRServiceClient, cfg Config) (*Gateway, error) {
	if ctx == nil {
		return nil, fmt.Errorf("gateway context is nil")
	}
	if worker == nil {
		return nil, fmt.Errorf("audio worker client is nil")
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 1024 * 1024 // 1 MiB
	}
	registry, err := newSessionRegistry(cfg.MaxSessions)
	if err != nil {
		return nil, fmt.Errorf("create session registry: %w", err)
	}
	return &Gateway{
		ctx:      ctx,
		worker:   worker,
		cfg:      cfg,
		registry: registry,
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
	s := newSession(sessionID, g.worker)
	if err := g.registry.register(s); err != nil {
		writeRegisterError(w, err)
		return
	}
	defer g.registry.unregister(s)

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	// 正常情况下 session.run 已经完成 WebSocket close handshake，
	// 此处只是资源兜底。
	defer conn.CloseNow()

	conn.SetReadLimit(g.cfg.MaxMessageBytes)
	s.ws = conn

	if err := s.run(g.ctx); err != nil {
		log.Println("session failed", "error", err)
	}
}

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
