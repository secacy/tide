package gateway

import (
	"context"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// Config 描述 WebSocket Gateway 配置。
type Config struct {
	MaxMessageBytes int64 // Gateway 允许接收的单个 WebSocket Message 上限, 这个值只是安全限制
}

// Gateway 把 WebSocket 音频流桥接到 gRPC Worker。
type Gateway struct {
	ctx    context.Context
	worker asrv1.ASRServiceClient
	cfg    Config
}

// New 创建一个 Gateway。
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
	return &Gateway{
		ctx:    ctx,
		worker: worker,
		cfg:    cfg,
	}, nil
}

// ServeHTTP 只负责建立 WebSocket Connection。
//
// Session 的协议校验、gRPC stream、错误分类和关闭流程全部由 session.run 负责。
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	// 正常情况下 session.run 已经完成 WebSocket close handshake，
	// 此处只是资源兜底。
	defer conn.CloseNow()

	conn.SetReadLimit(g.cfg.MaxMessageBytes)

	session := newSession(conn, g.worker)
	_ = session.run(g.ctx)
}
