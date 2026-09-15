package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// Config 描述 WebSocket Gateway 配置。
type Config struct {
	MaxMessageBytes int64         // Gateway 允许接收的单个 WebSocket Message 上限, 这个值只是安全限制
	StartTimeout    time.Duration // 限制会话等待完整 start 消息的时间。为 0 时使用默认值；负值属于无效配置。
}

// Gateway 把 WebSocket 音频流桥接到 gRPC Worker。
type Gateway struct {
	ctx    context.Context
	worker asrv1.ASRServiceClient
	cfg    Config

	tracker *sessionTracker // 跟踪本 Gateway 已接纳但尚未完成清理的会话。用于停止接入以及等待所有会话退出。
}

const defaultStartTimeout = 10 * time.Second

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
	if cfg.StartTimeout < 0 {
		return nil, fmt.Errorf("start timeout is invalid")
	} else if cfg.StartTimeout == 0 {
		cfg.StartTimeout = defaultStartTimeout
	}
	return &Gateway{
		ctx:     ctx,
		worker:  worker,
		cfg:     cfg,
		tracker: newSessionTracker(),
	}, nil
}

// ServeHTTP 只负责建立 WebSocket Connection。
//
// Session 的协议校验、gRPC stream、错误分类和关闭流程全部由 session.run 负责。
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 登记会话
	if !g.tracker.tryEnter() {
		http.Error(w, "service is stopping", http.StatusServiceUnavailable)
		return
	}
	defer g.tracker.leave()

	// 升级 WebSocket
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	// 正常情况下 session.run 已经完成 WebSocket close handshake，
	// 此处只是资源兜底。
	defer conn.CloseNow()

	conn.SetReadLimit(g.cfg.MaxMessageBytes)

	s := newSession(conn, g.worker, g.cfg.StartTimeout)
	_ = s.run(g.ctx)
}

// StopAccepting 停止本 Gateway 接纳新会话。
// 允许重复调用，不会主动取消已有会话。
func (g *Gateway) StopAccepting() {
	g.tracker.stopAccepting()
}

// Wait 等待本 Gateway 停止接入且所有会话完成清理。
// 调用方应先调用 StopAccepting。
// ctx 只控制本次等待；取消等待不会取消已有会话。
func (g *Gateway) Wait(ctx context.Context) error {
	return g.tracker.wait(ctx)
}
