package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// Config 描述 WebSocket Gateway 配置。
type Config struct {
	MaxMessageBytes      int64         // Gateway 允许接收的单个 WebSocket Message 上限, 这个值只是安全限制
	StartTimeout         time.Duration // 限制会话等待完整 start 消息的时间。为 0 时使用默认值；负值属于无效配置。
	InputIdleTimeout     time.Duration // 限制音频输入阶段每次等待完整消息的时间。不包含向 Worker 转发音频的时间；收到合法 end 后不再使用。
	WorkerSendTimeout    time.Duration // 限制单次向 Worker 发送音频的等待时间。超时后取消整个会话 RPC；不代表模型处理期限。
	MaxSessions          int           // 限制本 Gateway 已接纳但尚未完成清理的会话数量
	TailTimeout          time.Duration // 限制协调者观察到合法 end 后，等待剩余结果转发和 Worker 响应流结束的时间
	ResultWriteTimeout   time.Duration // 限制单条结果的 WebSocket Write 等待
	MaxPendingAudioBytes int64         // 限制单会话未确认处理的音频字节数。0 表示关闭，正值启用，负值无效。
	V2                   *V2Config     // 非 nil 时显式启用可恢复实验入口；New 复制配置。
}

// Gateway 把 WebSocket 音频流桥接到 gRPC Worker。
type Gateway struct {
	ctx  context.Context
	pool WorkerSelector // 本 Gateway 的会话共享同一选择器。
	cfg  Config

	tracker  *sessionTracker  // 跟踪本 Gateway 已接纳但尚未完成清理的会话。用于停止接入以及等待所有会话退出。
	gate     *entryGate       // 独立握手预算及共同停服提交边界，与 v1 共用 tracker。
	registry *sessionRegistry // 仅定位 v2 逻辑会话，清理和恢复资格由运行器管理。
	v2       *V2Config        // 私有归一化配置；nil 表示入口禁用。
}

const (
	defaultStartTimeout       = 10 * time.Second
	defaultInputIdleTimeout   = 30 * time.Second
	defaultWorkerSendTimeout  = 2 * time.Second
	defaultMaxSessions        = 64
	defaultTailTimeout        = 15 * time.Second
	defaultResultWriteTimeout = 2 * time.Second
)

// New 创建 Gateway。
// pool 必须是已经完成初始化、支持并发调用的选择器。
// Gateway 复用选择器返回的客户端，连接生命周期由外部管理。
// nil 接口或包含 nil 动态值的选择器会被拒绝。
func New(ctx context.Context, pool WorkerSelector, cfg Config) (*Gateway, error) {
	if ctx == nil {
		return nil, fmt.Errorf("gateway context is nil")
	}
	if isNilWorkerSelector(pool) {
		return nil, fmt.Errorf("worker pool is nil")
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 1024 * 1024 // 1 MiB
	}
	if cfg.StartTimeout < 0 {
		return nil, fmt.Errorf("start timeout is invalid")
	} else if cfg.StartTimeout == 0 {
		cfg.StartTimeout = defaultStartTimeout
	}
	if cfg.InputIdleTimeout < 0 {
		return nil, fmt.Errorf("input idle timeout is invalid")
	} else if cfg.InputIdleTimeout == 0 {
		cfg.InputIdleTimeout = defaultInputIdleTimeout
	}
	if cfg.WorkerSendTimeout < 0 {
		return nil, fmt.Errorf("worker send timeout is invalid")
	} else if cfg.WorkerSendTimeout == 0 {
		cfg.WorkerSendTimeout = defaultWorkerSendTimeout
	}
	if cfg.MaxSessions < 0 {
		return nil, fmt.Errorf("max sessions is invalid")
	} else if cfg.MaxSessions == 0 {
		cfg.MaxSessions = defaultMaxSessions
	}
	if cfg.TailTimeout < 0 {
		return nil, fmt.Errorf("tail timeout is invalid")
	} else if cfg.TailTimeout == 0 {
		cfg.TailTimeout = defaultTailTimeout
	}
	if cfg.ResultWriteTimeout < 0 {
		return nil, fmt.Errorf("result write timeout is invalid")
	} else if cfg.ResultWriteTimeout == 0 {
		cfg.ResultWriteTimeout = defaultResultWriteTimeout
	}
	if cfg.MaxPendingAudioBytes < 0 {
		return nil, fmt.Errorf("max pending audio bytes is invalid")
	}
	v2, err := normalizeV2Config(cfg.V2, cfg.MaxMessageBytes)
	if err != nil {
		return nil, err
	}
	tracker := newSessionTracker(cfg.MaxSessions)
	handshakeLimit := defaultMaxHandshakes
	if v2 != nil {
		handshakeLimit = v2.MaxHandshakes
	}
	gate, err := newEntryGate(tracker, handshakeLimit)
	if err != nil {
		return nil, err
	}
	// 运行中仅使用私有 v2 副本，不保留调用者的可变指针。
	cfg.V2 = nil
	return &Gateway{
		ctx:      ctx,
		pool:     pool,
		cfg:      cfg,
		tracker:  tracker,
		gate:     gate,
		registry: newSessionRegistry(),
		v2:       v2,
	}, nil
}

// sessionConfig 从 Gateway.New 已校验并填充默认值的配置提取会话参数。
// MaxPendingAudioBytes 已保证非负，此处转换为会话计数使用的 uint64。
// 不重复校验或填充默认值，不包含网关准入与消息大小设置。
func (c Config) sessionConfig() sessionConfig {
	return sessionConfig{
		StartTimeout:         c.StartTimeout,
		InputIdleTimeout:     c.InputIdleTimeout,
		WorkerSendTimeout:    c.WorkerSendTimeout,
		TailTimeout:          c.TailTimeout,
		ResultWriteTimeout:   c.ResultWriteTimeout,
		MaxPendingAudioBytes: uint64(c.MaxPendingAudioBytes),
	}
}

// ServeHTTP 只负责建立 WebSocket Connection。
//
// Session 的协议校验、gRPC stream、错误分类和关闭流程全部由 session.run 负责。
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 登记会话
	enterErr := g.tracker.tryEnter()
	if errors.Is(enterErr, errGatewayStopping) {
		http.Error(w, "service is stopping", http.StatusServiceUnavailable)
		return
	}
	if errors.Is(enterErr, errSessionLimit) {
		http.Error(w, "session limit exceeded", http.StatusServiceUnavailable)
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

	s := newSession(conn, g.pool, g.cfg.sessionConfig())
	_ = s.run(g.ctx)
}

// StopAccepting 停止本 Gateway 接纳新会话。
// 允许重复调用，不会主动取消已有会话。
func (g *Gateway) StopAccepting() {
	g.gate.stopAccepting()
}

// Wait 等待本 Gateway 停止接入且所有会话完成清理。
// 调用方应先调用 StopAccepting。
// ctx 只控制本次等待；取消等待不会取消已有会话。
func (g *Gateway) Wait(ctx context.Context) error {
	return g.gate.wait(ctx)
}
