package wsclient

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
	"golang.org/x/sync/errgroup"
)

// Config 描述 WebSocket 模拟客户端配置。
type Config struct {
	Heartbeat  wsheartbeat.Config // 零值默认启用；客户端独立探测，不依赖 Gateway 的失败判定。
	URL        string             // Gateway 的 WebSocket 地址，例如：ws://localhost:8080/v1/asr
	ChunkBytes int                // 一次最多读取并发送多少 PCM 字节(客户端的发送粒度)，默认3200bytes(100ms)
	Realtime   bool               // 是否模拟真实麦克风速度。true: 10 秒音频大约需要 10 秒发送完成; false: 尽可能快地发送，适合吞吐测试
	ReadLimit  int64              // 限制服务端单个 WebSocket message 的最大大小，它影响客户端读取 Gateway 返回消息，不影响客户端发送 PCM
}

// validate 检查客户端配置是否合法。
func (c Config) validate() error {
	if c.URL == "" {
		return fmt.Errorf("websocket URL is required")
	}
	if c.ChunkBytes <= 0 {
		return fmt.Errorf("chunk bytes must be greater than zero")
	}
	if c.ChunkBytes%audio.BytesDepth != 0 {
		return fmt.Errorf("chunk bytes %d must align to %d-byte PCM samples", c.ChunkBytes, audio.BytesDepth)
	}
	return nil
}

// Client 是模拟 WebSocket 音频客户端。
type Client struct {
	cfg Config
}

// New 创建 WebSocket Client。
func New(cfg Config) (*Client, error) {
	var err error
	cfg.Heartbeat, err = cfg.Heartbeat.Normalize()
	if err != nil {
		return nil, err
	}
	if cfg.ChunkBytes <= 0 {
		cfg.ChunkBytes = audio.ChunkBytesDefault
	}
	if cfg.ReadLimit <= 0 {
		cfg.ReadLimit = 64 * 1024
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid websocket client config: %w", err)
	}

	return &Client{
		cfg: cfg,
	}, nil
}

// Run 建立一次完整的 WebSocket 音频会话，不自动重连或重放。
// source 的 Read 必须能够返回；Context 不能中断任意阻塞的 io.Reader。
func (c *Client) Run(ctx context.Context, source io.Reader) error {
	if source == nil {
		return fmt.Errorf("audio source is nil")
	}
	conn, _, err := websocket.Dial(ctx, c.cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("dial websocket gateway: %w", err)
	}

	defer conn.CloseNow() // CloseNow 是异常路径的兜底

	conn.SetReadLimit(c.cfg.ReadLimit)

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	heartbeat := wsheartbeat.Start(runCtx, c.cfg.Heartbeat, conn.Ping, func(err error) {
		cancel(err)
		_ = conn.CloseNow()
	})
	group, gctx := errgroup.WithContext(runCtx)

	// 发送方向
	group.Go(func() error {
		if err := c.send(gctx, conn, source); err != nil {
			heartbeat.Stop()
			_ = conn.CloseNow()
			return fmt.Errorf("send websocket stream: %w", err)
		}

		return nil
	})

	// 接收方向
	group.Go(func() error {
		// 接收保留父 Context；发送或探测失败通过 CloseNow 唤醒它，
		// 避免 errgroup 的取消把已收到的正常关闭原因遮蔽。
		err := c.receive(ctx, conn)
		heartbeat.Stop()
		if err != nil {
			_ = conn.CloseNow()
			return fmt.Errorf("receive websocket stream: %w", err)
		}

		return nil
	})

	err = group.Wait()
	heartbeat.Stop()
	heartbeatErr := heartbeat.Wait()
	if err != nil && errors.Is(context.Cause(runCtx), wsheartbeat.ErrFailed) {
		return errors.Join(context.Cause(runCtx), err)
	}
	if err != nil && ctx.Err() == nil && errors.Is(heartbeatErr, context.DeadlineExceeded) {
		// 控制帧写超时可先唤醒 Reader，再被 Monitor 观察到。
		return errors.Join(heartbeatErr, err)
	}
	return err
}
