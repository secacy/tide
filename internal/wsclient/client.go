package wsclient

import (
	"context"
	"fmt"
	"io"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"golang.org/x/sync/errgroup"
)

// Config 描述 WebSocket 模拟客户端配置。
type Config struct {
	URL        string // Gateway 的 WebSocket 地址，例如：ws://localhost:8080/v1/asr
	ChunkBytes int    // 一次最多读取并发送多少 PCM 字节(客户端的发送粒度)，默认3200bytes(100ms)
	Realtime   bool   // 是否模拟真实麦克风速度。true: 10 秒音频大约需要 10 秒发送完成; false: 尽可能快地发送，适合吞吐测试
	ReadLimit  int64  // 限制服务端单个 WebSocket message 的最大大小，它影响客户端读取 Gateway 返回消息，不影响客户端发送 PCM
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

// Run 建立一次完整的 WebSocket 音频会话。
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

	group, gctx := errgroup.WithContext(ctx)

	// 发送方向
	group.Go(func() error {
		if err := c.send(gctx, conn, source); err != nil {
			return fmt.Errorf("send websocket stream: %w", err)
		}

		return nil
	})

	// 接收方向
	group.Go(func() error {
		if err := c.receive(gctx, conn); err != nil {
			return fmt.Errorf("receive websocket stream: %w", err)
		}

		return nil
	})

	if err := group.Wait(); err != nil {
		return err
	}

	return nil
}
