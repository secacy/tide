package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// send 完成客户端发送方向的完整生命周期
func (c *Client) send(ctx context.Context, conn *websocket.Conn, source io.Reader) error {
	if err := c.writeJSON(ctx, conn, wsprotocol.StartMessage{
		Type:    wsprotocol.MessageTypeStart,
		Version: "v1",
	}); err != nil {
		return fmt.Errorf("send start message: %w", err)
	}

	if err := c.sendAudio(ctx, conn, source); err != nil {
		return err
	}

	if err := c.writeJSON(ctx, conn, wsprotocol.EndMessage{
		Type: wsprotocol.MessageTypeEnd,
	}); err != nil {
		return fmt.Errorf("send end message: %w", err)
	}

	return nil
}

// sendAudio 持续读取 PCM，并将每个 chunk 作为一个 WebSocket binary message 发送。
func (c *Client) sendAudio(ctx context.Context, conn *websocket.Conn, source io.Reader) error {
	buf := make([]byte, c.cfg.ChunkBytes)

	pacer := audio.NewPacer()

	for {
		n, readErr := io.ReadFull(source, buf)
		if n > 0 {
			if err := pacer.WaitBeforeSend(ctx); err != nil {
				return fmt.Errorf("wait before sending PCM: %w", err)
			}
			if err := conn.Write(ctx, websocket.MessageBinary, buf[:n]); err != nil {
				return fmt.Errorf("write PCM websocket message (%d bytes): %w", n, err)
			}
			pacer.Advance(n)
		}

		switch {
		case readErr == nil:
			continue
		case errors.Is(readErr, io.EOF): // 没有剩余数据
			return nil
		case errors.Is(readErr, io.ErrUnexpectedEOF): // 最后一个 chunk 不足 ChunkBytes
			return nil
		default:
			return fmt.Errorf("read PCM source: %w", readErr)
		}
	}
}

// writeJSON 把应用层控制消息编码为 WebSocket Text Message。
func (c *Client) writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal websocket message: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("write websocket text message: %w", err)
	}
	return nil
}
