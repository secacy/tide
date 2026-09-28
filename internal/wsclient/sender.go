package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// send 完成客户端发送方向的完整生命周期
func (c *Client) send(ctx context.Context, conn *websocket.Conn, source io.Reader) error {
	if err := c.writeJSON(ctx, conn, WriteStart, wsprotocol.StartMessage{
		Type:    wsprotocol.MessageTypeStart,
		Version: "v1",
	}); err != nil {
		return fmt.Errorf("send start message: %w", err)
	}

	if err := c.sendAudio(ctx, conn, source); err != nil {
		return err
	}

	if err := c.writeJSON(ctx, conn, WriteEnd, wsprotocol.EndMessage{
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
			if c.cfg.Realtime {
				if err := pacer.WaitBeforeSend(ctx); err != nil {
					return fmt.Errorf("wait before sending PCM: %w", err)
				}
			}
			if err := c.writeObserved(ctx, conn, WriteAudio, websocket.MessageBinary, buf[:n]); err != nil {
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
func (c *Client) writeJSON(ctx context.Context, conn *websocket.Conn, kind WriteKind, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal websocket message: %w", err)
	}

	if err := c.writeObserved(ctx, conn, kind, websocket.MessageText, data); err != nil {
		return fmt.Errorf("write websocket text message: %w", err)
	}

	return nil
}

// writeObserved 执行一次 WebSocket 写入，同步报告结果并返回原始错误。
// kind 表示业务用途；messageType 表示 WebSocket 消息类型。
// data 只用于本次写入，不交给观察回调。
func (c *Client) writeObserved(ctx context.Context, conn *websocket.Conn, kind WriteKind, messageType websocket.MessageType, data []byte) error {
	startedAt := time.Now()
	err := conn.Write(ctx, messageType, data)
	finishedAt := time.Now()

	audioBytes := 0
	if kind == WriteAudio {
		audioBytes = len(data)
	}

	if c.cfg.OnWrite != nil {
		c.cfg.OnWrite(WriteEvent{
			Kind:       kind,
			AudioBytes: audioBytes,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Err:        err,
		})
	}

	return err
}
