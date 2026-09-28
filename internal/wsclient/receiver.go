package wsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// receive 持续读取 Gateway 返回的消息。
func (c *Client) receive(ctx context.Context, conn *websocket.Conn) error {
	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			status := websocket.CloseStatus(err)
			if status == websocket.StatusNormalClosure { // Gateway 正常结束整个 session
				return nil
			}
			return fmt.Errorf("read websocket message: %w", err)
		}

		receivedAt := time.Now()

		if messageType != websocket.MessageText {
			return fmt.Errorf("unexpected websocket message type from gateway: %v", messageType)
		}

		if err := c.handleTextMessage(data, receivedAt); err != nil {
			return err
		}
	}
}

// envelope 只用于在完整反序列化前识别消息类型。
type envelope struct {
	Type wsprotocol.MessageType `json:"type"`
}

// handleTextMessage 解析应用层消息，并向观察者传递有效结果。
// receivedAt 由读取消息的调用方提供，避免使用解析完成时间代替接收时间。
func (c *Client) handleTextMessage(data []byte, receivedAt time.Time) error {
	var env envelope

	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("decode websocket message envelope: %w", err)
	}

	switch env.Type {
	case wsprotocol.MessageTypeResult:
		var result wsprotocol.ResultMessage

		if err := json.Unmarshal(data, &result); err != nil {
			return fmt.Errorf("decode result message: %w", err)
		}

		if c.cfg.OnResult != nil {
			c.cfg.OnResult(result, receivedAt)
		}

		return nil

	case wsprotocol.MessageTypeError:
		var gatewayErr wsprotocol.ErrorMessage

		if err := json.Unmarshal(data, &gatewayErr); err != nil {
			return fmt.Errorf("decode error message: %w", err)
		}

		return fmt.Errorf("gateway error: %s", gatewayErr.Message)

	default:
		return fmt.Errorf("unsupported websocket message type: %q", env.Type)
	}
}
