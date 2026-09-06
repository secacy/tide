package wsclient

import (
	"context"
	"encoding/json"
	"fmt"

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

		if messageType != websocket.MessageText {
			return fmt.Errorf("unexpected websocket message type from gateway: %v", messageType)
		}

		if err := c.handleTextMessage(data); err != nil {
			return err
		}
	}
}

// envelope 只用于在完整反序列化前识别消息类型。
type envelope struct {
	Type wsprotocol.MessageType `json:"type"`
}

// handleTextMessage 处理 Gateway 返回的应用层消息。
func (c *Client) handleTextMessage(data []byte) error {
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

		// 示例阶段直接输出。
		//
		// 后续可以进一步抽成 ResultHandler，让 WebSocket transport 不再依赖 stdout。
		fmt.Printf("result text=%q final=%v\n", result.Text, result.Final)

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
