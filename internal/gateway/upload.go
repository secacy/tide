package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// upload 负责 WebSocket -> gRPC 方向。
func (s *session) upload(ctx context.Context, stream workerStream, inputEnded chan<- struct{}) sessionResult {
	for {
		messageType, data, err := s.ws.Read(ctx)
		if err != nil {
			return clientDisconnected(err)
		}

		switch messageType {
		case websocket.MessageBinary:
			err := stream.Send(&asrv1.StreamingRecognizeRequest{
				Data: data,
			})
			switch {
			case err == nil:
				continue
			case errors.Is(err, io.EOF):
				// Send 返回 io.EOF 不表示 RPC 的最终状态。Worker 可能已经拒绝请求，而真正的 gRPC Status 要由 download 中的 Recv 得到。
				return s.waitAfterSendEOF(ctx)
			default:
				return sessionResult{
					kind: resultWorkerFailed,
					err:  fmt.Errorf("send audio to worker: %w", err),
				}
			}

		case websocket.MessageText:
			if err := parseEnd(data); err != nil {
				return protocolViolation(err)
			}

			// 必须先记录 End，再半关闭 RPC；Worker 可能立即返回最终 EOF。
			close(inputEnded)

			// End 只关闭 gRPC Request 方向。
			// 即使 CloseSend 返回错误，最终 RPC Status 仍应该尽量让 download/Recv 来确定。
			_ = stream.CloseSend()
			return s.waitAfterEnd(ctx)

		default:
			return protocolViolation(fmt.Errorf("unsupported websocket message type: %v", messageType))
		}
	}
}

// waitAfterEnd 在客户端发送 End 后继续监测 WebSocket。
//
// Worker 正常/异常结束时，session.run 会主动 Close WebSocket，从而唤醒这里正在阻塞的 Read。
func (s *session) waitAfterEnd(ctx context.Context) sessionResult {
	messageType, _, err := s.ws.Read(ctx)
	if err != nil {
		return clientDisconnected(err)
	}

	return protocolViolation(fmt.Errorf("message received after end: %v", messageType))
}

// waitAfterSendEOF 在 gRPC Send 返回 io.EOF 后继续保持WebSocket 读取能力。
func (s *session) waitAfterSendEOF(ctx context.Context) sessionResult {
	for {
		_, _, err := s.ws.Read(ctx)
		if err != nil {
			return clientDisconnected(err)
		}
	}
}

// parseEnd 校验音频阶段收到的 Text Message 是否为 End。
func parseEnd(data []byte) error {
	var message struct {
		Type wsprotocol.MessageType `json:"type"`
	}
	if err := json.Unmarshal(data, &message); err != nil {
		return fmt.Errorf("decode control message: %w", err)
	}
	if message.Type != wsprotocol.MessageTypeEnd {
		return fmt.Errorf("unexpected control message %q", message.Type)
	}
	return nil
}
