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

var errClientDisconnected = errors.New("websocket client disconnected")

// upload 负责 WebSocket -> gRPC 方向。
func (s *session) upload(ctx context.Context, stream workerStream) sessionResult {
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

// forwardAudioUntilEnd 持续读取 WebSocket 消息。
//
// 返回 nil 表示已经正常收到 End。
func (s *session) forwardAudioUntilEnd(ctx context.Context, stream asrv1.ASRService_StreamingRecognizeClient) error {
	for {
		messageType, data, err := s.ws.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: %v", errClientDisconnected, err)
		}

		switch messageType {
		case websocket.MessageBinary:
			if err := forwardAudio(stream, data); err != nil {
				return err
			}

		case websocket.MessageText:
			if err := parseEnd(data); err != nil {
				return err
			}
			return nil

		default:
			return fmt.Errorf("unsupported websocket message type: %v", messageType)
		}
	}
}

// forwardAudio 把一个 PCM WebSocket Message 立即转发给 Worker。
func forwardAudio(stream asrv1.ASRService_StreamingRecognizeClient, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty PCM message")
	}
	request := &asrv1.StreamingRecognizeRequest{
		Data: data,
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send PCM to worker: %w", err)
	}
	return nil
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
