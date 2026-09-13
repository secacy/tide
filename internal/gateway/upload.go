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

// readAudio 独占 WebSocket 读取；只尝试入队，不等待 Worker 或队列空位。
// 仅合法 End 关闭队列输入；异常退出由协调者取消 Sender。
func (s *session) readAudio(ctx context.Context, queue *audioQueue) sessionResult {
	for {
		messageType, data, err := s.ws.Read(ctx)
		if err != nil {
			return clientDisconnected(err)
		}
		switch messageType {
		case websocket.MessageBinary:
			if s.recovery != nil {
				var err error
				data, err = s.recovery.decodeAudio(data)
				if err != nil {
					return protocolViolation(err)
				}
			}
			if err := queue.tryPush(data); err != nil {
				if errors.Is(err, errProgressCapacity) || errors.Is(err, errProcessingTimeout) || errors.Is(err, errEndTimeout) {
					return processingFailure(err)
				}
				if errors.Is(err, errAudioQueueFull) {
					return sessionResult{kind: resultOverloaded, err: err}
				}
				return protocolViolation(err)
			}
		case websocket.MessageText:
			if err := s.parseEndMessage(data); err != nil {
				return protocolViolation(err)
			}
			queue.closeInput()
			return s.waitAfterEnd(ctx)
		default:
			return protocolViolation(fmt.Errorf("unsupported websocket message type: %v", messageType))
		}
	}
}

// sendAudio 独占 Send 和 CloseSend。ctx 使用 RPC 的取消范围，
// 从而在协调者取消 RPC 后立即解除空队列等待，无需等待 WebSocket 关闭握手。
func sendAudio(ctx context.Context, stream workerStream, queue *audioQueue, requestClosing chan<- struct{}) sessionResult {
	var seq uint64
	for {
		data, err := queue.pop(ctx)
		if errors.Is(err, io.EOF) {
			close(requestClosing)
			if err := stream.CloseSend(); err != nil {
				return sessionResult{kind: resultSendStopped, err: fmt.Errorf("close worker input: %w", err)}
			}
			return sessionResult{kind: resultInputSent}
		}
		if err != nil {
			return sessionResult{kind: resultWorkerFailed, err: fmt.Errorf("wait for audio: %w", err)}
		}
		seq++
		if err := queue.progress.startSend(seq); err != nil {
			return processingFailure(err)
		}
		err = stream.Send(&asrv1.StreamingRecognizeRequest{Data: data, AudioSeq: seq})
		switch {
		case err == nil:
			continue
		case errors.Is(err, io.EOF):
			// Send EOF 只表示不能继续发送；保留 Reader，由 Recv 判断 RPC 状态。
			return sessionResult{kind: resultSendStopped, err: fmt.Errorf("worker stopped accepting audio: %w", err)}
		default:
			return sessionResult{kind: resultWorkerFailed, err: fmt.Errorf("send audio to worker: %w", err)}
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
