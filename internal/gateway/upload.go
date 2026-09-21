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

// readInput 在音频输入阶段读取一条完整 WebSocket 消息。
//
// 它只负责读取与超时分类，不校验 audio/end 协议，
// 不向 Worker 转发数据，也不决定整个会话的最终结果。
func (s *session) readInput(ctx context.Context) (websocket.MessageType, []byte, error) {
	// 每次调用创建独立期限，成功读取后立即释放计时器。
	readCtx, cancelRead := context.WithTimeout(ctx, s.inputIdleTimeout)

	// 在 Read 可能触发连接关闭之前，公开这次读取的期限状态。
	s.inputReadMu.Lock()
	s.inputReadCtx = readCtx
	s.inputReadMu.Unlock()

	messageType, data, readErr := s.ws.Read(readCtx)
	readContextErr := readCtx.Err()
	cancelRead()

	if ctx.Err() != nil {
		return 0, nil, ctx.Err()
	}
	if errors.Is(readContextErr, context.DeadlineExceeded) {
		return 0, nil, ErrInputIdleTimeout
	}
	return messageType, data, readErr
}

// upload 读取客户端输入并转发音频。
// wsCtx 控制客户端读取；rpcCtx 与 cancelRPC 属于正在使用的 Worker stream。
func (s *session) upload(
	wsCtx context.Context,
	rpcCtx context.Context,
	cancelRPC context.CancelCauseFunc,
	stream workerStream,
	inputEnded chan<- struct{},
) sessionResult {
	for {
		messageType, data, err := s.readInput(wsCtx)
		if err != nil {
			// 输入超时是独立的退出原因，不属于客户端主动断开。
			if errors.Is(err, ErrInputIdleTimeout) {
				return sessionResult{
					kind: resultInputIdleTimeout,
					err:  err,
				}
			}
			return clientDisconnected(err)
		}

		switch messageType {
		case websocket.MessageBinary:
			err := sendWithTimeout(
				rpcCtx,
				cancelRPC,
				stream,
				&asrv1.StreamingRecognizeRequest{Data: data},
				s.workerSendTimeout,
			)
			switch {
			case err == nil:
				continue
			// ErrWorkerSendTimeout → 返回独立的发送超时结果
			case errors.Is(err, ErrWorkerSendTimeout):
				return sessionResult{
					kind: resultWorkerSendTimeout,
					err:  ErrWorkerSendTimeout,
				}
			case errors.Is(err, io.EOF):
				// Send 返回 io.EOF 不表示 RPC 的最终状态。Worker 可能已经拒绝请求，而真正的 gRPC Status 要由 download 中的 Recv 得到。
				return s.waitAfterSendEOF(wsCtx)
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
			return s.waitAfterEnd(wsCtx)

		default:
			return protocolViolation(fmt.Errorf("unsupported websocket message type: %v", messageType))
		}
	}
}

// inputIdleExpired 检查最近一次输入读取是否因期限到期而结束。
// 主动取消读取产生的 context.Canceled 不属于输入空闲超时。
func (s *session) inputIdleExpired() bool {
	s.inputReadMu.Lock()
	readCtx := s.inputReadCtx
	s.inputReadMu.Unlock()

	return readCtx != nil && errors.Is(readCtx.Err(), context.DeadlineExceeded)
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
