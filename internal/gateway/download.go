package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// download 负责 gRPC -> WebSocket 方向。
//
// 对于 RPC 最终状态，Recv 是权威来源：
//
//	Recv() == nil -> 普通 Worker response
//	Recv() == io.EOF -> RPC 正常完成
//	Recv() == gRPC status error -> RPC 失败
func (s *session) download(ctx context.Context, stream workerStream) sessionResult {
	for {
		response, err := stream.Recv()

		switch {
		case errors.Is(err, io.EOF):
			// RPC 正常结束；是否满足会话成功条件，由协调者结合 End 判断。
			return sessionResult{
				kind: resultCompleted,
			}
		case err != nil:
			return sessionResult{
				kind: resultWorkerFailed,
				err:  fmt.Errorf("receive worker response: %w", err),
			}
		}

		if response.GetProgress() != nil {
			// 校验不能同时携带文本结果字段
			if response.GetIsFinal() || response.GetSegmentId() != "" || response.GetText() != "" {
				return sessionResult{
					kind: resultWorkerFailed,
					err:  fmt.Errorf("worker response progress cannot be combined with text result fields"),
				}
			}
			if err := s.progress.acknowledge(response.GetProgress().GetProcessedAudioBytes()); err != nil {
				return sessionResult{
					kind: resultWorkerFailed,
					err:  fmt.Errorf("acknowledge worker response: %w", err),
				}
			}
			// 处理进度属于网关内部控制信息，不作为识别结果发送给客户端。
			continue
		}

		message := toResultMessage(response)
		if err := s.writeResult(ctx, message); err != nil {
			if errors.Is(err, ErrResultWriteTimeout) {
				return sessionResult{
					kind: resultResultWriteTimeout,
					err:  ErrResultWriteTimeout,
				}
			}
			return sessionResult{
				kind: resultClientDisconnected,
				err:  fmt.Errorf("write result to websocket: %w", err),
			}
		}
	}
}

// toResultMessage 将 Worker 的 gRPC 响应转换为 WebSocket 协议消息。
func toResultMessage(response *asrv1.StreamingRecognizeResponse) wsprotocol.ResultMessage {
	return wsprotocol.ResultMessage{
		Type:      wsprotocol.MessageTypeResult,
		SegmentID: response.GetSegmentId(),
		Text:      response.GetText(),
		IsFinal:   response.GetIsFinal(),
	}
}

func clientDisconnected(err error) sessionResult {
	return sessionResult{
		kind: resultClientDisconnected,
		err:  fmt.Errorf("websocket client disconnected: %w", err),
	}
}

func protocolViolation(err error) sessionResult {
	return sessionResult{
		kind: resultProtocolViolation,
		err:  err,
	}
}
