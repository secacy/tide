package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/coder/websocket"
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
func (s *session) download(ctx context.Context, stream workerStream, writeTimeout time.Duration, progress *processingProgress) sessionResult {
	for {
		response, err := stream.Recv()

		switch {
		case errors.Is(err, io.EOF):
			if s.recovery != nil && (!s.recovery.ready || s.recovery.committed != s.recovery.sent.Load()) {
				return processingFailure(fmt.Errorf("%w: missing checkpoint at EOF", errInvalidRecovery))
			}
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

		if response == nil {
			return processingFailure(fmt.Errorf("%w: nil response", errInvalidProgress))
		}
		if s.recovery != nil {
			message, handled, err := s.recovery.response(response)
			if err != nil {
				return processingFailure(fmt.Errorf("%w: %w", errInvalidRecovery, err))
			}
			if handled {
				if err := s.writeRecovery(ctx, message, writeTimeout); err.kind != resultCompleted {
					return err
				}
				continue
			}
		} else if response.Ready || response.Checkpoint != nil {
			return processingFailure(fmt.Errorf("recovery response in v1 stream"))
		}
		if response.Progress != nil {
			if response.SegmentId != "" || response.Text != "" || response.IsFinal {
				return processingFailure(fmt.Errorf("%w: mixed progress and result", errInvalidProgress))
			}
			if err := progress.acknowledge(response.Progress.ProcessedThroughSeq, time.Now()); err != nil {
				return processingFailure(err)
			}
			continue
		}
		message := toResultMessage(response)
		// 每个结果单独计时，成功后立即释放计时器，不占用下一条结果的预算。
		writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = writeJSON(writeCtx, s.ws, message)
		writeCtxErr := writeCtx.Err()
		cancel()
		if err != nil {
			if errors.Is(writeCtxErr, context.DeadlineExceeded) {
				return sessionResult{kind: resultWriteTimedOut, err: fmt.Errorf("%w after %s: %w", errResultWriteTimeout, writeTimeout, err)}
			}
			return sessionResult{
				kind: resultClientDisconnected,
				err:  fmt.Errorf("write result to websocket: %w", err),
			}
		}
	}
}

// errResultWriteTimeout 表示会话因持续无法发送转录结果而失败。
var errResultWriteTimeout = errors.New("result write timed out")

// toResultMessage 将 Worker 的 gRPC 响应转换为 WebSocket 协议消息。
func toResultMessage(response *asrv1.StreamingRecognizeResponse) wsprotocol.ResultMessage {
	return wsprotocol.ResultMessage{
		Type:      wsprotocol.MessageTypeResult,
		SegmentID: response.GetSegmentId(),
		Text:      response.GetText(),
		IsFinal:   response.GetIsFinal(),
	}
}

// writeJSON 把 Gateway 控制/结果消息编码为 WebSocket Text Message。
func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode websocket message: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("write websocket message: %w", err)
	}
	return nil
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
