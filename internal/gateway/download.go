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
			return sessionResult{
				kind: resultCompleted,
			}
		case err != nil:
			return sessionResult{
				kind: resultWorkerFailed,
				err:  fmt.Errorf("receive worker response: %w", err),
			}
		}

		message := toResultMessage(response)
		if err := writeJSON(ctx, s.ws, message); err != nil {
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
