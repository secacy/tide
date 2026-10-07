package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// v2EntryError 将内部入口错误映射为固定公开消息。
// 使用 errors.Is 识别包装错误，不暴露凭据或后端错误文本。
func v2EntryError(err error) wsprotocol.V2ErrorMessage {
	code, message := wsprotocol.V2ErrorInternalError, "internal error"
	// 显式入口类别优先于通用 context 期限，避免建流的下游超时被误判。
	switch {
	case errors.Is(err, errGatewayStopping):
		code, message = wsprotocol.V2ErrorServiceStopping, "service is stopping"
	case errors.Is(err, errHandshakeLimit):
		code, message = wsprotocol.V2ErrorHandshakeLimit, "handshake limit exceeded"
	case errors.Is(err, errSessionLimit):
		code, message = wsprotocol.V2ErrorSessionLimit, "session limit exceeded"
	case errors.Is(err, wsprotocol.ErrInvalidV2Handshake), errors.Is(err, websocket.ErrMessageTooBig):
		code, message = wsprotocol.V2ErrorInvalidHandshake, "invalid handshake"
	case errors.Is(err, errResumeUnavailable), errors.Is(err, errResumeClosed), errors.Is(err, errResumeExpired), errors.Is(err, errResumeGenerationExhausted):
		code, message = wsprotocol.V2ErrorResumeUnavailable, "resume unavailable"
	case errors.Is(err, errResumeAlreadyAttached), errors.Is(err, errConnectionRetiring):
		code, message = wsprotocol.V2ErrorSessionBusy, "session busy"
	case errors.Is(err, errResultReplayGap):
		code, message = wsprotocol.V2ErrorReplayGap, "result replay gap"
	case errors.Is(err, errResultAckAhead):
		code, message = wsprotocol.V2ErrorInvalidResumePosition, "invalid resume position"
	case errors.Is(err, errV2WorkerUnavailable):
		code, message = wsprotocol.V2ErrorWorkerUnavailable, "worker unavailable"
	case errors.Is(err, ErrHandshakeTimeout), errors.Is(err, context.DeadlineExceeded):
		code, message = wsprotocol.V2ErrorEntryTimeout, "entry timeout"
	}
	return wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: code, Message: message}
}

// writeV2EntryHTTPError 在 WebSocket 升级前发送拒绝响应。
// 使用 HTTP 503 和相同的 JSON 错误格式。
func writeV2EntryHTTPError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	// 响应已提交；写入失败不追加另一种响应，由 handler 继续退出清理。
	_ = json.NewEncoder(w).Encode(v2EntryError(err))
}
