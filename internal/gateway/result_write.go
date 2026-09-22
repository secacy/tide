package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// ErrResultWriteTimeout 表示单条结果写回客户端超时。
var ErrResultWriteTimeout = errors.New("result write timeout")

// writeResult 编码并同步写回一条识别结果。
// 期限只覆盖 WebSocket Write，不包含 JSON 编码。
// 单个会话只能由一个结果写入者串行调用。
// 本方法不取消 RPC、不归还会话名额。
func (s *session) writeResult(ctx context.Context, message wsprotocol.ResultMessage) error {
	if s.resultWriteTimeout <= 0 {
		return fmt.Errorf("result write timeout must be positive: %s", s.resultWriteTimeout)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal result message: %w", err)
	}
	writeCtx, cancelWrite := context.WithTimeout(ctx, s.resultWriteTimeout)
	s.resultWriteMu.Lock()
	s.resultWriteCtx = writeCtx
	s.resultWriteMu.Unlock()
	writeErr := s.ws.Write(writeCtx, websocket.MessageText, data)
	writeContextErr := writeCtx.Err()
	cancelWrite()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(writeContextErr, context.DeadlineExceeded) {
		return ErrResultWriteTimeout
	}
	if writeErr != nil {
		return fmt.Errorf("write result message: %w", writeErr)
	}
	return nil
}

// resultWriteExpired 判断最近一次结果写入是否因期限到期而结束。
// 尚未写入，或成功后主动取消 context，都不属于写入超时。
func (s *session) resultWriteExpired() bool {
	s.resultWriteMu.Lock()
	defer s.resultWriteMu.Unlock()
	return s.resultWriteCtx != nil && errors.Is(s.resultWriteCtx.Err(), context.DeadlineExceeded)
}
