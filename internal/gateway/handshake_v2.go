package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// ErrHandshakeTimeout 表示首条消息读取或校验超过期限。
var ErrHandshakeTimeout = errors.New("handshake message timeout")

// errInvalidV2HandshakeReaderConfig 表示参数非法。
var errInvalidV2HandshakeReaderConfig = errors.New("invalid v2 handshake reader config")

// readV2Handshake 同步读取并校验恰好一条首条文本消息。
// ctx 是入口操作生命周期。
// timeout 限制本次读取与校验，必须为正。
// maxMessageBytes 是握手消息上限，必须为正。
// 连接仍由调用方拥有；本函数不写 ready，也不关闭连接。
// 返回前必须等待实际 Read 返回；错误时请求值始终为零。
func readV2Handshake(
	ctx context.Context,
	conn connectionReadConn,
	timeout time.Duration,
	maxMessageBytes int64,
) (wsprotocol.V2Handshake, error) {
	if ctx == nil || conn == nil || timeout <= 0 || maxMessageBytes <= 0 {
		return wsprotocol.V2Handshake{}, errInvalidV2HandshakeReaderConfig
	}
	if cause := context.Cause(ctx); cause != nil {
		return wsprotocol.V2Handshake{}, cause
	}
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 不由底层 Read 的返回值决定取消与超时的竞争顺序。
	stopCause := func() error {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if errors.Is(readCtx.Err(), context.DeadlineExceeded) {
			return ErrHandshakeTimeout
		}
		return nil
	}
	conn.SetReadLimit(maxMessageBytes)
	if cause := stopCause(); cause != nil {
		return wsprotocol.V2Handshake{}, cause
	}
	typ, data, err := conn.Read(readCtx)
	if cause := stopCause(); cause != nil {
		return wsprotocol.V2Handshake{}, cause
	}
	if err != nil {
		return wsprotocol.V2Handshake{}, fmt.Errorf("read v2 handshake: %w", err)
	}
	if typ != websocket.MessageText {
		return wsprotocol.V2Handshake{}, fmt.Errorf("%w: first message must be text", wsprotocol.ErrInvalidV2Handshake)
	}
	handshake, err := wsprotocol.DecodeV2Handshake(data)
	if cause := stopCause(); cause != nil {
		return wsprotocol.V2Handshake{}, cause
	}
	if err != nil {
		return wsprotocol.V2Handshake{}, err
	}
	return handshake, nil
}
