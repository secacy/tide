package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// Config 描述 WebSocket Gateway 配置。
type Config struct {
	MaxMessageBytes int64 // Gateway 允许接收的单个 WebSocket Message 上限, 这个值只是安全限制
}

// Gateway 把 WebSocket 音频流桥接到 gRPC Worker。
type Gateway struct {
	ctx    context.Context
	worker asrv1.ASRServiceClient
	cfg    Config
}

// New 创建一个 Gateway。
//
// ctx 应该是应用级生命周期 Context。
// 服务关闭时取消 ctx，可以同时结束所有正在运行的 WebSocket session。
func New(ctx context.Context, worker asrv1.ASRServiceClient, cfg Config) (*Gateway, error) {
	if ctx == nil {
		return nil, fmt.Errorf("gateway context is nil")
	}
	if worker == nil {
		return nil, fmt.Errorf("audio worker client is nil")
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 1024 * 1024 // 1 MiB
	}
	return &Gateway{
		ctx:    ctx,
		worker: worker,
		cfg:    cfg,
	}, nil
}

// ServeHTTP 接收 WebSocket 连接并启动一次音频 Session.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()

	conn.SetReadLimit(g.cfg.MaxMessageBytes)

	ctx, cancel := context.WithCancel(g.ctx)
	defer cancel()

	if err := g.handleSession(ctx, conn); err != nil {
		// 当前阶段先使用 WebSocket Close Status 表示 session 失败。
		//
		// 后续如果确实需要让客户端获得详细业务错误，
		// 再增加 ErrorMessage 即可。
		_ = conn.Close(websocket.StatusInternalError, "audio session failed")
		return
	}

	// Worker 正常结束后，再完成 WebSocket Close Handshake。
	_ = conn.Close(websocket.StatusNormalClosure, "completed")
}

func (g *Gateway) handleSession(ctx context.Context, conn *websocket.Conn) error {
	// 第一个 WebSocket Message 必须是 StartMessage。
	if err := readStart(ctx, conn); err != nil {
		return fmt.Errorf("read start message: %w", err)
	}

	// Start 校验通过以后才建立 gRPC stream。
	stream, err := g.worker.StreamingRecognize(ctx)
	if err != nil {
		return fmt.Errorf("open worker stream: %w", err)
	}

	session := &session{
		ws:     conn,
		worker: stream,
	}

	if err := session.run(ctx); err != nil {
		return fmt.Errorf("run audio session: %w", err)
	}

	return nil
}

func readStart(ctx context.Context, conn *websocket.Conn) error {
	messageType, data, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read websocket start message: %w", err)
	}

	if messageType != websocket.MessageText {
		return fmt.Errorf("first websocket message must be text")
	}

	var start wsprotocol.StartMessage
	if err := json.Unmarshal(data, &start); err != nil {
		return fmt.Errorf("decode start message: %w", err)
	}

	if start.Type != wsprotocol.MessageTypeStart {
		return fmt.Errorf("first message must be start, got %q", start.Type)
	}

	if start.Version != "v1" {
		return fmt.Errorf("unsupported protocol version %q", start.Version)
	}

	return nil
}

// forwardResponses 持续读取 Worker 返回结果。
func (s *session) forwardResponses(ctx context.Context) error {
	for {
		resp, err := s.worker.Recv()

		switch {
		case errors.Is(err, io.EOF):
			return errWorkerCompleted
		case err != nil:
			return fmt.Errorf("receive worker response: %w", err)
		}

		result := wsprotocol.ResultMessage{
			Type:  wsprotocol.MessageTypeResult,
			Text:  resp.GetText(),
			Final: resp.GetIsFinal(),
		}

		if err := writeJSON(ctx, s.ws, result); err != nil {
			return fmt.Errorf("write result to websocket: %w", err)
		}
	}
}

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
