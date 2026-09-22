package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// workerStream 是 Gateway 对生成的 gRPC Stream 的最小依赖。
//
// 定义这个小接口的好处是：
//   - Gateway 不依赖具体生成的 Stream 类型名；
//   - 后续容易编写 Mock；
//   - 明确表达本层真正使用的能力。
type workerStream interface {
	Send(*asrv1.StreamingRecognizeRequest) error
	Recv() (*asrv1.StreamingRecognizeResponse, error)
	CloseSend() error
}

// session 表示一个 WebSocket Connection 与一个 gRPC stream 之间的一对一桥接关系。
type session struct {
	ws     *websocket.Conn
	worker asrv1.ASRServiceClient

	startTimeout     time.Duration // 等待完整 start 消息的期限
	inputIdleTimeout time.Duration // 限制音频输入阶段单次读取消息的等待时间

	inputReadMu  sync.Mutex      // 保护 inputReadCtx 的读写。只在保存或取得 context 时持有，不覆盖网络 I/O。
	inputReadCtx context.Context // 保存最近一次音频输入读取的 context。读取结束后仍保留，用于判定本次连接关闭是否伴随输入超时。

	workerSendTimeout  time.Duration   // 限制单次向 Worker 发送音频的等待时间
	tailTimeout        time.Duration   // 输入结束后的完成等待预算，不限制正常问诊时长
	resultWriteTimeout time.Duration   // 限制单条结果的 WebSocket Write 等待
	resultWriteMu      sync.Mutex      // 只保护 resultWriteCtx，不覆盖编码或网络写入
	resultWriteCtx     context.Context // 保存最近一次写入的 context。写入结束后仍保留，供协调者判断连接关闭是否伴随写入超时。
}

// sessionResultKind 描述能够决定整个 Session 结果的事件。
type sessionResultKind uint8

const (
	resultCompleted sessionResultKind = iota
	resultWorkerFailed
	resultClientDisconnected
	resultProtocolViolation
	resultServerStopping
	resultInputIdleTimeout   // 等待下一条完整输入消息超时
	resultWorkerSendTimeout  // 单次音频发送等待超时，已触发 RPC 取消
	resultTailTimeout        // 表示输入结束后，等待剩余结果及响应流结束超时
	resultResultWriteTimeout // 表示单条识别结果写回客户端超时
)

// ErrStartTimeout 表示会话未能在规定时间内完成 start 消息的读取与校验。
var ErrStartTimeout = errors.New("start message timeout")

// sessionResult 是 upload/download 向 session.run 汇报的结果。
//
// I/O goroutine 不负责决定 WebSocket 应该如何关闭。它们只负责描述“发生了什么”。
type sessionResult struct {
	kind sessionResultKind
	err  error
}

// ErrInputIdleTimeout 表示等待下一条完整输入消息超时。
var ErrInputIdleTimeout = errors.New("input idle timeout")

// newSession 创建会话。
func newSession(ws *websocket.Conn, worker asrv1.ASRServiceClient, startTimeout time.Duration, inputIdleTimeout time.Duration, workerSendTimeout time.Duration, tailTimeout time.Duration, resultWriteTimeout time.Duration) *session {
	return &session{
		ws:                 ws,
		worker:             worker,
		startTimeout:       startTimeout,
		inputIdleTimeout:   inputIdleTimeout,
		workerSendTimeout:  workerSendTimeout,
		tailTimeout:        tailTimeout,
		resultWriteTimeout: resultWriteTimeout,
	}
}

// run 管理一个音频 Session 的完整生命周期。
func (s *session) run(ctx context.Context) error {
	// 创建读取期限，同时继承服务取消
	startCtx, cancelStart := context.WithTimeout(ctx, s.startTimeout)

	// 先完成读取，再保存 context 状态，最后释放计时器
	readErr := readStart(startCtx, s.ws)
	startContextErr := startCtx.Err()
	cancelStart()

	if err := ctx.Err(); err != nil {
		_ = s.ws.CloseNow()
		return err
	}
	if errors.Is(startContextErr, context.DeadlineExceeded) {
		// 释放连接，返回 ErrStartTimeout
		_ = s.ws.CloseNow()
		return ErrStartTimeout
	}
	if readErr != nil {
		// 处理其他读取或协议校验错误
		_ = s.ws.Close(websocket.StatusPolicyViolation, "invalid start message")
		return readErr
	}

	// RPC 从建流阶段起就响应应用取消；WebSocket 保留独立的收尾时间，由 finish 在发送关闭帧后取消其 I/O。
	rpcCtx, cancelRPCWithCause := context.WithCancelCause(ctx)
	// 普通清理不主动指定业务原因；已有取消原因不会被覆盖。
	cancelRPC := func() {
		cancelRPCWithCause(nil)
	}
	defer cancelRPC()

	wsCtx, cancelWS := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWS()

	stream, err := s.worker.StreamingRecognize(rpcCtx)
	if err != nil {
		result := sessionResult{
			kind: resultWorkerFailed,
			err:  fmt.Errorf("open worker stream: %w", err),
		}
		if ctx.Err() != nil {
			result = sessionResult{kind: resultServerStopping, err: ctx.Err()}
		}
		_ = s.finish(wsCtx, result, cancelRPC, cancelWS)
		return result.err
	}

	// 只有 upload 能关闭它，表示已收到合法的 End。
	inputEnded := make(chan struct{})

	// upload 和 download 每个最多发送一个退出事件。
	events := make(chan sessionResult, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		events <- s.upload(wsCtx, rpcCtx, cancelRPCWithCause, stream, inputEnded)
	}()

	go func() {
		defer wg.Done()
		events <- s.download(wsCtx, stream)
	}()

	// 第一个真正具有决定性的事件确定 Session 结果。
	result := s.waitSessionResult(ctx, rpcCtx, events, inputEnded)

	// 先主动执行能够解除阻塞的 cleanup。
	cleanupErr := s.finish(wsCtx, result, cancelRPC, cancelWS)

	wg.Wait()

	if result.err != nil {
		return result.err
	}
	return cleanupErr
}

// readStart 校验当前 WebSocket Session 的第一条消息。
// V1 要求第一条消息必须是 {"type":"start","version":"v1"}
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

// waitSessionResult 等待退出事件，并在清理开始前确定会话结果。
// 判定顺序：服务停止、发送超时、输入空闲超时、收到的退出事件。
func (s *session) waitSessionResult(ctx context.Context, rpcCtx context.Context, events <-chan sessionResult, inputEnded <-chan struct{}) sessionResult {
	result := waitSessionEvent(ctx, events, inputEnded, s.tailTimeout)

	// 应用取消和 RPC 的取消错误可能同时到达。统一归类为服务停止，
	// 避免 select 的调度顺序让关闭码在 1001 和 1011 之间变化。
	if err := ctx.Err(); err != nil {
		return sessionResult{
			kind: resultServerStopping,
			err:  err,
		}
	}

	if errors.Is(context.Cause(rpcCtx), ErrWorkerSendTimeout) {
		return sessionResult{
			kind: resultWorkerSendTimeout,
			err:  ErrWorkerSendTimeout,
		}
	}

	if s.inputIdleExpired() {
		return sessionResult{
			kind: resultInputIdleTimeout,
			err:  ErrInputIdleTimeout,
		}
	}

	if s.resultWriteExpired() {
		return sessionResult{
			kind: resultResultWriteTimeout,
			err:  ErrResultWriteTimeout,
		}
	}

	if result.kind == resultCompleted {
		select {
		case <-inputEnded:
			// download 已确认 Worker 正常 EOF，且此前的结果都已转发。
		default:
			return sessionResult{
				kind: resultWorkerFailed,
				err:  fmt.Errorf("worker completed before client end"),
			}
		}
	}
	return result
}

// finish 根据已经确定的 Session 结果执行收尾。
func (s *session) finish(
	wsCtx context.Context,
	result sessionResult,
	cancelRPC context.CancelFunc,
	cancelWS context.CancelFunc,
) error {
	switch result.kind {
	case resultCompleted:
		// 确保先完成WebSocket正常关闭握手再清理剩余context
		closeErr := s.ws.Close(websocket.StatusNormalClosure, "completed")

		cancelRPC()
		cancelWS()

		if closeErr != nil {
			return fmt.Errorf("close completed websocket session: %w", closeErr)
		}

		return nil

	case resultWorkerFailed:
		// Worker 已经失败。首先立即取消 RPC，使可能仍阻塞在 Send 的 upload 退出。
		// 但是不要马上 cancel WebSocket Context，因为我们还需要发送 1011 close frame。
		cancelRPC()

		closeErr := s.ws.Close(websocket.StatusInternalError, "worker failed")

		cancelWS()

		if closeErr != nil {
			return fmt.Errorf("close failed websocket session: %w", closeErr)
		}

		return nil

	case resultProtocolViolation:
		cancelRPC()

		closeErr := s.ws.Close(websocket.StatusPolicyViolation, "protocol violation")
		cancelWS()

		if closeErr != nil {
			return fmt.Errorf("close protocol violation session: %w", closeErr)
		}

		return nil

	case resultClientDisconnected:
		// Client 已经消失，没有必要再尝试 graceful close handshake。
		cancelRPC()
		cancelWS()

		_ = s.ws.CloseNow()
		return nil

	case resultServerStopping:
		// Server shutdown 时先停止后端 RPC，再尽量通知 WebSocket Client 服务正在退出。
		cancelRPC()

		closeErr := s.ws.Close(websocket.StatusGoingAway, "server shutting down")
		cancelWS()

		if closeErr != nil {
			return fmt.Errorf("close websocket during shutdown: %w", closeErr)
		}

		return nil

	case resultInputIdleTimeout:
		// 读取超时可能已经关闭 WebSocket。
		// 取消剩余 I/O，并兜底释放连接，不再等待关闭握手。
		cancelRPC()
		cancelWS()
		_ = s.ws.CloseNow()
		return nil

	case resultWorkerSendTimeout:
		cancelRPC()
		closeErr := s.ws.Close(websocket.StatusInternalError, "worker send timeout")
		cancelWS()
		if closeErr != nil {
			return fmt.Errorf("worker send timeout: %w", closeErr)
		}
		return nil

	case resultTailTimeout:
		cancelRPC()
		closeErr := s.ws.Close(websocket.StatusInternalError, "tail timeout")
		cancelWS()
		if closeErr != nil {
			return fmt.Errorf("tail timeout: %w", closeErr)
		}
		return nil

	case resultResultWriteTimeout:
		cancelRPC()
		cancelWS()
		_ = s.ws.CloseNow()
		return nil

	default:
		cancelRPC()
		cancelWS()
		_ = s.ws.CloseNow()

		return fmt.Errorf("unknown session result: %d", result.kind)
	}
}
