package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

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
	id     string                 // 标识本次实时会话，在登记前确定
	ws     *websocket.Conn        // 接入流程在 run 前绑定一次；负责消息读写和关闭握手，abort 不访问此字段。
	worker asrv1.ASRServiceClient // 用于创建本会话的后端识别流

	ctx       context.Context         // 控制本会话的识别工作，在创建会话时确定
	cancel    context.CancelCauseFunc // 取消本会话并记录取消原因。强制停止时传入 errSessionAborted，正常释放时传入 nil。
	controlMu sync.Mutex              // 保护 aborted 和 transport。持锁期间不得进行网络操作，也不得等待会话退出。
	aborted   bool                    // 表示已经请求强制停止，只能从 false 变为 true。
	transport net.Conn                // 升级接管的底层连接，只绑定一次；用于强制中断网络读写和关闭握手。
}

// errSessionAborted 表示会话收到显式的强制停止请求。
var errSessionAborted = errors.New("session aborted")

// sessionResultKind 描述能够决定整个 Session 结果的事件。
type sessionResultKind uint8

const (
	resultCompleted          sessionResultKind = iota // 客户端已结束输入，Worker 正常完成。
	resultWorkerFailed                                // Worker 建流或识别失败。
	resultClientDisconnected                          // 客户端连接断开。
	resultProtocolViolation                           // 客户端违反会话协议。
	resultServerStopping                              // 父 Context 取消，服务正在停止。
	resultAborted                                     // 会话收到显式强制停止请求。
)

// sessionResult 是 upload/download 向 session.run 汇报的结果。
//
// I/O goroutine 不负责决定 WebSocket 应该如何关闭。它们只负责描述“发生了什么”。
type sessionResult struct {
	kind sessionResultKind
	err  error
}

// newSession 创建尚未绑定连接的会话，并建立独立的取消范围。
//
// parent 必须非 nil，应传入 Gateway 的生命周期 Context。
// 调用方负责在所有退出路径释放该会话的 Context。
func newSession(parent context.Context, id string, worker asrv1.ASRServiceClient) *session {
	ctx, cancel := context.WithCancelCause(parent)
	return &session{
		id:     id,
		ctx:    ctx,
		cancel: cancel,
		worker: worker,
	}
}

// run 管理一个音频 Session 的完整生命周期。
// 使用构造时确定的 s.ctx，每个 Session 只调用一次，调用前必须绑定 ws。
// 返回前等待内部 I/O goroutine 退出；Context 释放和注销由接入流程负责。
//
// 它是唯一负责以下事情的位置：
//
//   - 判断 Session 最终结果；
//   - 决定是否取消 gRPC；
//   - 决定 WebSocket Close Code；
//   - 唤醒仍然阻塞的 I/O goroutine；
//   - 等待所有 goroutine 真正退出。
func (s *session) run() error {
	ctx := s.ctx
	if result, canceled := cancellationResult(ctx); canceled {
		return result.err
	}
	if err := readStart(ctx, s.ws); err != nil {
		if result, canceled := cancellationResult(ctx); canceled {
			return result.err
		}
		_ = s.ws.Close(websocket.StatusPolicyViolation, "invalid start message")
		return err
	}
	if result, canceled := cancellationResult(ctx); canceled {
		return result.err
	}

	// RPC 从建流阶段起就响应应用取消；WebSocket 保留独立的收尾时间，
	// 由 finish 在发送关闭帧后取消其 I/O。
	rpcCtx, cancelRPC := context.WithCancel(ctx)
	defer cancelRPC()

	wsCtx, cancelWS := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWS()

	stream, err := s.worker.StreamingRecognize(rpcCtx)
	if err != nil {
		result := sessionResult{
			kind: resultWorkerFailed,
			err:  fmt.Errorf("open worker stream: %w", err),
		}
		if canceledResult, canceled := cancellationResult(ctx); canceled {
			result = canceledResult
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
		events <- s.upload(wsCtx, stream, inputEnded)
	}()

	go func() {
		defer wg.Done()
		events <- s.download(wsCtx, stream)
	}()

	// 第一个真正具有决定性的事件确定 Session 结果。
	result := waitSessionResult(ctx, events, inputEnded)

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

// waitSessionResult 等待第一个能够决定整个 Session 的事件。
func waitSessionResult(ctx context.Context, events <-chan sessionResult, inputEnded <-chan struct{}) sessionResult {
	var result sessionResult
	select {
	case result = <-events:
	case <-ctx.Done():
	}

	// 取消和它引发的 I/O 错误可能同时到达，使用取消原因统一分类。
	// 结果选定后，收尾期间的后续取消不会重新覆盖这个决定。
	if canceledResult, canceled := cancellationResult(ctx); canceled {
		return canceledResult
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

	case resultAborted:
		// abort 负责底层连接中断，这里取消剩余 I/O 并清理 WebSocket 对象。
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

	default:
		cancelRPC()
		cancelWS()
		_ = s.ws.CloseNow()

		return fmt.Errorf("unknown session result: %d", result.kind)
	}
}
