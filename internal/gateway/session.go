package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
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
	recovery *recoveryState         // v2 attempt state, bound after Start; nil preserves v1.
	id       string                 // 标识本次实时会话，在登记前确定
	ws       *websocket.Conn        // 接入流程在 run 前绑定一次；负责消息读写和关闭握手，abort 不访问此字段。
	worker   asrv1.ASRServiceClient // 用于创建本会话的后端识别流

	ctx       context.Context         // 控制本会话的识别工作，在创建会话时确定
	cancel    context.CancelCauseFunc // 取消本会话并记录取消原因。强制停止时传入 errSessionAborted，正常释放时传入 nil。
	controlMu sync.Mutex              // 保护 aborted 和 transport。持锁期间不得进行网络操作，也不得等待会话退出。
	aborted   bool                    // 表示已经请求强制停止，只能从 false 变为 true。
	transport net.Conn                // 升级接管的底层连接，只绑定一次；用于强制中断网络读写和关闭握手。
}

// errSessionAborted 表示会话收到显式的强制停止请求。
var errSessionAborted = errors.New("session aborted")

// sessionResultKind 区分会话失败、Worker 完成以及 Sender 的阶段事件。
type sessionResultKind uint8

const (
	resultCompleted          sessionResultKind = iota // download 已读取 Worker 正常 EOF，协调者仍需确认 Sender 完成。
	resultWorkerFailed                                // Worker 建流或识别失败。
	resultClientDisconnected                          // 客户端连接断开。
	resultProtocolViolation                           // 客户端违反会话协议。
	resultServerStopping                              // 父 Context 取消，服务正在停止。
	resultAborted                                     // 会话收到显式强制停止请求。
	resultOverloaded                                  // 音频积压达到上限，无法继续接纳音频。
	resultProcessingTimedOut                          // 最早未确认处理音频超过期限。
	resultEndTimedOut                                 // End 后超过完成期限。
	resultWriteTimedOut                               // 单次结果写入超过等待期限。
	resultInputSent                                   // Sender 已排空并完成 CloseSend；尚不代表 Worker 完成。
	resultSendStopped                                 // Send EOF 或 CloseSend 失败，等待 Recv 给出 RPC 最终状态。
	resultHeartbeatFailed                             // Ping/Pong 未能在预算内完成；不等同于 Worker 失败。
)

// sessionResult 是 Reader、Sender、download 向 session.run 汇报的事件。
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
// cfg 必须已经由 Gateway.New 填充默认值并校验。
// 返回前等待内部 I/O goroutine 退出；Context 释放和注销由接入流程负责。
//
// 它是唯一负责以下事情的位置：
//
//   - 判断 Session 最终结果；
//   - 决定是否取消 gRPC；
//   - 决定 WebSocket Close Code；
//   - 唤醒仍然阻塞的 I/O goroutine；
//   - 等待所有 goroutine 真正退出。
func (s *session) run(cfg Config) (runErr error) {
	ctx := s.ctx
	if result, canceled := cancellationResult(ctx); canceled {
		return result.err
	}
	queue, err := newAudioQueue(cfg.AudioQueueMaxBytes, cfg.AudioQueueMaxChunks)
	if err != nil {
		return err // 调用方必须提供经 Gateway.New 校验的配置。
	}
	progress, err := newProcessingProgress(cfg.MaxUnprocessedChunks, cfg.ProcessingTimeout, cfg.EndTimeout)
	if err != nil {
		return err
	}
	queue.progress = progress
	heartbeat := wsheartbeat.Start(ctx, cfg.Heartbeat, s.ws.Ping, s.abortWithCause)
	defer func() {
		heartbeat.Stop()
		if err := heartbeat.Wait(); runErr != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) && !errors.Is(runErr, wsheartbeat.ErrFailed) {
			// 等待 Start 时也可能先由 Ping 写超时唤醒 Read。
			runErr = errors.Join(runErr, err)
		}
	}()
	start, err := readStartMessage(ctx, s.ws)
	if err != nil {
		if result, canceled := cancellationResult(ctx); canceled {
			return result.err
		}
		heartbeat.Stop()
		_ = heartbeat.Wait()
		_ = s.ws.Close(websocket.StatusPolicyViolation, "invalid start message")
		return err
	}
	if result, canceled := cancellationResult(ctx); canceled {
		return result.err
	}

	if start.Version == "v2" {
		s.recovery = newRecoveryState(start)
	}

	// RPC 从建流阶段起就响应应用取消；WebSocket 保留独立的收尾时间，
	// 由 finish 在发送关闭帧后取消其 I/O。
	rpcCtx, cancelRPC := context.WithCancel(ctx)
	defer cancelRPC()

	wsCtx, cancelWS := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWS()

	// 建流放在 Sender 内；Reader 与期限协调者不等待下游准备完成。
	// 只传递一次流对象，缓冲保证取消时 Sender 不因交接而滞留。
	streams := make(chan workerStream, 1)

	// Sender 排空后、调用 CloseSend 前关闭。Worker EOF 可以先于 CloseSend 返回，
	// 因而“开始半关闭”和“半关闭调用返回”必须分开观察。
	requestClosing := make(chan struct{})

	// 三个执行流各汇报一次，即使协调者已选定结果也不会阻塞退出。
	events := make(chan sessionResult, 3)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		events <- s.readAudio(wsCtx, queue)
	}()

	go func() {
		defer wg.Done()
		stream, err := s.openStream(rpcCtx)
		if err != nil {
			events <- sessionResult{kind: resultWorkerFailed, err: fmt.Errorf("open worker stream: %w", err)}
			return
		}
		streams <- stream
		events <- sendAudio(rpcCtx, stream, queue, requestClosing)
	}()
	var downloaded sessionResult // 仅 download 写入，协调者在 wg.Wait 后读取。
	go func() {
		defer wg.Done()
		var stream workerStream
		select {
		case stream = <-streams:
		case <-rpcCtx.Done():
			return
		}
		downloaded = s.download(wsCtx, stream, cfg.ResultWriteTimeout, progress)
		events <- downloaded
	}()

	// 第一个真正具有决定性的事件确定 Session 结果。
	result := waitSessionResult(ctx, events, requestClosing, progress)

	// 业务失败先取消 RPC，不让正常停用心跳的 join 推迟处理期限。
	heartbeat.Stop()
	if result.kind != resultCompleted {
		cancelRPC()
	}
	if err := heartbeat.Wait(); err != nil {
		// 在途探测也已失败，不再向同一连接等待关闭握手；保留已选业务原因。
		_ = s.ws.CloseNow()
		if result.kind == resultCompleted {
			result = sessionResult{kind: resultHeartbeatFailed, err: err}
		} else if result.kind == resultClientDisconnected && errors.Is(err, context.DeadlineExceeded) {
			// Ping 写超时可能先关闭连接，再返回探测错误；保留两者以免日志只剩 EOF。
			result.err = errors.Join(result.err, err)
		}
	}
	// 先主动执行能够解除阻塞的 cleanup。
	cleanupErr := s.finish(wsCtx, result, cancelRPC, cancelWS)

	wg.Wait()
	// WebSocket 写入超时也会关闭连接，Reader 可能先汇报断开。
	// 保留先选定的失败，同时补充已经确认的写入超时，避免日志丢失具体原因。
	if result.kind == resultClientDisconnected && downloaded.kind == resultWriteTimedOut {
		result.err = errors.Join(result.err, downloaded.err)
	}

	if result.err != nil {
		return result.err
	}
	return cleanupErr
}

// readStart 校验当前 WebSocket Session 的第一条消息。
// V1 要求第一条消息必须是 {"type":"start","version":"v1"}
func readStartMessage(ctx context.Context, conn *websocket.Conn) (wsprotocol.StartMessage, error) {
	messageType, data, err := conn.Read(ctx)
	if err != nil {
		return wsprotocol.StartMessage{}, fmt.Errorf("read websocket start message: %w", err)
	}

	if messageType != websocket.MessageText {
		return wsprotocol.StartMessage{}, fmt.Errorf("first websocket message must be text")
	}

	var start wsprotocol.StartMessage
	if err := json.Unmarshal(data, &start); err != nil {
		return wsprotocol.StartMessage{}, fmt.Errorf("decode start message: %w", err)
	}

	if start.Type != wsprotocol.MessageTypeStart {
		return wsprotocol.StartMessage{}, fmt.Errorf("first message must be start, got %q", start.Type)
	}

	if start.Version != "v1" && start.Version != "v2" {
		return wsprotocol.StartMessage{}, fmt.Errorf("unsupported protocol version %q", start.Version)
	}

	if start.Version == "v2" && (len(start.SessionID) == 0 || len(start.SessionID) > 128 || len(start.AttemptID) == 0 || len(start.AttemptID) > 128) {
		return start, fmt.Errorf("invalid recovery identity")
	}
	return start, nil
}

// waitSessionResult 等待第一个能够决定整个 Session 的事件。
func waitSessionResult(ctx context.Context, events <-chan sessionResult, requestClosing <-chan struct{}, progress *processingProgress) sessionResult {
	inputSent, workerCompleted := false, false
	var sendErr error
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var changed <-chan struct{}
	if progress != nil {
		changed = progress.changed
	}
	for {
		deadline, err := progress.status(time.Now())
		if canceled, ok := cancellationResult(ctx); ok {
			return canceled
		}
		if err != nil {
			return processingFailure(err)
		}
		var expired <-chan time.Time
		if !deadline.IsZero() {
			timer.Reset(time.Until(deadline))
			expired = timer.C
		} else {
			timer.Stop()
		}
		var result sessionResult
		select {
		case result = <-events:
		case <-ctx.Done():
		case <-changed:
			continue
		case <-expired:
			continue
		}
		if canceled, ok := cancellationResult(ctx); ok {
			return canceled
		}
		switch result.kind {
		case resultInputSent:
			inputSent = true
		case resultSendStopped:
			sendErr = result.err
		case resultCompleted:
			select {
			case <-requestClosing:
				workerCompleted = true
			default:
				return sessionResult{kind: resultWorkerFailed, err: fmt.Errorf("worker completed before audio drain and request half-close")}
			}
		default:
			return result
		}
		if workerCompleted {
			if sendErr != nil {
				return sessionResult{kind: resultWorkerFailed, err: sendErr}
			}
			if inputSent {
				if err := progress.complete(time.Now()); err != nil {
					return processingFailure(err)
				}
				return sessionResult{kind: resultCompleted}
			}
		}
	}
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

		code, reason := websocket.StatusInternalError, "worker failed"
		if s.recovery != nil && isRecoveryUnsupported(result.err) {
			code, reason = websocket.StatusPolicyViolation, "recovery_unsupported"
		}
		if errors.Is(result.err, errInvalidRecovery) {
			code, reason = websocket.StatusPolicyViolation, "recovery_protocol"
		}
		closeErr := s.ws.Close(code, reason)

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

	case resultProcessingTimedOut, resultEndTimedOut:
		cancelRPC()
		code, reason := websocket.StatusTryAgainLater, "processing_timeout"
		if result.kind == resultEndTimedOut {
			code, reason = websocket.StatusInternalError, "end_timeout"
		}
		closeErr := s.ws.Close(code, reason)
		cancelWS()
		return closeErr

	case resultOverloaded:
		cancelRPC() // 先解除 Sender 的 Send/pop 等待，再尝试通知客户端。
		reason := "audio queue full"
		if errors.Is(result.err, errProgressCapacity) {
			reason = "progress_capacity"
		}
		closeErr := s.ws.Close(websocket.StatusTryAgainLater, reason)
		cancelWS()
		return closeErr

	case resultClientDisconnected, resultWriteTimedOut:
		// Client 已经消失，没有必要再尝试 graceful close handshake。
		cancelRPC()
		cancelWS()

		_ = s.ws.CloseNow()
		return nil

	case resultAborted, resultHeartbeatFailed:
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
