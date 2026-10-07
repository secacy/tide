package gateway

import (
	"context"
	"errors"
	"io"
	"time"
)

var errInvalidSessionControlCommand = errors.New("invalid session control command")

// sessionControlKind 表示控制循环支持的内部操作。
type sessionControlKind uint8

const (
	controlResume           sessionControlKind = iota // 尝试恢复连接，成功返回新代次。
	controlDetach                                     // 报告某代次连接断开。
	controlClose                                      // 结束恢复资格并退出控制循环。
	controlAudio                                      // 接纳当前代次的一块音频。
	controlEnd                                        // 接纳当前代次的输入终点。
	controlTakeResult                                 // 取出下一条结果并建立当前代写入授权。
	controlResultWritten                              // 报告当前代已授权结果写入成功，不释放保留结果。
	controlResultAck                                  // 接纳客户端累计应用确认，释放已确认前缀。
	controlResumeConnection                           // 同一提交中接纳恢复位置、切换代次并安装候选连接。
)

// sessionControlCommand 表示一次不可复用的内部控制请求。
type sessionControlCommand struct {
	kind       sessionControlKind          // 本次操作。
	ctx        context.Context             // 请求取消信号，仅在处理前检查。
	generation uint64                      // detach/audio/end 的连接代次。
	reply      chan<- sessionControlResult // 独立、容量为 1，循环只发送一次。
	offset     uint64                      // audio 的起点，end 的最终接纳位置；控制命令忽略。
	payload    []byte                      // 仅 audio 使用；从请求开始到回复前只读，调用方不得修改。
	resultSeq  uint64                      // resume 的客户端已应用位置；written/ack 的结果序号。其他命令忽略。
	candidate  *connectionCandidate        // 仅 controlResumeConnection 使用；命令成功以前仍由提交者负责关闭。
}

// sessionControlResult 是某次控制操作的确定结果。
// 零代次用于失败或非恢复操作；detached=false,nil 表示无需改变状态。
type sessionControlResult struct {
	generation uint64      // resume 成功时的新连接代次。
	detached   bool        // detach 是否实际将当前连接改为断开保留。
	err        error       // 状态错误、请求取消或控制循环终止原因。
	accepted   bool        // 本次是否新接纳；完整重发或重复 end 为 false。
	nextOffset uint64      // 回复时 Gateway 已连续接纳的位置，不代表 Worker 处理位置。
	offer      resultOffer // take 成功时的快照，available 可以为 false。
	handled    bool        // written 是否接纳了当前代的匹配报告。
	advanced   bool        // ACK 是否使累计确认前进；重复/旧 ACK 为 false。
}

// runControl 由生命周期拥有者恰好启动一次，串行处理控制命令。
// ctx 属于逻辑会话生命周期，不能绑定任意一条客户端连接。
// now 获取当前时间，生产传 time.Now；须与 Timer 时钟一致推进且不得阻塞。
// ctx 和 now 必须非 nil。循环退出前关闭恢复资格，再关闭 controlDone。
// 本步不操作网络、注册表或准入。
// detached 状态下由控制循环独占恢复期限 Timer；
// Timer 仅负责唤醒，实际过期判断仍由 resumeState 完成。
func (s *resumableSession) runControl(ctx context.Context, now func() time.Time) {
	_ = s.runCoordinator(ctx, now, nil, nil)
}

// runCoordinator 串行拥有连接恢复、输入、Worker 事件与结果保留状态。
// worker=nil 保留纯控制行为；退出时关闭恢复资格与 controlDone。
// connections=nil 为部件模式；非 nil 时管理固定连接事件与候选安装。
// 不在循环内等待 I/O/CloseNow；收尾由外层运行器等待，controlDone 不代表资源释放。
func (s *resumableSession) runCoordinator(
	ctx context.Context,
	now func() time.Time,
	worker *sessionWorker,
	connections *sessionConnections,
) error {
	if ctx == nil {
		panic("gateway: nil session control context")
	}
	if now == nil {
		panic("gateway: nil session control clock")
	}

	var wakeTimer *time.Timer
	var wakeC <-chan time.Time

	var tailDeadline time.Time
	var statusDeadline time.Time
	var retentionDeadline time.Time

	var recvDone <-chan struct{}
	var recvEvents <-chan workerReceiveEvent

	if worker != nil {
		recvDone = worker.receiver.done
		recvEvents = worker.receiver.events
	}

	// stopWakeTimer 停止唤醒计时器并禁用其 select 分支，可以重复调用。
	stopWakeTimer := func() {
		if wakeTimer != nil {
			wakeTimer.Stop()
		}

		wakeTimer = nil
		wakeC = nil
	}

	// nextDeadline 选择当前阶段适用的最早绝对期限；没有期限时返回 false。
	nextDeadline := func() (time.Time, bool) {
		var deadline time.Time
		var have bool

		consider := func(candidate time.Time) {
			if candidate.IsZero() {
				return
			}

			if !have || candidate.Before(deadline) {
				deadline = candidate
				have = true
			}
		}

		if s.resume.phase == resumeDetached {
			consider(s.resume.expiresAt)
		}

		if worker != nil {
			switch worker.phase {
			case workerRunning:
				consider(tailDeadline)
				consider(statusDeadline)

			case workerRetaining:
				consider(retentionDeadline)
			}
		}

		return deadline, have
	}

	// armWakeTimer 按已有绝对期限安排唤醒，不改变任何业务截止时间。
	armWakeTimer := func() {
		stopWakeTimer()

		deadline, ok := nextDeadline()
		if !ok {
			return
		}

		delay := deadline.Sub(now())
		if delay < 0 {
			delay = 0
		}

		wakeTimer = time.NewTimer(delay)
		wakeC = wakeTimer.C
	}

	// deadlineKind 标识到期原因，仅选中恢复期限时需要推进 resumeState。
	type deadlineKind uint8

	const (
		deadlineResume deadlineKind = iota
		deadlineTail
		deadlineStatus
		deadlineRetention
	)

	// deadlineCandidate 将绝对时间与到期时应报告的原因关联。
	type deadlineCandidate struct {
		at   time.Time    // 绝对截止时间；零值表示尚未建立期限。
		kind deadlineKind // 期限类别。
		err  error        // 该期限最早到期时的退出原因。
	}

	// 先选择当前适用期限中绝对时间最早的一个。
	// 截止时间相同时，由 consider 的调用顺序决定优先级。
	checkExpired := func(at time.Time) error {
		var selected deadlineCandidate
		var have bool

		consider := func(candidate deadlineCandidate) {
			if candidate.at.IsZero() {
				return
			}

			if !have || candidate.at.Before(selected.at) {
				selected = candidate
				have = true
			}
		}

		if s.resume.phase == resumeDetached {
			consider(deadlineCandidate{
				at:   s.resume.expiresAt,
				kind: deadlineResume,
				err:  errResumeExpired,
			})
		}

		if worker != nil {
			switch worker.phase {
			case workerRunning:
				consider(deadlineCandidate{
					at:   tailDeadline,
					kind: deadlineTail,
					err:  ErrTailTimeout,
				})

				consider(deadlineCandidate{
					at:   statusDeadline,
					kind: deadlineStatus,
					err:  errWorkerStatusTimeout,
				})

			case workerRetaining:
				consider(deadlineCandidate{
					at:   retentionDeadline,
					kind: deadlineRetention,
					err:  errResultRetentionExpired,
				})
			}
		}

		if !have || at.Before(selected.at) {
			return nil
		}

		// 只有恢复期限最终胜出时才修改 resumeState。
		if selected.kind == deadlineResume {
			if !s.resume.expire(at) {
				return nil
			}
		}

		return selected.err
	}

	// 所有会推进协调状态的事件在提交状态变化前都经过这里。
	// retaining 阶段忽略为释放已完成 RPC 而产生的内部取消。
	stopCause := func() error {
		at := now()

		if cause := context.Cause(ctx); cause != nil {
			return cause
		}

		if worker != nil &&
			worker.phase == workerRunning {
			if cause := context.Cause(worker.config.rpcCtx); cause != nil {
				return cause
			}
		}

		return checkExpired(at)
	}

	defer func() {
		stopWakeTimer()
		s.resume.close()

		// 最终退出只关闭当前通知，不再创建下一代 channel。
		if worker != nil && worker.delivery.changed != nil {
			close(worker.delivery.changed)
		}

		close(s.controlDone)
	}()

	if s.resume.phase == resumeDetached {
		armWakeTimer()
	}

	uploadPhase := uploadIdle

	var pending workerUploadCommand

	// Recv EOF 和 CloseSend 成功可能以任意顺序被协调者观察。
	var recvEOF bool

	// enterRetaining 在正常接收 EOF 和成功半关闭均成立后调用。
	// 固定一次结果保留期限，释放原 RPC；协调者继续处理连接恢复。
	enterRetaining := func() {
		if worker.phase == workerRetaining {
			return
		}

		worker.phase = workerRetaining

		tailDeadline = time.Time{}
		statusDeadline = time.Time{}

		// 从 Worker 双向正常完成时建立固定期限，detach/resume 不续期。
		retentionDeadline = now().Add(
			worker.config.resultRetentionTimeout,
		)

		recvDone = nil
		recvEvents = nil

		// 释放已正常完成的 RPC；retaining 不再把这次取消判为业务失败。
		worker.config.cancelRPC(nil)

		// 即使没有新结果，写任务也必须知道 Worker 已经完成，
		// 从而进入后续终态投递流程。
		worker.notifyResultChange()

		armWakeTimer()
	}

	// detachGeneration 统一提交有效断开、重置投递与安排本代停止。
	// 重复/旧代通知不续期；stop 不执行阻塞连接清理。
	detachGeneration := func(
		generation uint64,
		cause error,
	) bool {
		detached := s.resume.detach(
			generation,
			now(),
		)

		if detached && worker != nil {
			worker.resetResultDelivery()
			worker.notifyResultChange()
		}

		if connections != nil &&
			connections.current != nil &&
			connections.current.generation == generation {

			// 有效 detach 或已经处于同代 detached 时都可以安全重复 stop。
			// stop 不执行阻塞清理。
			if detached || s.resume.phase == resumeDetached {
				connections.current.stop(cause)
			}
		}

		if detached {
			// resume.detach 已经建立绝对恢复期限。
			armWakeTimer()
		}

		return detached
	}

	// handleConnectionEvent 在本协调循环中裁决固定代次的退出事实。
	// 已报告的不可恢复错误在同代 detached 后仍有效，旧代事件忽略。
	handleConnectionEvent := func(
		event connectionEvent,
	) error {
		if connections == nil ||
			connections.current == nil {
			return nil
		}

		current := connections.current

		// 退休连接或旧代事件不能修改后来安装的连接状态。
		if event.generation != current.generation ||
			event.generation != s.resume.generation {
			return nil
		}

		handleTransportFailure := func(err error) error {
			if err == nil {
				return errInvalidConnectionEvent
			}

			if !recoverableConnectionFailure(err) {
				return err
			}

			// 如果兄弟任务已经先完成 detach，本次调用返回 false，
			// 因而不会重新建立恢复期限。
			detachGeneration(
				event.generation,
				err,
			)

			return nil
		}

		taskStopExpected := func() bool {
			if context.Cause(current.ctx) != nil {
				return true
			}

			return s.resume.phase != resumeAttached
		}

		switch event.task {
		case connectionReaderTask:
			switch event.reader.kind {
			case readerReadFailed:
				return handleTransportFailure(
					event.reader.err,
				)

			case readerProtocolFailed,
				readerCommandFailed:
				if event.reader.err == nil {
					return errInvalidConnectionEvent
				}

				// 即使本代已经因为兄弟任务先进入 detached，
				// 已经上报的协议/命令错误仍然必须终止整场会话。
				return event.reader.err

			case readerStopped:
				if taskStopExpected() {
					return nil
				}

				return errors.Join(
					errUnexpectedConnectionStop,
					event.reader.err,
				)

			default:
				return errInvalidConnectionEvent
			}

		case connectionWriterTask:
			switch event.writer.kind {
			case writerResultsComplete:
				connections.outputComplete = true
				connections.lastSeq = event.writer.lastSeq
				return nil

			case writerWriteFailed:
				return handleTransportFailure(
					event.writer.err,
				)

			case writerControlFailed:
				if event.writer.err == nil {
					return errInvalidConnectionEvent
				}

				return event.writer.err

			case writerStopped:
				if taskStopExpected() {
					return nil
				}

				return errors.Join(
					errUnexpectedConnectionStop,
					event.writer.err,
				)

			default:
				return errInvalidConnectionEvent
			}

		default:
			return errInvalidConnectionEvent
		}
	}

	// drainCompletedConnection 仅在 current.done 已关闭后调用。
	// 两个事件生产者均已退出；先排空残留错误，再检查关闭结果并解除 current。
	drainCompletedConnection := func(
		current *connectionAttachment,
	) error {
		for {
			select {
			case event := <-current.events:
				if err := handleConnectionEvent(event); err != nil {
					return err
				}

			default:
				goto drained
			}
		}

	drained:
		// done 的关闭保证 CloseNow 已经返回，因此此时读取 closeErr 安全。
		if current.closeErr != nil {
			return current.closeErr
		}

		if connections.current == current {
			connections.current = nil
			connections.outputComplete = false
			connections.lastSeq = 0
		}

		return nil
	}

	// reapCurrentConnection 非阻塞检查当前连接是否已经退休完毕。
	// retiring=true 表示仍在关闭/等待，协调者绝不能阻塞等待。
	reapCurrentConnection := func() (
		retiring bool,
		err error,
	) {
		if connections == nil ||
			connections.current == nil {
			return false, nil
		}

		current := connections.current

		select {
		case <-current.done:
			if err := drainCompletedConnection(current); err != nil {
				return false, err
			}

			return false, nil

		default:
			return true, nil
		}
	}

	for {
		if err := stopCause(); err != nil {
			return err
		}

		// 一个时刻最多准备一个上传操作。
		if worker != nil &&
			worker.phase == workerRunning &&
			uploadPhase == uploadIdle {

			if chunk, ok := worker.input.take(); ok {
				pending = workerUploadCommand{
					kind:  workerUploadAudio,
					chunk: chunk,
				}

				uploadPhase = uploadPending

			} else if worker.input.inputDrained() {
				// 所有已接纳音频都已经成功 Send 后，
				// dispatchedBytes 应与最终接纳位置完全一致。
				if worker.dispatchedBytes != worker.input.input.nextOffset {
					panic("gateway: drained input has undispatched audio")
				}

				pending = workerUploadCommand{
					kind: workerUploadCloseSend,
				}

				uploadPhase = uploadPending
			}
		}

		var jobs chan<- workerUploadCommand
		var uploadResults <-chan workerUploadResult
		var rpcDone <-chan struct{}

		var connectionEvents <-chan connectionEvent
		var connectionDone <-chan struct{}

		if connections != nil &&
			connections.current != nil {
			connectionEvents = connections.current.events
			connectionDone = connections.current.done
		}

		if worker != nil {
			if worker.phase == workerRunning {
				rpcDone = worker.config.rpcCtx.Done()
			}

			switch uploadPhase {
			case uploadPending:
				jobs = worker.uploader.jobs

			case uploadWaiting:
				uploadResults = worker.uploader.results
			}
		}

		select {
		case <-ctx.Done():
			if err := stopCause(); err != nil {
				return err
			}

			panic("gateway: session context done without cause")

		case <-rpcDone:
			if err := stopCause(); err != nil {
				return err
			}

			panic("gateway: worker RPC done without cause")

		case <-wakeC:
			stopWakeTimer()

			if err := stopCause(); err != nil {
				return err
			}

			armWakeTimer()

		case jobs <- pending:
			// 从 send case 被选中的这一刻开始，任务已经真实交给
			// uploader。之后即使立即观察到取消，也不能撤销这个事实。
			switch pending.kind {
			case workerUploadAudio:
				if pending.chunk.offset != worker.dispatchedBytes {
					return errWorkerUploadResultMismatch
				}

				end := pending.chunk.offset +
					uint64(len(pending.chunk.data))

				if end < pending.chunk.offset {
					panic("gateway: dispatched audio offset overflow")
				}

				// Worker 可能在 Send 返回之前就报告这段音频的进度。
				worker.dispatchedBytes = end

			case workerUploadCloseSend:

			default:
				return errWorkerUploadResultMismatch
			}

			uploadPhase = uploadWaiting

			// uploader 已取得命令中的 slice。
			// audioInputBuffer 仍持有队首，直到 Send 成功 complete。
			pending.chunk.data = nil

			if err := stopCause(); err != nil {
				return err
			}

		case result := <-uploadResults:
			if err := stopCause(); err != nil {
				return err
			}

			if result.kind != pending.kind ||
				result.offset != pending.chunk.offset {
				return errWorkerUploadResultMismatch
			}

			if result.err != nil {
				if !errors.Is(result.err, io.EOF) {
					return result.err
				}

				// grpc Send 返回 EOF 不能证明当前任务成功。
				// 当前任务不 complete，也不继续上传，等待 Recv
				// 给出真正的 Worker RPC 终态。
				pending = workerUploadCommand{}
				uploadPhase = uploadAwaitingStatus

				// 如果 Recv EOF 已经更早被观察到，则真实接收终态
				// 已经明确，不需要再等待 status deadline。
				if recvEOF {
					return errWorkerEndedEarly
				}

				if statusDeadline.IsZero() {
					statusDeadline = now().Add(
						worker.config.statusTimeout,
					)
				}

				armWakeTimer()
				continue
			}

			switch result.kind {
			case workerUploadAudio:
				// 音频只有在 Send 明确成功以后才能释放 buffer。
				if err := worker.input.complete(result.offset); err != nil {
					return err
				}

				pending = workerUploadCommand{}
				uploadPhase = uploadIdle

			case workerUploadCloseSend:
				pending = workerUploadCommand{}
				uploadPhase = uploadHalfClosed

				// Recv EOF 可能比 CloseSend 成功更早被观察。
				if recvEOF {
					enterRetaining()
				}

			default:
				return errWorkerUploadResultMismatch
			}

		case event := <-recvEvents:
			if err := stopCause(); err != nil {
				return err
			}

			switch event.kind {
			case workerReceiveResult:
				// 只有保存成功才消耗结果序号。
				if _, err := worker.results.append(
					event.segmentID,
					event.text,
					event.isFinal,
				); err != nil {
					return err
				}

				// 正在等待“下一条结果”的写任务需要重新查询。
				worker.notifyResultChange()

			case workerReceiveProgress:
				if err := worker.acknowledgeProgress(
					event.processedBytes,
				); err != nil {
					return err
				}

			default:
				return errInvalidWorkerResponse
			}

		case <-recvDone:
			// 正常 EOF 不能越过已经发生的逻辑取消、
			// RPC 取消或更早的绝对期限。
			if err := stopCause(); err != nil {
				return err
			}

			// done 是关闭型 channel，只能消费一次。
			recvDone = nil
			recvEvents = nil

			// receiver 保证先写 err，再关闭 done。
			if worker.receiver.err != nil {
				return worker.receiver.err
			}

			recvEOF = true

			switch uploadPhase {
			case uploadHalfClosed:
				enterRetaining()

			case uploadWaiting:
				// 只有已经真实交付出去的 CloseSend 可以允许
				// Recv EOF 先于它的成功结果被协调者观察。
				if pending.kind != workerUploadCloseSend {
					return errWorkerEndedEarly
				}

			case uploadAwaitingStatus:
				// 上传方向已经以 EOF 停止，没有取得正常任务结果。
				return errWorkerEndedEarly

			case uploadIdle, uploadPending:
				// CloseSend 尚未成功完成。
				return errWorkerEndedEarly

			default:
				return errWorkerEndedEarly
			}

		case cmd := <-s.commands:
			// 命令已经被协调者取得，因此无论随后发现什么停止原因，
			// 都必须先给提交方唯一回复。
			if err := stopCause(); err != nil {
				cmd.reply <- sessionControlResult{
					err: errResumeClosed,
				}
				return err
			}

			if err := cmd.ctx.Err(); err != nil {
				cmd.reply <- sessionControlResult{
					err: err,
				}
				continue
			}

			switch cmd.kind {
			case controlResume:
				if connections != nil {
					cmd.reply <- sessionControlResult{
						err: errConnectionCandidateRequired,
					}
					continue
				}

				// 纯控制模式没有任何结果状态。
				if worker == nil && cmd.resultSeq != 0 {
					cmd.reply <- sessionControlResult{
						err: errSessionWorkerUnavailable,
					}
					continue
				}

				// 只有 detached 状态才有资格真正恢复。
				// 在切换 generation 之前先验证客户端声明的位置，
				// 这样错误声明不会夺得新的连接代次。
				if worker != nil &&
					s.resume.phase == resumeDetached {

					if cmd.resultSeq < worker.results.ackedSeq {
						cmd.reply <- sessionControlResult{
							err: errResultReplayGap,
						}
						continue
					}

					if cmd.resultSeq > worker.delivery.offeredSeq {
						cmd.reply <- sessionControlResult{
							err: errResultAckAhead,
						}
						continue
					}
				}

				generation, err := s.resume.resume(now())
				if err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}

					// resumeState 可能因为绝对恢复期限到达而自行关闭。
					if s.resume.phase == resumeClosed {
						return err
					}

					continue
				}

				if worker != nil {
					// 前面的范围检查已经保证：
					//
					// ackedSeq <= appliedSeq <= offeredSeq <= lastSeq
					//
					// 因而这里不应再出现正常协议错误。
					if _, err := worker.acknowledgeResult(
						cmd.resultSeq,
					); err != nil {
						cmd.reply <- sessionControlResult{
							err: err,
						}
						return err
					}

					// 新代从客户端已经确认的位置重新开始。
					worker.resetResultDelivery()

					// ACK 接纳、代次切换和游标重置作为一个协调事务
					// 完成以后再统一通知。
					worker.notifyResultChange()
				}

				armWakeTimer()

				cmd.reply <- sessionControlResult{
					generation: generation,
				}

			case controlResumeConnection:
				if connections == nil {
					cmd.reply <- sessionControlResult{
						err: errConnectionManagementUnavailable,
					}
					continue
				}

				if worker == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionWorkerUnavailable,
					}
					continue
				}

				if err := validateConnectionCandidate(
					cmd.candidate,
				); err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}
					continue
				}

				// 只有 detached 才允许恢复。
				// 其他状态仍利用 resumeState 自己的规则产生规范错误。
				if s.resume.phase != resumeDetached {
					probe := *s.resume

					_, err := probe.resume(now())
					if err == nil {
						panic("gateway: non-detached resume unexpectedly succeeded")
					}

					cmd.reply <- sessionControlResult{
						err: err,
					}

					if probe.phase == resumeClosed {
						return err
					}

					continue
				}

				// 旧连接必须已经真正完成 CloseNow + reader/writer 退出，
				// 并且所有退出事件已经被处理以后，才允许接管新候选。
				if connections.current != nil {
					retiring, err := reapCurrentConnection()
					if err != nil {
						cmd.reply <- sessionControlResult{
							err: err,
						}
						return err
					}

					if retiring {
						cmd.reply <- sessionControlResult{
							err: errConnectionRetiring,
						}
						continue
					}
				}

				// 恢复位置必须在正式代次切换之前验证。
				if cmd.resultSeq < worker.results.ackedSeq {
					cmd.reply <- sessionControlResult{
						err: errResultReplayGap,
					}
					continue
				}

				if cmd.resultSeq > worker.delivery.offeredSeq {
					cmd.reply <- sessionControlResult{
						err: errResultAckAhead,
					}
					continue
				}

				// 先在副本上完成期限、状态和 generation 溢出检查。
				// 此时正式 resumeState 完全没有变化。
				probe := *s.resume

				generation, err := probe.resume(now())
				if err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}

					if probe.phase == resumeClosed {
						// 到期/终止条件已经确定，正式协调循环也结束。
						return err
					}

					continue
				}

				attachment, err := newConnectionAttachment(
					s,
					ctx,
					generation,
					cmd.candidate,
				)
				if err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}
					continue
				}

				// attachment 还没安装，因此这里只能取消它创建的子 context。
				// socket 的关闭责任仍属于候选提交者。
				discardAttachment := func(cause error) {
					attachment.stop(cause)
				}

				// 构造期间可能已经发生逻辑取消或绝对期限到达。
				if err := stopCause(); err != nil {
					discardAttachment(err)

					cmd.reply <- sessionControlResult{
						err: errResumeClosed,
					}

					return err
				}

				// 命令在交付 coordinator 后仍可能在预构造期间被调用方取消。
				// 此时还没有接管 candidate。
				if err := cmd.ctx.Err(); err != nil {
					discardAttachment(err)

					cmd.reply <- sessionControlResult{
						err: err,
					}
					continue
				}

				// 范围已经在同一个协调段内验证过，因此这里不应产生普通协议错误。
				// 一旦失败说明内部不变量破坏；正式 generation 尚未提交，
				// candidate 仍由调用者拥有。
				if _, err := worker.acknowledgeResult(
					cmd.resultSeq,
				); err != nil {
					discardAttachment(err)

					cmd.reply <- sessionControlResult{
						err: err,
					}

					return err
				}

				// 从这里开始提交恢复事务。
				//
				// 不再次调用 resume(now)，而是直接提交刚才验证成功的副本；
				// 避免一次恢复在两个 now() 之间产生不同结果。
				*s.resume = probe

				worker.resetResultDelivery()
				worker.notifyResultChange()

				connections.current = attachment
				connections.outputComplete = false
				connections.lastSeq = 0

				// 安装 current 是候选清理责任的转移点。
				// 之后即使 cmd.ctx 立即取消，调用者也不能再关闭 candidate。
				go attachment.run()

				armWakeTimer()

				cmd.reply <- sessionControlResult{
					generation: generation,
				}

			case controlDetach:
				detached := detachGeneration(
					cmd.generation,
					context.Canceled,
				)

				cmd.reply <- sessionControlResult{
					detached: detached,
				}

			case controlClose:
				s.resume.close()

				cmd.reply <- sessionControlResult{}

				return nil

			case controlAudio:
				if worker == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionWorkerUnavailable,
					}
					continue
				}

				// 历史重发也必须先通过当前连接资格和代次检查。
				if s.resume.phase != resumeAttached {
					cmd.reply <- sessionControlResult{
						err: errSessionNotAttached,
					}
					continue
				}

				if cmd.generation != s.resume.generation {
					cmd.reply <- sessionControlResult{
						err: errSessionGenerationMismatch,
					}
					continue
				}

				if uploadPhase == uploadAwaitingStatus {
					// 等待 Worker 最终状态期间，只接受完整历史重发。
					// 其他范围错误或新音频统一拒绝，不能覆盖真实 RPC 终态。
					kind, err := worker.input.input.classifyAudio(
						cmd.offset,
						uint64(len(cmd.payload)),
					)

					if err == nil && kind == audioChunkDuplicate {
						cmd.reply <- sessionControlResult{
							accepted:   false,
							nextOffset: worker.input.input.nextOffset,
						}
						continue
					}

					cmd.reply <- sessionControlResult{
						err: errWorkerInputStopped,
					}
					continue
				}

				inputStopped :=
					worker.phase == workerRetaining ||
						uploadPhase == uploadHalfClosed

				var (
					accepted bool
					err      error
				)

				if inputStopped {
					accepted, err = worker.offerHistoricalAudio(
						cmd.offset,
						cmd.payload,
					)
				} else {
					accepted, err = worker.offerAudio(
						cmd.offset,
						cmd.payload,
					)
				}

				if err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}

					// 输入已经停止时的新音频只拒绝本次请求。
					if errors.Is(err, errWorkerInputStopped) {
						continue
					}

					// 正常运行路径中的范围、容量和 backlog 错误
					// 属于当前逻辑会话的不可恢复输入错误。
					return err
				}

				cmd.reply <- sessionControlResult{
					accepted:   accepted,
					nextOffset: worker.input.input.nextOffset,
				}

			case controlEnd:
				if worker == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionWorkerUnavailable,
					}
					continue
				}

				if s.resume.phase != resumeAttached {
					cmd.reply <- sessionControlResult{
						err: errSessionNotAttached,
					}
					continue
				}

				if cmd.generation != s.resume.generation {
					cmd.reply <- sessionControlResult{
						err: errSessionGenerationMismatch,
					}
					continue
				}

				if uploadPhase == uploadAwaitingStatus {
					// 如果原 end 已经存在，只允许完全相同的重复 end。
					// acceptEnd 的错误路径不修改 inputState；不匹配的
					// end 在这里转换成 InputStopped，不结束协调循环。
					if worker.input.input.ended {
						accepted, err := worker.input.acceptEnd(
							cmd.offset,
						)

						if err == nil && !accepted {
							cmd.reply <- sessionControlResult{
								accepted:   false,
								nextOffset: worker.input.input.nextOffset,
							}
							continue
						}

						if err == nil && accepted {
							panic("gateway: accepted end while awaiting worker status")
						}
					}

					cmd.reply <- sessionControlResult{
						err: errWorkerInputStopped,
					}
					continue
				}

				inputStopped :=
					worker.phase == workerRetaining ||
						uploadPhase == uploadHalfClosed

				if inputStopped {
					// Worker 输入已经停止后不能首次建立 end。
					if !worker.input.input.ended {
						cmd.reply <- sessionControlResult{
							err: errWorkerInputStopped,
						}
						continue
					}

					// 已经接纳过 end 时仍沿用 inputState 的幂等规则。
					accepted, err := worker.input.acceptEnd(
						cmd.offset,
					)
					if err != nil {
						cmd.reply <- sessionControlResult{
							err: err,
						}
						return err
					}

					if accepted {
						panic("gateway: accepted end after worker input stopped")
					}

					cmd.reply <- sessionControlResult{
						accepted:   false,
						nextOffset: worker.input.input.nextOffset,
					}
					continue
				}

				accepted, err := worker.input.acceptEnd(
					cmd.offset,
				)
				if err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}
					return err
				}

				// tail deadline 只由首次合法 end 建立一次。
				if accepted && tailDeadline.IsZero() {
					tailDeadline = now().Add(
						worker.config.tailTimeout,
					)
					armWakeTimer()
				}

				cmd.reply <- sessionControlResult{
					accepted:   accepted,
					nextOffset: worker.input.input.nextOffset,
				}

			case controlTakeResult:
				if worker == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionWorkerUnavailable,
					}
					continue
				}

				if s.resume.phase != resumeAttached {
					cmd.reply <- sessionControlResult{
						err: errSessionNotAttached,
					}
					continue
				}

				if cmd.generation != s.resume.generation {
					cmd.reply <- sessionControlResult{
						err: errSessionGenerationMismatch,
					}
					continue
				}

				offer, err := worker.offerResult()
				if err != nil {
					// in-flight 冲突和读取位置错误只拒绝本次操作，
					// 不改变会话生命周期。
					cmd.reply <- sessionControlResult{
						err: err,
					}
					continue
				}

				cmd.reply <- sessionControlResult{
					offer: offer,
				}

			case controlResultWritten:
				if worker == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionWorkerUnavailable,
					}
					continue
				}

				// 写成功回调属于某个具体连接代。
				// 旧代或者已经 detach 的迟到回调必须直接忽略，
				// 不能碰当前代的 inFlightSeq。
				if s.resume.phase != resumeAttached ||
					cmd.generation != s.resume.generation {
					cmd.reply <- sessionControlResult{
						handled: false,
					}
					continue
				}

				if err := worker.completeResultWrite(
					cmd.resultSeq,
				); err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}
					continue
				}

				// inFlight 已经释放，下一条结果现在可能可以授权。
				worker.notifyResultChange()

				cmd.reply <- sessionControlResult{
					handled: true,
				}

			case controlResultAck:
				if worker == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionWorkerUnavailable,
					}
					continue
				}

				if s.resume.phase != resumeAttached {
					cmd.reply <- sessionControlResult{
						err: errSessionNotAttached,
					}
					continue
				}

				if cmd.generation != s.resume.generation {
					cmd.reply <- sessionControlResult{
						err: errSessionGenerationMismatch,
					}
					continue
				}

				advanced, err := worker.acknowledgeResult(
					cmd.resultSeq,
				)
				if err != nil {
					// ACK 超过 offeredSeq 只拒绝本次声明，
					// 不能因此结束整个 Worker 会话。
					cmd.reply <- sessionControlResult{
						err: err,
					}
					continue
				}

				if advanced {
					// ACK 可能推进 cursor，也会释放 resultBuffer 预算。
					worker.notifyResultChange()
				}

				cmd.reply <- sessionControlResult{
					advanced: advanced,
				}

			default:
				cmd.reply <- sessionControlResult{
					err: errInvalidSessionControlCommand,
				}
			}

		case event := <-connectionEvents:
			if err := stopCause(); err != nil {
				return err
			}

			if err := handleConnectionEvent(event); err != nil {
				return err
			}

		case <-connectionDone:
			if err := stopCause(); err != nil {
				return err
			}

			current := connections.current
			if current == nil {
				continue
			}

			// channel 之所以进入此 case，就是因为当前 attachment.done 已关闭。
			// 先消费所有剩余任务事件，再判断 closeErr，最后才允许清掉 current。
			if err := drainCompletedConnection(current); err != nil {
				return err
			}
		}
	}
}

// submitCommand 为一次不可复用命令安装 ctx 和独立容量 1 的 reply。
// 交付前可取消；交付后只等待唯一回复，不能因 ctx 取消提前归还 payload 所有权。
// cmd 的 ctx/reply 由该函数设置，调用者仅提供操作及业务字段。
// 支持多个调用方并发提交；循环已结束时返回 errResumeClosed；不得持有注册表锁调用。
func (s *resumableSession) submitCommand(ctx context.Context, cmd sessionControlCommand) sessionControlResult {
	if ctx == nil {
		panic("gateway: nil session control request context")
	}

	if err := ctx.Err(); err != nil {
		return sessionControlResult{
			err: err,
		}
	}

	reply := make(chan sessionControlResult, 1)

	cmd.ctx = ctx
	cmd.reply = reply

	select {
	case s.commands <- cmd:
		// 提交点。
		//
		// 从这里开始不能再因 ctx.Done() 或 controlDone 提前返回。
		// coordinator 已经拥有请求；audio 调用方也必须继续保持
		// payload 只读，直到取得唯一回复。
		return <-reply

	case <-ctx.Done():
		return sessionControlResult{
			err: ctx.Err(),
		}

	case <-s.controlDone:
		return sessionControlResult{
			err: errResumeClosed,
		}
	}
}

// submitControl 将既有控制操作包装为完整命令，复用唯一提交/回复契约。
// generation 仅用于 detach；音频和 end 由各自请求入口提供业务字段。
func (s *resumableSession) submitControl(
	ctx context.Context,
	kind sessionControlKind,
	generation uint64,
) sessionControlResult {
	return s.submitCommand(ctx, sessionControlCommand{
		kind:       kind,
		generation: generation,
	})
}

// reportDetach 报告指定代次断开，false,nil 表示旧代次或重复通知被忽略。
// 使用独立的控制请求 ctx，不能直接使用已因断连取消的连接 ctx。
// 交付失败时调用方仍负责重试或终止，不能静默丢失断开事件。
func (s *resumableSession) reportDetach(ctx context.Context, generation uint64) (bool, error) {
	result := s.submitControl(ctx, controlDetach, generation)
	if result.err != nil {
		return false, result.err
	}
	return result.detached, nil
}

// requestClose 请求结束恢复资格并等待控制循环退出。
// controlDone 已关闭时立即返回 nil，可重复调用。
// 尚未交付时允许 ctx 取消；成功交付后按确定结果收尾。
// 返回 nil 表示控制循环已退出，不表示 Worker 或网络资源已清理。
func (s *resumableSession) requestClose(ctx context.Context) error {
	if ctx == nil {
		panic("gateway: nil session close context")
	}

	// 重复关闭
	select {
	case <-s.controlDone:
		return nil
	default:
	}

	result := s.submitControl(ctx, controlClose, 0)

	if result.err != nil && !errors.Is(result.err, errResumeClosed) {
		return result.err
	}

	// close 命令已经成功处理，或者循环已经自行结束。
	//
	// 这里不再监听 ctx，因为成功提交后的关闭也必须观察到
	// controlDone，才能满足 requestClose 的返回语义。
	<-s.controlDone

	return nil
}

// requestAudio 提交当前连接代次的一块音频。
// payload 在函数返回前必须保持只读；成功后缓冲拥有独立副本。
// 返回 accepted、nextOffset、error；任何错误时前两项均为零值。
func (s *resumableSession) requestAudio(
	ctx context.Context,
	generation uint64,
	offset uint64,
	payload []byte,
) (bool, uint64, error) {
	result := s.submitCommand(ctx, sessionControlCommand{
		kind:       controlAudio,
		generation: generation,
		offset:     offset,
		payload:    payload,
	})

	if result.err != nil {
		return false, 0, result.err
	}

	return result.accepted, result.nextOffset, nil
}

// requestEnd 提交当前连接代次的输入终点。
// 成功只表示接纳 end；重复 end 返回 false、当前接纳位置、nil。
// 不代表 CloseSend 已执行，更不代表识别完成。
func (s *resumableSession) requestEnd(
	ctx context.Context,
	generation uint64,
	finalOffset uint64,
) (bool, uint64, error) {
	result := s.submitCommand(ctx, sessionControlCommand{
		kind:       controlEnd,
		generation: generation,
		offset:     finalOffset,
	})

	if result.err != nil {
		return false, 0, result.err
	}

	return result.accepted, result.nextOffset, nil
}
