package gateway

import (
	"context"
	"errors"
	"time"
)

var errInvalidSessionControlCommand = errors.New("invalid session control command")

// sessionControlKind 表示控制循环支持的内部操作。
type sessionControlKind uint8

const (
	controlResume sessionControlKind = iota // 尝试恢复连接，成功返回新代次。
	controlDetach                           // 报告某代次连接断开。
	controlClose                            // 结束恢复资格并退出控制循环。
	controlAudio                            // 接纳当前代次的一块音频。
	controlEnd                              // 接纳当前代次的输入终点。
)

// sessionControlCommand 表示一次不可复用的内部控制请求。
type sessionControlCommand struct {
	kind       sessionControlKind          // 本次操作。
	ctx        context.Context             // 请求取消信号，仅在处理前检查。
	generation uint64                      // detach/audio/end 的连接代次。
	reply      chan<- sessionControlResult // 独立、容量为 1，循环只发送一次。
	offset     uint64                      // audio 的起点，end 的最终接纳位置；控制命令忽略。
	payload    []byte                      // 仅 audio 使用；从请求开始到回复前只读，调用方不得修改。
}

// sessionControlResult 是某次控制操作的确定结果。
// 零代次用于失败或非恢复操作；detached=false,nil 表示无需改变状态。
type sessionControlResult struct {
	generation uint64 // resume 成功时的新连接代次。
	detached   bool   // detach 是否实际将当前连接改为断开保留。
	err        error  // 状态错误、请求取消或控制循环终止原因。
	accepted   bool   // 本次是否新接纳；完整重发或重复 end 为 false。
	nextOffset uint64 // 回复时 Gateway 已连续接纳的位置，不代表 Worker 处理位置。
}

// runControl 由生命周期拥有者恰好启动一次，串行处理控制命令。
// ctx 属于逻辑会话生命周期，不能绑定任意一条客户端连接。
// now 获取当前时间，生产传 time.Now；须与 Timer 时钟一致推进且不得阻塞。
// ctx 和 now 必须非 nil。循环退出前关闭恢复资格，再关闭 controlDone。
// 本步不操作网络、注册表或准入。
// detached 状态下由控制循环独占恢复期限 Timer；
// Timer 仅负责唤醒，实际过期判断仍由 resumeState 完成。
func (s *resumableSession) runControl(ctx context.Context, now func() time.Time) {
	_ = s.runCoordinator(ctx, now, nil)
}

// runCoordinator 是唯一状态循环，串行处理控制命令、输入与上传结果。
// upload=nil 时保留已有纯控制行为，并拒绝音频/end 操作。
// 退出时停止 Timer、关闭恢复资格并关闭 controlDone；不在循环内等待 I/O 退出。
func (s *resumableSession) runCoordinator(
	ctx context.Context,
	now func() time.Time,
	upload *sessionUpload,
) error {
	if ctx == nil {
		panic("gateway: nil session control context")
	}
	if now == nil {
		panic("gateway: nil session control clock")
	}

	var expiryTimer *time.Timer
	var expiryC <-chan time.Time

	// stopExpiryTimer 停止并丢弃当前恢复期限 Timer。
	// 允许重复调用；不关闭 Timer.C，也不阻塞读取旧 channel。
	stopExpiryTimer := func() {
		if expiryTimer != nil {
			expiryTimer.Stop()
		}

		expiryTimer = nil
		expiryC = nil
	}

	// armExpiryTimer 根据 detached 状态已有的绝对截止时间安排一次唤醒。
	// 不修改 expiresAt，也不延长恢复窗口。
	armExpiryTimer := func() {
		stopExpiryTimer()

		if s.resume.phase != resumeDetached {
			return
		}

		remaining := max(s.resume.expiresAt.Sub(now()), 0)

		expiryTimer = time.NewTimer(remaining)
		expiryC = expiryTimer.C
	}

	defer func() {
		stopExpiryTimer()
		s.resume.close()
		close(s.controlDone)
	}()

	if s.resume.phase == resumeDetached {
		armExpiryTimer()
	}

	phase := uploadIdle
	var pending workerUploadCommand

	for {
		// 已知逻辑会话或原 Worker RPC 已终止时，
		// 不再从缓冲准备新的上传任务。
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}

		if upload != nil {
			if cause := context.Cause(upload.config.rpcCtx); cause != nil {
				return cause
			}
		}

		// 只有没有未完成上传操作时才能取下一块。
		// take 仅借出队首，不释放槽位或字节预算。
		if upload != nil && phase == uploadIdle {
			if chunk, ok := upload.input.take(); ok {
				pending = workerUploadCommand{
					kind:  workerUploadAudio,
					chunk: chunk,
				}
				phase = uploadPending

			} else if upload.input.inputDrained() {
				// 所有已接纳音频均已 complete，而且已经接纳 end。
				// CloseSend 也通过 uploader 串行执行。
				pending = workerUploadCommand{
					kind: workerUploadCloseSend,
				}
				phase = uploadPending
			}
		}

		// nil channel 的 select case 永远不会就绪。
		// 因而仅在正确 phase 启用任务交付或结果处理。
		var jobs chan<- workerUploadCommand
		var results <-chan workerUploadResult
		var rpcDone <-chan struct{}

		if upload != nil {
			rpcDone = upload.config.rpcCtx.Done()

			switch phase {
			case uploadPending:
				jobs = upload.uploader.jobs

			case uploadWaiting:
				results = upload.uploader.results
			}
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)

		case <-rpcDone:
			return context.Cause(upload.config.rpcCtx)

		case <-expiryC:
			// Timer 只负责唤醒；期限仍以 resumeState 的绝对时间为准。
			stopExpiryTimer()

			if s.resume.expire(now()) {
				return errResumeExpired
			}

			if s.resume.phase == resumeDetached {
				armExpiryTimer()
			}

		case jobs <- pending:
			// 到这里 uploader 已真正接收任务。
			// 同一个 pending 不得再次交付。
			phase = uploadWaiting

			// coordinator 不再额外持有 payload 引用；
			// kind/offset 仍留下，用于匹配唯一结果。
			pending.chunk.data = nil

		case result := <-results:
			// 当前只有一个已交付但未处理的任务，
			// 返回结果必须严格匹配该任务。
			if result.kind != pending.kind ||
				result.offset != pending.chunk.offset {
				return errWorkerUploadResultMismatch
			}

			// 上传失败不 complete、不重试，也不继续安排 CloseSend。
			if result.err != nil {
				return result.err
			}

			switch result.kind {
			case workerUploadAudio:
				// 只有匹配的 Send 成功结果才能真正释放 buffer 预算。
				if err := upload.input.complete(result.offset); err != nil {
					return err
				}

				pending = workerUploadCommand{}
				phase = uploadIdle

			case workerUploadCloseSend:
				// 半关闭成功只表示不会再发送 Worker 请求。
				// RPC 和协调者继续存在，响应方向仍可能产生尾部结果。
				pending = workerUploadCommand{}
				phase = uploadHalfClosed

			default:
				return errWorkerUploadResultMismatch
			}

		case cmd := <-s.commands:
			// 命令已经被接收；从这里开始必须恰好回复一次。

			// select 可能在取消和命令同时就绪时选择命令，
			// 因此接收后先重新检查逻辑会话生命周期。
			if cause := context.Cause(ctx); cause != nil {
				cmd.reply <- sessionControlResult{
					err: errResumeClosed,
				}
				return cause
			}

			// 带上传资源时，同样重新检查原 Worker RPC。
			if upload != nil {
				if cause := context.Cause(upload.config.rpcCtx); cause != nil {
					cmd.reply <- sessionControlResult{
						err: errResumeClosed,
					}
					return cause
				}
			}

			// 请求自己的取消只取消本次请求，不终止其他有效会话操作。
			if err := cmd.ctx.Err(); err != nil {
				cmd.reply <- sessionControlResult{
					err: err,
				}
				continue
			}

			switch cmd.kind {
			case controlResume:
				generation, err := s.resume.resume(now())

				if err == nil {
					// 成功恢复后已经不再 detached。
					stopExpiryTimer()
				}

				cmd.reply <- sessionControlResult{
					generation: generation,
					err:        err,
				}

				// resume 自己检查绝对恢复期限。
				// 若发现已过期，会推进到 closed。
				if s.resume.phase == resumeClosed {
					if err != nil {
						return err
					}
					return errResumeExpired
				}

			case controlDetach:
				detached := s.resume.detach(
					cmd.generation,
					now(),
				)

				// 只有真正 attached -> detached 才建立新 Timer。
				// 旧代次或重复 detach 不得重置恢复期限。
				if detached {
					armExpiryTimer()
				}

				cmd.reply <- sessionControlResult{
					detached: detached,
				}

			case controlClose:
				s.resume.close()

				cmd.reply <- sessionControlResult{}

				return nil

			case controlAudio:
				if upload == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionUploadUnavailable,
					}
					continue
				}

				// 必须先验证连接资格，再检查音频是否重复。
				// 历史重发不能绕过连接代次隔离。
				if s.resume.phase != resumeAttached {
					cmd.reply <- sessionControlResult{
						err: errSessionNotAttached,
					}
					continue
				}

				// 当前代次校验与音频接纳由同一协调者连续执行。
				if cmd.generation != s.resume.generation {
					cmd.reply <- sessionControlResult{
						err: errSessionGenerationMismatch,
					}
					continue
				}

				accepted, err := upload.input.offer(
					cmd.offset,
					cmd.payload,
				)
				if err != nil {
					// 当前合法代次的范围错误、输入错误或容量耗尽
					// 属于不可恢复输入错误：先回复，再结束协调路径。
					cmd.reply <- sessionControlResult{
						err: err,
					}
					return err
				}

				cmd.reply <- sessionControlResult{
					accepted:   accepted,
					nextOffset: upload.input.input.nextOffset,
				}

			case controlEnd:
				if upload == nil {
					cmd.reply <- sessionControlResult{
						err: errSessionUploadUnavailable,
					}
					continue
				}

				if s.resume.phase != resumeAttached {
					cmd.reply <- sessionControlResult{
						err: errSessionNotAttached,
					}
					continue
				}

				// end 同样必须来自当前有效连接代次。
				if cmd.generation != s.resume.generation {
					cmd.reply <- sessionControlResult{
						err: errSessionGenerationMismatch,
					}
					continue
				}

				accepted, err := upload.input.acceptEnd(cmd.offset)
				if err != nil {
					cmd.reply <- sessionControlResult{
						err: err,
					}
					return err
				}

				cmd.reply <- sessionControlResult{
					accepted:   accepted,
					nextOffset: upload.input.input.nextOffset,
				}

			default:
				cmd.reply <- sessionControlResult{
					err: errInvalidSessionControlCommand,
				}
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

// requestResume 请求恢复；成功返回新代次，失败返回零和原因。
// 成功后即使 ctx 已取消，调用方也必须负责该代次的连接交接或断开报告。
func (s *resumableSession) requestResume(ctx context.Context) (uint64, error) {
	result := s.submitControl(ctx, controlResume, 0)
	if result.err != nil {
		return 0, result.err
	}
	return result.generation, nil
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
