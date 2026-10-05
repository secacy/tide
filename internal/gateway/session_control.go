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
)

// sessionControlCommand 表示一次不可复用的内部控制请求。
type sessionControlCommand struct {
	kind       sessionControlKind          // 本次操作。
	ctx        context.Context             // 请求取消信号，仅在处理前检查。
	generation uint64                      // detach 的连接代次，其他命令忽略。
	reply      chan<- sessionControlResult // 独立、容量为 1，循环只发送一次。
}

// sessionControlResult 是某次控制操作的确定结果。
// 零代次用于失败或非恢复操作；detached=false,nil 表示无需改变状态。
type sessionControlResult struct {
	generation uint64 // resume 成功时的新连接代次。
	detached   bool   // detach 是否实际将当前连接改为断开保留。
	err        error  // 状态错误、请求取消或控制循环终止原因。
}

// runControl 由生命周期拥有者恰好启动一次，串行处理控制命令。
// ctx 属于逻辑会话生命周期，不能绑定任意一条客户端连接。
// now 获取当前时间，生产传 time.Now；须与 Timer 时钟一致推进且不得阻塞。
// ctx 和 now 必须非 nil。循环退出前关闭恢复资格，再关闭 controlDone。
// 本步不操作网络、注册表或准入。
// detached 状态下由控制循环独占恢复期限 Timer；
// Timer 仅负责唤醒，实际过期判断仍由 resumeState 完成。
func (s *resumableSession) runControl(ctx context.Context, now func() time.Time) {
	if ctx == nil {
		panic("gateway: nil session control context")
	}
	if now == nil {
		panic("gateway: nil session control clock")
	}

	var expiryTimer *time.Timer  // 当前恢复期限的 Timer，由控制循环独占。
	var expiryC <-chan time.Time // 当前监听来源；nil 表示禁用到期分支。

	// stopExpiryTimer 停止并丢弃当前恢复期限 Timer。
	// 允许重复调用；不关闭 Timer.C，也不阻塞读取旧 channel。
	stopExpiryTimer := func() {
		if expiryTimer != nil {
			expiryTimer.Stop()
		}

		expiryTimer = nil
		expiryC = nil
	}

	// armExpiryTimer 根据当前 detached 状态已有的绝对截止时间
	// 安排一次唤醒。它不会修改 expiresAt，也不会延长恢复窗口。
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

	// 支持控制循环启动前已经处于 detached 的情况。
	if s.resume.phase == resumeDetached {
		armExpiryTimer()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-expiryC:
			// Timer 只负责唤醒。收到通知后先丢弃当前 Timer，
			// 再根据当前状态和当前绝对截止时间重新判断。
			stopExpiryTimer()

			if s.resume.expire(now()) {
				return
			}

			// 理论上正常 Timer 不应早于期限触发，但仍保留重新安排
			// 逻辑，以当前 expiresAt 为唯一判断依据。
			if s.resume.phase == resumeDetached {
				armExpiryTimer()
			}

		case cmd := <-s.commands:
			// 命令已经被接收，因此从这里开始，无论发生什么，
			// 都必须给这条命令恰好一次回复。

			if ctx.Err() != nil {
				cmd.reply <- sessionControlResult{
					err: errResumeClosed,
				}
				return
			}

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
					// 成功恢复后已经不再 detached，
					// 当前恢复期限 Timer 必须停止。
					stopExpiryTimer()
				}

				cmd.reply <- sessionControlResult{
					generation: generation,
					err:        err,
				}

				// resume 自己检查期限。
				// 若发现已经过期，它会把状态推进到 closed。
				if s.resume.phase == resumeClosed {
					return
				}

			case controlDetach:
				detached := s.resume.detach(
					cmd.generation,
					now(),
				)

				// 只有真正从 attached -> detached 时才创建新 Timer。
				// 旧代次和重复 detach 不得重置恢复期限。
				if detached {
					armExpiryTimer()
				}

				cmd.reply <- sessionControlResult{
					detached: detached,
				}

			case controlClose:
				s.resume.close()

				cmd.reply <- sessionControlResult{}

				return

			default:
				cmd.reply <- sessionControlResult{
					err: errInvalidSessionControlCommand,
				}
			}
		}
	}
}

// submitControl 提交命令并等待唯一回复，支持多个调用方并发调用。
// ctx 必须非 nil。交付前可取消；交付后必须取得明确处理结果。
// 循环已结束时返回 errResumeClosed。不得持有注册表锁调用。
func (s *resumableSession) submitControl(ctx context.Context, kind sessionControlKind, generation uint64) sessionControlResult {
	if ctx == nil {
		panic("gateway: nil session control request context")
	}

	// 已经取消的请求不要尝试交付。
	if err := ctx.Err(); err != nil {
		return sessionControlResult{
			err: err,
		}
	}

	reply := make(chan sessionControlResult, 1)

	cmd := sessionControlCommand{
		kind:       kind,
		ctx:        ctx,
		generation: generation,
		reply:      reply,
	}

	select {
	case s.commands <- cmd:
		// 这是提交点。
		//
		// 从这里开始禁止再 select ctx.Done() 或 controlDone。
		// 协调者已经拥有请求，并保证给它一次确定回复。
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
