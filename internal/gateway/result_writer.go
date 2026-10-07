package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

var (
	errInvalidResultWriterConfig = errors.New("invalid result writer config") // 写任务缺少固定连接、会话、控制生命周期、有效代次或单次写期限。
	errResultWriterSuperseded    = errors.New("result writer superseded")     // 一次成功 Write 的迟到报告已不再属于当前附着代次。
)

// resultWriteConn 是写任务所需的最小 I/O 能力；*websocket.Conn 直接满足。
// Write 必须响应 ctx；连接关闭及读任务由连接拥有者负责。
type resultWriteConn interface {
	Write(ctx context.Context, typ websocket.MessageType, data []byte) error
}

// resultWriterConfig 在构造后保持不变。
type resultWriterConfig struct {
	session      *resumableSession // 已运行的逻辑会话，通过命令访问其状态。
	generation   uint64            // 本次连接的固定代次，必须非零。
	conn         resultWriteConn   // 本次连接的固定句柄。
	writeTimeout time.Duration     // 单次 Write 期限，必须为正。
	controlCtx   context.Context   // 逻辑会话生命周期，用于成功报告。
}

// resultWriter 只运行一次；自身不启动 goroutine、不拥有连接关闭权。
// 调用方保证同一连接最多启动一个结果写任务。
type resultWriter struct {
	config resultWriterConfig
}

// resultWriterExitKind 说明任务为何返回，供连接拥有者做后续处理。
type resultWriterExitKind uint8

const (
	writerResultsComplete resultWriterExitKind = iota // Worker 正常完成，当前需要投递的结果已写完或被确认跳过。
	writerStopped                                     // 生命周期取消、控制结束或本代附着资格已失效。
	writerWriteFailed                                 // 实际 WebSocket Write 出错或达到单次期限。
	writerControlFailed                               // 命令/编码等内部错误，不能一律解释为断网。
	writerCompletionSent                              // 本代 completed 已写成功，仍等待客户端确认。
)

// resultWriterExit 在 run 返回时交给连接拥有者，不等同于整场会话终态。
type resultWriterExit struct {
	kind    resultWriterExitKind
	lastSeq uint64 // 两种正常输出完成时有效；无识别结果为 0。
	err     error  // 完成时 nil；其他退出必须带原因，保留 errors.Is 身份。
}

// newResultWriter 校验 session、conn、controlCtx 非 nil，generation 非零及期限为正。
// 非法参数返回 nil 和 errInvalidResultWriterConfig；不启动任务或操作/关闭连接。
func newResultWriter(config resultWriterConfig) (*resultWriter, error) {
	if config.session == nil ||
		config.generation == 0 ||
		config.conn == nil ||
		config.writeTimeout <= 0 ||
		config.controlCtx == nil {
		return nil, errInvalidResultWriterConfig
	}

	return &resultWriter{
		config: config,
	}, nil
}

// run 在调用方提供的 goroutine 中执行固定代次的结果发送循环。
// ctx 属于本代连接，必须继承 config.controlCtx 的逻辑会话生命周期；nil 为编程错误。
// 连接拥有者还须在 controlDone 关闭时取消本代 ctx，并负责关闭连接和等待读写任务。
// 正常结果发送结束仍保留连接，后续终态投递/确认由连接输出流程继续完成。
func (w *resultWriter) run(ctx context.Context) resultWriterExit {
	return w.runLoop(ctx, nil)
}

// runLoop 串行发送输入累计确认和已经授权的结果。
// lastInput 仅由当前写任务访问；非 nil 时初值来自已成功写出的 ready，
// nil 保留独立结果部件入口的行为。生产附着始终通过 runWithReady 启用确认。
// ctx 为本代连接生命周期；结果写成功仍使用 controlCtx 汇报。
func (w *resultWriter) runLoop(ctx context.Context, lastInput *inputAcceptance) resultWriterExit {
	if ctx == nil {
		panic("gateway: nil result writer context")
	}

	for {
		if cause := w.stopCause(ctx); cause != nil {
			return w.stoppedExit(cause)
		}

		offer, err := w.config.session.requestResult(
			ctx,
			w.config.generation,
		)
		if err != nil {
			return w.classifyControlError(ctx, err)
		}

		// requestResult 可能已经建立 inFlight 授权。
		// 此后发现连接停止时不能自行撤销授权；
		// 连接拥有者会通过 detach/close 完成代次清理。
		if cause := w.stopCause(ctx); cause != nil {
			return w.stoppedExit(cause)
		}

		if lastInput != nil && *lastInput != offer.input {
			message := wsprotocol.AudioAckMessage{
				Type:       wsprotocol.MessageTypeAudioAck,
				Generation: w.config.generation,
				NextOffset: offer.input.nextOffset,
				InputEnded: offer.input.inputEnded,
			}
			data, err := json.Marshal(message)
			if err != nil {
				return w.controlExit(fmt.Errorf("marshal audio acknowledgement: %w", err))
			}
			if cause := w.stopCause(ctx); cause != nil {
				return w.stoppedExit(cause)
			}
			if err := w.writeMessage(ctx, data); err != nil {
				if cause := w.stopCause(ctx); cause != nil {
					return w.stoppedExit(cause)
				}
				return resultWriterExit{kind: writerWriteFailed, err: err}
			}
			// 只记录本次实际写出的快照；Write 期间的新输入留给下一轮。
			// 本次 offer 可能已经借出结果，不能在 ACK 后直接 continue。
			*lastInput = offer.input
		}

		if cause := w.stopCause(ctx); cause != nil {
			return w.stoppedExit(cause)
		}

		if offer.available {
			message := wsprotocol.SequencedResultMessage{
				Type:      wsprotocol.MessageTypeResult,
				Seq:       offer.result.seq,
				SegmentID: offer.result.segmentID,
				Text:      offer.result.text,
				IsFinal:   offer.result.isFinal,
			}

			data, err := json.Marshal(message)
			if err != nil {
				return w.controlExit(
					fmt.Errorf("marshal sequenced result: %w", err),
				)
			}

			// controlCtx 按契约应是连接 ctx 的祖先生命周期。
			// 这里再检查一次，避免编码期间会话已经结束后仍开始新 Write。
			if cause := w.stopCause(ctx); cause != nil {
				return w.stoppedExit(cause)
			}

			if err := w.writeMessage(ctx, data); err != nil {
				// 生命周期取消优先于 I/O 分类。
				if cause := w.stopCause(ctx); cause != nil {
					return w.stoppedExit(cause)
				}

				return resultWriterExit{
					kind: writerWriteFailed,
					err:  err,
				}
			}

			// Write 已经明确成功。即使连接 ctx 此刻刚被取消，
			// 仍先使用逻辑会话 context 报告这个已经发生的事实。
			handled, err := w.config.session.reportResultWritten(
				w.config.controlCtx,
				w.config.generation,
				offer.result.seq,
			)
			if err != nil {
				return w.classifyControlError(ctx, err)
			}

			if !handled {
				return w.stoppedExit(
					errResultWriterSuperseded,
				)
			}

			continue
		}

		if offer.workerCompleted {
			if lastInput != nil {
				return w.sendCompletion(ctx)
			}
			return resultWriterExit{
				kind:    writerResultsComplete,
				lastSeq: offer.lastSeq,
			}
		}

		// 这些通道只负责唤醒。
		// 多个停止信号可能同时就绪，不能由 select 随机决定退出原因。
		// 下一轮循环先经过 stopCause，统一按既定优先级裁决。
		select {
		case <-offer.changed:
		case <-w.config.controlCtx.Done():
		case <-ctx.Done():
		case <-w.config.session.controlDone:
		}
	}
}

// sendCompletion 在本代结果全部写完后取得授权并限时写正常完成通知。
// ctx 属于连接；不关闭连接、不等待客户端确认、不创建额外任务。
func (w *resultWriter) sendCompletion(ctx context.Context) resultWriterExit {
	if cause := w.stopCause(ctx); cause != nil {
		return w.stoppedExit(cause)
	}
	snapshot, err := w.config.session.requestCompletion(ctx, w.config.generation)
	if err != nil {
		return w.classifyControlError(ctx, err)
	}
	if cause := w.stopCause(ctx); cause != nil {
		return w.stoppedExit(cause)
	}
	data, err := json.Marshal(wsprotocol.CompletedMessage{
		Type: wsprotocol.MessageTypeCompleted, Generation: w.config.generation,
		FinalOffset: snapshot.finalOffset, LastSeq: snapshot.lastSeq,
	})
	if err != nil {
		return w.controlExit(fmt.Errorf("marshal session completion: %w", err))
	}
	if cause := w.stopCause(ctx); cause != nil {
		return w.stoppedExit(cause)
	}
	if err := w.writeMessage(ctx, data); err != nil {
		if cause := w.stopCause(ctx); cause != nil {
			return w.stoppedExit(cause)
		}
		return resultWriterExit{kind: writerWriteFailed, err: err}
	}
	return resultWriterExit{kind: writerCompletionSent, lastSeq: snapshot.lastSeq}
}

// writeMessage 同步写入已经编码的一条连接文本消息。
// 每次建立独立期限，返回前读取该期限的状态，再取消计时资源。
// 父 ctx 取消优先，其次本次超时 ErrResultWriteTimeout，最后保留 Write 原始错误。
// 不启动另一个 goroutine 竞速超时；须等实际 Write 返回才结束调用。
func (w *resultWriter) writeMessage(ctx context.Context, data []byte) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	writeCtx, cancel := context.WithTimeout(
		ctx,
		w.config.writeTimeout,
	)

	err := w.config.conn.Write(
		writeCtx,
		websocket.MessageText,
		data,
	)

	// 必须在 cancel 前保存状态；否则主动 cancel 会污染
	// 对本次 Write 是否真正超时的判断。
	writeCtxErr := writeCtx.Err()
	parentCause := context.Cause(ctx)

	cancel()

	// 连接生命周期取消优先于本次 Write 的期限和底层错误。
	if parentCause != nil {
		return parentCause
	}

	if errors.Is(writeCtxErr, context.DeadlineExceeded) {
		return ErrResultWriteTimeout
	}

	if err != nil {
		return fmt.Errorf("write connection message: %w", err)
	}

	return nil
}

// stopCause 返回写任务当前已经能够观察到的停止原因。
// controlCtx 优先于连接 ctx；controlDone 负责覆盖协调循环已经退出、
// 但调用方尚未来得及取消连接 ctx 的窗口。
func (w *resultWriter) stopCause(ctx context.Context) error {
	if cause := context.Cause(w.config.controlCtx); cause != nil {
		return cause
	}

	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	select {
	case <-w.config.session.controlDone:
		return errResumeClosed
	default:
		return nil
	}
}

// stoppedExit 返回本代写任务的停止原因；非完成退出不携带 lastSeq。
func (w *resultWriter) stoppedExit(err error) resultWriterExit {
	return resultWriterExit{
		kind: writerStopped,
		err:  err,
	}
}

// controlExit 保留内部命令或编码错误，供连接拥有者区别于传输故障。
func (w *resultWriter) controlExit(err error) resultWriterExit {
	return resultWriterExit{
		kind: writerControlFailed,
		err:  err,
	}
}

// classifyControlError 对协调命令失败进行分类。
// 若生命周期已经停止，停止原因优先于命令返回的竞争错误。
func (w *resultWriter) classifyControlError(
	ctx context.Context,
	err error,
) resultWriterExit {
	if cause := w.stopCause(ctx); cause != nil {
		return w.stoppedExit(cause)
	}

	if errors.Is(err, errResumeClosed) ||
		errors.Is(err, errSessionNotAttached) ||
		errors.Is(err, errSessionGenerationMismatch) {
		return w.stoppedExit(err)
	}

	return w.controlExit(err)
}

// runWithReady 是已接管连接唯一写任务的入口，只运行一次。
// ctx 属于本代连接，继承逻辑会话生命周期。
// 先发送 ready，成功后同步进入累计输入确认和结果循环。
// 不启动额外 goroutine，不关闭连接或自行 detach。
func (w *resultWriter) runWithReady(ctx context.Context) resultWriterExit {
	if ctx == nil {
		panic("gateway: nil result writer context")
	}

	if cause := w.stopCause(ctx); cause != nil {
		return w.stoppedExit(cause)
	}

	ready, err := w.config.session.requestConnectionReady(
		ctx,
		w.config.generation,
	)
	if err != nil {
		return w.classifyControlError(ctx, err)
	}

	// 快照命令已经完成，但连接可能在此时失去资格。
	// 不能在已经观察到停止以后继续写 ready。
	if cause := w.stopCause(ctx); cause != nil {
		return w.stoppedExit(cause)
	}

	message := wsprotocol.ReadyMessage{
		Type:           wsprotocol.MessageTypeReady,
		SessionID:      ready.sessionID,
		ResumeToken:    ready.resumeToken,
		Generation:     ready.generation,
		NextOffset:     ready.nextOffset,
		InputEnded:     ready.inputEnded,
		AckedResultSeq: ready.ackedResultSeq,
	}

	data, err := json.Marshal(message)
	if err != nil {
		// 不要把 message 或 token 放进错误文本。
		return w.controlExit(
			fmt.Errorf("marshal ready message: %w", err),
		)
	}

	// 编码本身不修改状态，但编码期间生命周期仍可能结束。
	if cause := w.stopCause(ctx); cause != nil {
		return w.stoppedExit(cause)
	}

	if err := w.writeMessage(ctx, data); err != nil {
		// 生命周期停止优先于网络错误分类，与普通 result Write 一致。
		if cause := w.stopCause(ctx); cause != nil {
			return w.stoppedExit(cause)
		}

		return resultWriterExit{
			kind: writerWriteFailed,
			err:  err,
		}
	}

	// ready 已经同步写成功。
	// 用真正发出的 ready 初始化本代输入确认；不能重新查询后跳过未发送变化。
	lastInput := inputAcceptance{nextOffset: ready.nextOffset, inputEnded: ready.inputEnded}
	return w.runLoop(ctx, &lastInput)
}
