package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

var (
	errInvalidConnectionReaderConfig = errors.New("invalid connection reader config") // 固定连接读取任务配置非法。
)

// connectionReadConn 是固定连接的读取能力；*websocket.Conn 直接满足。
// Read 必须响应 ctx 取消；同一连接只允许一个应用读取任务。
type connectionReadConn interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	SetReadLimit(int64)
}

// connectionReaderConfig 构造后不可变，不转移连接关闭责任。
type connectionReaderConfig struct {
	session         *resumableSession  // 已运行的逻辑会话，所有状态通过命令访问。
	generation      uint64             // 服务端分配的固定代次，非零。
	conn            connectionReadConn // 本次连接的固定句柄。
	controlCtx      context.Context    // 逻辑生命周期，用于识别停止原因。
	maxMessageBytes int64              // 完整消息上限，二进制包括 8 字节头；至少 9。
}

// connectionReader 由拥有者启动一次，不自行创建 goroutine。
type connectionReader struct{ config connectionReaderConfig }

// connectionReaderExitKind 区分停止、传输、线格式及会话命令失败。
type connectionReaderExitKind uint8

const (
	readerStopped        connectionReaderExitKind = iota // 生命周期/控制结束/代次失效。
	readerReadFailed                                     // 传输读取失败；包括对端正常关闭，不等于问诊完成。
	readerProtocolFailed                                 // 消息过大、不支持的类型或非法线格式。
	readerCommandFailed                                  // 协调者拒绝业务命令或内部控制错误；保留原错误。
)

// connectionReaderExit 是任务退出事实，所有退出均带非 nil 原因。
// 由后续连接拥有者决定 detach 或终止；reader 不做生命周期决策。
type connectionReaderExit struct {
	kind connectionReaderExitKind
	err  error
}

// newConnectionReader 校验非 nil 依赖、非零代次和至少 9 字节的消息上限。
// 非法配置返回 nil 和 errInvalidConnectionReaderConfig；不启动任务或操作连接。
func newConnectionReader(config connectionReaderConfig) (*connectionReader, error) {
	if config.session == nil ||
		config.generation == 0 ||
		config.conn == nil ||
		config.controlCtx == nil ||
		config.maxMessageBytes < 9 {
		return nil, errInvalidConnectionReaderConfig
	}

	return &connectionReader{
		config: config,
	}, nil
}

// stopCause 统一裁决 reader 当前已经能够观察到的停止原因。
// 逻辑会话取消优先于连接取消，controlDone 最后。
func (r *connectionReader) stopCause(
	ctx context.Context,
) error {
	if cause := context.Cause(r.config.controlCtx); cause != nil {
		return cause
	}

	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	select {
	case <-r.config.session.controlDone:
		return errResumeClosed

	default:
		return nil
	}
}

// stoppedExit 将已确定的生命周期或代次失效原因作为停止事实返回。
func (r *connectionReader) stoppedExit(err error) connectionReaderExit {
	return connectionReaderExit{
		kind: readerStopped,
		err:  err,
	}
}

// readFailedExit 保留原始传输错误，包括对端正常关闭。
func (r *connectionReader) readFailedExit(err error) connectionReaderExit {
	return connectionReaderExit{
		kind: readerReadFailed,
		err:  err,
	}
}

// protocolFailedExit 汇报线格式或单消息大小违规，不自行终止会话。
func (r *connectionReader) protocolFailedExit(err error) connectionReaderExit {
	return connectionReaderExit{
		kind: readerProtocolFailed,
		err:  err,
	}
}

// commandFailedExit 保留未被生命周期停止覆盖的会话命令错误。
func (r *connectionReader) commandFailedExit(err error) connectionReaderExit {
	return connectionReaderExit{
		kind: readerCommandFailed,
		err:  err,
	}
}

// classifyCommandError 对协调命令失败分类。
// 生命周期已经停止时，停止原因优先于与它竞争返回的业务错误。
func (r *connectionReader) classifyCommandError(
	ctx context.Context,
	err error,
) connectionReaderExit {
	if cause := r.stopCause(ctx); cause != nil {
		return r.stoppedExit(cause)
	}

	if errors.Is(err, errResumeClosed) ||
		errors.Is(err, errSessionNotAttached) ||
		errors.Is(err, errSessionGenerationMismatch) {
		return r.stoppedExit(err)
	}

	return r.commandFailedExit(err)
}

// run 在调用方提供的 goroutine 中串行读取固定连接。
// 一条输入同步提交完成以后，才读取下一条。
// ctx 必须非 nil 且继承 controlCtx；连接拥有者在 controlDone 时取消 ctx，
// 并负责关闭连接、等待本任务退出。合法 end 后仍继续读取 ACK。
// 本任务不写消息、不关闭连接、不决定 detach，也不取消 Worker RPC。
func (r *connectionReader) run(
	ctx context.Context,
) connectionReaderExit {
	if ctx == nil {
		panic("gateway: nil connection reader context")
	}

	if cause := r.stopCause(ctx); cause != nil {
		return r.stoppedExit(cause)
	}

	// 必须在第一次应用层 Read 之前建立上限。
	r.config.conn.SetReadLimit(
		r.config.maxMessageBytes,
	)

	for {
		// 停止以后不能开始新的 Read。
		if cause := r.stopCause(ctx); cause != nil {
			return r.stoppedExit(cause)
		}

		messageType, data, err := r.config.conn.Read(ctx)

		// 自己的会话/连接取消可能正是 Read 返回错误的原因。
		// 停止原因必须优先于底层网络错误。
		if cause := r.stopCause(ctx); cause != nil {
			return r.stoppedExit(cause)
		}

		if err != nil {
			if errors.Is(err, websocket.ErrMessageTooBig) {
				return r.protocolFailedExit(err)
			}

			return r.readFailedExit(err)
		}

		input, err := decodeConnectionInput(
			messageType,
			data,
		)
		if err != nil {
			// 解析期间也可能发生生命周期停止。
			if cause := r.stopCause(ctx); cause != nil {
				return r.stoppedExit(cause)
			}

			return r.protocolFailedExit(err)
		}

		// Read 与解析都完成以后再检查一次，避免在已停止的代次上
		// 提交一条刚刚读到的业务输入。
		if cause := r.stopCause(ctx); cause != nil {
			return r.stoppedExit(cause)
		}

		switch input.Kind {
		case wsprotocol.V2InputAudio:
			accepted, _, err := r.config.session.requestAudio(
				ctx,
				r.config.generation,
				input.Offset,
				input.Payload,
			)
			if err != nil {
				return r.classifyCommandError(ctx, err)
			}

			// accepted=false 表示合法历史重发。
			// Payload 的借用期到 requestAudio 返回为止。
			_ = accepted

		case wsprotocol.V2InputEnd:
			accepted, _, err := r.config.session.requestEnd(
				ctx,
				r.config.generation,
				input.Offset,
			)
			if err != nil {
				return r.classifyCommandError(ctx, err)
			}

			// accepted=false 可以是合法重复 end。
			// end 不结束 reader；客户端仍需要发送结果 ACK。
			_ = accepted

		case wsprotocol.V2InputResultAck:
			advanced, err := r.config.session.requestResultAck(
				ctx,
				r.config.generation,
				input.Seq,
			)
			if err != nil {
				return r.classifyCommandError(ctx, err)
			}

			// advanced=false 是合法的旧/重复累计确认。
			_ = advanced

		default:
			panic("gateway: decoded invalid v2 input kind")
		}

		// 命令已经成功提交的状态不会因为随后取消而回滚。
		// 这里只决定是否还能开始下一次 Read。
		if cause := r.stopCause(ctx); cause != nil {
			return r.stoppedExit(cause)
		}
	}
}

// decodeConnectionInput 只根据 WebSocket 消息类型选择线格式解析器。
// 音频范围、end 一致性和 ACK 上限继续由协调者判断。
func decodeConnectionInput(
	messageType websocket.MessageType,
	data []byte,
) (wsprotocol.V2Input, error) {
	switch messageType {
	case websocket.MessageBinary:
		return wsprotocol.DecodeV2Audio(data)

	case websocket.MessageText:
		return wsprotocol.DecodeV2Control(data)

	default:
		return wsprotocol.V2Input{}, fmt.Errorf(
			"%w: unsupported websocket message type %d",
			wsprotocol.ErrInvalidV2Input,
			messageType,
		)
	}
}
