package gateway

import (
	"context"
	"errors"
	"net"
	"time"
)

var (
	errInvalidConnectionCandidate  = errors.New("invalid connection candidate")  // 候选连接或它的固定配置非法。
	errInvalidConnectionAttachment = errors.New("invalid connection attachment") // attachment 的会话、生命周期或代次非法。
)

// sessionConnection 表示一条固定 WebSocket 的读写和关闭能力。
// CloseNow 可能等待，不能在会话协调循环中调用。
type sessionConnection interface {
	connectionReadConn
	resultWriteConn
	CloseNow() error
}

// connectionCandidate 尚未被会话接管。
// 接纳之前，由提交者负责关闭；构造后不可修改或并发复用。
type connectionCandidate struct {
	conn            sessionConnection // 固定句柄，成功安装前由提交者负责关闭。
	writeTimeout    time.Duration     // 单次写期限，必须为正。
	maxMessageBytes int64             // 完整消息上限，至少 9。
}

// newConnectionCandidate 校验配置，不启动任务、不设置连接参数、不关闭连接。
// 非法配置返回 nil、errInvalidConnectionCandidate；清理责任仍属调用者。
func newConnectionCandidate(conn sessionConnection, writeTimeout time.Duration, maxMessageBytes int64) (*connectionCandidate, error) {
	candidate := &connectionCandidate{
		conn:            conn,
		writeTimeout:    writeTimeout,
		maxMessageBytes: maxMessageBytes,
	}

	if err := validateConnectionCandidate(candidate); err != nil {
		return nil, err
	}

	return candidate, nil
}

// connectionTaskKind 标识退出事件来自哪个任务；零值无效。
type connectionTaskKind uint8

const (
	connectionTaskInvalid connectionTaskKind = iota // 无效事件来源。
	connectionReaderTask                            // 本代唯一读取任务的退出事实。
	connectionWriterTask                            // 本代唯一结果写任务的退出事实。
)

// connectionEvent 只传递已经发生的退出事实，不自行决定会话终态。
type connectionEvent struct {
	generation uint64             // 固定服务端代次，不能来自客户端消息。
	task       connectionTaskKind // 决定下面哪个退出值有效。
	reader     connectionReaderExit
	writer     resultWriterExit
}

// connectionAttachment 拥有一条已接管连接及其任务；构造后以指针使用。
// 控制字段在启动前设置，此后不替换 conn/generation/reader/writer。
type connectionAttachment struct {
	generation  uint64                  // 安装时分配的固定代次。
	conn        sessionConnection       // 固定句柄，唯一主动关闭者为本对象的 run。
	controlCtx  context.Context         // 逻辑生命周期，判定停止原因时优先于本代 ctx。
	ctx         context.Context         // 逻辑生命周期的子 context。
	cancel      context.CancelCauseFunc // 停止本代，不向上取消 Worker。
	controlDone <-chan struct{}         // 控制结束可先于逻辑 ctx 取消。
	reader      *connectionReader       // 本代唯一读取任务。
	writer      *resultWriter           // 本代唯一结果写任务。
	events      chan connectionEvent    // 容量恰好 2，两个任务各报告一次。
	done        chan struct{}           // run 在实际关闭和读写全部返回后关闭。
	closeErr    error                   // 仅 run 写；其他调用者只能在 done 后读取。
}

// validateConnectionCandidate 校验非 nil 候选、固定 socket 及正写期限/消息上限。
// 只读配置，不操作 socket，也不转移其清理责任。
func validateConnectionCandidate(candidate *connectionCandidate) error {
	if candidate == nil ||
		candidate.conn == nil ||
		candidate.writeTimeout <= 0 ||
		candidate.maxMessageBytes < 9 {
		return errInvalidConnectionCandidate
	}

	return nil
}

// newConnectionAttachment 准备本代 context 和真实读写任务。
// 不启动 goroutine、不操作 socket，也不转移候选的关闭责任。
// controlCtx 属于逻辑会话，generation 是服务端分配的非零代次。
// 失败释放已经创建的子 context；成功但未安装时，调用者仅取消该子 context。
func newConnectionAttachment(
	s *resumableSession,
	controlCtx context.Context,
	generation uint64,
	candidate *connectionCandidate,
) (*connectionAttachment, error) {
	if s == nil ||
		controlCtx == nil ||
		generation == 0 {
		return nil, errInvalidConnectionAttachment
	}

	if err := validateConnectionCandidate(candidate); err != nil {
		return nil, err
	}

	connectionCtx, cancel := context.WithCancelCause(controlCtx)

	reader, err := newConnectionReader(connectionReaderConfig{
		session:         s,
		generation:      generation,
		conn:            candidate.conn,
		controlCtx:      controlCtx,
		maxMessageBytes: candidate.maxMessageBytes,
	})
	if err != nil {
		cancel(err)
		return nil, err
	}

	writer, err := newResultWriter(resultWriterConfig{
		session:      s,
		generation:   generation,
		conn:         candidate.conn,
		writeTimeout: candidate.writeTimeout,
		controlCtx:   controlCtx,
	})
	if err != nil {
		cancel(err)
		return nil, err
	}

	return &connectionAttachment{
		generation:  generation,
		conn:        candidate.conn,
		controlCtx:  controlCtx,
		ctx:         connectionCtx,
		cancel:      cancel,
		controlDone: s.controlDone,
		reader:      reader,
		writer:      writer,
		events:      make(chan connectionEvent, 2),
		done:        make(chan struct{}),
	}, nil
}

// stop 只取消本代 context，可重复调用。
// 不执行 CloseNow，不等待任务。
func (a *connectionAttachment) stop(cause error) {
	a.cancel(cause)
}

// stopCause 只在 attachment 自己决定如何结束资源等待时使用。
// controlCtx 优先，其次本代 ctx，最后是 controlDone。
func (a *connectionAttachment) stopCause() error {
	if cause := context.Cause(a.controlCtx); cause != nil {
		return cause
	}

	if cause := context.Cause(a.ctx); cause != nil {
		return cause
	}

	select {
	case <-a.controlDone:
		return errResumeClosed
	default:
		return nil
	}
}

// run 启动读写任务，并承担本连接的关闭和共同等待。
// 由会话拥有者启动一次。
// 两个任务各报告一次到容量为 2 的 events，不需要协调者继续消费才能退出。
// 正常 writer 返回仍保留 reader；done 仅在关闭调用和两个任务均返回后关闭。
func (a *connectionAttachment) run() {
	readerDone := make(chan struct{})
	writerDone := make(chan struct{})

	go func() {
		exit := a.reader.run(a.ctx)

		a.events <- connectionEvent{
			generation: a.generation,
			task:       connectionReaderTask,
			reader:     exit,
		}

		close(readerDone)
	}()

	go func() {
		exit := a.writer.run(a.ctx)

		a.events <- connectionEvent{
			generation: a.generation,
			task:       connectionWriterTask,
			writer:     exit,
		}

		close(writerDone)
	}()

	// 任务事件本身不会结束 attachment。
	// 协调者先决定 detach/terminate，再通过 stop 或 controlDone 唤醒这里。
	select {
	case <-a.ctx.Done():
	case <-a.controlDone:
	}

	cause := a.stopCause()
	if cause == nil {
		cause = errResumeClosed
	}

	a.stop(cause)

	closeErr := a.conn.CloseNow()
	if errors.Is(closeErr, net.ErrClosed) {
		closeErr = nil
	}
	a.closeErr = closeErr

	<-readerDone
	<-writerDone

	close(a.done)
}
