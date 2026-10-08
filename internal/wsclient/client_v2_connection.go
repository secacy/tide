package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// v2ManagedConnection 加上生命周期能力，真实 socket 与可控连接均可使用。
type v2ManagedConnection interface {
	v2Connection
	SetReadLimit(int64)
	CloseNow() error
}

// v2Dialer 每次创建一个候选，不访问业务状态；生产使用 websocket.Dial。
type v2Dialer func(context.Context, string) (v2ManagedConnection, *http.Response, error)

// v2AttemptFailureKind 在错误产生处确定；回调/源错误不进入传输分类。
type v2AttemptFailureKind uint8

const (
	v2AttemptOK        v2AttemptFailureKind = iota // 无失败。
	v2AttemptTransport                             // I/O 故障，重试前再排除协议/策略错误。
	v2AttemptEntry                                 // 有效入口拒绝，按稳定 code 判断。
	v2AttemptTerminal                              // 协议、状态或清理失败，不能重试。
)

// v2AttemptFailure 保留错误类别与原因，不通过提示文本决定恢复。
type v2AttemptFailure struct {
	kind v2AttemptFailureKind
	err  error
}

// retryableV2Failure 只判断类别；生命周期、身份、时间及次数由协调者检查。
func retryableV2Failure(f v2AttemptFailure) bool {
	if f.err == nil {
		return false
	}
	switch f.kind {
	case v2AttemptTransport:
		if errors.Is(f.err, errV2ClientProtocol) || errors.Is(f.err, websocket.ErrMessageTooBig) {
			return false
		}
		switch websocket.CloseStatus(f.err) {
		case -1, websocket.StatusNormalClosure, websocket.StatusGoingAway:
			return true
		default:
			return false
		}
	case v2AttemptEntry:
		var entry *V2EntryError
		if !errors.As(f.err, &entry) {
			return false
		}
		switch entry.Code {
		case wsprotocol.V2ErrorSessionBusy, wsprotocol.V2ErrorHandshakeLimit, wsprotocol.V2ErrorEntryTimeout:
			return true
		}
	}
	return false
}

// v2HandshakeResult 只在 done 关闭后读取；conn 非 nil 时拥有尚未交接的候选。
type v2HandshakeResult struct {
	conn    v2ManagedConnection
	ready   wsprotocol.ReadyMessage
	failure v2AttemptFailure
}

// v2HandshakeAttempt 使用后不复制；任务只写 result 一次，再关闭 done。
// cancel 只终止握手，不作为未来连接任务的父 context。
type v2HandshakeAttempt struct {
	cancel context.CancelFunc
	done   chan struct{}
	result v2HandshakeResult
}

// startV2Handshake 启动一次异步限时握手；request 是独立协议值，含凭据勿记录。
// 任务独占候选，失败实际关闭后发布 done，成功等待协调者取走 conn。
func (c *V2Client) startV2Handshake(ctx context.Context, request any, deadline time.Time, dial v2Dialer) *v2HandshakeAttempt {
	handshakeCtx, cancel := context.WithDeadline(ctx, deadline)
	attempt := &v2HandshakeAttempt{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(attempt.done)
		defer cancel()
		var conn v2ManagedConnection
		defer func() {
			if conn != nil && attempt.result.conn == nil {
				if err := closeV2Socket(conn); err != nil {
					attempt.result.failure = v2AttemptFailure{kind: v2AttemptTerminal, err: errors.Join(attempt.result.failure.err, err)}
				}
			}
		}()
		conn, response, err := dial(handshakeCtx, c.cfg.URL)
		if err != nil {
			failure := v2AttemptFailure{kind: v2AttemptTransport, err: fmt.Errorf("dial v2 websocket: %w", err)}
			if response != nil {
				failure.kind = v2AttemptTerminal
				if response.StatusCode == http.StatusServiceUnavailable {
					failure.kind = v2AttemptTransport
					if response.Body != nil {
						// coder/websocket 提供最多 1024 字节的失败响应副本，网络 Body 已关闭。
						data, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
						if msg, parseErr := decodeV2Message(data); parseErr == nil && msg.typ == wsprotocol.MessageTypeError {
							failure = v2AttemptFailure{kind: v2AttemptEntry, err: &V2EntryError{Code: msg.entryError.Code}}
						}
					}
				}
			}
			attempt.result.failure = failure
			return
		}
		conn.SetReadLimit(c.cfg.ReadLimit)
		data, err := json.Marshal(request)
		if err != nil {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptTerminal, err: errV2ClientState}
			return
		}
		if err := conn.Write(handshakeCtx, websocket.MessageText, data); err != nil {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptTransport, err: fmt.Errorf("write v2 handshake: %w", err)}
			return
		}
		typ, data, err := conn.Read(handshakeCtx)
		if err != nil {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptTransport, err: fmt.Errorf("read v2 ready: %w", err)}
			return
		}
		if typ != websocket.MessageText {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptTerminal, err: errV2ClientProtocol}
			return
		}
		msg, err := decodeV2Message(data)
		if err != nil {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptTerminal, err: err}
			return
		}
		if msg.typ == wsprotocol.MessageTypeError {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptEntry, err: &V2EntryError{Code: msg.entryError.Code}}
			return
		}
		if msg.typ != wsprotocol.MessageTypeReady {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptTerminal, err: errV2ClientProtocol}
			return
		}
		if err := handshakeCtx.Err(); err != nil {
			attempt.result.failure = v2AttemptFailure{kind: v2AttemptTransport, err: err}
			return
		}
		attempt.result.conn, attempt.result.ready = conn, msg.ready
	}()
	return attempt
}

// v2ClientConnection 只拥有本次 socket 和 reader/writer；不拥有源或业务状态。
// 使用后不复制；协调者独占调度字段，cleanup 只在 done 前写 closeErr。
type v2ClientConnection struct {
	conn                 v2ManagedConnection // 本次 socket，由 beginClose 实际关闭。
	generation           uint64              // 绑定当前事件，不使用报文自报代次替代。
	cancel               context.CancelFunc  // 只取消本连接。
	readEvents           chan v2ReadEvent    // 容量 1，消息与终态有序交付。
	writeJobs            chan v2WriteJob     // 容量 1，包含控制帧在内仅一个待写/在途任务。
	writeReceipts        chan v2WriteReceipt // 容量 1，保留每个实际 Write 的结果。
	tasks                sync.WaitGroup      // 只等待本连接 reader/writer。
	closing              bool                // 协调者保证清理只启动一次。
	done                 chan struct{}       // CloseNow 和两个任务实际退出后关闭。
	closeErr             error               // 只能在观察 done 后读取。
	writerBusy           bool                // 派发前设置，实际回执处理后清零。
	lastWriteKind        v2WriteKind         // 本连接 ACK/音频交替调度依据。
	lastResultAckWritten uint64              // 从 ready 校准，之后只按成功写回执推进。
}

// startV2Connection 在 ready 已接纳后启动固定连接的两个任务。
// 业务 context 继承整场，不继承握手的限时 context。
func (c *V2Client) startV2Connection(ctx context.Context, conn v2ManagedConnection, ready wsprotocol.ReadyMessage) *v2ClientConnection {
	connectionCtx, cancel := context.WithCancel(ctx)
	a := &v2ClientConnection{
		conn: conn, generation: ready.Generation, cancel: cancel,
		readEvents:    make(chan v2ReadEvent, 1),
		writeJobs:     make(chan v2WriteJob, 1),
		writeReceipts: make(chan v2WriteReceipt, 1),
		done:          make(chan struct{}), lastResultAckWritten: ready.AckedResultSeq,
	}
	a.tasks.Go(func() { readV2Events(connectionCtx, conn, a.readEvents) })
	a.tasks.Go(func() { c.writeV2Jobs(connectionCtx, conn, a.writeJobs, a.writeReceipts) })
	return a
}

// beginClose 异步且恰好一次关闭/等待；协调者仍可接收新采集音频。
func (a *v2ClientConnection) beginClose() {
	if a.closing {
		return
	}
	a.closing = true
	if a.cancel != nil {
		a.cancel()
	}
	go func() {
		a.closeErr = closeV2Socket(a.conn)
		a.tasks.Wait()
		close(a.done)
	}()
}

// closeV2Socket 忽略已经关闭的预期结果，其他清理错误保留。
func closeV2Socket(conn v2ManagedConnection) error {
	if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("close v2 websocket: %w", err)
	}
	return nil
}
