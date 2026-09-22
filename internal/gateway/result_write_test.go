package gateway

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// resultWriteGate 在握手后阻塞底层 Write，Close 必须能将其唤醒。
// 它用于确定性验证真实 WebSocket 的取消路径，不代表 TCP 缓冲容量或性能。
type resultWriteGate struct {
	enabled   atomic.Bool
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

type resultWriteGateConn struct {
	net.Conn
	gate *resultWriteGate
}

func (c *resultWriteGateConn) Write(p []byte) (int, error) {
	if c.gate.enabled.Load() {
		c.gate.startOnce.Do(func() { close(c.gate.started) })
		<-c.gate.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}

func (c *resultWriteGateConn) Close() error {
	err := c.Conn.Close()
	c.gate.closeOnce.Do(func() { close(c.gate.closed) })
	return err
}

// resultWriteGateListener 仅包装本测试的一条客户端连接。
type resultWriteGateListener struct {
	net.Listener
	gate *resultWriteGate
}

func (l *resultWriteGateListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &resultWriteGateConn{Conn: c, gate: l.gate}, nil
}

// newResultWriter 建立真实 WebSocket；返回后由测试串行调用服务端 writeResult。
// handler 保持存活直至测试清理，避免其 defer 提前关闭连接。
func newResultWriter(t *testing.T, timeout time.Duration, blocked bool) (*session, *websocket.Conn, *resultWriteGate) {
	t.Helper()
	gate := &resultWriteGate{started: make(chan struct{}), closed: make(chan struct{})}
	ready := make(chan *session, 1)
	finish := make(chan struct{})
	handlerDone := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		ws, err := websocket.Accept(rw, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer ws.CloseNow()
		gate.enabled.Store(blocked)
		ready <- &session{ws: ws, resultWriteTimeout: timeout}
		<-finish
	}))
	server.Listener = &resultWriteGateListener{Listener: server.Listener, gate: gate}
	server.Start()
	var client *websocket.Conn
	t.Cleanup(func() {
		// 若用例失败，先关闭底层写入以唤醒待测调用，再让 handler 退出。
		gate.closeOnce.Do(func() { close(gate.closed) })
		if client != nil {
			_ = client.CloseNow()
		}
		close(finish)
		server.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var err error
	client, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-ready:
		return s, client, gate
	case <-handlerDone:
		t.Fatal("handler exited before initialization")
	case <-ctx.Done():
		t.Fatal("writer initialization timeout")
	}
	return nil, nil, nil
}

// TestResultWriteInvalidTimeout 将配置错误与实际写入期限到期区分开；nil ws 确保不进入网络操作。
func TestResultWriteInvalidTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			s := &session{resultWriteTimeout: timeout}
			err := s.writeResult(context.Background(), wsprotocol.ResultMessage{})
			if err == nil {
				t.Fatal("invalid timeout accepted")
			}
			if errors.Is(err, ErrResultWriteTimeout) {
				t.Fatalf("configuration error classified as runtime write timeout: %v", err)
			}
			if s.resultWriteCtx != nil {
				t.Fatal("invalid configuration started a write context")
			}
		})
	}
}

// TestResultWriteParentAlreadyDone 验证父取消/期限已经到达时不开始写入。
func TestResultWriteParentAlreadyDone(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			want := context.Canceled
			if deadline {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				want = context.DeadlineExceeded
			}
			s := &session{resultWriteTimeout: time.Second}
			err := s.writeResult(ctx, wsprotocol.ResultMessage{})
			if !errors.Is(err, want) || errors.Is(err, ErrResultWriteTimeout) {
				t.Fatalf("parent error=%v want=%v", err, want)
			}
			if s.resultWriteCtx != nil {
				t.Fatal("write started after parent ended")
			}
		})
	}
}

// TestResultWriteSuccessAndFreshDeadline 验证两条结果在各自预算内写入，不共用整段会话期限。
func TestResultWriteSuccessAndFreshDeadline(t *testing.T) {
	s, client, _ := newResultWriter(t, 200*time.Millisecond, false)
	if s.resultWriteExpired() {
		t.Fatal("no write reported expired")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var previous context.Context
	for i, text := range []string{"第一条结果", "第二条结果"} {
		msg := wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: text, IsFinal: i == 1}
		if err := s.writeResult(ctx, msg); err != nil {
			t.Fatal(err)
		}
		assertRecognitionResult(t, ctx, client, msg)
		s.resultWriteMu.Lock()
		writeCtx := s.resultWriteCtx
		s.resultWriteMu.Unlock()
		if writeCtx == nil || writeCtx == previous {
			t.Fatal("missing fresh write context")
		}
		if !errors.Is(writeCtx.Err(), context.Canceled) || s.resultWriteExpired() {
			t.Fatalf("successful write state: %v", writeCtx.Err())
		}
		previous = writeCtx
		if i == 0 {
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// TestResultWriteBlockedCancellation 用受控的底层 Write 验证超时和父取消均结束实际写入。
func TestResultWriteBlockedCancellation(t *testing.T) {
	for _, parentCancel := range []bool{false, true} {
		name := "own_deadline"
		budget := 75 * time.Millisecond
		if parentCancel {
			name = "parent_cancel"
			budget = time.Hour
		}
		t.Run(name, func(t *testing.T) {
			s, _, gate := newResultWriter(t, budget, true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- s.writeResult(ctx, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: "blocked"})
			}()
			joined := false
			defer func() {
				cancel()
				if !joined {
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("write did not exit after cleanup cancellation")
					}
				}
			}()
			select {
			case <-gate.started:
			case <-time.After(2 * time.Second):
				t.Fatal("write did not reach transport")
			}
			// Write 尚未返回时就能读到 context，协调者才有机会识别先到达的读错误。
			s.resultWriteMu.Lock()
			published := s.resultWriteCtx
			s.resultWriteMu.Unlock()
			if published == nil {
				t.Fatal("write context not published before transport Write")
			}
			if parentCancel {
				cancel()
			}
			var err error
			select {
			case err = <-done:
				joined = true
			case <-time.After(2 * time.Second):
				t.Fatal("actual Write did not exit")
			}
			if parentCancel {
				if !errors.Is(err, context.Canceled) || errors.Is(err, ErrResultWriteTimeout) || s.resultWriteExpired() {
					t.Fatalf("parent cancel misclassified: %v", err)
				}
			} else {
				if !errors.Is(err, ErrResultWriteTimeout) || !s.resultWriteExpired() {
					t.Fatalf("missing write timeout classification/state: %v", err)
				}
				if ctx.Err() != nil {
					t.Fatal("write deadline canceled parent")
				}
			}
			select {
			case <-gate.closed:
			default:
				t.Fatal("transport not closed by write cancellation")
			}
		})
	}
}

// TestResultWriteClosedConnection 保留已关闭连接的写入错误，不将其归入超时。
func TestResultWriteClosedConnection(t *testing.T) {
	s, _, _ := newResultWriter(t, time.Second, false)
	_ = s.ws.CloseNow()
	err := s.writeResult(context.Background(), wsprotocol.ResultMessage{Text: "after close"})
	if err == nil || errors.Is(err, ErrResultWriteTimeout) || s.resultWriteExpired() {
		t.Fatalf("closed connection error=%v", err)
	}
}
