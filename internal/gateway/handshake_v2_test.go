package gateway

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

const validV2Start = `{"type":"start","version":"v2"}`

// assertHandshakeReadFailure 不打印含恢复凭据的请求值。
func assertHandshakeReadFailure(t *testing.T, got wsprotocol.V2Handshake, err, want error) {
	t.Helper()
	if got != (wsprotocol.V2Handshake{}) || !errors.Is(err, want) {
		t.Fatal("unexpected handshake failure (credentials redacted)", err)
	}
}

func TestV2HandshakeReadInvalidConfig(t *testing.T) {
	for _, name := range []string{"nil_context", "nil_connection", "zero_timeout", "negative_timeout", "zero_limit", "negative_limit"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			conn := &attachmentTestConn{}
			var reader connectionReadConn = conn
			timeout, limit := time.Second, int64(128)
			switch name {
			case "nil_context":
				ctx = nil
			case "nil_connection":
				reader = nil
			case "zero_timeout":
				timeout = 0
			case "negative_timeout":
				timeout = -time.Second
			case "zero_limit":
				limit = 0
			case "negative_limit":
				limit = -1
			}
			got, err := readV2Handshake(ctx, reader, timeout, limit)
			assertHandshakeReadFailure(t, got, err, errInvalidV2HandshakeReaderConfig)
			if conn.reads.Load() != 0 || conn.limit.Load() != 0 || conn.writes.Load() != 0 || conn.closes.Load() != 0 {
				t.Fatal("invalid config touched connection")
			}
		})
	}
}

func TestV2HandshakeReadPrecanceled(t *testing.T) {
	for _, name := range []string{"cause", "deadline"} {
		t.Run(name, func(t *testing.T) {
			cause := errors.New("gateway stopping")
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(cause)
			var parent context.Context = ctx
			want := error(cause)
			if name == "deadline" {
				expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stop()
				parent = expired
				want = context.DeadlineExceeded
			}
			conn := &attachmentTestConn{}
			got, err := readV2Handshake(parent, conn, time.Second, 128)
			assertHandshakeReadFailure(t, got, err, want)
			if conn.reads.Load() != 0 || conn.limit.Load() != 0 {
				t.Fatal("already stopped parent started reading")
			}
		})
	}
}

func TestV2HandshakeReadSingleMessageAndContextRelease(t *testing.T) {
	for _, name := range []string{"start", "resume", "protocol_failure", "binary", "transport"} {
		t.Run(name, func(t *testing.T) {
			conn := &attachmentTestConn{}
			var readCtx context.Context
			wire := validV2Start
			kind := wsprotocol.V2HandshakeStart
			transport := errors.New("read transport failure")
			if name == "resume" {
				wire = `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"token","appliedSeq":"12"}`
				kind = wsprotocol.V2HandshakeResume
			}
			conn.readFn = func(ctx context.Context) (websocket.MessageType, []byte, error) {
				readCtx = ctx
				if conn.limit.Load() != 128 {
					t.Error("Read happened before setting message limit")
				}
				if conn.reads.Load() != 1 {
					t.Error("handshake consumed more than one message")
				}
				switch name {
				case "protocol_failure":
					return websocket.MessageText, []byte(`{"type":"end"}`), nil
				case "binary":
					return websocket.MessageBinary, []byte(validV2Start), nil
				case "transport":
					return 0, nil, transport
				}
				return websocket.MessageText, []byte(wire), nil
			}
			got, err := readV2Handshake(context.Background(), conn, time.Second, 128)
			switch name {
			case "protocol_failure", "binary":
				assertHandshakeReadFailure(t, got, err, wsprotocol.ErrInvalidV2Handshake)
			case "transport":
				assertHandshakeReadFailure(t, got, err, transport)
			default:
				if err != nil || got.Kind != kind {
					t.Fatal("valid handshake rejected", err)
				}
				if name == "resume" && (got.SessionID != "id" || got.ResumeToken != "token" || got.AppliedSeq != 12) {
					t.Fatal("resume fields changed")
				}
			}
			if conn.reads.Load() != 1 || conn.writes.Load() != 0 || conn.closes.Load() != 0 {
				t.Fatal("handshake took extra I/O ownership")
			}
			if readCtx == nil || !errors.Is(readCtx.Err(), context.Canceled) {
				t.Fatal("child context not released after return")
			}
		})
	}
}

func TestV2HandshakeReadCancellationPriority(t *testing.T) {
	for _, name := range []string{"parent_cause_over_success", "parent_cause_over_protocol", "parent_cause_over_transport", "own_timeout_over_success", "own_timeout_over_transport", "parent_deadline", "parent_cause_over_own_timeout"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				cause := errors.New("logical operation canceled")
				var ctx context.Context = parent
				want := error(cause)
				if name == "parent_deadline" {
					deadline, stop := context.WithTimeout(parent, time.Second)
					defer stop()
					ctx = deadline
					want = context.DeadlineExceeded
				}
				conn := &attachmentTestConn{readFn: func(readCtx context.Context) (websocket.MessageType, []byte, error) {
					switch name {
					case "own_timeout_over_success", "own_timeout_over_transport", "parent_deadline", "parent_cause_over_own_timeout":
						<-readCtx.Done()
						if name == "parent_cause_over_own_timeout" {
							cancel(cause)
						}
					default:
						cancel(cause)
					}
					if name == "parent_cause_over_protocol" {
						return websocket.MessageText, []byte(`{`), nil
					}
					if name == "parent_cause_over_transport" || name == "own_timeout_over_transport" {
						return 0, nil, io.EOF
					}
					return websocket.MessageText, []byte(validV2Start), nil
				}}
				if name == "own_timeout_over_success" || name == "own_timeout_over_transport" {
					want = ErrHandshakeTimeout
				}
				got, err := readV2Handshake(ctx, conn, 3*time.Second, 128)
				assertHandshakeReadFailure(t, got, err, want)
				if conn.reads.Load() != 1 || conn.closes.Load() != 0 {
					t.Fatal("unexpected read/close count")
				}
			})
		})
	}
}

func TestV2HandshakeReadWaitsForActualReturn(t *testing.T) {
	for _, name := range []string{"timeout", "parent_cancel"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				entered, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				conn := &attachmentTestConn{readFn: func(ctx context.Context) (websocket.MessageType, []byte, error) {
					close(entered)
					<-ctx.Done()
					close(stopped)
					<-release
					return websocket.MessageText, []byte(validV2Start), nil
				}}
				finished := make(chan struct{})
				var got wsprotocol.V2Handshake
				var err error
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				go func() { got, err = readV2Handshake(parent, conn, time.Second, 128); close(finished) }()
				t.Cleanup(func() { cancel(nil); unblock(); <-finished })
				<-entered
				want := error(ErrHandshakeTimeout)
				if name == "parent_cancel" {
					want = errors.New("parent stop")
					cancel(want)
				} else {
					time.Sleep(time.Second)
				}
				<-stopped
				synctest.Wait()
				select {
				case <-finished:
					t.Fatal("handshake returned while actual Read still blocked")
				default:
				}
				unblock()
				<-finished
				assertHandshakeReadFailure(t, got, err, want)
				if conn.closes.Load() != 0 {
					t.Fatal("handshake closed caller-owned connection")
				}
			})
		})
	}
}
