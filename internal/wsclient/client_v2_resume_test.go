package wsclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

func TestV2ResumeConfig(t *testing.T) {
	t.Run("defaults_and_copy", func(t *testing.T) {
		cfg := &V2ResumeConfig{}
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: cfg})
		cfg.MaxAttempts = 99
		if c.cfg.Resume.MaxAttempts != 5 || c.cfg.Resume.Window != 5*time.Second || c.cfg.Resume.InitialBackoff != 100*time.Millisecond || c.cfg.Resume.MaxBackoff != time.Second {
			t.Fatalf("config=%+v", c.cfg.Resume)
		}
	})
	cases := map[string]V2ResumeConfig{"attempts": {MaxAttempts: -1}, "window": {Window: -1}, "initial": {InitialBackoff: -1}, "max": {MaxBackoff: -1}, "order": {InitialBackoff: 2 * time.Second, MaxBackoff: time.Second}}
	for name, resume := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewV2(V2Config{URL: "ws://test", Resume: &resume}); err == nil {
				t.Fatal("accepted invalid recovery config")
			}
		})
	}
	t.Run("backoff_cap_and_overflow", func(t *testing.T) {
		cfg := V2ResumeConfig{InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}
		want := []time.Duration{100, 200, 400, 800, 1000, 1000}
		for i, d := range want {
			if got := v2BackoffDelay(cfg, i+1); got != d*time.Millisecond {
				t.Fatalf("failure=%d delay=%v", i+1, got)
			}
		}
		cfg.InitialBackoff = time.Duration(1 << 62)
		cfg.MaxBackoff = time.Duration(1<<63 - 1)
		if got := v2BackoffDelay(cfg, 100); got != cfg.MaxBackoff {
			t.Fatalf("overflow delay=%v", got)
		}
	})
}

func TestV2ResumeFailureClassification(t *testing.T) {
	cases := []struct {
		name    string
		failure v2AttemptFailure
		want    bool
	}{
		{"network", v2AttemptFailure{v2AttemptTransport, io.EOF}, true},
		{"write_timeout", v2AttemptFailure{v2AttemptTransport, context.DeadlineExceeded}, true},
		{"oversized", v2AttemptFailure{v2AttemptTransport, websocket.ErrMessageTooBig}, false},
		{"binary_protocol", v2AttemptFailure{v2AttemptTransport, errV2ClientProtocol}, false},
		{"normal_close", v2AttemptFailure{v2AttemptTransport, websocket.CloseError{Code: websocket.StatusNormalClosure}}, true},
		{"going_away", v2AttemptFailure{v2AttemptTransport, websocket.CloseError{Code: websocket.StatusGoingAway}}, true},
		{"policy_close", v2AttemptFailure{v2AttemptTransport, websocket.CloseError{Code: websocket.StatusPolicyViolation}}, false},
		{"callback_timeout", v2AttemptFailure{v2AttemptTerminal, context.DeadlineExceeded}, false},
		{"success", v2AttemptFailure{}, false},
		{"empty_transport_failure", v2AttemptFailure{kind: v2AttemptTransport}, false},
	}
	for _, code := range []wsprotocol.V2ErrorCode{wsprotocol.V2ErrorServiceStopping, wsprotocol.V2ErrorHandshakeLimit, wsprotocol.V2ErrorSessionLimit, wsprotocol.V2ErrorInvalidHandshake, wsprotocol.V2ErrorEntryTimeout, wsprotocol.V2ErrorResumeUnavailable, wsprotocol.V2ErrorSessionBusy, wsprotocol.V2ErrorReplayGap, wsprotocol.V2ErrorInvalidResumePosition, wsprotocol.V2ErrorWorkerUnavailable, wsprotocol.V2ErrorInternalError, "future"} {
		cases = append(cases, struct {
			name    string
			failure v2AttemptFailure
			want    bool
		}{string(code), v2AttemptFailure{v2AttemptEntry, &V2EntryError{Code: code}}, code == wsprotocol.V2ErrorSessionBusy || code == wsprotocol.V2ErrorHandshakeLimit || code == wsprotocol.V2ErrorEntryTimeout})
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryableV2Failure(tt.failure); got != tt.want {
				t.Fatalf("retry=%v want=%v", got, tt.want)
			}
		})
	}
}

// 恢复夹具走实际整场协调及清理逻辑，只有连接/拨号由可控替身提供。
type recoveryDialRequest struct {
	ctx   context.Context
	reply chan recoveryDialReply
}
type recoveryDialReply struct {
	conn     v2ManagedConnection
	response *http.Response
	err      error
}
type recoveryOutcome struct {
	result V2RunResult
	err    error
}
type recoveryFixture struct {
	run   *v2ClientRun
	first *controlledV2Connection
	dials chan recoveryDialRequest
	done  chan recoveryOutcome
}

func newRecoveryFixture(t *testing.T, cfg V2Config, source io.ReadCloser, setup func(*recoveryFixture)) *recoveryFixture {
	t.Helper()
	cfg.URL = "ws://test"
	if cfg.Resume == nil {
		cfg.Resume = &V2ResumeConfig{}
	}
	c := v2TestClient(t, cfg)
	run, err := c.newV2Run(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	ready := wsprotocol.ReadyMessage{Type: wsprotocol.MessageTypeReady, SessionID: "s", ResumeToken: "secret", Generation: 1}
	if err := run.state.acceptReady(ready); err != nil {
		t.Fatal(err)
	}
	f := &recoveryFixture{run: run, first: newControlledV2Connection(), dials: make(chan recoveryDialRequest, 1), done: make(chan recoveryOutcome, 1)}
	run.dial = func(ctx context.Context, _ string) (v2ManagedConnection, *http.Response, error) {
		req := recoveryDialRequest{ctx: ctx, reply: make(chan recoveryDialReply, 1)}
		select {
		case f.dials <- req:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		select {
		case reply := <-req.reply:
			return reply.conn, reply.response, reply.err
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	if setup != nil {
		setup(f)
	}
	run.current = c.startV2Connection(run.ctx, f.first, ready)
	run.phase = v2Streaming
	run.startCapture()
	go func() {
		err := run.coordinate()
		err = errors.Join(err, run.cleanup())
		f.done <- recoveryOutcome{result: run.result(), err: err}
	}()
	return f
}
func takeRecoveryWrite(t *testing.T, conn *controlledV2Connection, kind wsprotocol.V2InputKind) *controlledV2Write {
	t.Helper()
	call := <-conn.writes
	var input wsprotocol.V2Input
	var err error
	if call.typ == websocket.MessageBinary {
		input, err = wsprotocol.DecodeV2Audio(call.data)
	} else {
		input, err = wsprotocol.DecodeV2Control(call.data)
	}
	if err != nil || input.Kind != kind {
		t.Fatalf("input=%+v err=%v", input, err)
	}
	return call
}
func takeRecoveryHandshake(t *testing.T, conn *controlledV2Connection) wsprotocol.V2Handshake {
	t.Helper()
	call := <-conn.writes
	request, err := wsprotocol.DecodeV2Handshake(call.data)
	if err != nil || request.Kind != wsprotocol.V2HandshakeResume {
		t.Fatalf("request=%+v err=%v", request, err)
	}
	call.reply <- nil
	return request
}
func (f *recoveryFixture) resume(t *testing.T, ready wsprotocol.ReadyMessage) *controlledV2Connection {
	t.Helper()
	request := <-f.dials
	conn := newControlledV2Connection()
	request.reply <- recoveryDialReply{conn: conn}
	takeRecoveryHandshake(t, conn)
	conn.send(ready)
	return conn
}
func resumedReady(generation, offset, seq uint64, ended bool) wsprotocol.ReadyMessage {
	return wsprotocol.ReadyMessage{Type: wsprotocol.MessageTypeReady, SessionID: "s", ResumeToken: "secret", Generation: generation, NextOffset: offset, AckedResultSeq: seq, InputEnded: ended}
}
func (f *recoveryFixture) finish(t *testing.T, want error) recoveryOutcome {
	t.Helper()
	out := <-f.done
	if want == nil && out.err != nil || want != nil && !errors.Is(out.err, want) {
		t.Fatalf("outcome=%+v want=%v", out, want)
	}
	return out
}

func TestV2ResumeCaptureDuringWait(t *testing.T) {
	for _, phase := range []string{"closing", "dialing", "handshake", "backoff"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				closeRelease := make(chan struct{})
				var closes atomic.Int32
				source := &trackedV2Source{reader: pr, close: pr.Close}
				f := newRecoveryFixture(t, V2Config{ChunkBytes: 2}, source, func(f *recoveryFixture) {
					if phase == "closing" {
						f.first.close = func() error { closes.Add(1); <-closeRelease; return nil }
					}
				})
				_, _ = pw.Write([]byte("aa"))
				write := takeRecoveryWrite(t, f.first, wsprotocol.V2InputAudio)
				write.reply <- io.ErrClosedPipe
				var dial recoveryDialRequest
				if phase != "closing" {
					dial = <-f.dials
				}
				if phase == "handshake" || phase == "backoff" {
					conn := newControlledV2Connection()
					dial.reply <- recoveryDialReply{conn: conn}
					takeRecoveryHandshake(t, conn)
					if phase == "backoff" {
						conn.send(wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: wsprotocol.V2ErrorSessionBusy})
					}
				}
				synctest.Wait()
				_, _ = pw.Write([]byte("bb"))
				_, _ = pw.Write([]byte("cc"))
				synctest.Wait()
				if f.run.state.audio.nextOffset != 6 || f.run.state.offeredOffset != 2 {
					t.Fatal("did not keep capturing during connection wait")
				}
				if source.closes.Load() != 0 {
					t.Fatal("source closed with old connection")
				}
				if phase == "closing" {
					select {
					case <-f.dials:
						t.Fatal("new Dial overlapped closing")
					default:
					}
					close(closeRelease)
				}
				f.run.cancel()
				f.finish(t, context.Canceled)
				v2CheckClosed(t, source)
				if phase == "closing" && closes.Load() != 1 {
					t.Fatal("duplicate socket cleanup")
				}
			})
		})
	}
}

func TestV2ResumeBufferFullWhileDisconnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		source := &trackedV2Source{reader: pr, close: pr.Close}
		f := newRecoveryFixture(t, V2Config{ChunkBytes: 2, MaxReplayBytes: 2, MaxReplayChunks: 1}, source, nil)
		_, _ = pw.Write([]byte("aa"))
		takeRecoveryWrite(t, f.first, wsprotocol.V2InputAudio).reply <- io.ErrClosedPipe
		<-f.dials // 阻塞拨号，不暂停源来等待空位。
		_, _ = pw.Write([]byte("bb"))
		out := f.finish(t, errReplayBufferFull)
		if out.result.Completed || f.run.state.audio.nextOffset != 2 {
			t.Fatal("buffer overflow concealed")
		}
		v2CheckClosed(t, source)
	})
}

func TestV2ResumePositionsAndDedup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		var applied []uint64
		f := newRecoveryFixture(t, V2Config{ChunkBytes: 2, OnResult: func(_ context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
			applied = append(applied, r.Seq)
			return nil
		}}, pr, nil)
		_, _ = pw.Write([]byte("aa"))
		audioWrite := takeRecoveryWrite(t, f.first, wsprotocol.V2InputAudio)
		f.first.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1})
		synctest.Wait()
		audioWrite.reply <- nil
		ack := takeRecoveryWrite(t, f.first, wsprotocol.V2InputResultAck)
		ack.reply <- io.ErrClosedPipe // 业务已应用，ACK 丢失。
		request := <-f.dials
		_, _ = pw.Write([]byte("bb"))
		_ = pw.Close()
		synctest.Wait()
		conn := newControlledV2Connection()
		request.reply <- recoveryDialReply{conn: conn}
		handshake := takeRecoveryHandshake(t, conn)
		if handshake.SessionID != "s" || handshake.ResumeToken != "secret" || handshake.AppliedSeq != 1 {
			t.Fatalf("handshake=%+v", handshake)
		}
		conn.send(resumedReady(3, 2, 1, false)) // 跳代，旧音频 ACK 丢失但 Gateway 已接纳。
		write := takeRecoveryWrite(t, conn, wsprotocol.V2InputAudio)
		input, _ := wsprotocol.DecodeV2Audio(write.data)
		if input.Offset != 2 || string(input.Payload) != "bb" {
			t.Fatalf("replay=%+v", input)
		}
		conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1}) // 重复不再应用。
		conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 2})
		synctest.Wait()
		write.reply <- nil
		end := takeRecoveryWrite(t, conn, wsprotocol.V2InputEnd)
		end.reply <- nil
		conn.send(wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 3, FinalOffset: 4, LastSeq: 2})
		// completed 可与 end 后的累计 result_ack 派发竞争，两者均需实际回执。
		for {
			call := <-conn.writes
			control, _ := wsprotocol.DecodeV2Control(call.data)
			call.reply <- nil
			if control.Kind == wsprotocol.V2InputCompletedAck {
				break
			}
		}
		out := f.finish(t, nil)
		if !out.result.Completed || !out.result.CompletionAckWritten || out.result.FinalOffset != 4 || out.result.ResumeAttempts != 1 || out.result.ResumedConnections != 1 || len(applied) != 2 || applied[0] != 1 || applied[1] != 2 {
			t.Fatalf("out=%+v applied=%v", out, applied)
		}
	})
}

func TestV2ResumeWindowAndAttemptLimits(t *testing.T) {
	t.Run("fixed_window", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{Window: time.Second, MaxAttempts: 100}}, pr, nil)
			began := time.Now()
			f.first.reads <- v2ReadEvent{err: io.EOF}
			for i := 0; i < 3; i++ {
				req := <-f.dials
				req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
			}
			req := <-f.dials
			_ = req // 第四次握手挂起，剩余固定窗口限制它。
			out := f.finish(t, ErrV2RecoveryExhausted)
			if !errors.Is(out.err, context.DeadlineExceeded) || time.Since(began) != time.Second || out.result.ResumeAttempts != 4 {
				t.Fatalf("out=%+v elapsed=%v", out, time.Since(began))
			}
		})
	})
	t.Run("total_attempts_survive_success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{MaxAttempts: 2}}, pr, nil)
			for generation := uint64(2); generation <= 3; generation++ {
				old := f.first
				if generation == 3 {
					old = f.run.current.conn.(*controlledV2Connection)
				}
				old.reads <- v2ReadEvent{err: io.EOF}
				f.resume(t, resumedReady(generation, 0, 0, false))
				synctest.Wait()
			}
			current := f.run.current.conn.(*controlledV2Connection)
			current.reads <- v2ReadEvent{err: io.EOF}
			out := f.finish(t, ErrV2RecoveryExhausted)
			if out.result.ResumeAttempts != 2 || out.result.ResumedConnections != 2 {
				t.Fatalf("out=%+v", out)
			}
			select {
			case <-f.dials:
				t.Fatal("third Dial exceeded budget")
			default:
			}
		})
	})
	t.Run("cancel_backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			f := newRecoveryFixture(t, V2Config{}, pr, nil)
			f.first.reads <- v2ReadEvent{err: io.EOF}
			req := <-f.dials
			req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
			synctest.Wait()
			f.run.cancel()
			f.finish(t, context.Canceled)
			select {
			case <-f.dials:
				t.Fatal("Dial started after cancel")
			default:
			}
		})
	})
}

func TestV2ResumeHandshakeRejections(t *testing.T) {
	for _, code := range []wsprotocol.V2ErrorCode{wsprotocol.V2ErrorSessionBusy, wsprotocol.V2ErrorHandshakeLimit, wsprotocol.V2ErrorEntryTimeout, wsprotocol.V2ErrorResumeUnavailable, wsprotocol.V2ErrorServiceStopping, "future"} {
		t.Run(string(code), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				f := newRecoveryFixture(t, V2Config{}, pr, nil)
				f.first.reads <- v2ReadEvent{err: io.EOF}
				req := <-f.dials
				conn := newControlledV2Connection()
				req.reply <- recoveryDialReply{conn: conn}
				takeRecoveryHandshake(t, conn)
				conn.send(wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: code})
				if code == wsprotocol.V2ErrorSessionBusy || code == wsprotocol.V2ErrorHandshakeLimit || code == wsprotocol.V2ErrorEntryTimeout {
					next := <-f.dials
					if next.ctx.Err() != nil {
						t.Fatal("retry context canceled")
					}
					f.run.cancel()
					f.finish(t, context.Canceled)
				} else {
					out := <-f.done
					var entry *V2EntryError
					if !errors.As(out.err, &entry) || entry.Code != code || out.result.ResumeAttempts != 1 {
						t.Fatalf("out=%+v", out)
					}
					select {
					case <-f.dials:
						t.Fatal("terminal rejection retried")
					default:
					}
				}
			})
		})
	}
}

func TestV2ResumeCompletionDeadlinePersists(t *testing.T) {
	for _, ended := range []bool{false, true} {
		name := "resend_end"
		if ended {
			name = "end_already_accepted"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newRecoveryFixture(t, V2Config{CompletionTimeout: time.Second}, io.NopCloser(bytes.NewReader(nil)), nil)
				end := takeRecoveryWrite(t, f.first, wsprotocol.V2InputEnd)
				began := time.Now()
				end.reply <- nil
				time.Sleep(700 * time.Millisecond)
				f.first.reads <- v2ReadEvent{err: io.EOF}
				conn := f.resume(t, resumedReady(2, 0, 0, ended))
				if !ended {
					takeRecoveryWrite(t, conn, wsprotocol.V2InputEnd).reply <- nil
				}
				out := f.finish(t, context.DeadlineExceeded)
				if out.result.Completed || out.result.ResumedConnections != 1 || time.Since(began) != time.Second {
					t.Fatalf("out=%+v elapsed=%v", out, time.Since(began))
				}
				if ended {
					select {
					case <-conn.writes:
						t.Fatal("end duplicated after trusted acceptance")
					default:
					}
				}
			})
		})
	}
}

func TestV2ResumeTerminalFailures(t *testing.T) {
	for _, mode := range []string{"protocol", "oversized", "callback_error", "callback_timeout", "completed_ack_error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := V2Config{ApplyTimeout: time.Second}
				failure := errors.New("application failed")
				if mode == "callback_error" {
					cfg.OnResult = func(context.Context, wsprotocol.SequencedResultMessage, time.Time) error { return failure }
				}
				if mode == "callback_timeout" {
					cfg.OnResult = func(ctx context.Context, _ wsprotocol.SequencedResultMessage, _ time.Time) error {
						<-ctx.Done()
						return nil
					}
				}
				var source io.ReadCloser
				if mode == "completed_ack_error" {
					source = io.NopCloser(bytes.NewReader(nil))
				} else {
					pr, pw := io.Pipe()
					defer pw.Close()
					source = pr
				}
				f := newRecoveryFixture(t, cfg, source, nil)
				var want error
				switch mode {
				case "protocol":
					want = errV2ClientProtocol
					f.first.reads <- v2ReadEvent{data: []byte(`{"type":"future"}`)}
				case "oversized":
					want = websocket.ErrMessageTooBig
					f.first.reads <- v2ReadEvent{err: want}
				case "callback_error":
					want = failure
					f.first.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1})
				case "callback_timeout":
					want = context.DeadlineExceeded
					f.first.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1})
				case "completed_ack_error":
					want = io.ErrClosedPipe
					takeRecoveryWrite(t, f.first, wsprotocol.V2InputEnd).reply <- nil
					f.first.send(wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1})
					takeRecoveryWrite(t, f.first, wsprotocol.V2InputCompletedAck).reply <- want
				}
				out := f.finish(t, want)
				if out.result.ResumeAttempts != 0 || out.result.Completed != (mode == "completed_ack_error") || out.result.AppliedSeq != 0 {
					t.Fatalf("out=%+v", out)
				}
				select {
				case <-f.dials:
					t.Fatal("terminal failure retried")
				default:
				}
			})
		})
	}
}

func TestV2ResumeCandidateOwnership(t *testing.T) {
	for _, mode := range []string{"cancel", "expired_ready", "invalid_ready", "cleanup_error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				var closes atomic.Int32
				f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{Window: time.Second}}, pr, nil)
				f.first.reads <- v2ReadEvent{err: io.EOF}
				req := <-f.dials
				conn := newControlledV2Connection()
				cleanupErr := errors.New("close failure")
				conn.close = func() error {
					closes.Add(1)
					if mode == "cleanup_error" {
						return cleanupErr
					}
					return nil
				}
				req.reply <- recoveryDialReply{conn: conn}
				takeRecoveryHandshake(t, conn)
				var want error
				switch mode {
				case "cancel":
					want = context.Canceled
					conn.send(resumedReady(2, 0, 0, false))
					f.run.cancel()
				case "expired_ready":
					want = ErrV2RecoveryExhausted
					go func() { time.Sleep(time.Second); conn.send(resumedReady(2, 0, 0, false)) }()
				case "invalid_ready":
					want = errV2ClientProtocol
					conn.send(resumedReady(2, 2, 0, false))
				case "cleanup_error":
					want = cleanupErr
					conn.send(wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: wsprotocol.V2ErrorSessionBusy})
				}
				f.finish(t, want)
				if closes.Load() != 1 {
					t.Fatalf("candidate Close=%d", closes.Load())
				}
				if mode == "expired_ready" && f.run.state.generation != 1 {
					t.Fatal("expired ready accepted")
				}
			})
		})
	}
}

func TestV2ResumeHTTPErrors(t *testing.T) {
	for _, status := range []int{503, 403} {
		name := http.StatusText(status)
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				f := newRecoveryFixture(t, V2Config{}, pr, nil)
				f.first.reads <- v2ReadEvent{err: io.EOF}
				req := <-f.dials
				req.reply <- recoveryDialReply{response: &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader([]byte("proxy failure")))}, err: io.ErrClosedPipe}
				if status == 503 {
					<-f.dials
					f.run.cancel()
					f.finish(t, context.Canceled)
				} else {
					f.finish(t, io.ErrClosedPipe)
					select {
					case <-f.dials:
						t.Fatal("forbidden retried")
					default:
					}
				}
			})
		})
	}
}

// 完整初始握手仍没有可信身份时，即使显式开启恢复也不能再 start。
func TestV2ResumeNoIdentityNoRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: &V2ResumeConfig{}})
		source := staticV2Source(nil)
		r, err := c.newV2Run(context.Background(), source)
		if err != nil {
			t.Fatal(err)
		}
		var dials atomic.Int32
		r.dial = func(context.Context, string) (v2ManagedConnection, *http.Response, error) {
			dials.Add(1)
			return nil, nil, io.EOF
		}
		r.startHandshake(false)
		err = r.coordinate()
		err = errors.Join(err, r.cleanup())
		if !errors.Is(err, io.EOF) || dials.Load() != 1 || r.result().ResumeAttempts != 0 || source.reads.Load() != 0 {
			t.Fatalf("err=%v dials=%d result=%+v", err, dials.Load(), r.result())
		}
		v2CheckClosed(t, source)
	})
}

func TestV2ResumeOldEventsAreIsolated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		release := make(chan struct{})
		f := newRecoveryFixture(t, V2Config{}, pr, func(f *recoveryFixture) { f.first.close = func() error { <-release; return nil } })
		f.first.reads <- v2ReadEvent{err: io.EOF}
		synctest.Wait()
		old := f.run.current
		old.readEvents <- v2ReadEvent{data: []byte(`{"type":"result","seq":"1"}`)}
		close(release)
		conn := f.resume(t, resumedReady(2, 0, 0, false))
		synctest.Wait()
		if f.run.state.generation != 2 || f.run.state.appliedSeq != 0 {
			t.Fatal("old queued event modified new generation")
		}
		conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1})
		ack := takeRecoveryWrite(t, conn, wsprotocol.V2InputResultAck)
		ack.reply <- nil
		f.run.cancel()
		out := f.finish(t, context.Canceled)
		if out.result.AppliedSeq != 1 {
			t.Fatal("new generation result was not applied")
		}
	})
}

func TestV2ResumeCleanupErrorStopsNextDial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		failure := errors.New("socket cleanup failed")
		f := newRecoveryFixture(t, V2Config{}, pr, func(f *recoveryFixture) { f.first.close = func() error { return failure } })
		f.first.reads <- v2ReadEvent{err: io.EOF}
		out := f.finish(t, failure)
		if out.result.ResumeAttempts != 0 {
			t.Fatal("Dial proceeded despite cleanup failure")
		}
		select {
		case <-f.dials:
			t.Fatal("new Dial started")
		default:
		}
	})
}
