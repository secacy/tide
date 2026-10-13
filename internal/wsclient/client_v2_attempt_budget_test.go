package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

func TestV2AttemptWindowConfig(t *testing.T) {
	if _, err := NewV2(V2Config{URL: "ws://test", Resume: &V2ResumeConfig{AttemptWindow: -time.Nanosecond}}); err == nil {
		t.Fatal("negative attempt window accepted")
	}
	cfg := &V2ResumeConfig{AttemptWindow: time.Minute}
	c := v2TestClient(t, V2Config{Resume: cfg, URL: "ws://test"})
	cfg.AttemptWindow = 0
	if c.cfg.Resume.AttemptWindow != time.Minute || c.cfg.Resume.MaxAttempts != 5 || c.cfg.Resume.Window != 5*time.Second {
		t.Fatalf("copy/defaults=%+v", c.cfg.Resume)
	}
	data, err := json.Marshal(c.cfg.Resume)
	if err != nil || !strings.Contains(string(data), `"AttemptWindow":60000000000`) {
		t.Fatalf("config JSON=%s err=%v", data, err)
	}
	legacy := v2TestClient(t, V2Config{URL: "ws://test", Resume: &V2ResumeConfig{}})
	data, err = json.Marshal(legacy.cfg.Resume)
	if err != nil || string(data) != `{"MaxAttempts":5,"Window":5000000000,"InitialBackoff":100000000,"MaxBackoff":1000000000}` {
		t.Fatalf("legacy config changed: %s err=%v", data, err)
	}
}

// 精确边界单独用单调时钟验证；协调者测试另外核实实际 Dial 和清理。
func TestV2AttemptWindowPartialExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: &V2ResumeConfig{MaxAttempts: 2, AttemptWindow: time.Second}})
		r, err := c.newV2Run(context.Background(), io.NopCloser(strings.NewReader("")))
		if err != nil {
			t.Fatal(err)
		}
		defer r.cleanup()
		t0 := time.Now()
		r.recordResumeAttempt(t0)
		time.Sleep(400 * time.Millisecond)
		t1 := time.Now()
		r.recordResumeAttempt(t1)
		time.Sleep(600*time.Millisecond - time.Nanosecond)
		if r.resumeAttemptAllowed(time.Now()) {
			t.Fatal("record expired one nanosecond early")
		}
		time.Sleep(time.Nanosecond)
		if !r.resumeAttemptAllowed(time.Now()) || len(r.recovery.attemptTimes) != 1 || !r.recovery.attemptTimes[0].Equal(t1) {
			t.Fatalf("partial boundary expiry=%v", r.recovery.attemptTimes)
		}
		tail := r.recovery.attemptTimes[:cap(r.recovery.attemptTimes)]
		for _, stamp := range tail[1:] {
			if !stamp.IsZero() {
				t.Fatal("expired tail retained time reference")
			}
		}
		r.recordResumeAttempt(time.Now())
		if r.resumeAttemptAllowed(time.Now()) || r.result().ResumeAttempts != 3 {
			t.Fatal("new attempt/history lost")
		}
		time.Sleep(400 * time.Millisecond)
		if !r.resumeAttemptAllowed(time.Now()) || len(r.recovery.attemptTimes) != 1 {
			t.Fatal("second record did not expire independently")
		}
	})
}

// 只有连接替身：经过真实协调者、采集、握手任务及 cleanup；不测网络性能。
func TestV2AttemptWindowRecoveries(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		gaps      []time.Duration
		exhausted bool
	}{
		{"spaced_exceeds_lifetime_limit", []time.Duration{0, time.Second, time.Second, time.Second, time.Second, time.Second}, false},
		{"dense_success_does_not_refill", []time.Duration{0, 0}, true},
		{"only_oldest_expires", []time.Duration{0, 400 * time.Millisecond, 600 * time.Millisecond}, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				source := &trackedV2Source{reader: pr, close: pr.Close}
				f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{MaxAttempts: 2, AttemptWindow: time.Second}}, source, nil)
				conn := f.first
				for i, gap := range scenario.gaps {
					time.Sleep(gap)
					conn.reads <- v2ReadEvent{err: io.EOF}
					conn = f.resume(t, resumedReady(uint64(i+2), 0, 0, false))
					synctest.Wait()
					if f.run.recovery.attempts != i+1 || len(f.run.recovery.attemptTimes) > 2 {
						t.Fatal("history lost or recent storage exceeded limit")
					}
				}
				// 用尽额度后仍可保持正常 streaming，不能在 checkBudget 中立即终止。
				select {
				case out := <-f.done:
					t.Fatalf("terminated healthy stream: %+v", out)
				default:
				}
				if scenario.exhausted {
					conn.reads <- v2ReadEvent{err: io.ErrUnexpectedEOF}
					began := time.Now()
					out := f.finish(t, ErrV2RecoveryExhausted)
					if !errors.Is(out.err, io.ErrUnexpectedEOF) || errors.Is(out.err, context.DeadlineExceeded) || time.Since(began) != 0 || out.result.ResumeAttempts != len(scenario.gaps) {
						t.Fatalf("out=%+v", out)
					}
				} else {
					pw.Close()
					takeRecoveryWrite(t, conn, wsprotocol.V2InputEnd).reply <- nil
					conn.send(wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 7, FinalOffset: 0, LastSeq: 0})
					takeRecoveryWrite(t, conn, wsprotocol.V2InputCompletedAck).reply <- nil
					out := f.finish(t, nil)
					if !out.result.Completed || !out.result.CompletionAckWritten || out.result.ResumeAttempts != 6 || out.result.ResumedConnections != 6 {
						t.Fatalf("out=%+v", out)
					}
				}
				select {
				case <-f.dials:
					t.Fatal("Dial exceeded allowance")
				default:
				}
				v2CheckClosed(t, source)
			})
		})
	}
}

func TestV2AttemptWindowFailedHandshakesConsume(t *testing.T) {
	for _, kind := range []string{"dial", "entry"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				source := &trackedV2Source{reader: pr, close: pr.Close}
				f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{MaxAttempts: 2, AttemptWindow: time.Minute}}, source, nil)
				f.first.reads <- v2ReadEvent{err: io.EOF}
				req := <-f.dials
				if kind == "dial" {
					req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
				} else {
					rejected := newControlledV2Connection()
					req.reply <- recoveryDialReply{conn: rejected}
					takeRecoveryHandshake(t, rejected)
					rejected.send(wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: wsprotocol.V2ErrorSessionBusy})
				}
				conn := f.resume(t, resumedReady(2, 0, 0, false))
				synctest.Wait()
				conn.reads <- v2ReadEvent{err: io.EOF}
				out := f.finish(t, ErrV2RecoveryExhausted)
				if out.result.ResumeAttempts != 2 || out.result.ResumedConnections != 1 {
					t.Fatalf("out=%+v", out)
				}
				select {
				case <-f.dials:
					t.Fatal("failed handshake did not consume allowance")
				default:
				}
				v2CheckClosed(t, source)
			})
		})
	}
}

func TestV2AttemptWindowDoesNotExtendDeadlines(t *testing.T) {
	t.Run("failed_attempts_expire_but_outage_deadline_fixed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			source := &trackedV2Source{reader: pr, close: pr.Close}
			f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{MaxAttempts: 1, AttemptWindow: 50 * time.Millisecond, Window: time.Second}}, source, nil)
			began := time.Now()
			f.first.reads <- v2ReadEvent{err: io.EOF}
			// 每次先让近期次数过期；允许继续尝试，但本轮 deadline 不能跟随续期。
			for i := 0; i < 3; i++ {
				req := <-f.dials
				time.Sleep(60 * time.Millisecond)
				req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
			}
			<-f.dials // 第四次 Dial 挂起，由原固定恢复期限取消。
			out := f.finish(t, ErrV2RecoveryExhausted)
			if !errors.Is(out.err, context.DeadlineExceeded) || time.Since(began) != time.Second || out.result.ResumeAttempts != 4 {
				t.Fatalf("out=%+v elapsed=%v", out, time.Since(began))
			}
			v2CheckClosed(t, source)
		})
	})
	t.Run("ready_after_outage_deadline_rejected", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{AttemptWindow: time.Minute, Window: time.Second}}, pr, nil)
			f.first.reads <- v2ReadEvent{err: io.EOF}
			req := <-f.dials
			conn := newControlledV2Connection()
			req.reply <- recoveryDialReply{conn: conn}
			takeRecoveryHandshake(t, conn)
			go func() { time.Sleep(time.Second); conn.send(resumedReady(2, 0, 0, false)) }()
			out := f.finish(t, ErrV2RecoveryExhausted)
			if !errors.Is(out.err, context.DeadlineExceeded) || out.result.ResumedConnections != 0 || f.run.state.generation != 1 {
				t.Fatalf("late ready=%+v", out)
			}
		})
	})
	t.Run("completion_deadline_survives_new_ready", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newRecoveryFixture(t, V2Config{CompletionTimeout: time.Second, Resume: &V2ResumeConfig{AttemptWindow: time.Minute}}, io.NopCloser(strings.NewReader("")), nil)
			end := takeRecoveryWrite(t, f.first, wsprotocol.V2InputEnd)
			began := time.Now()
			end.reply <- nil
			time.Sleep(700 * time.Millisecond)
			f.first.reads <- v2ReadEvent{err: io.EOF}
			f.resume(t, resumedReady(2, 0, 0, true))
			out := f.finish(t, context.DeadlineExceeded)
			if out.result.Completed || out.result.ResumedConnections != 1 || time.Since(began) != time.Second {
				t.Fatalf("out=%+v elapsed=%v", out, time.Since(began))
			}
		})
	})
	t.Run("cancel_during_backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			source := &trackedV2Source{reader: pr, close: pr.Close}
			f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{AttemptWindow: time.Minute}}, source, nil)
			f.first.reads <- v2ReadEvent{err: io.EOF}
			req := <-f.dials
			req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
			synctest.Wait()
			f.run.cancel()
			out := f.finish(t, context.Canceled)
			if out.result.ResumeAttempts != 1 {
				t.Fatalf("out=%+v", out)
			}
			select {
			case <-f.dials:
				t.Fatal("Dial after cancellation")
			default:
			}
			v2CheckClosed(t, source)
		})
	})
}
