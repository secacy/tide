package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

type writerTestConn func(context.Context, websocket.MessageType, []byte) error

func (fn writerTestConn) Write(ctx context.Context, typ websocket.MessageType, data []byte) error {
	return fn(ctx, typ, data)
}

func newTestGenerationWriter(t *testing.T, f *workerCoordinatorFixture, generation uint64, conn resultWriteConn, timeout time.Duration) *resultWriter {
	t.Helper()
	w, err := newResultWriter(resultWriterConfig{session: f.session, generation: generation, conn: conn, writeTimeout: timeout, controlCtx: f.lifeCtx})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// resultWriterRunFixture 在 finished 之后读取 exit；清理等待实际 run 返回。
type resultWriterRunFixture struct {
	finished chan struct{}
	exit     resultWriterExit
	cancel   context.CancelCauseFunc
}

func startTestGenerationWriter(t *testing.T, w *resultWriter) *resultWriterRunFixture {
	t.Helper()
	ctx, cancel := context.WithCancelCause(w.config.controlCtx)
	return startTestGenerationWriterContext(t, w, ctx, cancel)
}

func startTestGenerationWriterContext(t *testing.T, w *resultWriter, ctx context.Context, cancel context.CancelCauseFunc) *resultWriterRunFixture {
	t.Helper()
	f := &resultWriterRunFixture{finished: make(chan struct{}), cancel: cancel}
	go func() {
		f.exit = w.run(ctx)
		close(f.finished)
	}()
	t.Cleanup(func() { cancel(nil); <-f.finished })
	return f
}

func (f *resultWriterRunFixture) assertExit(t *testing.T, kind resultWriterExitKind, seq uint64, want error) {
	t.Helper()
	synctest.Wait()
	select {
	case <-f.finished:
	default:
		t.Fatal("writer has not returned at the expected stage")
	}
	if f.exit.kind != kind || f.exit.lastSeq != seq || !errors.Is(f.exit.err, want) {
		t.Fatalf("writer exit = %+v, want kind=%d seq=%d err=%v", f.exit, kind, seq, want)
	}
}

func decodeWriterMessage(t *testing.T, typ websocket.MessageType, data []byte) wsprotocol.SequencedResultMessage {
	t.Helper()
	if typ != websocket.MessageText {
		t.Errorf("message type = %v, want text", typ)
	}
	var msg wsprotocol.SequencedResultMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Errorf("decode message: %v", err)
	}
	if msg.Type != wsprotocol.MessageTypeResult || msg.Seq == 0 {
		t.Errorf("invalid result message: %+v", msg)
	}
	return msg
}

func TestResultWriterTaskConstruction(t *testing.T) {
	f := newWorkerCoordinatorFixture(t, 8, 2, func(context.Context) workerStream { return &uploadTestStream{} })
	var writes atomic.Int32
	config := resultWriterConfig{session: f.session, generation: 1, controlCtx: f.lifeCtx, writeTimeout: time.Second,
		conn: writerTestConn(func(context.Context, websocket.MessageType, []byte) error { writes.Add(1); return nil })}
	for _, name := range []string{"nil_session", "zero_generation", "nil_conn", "nil_control", "zero_timeout", "negative_timeout", "valid_without_start"} {
		t.Run(name, func(t *testing.T) {
			cfg := config
			switch name {
			case "nil_session":
				cfg.session = nil
			case "zero_generation":
				cfg.generation = 0
			case "nil_conn":
				cfg.conn = nil
			case "nil_control":
				cfg.controlCtx = nil
			case "zero_timeout":
				cfg.writeTimeout = 0
			case "negative_timeout":
				cfg.writeTimeout = -time.Second
			}
			w, err := newResultWriter(cfg)
			if name == "valid_without_start" {
				if err != nil || w == nil || w.config.session != f.session || w.config.generation != 1 {
					t.Fatalf("valid construction = (%v,%v)", w, err)
				}
			} else if w != nil || !errors.Is(err, errInvalidResultWriterConfig) {
				t.Fatalf("invalid construction = (%v,%v)", w, err)
			}
			if writes.Load() != 0 || context.Cause(f.lifeCtx) != nil || context.Cause(f.rpcCtx) != nil {
				t.Fatal("construction performed I/O or took cancellation ownership")
			}
		})
	}
	defer func() {
		if recover() == nil {
			t.Error("nil run context did not panic")
		}
	}()
	w, _ := newResultWriter(config)
	w.run(nil)
}

func TestResultWriterTaskWaitsAndWritesTailInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		messages := make(chan wsprotocol.SequencedResultMessage, 8)
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(_ context.Context, typ websocket.MessageType, data []byte) error {
			messages <- decodeWriterMessage(t, typ, data)
			return nil
		}), time.Second)
		run := startTestGenerationWriter(t, w)
		synctest.Wait()
		select {
		case <-run.finished:
			t.Fatal("empty running stream ended writer")
		default:
		}
		if len(messages) != 0 {
			t.Fatal("writer fabricated a result")
		}
		feedDeliveryResult(t, feed, "partial")
		first := <-messages
		if first.Seq != 1 || first.Text != "partial" {
			t.Fatal(first)
		}
		time.Sleep(2 * time.Second) // 每次 Write 有独立期限，不限制空闲等待总时长。
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		feedDeliveryResult(t, feed, "tail")
		second := <-messages
		if second.Seq != 2 || second.Text != "tail" {
			t.Fatal(second)
		}
		feed <- workerReadStep{err: io.EOF}
		run.assertExit(t, writerResultsComplete, 2, nil)
		assertWorkerRetaining(t, f)
		// 写完尚未 ACK：原结果还在，detach/resume 从第一条重放。
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(err)
		}
		if g, err := f.session.requestResume(context.Background(), 0); g != 2 || err != nil {
			t.Fatal(g, err)
		}
		if offer := takeDeliveryResult(t, f.session, 2, 1); offer.result.text != "partial" {
			t.Fatal(offer)
		}
		closeDeliveryFixture(t, f)
	})
}

func TestResultWriterTaskEmptyCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(context.Context, websocket.MessageType, []byte) error {
			t.Error("empty stream performed Write")
			return nil
		}), time.Second)
		run := startTestGenerationWriter(t, w)
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		run.assertExit(t, writerResultsComplete, 0, nil)
		assertWorkerRetaining(t, f)
		closeDeliveryFixture(t, f)
	})
}

func TestResultWriterTaskBlockedWriteAllowsAckAndControl(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		entered, release := make(chan uint64, 2), make(chan struct{})
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, typ websocket.MessageType, data []byte) error {
			seq := decodeWriterMessage(t, typ, data).Seq
			entered <- seq
			if seq == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			}
			return nil
		}), time.Minute)
		run := startTestGenerationWriter(t, w)
		feedDeliveryResult(t, feed, "one")
		if seq := <-entered; seq != 1 {
			t.Fatal(seq)
		}
		feedDeliveryResult(t, feed, "two")
		ackDeliveryResult(t, f.session, 1, 1, true)
		if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResultWriteInFlight) {
			t.Fatal(err)
		}
		if len(entered) != 0 {
			t.Fatal("second Write started before first returned")
		}
		close(release)
		synctest.Wait()
		if seq := <-entered; seq != 2 {
			t.Fatal(seq)
		}
		ackDeliveryResult(t, f.session, 1, 2, true)
		run.cancel(nil)
		run.assertExit(t, writerStopped, 0, context.Canceled)
		closeDeliveryFixture(t, f)
	})
}

func TestResultWriterTaskWriteFailuresKeepAuthorization(t *testing.T) {
	for _, name := range []string{"transport", "deadline", "connection_cancel", "logical_cancel"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				cause := errors.New("injected write failure")
				entered := make(chan struct{})
				var calls atomic.Int32
				w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, _ websocket.MessageType, _ []byte) error {
					calls.Add(1)
					close(entered)
					if name == "transport" {
						return cause
					}
					<-ctx.Done()
					return context.Cause(ctx)
				}), time.Second)
				run := startTestGenerationWriter(t, w)
				feedDeliveryResult(t, feed, "one")
				<-entered
				kind, want := writerWriteFailed, cause
				switch name {
				case "deadline":
					time.Sleep(time.Second)
					want = ErrResultWriteTimeout
				case "connection_cancel":
					run.cancel(cause)
					kind = writerStopped
				case "logical_cancel":
					f.cancelLife(cause)
					kind = writerStopped
				}
				run.assertExit(t, kind, 0, want)
				if calls.Load() != 1 {
					t.Fatal("writer retried failed Write")
				}
				if name == "logical_cancel" {
					f.assertFinishedAtCurrentTime(t, cause)
					return
				}
				if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResultWriteInFlight) {
					t.Fatalf("failed Write cleared authorization: %v", err)
				}
				if context.Cause(f.rpcCtx) != nil {
					t.Fatal("writer canceled Worker")
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestResultWriterTaskWaitStops(t *testing.T) {
	for _, name := range []string{"connection", "logical", "control", "detached", "old_generation", "no_worker"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				if name == "no_worker" {
					startTestSessionControl(t, f.session, time.Now)
				} else {
					f.start()
				}
				generation := uint64(1)
				if name == "old_generation" {
					generation = 2
				}
				w := newTestGenerationWriter(t, f, generation, writerTestConn(func(context.Context, websocket.MessageType, []byte) error {
					t.Error("unexpected Write")
					return nil
				}), time.Second)
				run := startTestGenerationWriter(t, w)
				synctest.Wait()
				kind, want := writerStopped, errors.New("owner stopped writer")
				switch name {
				case "connection":
					run.cancel(want)
				case "logical":
					f.cancelLife(want)
				case "control":
					if err := f.session.requestClose(context.Background()); err != nil {
						t.Fatal(err)
					}
					want = errResumeClosed
				case "detached":
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatal(err)
					}
					want = errSessionNotAttached
				case "old_generation":
					want = errSessionGenerationMismatch
				case "no_worker":
					kind, want = writerControlFailed, errSessionWorkerUnavailable
				}
				run.assertExit(t, kind, 0, want)
				if name == "logical" {
					f.assertFinishedAtCurrentTime(t, want)
				} else if name != "no_worker" && name != "control" {
					closeDeliveryFixture(t, f)
				}
			})
		})
	}
}

func TestResultWriterTaskCompetingWriterIsControlFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "one")
		takeDeliveryResult(t, f.session, 1, 1)
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(context.Context, websocket.MessageType, []byte) error {
			t.Error("competing writer performed Write")
			return nil
		}), time.Second)
		run := startTestGenerationWriter(t, w)
		run.assertExit(t, writerControlFailed, 0, errResultWriteInFlight)
		writeDeliveryResult(t, f.session, 1, 1)
		closeDeliveryFixture(t, f)
	})
}

func TestResultWriterTaskLateSuccessCannotClearNewGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		entered, release := make(chan struct{}), make(chan struct{})
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, _ websocket.MessageType, _ []byte) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}), time.Minute)
		run := startTestGenerationWriter(t, w)
		feedDeliveryResult(t, feed, "one")
		<-entered
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(err)
		}
		if g, err := f.session.requestResume(context.Background(), 0); g != 2 || err != nil {
			t.Fatal(g, err)
		}
		takeDeliveryResult(t, f.session, 2, 1)
		close(release)
		run.assertExit(t, writerStopped, 0, errResultWriterSuperseded)
		if _, err := f.session.requestResult(context.Background(), 2); !errors.Is(err, errResultWriteInFlight) {
			t.Fatal("old writer cleared new authorization", err)
		}
		writeDeliveryResult(t, f.session, 2, 1)
		closeDeliveryFixture(t, f)
	})
}

func TestResultWriterTaskCancellationWaitsForActualWriteReturn(t *testing.T) {
	for _, name := range []string{"connection", "deadline"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, _ websocket.MessageType, _ []byte) error {
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-release
					return nil
				}), time.Second)
				run := startTestGenerationWriter(t, w)
				feedDeliveryResult(t, feed, "one")
				<-entered
				kind, want := writerWriteFailed, ErrResultWriteTimeout
				if name == "connection" {
					kind, want = writerStopped, context.Canceled
					run.cancel(nil)
				} else {
					time.Sleep(time.Second)
				}
				<-canceled
				synctest.Wait()
				select {
				case <-run.finished:
					t.Fatal("writer reported exit while actual Write still held data")
				default:
				}
				unblock()
				run.assertExit(t, kind, 0, want)
				if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResultWriteInFlight) {
					t.Fatal("late nil Write was reported as success", err)
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestResultWriterTaskWriteMessageDeadlineAndErrorIdentity(t *testing.T) {
	for _, name := range []string{"fresh_deadline", "precanceled", "parent_deadline", "transport_deadline_identity"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				ctx, cancel := context.WithCancelCause(f.lifeCtx)
				defer cancel(nil)
				cause := errors.New("caller cancellation")
				if name == "precanceled" {
					cancel(cause)
				}
				if name == "parent_deadline" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(f.lifeCtx, time.Millisecond)
					defer stop()
				}
				var writes int
				var observed []context.Context
				w := newTestGenerationWriter(t, f, 1, writerTestConn(func(writeCtx context.Context, _ websocket.MessageType, _ []byte) error {
					writes++
					observed = append(observed, writeCtx)
					if name == "parent_deadline" {
						<-writeCtx.Done()
						return context.Cause(writeCtx)
					}
					if name == "transport_deadline_identity" {
						return context.DeadlineExceeded
					}
					return nil
				}), time.Second)
				err := w.writeMessage(ctx, []byte("{}"))
				switch name {
				case "precanceled":
					if !errors.Is(err, cause) || writes != 0 {
						t.Fatal(err, writes)
					}
				case "parent_deadline", "transport_deadline_identity":
					if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrResultWriteTimeout) {
						t.Fatal("wrong deadline source", err)
					}
				case "fresh_deadline":
					if err != nil {
						t.Fatal(err)
					}
					time.Sleep(2 * time.Second)
					if err := w.writeMessage(ctx, []byte("{}")); err != nil {
						t.Fatal(err)
					}
					if len(observed) != 2 || observed[0] == observed[1] {
						t.Fatal("Write contexts reused")
					}
					for _, saved := range observed {
						if !errors.Is(saved.Err(), context.Canceled) {
							t.Fatal("write timer context not released")
						}
					}
				}
			})
		})
	}
}

// writerIdleGateContext 暂停空查询之后等待 select 的求值，让多个停止源在选择前就绪。
// 第一次 Done 是 submitCommand，第二次是空结果等待；不改变真实 context 的语义。
type writerIdleGateContext struct {
	context.Context
	entered, release chan struct{}
	calls            atomic.Int32
}

func (c *writerIdleGateContext) Done() <-chan struct{} {
	if c.calls.Add(1) == 2 {
		close(c.entered)
		<-c.release
	}
	return c.Context.Done()
}

func TestResultWriterTaskIdleStopPriority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 对同时就绪的 select 重复检查；不同唤醒路径必须都服从共同优先级。
		for round := range 32 {
			f, _ := newDuplexWorkerFixture(t, nil, nil)
			f.start()
			connCtx, cancelConn := context.WithCancelCause(f.lifeCtx)
			ctx := &writerIdleGateContext{Context: connCtx, entered: make(chan struct{}), release: make(chan struct{})}
			w := newTestGenerationWriter(t, f, 1, writerTestConn(func(context.Context, websocket.MessageType, []byte) error { t.Error("unexpected Write"); return nil }), time.Second)
			run := startTestGenerationWriterContext(t, w, ctx, cancelConn)
			<-ctx.entered
			connCause, lifeCause := errors.New("connection lost first"), errors.New("logical session shutdown")
			cancelConn(connCause) // 子 context 保留自己的较早原因。
			f.cancelLife(lifeCause)
			synctest.Wait()
			close(ctx.release)
			synctest.Wait()
			select {
			case <-run.finished:
			default:
				t.Fatal("idle writer did not stop")
			}
			if run.exit.kind != writerStopped || !errors.Is(run.exit.err, lifeCause) {
				t.Errorf("round %d returned selected channel's cause instead of logical priority: kind=%d err=%v", round, run.exit.kind, run.exit.err)
			}
		}
	})
}

func TestResultWriterTaskSuccessReportSurvivesConnectionCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		connCtx, cancelConn := context.WithCancelCause(f.lifeCtx)
		defer cancelConn(nil)
		cause := errors.New("connection canceled after successful Write")
		var once sync.Once
		var writeReturned atomic.Bool
		// 成功报告提交时才取消连接，精确落在 writeMessage 已返回 nil 的窗口。
		controlCtx := &controlCheckedContext{Context: f.lifeCtx, check: func() error {
			err := f.lifeCtx.Err()
			if writeReturned.Load() {
				once.Do(func() { cancelConn(cause) })
			}
			return err
		}}
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(context.Context, websocket.MessageType, []byte) error {
			writeReturned.Store(true)
			return nil
		}), time.Second)
		w.config.controlCtx = controlCtx // 启动前配置测试同步包装，基础生命周期仍为 f.lifeCtx。
		run := startTestGenerationWriterContext(t, w, connCtx, cancelConn)
		feedDeliveryResult(t, feed, "one")
		run.assertExit(t, writerStopped, 0, cause)
		offer, err := f.session.requestResult(context.Background(), 1)
		if err != nil || offer.available || offer.lastSeq != 1 || offer.ackedSeq != 0 {
			t.Fatalf("successful Write was not reported with independent control context: (%+v,%v)", offer, err)
		}
		closeDeliveryFixture(t, f)
	})
}

func TestResultWriterTaskCancelAroundAuthorization(t *testing.T) {
	for _, name := range []string{"before_run", "after_request_check"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				feedDeliveryResult(t, feed, "one")
				connCtx, cancelConn := context.WithCancelCause(f.lifeCtx)
				defer cancelConn(nil)
				cause := errors.New("authorization caller canceled")
				var checks atomic.Int32
				var ctx context.Context = connCtx
				if name == "before_run" {
					cancelConn(cause)
				} else {
					ctx = &controlCheckedContext{Context: connCtx, check: func() error {
						err := connCtx.Err()
						// Go 1.26 的 Cause 会先调用 Err：停止检查、交付前检查、协调者检查。
						if checks.Add(1) == 3 {
							cancelConn(cause)
						}
						return err
					}}
				}
				w := newTestGenerationWriter(t, f, 1, writerTestConn(func(context.Context, websocket.MessageType, []byte) error {
					t.Error("Write started after observed connection cancel")
					return nil
				}), time.Second)
				run := startTestGenerationWriterContext(t, w, ctx, cancelConn)
				run.assertExit(t, writerStopped, 0, cause)
				if name == "before_run" {
					takeDeliveryResult(t, f.session, 1, 1)
				} else {
					if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResultWriteInFlight) {
						t.Fatal("committed authorization silently disappeared", err)
					}
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}
