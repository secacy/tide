package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// attachmentTestConn 控制真正 attachment 的 I/O 边界；计数可并发读取。
type attachmentTestConn struct {
	readFn                func(context.Context) (websocket.MessageType, []byte, error)
	writeFn               func(context.Context, websocket.MessageType, []byte) error
	closeFn               func() error
	reads, writes, closes atomic.Int32
	limit                 atomic.Int64
}

func (c *attachmentTestConn) SetReadLimit(n int64) { c.limit.Store(n) }
func (c *attachmentTestConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	c.reads.Add(1)
	if c.readFn != nil {
		return c.readFn(ctx)
	}
	<-ctx.Done()
	return 0, nil, context.Cause(ctx)
}
func (c *attachmentTestConn) Write(ctx context.Context, typ websocket.MessageType, data []byte) error {
	c.writes.Add(1)
	if c.writeFn != nil {
		return c.writeFn(ctx, typ, data)
	}
	return nil
}
func (c *attachmentTestConn) CloseNow() error {
	c.closes.Add(1)
	if c.closeFn != nil {
		return c.closeFn()
	}
	return nil
}

func newAttachmentCandidate(t *testing.T, conn sessionConnection) *connectionCandidate {
	t.Helper()
	c, err := newConnectionCandidate(conn, time.Second, 128)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func startManagedFixture(t *testing.T, f *workerCoordinatorFixture, conn sessionConnection) {
	t.Helper()
	candidate := newAttachmentCandidate(t, conn)
	f.started = true
	go func() {
		f.err = f.session.runWithConnection(f.lifeCtx, f.now, f.worker, candidate)
		close(f.finished)
	}()
}

// resumeBoundaryContext 在命令取消检查处注入边界条件，无需改变核心代码。
// Err 的前三次检查分别是提交前、协调者处理前和候选准备后的最终检查。
type resumeBoundaryContext struct {
	context.Context
	checks atomic.Int32
	hook   func(int32)
}

func (c *resumeBoundaryContext) Err() error {
	n := c.checks.Add(1)
	if c.hook != nil {
		c.hook(n)
	}
	return c.Context.Err()
}

func TestConnectionAttachmentConstruction(t *testing.T) {
	f, _ := newDuplexWorkerFixture(t, nil, nil)
	conn := &attachmentTestConn{}
	for _, name := range []string{"nil_conn", "zero_write", "negative_write", "short_limit", "valid"} {
		t.Run(name, func(t *testing.T) {
			var socket sessionConnection = conn
			budget, limit := time.Second, int64(128)
			switch name {
			case "nil_conn":
				socket = nil
			case "zero_write":
				budget = 0
			case "negative_write":
				budget = -1
			case "short_limit":
				limit = 8
			}
			c, err := newConnectionCandidate(socket, budget, limit)
			if name == "valid" {
				if err != nil || c == nil {
					t.Fatal(c, err)
				}
			} else if c != nil || !errors.Is(err, errInvalidConnectionCandidate) {
				t.Fatal(c, err)
			}
		})
	}
	candidate := newAttachmentCandidate(t, conn)
	for _, name := range []string{"nil_session", "nil_context", "zero_generation", "nil_candidate", "valid"} {
		t.Run(name, func(t *testing.T) {
			s, ctx, generation, c := f.session, f.lifeCtx, uint64(1), candidate
			switch name {
			case "nil_session":
				s = nil
			case "nil_context":
				ctx = nil
			case "zero_generation":
				generation = 0
			case "nil_candidate":
				c = nil
			}
			a, err := newConnectionAttachment(s, ctx, generation, c)
			if name == "valid" {
				if err != nil || a == nil || cap(a.events) != 2 || a.reader.config.conn != conn || a.writer.config.conn != conn {
					t.Fatal(a, err)
				}
				a.stop(nil) // 未安装时只释放子 context，不关闭 socket。
			} else {
				want := errInvalidConnectionAttachment
				if name == "nil_candidate" {
					want = errInvalidConnectionCandidate
				}
				if a != nil || !errors.Is(err, want) {
					t.Fatal(a, err)
				}
			}
		})
	}
	if conn.reads.Load() != 0 || conn.writes.Load() != 0 || conn.closes.Load() != 0 || conn.limit.Load() != 0 || context.Cause(f.rpcCtx) != nil {
		t.Fatal("construction acquired I/O or Worker cancellation ownership")
	}
}

func TestConnectionAttachmentWaitsForActualCloseAndTasks(t *testing.T) {
	for _, name := range []string{"stop", "control_done", "already_canceled", "close_error", "already_closed"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				releaseClose, closeStarted := make(chan struct{}), make(chan struct{})
				closeFailure := errors.New("close failed")
				conn := &attachmentTestConn{closeFn: func() error {
					close(closeStarted)
					<-releaseClose
					if name == "close_error" {
						return closeFailure
					}
					if name == "already_closed" {
						return fmt.Errorf("closed: %w", net.ErrClosed)
					}
					return nil
				}}
				a, err := newConnectionAttachment(f.session, f.lifeCtx, 1, newAttachmentCandidate(t, conn))
				if err != nil {
					t.Fatal(err)
				}
				if name == "already_canceled" {
					f.cancelLife(io.EOF)
				}
				go a.run()
				synctest.Wait()
				if name == "control_done" {
					close(f.session.controlDone)
				} else {
					a.stop(io.EOF)
					a.stop(errors.New("duplicate stop"))
				}
				<-closeStarted
				synctest.Wait()
				select {
				case <-a.done:
					t.Fatal("done preceded actual CloseNow return")
				default:
				}
				close(releaseClose)
				<-a.done
				if conn.closes.Load() != 1 || len(a.events) != 2 {
					t.Fatal("close ownership or bounded final reports incorrect")
				}
				seen := map[connectionTaskKind]bool{}
				for range 2 {
					event := <-a.events
					if event.generation != 1 || seen[event.task] {
						t.Fatal(event)
					}
					seen[event.task] = true
				}
				if !seen[connectionReaderTask] || !seen[connectionWriterTask] {
					t.Fatal(seen)
				}
				want := error(nil)
				if name == "close_error" {
					want = closeFailure
				}
				if !errors.Is(a.closeErr, want) || context.Cause(f.rpcCtx) != nil && name != "already_canceled" {
					t.Fatal("close classification or parent cancellation", a.closeErr)
				}
			})
		})
	}
}

func TestSessionConnectionsRetiringPreservesWorkerAndWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		release := make(chan struct{})
		old := &attachmentTestConn{closeFn: func() error { <-release; return nil }}
		startManagedFixture(t, f, old)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(d, err)
		}
		synctest.Wait()
		deadline := f.session.resume.expiresAt
		candidateConn := &attachmentTestConn{}
		if g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, candidateConn), 0); g != 0 || !errors.Is(err, errConnectionRetiring) {
			t.Fatal(g, err)
		}
		if d, err := f.session.reportDetach(context.Background(), 1); d || err != nil {
			t.Fatal(d, err)
		}
		if f.session.resume.expiresAt != deadline || context.Cause(f.rpcCtx) != nil || candidateConn.closes.Load() != 0 || candidateConn.reads.Load() != 0 {
			t.Fatal("retiring retry renewed deadline, canceled Worker or acquired candidate")
		}
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-f.finished:
			t.Fatal("runner returned before connection cleanup")
		default:
		}
		if f.worker.input == nil || f.worker.results == nil {
			t.Fatal("buffers freed before attachment returned")
		}
		close(release)
		f.assertFinishedAtCurrentTime(t, nil)
	})
}

func TestSessionConnectionsCanceledCandidateDoesNotCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		startManagedFixture(t, f, &attachmentTestConn{})
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(d, err)
		}
		synctest.Wait()
		original := f.session.resume
		deadline := original.expiresAt
		base, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := &resumeBoundaryContext{Context: base, hook: func(n int32) {
			if n == 3 {
				cancel()
			}
		}}
		conn := &attachmentTestConn{}
		g, err := f.session.requestResumeConnection(ctx, newAttachmentCandidate(t, conn), 0)
		if g != 0 || !errors.Is(err, context.Canceled) || ctx.checks.Load() != 3 {
			t.Fatalf("canceled preparation = (%d,%v), checks=%d", g, err, ctx.checks.Load())
		}
		synctest.Wait()
		if f.session.resume != original || f.session.resume.phase != resumeDetached || f.session.resume.generation != 1 || f.session.resume.expiresAt != deadline || conn.reads.Load() != 0 || conn.closes.Load() != 0 {
			t.Errorf("rejected candidate changed live recovery state: phase=%v generation=%d deadline=%v; original deadline=%v", f.session.resume.phase, f.session.resume.generation, f.session.resume.expiresAt, deadline)
		}
		closeDeliveryFixture(t, f)
	})
}

func TestSessionConnectionsPreparationCannotEraseExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		var armed atomic.Bool
		var checks atomic.Int32
		var deadline time.Time // 在 armed 之前赋值，Load/Store 建立同步。
		f.now = func() time.Time {
			if armed.Load() && checks.Add(1) >= 2 {
				return deadline
			}
			return time.Now()
		}
		startManagedFixture(t, f, &attachmentTestConn{})
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(d, err)
		}
		synctest.Wait()
		deadline = f.session.resume.expiresAt
		ctx := &resumeBoundaryContext{Context: context.Background(), hook: func(n int32) {
			if n == 2 {
				armed.Store(true)
			}
		}}
		conn := &attachmentTestConn{}
		g, err := f.session.requestResumeConnection(ctx, newAttachmentCandidate(t, conn), 0)
		if g != 0 || !errors.Is(err, errResumeClosed) {
			t.Errorf("candidate accepted after preparation reached deadline: (%d,%v)", g, err)
		}
		synctest.Wait()
		select {
		case <-f.finished:
			if !errors.Is(f.err, errResumeExpired) || f.session.resume.generation != 1 {
				t.Errorf("expired runner = %v, generation=%d", f.err, f.session.resume.generation)
			}
		default:
			t.Error("preparation erased recovery deadline and left session running")
			if err := f.session.requestClose(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if conn.reads.Load() != 0 || conn.closes.Load() != 0 {
			t.Error("rejected/expired candidate was taken into ownership")
		}
	})
}

func TestSessionConnectionsFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		recoverable bool
	}{
		{"nil", nil, false}, {"io", io.EOF, true}, {"write_timeout", ErrResultWriteTimeout, true},
		{"normal_close", websocket.CloseError{Code: websocket.StatusNormalClosure}, true},
		{"going_away", websocket.CloseError{Code: websocket.StatusGoingAway}, true},
		{"protocol", websocket.CloseError{Code: websocket.StatusProtocolError}, false},
		{"payload", websocket.CloseError{Code: websocket.StatusInvalidFramePayloadData}, false},
		{"policy", websocket.CloseError{Code: websocket.StatusPolicyViolation}, false},
		{"size", websocket.CloseError{Code: websocket.StatusMessageTooBig}, false},
		{"remote_internal", websocket.CloseError{Code: websocket.StatusInternalError}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := recoverableConnectionFailure(tc.err); got != tc.recoverable {
				t.Fatal(got)
			}
			if tc.err != nil && recoverableConnectionFailure(fmt.Errorf("wrapped: %w", tc.err)) != tc.recoverable {
				t.Fatal("wrapped close lost classification")
			}
		})
	}
}

func TestSessionConnectionsRejectsModesAndPositions(t *testing.T) {
	for _, name := range []string{"unmanaged", "missing_candidate", "attached", "ack_ahead", "replay_gap"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				if name == "replay_gap" {
					f.worker.results.lastSeq, f.worker.results.ackedSeq = 2, 2
					f.worker.delivery.cursor, f.worker.delivery.offeredSeq = 2, 2
				}
				if name == "unmanaged" {
					f.start()
				} else {
					startManagedFixture(t, f, &attachmentTestConn{})
				}
				if name == "ack_ahead" || name == "replay_gap" {
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatal(d, err)
					}
					synctest.Wait()
				}
				conn := &attachmentTestConn{}
				var g uint64
				var err error
				var want error
				if name == "missing_candidate" {
					g, err = f.session.requestResume(context.Background(), 0)
					want = errConnectionCandidateRequired
				} else {
					applied := uint64(0)
					if name == "ack_ahead" || name == "replay_gap" {
						applied = 1
					}
					g, err = f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, conn), applied)
					switch name {
					case "unmanaged":
						want = errConnectionManagementUnavailable
					case "attached":
						want = errResumeAlreadyAttached
					case "ack_ahead":
						want = errResultAckAhead
					case "replay_gap":
						want = errResultReplayGap
					}
				}
				if g != 0 || !errors.Is(err, want) {
					t.Fatal(g, err, want)
				}
				synctest.Wait()
				if f.session.resume.generation != 1 || conn.reads.Load() != 0 || conn.closes.Load() != 0 {
					t.Fatal("rejection committed candidate")
				}
				if name == "replay_gap" && f.worker.results.ackedSeq != 2 {
					t.Fatal("rejected recovery changed cumulative ACK")
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestSessionConnectionsWriterCompletionKeepsReader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		conn := &attachmentTestConn{}
		startManagedFixture(t, f, conn)
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		synctest.Wait()
		if conn.closes.Load() != 0 || f.session.resume.phase != resumeAttached {
			t.Fatal("normal writer completion closed connection")
		}
		if advanced, err := f.session.requestResultAck(context.Background(), 1, 0); advanced || err != nil {
			t.Fatal(advanced, err)
		}
		assertControlAlive(t, f.session)
		closeDeliveryFixture(t, f)
		if conn.closes.Load() != 1 {
			t.Fatal("final cleanup did not own socket close")
		}
	})
}

func TestSessionConnectionsInitialCanceledAndCleanupError(t *testing.T) {
	for _, name := range []string{"already_canceled", "cleanup_join"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				cause, closeFailure := errors.New("logical stop before start"), errors.New("cleanup failure")
				conn := &attachmentTestConn{}
				want := error(cause)
				if name == "already_canceled" {
					f.cancelLife(cause)
				} else {
					conn.readFn = func(context.Context) (websocket.MessageType, []byte, error) {
						return websocket.MessageText, []byte(`{"type":"unsupported"}`), nil
					}
					conn.closeFn = func() error { return closeFailure }
					want = wsprotocol.ErrInvalidV2Input
				}
				startManagedFixture(t, f, conn)
				f.assertFinishedAtCurrentTime(t, want)
				if conn.closes.Load() != 1 || name == "cleanup_join" && !errors.Is(f.err, closeFailure) {
					t.Fatal("initial ownership or joined cause lost", f.err)
				}
			})
		})
	}
}

// 此测试直接向协调者注入已报告事件，专门验证事件/完成信号的裁决顺序。
// attachment 的实际 I/O 与 done 顺序由上述任务测试及真实网络测试覆盖。
func TestSessionConnectionsReportedFatalSurvivesDetachAndDone(t *testing.T) {
	for _, fatalTask := range []connectionTaskKind{connectionReaderTask, connectionWriterTask} {
		for _, doneReady := range []bool{false, true} {
			for _, stale := range []bool{false, true} {
				name := fmt.Sprintf("task_%d/done_%t/stale_%t", fatalTask, doneReady, stale)
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						f, _ := newDuplexWorkerFixture(t, nil, nil)
						a, err := newConnectionAttachment(f.session, f.lifeCtx, 1, newAttachmentCandidate(t, &attachmentTestConn{}))
						if err != nil {
							t.Fatal(err)
						}
						defer a.stop(nil)
						fatal := errors.New("already reported unrecoverable failure")
						transportEvent := connectionEvent{generation: 1, task: connectionWriterTask, writer: resultWriterExit{kind: writerWriteFailed, err: io.EOF}}
						fatalEvent := connectionEvent{generation: 1, task: connectionReaderTask, reader: connectionReaderExit{kind: readerProtocolFailed, err: fatal}}
						if fatalTask == connectionWriterTask {
							transportEvent = connectionEvent{generation: 1, task: connectionReaderTask, reader: connectionReaderExit{kind: readerReadFailed, err: io.EOF}}
							fatalEvent = connectionEvent{generation: 1, task: connectionWriterTask, writer: resultWriterExit{kind: writerControlFailed, err: fatal}}
						}
						if stale {
							fatalEvent.generation = 0
						}
						a.events <- transportEvent
						a.events <- fatalEvent
						if doneReady {
							close(a.done)
						}
						connections := &sessionConnections{current: a}
						f.worker.startIO()
						f.started = true
						go func() {
							f.err = f.session.runCoordinator(f.lifeCtx, f.now, f.worker, connections)
							f.worker.stopIO(f.err)
							f.worker.waitIO()
							f.worker.releaseBuffers()
							close(f.finished)
						}()
						if stale {
							synctest.Wait()
							assertControlAlive(t, f.session)
							if f.session.resume.phase != resumeDetached || f.session.resume.generation != 1 {
								t.Fatal("stale event changed state")
							}
							closeDeliveryFixture(t, f)
						} else {
							f.assertFinishedAtCurrentTime(t, fatal)
						}
					})
				})
			}
		}
	}
}

func TestSessionConnectionsConcurrentCandidatesHaveOneOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		startManagedFixture(t, f, &attachmentTestConn{})
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(d, err)
		}
		synctest.Wait()
		const count = 32
		conns := make([]*attachmentTestConn, count)
		var winners atomic.Int32
		var wg sync.WaitGroup
		for i := range count {
			conns[i] = &attachmentTestConn{}
			candidate := newAttachmentCandidate(t, conns[i])
			wg.Go(func() {
				g, err := f.session.requestResumeConnection(context.Background(), candidate, 0)
				if g == 2 && err == nil {
					winners.Add(1)
				} else if g != 0 || !errors.Is(err, errResumeAlreadyAttached) {
					t.Errorf("candidate = (%d,%v)", g, err)
				}
			})
		}
		wg.Wait()
		synctest.Wait()
		if winners.Load() != 1 || f.session.resume.generation != 2 {
			t.Fatal("multiple candidates acquired ownership", winners.Load())
		}
		var acquired int
		for _, conn := range conns {
			if conn.closes.Load() != 0 {
				t.Fatal("rejected candidate was closed by coordinator")
			}
			if conn.reads.Load() != 0 {
				acquired++
			}
		}
		if acquired != 1 {
			t.Fatal("more than one reader was installed", acquired)
		}
		closeDeliveryFixture(t, f)
		var ownedCloses int32
		for _, conn := range conns {
			ownedCloses += conn.closes.Load()
		}
		if ownedCloses != 1 {
			t.Fatal("cleanup did not close exactly the accepted candidate", ownedCloses)
		}
	})
}

func TestSessionConnectionsRepeatedHandoffDoesNotAccumulateOwners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		current := &attachmentTestConn{}
		startManagedFixture(t, f, current)
		all := []*attachmentTestConn{current}
		for generation := uint64(1); generation <= 8; generation++ {
			if d, err := f.session.reportDetach(context.Background(), generation); !d || err != nil {
				t.Fatal(d, err)
			}
			synctest.Wait()
			if current.closes.Load() != 1 || context.Cause(f.rpcCtx) != nil {
				t.Fatal("old owner not released or original Worker canceled")
			}
			next := &attachmentTestConn{}
			if g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, next), 0); g != generation+1 || err != nil {
				t.Fatal(g, err)
			}
			synctest.Wait()
			all = append(all, next)
			for _, old := range all[:len(all)-1] {
				if old.closes.Load() != 1 {
					t.Fatal("retired owners accumulated")
				}
			}
			if next.closes.Load() != 0 || next.reads.Load() != 1 {
				t.Fatal("new attachment not running alone")
			}
			current = next
		}
		closeDeliveryFixture(t, f)
		for _, conn := range all {
			if conn.closes.Load() != 1 {
				t.Fatal("accepted connection close count differs from one")
			}
		}
	})
}

func TestSessionConnectionsExpiryWhileCloseIsBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		release := make(chan struct{})
		var releaseOnce sync.Once
		// 失败时也先解除关闭阻塞，再执行运行器夹具的清理等待。
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		conn := &attachmentTestConn{closeFn: func() error { <-release; return nil }}
		startManagedFixture(t, f, conn)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(d, err)
		}
		synctest.Wait()
		time.Sleep(f.session.resume.window) // 虚拟时钟推进到绝对恢复截止时间。
		synctest.Wait()
		select {
		case <-f.session.controlDone:
		default:
			t.Fatal("blocked socket close prevented recovery expiry")
		}
		if !errors.Is(context.Cause(f.rpcCtx), errResumeExpired) {
			t.Fatal("expiry did not cancel the original Worker", context.Cause(f.rpcCtx))
		}
		select {
		case <-f.finished:
			t.Fatal("runner reported cleanup while CloseNow was still blocked")
		default:
		}
		if f.worker.input == nil || f.worker.results == nil {
			t.Fatal("expiry released buffers before connection cleanup")
		}
		releaseOnce.Do(func() { close(release) })
		f.assertFinishedAtCurrentTime(t, errResumeExpired)
		if conn.closes.Load() != 1 {
			t.Fatal("expiry duplicated socket cleanup")
		}
	})
}

func TestSessionConnectionsGenerationExhaustionDoesNotTakeCandidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		// 初始化合法的退休状态，直接验证候选协调路径，不执行海量历史交接。
		f.session.resume.detach(1, time.Now())
		f.session.resume.generation = math.MaxUint64
		deadline := f.session.resume.expiresAt
		connections := &sessionConnections{}
		f.worker.startIO()
		f.started = true
		go func() {
			f.err = f.session.runCoordinator(f.lifeCtx, f.now, f.worker, connections)
			f.worker.stopIO(f.err)
			f.worker.waitIO()
			f.worker.releaseBuffers()
			close(f.finished)
		}()
		conn := &attachmentTestConn{}
		g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, conn), 0)
		if g != 0 || !errors.Is(err, errResumeGenerationExhausted) {
			t.Fatal(g, err)
		}
		synctest.Wait()
		if f.session.resume.generation != math.MaxUint64 || f.session.resume.phase != resumeDetached || f.session.resume.expiresAt != deadline || conn.reads.Load() != 0 || conn.closes.Load() != 0 || context.Cause(f.rpcCtx) != nil {
			t.Fatal("generation rejection changed state or took candidate ownership")
		}
		closeDeliveryFixture(t, f)
	})
}
