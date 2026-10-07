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

// startTestReadyWriter 启动同一个写任务的 ready+result 入口，清理时等待实际返回。
func startTestReadyWriter(t *testing.T, w *resultWriter, ctx context.Context, cancel context.CancelCauseFunc) *resultWriterRunFixture {
	t.Helper()
	run := &resultWriterRunFixture{finished: make(chan struct{}), cancel: cancel}
	go func() {
		run.exit = w.runWithReady(ctx)
		close(run.finished)
	}()
	t.Cleanup(func() { cancel(nil); <-run.finished })
	return run
}

func decodeReadyMessage(t *testing.T, typ websocket.MessageType, data []byte) wsprotocol.ReadyMessage {
	t.Helper()
	var message wsprotocol.ReadyMessage
	if typ != websocket.MessageText || json.Unmarshal(data, &message) != nil || message.Type != wsprotocol.MessageTypeReady || message.Generation == 0 {
		t.Error("first output is not a valid ready text message (payload redacted)")
	}
	return message
}

func assertReadyMessage(t *testing.T, got wsprotocol.ReadyMessage, f *workerCoordinatorFixture, generation, offset, acked uint64, ended bool) {
	t.Helper()
	if got.Type != wsprotocol.MessageTypeReady {
		t.Error("message type is not ready")
	}
	assertReadySnapshot(t, connectionReady{sessionID: got.SessionID, resumeToken: got.ResumeToken,
		generation: got.Generation, nextOffset: got.NextOffset, inputEnded: got.InputEnded, ackedResultSeq: got.AckedResultSeq}, f, generation, offset, acked, ended)
}

func TestResultWriterReadyPrecedesExistingResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "first")
		feedDeliveryResult(t, feed, "second")
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		messages := make(chan []byte, 8)
		var calls atomic.Int32
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, typ websocket.MessageType, data []byte) error {
			messages <- append([]byte(nil), data...)
			if calls.Add(1) == 1 {
				decodeReadyMessage(t, typ, data)
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			}
			var tag struct {
				Type wsprotocol.MessageType `json:"type"`
			}
			if err := json.Unmarshal(data, &tag); err != nil {
				t.Error(err)
			}
			if tag.Type == wsprotocol.MessageTypeAudioAck {
				decodeAudioAckMessage(t, typ, data, 1, 2, true)
			} else if tag.Type == wsprotocol.MessageTypeCompleted {
				decodeCompletionMessage(t, typ, data, 1, 2, 2)
			} else {
				decodeWriterMessage(t, typ, data)
			}
			return nil
		}), time.Second)
		ctx, cancel := context.WithCancelCause(f.lifeCtx)
		run := startTestReadyWriter(t, w, ctx, cancel)
		t.Cleanup(unblock) // 失败时先解除 Write，再等待任务清理。
		<-entered
		synctest.Wait()
		if calls.Load() != 1 || f.worker.delivery.offeredSeq != 0 || f.worker.delivery.inFlightSeq != 0 || f.worker.delivery.cursor != 0 {
			t.Fatal("result was authorized before ready Write completed")
		}
		// Write 阻塞期间仍能处理输入和查询；已编码 ready 保持旧前缀快照。
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		synctest.Wait()
		current, err := f.session.requestConnectionReady(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		assertReadySnapshot(t, current, f, 1, 2, 0, true)
		ready := decodeReadyMessage(t, websocket.MessageText, <-messages)
		assertReadyMessage(t, ready, f, 1, 0, 0, false)
		unblock()
		synctest.Wait()
		decodeAudioAckMessage(t, websocket.MessageText, <-messages, 1, 2, true)
		for seq := uint64(1); seq <= 2; seq++ {
			result := decodeWriterMessage(t, websocket.MessageText, <-messages)
			if result.Seq != seq {
				t.Fatal("results were not sent in original order", result.Seq)
			}
		}
		feed <- workerReadStep{err: io.EOF}
		run.assertExit(t, writerCompletionSent, 2, nil)
		decodeCompletionMessage(t, websocket.MessageText, <-messages, 1, 2, 2)
		if calls.Load() != 5 {
			t.Fatal("ready or cumulative ACK repeated within one generation")
		}
		closeDeliveryFixture(t, f)
	})
}

func TestResultWriterReadyFailureDoesNotAuthorizeResults(t *testing.T) {
	for _, name := range []string{"transport", "timeout", "connection_cancel", "logical_cancel", "control_closed", "precanceled", "stale", "detached"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				feedDeliveryResult(t, feed, "saved")
				ctx, cancel := context.WithCancelCause(f.lifeCtx)
				cause := errors.New("ready transport/connection failure")
				generation := uint64(1)
				if name == "precanceled" {
					cancel(cause)
				}
				if name == "stale" {
					generation = 2
				}
				if name == "detached" {
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatal(d, err)
					}
				}
				entered := make(chan struct{})
				var writes atomic.Int32
				w := newTestGenerationWriter(t, f, generation, writerTestConn(func(writeCtx context.Context, typ websocket.MessageType, data []byte) error {
					writes.Add(1)
					decodeReadyMessage(t, typ, data)
					close(entered)
					if name == "transport" {
						return cause
					}
					<-writeCtx.Done()
					return context.Cause(writeCtx)
				}), time.Second)
				run := startTestReadyWriter(t, w, ctx, cancel)
				kind, want := writerWriteFailed, error(cause)
				switch name {
				case "transport":
				case "timeout":
					<-entered
					time.Sleep(time.Second)
					want = ErrResultWriteTimeout
				case "connection_cancel":
					<-entered
					cancel(cause)
					kind = writerStopped
				case "logical_cancel":
					<-entered
					f.cancelLife(cause)
					kind = writerStopped
				case "control_closed":
					<-entered
					closeDeliveryFixture(t, f)
					// 部件夹具不拥有连接；按 attachment 的职责取消本代。
					cancel(errResumeClosed)
					kind, want = writerStopped, errResumeClosed
				case "precanceled":
					kind = writerStopped
				case "stale":
					kind, want = writerStopped, errSessionGenerationMismatch
				case "detached":
					kind, want = writerStopped, errSessionNotAttached
				}
				run.assertExit(t, kind, 0, want)
				wantWrites := int32(1)
				if name == "precanceled" || name == "stale" || name == "detached" {
					wantWrites = 0
				}
				if writes.Load() != wantWrites {
					t.Fatal("ready failure continued writing results", writes.Load())
				}
				if name == "logical_cancel" {
					f.assertFinishedAtCurrentTime(t, cause)
				} else if name != "control_closed" {
					synctest.Wait()
					if f.worker.delivery.offeredSeq != 0 || f.worker.delivery.inFlightSeq != 0 || f.worker.results.count != 1 || f.worker.results.ackedSeq != 0 {
						t.Fatal("ready failure changed result authorization or retention")
					}
					if name == "detached" {
						if _, err := f.session.requestResume(context.Background(), 0); err != nil {
							t.Fatal(err)
						}
						generation = 2
					} else {
						generation = 1
					}
					takeDeliveryResult(t, f.session, generation, 1)
					closeDeliveryFixture(t, f)
				}
			})
		})
	}
}

func TestResultWriterReadyWaitsForActualWriteReturn(t *testing.T) {
	for _, name := range []string{"connection", "deadline"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				feedDeliveryResult(t, feed, "saved")
				entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, typ websocket.MessageType, data []byte) error {
					decodeReadyMessage(t, typ, data)
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-release
					return nil // 模拟实际调用迟到返回 nil，不应被误判为成功。
				}), time.Second)
				ctx, cancel := context.WithCancelCause(f.lifeCtx)
				run := startTestReadyWriter(t, w, ctx, cancel)
				t.Cleanup(unblock)
				<-entered
				kind, want := writerWriteFailed, ErrResultWriteTimeout
				if name == "connection" {
					cancel(nil)
					kind, want = writerStopped, context.Canceled
				} else {
					time.Sleep(time.Second)
				}
				<-canceled
				synctest.Wait()
				select {
				case <-run.finished:
					t.Fatal("ready task returned while Write still retained data")
				default:
				}
				if f.worker.delivery.offeredSeq != 0 || f.worker.delivery.inFlightSeq != 0 {
					t.Fatal("blocked ready established result authorization")
				}
				unblock()
				run.assertExit(t, kind, 0, want)
				takeDeliveryResult(t, f.session, 1, 1)
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestConnectionReadyFailedInstallKeepsNewGenerationAndWorker(t *testing.T) {
	for _, name := range []string{"transport", "timeout"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				// 启动前建立已有结果，随后由真实 managed runner 处理授权/投递。
				if _, err := f.worker.results.append("s", "saved", true); err != nil {
					t.Fatal(err)
				}
				failWrite := func(ctx context.Context, typ websocket.MessageType, data []byte) error {
					decodeReadyMessage(t, typ, data)
					if name == "timeout" {
						<-ctx.Done()
						return context.Cause(ctx)
					}
					return io.EOF
				}
				first := &attachmentTestConn{writeFn: failWrite}
				startManagedFixture(t, f, first)
				synctest.Wait()
				if name == "timeout" {
					time.Sleep(time.Second)
					synctest.Wait()
				}
				if first.closes.Load() != 1 || f.session.resume.phase != resumeDetached || context.Cause(f.rpcCtx) != nil {
					t.Fatal("ready failure did not detach while preserving original Worker")
				}
				second := &attachmentTestConn{writeFn: failWrite}
				g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, second), 0)
				if g != 2 || err != nil {
					t.Fatal(g, err)
				}
				synctest.Wait()
				if name == "timeout" {
					time.Sleep(time.Second)
					synctest.Wait()
				}
				if f.session.resume.generation != 2 || f.session.resume.phase != resumeDetached || second.closes.Load() != 1 || second.writes.Load() != 1 || context.Cause(f.rpcCtx) != nil || f.worker.results.count != 1 || f.worker.delivery.offeredSeq != 0 {
					t.Fatal("ready failure rolled back installation or lost original results/Worker")
				}
				messages := make(chan []byte, 4)
				third := &attachmentTestConn{writeFn: func(_ context.Context, _ websocket.MessageType, data []byte) error {
					messages <- append([]byte(nil), data...)
					return nil
				}}
				if g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, third), 0); g != 3 || err != nil {
					t.Fatal(g, err)
				}
				synctest.Wait()
				assertReadyMessage(t, decodeReadyMessage(t, websocket.MessageText, <-messages), f, 3, 0, 0, false)
				result := decodeWriterMessage(t, websocket.MessageText, <-messages)
				if result.Seq != 1 || result.Text != "saved" || result.SegmentID != "s" {
					t.Fatal("original result did not survive failed ready writes")
				}
				closeDeliveryFixture(t, f)
				if third.closes.Load() != 1 {
					t.Fatal("successful generation did not complete owned cleanup")
				}
			})
		})
	}
}

func TestConnectionReadyRetentionExpiryWhileWriteBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.resultRetentionTimeout = 100 * time.Millisecond
		entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		conn := &attachmentTestConn{writeFn: func(ctx context.Context, typ websocket.MessageType, data []byte) error {
			decodeReadyMessage(t, typ, data)
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil
		}}
		startManagedFixture(t, f, conn)
		t.Cleanup(unblock)
		<-entered
		feedDeliveryResult(t, feed, "tail")
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		assertWorkerRetaining(t, f)
		time.Sleep(100 * time.Millisecond)
		<-canceled
		synctest.Wait()
		select {
		case <-f.session.controlDone:
		default:
			t.Fatal("blocked ready prevented fixed retention expiry")
		}
		select {
		case <-f.finished:
			t.Fatal("runner returned while ready Write still retained data")
		default:
		}
		if conn.closes.Load() != 1 || f.worker.input == nil || f.worker.results == nil || f.worker.delivery.offeredSeq != 0 || f.worker.delivery.inFlightSeq != 0 || f.worker.results.count != 1 {
			t.Fatal("expiry skipped socket cleanup, freed buffers early, or authorized results")
		}
		unblock()
		f.assertFinishedAtCurrentTime(t, errResultRetentionExpired)
	})
}
