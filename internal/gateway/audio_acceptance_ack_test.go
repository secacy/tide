package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

func decodeAudioAckMessage(t *testing.T, typ websocket.MessageType, data []byte, generation, offset uint64, ended bool) {
	t.Helper()
	var message wsprotocol.AudioAckMessage
	if typ != websocket.MessageText || json.Unmarshal(data, &message) != nil || message != (wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: generation, NextOffset: offset, InputEnded: ended}) {
		t.Error("audio ACK type, generation, position or end status mismatch")
	}
}

func TestAudioAcceptanceAckSnapshotAndNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		initial, err := f.session.requestResult(context.Background(), 1)
		if err != nil || initial.available || initial.input != (inputAcceptance{}) {
			t.Fatal("initial output snapshot incorrect", err)
		}
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		assertDeliveryNotification(t, initial.changed, true)
		accepted, err := f.session.requestResult(context.Background(), 1)
		if err != nil || accepted.available || accepted.input != (inputAcceptance{nextOffset: 2}) {
			t.Fatal("audio snapshot incorrect", err)
		}
		a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		assertDeliveryNotification(t, accepted.changed, false)
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		assertDeliveryNotification(t, accepted.changed, true)
		ended, err := f.session.requestResult(context.Background(), 1)
		if err != nil || ended.available || ended.input != (inputAcceptance{nextOffset: 2, inputEnded: true}) {
			t.Fatal("end snapshot incorrect", err)
		}
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		assertDeliveryNotification(t, ended.changed, false)
		if initial.input != (inputAcceptance{}) || accepted.input.inputEnded {
			t.Fatal("old value snapshots changed after later input")
		}
		closeDeliveryFixture(t, f)
	})
}

func TestAudioAcceptanceAckWithoutResultsAndNoDuplicateMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		messages := make(chan []byte, 8)
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(_ context.Context, _ websocket.MessageType, data []byte) error {
			messages <- append([]byte(nil), data...)
			return nil
		}), time.Second)
		ctx, cancel := context.WithCancelCause(f.lifeCtx)
		startTestReadyWriter(t, w, ctx, cancel)
		synctest.Wait()
		assertReadyMessage(t, decodeReadyMessage(t, websocket.MessageText, <-messages), f, 1, 0, 0, false)
		if len(messages) != 0 {
			t.Fatal("unchanged ready triggered an extra audio ACK")
		}
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		synctest.Wait()
		decodeAudioAckMessage(t, websocket.MessageText, <-messages, 1, 2, false)
		a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		synctest.Wait()
		if len(messages) != 0 {
			t.Fatal("historical audio emitted duplicate ACK")
		}
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		synctest.Wait()
		decodeAudioAckMessage(t, websocket.MessageText, <-messages, 1, 2, true)
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		synctest.Wait()
		if len(messages) != 0 {
			t.Fatal("duplicate end emitted extra ACK")
		}
		closeDeliveryFixture(t, f)
	})
}

func TestAudioAcceptanceAckCoalescesAndServesBorrowedResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "first")
		readyStarted, readyRelease := make(chan struct{}), make(chan struct{})
		ackStarted, ackRelease := make(chan struct{}), make(chan struct{})
		var readyOnce, ackOnce sync.Once
		unblock := func() { readyOnce.Do(func() { close(readyRelease) }); ackOnce.Do(func() { close(ackRelease) }) }
		messages := make(chan []byte, 8)
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, _ websocket.MessageType, data []byte) error {
			messages <- append([]byte(nil), data...)
			var tag struct {
				Type wsprotocol.MessageType `json:"type"`
			}
			if err := json.Unmarshal(data, &tag); err != nil {
				t.Error(err)
			}
			var release <-chan struct{}
			if tag.Type == wsprotocol.MessageTypeReady {
				close(readyStarted)
				release = readyRelease
			} else if tag.Type == wsprotocol.MessageTypeAudioAck {
				var ack wsprotocol.AudioAckMessage
				if err := json.Unmarshal(data, &ack); err != nil {
					t.Error(err)
				}
				if ack.NextOffset == 2 && !ack.InputEnded {
					close(ackStarted)
					release = ackRelease
				}
			}
			if release != nil {
				select {
				case <-release:
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			}
			return nil
		}), time.Second)
		ctx, cancel := context.WithCancelCause(f.lifeCtx)
		run := startTestReadyWriter(t, w, ctx, cancel)
		t.Cleanup(unblock)
		<-readyStarted
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		readyOnce.Do(func() { close(readyRelease) })
		<-ackStarted
		synctest.Wait()
		if f.worker.delivery.inFlightSeq != 1 {
			t.Fatal("test did not establish a result lease before ACK write")
		}
		for _, chunk := range []struct {
			offset uint64
			data   string
		}{{2, "cd"}, {4, "ef"}} {
			a, n, err := f.session.requestAudio(context.Background(), 1, chunk.offset, []byte(chunk.data))
			assertCoordinatorInput(t, a, n, err, true, chunk.offset+2, nil)
		}
		a, n, err = f.session.requestEnd(context.Background(), 1, 6)
		assertCoordinatorInput(t, a, n, err, true, 6, nil)
		feedDeliveryResult(t, feed, "second")
		ackOnce.Do(func() { close(ackRelease) })
		synctest.Wait()
		assertReadyMessage(t, decodeReadyMessage(t, websocket.MessageText, <-messages), f, 1, 0, 0, false)
		decodeAudioAckMessage(t, websocket.MessageText, <-messages, 1, 2, false)
		if msg := decodeWriterMessage(t, websocket.MessageText, <-messages); msg.Seq != 1 || msg.Text != "first" {
			t.Fatal("borrowed result was skipped or delayed behind another ACK")
		}
		decodeAudioAckMessage(t, websocket.MessageText, <-messages, 1, 6, true)
		if msg := decodeWriterMessage(t, websocket.MessageText, <-messages); msg.Seq != 2 || msg.Text != "second" {
			t.Fatal("following result was skipped")
		}
		if len(messages) != 0 {
			t.Fatal("intermediate input states were queued instead of coalesced")
		}
		feed <- workerReadStep{err: io.EOF}
		run.assertExit(t, writerResultsComplete, 2, nil)
		closeDeliveryFixture(t, f)
	})
}

func TestAudioAcceptanceAckUpdateBeforeIdleWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		base, cancel := context.WithCancelCause(f.lifeCtx)
		ctx := &writerIdleGateContext{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
		messages := make(chan []byte, 4)
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(_ context.Context, _ websocket.MessageType, data []byte) error {
			messages <- append([]byte(nil), data...)
			return nil
		}), time.Second)
		done := make(chan struct{})
		go func() { last := inputAcceptance{}; _ = w.runLoop(ctx, &last); close(done) }()
		t.Cleanup(func() { cancel(nil); <-done })
		var once sync.Once
		unblock := func() { once.Do(func() { close(ctx.release) }) }
		t.Cleanup(unblock)
		<-ctx.entered // 已取得空 offer，尚未进入等待 select。
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		unblock()
		synctest.Wait()
		select {
		case data := <-messages:
			decodeAudioAckMessage(t, websocket.MessageText, data, 1, 2, false)
		default:
			t.Fatal("update between query and wait was lost")
		}
		closeDeliveryFixture(t, f)
	})
}

func TestAudioAcceptanceAckEmptyEndBeforeCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		messages := make(chan []byte, 4)
		w := newTestGenerationWriter(t, f, 1, writerTestConn(func(_ context.Context, _ websocket.MessageType, data []byte) error {
			messages <- append([]byte(nil), data...)
			return nil
		}), time.Second)
		ctx, cancel := context.WithCancelCause(f.lifeCtx)
		run := startTestReadyWriter(t, w, ctx, cancel)
		synctest.Wait()
		assertReadyMessage(t, decodeReadyMessage(t, websocket.MessageText, <-messages), f, 1, 0, 0, false)
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		run.assertExit(t, writerResultsComplete, 0, nil)
		decodeAudioAckMessage(t, websocket.MessageText, <-messages, 1, 0, true)
		if len(messages) != 0 {
			t.Fatal("empty completion produced duplicate ACK")
		}
		closeDeliveryFixture(t, f)
	})
}

func TestAudioAcceptanceAckFailurePreservesResultForRecovery(t *testing.T) {
	for _, name := range []string{"transport", "timeout", "detach"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				if _, err := f.worker.results.append("s", "saved", true); err != nil {
					t.Fatal(err)
				}
				readyStarted, readyRelease, ackStarted := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(readyRelease) }) }
				conn := &attachmentTestConn{writeFn: func(ctx context.Context, typ websocket.MessageType, data []byte) error {
					var tag struct {
						Type wsprotocol.MessageType `json:"type"`
					}
					if err := json.Unmarshal(data, &tag); err != nil {
						t.Error(err)
					}
					if tag.Type == wsprotocol.MessageTypeReady {
						close(readyStarted)
						select {
						case <-readyRelease:
							return nil
						case <-ctx.Done():
							return context.Cause(ctx)
						}
					}
					decodeAudioAckMessage(t, typ, data, 1, 2, false)
					close(ackStarted)
					if name == "transport" {
						return io.EOF
					}
					<-ctx.Done()
					return context.Cause(ctx)
				}}
				startManagedFixture(t, f, conn)
				t.Cleanup(unblock)
				<-readyStarted
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				unblock()
				<-ackStarted
				if name == "timeout" {
					time.Sleep(time.Second)
				}
				if name == "detach" {
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatal(d, err)
					}
				}
				synctest.Wait()
				if f.session.resume.phase != resumeDetached || context.Cause(f.rpcCtx) != nil || conn.closes.Load() != 1 || f.worker.delivery.inFlightSeq != 0 || f.worker.delivery.cursor != 0 || f.worker.delivery.offeredSeq != 1 || f.worker.results.count != 1 || f.worker.results.ackedSeq != 0 {
					t.Fatal("ACK failure falsely completed result, lost Worker, or failed cleanup")
				}
				messages := make(chan []byte, 4)
				next := &attachmentTestConn{writeFn: func(_ context.Context, _ websocket.MessageType, data []byte) error {
					messages <- append([]byte(nil), data...)
					return nil
				}}
				if g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, next), 0); g != 2 || err != nil {
					t.Fatal(g, err)
				}
				synctest.Wait()
				assertReadyMessage(t, decodeReadyMessage(t, websocket.MessageText, <-messages), f, 2, 2, 0, false)
				if result := decodeWriterMessage(t, websocket.MessageText, <-messages); result.Seq != 1 || result.Text != "saved" {
					t.Fatal("original result was not replayed")
				}
				if len(messages) != 0 {
					t.Fatal("ready input position was redundantly acknowledged")
				}
				closeDeliveryFixture(t, f)
				if next.closes.Load() != 1 {
					t.Fatal("recovery cleanup did not close owned connection")
				}
			})
		})
	}
}

func TestAudioAcceptanceAckWaitsForActualWriteReturn(t *testing.T) {
	for _, name := range []string{"cancel", "deadline"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				feedDeliveryResult(t, feed, "saved")
				entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, typ websocket.MessageType, data []byte) error {
					decodeAudioAckMessage(t, typ, data, 1, 2, false)
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-release
					return nil
				}), time.Second)
				ctx, cancel := context.WithCancelCause(f.lifeCtx)
				run := &resultWriterRunFixture{finished: make(chan struct{}), cancel: cancel}
				last := inputAcceptance{}
				go func() { run.exit = w.runLoop(ctx, &last); close(run.finished) }()
				t.Cleanup(func() { cancel(nil); <-run.finished })
				t.Cleanup(unblock)
				<-entered
				kind, want := writerWriteFailed, ErrResultWriteTimeout
				if name == "cancel" {
					cancel(nil)
					kind, want = writerStopped, context.Canceled
				} else {
					time.Sleep(time.Second)
				}
				<-canceled
				synctest.Wait()
				select {
				case <-run.finished:
					t.Fatal("ACK task returned while actual Write still retained data")
				default:
				}
				if last != (inputAcceptance{}) || f.worker.delivery.inFlightSeq != 1 {
					t.Fatal("failed ACK changed last sent input or lost result lease")
				}
				unblock()
				run.assertExit(t, kind, 0, want)
				if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResultWriteInFlight) {
					t.Fatal("ACK was falsely reported as result success", err)
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestAudioAcceptanceAckManagedShutdownWaitsForWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		readyWritten, entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		conn := &attachmentTestConn{writeFn: func(ctx context.Context, typ websocket.MessageType, data []byte) error {
			var tag struct {
				Type wsprotocol.MessageType `json:"type"`
			}
			if err := json.Unmarshal(data, &tag); err != nil {
				t.Error(err)
			}
			if tag.Type == wsprotocol.MessageTypeReady {
				decodeReadyMessage(t, typ, data)
				close(readyWritten)
				return nil
			}
			decodeAudioAckMessage(t, typ, data, 1, 2, false)
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil
		}}
		startManagedFixture(t, f, conn)
		t.Cleanup(unblock)
		<-readyWritten
		synctest.Wait()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		<-entered
		cause := errors.New("logical shutdown during audio ACK")
		f.cancelLife(cause)
		<-canceled
		synctest.Wait()
		select {
		case <-f.session.controlDone:
		default:
			t.Fatal("ACK Write prevented control shutdown")
		}
		select {
		case <-f.finished:
			t.Fatal("managed runner returned before actual ACK Write")
		default:
		}
		if conn.closes.Load() != 1 || f.worker.input == nil || f.worker.results == nil || !errors.Is(context.Cause(f.rpcCtx), cause) {
			t.Fatal("shutdown ownership or buffer retention incorrect")
		}
		unblock()
		f.assertFinishedAtCurrentTime(t, cause)
	})
}
