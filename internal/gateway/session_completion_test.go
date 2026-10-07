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
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// decodeCompletionMessage 检查精确终态范围，不跳过意外的结果或输入确认。
func decodeCompletionMessage(t *testing.T, typ websocket.MessageType, data []byte, generation, offset, seq uint64) {
	t.Helper()
	var msg wsprotocol.CompletedMessage
	if typ != websocket.MessageText || json.Unmarshal(data, &msg) != nil || msg.Type != wsprotocol.MessageTypeCompleted ||
		msg.Generation != generation || msg.FinalOffset != offset || msg.LastSeq != seq {
		t.Error("unexpected completion type or range")
	}
}

func readCompletionNetwork(t *testing.T, ctx context.Context, client *websocket.Conn, generation, offset, seq uint64) {
	t.Helper()
	typ, data, err := client.Read(ctx)
	if err != nil {
		t.Fatal("read completion", err)
	}
	decodeCompletionMessage(t, typ, data, generation, offset, seq)
}

// finishCompletionWorker 只完成计算，故意不提交客户端确认。
func finishCompletionWorker(t *testing.T, f *workerCoordinatorFixture, feed chan workerReadStep, offset uint64) {
	t.Helper()
	a, n, err := f.session.requestEnd(context.Background(), 1, offset)
	assertCoordinatorInput(t, a, n, err, true, offset, nil)
	synctest.Wait()
	feed <- workerReadStep{err: io.EOF}
	assertWorkerRetaining(t, f)
}

func TestCompletionRejectsPrematureAndInvalidAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		checkNotReady := func() {
			t.Helper()
			if got, err := f.session.requestCompletion(context.Background(), 1); got != (completionSnapshot{}) || !errors.Is(err, errCompletionNotReady) {
				t.Fatal(got, err)
			}
			if err := f.session.requestCompletionAck(context.Background(), 1, 0, 1); !errors.Is(err, errCompletionNotReady) {
				t.Fatal(err)
			}
		}
		checkNotReady()
		feedDeliveryResult(t, feed, "segment final")
		checkNotReady()
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		checkNotReady()
		feed <- workerReadStep{err: io.EOF}
		assertWorkerRetaining(t, f)
		if _, err := f.session.requestCompletion(context.Background(), 1); !errors.Is(err, errCompletionNotReady) {
			t.Fatal(err)
		}
		takeDeliveryResult(t, f.session, 1, 1)
		ackDeliveryResult(t, f.session, 1, 1, true) // 早 ACK 不能清除实际在途写入。
		if _, err := f.session.requestCompletion(context.Background(), 1); !errors.Is(err, errCompletionNotReady) {
			t.Fatal(err)
		}
		writeDeliveryResult(t, f.session, 1, 1)
		if err := f.session.requestCompletionAck(context.Background(), 1, 0, 1); !errors.Is(err, errCompletionNotOffered) {
			t.Fatal(err)
		}
		got, err := f.session.requestCompletion(context.Background(), 1)
		if err != nil || got != (completionSnapshot{lastSeq: 1}) {
			t.Fatal(got, err)
		}
		for _, tc := range []struct {
			name                    string
			generation, offset, seq uint64
			want                    error
		}{
			{"wrong_generation", 2, 0, 1, errSessionGenerationMismatch},
			{"wrong_offset", 1, 1, 1, errCompletionMismatch},
			{"short_seq", 1, 0, 0, errCompletionMismatch},
			{"ahead_seq", 1, 0, 2, errCompletionMismatch},
		} {
			{
				if err := f.session.requestCompletionAck(context.Background(), tc.generation, tc.offset, tc.seq); !errors.Is(err, tc.want) {
					t.Fatal(tc.name, err)
				}
				synctest.Wait()
				if f.worker.completion.acknowledged || f.worker.results.ackedSeq != 1 || f.worker.completion.offeredGeneration != 1 {
					t.Fatal("invalid ACK changed completion state")
				}
				assertControlAlive(t, f.session)
			}
		}
		if err := f.session.requestCompletionAck(context.Background(), 1, 0, 1); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
		if !f.worker.completion.acknowledged {
			t.Fatal("ordinary ACK hid whole-session ACK")
		}
		if err := f.session.requestCompletionAck(context.Background(), 1, 0, 1); !errors.Is(err, errResumeClosed) {
			t.Fatal(err)
		}
	})
}

func TestCompletionAckCumulativelyConfirmsResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "tail")
		finishCompletionWorker(t, f, feed, 0)
		takeDeliveryResult(t, f.session, 1, 1)
		writeDeliveryResult(t, f.session, 1, 1)
		if _, err := f.session.requestCompletion(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		buffer := f.worker.results
		if buffer.ackedSeq != 0 || buffer.count != 1 {
			t.Fatal("write or offer released unconfirmed results")
		}
		for _, offset := range []uint64{1, 2} {
			if err := f.session.requestCompletionAck(context.Background(), 1, offset, 1); !errors.Is(err, errCompletionMismatch) {
				t.Fatal(err)
			}
			synctest.Wait()
			if buffer.ackedSeq != 0 || buffer.count != 1 || f.worker.completion.acknowledged {
				t.Fatal("invalid completion range partially confirmed results")
			}
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := f.session.requestCompletionAck(canceled, 1, 0, 1); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := f.session.requestCompletionAck(context.Background(), 1, 0, 1); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
		if !f.worker.completion.acknowledged || buffer.ackedSeq != 1 {
			t.Fatal("whole ACK did not confirm result prefix")
		}
	})
}

func TestCompletionRecoveryRequiresNewGenerationOffer(t *testing.T) {
	for _, applied := range []uint64{0, 1} {
		t.Run(map[uint64]string{0: "replay_results", 1: "already_applied"}[applied], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				feedDeliveryResult(t, feed, "tail")
				finishCompletionWorker(t, f, feed, 2)
				takeDeliveryResult(t, f.session, 1, 1)
				writeDeliveryResult(t, f.session, 1, 1)
				first, err := f.session.requestCompletion(context.Background(), 1)
				if err != nil {
					t.Fatal(err)
				}
				if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
					t.Fatal(d, err)
				}
				if err := f.session.requestCompletionAck(context.Background(), 1, 2, 1); !errors.Is(err, errSessionNotAttached) {
					t.Fatal(err)
				}
				generation, err := f.session.requestResume(context.Background(), applied)
				if err != nil || generation != 2 {
					t.Fatal(generation, err)
				}
				if err := f.session.requestCompletionAck(context.Background(), 1, 2, 1); !errors.Is(err, errSessionGenerationMismatch) {
					t.Fatal(err)
				}
				if err := f.session.requestCompletionAck(context.Background(), 2, 2, 1); !errors.Is(err, errCompletionNotOffered) {
					t.Fatal(err)
				}
				messages := make(chan []byte, 4)
				w := newTestGenerationWriter(t, f, 2, writerTestConn(func(_ context.Context, _ websocket.MessageType, data []byte) error {
					messages <- append([]byte(nil), data...)
					return nil
				}), time.Second)
				ctx, cancel := context.WithCancelCause(f.lifeCtx)
				run := startTestReadyWriter(t, w, ctx, cancel)
				run.assertExit(t, writerCompletionSent, 1, nil)
				assertReadyMessage(t, decodeReadyMessage(t, websocket.MessageText, <-messages), f, 2, 2, applied, true)
				if applied == 0 {
					if msg := decodeWriterMessage(t, websocket.MessageText, <-messages); msg.Seq != 1 {
						t.Fatal(msg.Seq)
					}
				}
				decodeCompletionMessage(t, websocket.MessageText, <-messages, 2, first.finalOffset, first.lastSeq)
				if err := f.session.requestCompletionAck(context.Background(), 2, 2, 1); err != nil {
					t.Fatal(err)
				}
				f.assertFinishedAtCurrentTime(t, nil)
			})
		})
	}
}

func TestCompletionFastAckWaitsForActualWriteAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		conn := &attachmentTestConn{writeFn: func(_ context.Context, typ websocket.MessageType, data []byte) error {
			var tag struct {
				Type wsprotocol.MessageType `json:"type"`
			}
			if err := json.Unmarshal(data, &tag); err != nil {
				t.Error(err)
			}
			if tag.Type == wsprotocol.MessageTypeCompleted {
				decodeCompletionMessage(t, typ, data, 1, 0, 0)
				close(entered)
				<-release
			}
			return nil
		}}
		startManagedFixture(t, f, conn)
		t.Cleanup(unblock)
		finishCompletionWorker(t, f, feed, 0)
		<-entered
		if err := f.session.requestCompletionAck(context.Background(), 1, 0, 0); err != nil {
			t.Fatal(err)
		}
		<-f.session.controlDone
		synctest.Wait()
		select {
		case <-f.finished:
			t.Fatal("runner returned before actual Write")
		default:
		}
		if f.worker.input == nil || f.worker.results == nil || conn.closes.Load() != 1 {
			t.Fatal("premature buffer release or missing socket close")
		}
		unblock()
		f.assertFinishedAtCurrentTime(t, nil)
		if !f.worker.completion.acknowledged || conn.closes.Load() != 1 {
			t.Fatal("late writer exit replaced confirmed completion")
		}
	})
}

func TestCompletionWriteFailureAllowsResume(t *testing.T) {
	for _, name := range []string{"transport", "timeout"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				finishCompletionWorker(t, f, feed, 0)
				cause := errors.New("completion write failed")
				entered := make(chan struct{})
				w := newTestGenerationWriter(t, f, 1, writerTestConn(func(ctx context.Context, typ websocket.MessageType, data []byte) error {
					var tag struct {
						Type wsprotocol.MessageType `json:"type"`
					}
					json.Unmarshal(data, &tag)
					if tag.Type != wsprotocol.MessageTypeCompleted {
						return nil
					}
					decodeCompletionMessage(t, typ, data, 1, 0, 0)
					close(entered)
					if name == "timeout" {
						<-ctx.Done()
						return context.Cause(ctx)
					}
					return cause
				}), time.Second)
				ctx, cancel := context.WithCancelCause(f.lifeCtx)
				run := startTestReadyWriter(t, w, ctx, cancel)
				<-entered
				want := error(cause)
				if name == "timeout" {
					time.Sleep(time.Second)
					want = ErrResultWriteTimeout
				}
				run.assertExit(t, writerWriteFailed, 0, want)
				assertControlAlive(t, f.session)
				if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
					t.Fatal(d, err)
				}
				if gen, err := f.session.requestResume(context.Background(), 0); gen != 2 || err != nil {
					t.Fatal(gen, err)
				}
				if err := f.session.requestCompletionAck(context.Background(), 2, 0, 0); !errors.Is(err, errCompletionNotOffered) {
					t.Fatal(err)
				}
				if got, err := f.session.requestCompletion(context.Background(), 2); got != (completionSnapshot{}) || err != nil {
					t.Fatal(got, err)
				}
				if err := f.session.requestCompletionAck(context.Background(), 2, 0, 0); err != nil {
					t.Fatal(err)
				}
				f.assertFinishedAtCurrentTime(t, nil)
			})
		})
	}
}

func TestCompletionOffersAndResumesDoNotExtendRetention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.resultRetentionTimeout = 9 * time.Second
		f.start()
		finishCompletionWorker(t, f, feed, 0)
		for gen := uint64(1); gen <= 3; gen++ {
			if _, err := f.session.requestCompletion(context.Background(), gen); err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * time.Second)
			if gen < 3 {
				if d, err := f.session.reportDetach(context.Background(), gen); !d || err != nil {
					t.Fatal(d, err)
				}
				if n, err := f.session.requestResume(context.Background(), 0); n != gen+1 || err != nil {
					t.Fatal(n, err)
				}
			}
		}
		f.assertFinishedAtCurrentTime(t, errResultRetentionExpired)
		if f.worker.completion.acknowledged {
			t.Fatal("unconfirmed delivery marked confirmed")
		}
		if err := f.session.requestCompletionAck(context.Background(), 3, 0, 0); !errors.Is(err, errResumeClosed) {
			t.Fatal(err)
		}
	})
}

func TestCompletionReaderUsesFixedGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		finishCompletionWorker(t, f, feed, 0)
		if _, err := f.session.requestCompletion(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		r := newTestConnectionReader(t, f, 1, scriptedReaderConn(readerTestStep{typ: websocket.MessageText, data: []byte(`{"type":"completed_ack","finalOffset":"0","lastSeq":"0","generation":"999"}`)}), 128)
		assertReaderExit(t, r.run(f.lifeCtx), readerStopped, errResumeClosed)
		f.assertFinishedAtCurrentTime(t, nil)
		if !f.worker.completion.acknowledged {
			t.Fatal("reader did not submit completion")
		}
	})
}

func TestCompletionFailuresNeverAnnounceSuccess(t *testing.T) {
	for _, name := range []string{"early_eof", "worker_error", "tail_timeout", "protocol_error"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.worker.config.tailTimeout = 3 * time.Second
				inputs := make(chan readerTestStep, 1)
				messages := make(chan []byte, 8)
				conn := &attachmentTestConn{
					readFn: func(ctx context.Context) (websocket.MessageType, []byte, error) {
						select {
						case step := <-inputs:
							return step.typ, step.data, step.err
						case <-ctx.Done():
							return 0, nil, context.Cause(ctx)
						}
					},
					writeFn: func(_ context.Context, _ websocket.MessageType, data []byte) error {
						messages <- append([]byte(nil), data...)
						return nil
					},
				}
				startManagedFixture(t, f, conn)
				synctest.Wait()
				want := error(errWorkerEndedEarly)
				switch name {
				case "early_eof":
					feed <- workerReadStep{err: io.EOF}
				case "worker_error":
					want = errors.New("worker rejected request")
					feed <- workerReadStep{err: want}
				case "tail_timeout":
					a, n, err := f.session.requestEnd(context.Background(), 1, 0)
					assertCoordinatorInput(t, a, n, err, true, 0, nil)
					time.Sleep(3 * time.Second)
					want = ErrTailTimeout
				case "protocol_error":
					inputs <- readerTestStep{typ: websocket.MessageText, data: []byte(`{"type":"completed_ack","finalOffset":"0"}`)}
					want = wsprotocol.ErrInvalidV2Input
				}
				f.assertFinishedAtCurrentTime(t, want)
				close(messages)
				for data := range messages {
					var tag struct {
						Type wsprotocol.MessageType `json:"type"`
					}
					if err := json.Unmarshal(data, &tag); err != nil {
						t.Fatal(err)
					}
					if tag.Type == wsprotocol.MessageTypeCompleted {
						t.Fatal("failure announced normal completion")
					}
				}
				if f.worker.completion.acknowledged || conn.closes.Load() != 1 {
					t.Fatal("failure outcome or cleanup incorrect")
				}
			})
		})
	}
}

func TestCompletionWebSocketAckAndLostDelivery(t *testing.T) {
	for _, name := range []string{"direct_ack", "notice_lost", "ack_lost", "empty"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
			server := newManagedNetworkConn(legacy.ws)
			f, feed := newDuplexWorkerFixture(t, nil, nil)
			startManagedFixture(t, f, server)
			waitManagedRead(t, ctx, server, 1)
			readConnectionReadyNetwork(t, ctx, client, f, 1, 0, 0, false)
			writeManagedInput(t, ctx, client, server, websocket.MessageText, []byte(`{"type":"end","finalOffset":"0"}`), 2)
			readAudioAckNetwork(t, ctx, client, 1, 0, true)
			lastSeq := uint64(0)
			if name != "empty" {
				lastSeq = 1
				sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "tail", IsFinal: true}})
				readSequencedNetworkResult(t, ctx, client, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1, SegmentID: "s", Text: "tail", IsFinal: true})
			}
			sendWriterNetworkStep(t, ctx, feed, workerReadStep{err: io.EOF})
			generation := uint64(1)
			if name != "notice_lost" {
				readCompletionNetwork(t, ctx, client, 1, 0, lastSeq)
			}
			if name == "notice_lost" || name == "ack_lost" {
				if err := client.CloseNow(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-server.closed:
				case <-ctx.Done():
					t.Fatal("lost-delivery connection not closed")
				}
				legacy2, client2, _ := newLegacyResultWriteFixture(t, time.Second, false)
				server2 := newManagedNetworkConn(legacy2.ws)
				generation = resumeManagedNetwork(t, ctx, f.session, server2, lastSeq)
				if generation != 2 {
					t.Fatal(generation)
				}
				waitManagedRead(t, ctx, server2, 1)
				readConnectionReadyNetwork(t, ctx, client2, f, 2, 0, lastSeq, true)
				readCompletionNetwork(t, ctx, client2, 2, 0, lastSeq)
				client, server = client2, server2
			}
			data, err := json.Marshal(wsprotocol.CompletedAckMessage{Type: wsprotocol.MessageTypeCompletedAck, LastSeq: lastSeq})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Write(ctx, websocket.MessageText, data); err != nil {
				t.Fatal(err)
			}
			select {
			case <-f.finished:
			case <-ctx.Done():
				t.Fatal("completion ACK did not finish runner")
			}
			if f.err != nil || !f.worker.completion.acknowledged || f.worker.input != nil || f.worker.results != nil || server.closes.Load() != 1 {
				t.Fatal("completion ACK cleanup incorrect", f.err)
			}
			if err := f.session.requestCompletionAck(ctx, generation, 0, lastSeq); !errors.Is(err, errResumeClosed) {
				t.Fatal(err)
			}
			if server.closes.Load() != 1 {
				t.Fatal("duplicate ACK closed socket twice")
			}
		})
	}
}
