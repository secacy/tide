package gateway

import (
	"context"
	"errors"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// feedDeliveryResult 经过实际 Recv 与协调者保存结果；Wait 只用于测试同步。
func feedDeliveryResult(t *testing.T, feed chan workerReadStep, text string) {
	t.Helper()
	feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: text, IsFinal: true}}
	synctest.Wait()
}

func takeDeliveryResult(t *testing.T, s *resumableSession, generation, seq uint64) resultOffer {
	t.Helper()
	offer, err := s.requestResult(context.Background(), generation)
	if err != nil || !offer.available || offer.result.seq != seq || offer.changed == nil {
		t.Fatalf("take = (%+v,%v), want seq %d", offer, err, seq)
	}
	return offer
}

func writeDeliveryResult(t *testing.T, s *resumableSession, generation, seq uint64) {
	t.Helper()
	if handled, err := s.reportResultWritten(context.Background(), generation, seq); !handled || err != nil {
		t.Fatalf("written %d = (%v,%v)", seq, handled, err)
	}
}

func ackDeliveryResult(t *testing.T, s *resumableSession, generation, seq uint64, advanced bool) {
	t.Helper()
	if got, err := s.requestResultAck(context.Background(), generation, seq); got != advanced || err != nil {
		t.Fatalf("ack %d = (%v,%v), want advanced=%v", seq, got, err, advanced)
	}
}

func assertDeliveryNotification(t *testing.T, changed <-chan struct{}, closed bool) {
	t.Helper()
	if changed == nil {
		t.Fatal("missing result notification")
	}
	select {
	case <-changed:
		if !closed {
			t.Fatal("notification closed without a relevant committed change")
		}
	default:
		if closed {
			t.Fatal("committed change did not wake the observer")
		}
	}
}

func closeDeliveryFixture(t *testing.T, f *workerCoordinatorFixture) {
	t.Helper()
	if err := f.session.requestClose(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.assertFinishedAtCurrentTime(t, nil)
}

func TestSessionResultsAuthorizeBeforeAckAndWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "first")
		feedDeliveryResult(t, feed, "second")
		if advanced, err := f.session.requestResultAck(context.Background(), 1, 1); advanced || !errors.Is(err, errResultAckAhead) {
			t.Fatalf("unoffered ACK = (%v,%v)", advanced, err)
		}
		first := takeDeliveryResult(t, f.session, 1, 1)
		if first.lastSeq != 2 || first.ackedSeq != 0 || first.workerCompleted {
			t.Fatalf("incorrect running snapshot: %+v", first)
		}
		if advanced, err := f.session.requestResultAck(context.Background(), 1, 2); advanced || !errors.Is(err, errResultAckAhead) {
			t.Fatalf("saved but not offered ACK = (%v,%v)", advanced, err)
		}
		ackDeliveryResult(t, f.session, 1, 1, true) // 尚未报告 Write 成功。
		if offer, err := f.session.requestResult(context.Background(), 1); offer != (resultOffer{}) || !errors.Is(err, errResultWriteInFlight) {
			t.Fatalf("ACK incorrectly cleared in-flight write: (%+v,%v)", offer, err)
		}
		writeDeliveryResult(t, f.session, 1, 1)
		second := takeDeliveryResult(t, f.session, 1, 2)
		if second.ackedSeq != 1 || first.result.text != "first" {
			t.Fatal("ACK position or borrowed immutable result changed")
		}
		writeDeliveryResult(t, f.session, 1, 2)
		empty, err := f.session.requestResult(context.Background(), 1)
		if err != nil || empty.available || empty.ackedSeq != 1 || empty.lastSeq != 2 {
			t.Fatalf("Write success released or replayed unacknowledged data: (%+v,%v)", empty, err)
		}
		ackDeliveryResult(t, f.session, 1, 2, true)
		for _, seq := range []uint64{2, 1, 0} {
			ackDeliveryResult(t, f.session, 1, seq, false)
		}
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsSingleConcurrentLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "one")
		feedDeliveryResult(t, feed, "two")
		var wg sync.WaitGroup
		var winners, rejected atomic.Int32
		for range 32 {
			wg.Go(func() {
				offer, err := f.session.requestResult(context.Background(), 1)
				switch {
				case err == nil && offer.available && offer.result.seq == 1:
					winners.Add(1)
				case errors.Is(err, errResultWriteInFlight) && offer == (resultOffer{}):
					rejected.Add(1)
				default:
					t.Errorf("concurrent lease = (%+v,%v)", offer, err)
				}
			})
		}
		wg.Wait()
		if winners.Load() != 1 || rejected.Load() != 31 {
			t.Fatalf("lease winners/rejections = %d/%d", winners.Load(), rejected.Load())
		}
		writeDeliveryResult(t, f.session, 1, 1)
		takeDeliveryResult(t, f.session, 1, 2)
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsWriteMismatchKeepsLease(t *testing.T) {
	for _, seq := range []uint64{0, 2, math.MaxUint64} {
		t.Run(strconv.FormatUint(seq, 10), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				feedDeliveryResult(t, feed, "one")
				offer := takeDeliveryResult(t, f.session, 1, 1)
				if handled, err := f.session.reportResultWritten(context.Background(), 1, seq); handled || !errors.Is(err, errResultWriteMismatch) {
					t.Fatalf("mismatched write = (%v,%v)", handled, err)
				}
				assertDeliveryNotification(t, offer.changed, false)
				if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResultWriteInFlight) {
					t.Fatalf("mismatch destroyed lease: %v", err)
				}
				writeDeliveryResult(t, f.session, 1, 1)
				assertDeliveryNotification(t, offer.changed, true)
				if handled, err := f.session.reportResultWritten(context.Background(), 1, 1); handled || !errors.Is(err, errResultWriteMismatch) {
					t.Fatalf("duplicate write = (%v,%v)", handled, err)
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestSessionResultsOldGenerationCannotAffectNewLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "one")
		old := takeDeliveryResult(t, f.session, 1, 1)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatalf("detach = (%v,%v)", d, err)
		}
		assertDeliveryNotification(t, old.changed, true)
		if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errSessionNotAttached) {
			t.Fatal(err)
		}
		if _, err := f.session.requestResultAck(context.Background(), 1, 0); !errors.Is(err, errSessionNotAttached) {
			t.Fatal(err)
		}
		if handled, err := f.session.reportResultWritten(context.Background(), 1, 1); handled || err != nil {
			t.Fatalf("detached write callback = (%v,%v)", handled, err)
		}
		if generation, err := f.session.requestResume(context.Background(), 0); generation != 2 || err != nil {
			t.Fatalf("resume = (%d,%v)", generation, err)
		}
		current := takeDeliveryResult(t, f.session, 2, 1)
		if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errSessionGenerationMismatch) {
			t.Fatal(err)
		}
		if _, err := f.session.requestResultAck(context.Background(), 1, 0); !errors.Is(err, errSessionGenerationMismatch) {
			t.Fatalf("old idempotent ACK bypassed generation check: %v", err)
		}
		if handled, err := f.session.reportResultWritten(context.Background(), 1, 1); handled || err != nil {
			t.Fatalf("stale write callback = (%v,%v)", handled, err)
		}
		if d, err := f.session.reportDetach(context.Background(), 1); d || err != nil {
			t.Fatalf("stale detach = (%v,%v)", d, err)
		}
		assertDeliveryNotification(t, current.changed, false)
		if _, err := f.session.requestResult(context.Background(), 2); !errors.Is(err, errResultWriteInFlight) {
			t.Fatalf("old callback cleared new lease: %v", err)
		}
		writeDeliveryResult(t, f.session, 2, 1)
		ackDeliveryResult(t, f.session, 2, 1, true)
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsResumeAppliesLostAck(t *testing.T) {
	for _, applied := range []uint64{0, 1, 2} {
		t.Run(strconv.FormatUint(applied, 10), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				for _, text := range []string{"one", "two", "three"} {
					feedDeliveryResult(t, feed, text)
				}
				for seq := uint64(1); seq <= 2; seq++ {
					takeDeliveryResult(t, f.session, 1, seq)
					if seq == 1 {
						writeDeliveryResult(t, f.session, 1, seq)
					}
				}
				// 第二条尚未报告写成功；恢复位置仍可覆盖此前的授权。
				if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
					t.Fatalf("detach = (%v,%v)", d, err)
				}
				if g, err := f.session.requestResume(context.Background(), applied); g != 2 || err != nil {
					t.Fatalf("resume applied %d = (%d,%v)", applied, g, err)
				}
				offer := takeDeliveryResult(t, f.session, 2, applied+1)
				if offer.ackedSeq != applied || offer.lastSeq != 3 {
					t.Fatalf("resume ACK not committed with cursor: %+v", offer)
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestSessionResultsAckCanSkipReplayWithoutCursorRegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		for _, text := range []string{"one", "two", "three", "four"} {
			feedDeliveryResult(t, feed, text)
		}
		for seq := uint64(1); seq <= 3; seq++ {
			takeDeliveryResult(t, f.session, 1, seq)
			writeDeliveryResult(t, f.session, 1, seq)
		}
		ackDeliveryResult(t, f.session, 1, 1, true)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(err)
		}
		if g, err := f.session.requestResume(context.Background(), 1); g != 2 || err != nil {
			t.Fatalf("resume = (%d,%v)", g, err)
		}
		replay := takeDeliveryResult(t, f.session, 2, 2)
		ackDeliveryResult(t, f.session, 2, 3, true) // 旧代已经授权 3，但新代尚在重放 2。
		if _, err := f.session.requestResult(context.Background(), 2); !errors.Is(err, errResultWriteInFlight) {
			t.Fatalf("crossing ACK cleared replay write: %v", err)
		}
		writeDeliveryResult(t, f.session, 2, 2)
		next := takeDeliveryResult(t, f.session, 2, 4)
		if next.ackedSeq != 3 || replay.result.text != "two" {
			t.Fatal("late Write regressed cursor or released borrowed value")
		}
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsInvalidResumeIsAtomicAndDoesNotRenew(t *testing.T) {
	for _, name := range []string{"below_cleaned", "above_offered", "generation_exhausted"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				if name == "generation_exhausted" {
					f.session.resume.generation = math.MaxUint64
				}
				generation := f.session.resume.generation
				f.start()
				feedDeliveryResult(t, feed, "one")
				feedDeliveryResult(t, feed, "two")
				takeDeliveryResult(t, f.session, generation, 1)
				writeDeliveryResult(t, f.session, generation, 1)
				ackDeliveryResult(t, f.session, generation, 1, true)
				if d, err := f.session.reportDetach(context.Background(), generation); !d || err != nil {
					t.Fatalf("detach = (%v,%v)", d, err)
				}
				synctest.Wait()
				deadline, delivery := f.session.resume.expiresAt, f.worker.delivery
				applied, want := uint64(0), errResultReplayGap
				if name == "above_offered" {
					applied, want = 2, errResultAckAhead
				} else if name == "generation_exhausted" {
					applied, want = 1, errResumeGenerationExhausted
				}
				time.Sleep(3 * time.Second)
				if g, err := f.session.requestResume(context.Background(), applied); g != 0 || !errors.Is(err, want) {
					t.Fatalf("invalid resume = (%d,%v), want %v", g, err, want)
				}
				synctest.Wait()
				if f.session.resume.phase != resumeDetached || f.session.resume.generation != generation ||
					f.session.resume.expiresAt != deadline || f.worker.delivery != delivery ||
					f.worker.results.ackedSeq != 1 || f.worker.results.count != 1 || f.worker.results.lastSeq != 2 {
					t.Fatal("rejected resume changed state, confirmation or deadline")
				}
				assertDeliveryNotification(t, delivery.changed, false)
				// 发送无效果命令建立读取到后续协调清理的同步边界。
				if d, err := f.session.reportDetach(context.Background(), 0); d || err != nil {
					t.Fatal(err)
				}
				time.Sleep(7 * time.Second)
				f.assertFinishedAtCurrentTime(t, errResumeExpired)
				assertDeliveryNotification(t, delivery.changed, true)
			})
		})
	}
}

func TestSessionResultsAttachedResumeDoesNotApplyAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "one")
		offer := takeDeliveryResult(t, f.session, 1, 1)
		if g, err := f.session.requestResume(context.Background(), 1); g != 0 || !errors.Is(err, errResumeAlreadyAttached) {
			t.Fatalf("attached resume = (%d,%v)", g, err)
		}
		assertDeliveryNotification(t, offer.changed, false)
		ackDeliveryResult(t, f.session, 1, 1, true) // 若失败恢复已经应用，则这里会返回 false。
		if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResultWriteInFlight) {
			t.Fatalf("failed resume reset lease: %v", err)
		}
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsAckReusesBothBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		buffer, err := newResultBuffer(2, 1) // 一个 segment 字节 + 一个 text 字节。
		if err != nil {
			t.Fatal(err)
		}
		f.worker.results = buffer
		f.start()
		feedDeliveryResult(t, feed, "a")
		borrowed := takeDeliveryResult(t, f.session, 1, 1)
		ackDeliveryResult(t, f.session, 1, 1, true)
		feedDeliveryResult(t, feed, "b") // receiver 使用 ACK 释放出的同一个槽位。
		writeDeliveryResult(t, f.session, 1, 1)
		second := takeDeliveryResult(t, f.session, 1, 2)
		if borrowed.result.text != "a" || second.result.text != "b" || second.ackedSeq != 1 {
			t.Fatal("slot reuse altered borrowed result or lost sequence")
		}
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsNotificationRotation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		empty, err := f.session.requestResult(context.Background(), 1)
		if err != nil || empty.available || empty.workerCompleted || empty.lastSeq != 0 || empty.ackedSeq != 0 {
			t.Fatalf("initial query = (%+v,%v)", empty, err)
		}
		assertDeliveryNotification(t, empty.changed, false)
		// 查询完成后先追加，再等待旧通知，仍然能够观察到关闭。
		feedDeliveryResult(t, feed, "one")
		assertDeliveryNotification(t, empty.changed, true)
		offer := takeDeliveryResult(t, f.session, 1, 1)
		if offer.changed == empty.changed {
			t.Fatal("notification not replaced")
		}
		assertDeliveryNotification(t, offer.changed, false)
		ackDeliveryResult(t, f.session, 1, 1, true)
		assertDeliveryNotification(t, offer.changed, true)
		writeDeliveryResult(t, f.session, 1, 1)
		empty, err = f.session.requestResult(context.Background(), 1)
		if err != nil || empty.available {
			t.Fatal(err)
		}
		ackDeliveryResult(t, f.session, 1, 1, false)
		if _, err := f.session.requestResultAck(context.Background(), 1, 2); !errors.Is(err, errResultAckAhead) {
			t.Fatal(err)
		}
		assertDeliveryNotification(t, empty.changed, false)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(err)
		}
		assertDeliveryNotification(t, empty.changed, true)
		synctest.Wait()
		detachedChanged := f.worker.delivery.changed
		if g, err := f.session.requestResume(context.Background(), 1); g != 2 || err != nil {
			t.Fatalf("resume = (%d,%v)", g, err)
		}
		assertDeliveryNotification(t, detachedChanged, true)
		empty, err = f.session.requestResult(context.Background(), 2)
		if err != nil || empty.available || empty.ackedSeq != 1 {
			t.Fatalf("new query = (%+v,%v)", empty, err)
		}
		assertDeliveryNotification(t, empty.changed, false)
		closeDeliveryFixture(t, f)
		assertDeliveryNotification(t, empty.changed, true)
	})
}

func TestSessionResultsAllAckedStillRetainsUntilFixedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.resultRetentionTimeout = 3 * time.Second
		f.start()
		feedDeliveryResult(t, feed, "one")
		takeDeliveryResult(t, f.session, 1, 1)
		writeDeliveryResult(t, f.session, 1, 1)
		ackDeliveryResult(t, f.session, 1, 1, true)
		before, err := f.session.requestResult(context.Background(), 1)
		if err != nil || before.available || before.workerCompleted {
			t.Fatalf("running empty = (%+v,%v)", before, err)
		}
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		assertWorkerRetaining(t, f)
		assertDeliveryNotification(t, before.changed, true)
		completed, err := f.session.requestResult(context.Background(), 1)
		if err != nil || completed.available || !completed.workerCompleted || completed.ackedSeq != 1 || completed.lastSeq != 1 {
			t.Fatalf("completed snapshot = (%+v,%v)", completed, err)
		}
		time.Sleep(time.Second)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(err)
		}
		if g, err := f.session.requestResume(context.Background(), 1); g != 2 || err != nil {
			t.Fatalf("all-ACKed session lost recovery eligibility: (%d,%v)", g, err)
		}
		ackDeliveryResult(t, f.session, 2, 1, false)
		time.Sleep(2 * time.Second)
		f.assertFinishedAtCurrentTime(t, errResultRetentionExpired)
	})
}

func TestSessionResultsDeliveryDuringTailAndRetention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		f, feed := newDuplexWorkerFixture(t, nil, func(ctx context.Context) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		})
		f.start()
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		<-entered
		feedDeliveryResult(t, feed, "tail")
		tail := takeDeliveryResult(t, f.session, 1, 1)
		if tail.workerCompleted {
			t.Fatal("blocked half-close marked Worker completed")
		}
		ackDeliveryResult(t, f.session, 1, 1, true)
		writeDeliveryResult(t, f.session, 1, 1)
		feedDeliveryResult(t, feed, "final")
		close(release)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		assertWorkerRetaining(t, f)
		final := takeDeliveryResult(t, f.session, 1, 2)
		if !final.workerCompleted || final.ackedSeq != 1 {
			t.Fatalf("retaining offer = %+v", final)
		}
		ackDeliveryResult(t, f.session, 1, 2, true)
		writeDeliveryResult(t, f.session, 1, 2)
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsPureControlRejectsWorkerOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		startTestSessionControl(t, s, time.Now)
		if offer, err := s.requestResult(context.Background(), 1); offer != (resultOffer{}) || !errors.Is(err, errSessionWorkerUnavailable) {
			t.Fatalf("take without Worker = (%+v,%v)", offer, err)
		}
		if handled, err := s.reportResultWritten(context.Background(), 1, 1); handled || !errors.Is(err, errSessionWorkerUnavailable) {
			t.Fatalf("written without Worker = (%v,%v)", handled, err)
		}
		if advanced, err := s.requestResultAck(context.Background(), 1, 1); advanced || !errors.Is(err, errSessionWorkerUnavailable) {
			t.Fatalf("ACK without Worker = (%v,%v)", advanced, err)
		}
		if d, err := s.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(err)
		}
		if g, err := s.requestResume(context.Background(), 1); g != 0 || !errors.Is(err, errSessionWorkerUnavailable) {
			t.Fatalf("nonzero position without Worker = (%d,%v)", g, err)
		}
		if g, err := s.requestResume(context.Background(), 0); g != 2 || err != nil {
			t.Fatalf("legacy control resume = (%d,%v)", g, err)
		}
	})
}

func TestSessionResultsPreCanceledOperationsDoNotMutate(t *testing.T) {
	for _, name := range []string{"take", "written", "ack", "resume"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				feedDeliveryResult(t, feed, "one")
				if name != "take" {
					takeDeliveryResult(t, f.session, 1, 1)
				}
				if name == "resume" {
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				before, deadline := f.worker.delivery, f.session.resume.expiresAt
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				var err error
				switch name {
				case "take":
					_, err = f.session.requestResult(ctx, 1)
				case "written":
					_, err = f.session.reportResultWritten(ctx, 1, 1)
				case "ack":
					_, err = f.session.requestResultAck(ctx, 1, 1)
				case "resume":
					_, err = f.session.requestResume(ctx, 1)
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("pre-canceled %s = %v", name, err)
				}
				synctest.Wait()
				if f.worker.delivery != before || f.worker.results.ackedSeq != 0 || f.session.resume.generation != 1 || f.session.resume.expiresAt != deadline {
					t.Fatal("pre-canceled operation changed state")
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestSessionResultsDeliveredTakeKeepsOwnershipAfterRequestCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		feedDeliveryResult(t, feed, "one")
		requestCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		var checks atomic.Int32
		ctx := &controlCheckedContext{Context: requestCtx, check: func() error {
			err := requestCtx.Err()
			if checks.Add(1) == 2 {
				close(entered)
				<-release
			}
			return err
		}}
		reply := make(chan sessionControlResult, 1)
		go func() {
			reply <- f.session.submitCommand(ctx, sessionControlCommand{kind: controlTakeResult, generation: 1})
		}()
		<-entered
		cancel()
		synctest.Wait()
		select {
		case got := <-reply:
			t.Fatalf("delivered request abandoned reply: %+v", got)
		default:
		}
		unblock()
		got := <-reply
		if got.err != nil || !got.offer.available || got.offer.result.seq != 1 {
			t.Fatalf("committed authorization lost: %+v", got)
		}
		writeDeliveryResult(t, f.session, 1, 1) // 用有效请求 ctx 完成已接管授权。
		closeDeliveryFixture(t, f)
	})
}

func TestSessionResultsMaximumSequenceDoesNotWrap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		// 构造合法的已确认历史，仅余一个可分配序号。
		f.worker.results.lastSeq = math.MaxUint64 - 1
		f.worker.results.ackedSeq = math.MaxUint64 - 1
		f.worker.delivery.offeredSeq = math.MaxUint64 - 1
		f.worker.delivery.cursor = math.MaxUint64 - 1
		f.start()
		feedDeliveryResult(t, feed, "last")
		takeDeliveryResult(t, f.session, 1, math.MaxUint64)
		ackDeliveryResult(t, f.session, 1, math.MaxUint64, true)
		writeDeliveryResult(t, f.session, 1, math.MaxUint64)
		empty, err := f.session.requestResult(context.Background(), 1)
		if err != nil || empty.available || empty.lastSeq != math.MaxUint64 || empty.ackedSeq != math.MaxUint64 {
			t.Fatalf("maximum sequence query = (%+v,%v)", empty, err)
		}
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(err)
		}
		if g, err := f.session.requestResume(context.Background(), math.MaxUint64); g != 2 || err != nil {
			t.Fatalf("maximum position resume = (%d,%v)", g, err)
		}
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Text: "overflow"}}
		f.assertFinishedAtCurrentTime(t, errResultSequenceExhausted)
	})
}

func TestSessionResultsLifecycleExitWakesObserver(t *testing.T) {
	for _, name := range []string{"worker_error", "life_cancel", "recovery_expiry"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				empty, err := f.session.requestResult(context.Background(), 1)
				if err != nil || empty.available {
					t.Fatal(err)
				}
				want := errors.New("delivery lifecycle stopped")
				switch name {
				case "worker_error":
					feed <- workerReadStep{err: want}
				case "life_cancel":
					f.cancelLife(want)
				case "recovery_expiry":
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatal(err)
					}
					// detach 已轮换通知；最终退出还须关闭新的当前通道。
					synctest.Wait()
					empty.changed = f.worker.delivery.changed
					if d, err := f.session.reportDetach(context.Background(), 0); d || err != nil {
						t.Fatal(err)
					}
					time.Sleep(10 * time.Second)
					want = errResumeExpired
				}
				f.assertFinishedAtCurrentTime(t, want)
				assertDeliveryNotification(t, empty.changed, true)
				if _, err := f.session.requestResult(context.Background(), 1); !errors.Is(err, errResumeClosed) {
					t.Fatalf("query after exit = %v", err)
				}
			})
		})
	}
}

func TestSessionResultsWaitingWorkerStatusStillAllowsDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, func(context.Context) error { return io.EOF })
		f.start()
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		synctest.Wait()
		feedDeliveryResult(t, feed, "before-status")
		offer := takeDeliveryResult(t, f.session, 1, 1)
		if offer.workerCompleted {
			t.Fatal("upload EOF mistaken for successful Worker completion")
		}
		ackDeliveryResult(t, f.session, 1, 1, true)
		writeDeliveryResult(t, f.session, 1, 1)
		want := errors.New("authoritative terminal status")
		feed <- workerReadStep{err: want}
		f.assertFinishedAtCurrentTime(t, want)
	})
}

func TestSessionResultsSessionsKeepIndependentAuthorization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, feedA := newDuplexWorkerFixture(t, nil, nil)
		b, feedB := newDuplexWorkerFixture(t, nil, nil)
		a.start()
		b.start()
		feedDeliveryResult(t, feedA, "session-a")
		feedDeliveryResult(t, feedB, "session-b")
		offerA := takeDeliveryResult(t, a.session, 1, 1)
		if advanced, err := b.session.requestResultAck(context.Background(), 1, 1); advanced || !errors.Is(err, errResultAckAhead) {
			t.Fatalf("other session's authorization accepted: (%v,%v)", advanced, err)
		}
		offerB := takeDeliveryResult(t, b.session, 1, 1)
		if offerA.changed == offerB.changed || offerA.result.text != "session-a" || offerB.result.text != "session-b" {
			t.Fatal("sessions shared notification or result data")
		}
		ackDeliveryResult(t, a.session, 1, 1, true)
		assertDeliveryNotification(t, offerB.changed, false)
		closeDeliveryFixture(t, a)
		writeDeliveryResult(t, b.session, 1, 1)
		ackDeliveryResult(t, b.session, 1, 1, true)
		closeDeliveryFixture(t, b)
	})
}
