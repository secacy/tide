package gateway

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func assertReadySnapshot(t *testing.T, got connectionReady, f *workerCoordinatorFixture, generation, offset, acked uint64, ended bool) {
	t.Helper()
	if got.sessionID != f.session.identity.id || got.resumeToken != f.session.identity.resumeToken ||
		got.generation != generation || got.nextOffset != offset || got.inputEnded != ended || got.ackedResultSeq != acked {
		t.Fatalf("ready fields mismatch: generation=%d offset=%d ended=%t acked=%d (identity redacted)", got.generation, got.nextOffset, got.inputEnded, got.ackedResultSeq)
	}
}

// StateProgression runs real audio/result commands and preserves an old snapshot
// across end, ACK, detach, and resume to check its value semantics.
func TestSessionReadyStateProgression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.start()
		initial, err := f.session.requestConnectionReady(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		assertReadySnapshot(t, initial, f, 1, 0, 0, false)
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		feedDeliveryResult(t, feed, "first")
		takeDeliveryResult(t, f.session, 1, 1)
		synctest.Wait()
		beforeDelivery, beforeResume := f.worker.delivery, *f.session.resume
		beforeCount, beforeBytes := f.worker.results.count, f.worker.results.retainedBytes
		before, err := f.session.requestConnectionReady(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		assertReadySnapshot(t, before, f, 1, 2, 0, false)
		synctest.Wait()
		if f.worker.delivery != beforeDelivery || *f.session.resume != beforeResume || f.worker.results.count != beforeCount || f.worker.results.retainedBytes != beforeBytes {
			t.Fatal("ready query changed result authorization, retention, or resume state")
		}
		writeDeliveryResult(t, f.session, 1, 1)
		ackDeliveryResult(t, f.session, 1, 1, true)
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		assertWorkerRetaining(t, f)
		ended, err := f.session.requestConnectionReady(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		assertReadySnapshot(t, ended, f, 1, 2, 1, true)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(d, err)
		}
		if g, err := f.session.requestResume(context.Background(), 1); g != 2 || err != nil {
			t.Fatal(g, err)
		}
		resumed, err := f.session.requestConnectionReady(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		assertReadySnapshot(t, resumed, f, 2, 2, 1, true)
		assertReadySnapshot(t, initial, f, 1, 0, 0, false)
		assertReadySnapshot(t, before, f, 1, 2, 0, false)
		closeDeliveryFixture(t, f)
	})
}

func TestSessionReadyRejectsInvalidRequests(t *testing.T) {
	for _, name := range []string{"no_worker", "detached", "stale_generation", "zero_generation", "precanceled", "closed", "expired"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				if name == "no_worker" {
					startTestSessionControl(t, f.session, time.Now)
				} else {
					f.start()
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				generation, want := uint64(1), errSessionWorkerUnavailable
				switch name {
				case "detached", "expired":
					if d, err := f.session.reportDetach(ctx, 1); !d || err != nil {
						t.Fatal(d, err)
					}
					want = errSessionNotAttached
					if name == "expired" {
						time.Sleep(f.session.resume.window)
						f.assertFinishedAtCurrentTime(t, errResumeExpired)
						want = errResumeClosed
					}
				case "stale_generation":
					generation, want = 2, errSessionGenerationMismatch
				case "zero_generation":
					generation, want = 0, errSessionGenerationMismatch
				case "precanceled":
					cancel()
					want = context.Canceled
				case "closed":
					closeDeliveryFixture(t, f)
					want = errResumeClosed
				}
				synctest.Wait()
				before := *f.session.resume
				got, err := f.session.requestConnectionReady(ctx, generation)
				if got != (connectionReady{}) || !errors.Is(err, want) {
					t.Fatal("invalid ready request returned data or wrong error", err, want)
				}
				synctest.Wait()
				if *f.session.resume != before {
					t.Fatal("rejected ready changed attachment or deadline")
				}
				if name != "closed" && name != "expired" && name != "no_worker" {
					closeDeliveryFixture(t, f)
				}
			})
		})
	}
}
