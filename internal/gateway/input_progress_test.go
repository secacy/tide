package gateway

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

func TestInputProgressConfig(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value, want time.Duration
	}{
		{"default", 0, 30 * time.Second},
		{"explicit", 5 * time.Second, 5 * time.Second},
		{"negative", -time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &V2Config{InputProgressTimeout: tc.value}
			got, err := normalizeV2Config(input, 128)
			if tc.value < 0 {
				if got != nil || err == nil {
					t.Fatal(got, err)
				}
				return
			}
			if err != nil || got.InputProgressTimeout != tc.want || input.InputProgressTimeout != tc.value {
				t.Fatal("normalization or caller config changed", got, err)
			}
		})
	}
	for _, value := range []time.Duration{0, -time.Second} {
		t.Run(value.String(), func(t *testing.T) {
			f, _ := newDuplexWorkerFixture(t, nil, nil)
			cfg := f.worker.config
			cfg.inputProgressTimeout = value
			if w, err := newSessionWorker(cfg); w != nil || !errors.Is(err, errInvalidSessionWorkerConfig) {
				t.Fatal(w, err)
			}
		})
	}
}

// progressBarrier 把测试对协调状态的读取与后续迁移建立同步；不刷新期限。
func progressBarrier(t *testing.T, f *workerCoordinatorFixture) {
	t.Helper()
	if d, err := f.session.reportDetach(context.Background(), 0); d || err != nil {
		t.Fatal("barrier", d, err)
	}
}

func TestInputProgressIdleAndFreshResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.inputProgressTimeout = 5 * time.Second // 启动前配置。
		stopped := make(chan error, 1)
		conn := &attachmentTestConn{readFn: func(ctx context.Context) (websocket.MessageType, []byte, error) {
			<-ctx.Done()
			cause := context.Cause(ctx)
			stopped <- cause
			return 0, nil, cause
		}}
		start := time.Now()
		startManagedFixture(t, f, conn)
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		assertResumeState(t, f.session.resume, resumeDetached, 1, start.Add(15*time.Second))
		if conn.closes.Load() != 1 || context.Cause(f.rpcCtx) != nil {
			t.Fatal("input timeout did not close only the attachment")
		}
		if cause := <-stopped; !errors.Is(cause, ErrInputProgressTimeout) {
			t.Fatal("attachment has incorrect timeout cause", cause)
		}
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 0, errSessionNotAttached)
		time.Sleep(2 * time.Second)
		if g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, &attachmentTestConn{}), 0); g != 2 || err != nil {
			t.Fatal(g, err)
		}
		time.Sleep(4 * time.Second)
		if _, err := f.session.requestConnectionReady(context.Background(), 2); err != nil {
			t.Fatal("fresh resume did not receive its own input budget", err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		assertResumeState(t, f.session.resume, resumeDetached, 2, start.Add(22*time.Second))
		progressBarrier(t, f)
		time.Sleep(10 * time.Second)
		f.assertFinishedAtCurrentTime(t, errResumeExpired)
	})
}

func TestInputProgressOnlyNewAudioRefreshes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.inputProgressTimeout = 5 * time.Second
		start := time.Now()
		startManagedFixture(t, f, &attachmentTestConn{})
		synctest.Wait()
		time.Sleep(4 * time.Second)
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		synctest.Wait()
		time.Sleep(4 * time.Second)
		a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "partial", Text: "ab"}}
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 2}}}
		synctest.Wait()
		if advanced, err := f.session.requestResultAck(context.Background(), 1, 1); !advanced || err != nil {
			t.Fatal("result ACK", advanced, err)
		}
		for i := 0; i < 3; i++ {
			if advanced, err := f.session.requestResultAck(context.Background(), 1, 1); advanced || err != nil {
				t.Fatal(advanced, err)
			}
		}
		time.Sleep(time.Second)
		synctest.Wait()
		assertResumeState(t, f.session.resume, resumeDetached, 1, start.Add(19*time.Second))
		if f.worker.input.input.nextOffset != 2 || f.worker.results.ackedSeq != 1 || context.Cause(f.rpcCtx) != nil {
			t.Fatal("detach rolled back accepted audio or ACK, or canceled RPC")
		}
		progressBarrier(t, f)
		closeDeliveryFixture(t, f)
	})
}

func TestInputProgressEndAndRetainingAreExempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.inputProgressTimeout = 5 * time.Second
		startManagedFixture(t, f, &attachmentTestConn{})
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		time.Sleep(6 * time.Second)
		if _, err := f.session.requestConnectionReady(context.Background(), 1); err != nil {
			t.Fatal("end tail wait hit idle timeout", err)
		}
		feed <- workerReadStep{err: io.EOF}
		assertWorkerRetaining(t, f)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatal(d, err)
		}
		synctest.Wait()
		if g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, &attachmentTestConn{}), 0); g != 2 || err != nil {
			t.Fatal(g, err)
		}
		time.Sleep(6 * time.Second)
		ready, err := f.session.requestConnectionReady(context.Background(), 2)
		if err != nil || !ready.inputEnded {
			t.Fatal("ended resume restarted input timeout", ready, err)
		}
		closeDeliveryFixture(t, f)
	})
}

func TestInputProgressUploadEOFAwaitsStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, func(context.Context, *asrv1.StreamingRecognizeRequest) error { return io.EOF }, nil)
		f.worker.config.inputProgressTimeout = 5 * time.Second
		f.worker.config.statusTimeout = 10 * time.Second
		startManagedFixture(t, f, &attachmentTestConn{})
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		synctest.Wait()
		time.Sleep(6 * time.Second)
		if _, err := f.session.requestConnectionReady(context.Background(), 1); err != nil {
			t.Fatal("waiting status misclassified as idle", err)
		}
		time.Sleep(4 * time.Second)
		f.assertFinishedAtCurrentTime(t, errWorkerStatusTimeout)
	})
}

// 只在测试中跳过观察时刻，模拟协调者迟到；不用真实睡眠衡量性能。
func TestInputProgressLateObservation(t *testing.T) {
	for _, observed := range []int64{8, 20} {
		t.Run(time.Duration(observed*time.Second.Nanoseconds()).String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				f.worker.config.inputProgressTimeout = 5 * time.Second
				start := time.Now()
				var jump atomic.Int64
				f.now = func() time.Time { return time.Now().Add(time.Duration(jump.Load()) * time.Second) }
				startManagedFixture(t, f, &attachmentTestConn{})
				synctest.Wait()
				jump.Store(observed)
				_, err := f.session.reportDetach(context.Background(), 0)
				if observed == 20 {
					if !errors.Is(err, errResumeClosed) {
						t.Fatal(err)
					}
					f.assertFinishedAtCurrentTime(t, errResumeExpired)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				assertResumeState(t, f.session.resume, resumeDetached, 1, start.Add(15*time.Second))
				if context.Cause(f.rpcCtx) != nil {
					t.Fatal("late detach canceled original RPC")
				}
				progressBarrier(t, f)
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestInputProgressDeadlineRejectsLateInput(t *testing.T) {
	for _, end := range []bool{false, true} {
		name := "audio"
		if end {
			name = "end"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				f.worker.config.inputProgressTimeout = 5 * time.Second
				var jump atomic.Int64
				f.now = func() time.Time { return time.Now().Add(time.Duration(jump.Load()) * time.Second) }
				startManagedFixture(t, f, &attachmentTestConn{})
				synctest.Wait()
				jump.Store(5)
				var a bool
				var n uint64
				var err error
				if end {
					a, n, err = f.session.requestEnd(context.Background(), 1, 0)
				} else {
					a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				}
				assertCoordinatorInput(t, a, n, err, false, 0, errSessionNotAttached)
				synctest.Wait()
				if f.worker.input.input.nextOffset != 0 || f.worker.input.input.ended {
					t.Fatal("expired input changed formal state")
				}
				progressBarrier(t, f)
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestInputProgressBlockedCleanupKeepsResources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.inputProgressTimeout = 5 * time.Second
		release := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		conn := &attachmentTestConn{closeFn: func() error { <-release; return nil }}
		startManagedFixture(t, f, conn)
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		candidateConn := &attachmentTestConn{}
		if g, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, candidateConn), 0); g != 0 || !errors.Is(err, errConnectionRetiring) {
			t.Fatal(g, err)
		}
		if candidateConn.reads.Load() != 0 || candidateConn.closes.Load() != 0 || context.Cause(f.rpcCtx) != nil {
			t.Fatal("candidate acquired or Worker canceled before window expired")
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		select {
		case <-f.session.controlDone:
		default:
			t.Fatal("logical expiry blocked by socket close")
		}
		select {
		case <-f.finished:
			t.Fatal("runner returned before actual close")
		default:
		}
		if f.worker.input == nil || f.worker.results == nil || !errors.Is(context.Cause(f.rpcCtx), errResumeExpired) {
			t.Fatal("resource release did not wait for I/O cleanup")
		}
		close(release)
		f.assertFinishedAtCurrentTime(t, errResumeExpired)
	})
}

func TestInputProgressCancellationPriority(t *testing.T) {
	for _, rpc := range []bool{false, true} {
		name := "logical_context"
		if rpc {
			name = "rpc_context"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				f.worker.config.inputProgressTimeout = 5 * time.Second
				var jump atomic.Int64
				f.now = func() time.Time { return time.Now().Add(time.Duration(jump.Load()) * time.Second) }
				startManagedFixture(t, f, &attachmentTestConn{})
				synctest.Wait()
				jump.Store(5)
				want := errors.New("priority cancellation")
				if rpc {
					f.cancelRPC(want)
				} else {
					f.cancelLife(want)
				}
				f.assertFinishedAtCurrentTime(t, want)
			})
		})
	}
}

func TestInputProgressPureComponentHasNoIdleTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.inputProgressTimeout = time.Second
		f.start()
		time.Sleep(time.Minute)
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		closeDeliveryFixture(t, f)
	})
}
