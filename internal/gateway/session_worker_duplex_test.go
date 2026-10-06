package gateway

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// workerReadStep 在真实接收任务的 Recv 边界注入一条响应或终态。
type workerReadStep struct {
	response *asrv1.StreamingRecognizeResponse
	err      error
}

// newDuplexWorkerFixture 运行真正的双向任务；feed 没有数据时 Recv 等待或响应 RPC 取消。
func newDuplexWorkerFixture(t *testing.T, send func(context.Context, *asrv1.StreamingRecognizeRequest) error, halfClose func(context.Context) error) (*workerCoordinatorFixture, chan workerReadStep) {
	t.Helper()
	feed := make(chan workerReadStep)
	f := newWorkerCoordinatorFixture(t, 32, 8, func(ctx context.Context) workerStream {
		return &uploadTestStream{
			send: func(req *asrv1.StreamingRecognizeRequest) error {
				if send != nil {
					return send(ctx, req)
				}
				return nil
			},
			closeSend: func() error {
				if halfClose != nil {
					return halfClose(ctx)
				}
				return nil
			},
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				select {
				case step := <-feed:
					return step.response, step.err
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				}
			},
		}
	})
	return f, feed
}

func assertWorkerRetaining(t *testing.T, f *workerCoordinatorFixture) {
	t.Helper()
	synctest.Wait()
	assertControlAlive(t, f.session)
	if f.worker.phase != workerRetaining || f.worker.input == nil || f.worker.results == nil {
		t.Fatal("normal Worker completion did not preserve the coordination state and results")
	}
	if context.Cause(f.rpcCtx) == nil {
		t.Fatal("completed RPC context was not released")
	}
	select {
	case <-f.worker.receiver.done:
	default:
		t.Fatal("retaining began before receiver actually exited")
	}
	// 空操作控制命令把以上字段读取与之后可能发生的期限清理建立同步。
	// 单纯推进 synctest 时间不提供这个读→未来写的关系。
	if d, err := f.session.reportDetach(context.Background(), 0); d || err != nil {
		t.Fatalf("retaining synchronization = (%v,%v)", d, err)
	}
}

func TestSessionWorkerDuplexDetachedResultsAndEarlyProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		f, feed := newDuplexWorkerFixture(t, func(ctx context.Context, req *asrv1.StreamingRecognizeRequest) error {
			if string(req.Data) != "ab" {
				t.Errorf("unexpected uploaded data %q", req.Data)
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}, nil)
		f.start()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		<-entered
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 2}}}
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "a"}}
		synctest.Wait()
		if f.worker.processedBytes != 2 || f.worker.dispatchedBytes != 2 || !f.worker.input.inFlight {
			t.Fatal("progress preceding Send return was rejected or completed audio too early")
		}
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatalf("detach = (%v,%v)", d, err)
		}
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "ab", IsFinal: true}}
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "t", Text: "tail", IsFinal: true}}
		synctest.Wait()
		if f.worker.results.lastSeq != 3 {
			t.Fatal("detached results were lost or progress used a result sequence")
		}
		for i, want := range []string{"a", "ab", "tail"} {
			got, ok, err := f.worker.results.peekAfter(uint64(i))
			if err != nil || !ok || got.text != want || got.seq != uint64(i+1) {
				t.Fatalf("retained result %d = (%+v,%v,%v)", i, got, ok, err)
			}
		}
		if generation, err := f.session.requestResume(context.Background()); generation != 2 || err != nil {
			t.Fatalf("resume = (%d,%v)", generation, err)
		}
		a, n, err = f.session.requestAudio(context.Background(), 2, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		close(release)
		a, n, err = f.session.requestEnd(context.Background(), 2, 2)
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		synctest.Wait()
		feed <- workerReadStep{err: io.EOF}
		assertWorkerRetaining(t, f)
		if f.worker.results.lastSeq != 3 || f.worker.processedBytes != 2 {
			t.Fatal("normal completion changed retained results or progress")
		}
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
	})
}

func TestSessionWorkerDuplexInvalidProgress(t *testing.T) {
	for _, name := range []string{"backwards", "buffered_but_not_dispatched"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				f, feed := newDuplexWorkerFixture(t, func(ctx context.Context, _ *asrv1.StreamingRecognizeRequest) error {
					close(entered)
					<-ctx.Done()
					return context.Cause(ctx)
				}, nil)
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				<-entered
				a, n, err = f.session.requestAudio(context.Background(), 1, 2, []byte("c"))
				assertCoordinatorInput(t, a, n, err, true, 3, nil)
				feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 1}}}
				feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 1}}}
				synctest.Wait()
				assertControlAlive(t, f.session)
				bad := uint64(0)
				if name == "buffered_but_not_dispatched" {
					bad = 3
				}
				feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: bad}}}
				f.assertFinishedAtCurrentTime(t, errInvalidWorkerProgress)
				if f.worker.processedBytes != 1 || f.worker.dispatchedBytes != 2 {
					t.Fatal("invalid progress modified the counters")
				}
			})
		})
	}
}

func TestSessionWorkerDuplexBacklogAtomicAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		f, feed := newDuplexWorkerFixture(t, func(ctx context.Context, _ *asrv1.StreamingRecognizeRequest) error {
			close(entered)
			<-ctx.Done()
			return context.Cause(ctx)
		}, nil)
		f.worker.config.maxPendingAudioBytes = 2
		buffer := f.worker.input
		f.start()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		<-entered
		a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 1}}}
		synctest.Wait()
		a, n, err = f.session.requestAudio(context.Background(), 1, 2, []byte("c"))
		assertCoordinatorInput(t, a, n, err, true, 3, nil)
		a, n, err = f.session.requestAudio(context.Background(), 1, 3, []byte("d"))
		assertCoordinatorInput(t, a, n, err, false, 0, ErrAudioBacklogExceeded)
		f.assertFinishedAtCurrentTime(t, ErrAudioBacklogExceeded)
		if buffer.input.nextOffset != 3 || buffer.retainedBytes != 3 || buffer.count != 2 || f.worker.processedBytes != 1 {
			t.Fatal("over-budget audio was accepted or outstanding bytes were incorrectly released")
		}
	})
}

func TestSessionWorkerDuplexResultBudgets(t *testing.T) {
	for _, name := range []string{"bytes", "slots"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				maxBytes, maxResults := uint64(100), 1
				if name == "bytes" {
					maxBytes, maxResults = 2, 8
				}
				buffer, err := newResultBuffer(maxBytes, maxResults)
				if err != nil {
					t.Fatal(err)
				}
				f.worker.results = buffer
				f.start()
				feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "a"}}
				feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "b"}}
				f.assertFinishedAtCurrentTime(t, errResultBufferFull)
				if buffer.count != 1 || buffer.lastSeq != 1 || buffer.retainedBytes != 2 {
					t.Fatal("failed result consumed sequence or storage")
				}
			})
		})
	}
}

func TestSessionWorkerDuplexNormalCompletionObservationOrders(t *testing.T) {
	for _, name := range []string{"recv_first", "close_result_first"} {
		t.Run(name, func(t *testing.T) {
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
				if name == "recv_first" {
					feed <- workerReadStep{err: io.EOF}
					synctest.Wait()
					if f.worker.phase != workerRunning {
						t.Fatal("Recv EOF alone completed the Worker")
					}
					assertControlAlive(t, f.session)
					close(release)
				} else {
					close(release)
					synctest.Wait()
					assertControlAlive(t, f.session)
					feed <- workerReadStep{err: io.EOF}
				}
				assertWorkerRetaining(t, f)
				if err := f.session.requestClose(context.Background()); err != nil {
					t.Fatal(err)
				}
				f.assertFinishedAtCurrentTime(t, nil)
			})
		})
	}
}

func TestSessionWorkerDuplexPrematureEOF(t *testing.T) {
	for _, name := range []string{"no_end", "end_with_outstanding_audio"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				f, feed := newDuplexWorkerFixture(t, func(ctx context.Context, _ *asrv1.StreamingRecognizeRequest) error {
					close(entered)
					<-ctx.Done()
					return nil
				}, nil)
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("a"))
				assertCoordinatorInput(t, a, n, err, true, 1, nil)
				<-entered
				if name == "end_with_outstanding_audio" {
					a, n, err = f.session.requestEnd(context.Background(), 1, 1)
					assertCoordinatorInput(t, a, n, err, true, 1, nil)
				}
				feed <- workerReadStep{err: io.EOF}
				f.assertFinishedAtCurrentTime(t, errWorkerEndedEarly)
				if f.worker.phase != workerRunning {
					t.Fatal("early EOF was marked normal completion")
				}
			})
		})
	}
}

func TestSessionWorkerDuplexUploadEOFStatus(t *testing.T) {
	for _, operation := range []string{"send", "close"} {
		for _, status := range []string{"error", "EOF", "never"} {
			t.Run(operation+"/"+status, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f, feed := newDuplexWorkerFixture(t, func(context.Context, *asrv1.StreamingRecognizeRequest) error {
						if operation == "send" {
							return io.EOF
						}
						return nil
					}, func(context.Context) error { return io.EOF })
					f.start()
					if operation == "send" {
						a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("a"))
						assertCoordinatorInput(t, a, n, err, true, 1, nil)
					} else {
						a, n, err := f.session.requestEnd(context.Background(), 1, 0)
						assertCoordinatorInput(t, a, n, err, true, 0, nil)
					}
					synctest.Wait()
					assertControlAlive(t, f.session)
					if context.Cause(f.rpcCtx) != nil {
						t.Fatal("upload EOF canceled RPC before Recv status")
					}
					want := errors.New("authoritative Worker status")
					switch status {
					case "error":
						feed <- workerReadStep{err: want}
					case "EOF":
						feed <- workerReadStep{err: io.EOF}
						want = errWorkerEndedEarly
					case "never":
						want = errWorkerStatusTimeout
						time.Sleep(time.Second)
					}
					f.assertFinishedAtCurrentTime(t, want)
				})
			})
		}
	}
}

func TestSessionWorkerDuplexAwaitingStatusRejectsNewInputWithoutOverriding(t *testing.T) {
	for _, name := range []string{"new_audio_after_end", "different_end"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, func(context.Context) error { return io.EOF })
				f.start()
				a, n, err := f.session.requestEnd(context.Background(), 1, 0)
				assertCoordinatorInput(t, a, n, err, true, 0, nil)
				synctest.Wait()
				if name == "new_audio_after_end" {
					a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("a"))
				} else {
					a, n, err = f.session.requestEnd(context.Background(), 1, 1)
				}
				assertCoordinatorInput(t, a, n, err, false, 0, errWorkerInputStopped)
				assertControlAlive(t, f.session)
				want := errors.New("original Worker status")
				feed <- workerReadStep{err: want}
				f.assertFinishedAtCurrentTime(t, want)
			})
		})
	}
}

func TestSessionWorkerDuplexTailDeadlineDoesNotMove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.worker.config.tailTimeout = 5 * time.Second
		f.start()
		time.Sleep(6 * time.Second) // 合法 end 前没有尾部期限。
		assertControlAlive(t, f.session)
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		time.Sleep(time.Second)
		a, n, err = f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, false, 0, nil)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatalf("detach = (%v,%v)", d, err)
		}
		time.Sleep(time.Second)
		if g, err := f.session.requestResume(context.Background()); g != 2 || err != nil {
			t.Fatalf("resume = (%d,%v)", g, err)
		}
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{}}}
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Text: "tail", IsFinal: true}}
		time.Sleep(3 * time.Second)
		f.assertFinishedAtCurrentTime(t, ErrTailTimeout)
	})
}

func TestSessionWorkerDuplexRetentionBounds(t *testing.T) {
	for _, name := range []string{"attached", "resume_does_not_extend", "recovery_expires_first"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.worker.config.tailTimeout = time.Second
				f.worker.config.resultRetentionTimeout = 3 * time.Second
				if name == "recovery_expires_first" {
					f.worker.config.resultRetentionTimeout = 20 * time.Second
				}
				buffer := f.worker.results
				f.start()
				a, n, err := f.session.requestEnd(context.Background(), 1, 0)
				assertCoordinatorInput(t, a, n, err, true, 0, nil)
				synctest.Wait()
				feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Text: "last", IsFinal: true}}
				feed <- workerReadStep{err: io.EOF}
				assertWorkerRetaining(t, f)
				if name != "attached" {
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatalf("detach retained session = (%v,%v)", d, err)
					}
				}
				want := errResultRetentionExpired
				if name == "recovery_expires_first" {
					time.Sleep(10 * time.Second)
					want = errResumeExpired
				} else {
					time.Sleep(2 * time.Second)
					assertControlAlive(t, f.session) // 原 tailTimeout 已经过期但不限制结果保留。
					if name == "resume_does_not_extend" {
						if g, err := f.session.requestResume(context.Background()); g != 2 || err != nil {
							t.Fatalf("resume retained session = (%d,%v)", g, err)
						}
						a, n, err = f.session.requestEnd(context.Background(), 2, 0)
						assertCoordinatorInput(t, a, n, err, false, 0, nil)
					}
					time.Sleep(time.Second)
				}
				f.assertFinishedAtCurrentTime(t, want)
				if f.worker.phase != workerRetaining || buffer.lastSeq != 1 || buffer.slots[buffer.head].text != "last" {
					t.Fatal("retention expiry changed normal computation or lost retained data before cleanup")
				}
			})
		})
	}
}

// pauseWorkerCommand 已完成请求交付，在 Err 检查内暂停协调者，模拟暂时无法处理 Timer。
// 使用无业务效果的非法 kind，放行后由下一轮统一判断已过的截止时间。
func pauseWorkerCommand(t *testing.T, f *workerCoordinatorFixture) (chan struct{}, <-chan sessionControlResult) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	ctx := &controlCheckedContext{Context: context.Background(), check: func() error {
		close(entered)
		<-release
		return nil
	}}
	reply := make(chan sessionControlResult, 1)
	f.session.commands <- sessionControlCommand{kind: 255, ctx: ctx, reply: reply}
	<-entered
	return release, reply
}

func TestSessionWorkerDuplexDelayedObservationUsesEarliestDeadline(t *testing.T) {
	for _, name := range []string{"tail_before_resume", "status_before_tail", "retention_before_resume"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, func(context.Context) error {
					if name == "status_before_tail" {
						return io.EOF
					}
					return nil
				})
				f.worker.config.tailTimeout = 2 * time.Second
				f.worker.config.resultRetentionTimeout = 2 * time.Second
				if name == "status_before_tail" {
					f.worker.config.tailTimeout = 10 * time.Second
					f.worker.config.statusTimeout = 2 * time.Second
				}
				f.start()
				a, n, err := f.session.requestEnd(context.Background(), 1, 0)
				assertCoordinatorInput(t, a, n, err, true, 0, nil)
				synctest.Wait()
				want := ErrTailTimeout
				if name == "retention_before_resume" {
					feed <- workerReadStep{err: io.EOF}
					assertWorkerRetaining(t, f)
					want = errResultRetentionExpired
				}
				if name != "status_before_tail" {
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatalf("detach = (%v,%v)", d, err)
					}
				} else {
					want = errWorkerStatusTimeout
				}
				release, reply := pauseWorkerCommand(t, f)
				time.Sleep(11 * time.Second) // 模拟协调者迟到，一次观察时两个期限都已过。
				close(release)
				<-reply
				f.assertFinishedAtCurrentTime(t, want)
			})
		})
	}
}

func TestSessionWorkerDuplexRPCCausePrecedesExpiredDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := newDuplexWorkerFixture(t, nil, nil)
		f.session.resume.detach(1, time.Now().Add(-10*time.Second))
		cause := errors.New("RPC stopped before coordination")
		f.cancelRPC(cause)
		f.start()
		f.assertFinishedAtCurrentTime(t, cause)
	})
}

func TestSessionWorkerDuplexCancelBeforeCompletionCommit(t *testing.T) {
	for _, which := range []string{"RPC", "lifecycle"} {
		t.Run(which, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				entered, release := make(chan struct{}), make(chan struct{})
				var armed atomic.Bool
				f.now = func() time.Time {
					if armed.CompareAndSwap(true, false) {
						close(entered)
						<-release
					}
					return time.Now()
				}
				f.start()
				a, n, err := f.session.requestEnd(context.Background(), 1, 0)
				assertCoordinatorInput(t, a, n, err, true, 0, nil)
				synctest.Wait() // CloseSend 成功，协调者等待 Recv EOF。
				armed.Store(true)
				feed <- workerReadStep{err: io.EOF}
				<-entered // EOF 已选中，尚未提交正常完成；时钟仅用于测试同步。
				cause := errors.New("stopped before completion was committed")
				if which == "RPC" {
					f.cancelRPC(cause)
				} else {
					f.cancelLife(cause)
				}
				close(release)
				f.assertFinishedAtCurrentTime(t, cause)
				if f.worker.phase != workerRunning {
					t.Fatal("cancellation before completion commit was marked normal computation")
				}
			})
		})
	}
}

func TestSessionWorkerDuplexCleanupWaitsForBothDirections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sendEntered, recvEntered := make(chan struct{}), make(chan struct{})
		sendRelease, recvRelease := make(chan struct{}), make(chan struct{})
		f := newWorkerCoordinatorFixture(t, 8, 2, func(ctx context.Context) workerStream {
			return &uploadTestStream{
				send: func(*asrv1.StreamingRecognizeRequest) error {
					close(sendEntered)
					<-ctx.Done()
					<-sendRelease
					return nil
				},
				recv: func() (*asrv1.StreamingRecognizeResponse, error) {
					close(recvEntered)
					<-ctx.Done()
					<-recvRelease
					return nil, io.EOF
				},
			}
		})
		f.start()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("a"))
		assertCoordinatorInput(t, a, n, err, true, 1, nil)
		<-sendEntered
		<-recvEntered
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-f.finished:
			t.Fatal("run returned before either I/O finished")
		default:
		}
		close(sendRelease)
		synctest.Wait()
		if f.worker.input == nil || f.worker.results == nil {
			t.Fatal("buffers released after only one direction exited")
		}
		select {
		case <-f.finished:
			t.Fatal("run returned before Recv cleanup")
		default:
		}
		close(recvRelease)
		f.assertFinishedAtCurrentTime(t, nil)
	})
}

func TestSessionWorkerDuplexAdditionalConfigValidation(t *testing.T) {
	for _, name := range []string{"tail_zero", "tail_negative", "status_zero", "status_negative", "retention_zero", "retention_negative", "result_bytes_zero", "results_zero", "results_negative"} {
		t.Run(name, func(t *testing.T) {
			f, _ := newDuplexWorkerFixture(t, nil, nil)
			cfg := f.worker.config
			want := errInvalidSessionWorkerConfig
			switch name {
			case "tail_zero":
				cfg.tailTimeout = 0
			case "tail_negative":
				cfg.tailTimeout = -time.Second
			case "status_zero":
				cfg.statusTimeout = 0
			case "status_negative":
				cfg.statusTimeout = -time.Second
			case "retention_zero":
				cfg.resultRetentionTimeout = 0
			case "retention_negative":
				cfg.resultRetentionTimeout = -time.Second
			case "result_bytes_zero":
				cfg.maxResultBytes, want = 0, errInvalidResultBufferLimits
			case "results_zero":
				cfg.maxResults, want = 0, errInvalidResultBufferLimits
			case "results_negative":
				cfg.maxResults, want = -1, errInvalidResultBufferLimits
			}
			w, err := newSessionWorker(cfg)
			if w != nil || !errors.Is(err, want) || context.Cause(f.rpcCtx) != nil {
				t.Fatalf("invalid config = (%v,%v), RPC cause=%v", w, err, context.Cause(f.rpcCtx))
			}
		})
	}
}

func TestSessionWorkerDuplexReadFailures(t *testing.T) {
	for _, name := range []string{"nil_response", "mixed_progress", "read_error"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				step := workerReadStep{}
				want := errInvalidWorkerResponse
				if name == "mixed_progress" {
					step.response = &asrv1.StreamingRecognizeResponse{Text: "bad", Progress: &asrv1.AudioProgress{}}
				}
				if name == "read_error" {
					want = errors.New("read failed")
					step.err = want
				}
				feed <- step
				f.assertFinishedAtCurrentTime(t, want)
			})
		})
	}
}

func TestSessionWorkerDuplexStatusDeadlineDoesNotMove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, func(context.Context, *asrv1.StreamingRecognizeRequest) error { return io.EOF }, nil)
		f.worker.config.statusTimeout = 3 * time.Second
		f.start()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("a"))
		assertCoordinatorInput(t, a, n, err, true, 1, nil)
		synctest.Wait()
		time.Sleep(time.Second)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatalf("detach = (%v,%v)", d, err)
		}
		time.Sleep(time.Second)
		if g, err := f.session.requestResume(context.Background()); g != 2 || err != nil {
			t.Fatalf("resume = (%d,%v)", g, err)
		}
		a, n, err = f.session.requestAudio(context.Background(), 2, 0, []byte("a"))
		assertCoordinatorInput(t, a, n, err, false, 1, nil)
		a, n, err = f.session.requestEnd(context.Background(), 2, 1)
		assertCoordinatorInput(t, a, n, err, false, 0, errWorkerInputStopped)
		feed <- workerReadStep{response: &asrv1.StreamingRecognizeResponse{Text: "status pending"}}
		time.Sleep(time.Second)
		f.assertFinishedAtCurrentTime(t, errWorkerStatusTimeout)
	})
}

func TestSessionWorkerDuplexReceiveEOFFollowedByCloseEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		f, feed := newDuplexWorkerFixture(t, nil, func(ctx context.Context) error {
			close(entered)
			select {
			case <-release:
				return io.EOF
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		})
		f.start()
		a, n, err := f.session.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, true, 0, nil)
		<-entered
		feed <- workerReadStep{err: io.EOF}
		synctest.Wait()
		assertControlAlive(t, f.session)
		close(release)
		// 已有接收终态，不应再等 statusTimeout，也不能进入正常结果保留。
		f.assertFinishedAtCurrentTime(t, errWorkerEndedEarly)
		if f.worker.phase != workerRunning {
			t.Fatal("CloseSend EOF was treated as successful half-close")
		}
	})
}

func TestSessionWorkerDuplexEqualDeadlinePriority(t *testing.T) {
	for _, name := range []string{"resume_before_tail", "tail_before_status", "resume_before_retention"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, func(context.Context) error {
					if name == "tail_before_status" {
						return io.EOF
					}
					return nil
				})
				f.worker.config.tailTimeout = 10 * time.Second
				f.worker.config.statusTimeout = 10 * time.Second
				f.worker.config.resultRetentionTimeout = 10 * time.Second
				f.start()
				a, n, err := f.session.requestEnd(context.Background(), 1, 0)
				assertCoordinatorInput(t, a, n, err, true, 0, nil)
				synctest.Wait()
				if name == "resume_before_retention" {
					feed <- workerReadStep{err: io.EOF}
					assertWorkerRetaining(t, f)
				}
				want := errResumeExpired
				if name == "tail_before_status" {
					want = ErrTailTimeout
				} else if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
					t.Fatalf("detach = (%v,%v)", d, err)
				}
				release, reply := pauseWorkerCommand(t, f)
				time.Sleep(11 * time.Second)
				close(release)
				<-reply
				f.assertFinishedAtCurrentTime(t, want)
			})
		})
	}
}
