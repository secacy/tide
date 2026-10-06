package gateway

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// workerCoordinatorFixture 运行真正的协调入口；测试仅控制 Worker 的 I/O。
// err 在 finished 关闭后读取；可变会话/缓冲字段只在同步边界之后断言。
type workerCoordinatorFixture struct {
	session    *resumableSession
	worker     *sessionWorker
	lifeCtx    context.Context
	cancelLife context.CancelCauseFunc
	rpcCtx     context.Context
	cancelRPC  context.CancelCauseFunc
	finished   chan struct{}
	err        error
	started    bool
	now        func() time.Time // 默认真实/虚拟时钟；允许测试在提交状态前建立同步点。
}

// newWorkerCoordinatorFixture 创建同一逻辑生命周期下的 RPC，不启动后台任务。
// build 提供可取消的 stream；默认发送期限长于 10 秒恢复窗口。
func newWorkerCoordinatorFixture(t *testing.T, maxBytes uint64, maxChunks int, build func(context.Context) workerStream) *workerCoordinatorFixture {
	t.Helper()
	lifeCtx, cancelLife := context.WithCancelCause(context.Background())
	rpcCtx, cancelRPC := context.WithCancelCause(lifeCtx)
	stream := build(rpcCtx)
	if testStream, ok := stream.(*uploadTestStream); ok && testStream.recv == nil {
		testStream.recv = func() (*asrv1.StreamingRecognizeResponse, error) {
			<-rpcCtx.Done()
			return nil, context.Cause(rpcCtx)
		}
	}
	upload, err := newSessionWorker(sessionWorkerConfig{
		rpcCtx: rpcCtx, cancelRPC: cancelRPC, stream: stream,
		sendTimeout: time.Minute, tailTimeout: time.Minute, statusTimeout: time.Second,
		resultRetentionTimeout: time.Minute, maxAudioBytes: maxBytes, maxAudioChunks: maxChunks,
		maxResultBytes: 1024, maxResults: 16,
	})
	if err != nil {
		cancelLife(nil)
		cancelRPC(nil)
		t.Fatal(err)
	}
	f := &workerCoordinatorFixture{
		session: newTestResumableSession(t, identityTestMaterial()), worker: upload,
		lifeCtx: lifeCtx, cancelLife: cancelLife, rpcCtx: rpcCtx, cancelRPC: cancelRPC,
		finished: make(chan struct{}), now: time.Now,
	}
	t.Cleanup(func() {
		cancelLife(nil)
		cancelRPC(nil)
		if f.started {
			<-f.finished
		}
	})
	return f
}

func (f *workerCoordinatorFixture) start() {
	f.started = true
	go func() {
		f.err = f.session.runWithWorker(f.lifeCtx, f.now, f.worker)
		close(f.finished)
	}()
}

// assertFinishedAtCurrentTime 不等待未来期限，避免掩盖迟到的清理。
func (f *workerCoordinatorFixture) assertFinishedAtCurrentTime(t *testing.T, want error) {
	t.Helper()
	synctest.Wait()
	select {
	case <-f.finished:
	default:
		t.Fatal("runWithWorker has not finished at the expected stage")
	}
	if !errors.Is(f.err, want) {
		t.Fatalf("runWithWorker error = %v, want %v", f.err, want)
	}
	select {
	case <-f.worker.uploader.done:
	default:
		t.Fatal("runner returned before uploader exited")
	}
	if f.worker.input != nil {
		t.Fatal("runner retained the input buffer after upload cleanup")
	}
	select {
	case <-f.worker.receiver.done:
	default:
		t.Fatal("runner returned before receiver exited")
	}
	if f.worker.results != nil {
		t.Fatal("runner retained results after both I/O tasks exited")
	}
	assertResumeState(t, f.session.resume, resumeClosed, f.session.resume.generation, time.Time{})
}

func assertCoordinatorInput(t *testing.T, accepted bool, next uint64, err error, wantAccepted bool, wantNext uint64, wantErr error) {
	t.Helper()
	if accepted != wantAccepted || next != wantNext || !errors.Is(err, wantErr) {
		t.Fatalf("input result = (%v, %d, %v), want (%v, %d, %v)", accepted, next, err, wantAccepted, wantNext, wantErr)
	}
}

func TestSessionWorkerConstruction(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	config := sessionWorkerConfig{rpcCtx: ctx, cancelRPC: cancel, stream: &uploadTestStream{}, sendTimeout: time.Second,
		tailTimeout: time.Minute, statusTimeout: time.Second, resultRetentionTimeout: time.Minute,
		maxAudioBytes: 4, maxAudioChunks: 2, maxResultBytes: 1024, maxResults: 16}
	for _, name := range []string{"nil_context", "nil_cancel", "nil_stream", "zero_timeout", "negative_timeout", "zero_bytes", "zero_chunks", "negative_chunks"} {
		t.Run(name, func(t *testing.T) {
			cfg := config
			want := errInvalidSessionWorkerConfig
			switch name {
			case "nil_context":
				cfg.rpcCtx = nil
			case "nil_cancel":
				cfg.cancelRPC = nil
			case "nil_stream":
				cfg.stream = nil
			case "zero_timeout":
				cfg.sendTimeout = 0
			case "negative_timeout":
				cfg.sendTimeout = -time.Second
			case "zero_bytes":
				cfg.maxAudioBytes, want = 0, errInvalidAudioBufferLimits
			case "zero_chunks":
				cfg.maxAudioChunks, want = 0, errInvalidAudioBufferLimits
			case "negative_chunks":
				cfg.maxAudioChunks, want = -1, errInvalidAudioBufferLimits
			}
			u, err := newSessionWorker(cfg)
			if u != nil || !errors.Is(err, want) || context.Cause(ctx) != nil {
				t.Fatalf("invalid construction = (%v, %v), RPC cause=%v", u, err, context.Cause(ctx))
			}
		})
	}
	t.Run("independent_without_startup", func(t *testing.T) {
		a, err := newSessionWorker(config)
		if err != nil {
			t.Fatal(err)
		}
		b, err := newSessionWorker(config)
		if err != nil {
			t.Fatal(err)
		}
		if a.input == b.input || a.uploader == b.uploader || a.uploader.jobs == b.uploader.jobs {
			t.Fatal("upload objects must own independent buffers and task channels")
		}
		select {
		case <-a.uploader.done:
			t.Fatal("construction completed a task")
		case a.uploader.jobs <- workerUploadCommand{}:
			t.Fatal("construction started a task")
		default:
		}
		if context.Cause(ctx) != nil {
			t.Fatal("construction took over RPC cancellation")
		}
	})
}

func TestSessionWorkerInvalidStartup(t *testing.T) {
	for _, name := range []string{"nil_lifecycle", "nil_clock", "nil_upload"} {
		t.Run(name, func(t *testing.T) {
			f := newWorkerCoordinatorFixture(t, 4, 2, func(context.Context) workerStream { return &uploadTestStream{} })
			var ctx context.Context = f.lifeCtx
			now, upload := time.Now, f.worker
			switch name {
			case "nil_lifecycle":
				ctx = nil
			case "nil_clock":
				now = nil
			case "nil_upload":
				upload = nil
			}
			defer func() {
				if recover() == nil {
					t.Fatal("invalid startup did not panic")
				}
				select {
				case <-f.worker.uploader.done:
					t.Fatal("invalid startup started the uploader")
				case f.worker.uploader.jobs <- workerUploadCommand{}:
					t.Fatal("invalid startup started the uploader")
				default:
				}
			}()
			_ = f.session.runWithWorker(ctx, now, upload)
		})
	}
}

func TestSessionWorkerPureControlRejectsInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		startTestSessionControl(t, s, time.Now)
		a, n, err := s.requestAudio(context.Background(), 1, 0, []byte("a"))
		assertCoordinatorInput(t, a, n, err, false, 0, errSessionWorkerUnavailable)
		a, n, err = s.requestEnd(context.Background(), 1, 0)
		assertCoordinatorInput(t, a, n, err, false, 0, errSessionWorkerUnavailable)
		if detached, err := s.reportDetach(context.Background(), 1); !detached || err != nil {
			t.Fatalf("input rejection changed control availability: (%v, %v)", detached, err)
		}
		if g, err := s.requestResume(context.Background(), 0); g != 2 || err != nil {
			t.Fatalf("resume = (%d, %v)", g, err)
		}
	})
}

func TestSessionWorkerFIFOAndHalfClosePreserveRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release, halfClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		operations := make(chan string, 8)
		first := true         // 唯一 uploader 访问。
		tailReturned := false // 唯一 receiver 访问。
		f := newWorkerCoordinatorFixture(t, 16, 4, func(ctx context.Context) workerStream {
			return &uploadTestStream{
				send: func(req *asrv1.StreamingRecognizeRequest) error {
					if first {
						first = false
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
							return context.Cause(ctx)
						}
					}
					operations <- string(req.Data)
					return nil
				},
				closeSend: func() error { operations <- "close"; close(halfClosed); return nil },
				recv: func() (*asrv1.StreamingRecognizeResponse, error) {
					if !tailReturned {
						select {
						case <-halfClosed:
							tailReturned = true
							return &asrv1.StreamingRecognizeResponse{Text: "tail"}, nil
						case <-ctx.Done():
							return nil, context.Cause(ctx)
						}
					}
					<-ctx.Done()
					return nil, context.Cause(ctx)
				},
			}
		})
		f.start()
		payload := []byte("ab")
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, payload)
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		<-entered
		copy(payload, "XX") // 公开入口返回后原切片可以复用。
		for _, chunk := range []struct {
			offset uint64
			data   string
		}{{2, "cde"}, {5, "f"}} {
			a, n, err = f.session.requestAudio(context.Background(), 1, chunk.offset, []byte(chunk.data))
			assertCoordinatorInput(t, a, n, err, true, chunk.offset+uint64(len(chunk.data)), nil)
		}
		a, n, err = f.session.requestEnd(context.Background(), 1, 6)
		assertCoordinatorInput(t, a, n, err, true, 6, nil)
		select {
		case <-halfClosed:
			t.Fatal("CloseSend ran before outstanding audio completed")
		default:
		}
		unblock()
		<-halfClosed
		synctest.Wait()
		assertControlAlive(t, f.session)
		if context.Cause(f.rpcCtx) != nil {
			t.Fatal("normal half-close canceled the RPC")
		}
		a, n, err = f.session.requestEnd(context.Background(), 1, 6)
		assertCoordinatorInput(t, a, n, err, false, 6, nil)
		a, n, err = f.session.requestAudio(context.Background(), 1, 2, []byte("cde"))
		assertCoordinatorInput(t, a, n, err, false, 6, nil)
		synctest.Wait()
		tail, ok, err := f.worker.results.peekAfter(0) // 由真正 receiver 交给协调者保存。
		if err != nil || !ok || tail.text != "tail" {
			t.Fatalf("tail = (%v, %v, %v)", tail, ok, err)
		}
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
		var got []string
		for len(operations) != 0 {
			got = append(got, <-operations)
		}
		if !reflect.DeepEqual(got, []string{"ab", "cde", "f", "close"}) {
			t.Fatalf("Worker operations = %v", got)
		}
	})
}

func TestSessionWorkerBlockedSendAllowsResumeAndFencesOldInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release, halfClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		sent := make(chan string, 4)
		first := true
		f := newWorkerCoordinatorFixture(t, 4, 2, func(ctx context.Context) workerStream {
			return &uploadTestStream{
				send: func(req *asrv1.StreamingRecognizeRequest) error {
					if first {
						first = false
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
							return context.Cause(ctx)
						}
					}
					sent <- string(req.Data)
					return nil
				},
				closeSend: func() error { close(halfClosed); return nil },
			}
		})
		f.start()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		<-entered
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatalf("detach during Send = (%v, %v)", d, err)
		}
		a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 0, errSessionNotAttached)
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, false, 0, errSessionNotAttached)
		time.Sleep(3 * time.Second) // 虚拟时间：在原恢复窗口内恢复。
		if g, err := f.session.requestResume(context.Background(), 0); g != 2 || err != nil {
			t.Fatalf("resume during Send = (%d, %v)", g, err)
		}
		for _, input := range []struct {
			offset uint64
			data   string
		}{{0, "ab"}, {2, "c"}} {
			a, n, err = f.session.requestAudio(context.Background(), 1, input.offset, []byte(input.data))
			assertCoordinatorInput(t, a, n, err, false, 0, errSessionGenerationMismatch)
		}
		a, n, err = f.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, a, n, err, false, 0, errSessionGenerationMismatch)
		a, n, err = f.session.requestAudio(context.Background(), 2, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 2, nil)
		a, n, err = f.session.requestAudio(context.Background(), 2, 2, []byte("c"))
		assertCoordinatorInput(t, a, n, err, true, 3, nil)
		a, n, err = f.session.requestEnd(context.Background(), 2, 3)
		assertCoordinatorInput(t, a, n, err, true, 3, nil)
		unblock()
		<-halfClosed
		time.Sleep(7 * time.Second) // 旧 t=10 恢复期限已经越过，不能结束新代次。
		assertControlAlive(t, f.session)
		if len(sent) != 2 || <-sent != "ab" || <-sent != "c" {
			t.Fatal("recovery duplicated or lost accepted audio")
		}
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
		if f.session.resume.generation != 2 {
			t.Fatal("wrong retained generation")
		}
	})
}

func TestSessionWorkerDetachedAcceptedInputStillDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release, halfClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		sent := make(chan string, 3)
		first := true
		f := newWorkerCoordinatorFixture(t, 4, 2, func(ctx context.Context) workerStream {
			return &uploadTestStream{
				send: func(req *asrv1.StreamingRecognizeRequest) error {
					if first {
						first = false
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
							return context.Cause(ctx)
						}
					}
					sent <- string(req.Data)
					return nil
				},
				closeSend: func() error { close(halfClosed); return nil },
			}
		})
		f.start()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		<-entered
		a, n, err = f.session.requestAudio(context.Background(), 1, 2, []byte("c"))
		assertCoordinatorInput(t, a, n, err, true, 3, nil)
		a, n, err = f.session.requestEnd(context.Background(), 1, 3)
		assertCoordinatorInput(t, a, n, err, true, 3, nil)
		if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
			t.Fatalf("detach = (%v, %v)", d, err)
		}
		unblock()
		<-halfClosed
		assertControlAlive(t, f.session)
		if context.Cause(f.rpcCtx) != nil || len(sent) != 2 || <-sent != "ab" || <-sent != "c" {
			t.Fatal("detached input was canceled, reordered or lost")
		}
		if g, err := f.session.requestResume(context.Background(), 0); g != 2 || err != nil {
			t.Fatalf("resume = (%d, %v)", g, err)
		}
		a, n, err = f.session.requestEnd(context.Background(), 2, 3)
		assertCoordinatorInput(t, a, n, err, false, 3, nil)
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
	})
}

func TestSessionWorkerControlExitPrecedesActualCleanup(t *testing.T) {
	for _, name := range []string{"explicit_close", "lifecycle_cancel", "rpc_cancel"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, cancelSeen, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				f := newWorkerCoordinatorFixture(t, 2, 1, func(ctx context.Context) workerStream {
					return &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
						close(entered)
						<-ctx.Done()
						close(cancelSeen)
						<-release
						return nil // 取消原因必须覆盖底层 nil。
					}}
				})
				buffer := f.worker.input
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				<-entered
				cause := errors.New("controlled termination")
				var want error
				switch name {
				case "explicit_close":
					if err := f.session.requestClose(context.Background()); err != nil {
						t.Fatal(err)
					}
				case "lifecycle_cancel":
					f.cancelLife(cause)
					want = cause
				case "rpc_cancel":
					f.cancelRPC(cause)
					want = cause
				}
				<-f.session.controlDone
				<-cancelSeen
				synctest.Wait()
				if err := f.session.requestClose(context.Background()); err != nil {
					t.Fatal(err)
				}
				select {
				case <-f.finished:
					t.Fatal("runner returned while Send still held audio")
				default:
				}
				select {
				case <-f.worker.uploader.done:
					t.Fatal("uploader exited before Send returned")
				default:
				}
				if f.worker.input != buffer || buffer.retainedBytes != 2 || !buffer.inFlight {
					t.Fatal("buffer released before actual upload exit")
				}
				unblock()
				f.assertFinishedAtCurrentTime(t, want)
				if buffer.count != 1 || buffer.input.nextOffset != 2 {
					t.Fatal("canceled send incorrectly completed input")
				}
				assertTestWorkerUploadResult(t, <-f.worker.uploader.results, workerUploadAudio, 0, context.Cause(f.rpcCtx))
			})
		})
	}
}

func TestSessionWorkerExpiryCancelsBlockedSend(t *testing.T) {
	for _, initial := range []bool{false, true} {
		name := "detach_command"
		if initial {
			name = "initial_detached"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				f := newWorkerCoordinatorFixture(t, 2, 1, func(ctx context.Context) workerStream {
					return &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { close(entered); <-ctx.Done(); return io.EOF }}
				})
				buffer := f.worker.input
				if initial {
					if ok, err := buffer.offer(0, []byte("ab")); !ok || err != nil {
						t.Fatal(err)
					}
					f.session.resume.detach(1, time.Now())
				}
				f.start()
				if !initial {
					a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
					assertCoordinatorInput(t, a, n, err, true, 2, nil)
				}
				<-entered
				if !initial {
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatalf("detach = (%v, %v)", d, err)
					}
				}
				time.Sleep(10*time.Second - time.Nanosecond)
				assertControlAlive(t, f.session)
				time.Sleep(time.Nanosecond)
				f.assertFinishedAtCurrentTime(t, errResumeExpired)
				if !errors.Is(context.Cause(f.rpcCtx), errResumeExpired) || buffer.count != 1 || buffer.retainedBytes != 2 {
					t.Fatal("expiry did not preserve cause or outstanding budget")
				}
			})
		})
	}
}

func TestSessionWorkerSendTimeoutWaitsForUnderlyingReturn(t *testing.T) {
	for _, name := range []string{"send_returns_nil", "send_returns_eof"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				f := newWorkerCoordinatorFixture(t, 2, 1, func(ctx context.Context) workerStream {
					return &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
						close(entered)
						<-ctx.Done()
						time.Sleep(10 * time.Millisecond)
						// 明确排在阶段断言之后返回，避免虚拟时间本身
						// 被误当成缓冲指针读取与随后清空之间的同步。
						<-release
						if name == "send_returns_eof" {
							return io.EOF
						}
						return nil
					}}
				})
				f.worker.config.sendTimeout = time.Second
				buffer := f.worker.input
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				<-entered
				time.Sleep(time.Second)
				synctest.Wait()
				select {
				case <-f.session.controlDone:
				default:
					t.Fatal("send timeout did not stop control loop")
				}
				select {
				case <-f.finished:
					t.Fatal("runner returned before Send cleanup")
				default:
				}
				if f.worker.input != buffer || len(f.worker.uploader.results) != 0 {
					t.Fatal("premature buffer release or upload result")
				}
				time.Sleep(10 * time.Millisecond)
				unblock()
				f.assertFinishedAtCurrentTime(t, ErrWorkerSendTimeout)
				if buffer.count != 1 || buffer.input.nextOffset != 2 {
					t.Fatal("timed-out input was completed")
				}
				assertTestWorkerUploadResult(t, <-f.worker.uploader.results, workerUploadAudio, 0, ErrWorkerSendTimeout)
			})
		})
	}
}

func TestSessionWorkerInputFailuresReplyThenTerminate(t *testing.T) {
	for _, name := range []string{"empty", "gap", "overlap", "overflow", "wrong_end", "new_after_end", "byte_budget", "slot_budget"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				maxBytes, maxChunks := uint64(8), 2
				if name == "byte_budget" {
					maxBytes = 2
				}
				if name == "slot_budget" {
					maxChunks = 1
				}
				entered := make(chan struct{})
				f := newWorkerCoordinatorFixture(t, maxBytes, maxChunks, func(ctx context.Context) workerStream {
					return &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { close(entered); <-ctx.Done(); return nil }}
				})
				buffer := f.worker.input
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				<-entered
				var want error
				switch name {
				case "empty":
					a, n, err = f.session.requestAudio(context.Background(), 1, 2, nil)
					want = errAudioEmptyChunk
				case "gap":
					a, n, err = f.session.requestAudio(context.Background(), 1, 3, []byte("x"))
					want = errAudioGap
				case "overlap":
					a, n, err = f.session.requestAudio(context.Background(), 1, 1, []byte("yz"))
					want = errAudioOverlap
				case "overflow":
					a, n, err = f.session.requestAudio(context.Background(), 1, math.MaxUint64, []byte("x"))
					want = errAudioPositionOverflow
				case "wrong_end":
					a, n, err = f.session.requestEnd(context.Background(), 1, 1)
					want = errAudioEndMismatch
				case "new_after_end":
					a, n, err = f.session.requestEnd(context.Background(), 1, 2)
					assertCoordinatorInput(t, a, n, err, true, 2, nil)
					a, n, err = f.session.requestAudio(context.Background(), 1, 2, []byte("x"))
					want = errAudioInputEnded
				case "byte_budget", "slot_budget":
					a, n, err = f.session.requestAudio(context.Background(), 1, 2, []byte("x"))
					want = errAudioBufferFull
				}
				assertCoordinatorInput(t, a, n, err, false, 0, want)
				f.assertFinishedAtCurrentTime(t, want)
				if !errors.Is(context.Cause(f.rpcCtx), want) || buffer.input.nextOffset != 2 || buffer.count != 1 || buffer.retainedBytes != 2 {
					t.Fatal("failed input changed accepted state or lost termination cause")
				}
				if buffer.input.ended != (name == "new_after_end") {
					t.Fatal("failed input changed end state")
				}
			})
		})
	}
}

func TestSessionWorkerIOFailuresAndStatusWait(t *testing.T) {
	for _, name := range []string{"send_eof", "send_error", "half_close_error"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				want := errors.New("controlled worker failure")
				if name == "send_eof" {
					want = errWorkerStatusTimeout
				}
				operations := make(chan string, 3)
				f := newWorkerCoordinatorFixture(t, 2, 1, func(context.Context) workerStream {
					return &uploadTestStream{
						send: func(*asrv1.StreamingRecognizeRequest) error {
							operations <- "send"
							if name == "half_close_error" {
								return nil
							}
							if name == "send_eof" {
								return io.EOF
							}
							return want
						},
						closeSend: func() error { operations <- "close"; return want },
					}
				})
				buffer := f.worker.input
				f.start()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil) // 接纳成功不等于发送成功。
				if name == "half_close_error" {
					a, n, err = f.session.requestEnd(context.Background(), 1, 2)
					assertCoordinatorInput(t, a, n, err, true, 2, nil)
				}
				if name == "send_eof" {
					time.Sleep(time.Second)
				}
				f.assertFinishedAtCurrentTime(t, want)
				wantOps, wantCount := 1, 1
				if name == "half_close_error" {
					wantOps, wantCount = 2, 0
				}
				if len(operations) != wantOps || buffer.count != wantCount || !errors.Is(context.Cause(f.rpcCtx), want) {
					t.Fatal("I/O failure retried, incorrectly completed audio or lost error")
				}
			})
		})
	}
}

func TestSessionWorkerCanceledInputDoesNotMutate(t *testing.T) {
	for _, kind := range []sessionControlKind{controlAudio, controlEnd} {
		for _, direct := range []bool{false, true} {
			name := "audio"
			if kind == controlEnd {
				name = "end"
			}
			if direct {
				name += "_delivered"
			} else {
				name += "_before_delivery"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					halfClosed := make(chan struct{})
					operations := make(chan string, 3)
					f := newWorkerCoordinatorFixture(t, 2, 1, func(context.Context) workerStream {
						return &uploadTestStream{
							send:      func(*asrv1.StreamingRecognizeRequest) error { operations <- "send"; return nil },
							closeSend: func() error { operations <- "close"; close(halfClosed); return nil },
						}
					})
					f.start()
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					cmd := sessionControlCommand{kind: kind, generation: 1, offset: 0, payload: []byte("ab")}
					var result sessionControlResult
					if direct {
						reply := make(chan sessionControlResult, 1)
						cmd.ctx, cmd.reply = ctx, reply
						f.session.commands <- cmd
						result = <-reply
						if len(reply) != 0 {
							t.Fatal("multiple command replies")
						}
					} else {
						result = f.session.submitCommand(ctx, cmd)
					}
					assertCoordinatorInput(t, result.accepted, result.nextOffset, result.err, false, 0, context.Canceled)
					a, n, err := f.session.requestEnd(context.Background(), 1, 0)
					assertCoordinatorInput(t, a, n, err, true, 0, nil)
					<-halfClosed
					if err := f.session.requestClose(context.Background()); err != nil {
						t.Fatal(err)
					}
					f.assertFinishedAtCurrentTime(t, nil)
					if len(operations) != 1 || <-operations != "close" {
						t.Fatal("canceled input performed I/O")
					}
				})
			})
		}
	}
}

func TestSessionWorkerDeliveredCallerWaitsForReplyAfterCancel(t *testing.T) {
	for _, kind := range []sessionControlKind{controlAudio, controlEnd} {
		name := "audio"
		if kind == controlEnd {
			name = "end"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				returned := make(chan sessionControlResult, 1)
				go func() {
					var a bool
					var n uint64
					var err error
					if kind == controlAudio {
						a, n, err = s.requestAudio(ctx, 1, 0, []byte("ab"))
					} else {
						a, n, err = s.requestEnd(ctx, 1, 2)
					}
					returned <- sessionControlResult{accepted: a, nextOffset: n, err: err}
				}()
				cmd := <-s.commands // 接收夹具仅验证提交契约，不冒充真实接纳。
				cancel()
				synctest.Wait()
				select {
				case <-returned:
					t.Fatal("delivered request returned before its unique reply")
				default:
				}
				if cmd.kind != kind || cap(cmd.reply) != 1 {
					t.Fatal("incorrect submitted command")
				}
				cmd.reply <- sessionControlResult{accepted: true, nextOffset: 2}
				result := <-returned
				assertCoordinatorInput(t, result.accepted, result.nextOffset, result.err, true, 2, nil)
			})
		})
	}
}

func TestSessionWorkerPendingDeliveryKeepsControlResponsive(t *testing.T) {
	for _, name := range []string{"explicit_close", "lifecycle_cancel"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newWorkerCoordinatorFixture(t, 2, 1, func(context.Context) workerStream { return &uploadTestStream{} })
				// 刻意不启动 uploader，使 jobs 没有接收者，精确覆盖 pending 阶段。
				// 调用真正 runCoordinator；本夹具负责取消 RPC，不使用 runWithWorker 收尾。
				returned := make(chan error, 1)
				go func() { returned <- f.session.runCoordinator(f.lifeCtx, time.Now, f.worker) }()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
					t.Fatalf("pending detach = (%v, %v)", d, err)
				}
				if g, err := f.session.requestResume(context.Background(), 0); g != 2 || err != nil {
					t.Fatalf("pending resume = (%d, %v)", g, err)
				}
				var want error
				if name == "explicit_close" {
					if err := f.session.requestClose(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else {
					want = errors.New("pending cancellation")
					f.cancelLife(want)
				}
				if got := <-returned; !errors.Is(got, want) {
					t.Fatalf("pending exit = %v", got)
				}
				if !f.worker.input.inFlight || f.worker.input.retainedBytes != 2 || len(f.worker.uploader.results) != 0 {
					t.Fatal("pending task released budget or fabricated a result")
				}
			})
		})
	}
}

func TestSessionWorkerMismatchedResultDoesNotComplete(t *testing.T) {
	for _, name := range []string{"wrong_kind", "wrong_offset"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newWorkerCoordinatorFixture(t, 2, 1, func(context.Context) workerStream { return &uploadTestStream{} })
				// 用一次性通道替身注入损坏的内部结果，不调用真实 Worker I/O。
				returned := make(chan error, 1)
				go func() { returned <- f.session.runCoordinator(f.lifeCtx, time.Now, f.worker) }()
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				assertCoordinatorInput(t, a, n, err, true, 2, nil)
				job := <-f.worker.uploader.jobs
				result := workerUploadResult{kind: job.kind, offset: job.chunk.offset}
				if name == "wrong_kind" {
					result.kind = workerUploadCloseSend
				} else {
					result.offset++
				}
				f.worker.uploader.results <- result
				if got := <-returned; !errors.Is(got, errWorkerUploadResultMismatch) {
					t.Fatalf("mismatch = %v", got)
				}
				if !f.worker.input.inFlight || f.worker.input.count != 1 || f.worker.input.retainedBytes != 2 {
					t.Fatal("bad result released budget")
				}
			})
		})
	}
}

func TestSessionWorkerSuccessfulCompletionMakesBudgetReusable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release, halfClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		sent := make(chan string, 3)
		first := true
		f := newWorkerCoordinatorFixture(t, 2, 1, func(ctx context.Context) workerStream {
			return &uploadTestStream{
				send: func(req *asrv1.StreamingRecognizeRequest) error {
					if first {
						first = false
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
							return context.Cause(ctx)
						}
					}
					sent <- string(req.Data)
					return nil
				},
				closeSend: func() error { close(halfClosed); return nil },
			}
		})
		f.start()
		a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, true, 2, nil)
		<-entered
		a, n, err = f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, a, n, err, false, 2, nil) // 满额时重复块仍成功。
		unblock()
		synctest.Wait() // 等协调者处理结果，而非仅观察 Send 已返回。
		a, n, err = f.session.requestAudio(context.Background(), 1, 2, []byte("cd"))
		assertCoordinatorInput(t, a, n, err, true, 4, nil)
		a, n, err = f.session.requestEnd(context.Background(), 1, 4)
		assertCoordinatorInput(t, a, n, err, true, 4, nil)
		<-halfClosed
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
		if len(sent) != 2 || <-sent != "ab" || <-sent != "cd" {
			t.Fatal("duplicate send or incorrect FIFO")
		}
	})
}

func TestSessionWorkerConcurrentReplayHasOneAcceptance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		f := newWorkerCoordinatorFixture(t, 2, 1, func(ctx context.Context) workerStream {
			return &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { close(entered); <-ctx.Done(); return nil }}
		})
		f.start()
		start := make(chan struct{})
		results := make(chan sessionControlResult, 32)
		for range 32 {
			go func() {
				<-start
				a, n, err := f.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
				results <- sessionControlResult{accepted: a, nextOffset: n, err: err}
			}()
		}
		close(start)
		wins := 0
		for range 32 {
			got := <-results
			if got.err != nil || got.nextOffset != 2 {
				t.Fatalf("competing replay = %+v", got)
			}
			if got.accepted {
				wins++
			}
		}
		<-entered
		if wins != 1 {
			t.Fatalf("new acceptances = %d, want 1", wins)
		}
		if err := f.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.assertFinishedAtCurrentTime(t, nil)
	})
}

func TestSessionWorkerDifferentSessionsRemainIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocked := make(chan struct{})
		a := newWorkerCoordinatorFixture(t, 2, 1, func(ctx context.Context) workerStream {
			return &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { close(blocked); <-ctx.Done(); return nil }}
		})
		halfClosed := make(chan struct{})
		b := newWorkerCoordinatorFixture(t, 2, 1, func(context.Context) workerStream {
			return &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { return nil }, closeSend: func() error { close(halfClosed); return nil }}
		})
		a.start()
		b.start()
		ok, n, err := a.session.requestAudio(context.Background(), 1, 0, []byte("ab"))
		assertCoordinatorInput(t, ok, n, err, true, 2, nil)
		<-blocked
		ok, n, err = b.session.requestAudio(context.Background(), 1, 0, []byte("cd"))
		assertCoordinatorInput(t, ok, n, err, true, 2, nil)
		ok, n, err = b.session.requestEnd(context.Background(), 1, 2)
		assertCoordinatorInput(t, ok, n, err, true, 2, nil)
		<-halfClosed
		if err := a.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		a.assertFinishedAtCurrentTime(t, nil)
		assertControlAlive(t, b.session)
		if context.Cause(b.rpcCtx) != nil {
			t.Fatal("one session close canceled another RPC")
		}
		if err := b.session.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		b.assertFinishedAtCurrentTime(t, nil)
	})
}
