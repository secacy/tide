package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// uploadTestStream 将 I/O 交给可控替身；未配置的方法不应被调用。
type uploadTestStream struct {
	send      func(*asrv1.StreamingRecognizeRequest) error
	closeSend func() error
	recv      func() (*asrv1.StreamingRecognizeResponse, error)
}

func (s *uploadTestStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	if s.send == nil {
		panic("unexpected Send")
	}
	return s.send(req)
}

func (s *uploadTestStream) CloseSend() error {
	if s.closeSend == nil {
		panic("unexpected CloseSend")
	}
	return s.closeSend()
}

func (s *uploadTestStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	if s.recv == nil {
		panic("uploader must not receive")
	}
	return s.recv()
}

// startTestWorkerUploader 在 synctest 中启动唯一上传任务，结束时取消并等待退出。
// ctx 和 cancel 必须属于同一个 RPC；测试包装 context 时仍使用原 RPC 的 cancel。
func startTestWorkerUploader(t *testing.T, u *workerUploader, ctx context.Context, cancel context.CancelCauseFunc, stream workerStream) {
	t.Helper()
	go u.run(ctx, cancel, stream, time.Second)
	t.Cleanup(func() {
		cancel(nil)
		<-u.done
	})
}

func assertTestWorkerUploadResult(t *testing.T, got workerUploadResult, kind workerUploadKind, offset uint64, err error) {
	t.Helper()
	if got.kind != kind || got.offset != offset || !errors.Is(got.err, err) {
		t.Fatalf("upload result = %+v, want kind=%d offset=%d err=%v", got, kind, offset, err)
	}
}

// assertTestWorkerUploaderExited 只检查当前阶段已经退出，不等待未来期限掩盖问题。
func assertTestWorkerUploaderExited(t *testing.T, u *workerUploader) {
	t.Helper()
	synctest.Wait()
	select {
	case <-u.done:
	default:
		t.Fatal("uploader has not exited at the expected stage")
	}
}

func TestWorkerUploaderConstruction(t *testing.T) {
	a, b := newWorkerUploader(), newWorkerUploader()
	if a.jobs == nil || a.results == nil || a.done == nil || cap(a.jobs) != 0 || cap(a.results) != 1 {
		t.Fatal("incorrect uploader channels or capacities")
	}
	if a.jobs == b.jobs || a.results == b.results || a.done == b.done {
		t.Fatal("uploader instances must not share channels")
	}
	select {
	case <-a.done:
		t.Fatal("construction must not finish an upload task")
	case a.jobs <- workerUploadCommand{}:
		t.Fatal("construction must not start an upload task")
	default:
	}
}

func TestWorkerUploaderInvalidParameters(t *testing.T) {
	for _, name := range []string{"nil_context", "nil_cancel", "nil_stream", "zero_timeout", "negative_timeout"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			u := newWorkerUploader()
			var rpcCtx context.Context = ctx
			var rpcCancel context.CancelCauseFunc = cancel
			var stream workerStream = &uploadTestStream{}
			timeout := time.Second
			switch name {
			case "nil_context":
				rpcCtx = nil
			case "nil_cancel":
				rpcCancel = nil
			case "nil_stream":
				stream = nil
			case "zero_timeout":
				timeout = 0
			case "negative_timeout":
				timeout = -time.Second
			}
			defer func() {
				if recover() == nil {
					t.Fatal("invalid startup must panic before I/O")
				}
			}()
			u.run(rpcCtx, rpcCancel, stream, timeout)
		})
	}
}

func TestWorkerUploaderBufferFIFOAndHalfClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		u := newWorkerUploader()
		b := newTestAudioInputBuffer(t, 9, 3)
		chunks := []string{"ab", "cde", "f"}
		var requests []*asrv1.StreamingRecognizeRequest
		var operations []string
		stream := &uploadTestStream{
			send: func(req *asrv1.StreamingRecognizeRequest) error {
				operations = append(operations, "send")
				requests = append(requests, req)
				return nil
			},
			closeSend: func() error { operations = append(operations, "close"); return nil },
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return &asrv1.StreamingRecognizeResponse{Text: "tail", IsFinal: true}, nil
			},
		}
		startTestWorkerUploader(t, u, ctx, cancel, stream)
		for _, data := range chunks {
			if accepted, err := b.offer(b.input.nextOffset, []byte(data)); !accepted || err != nil {
				t.Fatalf("buffer offer = (%v, %v)", accepted, err)
			}
		}
		if accepted, err := b.acceptEnd(6); !accepted || err != nil {
			t.Fatalf("end = (%v, %v)", accepted, err)
		}
		for _, data := range chunks {
			chunk, ok := b.take()
			if !ok || !bytes.Equal(chunk.data, []byte(data)) {
				t.Fatalf("incorrect FIFO borrow: %+v, ok=%v", chunk, ok)
			}
			u.jobs <- workerUploadCommand{kind: workerUploadAudio, chunk: chunk}
			got := <-u.results // 处理结果后才交付下一任务。
			assertTestWorkerUploadResult(t, got, workerUploadAudio, chunk.offset, nil)
			if err := b.complete(got.offset); err != nil {
				t.Fatal(err)
			}
			assertTestAudioBufferInvariant(t, b)
		}
		if !b.inputDrained() {
			t.Fatal("cannot half-close until every accepted chunk is complete")
		}
		u.jobs <- workerUploadCommand{kind: workerUploadCloseSend}
		assertTestWorkerUploaderExited(t, u) // 尚未消费半关闭结果也必须可以退出。
		assertTestWorkerUploadResult(t, <-u.results, workerUploadCloseSend, 0, nil)
		if len(operations) != 4 || operations[3] != "close" || len(requests) != 3 || context.Cause(ctx) != nil {
			t.Fatalf("incorrect operations or premature RPC cancellation: %v cause=%v", operations, context.Cause(ctx))
		}
		for i, req := range requests {
			if !bytes.Equal(req.GetData(), []byte(chunks[i])) {
				t.Fatal("sent request data was modified after release")
			}
			for j := range i {
				if req == requests[j] {
					t.Fatal("each Send must use a separate request object")
				}
			}
		}
		response, err := stream.Recv() // 仅夹具接收尾部；uploader 本身不调用 Recv。
		if err != nil || response.GetText() != "tail" || !response.GetIsFinal() {
			t.Fatalf("RPC no longer permits tail reception: response=%v err=%v", response, err)
		}
		if len(u.results) != 0 {
			t.Fatal("an operation produced more than one result")
		}
	})
}

func TestWorkerUploaderBlockedSendRetainsBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		u := newWorkerUploader()
		b := newTestAudioInputBuffer(t, 3, 1)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		stream := &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
			close(entered)
			<-release
			return nil
		}}
		startTestWorkerUploader(t, u, ctx, cancel, stream)
		if accepted, err := b.offer(0, []byte("abc")); !accepted || err != nil {
			t.Fatalf("offer = (%v, %v)", accepted, err)
		}
		chunk, ok := b.take()
		if !ok {
			t.Fatal("no chunk available")
		}
		u.jobs <- workerUploadCommand{kind: workerUploadAudio, chunk: chunk}
		<-entered
		synctest.Wait()
		// 主测试扮演串行协调夹具，阻塞 Send 期间仍能处理另一个控制事件。
		controlEvent := make(chan struct{})
		go func() { controlEvent <- struct{}{} }()
		<-controlEvent
		before := snapshotTestAudioBuffer(b)
		if b.retainedBytes != 3 || b.count != 1 || !b.inFlight {
			t.Fatal("blocked Send released borrowed storage")
		}
		if accepted, err := b.offer(3, []byte("d")); accepted || !errors.Is(err, errAudioBufferFull) {
			t.Fatalf("in-flight budget bypassed: (%v, %v)", accepted, err)
		}
		assertTestAudioBufferUnchanged(t, b, before)
		select {
		case result := <-u.results:
			t.Fatalf("Send not finished but result appeared: %+v", result)
		default:
		}
		unblock()
		got := <-u.results
		assertTestWorkerUploadResult(t, got, workerUploadAudio, 0, nil)
		assertTestAudioBufferUnchanged(t, b, before) // 上传任务不能自行 complete。
		if err := b.complete(got.offset); err != nil {
			t.Fatal(err)
		}
		if accepted, err := b.offer(3, []byte("d")); !accepted || err != nil {
			t.Fatalf("completed budget could not be reused: (%v, %v)", accepted, err)
		}
		assertTestAudioBufferInvariant(t, b)
	})
}

func TestWorkerUploaderSendFailureDoesNotReleaseOrRetry(t *testing.T) {
	workerErr := errors.New("worker rejected upload")
	for _, tc := range []struct {
		name string
		err  error
	}{{"eof", io.EOF}, {"worker_error", workerErr}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				u := newWorkerUploader()
				b := newTestAudioInputBuffer(t, 3, 1)
				calls := 0
				stream := &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { calls++; return tc.err }}
				startTestWorkerUploader(t, u, ctx, cancel, stream)
				if accepted, err := b.offer(0, []byte("abc")); !accepted || err != nil {
					t.Fatalf("offer = (%v, %v)", accepted, err)
				}
				chunk, ok := b.take()
				if !ok {
					t.Fatal("no borrowed chunk")
				}
				before := snapshotTestAudioBuffer(b)
				u.jobs <- workerUploadCommand{kind: workerUploadAudio, chunk: chunk}
				assertTestWorkerUploaderExited(t, u)
				assertTestWorkerUploadResult(t, <-u.results, workerUploadAudio, 0, tc.err)
				if calls != 1 || context.Cause(ctx) != nil || len(u.results) != 0 {
					t.Fatalf("unexpected retry, cancellation or extra result: calls=%d cause=%v", calls, context.Cause(ctx))
				}
				assertTestAudioBufferUnchanged(t, b, before)
			})
		})
	}
}

func TestWorkerUploaderTimeoutPublishesBeforeExit(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{{"send_returns_nil", nil}, {"send_returns_eof", io.EOF}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				u := newWorkerUploader()
				b := newTestAudioInputBuffer(t, 3, 1)
				entered := make(chan struct{})
				returned := make(chan struct{})
				calls := 0
				stream := &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
					calls++
					close(entered)
					<-ctx.Done()
					time.Sleep(10 * time.Millisecond) // 虚拟时间：底层取消后的收尾。
					close(returned)
					return tc.err
				}}
				startTestWorkerUploader(t, u, ctx, cancel, stream)
				if accepted, err := b.offer(0, []byte("abc")); !accepted || err != nil {
					t.Fatalf("offer = (%v, %v)", accepted, err)
				}
				chunk, ok := b.take()
				if !ok {
					t.Fatal("no borrowed chunk")
				}
				before := snapshotTestAudioBuffer(b)
				start := time.Now()
				u.jobs <- workerUploadCommand{kind: workerUploadAudio, chunk: chunk}
				<-entered
				time.Sleep(time.Second)
				synctest.Wait()
				if !errors.Is(context.Cause(ctx), ErrWorkerSendTimeout) || len(u.results) != 0 {
					t.Fatal("timeout must cancel RPC but still wait for Send to return")
				}
				select {
				case <-returned:
					t.Fatal("underlying Send returned before its cancellation cleanup")
				default:
				}
				select {
				case <-u.done:
					t.Fatal("uploader exited while underlying Send was still running")
				default:
				}
				time.Sleep(10 * time.Millisecond)
				assertTestWorkerUploaderExited(t, u) // 无人读 results 时也应发布并退出。
				if calls != 1 || time.Since(start) != 1010*time.Millisecond || len(u.results) != 1 {
					t.Fatal("timeout result lost, Send retried or exit occurred at wrong stage")
				}
				select {
				case <-returned:
				default:
					t.Fatal("uploader exited before Send returned")
				}
				assertTestWorkerUploadResult(t, <-u.results, workerUploadAudio, 0, ErrWorkerSendTimeout)
				assertTestAudioBufferUnchanged(t, b, before)
			})
		})
	}
}

func TestWorkerUploaderIdleCancellation(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "while_waiting"
		if before {
			name = "before_start"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				u := newWorkerUploader()
				cause := errors.New("logical session stopped")
				if before {
					cancel(cause)
				}
				startTestWorkerUploader(t, u, ctx, cancel, &uploadTestStream{})
				if !before {
					synctest.Wait()
					cancel(cause)
				}
				assertTestWorkerUploaderExited(t, u)
				if len(u.results) != 0 || context.Cause(ctx) != cause {
					t.Fatal("idle cancellation must not invent a job result")
				}
			})
		})
	}
}

func TestWorkerUploaderCancellationDuringSendKeepsCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		u := newWorkerUploader()
		entered := make(chan struct{})
		stream := &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
			close(entered)
			<-ctx.Done()
			return nil // 底层返回 nil 不能掩盖已经发生的 RPC 取消。
		}}
		startTestWorkerUploader(t, u, ctx, cancel, stream)
		u.jobs <- workerUploadCommand{kind: workerUploadAudio, chunk: bufferedAudio{offset: 37, data: []byte("x")}}
		<-entered
		cause := errors.New("recovery expired")
		cancel(cause)
		assertTestWorkerUploaderExited(t, u)
		assertTestWorkerUploadResult(t, <-u.results, workerUploadAudio, 37, cause)
	})
}

// uploadTestGatedContext 仅在测试中暂停第二次 Cause 的 Err 查询。
// 第一次发生在等待任务前，第二次发生在已接收任务后的取消检查。
// 仍使用真实父 context 的取消语义；生产 context 不应这样阻塞。
type uploadTestGatedContext struct {
	context.Context
	entered, release chan struct{}
	errCalls         int // 仅上传任务访问。
}

func (c *uploadTestGatedContext) Err() error {
	c.errCalls++
	if c.errCalls == 2 {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}

func TestWorkerUploaderAcceptedCancellationBeforeIO(t *testing.T) {
	for _, kind := range []workerUploadKind{workerUploadAudio, workerUploadInvalid} {
		name := "valid_audio"
		if kind == workerUploadInvalid {
			name = "cancellation_before_invalid_command_check"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				u := newWorkerUploader()
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				wrapped := &uploadTestGatedContext{Context: ctx, entered: entered, release: release}
				startTestWorkerUploader(t, u, wrapped, cancel, &uploadTestStream{})
				u.jobs <- workerUploadCommand{kind: kind, chunk: bufferedAudio{offset: 11, data: []byte("x")}}
				<-entered
				cause := errors.New("cancel accepted task before I/O")
				cancel(cause)
				unblock()
				assertTestWorkerUploaderExited(t, u)
				assertTestWorkerUploadResult(t, <-u.results, kind, 11, cause)
			})
		})
	}
}

func TestWorkerUploaderCancellationAfterSuccessKeepsResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		u := newWorkerUploader()
		calls := 0
		stream := &uploadTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { calls++; return nil }}
		startTestWorkerUploader(t, u, ctx, cancel, stream)
		u.jobs <- workerUploadCommand{kind: workerUploadAudio, chunk: bufferedAudio{offset: 8, data: []byte("x")}}
		synctest.Wait() // 成功结果已发布，任务在等待下一次交付。
		if len(u.results) != 1 {
			t.Fatal("success not published")
		}
		cancel(errors.New("later shutdown"))
		assertTestWorkerUploaderExited(t, u)
		assertTestWorkerUploadResult(t, <-u.results, workerUploadAudio, 8, nil)
		if calls != 1 || len(u.results) != 0 {
			t.Fatal("cancellation replaced or duplicated committed success")
		}
	})
}

func TestWorkerUploaderHalfCloseErrorAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "close_error"
		if canceled {
			name = "canceled_during_close"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				u := newWorkerUploader()
				closeErr, cause := errors.New("close error"), errors.New("RPC canceled in close")
				calls := 0
				stream := &uploadTestStream{closeSend: func() error {
					calls++
					if canceled {
						cancel(cause)
					}
					return closeErr
				}}
				startTestWorkerUploader(t, u, ctx, cancel, stream)
				u.jobs <- workerUploadCommand{kind: workerUploadCloseSend}
				assertTestWorkerUploaderExited(t, u)
				want := closeErr
				if canceled {
					want = cause
				}
				assertTestWorkerUploadResult(t, <-u.results, workerUploadCloseSend, 0, want)
				if calls != 1 || (!canceled && context.Cause(ctx) != nil) {
					t.Fatal("half-close retried or canceled RPC unexpectedly")
				}
			})
		})
	}
}

func TestWorkerUploaderInvalidCommands(t *testing.T) {
	for _, tc := range []struct {
		name string
		job  workerUploadCommand
	}{
		{"zero_kind", workerUploadCommand{}},
		{"unknown_kind", workerUploadCommand{kind: 255}},
		{"nil_audio", workerUploadCommand{kind: workerUploadAudio}},
		{"empty_audio", workerUploadCommand{kind: workerUploadAudio, chunk: bufferedAudio{data: []byte{}}}},
		{"close_with_offset", workerUploadCommand{kind: workerUploadCloseSend, chunk: bufferedAudio{offset: 1}}},
		{"close_with_payload", workerUploadCommand{kind: workerUploadCloseSend, chunk: bufferedAudio{data: []byte("x")}}},
		{"close_with_non_nil_empty_payload", workerUploadCommand{kind: workerUploadCloseSend, chunk: bufferedAudio{data: []byte{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				u := newWorkerUploader()
				startTestWorkerUploader(t, u, ctx, cancel, &uploadTestStream{})
				u.jobs <- tc.job
				assertTestWorkerUploaderExited(t, u)
				assertTestWorkerUploadResult(t, <-u.results, tc.job.kind, tc.job.chunk.offset, errInvalidWorkerUploadCommand)
				if context.Cause(ctx) != nil || len(u.results) != 0 {
					t.Fatal("invalid command unexpectedly canceled RPC or replied twice")
				}
			})
		})
	}
}
