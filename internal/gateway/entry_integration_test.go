package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// v2StreamAdapter 仅为建流替身补齐 gRPC 客户端类型，业务 I/O 由真实运行器使用。
type v2StreamAdapter struct {
	grpc.ClientStream
	stream workerStream
}

func (s *v2StreamAdapter) Send(req *asrv1.StreamingRecognizeRequest) error  { return s.stream.Send(req) }
func (s *v2StreamAdapter) Recv() (*asrv1.StreamingRecognizeResponse, error) { return s.stream.Recv() }
func (s *v2StreamAdapter) CloseSend() error                                 { return s.stream.CloseSend() }

type v2OpenFunc func(context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error)

func (f v2OpenFunc) StreamingRecognize(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	return f(ctx)
}

func TestV2EntryRPCSurvivesRequestCompletion(t *testing.T) {
	life, cancelLife := context.WithCancel(context.Background())
	defer cancelLife()
	var rpc context.Context
	client := v2OpenFunc(func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		rpc = ctx
		return &v2StreamAdapter{stream: &uploadTestStream{}}, nil
	})
	g, err := New(life, &v2EntrySelector{client: client}, Config{V2: &V2Config{}})
	if err != nil {
		t.Fatal(err)
	}
	entry, cancelEntry := context.WithCancel(context.Background())
	w, err := g.prepareV2Worker(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer w.stopIO(nil)
	cancelEntry()
	// 等待入口取消的 AfterFunc 都结束，证明解除转发后不会有晚到的取消。
	if rpc.Err() != nil {
		t.Fatal("successful startup RPC inherited entry cancellation")
	}
	cancelLife()
	if rpc.Err() != context.Canceled {
		t.Fatal("RPC lost Gateway lifecycle")
	}
}

func TestV2EntryStartupRejectedAfterPreparation(t *testing.T) {
	for _, mode := range []string{"stop", "request_cancel"} {
		t.Run(mode, func(t *testing.T) {
			entry, cancel := context.WithCancel(context.Background())
			defer cancel()
			var g *Gateway
			var rpc context.Context
			client := v2OpenFunc(func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				rpc = ctx
				if mode == "stop" {
					g.StopAccepting()
				} else {
					cancel()
				}
				return &v2StreamAdapter{stream: &uploadTestStream{}}, nil
			})
			var err error
			g, err = New(context.Background(), &v2EntrySelector{client: client}, Config{V2: &V2Config{}})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.gate.tryEnterHandshake(); err != nil {
				t.Fatal(err)
			}
			candidate := &attachmentTestConn{}
			err = g.startV2(entry, newAttachmentCandidate(t, candidate))
			want := error(errGatewayStopping)
			if mode == "request_cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || !errors.Is(context.Cause(rpc), want) || g.Snapshot().ActiveSessions != 0 || candidate.closes.Load() != 0 || candidate.reads.Load() != 0 {
				t.Fatalf("startup rejection leaked or acquired resources: %v", err)
			}
			g.registry.mu.Lock()
			n := len(g.registry.entries)
			g.registry.mu.Unlock()
			if n != 0 {
				t.Fatal("rejected startup registered a session")
			}
			g.gate.leaveHandshake()
		})
	}
}

// 通过命令检查点控制最终提交；Mutex 等待不使用 synctest。
func TestV2EntryCoordinatorCommitBoundary(t *testing.T) {
	for _, mode := range []string{"stop_before_commit", "expiry_at_commit", "cancel_waiting_for_lock"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := newDuplexWorkerFixture(t, nil, nil)
			gate := testEntryGate(t, 1, 1)
			f.session.entryGate = gate
			var expired atomic.Bool
			var expiresAt time.Time
			f.now = func() time.Time {
				if expired.Load() {
					return expiresAt
				}
				return time.Now()
			}
			startManagedFixture(t, f, &attachmentTestConn{})
			if detached, err := f.session.reportDetach(context.Background(), 1); !detached || err != nil {
				t.Fatal(detached, err)
			}
			// 同步的恢复尝试只等待旧连接收割，不读取协调者可变状态。
			for {
				_, err := f.session.requestResumeConnection(context.Background(), newAttachmentCandidate(t, &attachmentTestConn{}), 1)
				if errors.Is(err, errResultAckAhead) {
					break
				}
				if !errors.Is(err, errConnectionRetiring) {
					t.Fatal("unexpected preparation status", err)
				}
				time.Sleep(time.Millisecond)
			}
			expiresAt = f.session.resume.expiresAt // 上个回复与下个命令建立同步。
			candidate := &attachmentTestConn{}
			request, cancel := context.WithCancel(context.Background())
			defer cancel()
			boundary := make(chan struct{})
			ctx := &resumeBoundaryContext{Context: request}
			if mode == "expiry_at_commit" {
				// 使用 Background，context.Cause 在 gate 内调用 Err，正好是第三次。
				ctx.Context = context.Background()
				ctx.hook = func(n int32) {
					if n == 3 {
						expired.Store(true)
					}
				}
			} else if mode == "stop_before_commit" {
				ctx.hook = func(n int32) {
					if n == 2 {
						gate.stopAccepting()
					}
				}
			} else {
				ctx.hook = func(n int32) {
					if n == 2 {
						close(boundary)
					}
				}
				gate.mu.Lock()
			}
			result := make(chan error, 1)
			go func() {
				_, err := f.session.requestResumeConnection(ctx, newAttachmentCandidate(t, candidate), 0)
				result <- err
			}()
			if mode == "cancel_waiting_for_lock" {
				select {
				case <-boundary:
				case <-time.After(5 * time.Second):
					gate.mu.Unlock()
					t.Fatal("command did not reach coordinator")
				}
				cancel()
				gate.mu.Unlock()
			}
			var err error
			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("commit did not reply")
			}
			want := error(errGatewayStopping)
			if mode == "expiry_at_commit" {
				want = errResumeClosed
			} else if mode == "cancel_waiting_for_lock" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || candidate.reads.Load() != 0 || candidate.writes.Load() != 0 || candidate.closes.Load() != 0 {
				t.Fatalf("candidate changed ownership: %v", err)
			}
			if mode == "expiry_at_commit" {
				select {
				case <-f.finished:
				case <-time.After(5 * time.Second):
					t.Fatal("expiry did not terminate session")
				}
				if !errors.Is(f.err, errResumeExpired) || f.session.resume.generation != 1 {
					t.Fatal("expiry mutated generation or lost cause", f.err)
				}
			} else {
				if context.Cause(f.rpcCtx) != nil {
					t.Fatal("candidate rejection canceled original RPC")
				}
				if err := f.session.requestClose(context.Background()); err != nil {
					t.Fatal(err)
				}
				<-f.finished
				if f.session.resume.generation != 1 {
					t.Fatal("rejection advanced generation")
				}
			}
		})
	}
}

func TestV2EntryWaitRetainsRegistryUntilActualCleanup(t *testing.T) {
	recvEntered, recvCanceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	client := v2OpenFunc(func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return &v2StreamAdapter{stream: &uploadTestStream{recv: func() (*asrv1.StreamingRecognizeResponse, error) {
			close(recvEntered)
			<-ctx.Done()
			close(recvCanceled)
			<-release
			return nil, context.Cause(ctx)
		}}}, nil
	})
	f := newV2EntryFixture(t, client, Config{V2: &V2Config{}})
	t.Cleanup(unblock)
	c := f.dial(t, "/v2/asr")
	v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
	var ready wsprotocol.ReadyMessage
	v2ReadJSON(t, f.ctx, c, &ready)
	f.waitReturn(t)
	s, _ := f.g.registry.lookup(ready.SessionID)
	select {
	case <-recvEntered:
	case <-f.ctx.Done():
		t.Fatal("receiver did not enter actual Recv")
	}
	f.g.StopAccepting()
	f.cancel()
	select {
	case <-recvCanceled:
	case <-f.ctx.Done():
		t.Fatal("Recv did not observe cancellation")
	}
	select {
	case <-s.controlDone:
	case <-f.ctx.Done():
		t.Fatal("coordinator did not exit")
	}
	if _, ok := f.g.registry.lookup(ready.SessionID); !ok || f.g.Snapshot().ActiveSessions != 1 {
		t.Fatal("controlDone released registration before real cleanup")
	}
	waitCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if err := f.g.Wait(waitCtx); err != context.DeadlineExceeded {
		t.Fatal("Wait did not retain pending resources", err)
	}
	unblock()
	if err := f.g.Wait(f.ctx); err != nil {
		t.Fatal(err)
	}
	v2RequireIdle(t, f)
}
