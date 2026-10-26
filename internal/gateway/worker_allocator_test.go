package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/realasr"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type allocatorFunc func() (*workerpool.WorkerLease, error)

func (f allocatorFunc) TryAcquire() (*workerpool.WorkerLease, error) { return f() }

// countedAllocator 保持真实池语义，单独记录 Gateway 的申请次数。
type countedAllocator struct {
	pool  *workerpool.CapacityRoundRobin
	calls atomic.Int32
}

func (a *countedAllocator) TryAcquire() (*workerpool.WorkerLease, error) {
	a.calls.Add(1)
	return a.pool.TryAcquire()
}

func newLeaseAllocator(t *testing.T, clients ...asrv1.ASRServiceClient) *countedAllocator {
	t.Helper()
	config := make([]workerpool.WorkerCapacity, len(clients))
	for i, client := range clients {
		config[i] = workerpool.WorkerCapacity{Worker: workerpool.Worker{ID: string(rune('A' + i)), Client: client}, Limit: 1}
	}
	pool, err := workerpool.NewCapacityRoundRobin(config)
	if err != nil {
		t.Fatal(err)
	}
	return &countedAllocator{pool: pool}
}

func requireLeaseOccupied(t *testing.T, pool *workerpool.CapacityRoundRobin) {
	t.Helper()
	lease, err := pool.TryAcquire()
	if lease != nil {
		lease.Release()
		t.Fatal("local reservation was returned too early")
	}
	if !errors.Is(err, workerpool.ErrNoWorkerCapacity) {
		t.Fatalf("unexpected capacity error: %v", err)
	}
}

func requireLeaseReusable(t *testing.T, pool *workerpool.CapacityRoundRobin) {
	t.Helper()
	lease, err := pool.TryAcquire()
	if err != nil || lease == nil {
		t.Fatalf("reservation leaked: %v", err)
	}
	lease.Release()
}

// newLeaseGatewayFixture 提供真实 HTTP/WebSocket，后端可为 TCP 服务或可控流。
// 清理先停服并等待 Gateway，再关 HTTP；不以强制清理结果代替测试阶段断言。
func newLeaseGatewayFixture(t *testing.T, allocator WorkerAllocator, cfg Config) *v2EntryFixture {
	t.Helper()
	life, cancel := context.WithCancel(context.Background())
	g, err := NewWithAllocator(life, allocator, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	f := &v2EntryFixture{g: g, cancel: cancel, returned: make(chan struct{}, 64)}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	f.ctx = ctx
	t.Cleanup(stop)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { f.returned <- struct{}{} }()
		if r.URL.Path == "/v1/asr" {
			g.ServeHTTP(w, r)
		} else {
			g.ServeV2HTTP(w, r)
		}
	}))
	f.url = "ws" + strings.TrimPrefix(server.URL, "http")
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := g.Wait(ctx); err != nil {
			t.Errorf("lease fixture cleanup: %v", err)
		}
		server.Close()
	})
	return f
}

func startLeaseV2(t *testing.T, f *v2EntryFixture) (*websocket.Conn, wsprotocol.ReadyMessage) {
	t.Helper()
	c := f.dial(t, "/v2/asr")
	v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
	var ready wsprotocol.ReadyMessage
	v2ReadJSON(t, f.ctx, c, &ready)
	if ready.Type != wsprotocol.MessageTypeReady {
		t.Fatal("expected ready without exposing credentials")
	}
	f.waitReturn(t)
	return c, ready
}

func finishLeaseV2(t *testing.T, f *v2EntryFixture, c *websocket.Conn, generation, offset, seq uint64) {
	t.Helper()
	v2WriteJSON(t, f.ctx, c, map[string]string{"type": "end", "finalOffset": "2"})
	v2ReadResult(t, f.ctx, c, seq, "complete")
	readCompletionNetwork(t, f.ctx, c, generation, offset, seq)
	v2WriteJSON(t, f.ctx, c, wsprotocol.CompletedAckMessage{Type: wsprotocol.MessageTypeCompletedAck, FinalOffset: offset, LastSeq: seq})
}

func TestGatewayAllocatorConstructionAndLegacyAdapter(t *testing.T) {
	panicAcquire := allocatorFunc(func() (*workerpool.WorkerLease, error) { panic("constructor must not acquire") })
	for _, tc := range []struct {
		name      string
		allocator WorkerAllocator
		bad       bool
	}{
		{"nil", nil, true}, {"typed_nil", (*countedAllocator)(nil), true},
		{"nil_func", allocatorFunc(nil), true}, {"value", panicAcquire, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := NewWithAllocator(context.Background(), tc.allocator, Config{V2: &V2Config{}})
			if (err != nil) != tc.bad || (g == nil) != tc.bad {
				t.Fatalf("constructor: gateway=%v err=%v", g, err)
			}
		})
	}
	if g, err := NewWithAllocator(nil, panicAcquire, Config{}); err == nil || g != nil {
		t.Fatal("accepted nil context")
	}
	for _, cfg := range []Config{{}, {V2: &V2Config{}}, {MaxSessions: -1}, {WorkerSendTimeout: -1}, {V2: &V2Config{MaxAudioChunks: -1}}} {
		legacy, e1 := New(context.Background(), constructorOnlyValue{}, cfg)
		current, e2 := NewWithAllocator(context.Background(), panicAcquire, cfg)
		if (e1 == nil) != (e2 == nil) {
			t.Fatal("constructor configuration validation diverged")
		}
		if e1 == nil && (!reflect.DeepEqual(legacy.cfg, current.cfg) || !reflect.DeepEqual(legacy.v2, current.v2)) {
			t.Fatal("constructor defaults diverged")
		}
	}
	selector := &v2EntrySelector{client: &recordingWorker{}}
	adapter := selectorAllocator{selector: selector}
	first, _ := adapter.TryAcquire()
	second, _ := adapter.TryAcquire()
	if first == second || first.Worker() != second.Worker() || selector.picks.Load() != 2 {
		t.Fatal("legacy adapter changed Pick semantics")
	}
	first.Release()
	first.Release()
	second.Release()
}

func TestGatewayLeaseV2AdmissionLayers(t *testing.T) {
	for _, max := range []int{1, 2} {
		t.Run(map[int]string{1: "session_limit", 2: "worker_capacity"}[max], func(t *testing.T) {
			backend := &v2EntryBackend{}
			a := newLeaseAllocator(t, newBaselineTCPWorkerClient(t, backend))
			f := newLeaseGatewayFixture(t, a, Config{MaxSessions: max, V2: &V2Config{}})
			c, _ := startLeaseV2(t, f)
			if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
				t.Fatal(err)
			}
			v2ReadResult(t, f.ctx, c, 1, "ab")
			rejected := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, rejected, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
			code, text, wantCalls := wsprotocol.V2ErrorWorkerUnavailable, "worker unavailable", int32(2)
			if max == 1 {
				code, text, wantCalls = wsprotocol.V2ErrorSessionLimit, "session limit exceeded", 1
			}
			v2ExpectError(t, f, rejected, code, text)
			if a.calls.Load() != wantCalls || backend.calls.Load() != 1 {
				t.Fatal("rejection opened an extra RPC or selected past session limit")
			}
			requireLeaseOccupied(t, a.pool)
			finishLeaseV2(t, f, c, 1, 2, 2)
			v2RequireIdle(t, f)
			requireLeaseReusable(t, a.pool)
		})
	}
}

func TestGatewayLeaseV2PreparationRollback(t *testing.T) {
	for _, mode := range []string{"open_failure", "entry_cancel", "gateway_cancel", "worker_config", "success_handoff"} {
		t.Run(mode, func(t *testing.T) {
			life, cancelLife := context.WithCancel(context.Background())
			defer cancelLife()
			entry, cancelEntry := context.WithCancel(context.Background())
			defer cancelEntry()
			want := errors.New("injected open failure")
			var rpc context.Context
			client := v2OpenFunc(func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				rpc = ctx
				switch mode {
				case "open_failure":
					return nil, want
				case "entry_cancel":
					cancelEntry()
				case "gateway_cancel":
					cancelLife()
				}
				return &v2StreamAdapter{stream: &uploadTestStream{}}, nil
			})
			a := newLeaseAllocator(t, client)
			g, err := NewWithAllocator(life, a, Config{V2: &V2Config{}})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "worker_config" {
				g.v2.MaxAudioChunks = -1
			}
			w, err := g.prepareV2Worker(entry)
			if mode == "success_handoff" {
				if err != nil || w == nil || rpc.Err() != nil {
					t.Fatalf("preparation: %v", err)
				}
				cancelEntry()
				if rpc.Err() != nil {
					t.Fatal("entry cancellation reached handed-off RPC")
				}
				requireLeaseOccupied(t, a.pool)
				w.stopIO(nil)
				w.releaseBuffers()
				w.releaseLease()
				w.releaseLease()
			} else {
				if err == nil || w != nil || rpc.Err() == nil {
					t.Fatalf("rollback: worker=%v err=%v", w, err)
				}
				if mode == "open_failure" && (!errors.Is(err, want) || !errors.Is(err, errV2WorkerUnavailable)) {
					t.Fatal("lost open failure identity")
				}
			}
			requireLeaseReusable(t, a.pool)
		})
	}
	t.Run("full_error_identity", func(t *testing.T) {
		a := newLeaseAllocator(t, &recordingWorker{})
		lease, err := a.pool.TryAcquire()
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release()
		g, err := NewWithAllocator(context.Background(), a, Config{V2: &V2Config{}})
		if err != nil {
			t.Fatal(err)
		}
		w, err := g.prepareV2Worker(context.Background())
		if w != nil || !errors.Is(err, workerpool.ErrNoWorkerCapacity) || !errors.Is(err, errV2WorkerUnavailable) {
			t.Fatalf("capacity identity: %v", err)
		}
	})
}

func TestGatewayLeaseV2CommitFailureRollsBack(t *testing.T) {
	var g *Gateway
	var rpc context.Context
	client := v2OpenFunc(func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		rpc = ctx
		g.StopAccepting()
		return &v2StreamAdapter{stream: &uploadTestStream{}}, nil // 提交失败时未启动，I/O 方法不能被调用。
	})
	a := newLeaseAllocator(t, client)
	var err error
	g, err = NewWithAllocator(context.Background(), a, Config{V2: &V2Config{}})
	if err != nil {
		t.Fatal(err)
	}
	candidate := newAttachmentCandidate(t, &attachmentTestConn{})
	err = g.startV2(context.Background(), candidate)
	if !errors.Is(err, errGatewayStopping) || rpc.Err() == nil || g.Snapshot().ActiveSessions != 0 {
		t.Fatalf("commit rollback: %v", err)
	}
	requireLeaseReusable(t, a.pool)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayLeaseV2ResumeAndRetentionKeepReservation(t *testing.T) {
	backend := &v2EntryBackend{}
	a := newLeaseAllocator(t, newBaselineTCPWorkerClient(t, backend))
	f := newLeaseGatewayFixture(t, a, Config{MaxSessions: 2, V2: &V2Config{ResumeWindow: 2 * time.Second}})
	c, ready := startLeaseV2(t, f)
	if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
		t.Fatal(err)
	}
	v2ReadResult(t, f.ctx, c, 1, "ab")
	_ = c.CloseNow()
	waitInstanceDetached(t, f, ready)
	requireLeaseOccupied(t, a.pool)
	// Detached 并不保证旧连接已收割；只在明确 busy 时重建候选重试恢复。
	for {
		c = f.dial(t, "/v2/asr")
		v2WriteJSON(t, f.ctx, c, wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: ready.SessionID, ResumeToken: ready.ResumeToken, AppliedSeq: 1})
		var raw json.RawMessage
		v2ReadJSON(t, f.ctx, c, &raw)
		f.waitReturn(t)
		var tag wsprotocol.V2ErrorMessage
		if err := json.Unmarshal(raw, &tag); err != nil {
			t.Fatal(err)
		}
		if tag.Type == wsprotocol.MessageTypeReady {
			break
		}
		if tag.Code != wsprotocol.V2ErrorSessionBusy {
			t.Fatal("unexpected resume rejection")
		}
		_ = c.CloseNow()
	}
	requireLeaseOccupied(t, a.pool)
	v2WriteJSON(t, f.ctx, c, map[string]string{"type": "end", "finalOffset": "2"})
	v2ReadResult(t, f.ctx, c, 2, "complete")
	readCompletionNetwork(t, f.ctx, c, 2, 2, 2)
	// Worker 已完成，但客户端尚未确认整场完成，保留阶段仍持有名额。
	requireLeaseOccupied(t, a.pool)
	if a.calls.Load() != 1 || backend.calls.Load() != 1 {
		t.Fatal("resume reacquired capacity or reopened Worker RPC")
	}
	v2WriteJSON(t, f.ctx, c, wsprotocol.CompletedAckMessage{Type: wsprotocol.MessageTypeCompletedAck, FinalOffset: 2, LastSeq: 2})
	v2RequireIdle(t, f)
	requireLeaseReusable(t, a.pool)
}

func TestGatewayLeaseWaitsForLocalIOOnShutdown(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			opened := make(chan context.Context, 1)
			client := v2OpenFunc(func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				opened <- ctx
				return &v2StreamAdapter{stream: &uploadTestStream{
					send: func(*asrv1.StreamingRecognizeRequest) error {
						close(entered)
						<-ctx.Done()
						<-release
						return ctx.Err()
					},
					recv: func() (*asrv1.StreamingRecognizeResponse, error) { <-ctx.Done(); return nil, ctx.Err() },
				}}, nil
			})
			a := newLeaseAllocator(t, client)
			f := newLeaseGatewayFixture(t, a, Config{V2: &V2Config{}})
			defer unblock() // 先解除测试阻塞，再让 fixture 兜底清理。
			var c *websocket.Conn
			if version == "v2" {
				c, _ = startLeaseV2(t, f)
			} else {
				c = f.dial(t, "/v1/asr")
				v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v1"})
			}
			payload := []byte("ab")
			if version == "v2" {
				payload = readerAudioFrame(0, "ab")
			}
			if err := c.Write(f.ctx, websocket.MessageBinary, payload); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-f.ctx.Done():
				t.Fatal("upload did not start")
			}
			rpc := <-opened
			f.g.StopAccepting()
			f.cancel()
			_ = c.CloseNow()
			select {
			case <-rpc.Done():
			case <-f.ctx.Done():
				t.Fatal("RPC was not canceled")
			}
			requireLeaseOccupied(t, a.pool)
			if f.g.Snapshot().ActiveSessions != 1 {
				t.Fatal("Gateway forgot a still-cleaning session")
			}
			waitCtx, stop := context.WithCancel(context.Background())
			stop()
			if err := f.g.Wait(waitCtx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Wait passed blocked cleanup: %v", err)
			}
			unblock()
			if err := f.g.Wait(f.ctx); err != nil {
				t.Fatal(err)
			}
			requireLeaseReusable(t, a.pool)
		})
	}
}

// waitLeaseActive 只等待本地清理计数，不把轮询耗时解释为性能数据。
func waitLeaseActive(t *testing.T, f *v2EntryFixture, want int) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for f.g.Snapshot().ActiveSessions != want {
		select {
		case <-tick.C:
		case <-f.ctx.Done():
			t.Fatalf("Gateway did not reach active=%d", want)
		}
	}
}

func TestGatewayLeaseStaggeredRouting(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			workers, backends := newRoutingBackends(t)
			a := newLeaseAllocator(t, workers[0].Client, workers[1].Client)
			f := newLeaseGatewayFixture(t, a, Config{MaxSessions: 2, V2: &V2Config{}})
			start := func(id, worker string) *websocket.Conn {
				var c *websocket.Conn
				if version == "v2" {
					c, _ = startLeaseV2(t, f)
				} else {
					c = f.dial(t, "/v1/asr")
					v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v1"})
				}
				payload := []byte(id + ":first")
				if version == "v2" {
					payload = readerAudioFrame(0, string(payload))
				}
				if err := c.Write(f.ctx, websocket.MessageBinary, payload); err != nil {
					t.Fatal(err)
				}
				if version == "v2" {
					v2ReadResult(t, f.ctx, c, 1, worker+":"+id+":first")
				} else {
					routingResult(t, f.ctx, c, id, worker, "first", false)
				}
				return c
			}
			finish := func(c *websocket.Conn, id, worker string) {
				if version == "v1" {
					finishRoutingSession(t, f.ctx, c, id, worker)
					f.waitReturn(t)
				} else {
					offset := uint64(len(id + ":first"))
					v2WriteJSON(t, f.ctx, c, wsprotocol.V2EndMessage{Type: wsprotocol.MessageTypeEnd, FinalOffset: fmt.Sprint(offset)})
					v2ReadResult(t, f.ctx, c, 2, worker+":"+id+":tail")
					readCompletionNetwork(t, f.ctx, c, 1, offset, 2)
					v2WriteJSON(t, f.ctx, c, wsprotocol.CompletedAckMessage{Type: wsprotocol.MessageTypeCompletedAck, FinalOffset: offset, LastSeq: 2})
				}
			}
			long := start("L", "A")
			short := start("S", "B")
			finish(short, "S", "B")
			waitLeaseActive(t, f, 1)
			next := start("N", "B") // A 仍被 L 占用，扫描必须跳到已归还的 B。
			if backends[0].started.Load() != 1 || backends[1].started.Load() != 2 {
				t.Fatal("new session did not use idle Worker B")
			}
			finish(next, "N", "B")
			waitLeaseActive(t, f, 1)
			// 原 L 在新场完成后仍能正常发送和收尾。
			finish(long, "L", "A")
			v2RequireIdle(t, f)
			if a.calls.Load() != 3 {
				t.Fatal("session acquired more than one reservation")
			}
			first, err := a.pool.TryAcquire()
			if err != nil {
				t.Fatal(err)
			}
			defer first.Release()
			second, err := a.pool.TryAcquire()
			if err != nil {
				t.Fatal(err)
			}
			defer second.Release()
			requireLeaseOccupied(t, a.pool)
		})
	}
}

func TestGatewayLeaseV1OpenFailureAndCapacityRejection(t *testing.T) {
	failure := &routingOpenFailure{contexts: make(chan context.Context, 1), cause: errors.New("open failure")}
	a := newLeaseAllocator(t, failure)
	f := newLeaseGatewayFixture(t, a, Config{MaxSessions: 2})
	c := f.dial(t, "/v1/asr")
	v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v1"})
	_, _, err := c.Read(f.ctx)
	if websocket.CloseStatus(err) != websocket.StatusInternalError {
		t.Fatalf("open failure close: %v", err)
	}
	f.waitReturn(t)
	waitLeaseActive(t, f, 0)
	if rpc := <-failure.contexts; rpc.Err() == nil {
		t.Fatal("open failure returned capacity before canceling RPC")
	}
	requireLeaseReusable(t, a.pool)
	held, err := a.pool.TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	c = f.dial(t, "/v1/asr")
	v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v1"})
	_, _, err = c.Read(f.ctx)
	if websocket.CloseStatus(err) != websocket.StatusInternalError {
		t.Fatalf("capacity failure close: %v", err)
	}
	f.waitReturn(t)
	waitLeaseActive(t, f, 0)
	if failure.calls.Load() != 1 {
		t.Fatal("full pool opened another RPC")
	}
	requireLeaseOccupied(t, a.pool)
}

// leaseHeldEngine 只阻塞 Close，其他同步调用按真实 Worker 接口串行执行。
// 这是可控 Engine + TCP 测试，不是原生模型的最坏取消耗时测量。
type leaseHeldEngine struct {
	ready        bool
	closeEntered chan struct{}
	releaseClose <-chan struct{}
}

func (e *leaseHeldEngine) Accept([]float32) error  { e.ready = true; return nil }
func (e *leaseHeldEngine) Ready() (bool, error)    { return e.ready, nil }
func (e *leaseHeldEngine) Decode() error           { e.ready = false; return nil }
func (e *leaseHeldEngine) Result() (string, error) { return "partial", nil }
func (e *leaseHeldEngine) Finish() error           { return nil }
func (e *leaseHeldEngine) Close() {
	if e.closeEntered != nil {
		close(e.closeEntered)
		<-e.releaseClose
	}
}

type leaseExitServer struct {
	asrv1.UnimplementedASRServiceServer
	worker *realasr.Worker
	exited chan error
}

func (s *leaseExitServer) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	err := s.worker.StreamingRecognize(stream)
	s.exited <- err // Worker 返回前已执行 Close 和实际名额归还。
	return err
}

func TestGatewayLeaseLocalReleaseDoesNotClaimRemoteRelease(t *testing.T) {
	closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(releaseClose) }) }
	var created atomic.Int32
	worker, err := realasr.New(realasr.Config{}, func(context.Context) (realasr.Engine, error) {
		if created.Add(1) == 1 {
			return &leaseHeldEngine{closeEntered: closeEntered, releaseClose: releaseClose}, nil
		}
		return &leaseHeldEngine{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &leaseExitServer{worker: worker, exited: make(chan error, 4)}
	a := newLeaseAllocator(t, newBaselineTCPWorkerClient(t, server))
	f := newLeaseGatewayFixture(t, a, Config{V2: &V2Config{}})
	defer unblock()
	c, ready := startLeaseV2(t, f)
	if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
		t.Fatal(err)
	}
	v2ReadResult(t, f.ctx, c, 1, "partial")
	s, ok := f.g.registry.lookup(ready.SessionID)
	if !ok {
		t.Fatal("session missing")
	}
	if err := s.requestClose(f.ctx); err != nil {
		t.Fatal(err)
	}
	_ = c.CloseNow()
	v2RequireIdle(t, f)
	select {
	case <-closeEntered:
	case <-f.ctx.Done():
		t.Fatal("remote engine did not begin Close")
	}
	requireLeaseReusable(t, a.pool) // 本地已经清理，远端 Close 却仍被屏障阻塞。
	client, err := wsclient.NewV2(wsclient.V2Config{URL: f.url + "/v2/asr"})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := client.Run(f.ctx, io.NopCloser(strings.NewReader("ab")))
	if runErr == nil || result.Completed || result.CompletionAckWritten {
		t.Fatal("busy remote was reported as complete")
	}
	v2RequireIdle(t, f)
	select {
	case err := <-server.exited:
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("busy RPC status: %v", err)
		}
	case <-f.ctx.Done():
		t.Fatal("busy RPC did not return")
	}
	if created.Load() != 1 || a.calls.Load() != 2 {
		t.Fatal("busy request created a model or transparently retried")
	}
	requireLeaseReusable(t, a.pool)
	unblock()
	select {
	case <-server.exited:
	case <-f.ctx.Done():
		t.Fatal("original remote cleanup did not exit")
	}
	result, runErr = client.Run(f.ctx, io.NopCloser(strings.NewReader("ab")))
	if runErr != nil || !result.Completed || !result.CompletionAckWritten {
		t.Fatalf("fresh session after actual release: %v", runErr)
	}
	v2RequireIdle(t, f)
	if created.Load() != 2 || a.calls.Load() != 3 {
		t.Fatal("fresh session did not use exactly one new model")
	}
	requireLeaseReusable(t, a.pool)
}
