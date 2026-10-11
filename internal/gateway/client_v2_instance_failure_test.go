package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// instanceWorkerRecord 分别记录一条流收到和实际处理的音频。
// 所有字段均由 instanceFailureWorker.mu 保护，流退出后也遵循同一规则。
type instanceWorkerRecord struct {
	audio     []byte
	processed uint64
	ended     bool
	exited    bool
}

// instanceFailureWorker 在首条 RPC 收到第二块音频后暂停处理，直到 RPC 被取消。
// 后续 RPC 正常处理；records 仅用于观测，各流从独立的处理位置开始。
type instanceFailureWorker struct {
	asrv1.UnimplementedASRServiceServer
	mu        sync.Mutex
	records   []*instanceWorkerRecord
	held      chan struct{} // 首条 RPC 收到第二块音频后关闭。
	firstDone chan struct{} // 首条 RPC 的 handler 实际退出后关闭。
	active    atomic.Int32
}

// StreamingRecognize 记录实际输入，正常块发送处理进度与文本，半关闭后发送尾部结果。
func (b *instanceFailureWorker) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	b.mu.Lock()
	r := &instanceWorkerRecord{}
	b.records = append(b.records, r)
	first := len(b.records) == 1
	b.mu.Unlock()
	b.active.Add(1)
	defer func() {
		b.mu.Lock()
		r.exited = true
		b.mu.Unlock()
		b.active.Add(-1)
		if first {
			close(b.firstDone)
		}
	}()
	chunks := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			b.mu.Lock()
			r.ended = true
			b.mu.Unlock()
			return stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "tail", Text: "complete", IsFinal: true})
		}
		if err != nil {
			return err
		}
		chunks++
		b.mu.Lock()
		r.audio = append(r.audio, req.Data...)
		b.mu.Unlock()
		if first && chunks == 2 {
			close(b.held)
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		b.mu.Lock()
		r.processed += uint64(len(req.Data))
		processed := r.processed
		b.mu.Unlock()
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: processed}}); err != nil {
			return err
		}
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "audio", Text: string(req.Data)}); err != nil {
			return err
		}
	}
}

// restartableInstanceWorker 拥有测试服务及共享客户端连接。
// restart 复用 TCP 地址和 ClientConn，旧 RPC 的处理状态不转移到新流。
type restartableInstanceWorker struct {
	address string
	backend *instanceFailureWorker
	server  *grpc.Server
	done    chan error
	conn    *grpc.ClientConn
}

// newRestartableInstanceWorker 创建首个服务与客户端，并登记全部退出责任。
func newRestartableInstanceWorker(t *testing.T) *restartableInstanceWorker {
	t.Helper()
	w := &restartableInstanceWorker{backend: &instanceFailureWorker{held: make(chan struct{}), firstDone: make(chan struct{})}}
	w.start(t)
	// 先登记服务清理，确保创建客户端失败时也释放监听器。
	t.Cleanup(func() {
		if w.conn != nil {
			_ = w.conn.Close()
		}
		w.stop(t)
	})
	var err error
	w.conn, err = grpc.NewClient(w.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// start 在首次创建时申请临时端口，重启时重新监听原地址。
func (w *restartableInstanceWorker) start(t *testing.T) {
	t.Helper()
	address := w.address
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	w.address = listener.Addr().String()
	w.server = grpc.NewServer(grpc.WaitForHandlers(true))
	asrv1.RegisterASRServiceServer(w.server, w.backend)
	w.done = make(chan error, 1)
	server, done := w.server, w.done
	go func() { done <- server.Serve(listener) }()
}

// stop 强制关闭 gRPC 传输，等待 handler，并检查 Serve 已退出；可重复调用。
// 这是同一测试进程中的受控服务故障，不是操作系统进程被强制终止。
func (w *restartableInstanceWorker) stop(t *testing.T) {
	t.Helper()
	if w.server == nil {
		return
	}
	w.server.Stop()
	select {
	case err := <-w.done:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Worker Serve exit: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("Worker Serve did not exit")
	}
	w.server = nil
}

// restart 要求旧服务已停止，并等待共享客户端连接重新可用；ctx 是测试保护期限。
func (w *restartableInstanceWorker) restart(t *testing.T, ctx context.Context) {
	t.Helper()
	if w.server != nil {
		t.Fatal("restart requires the previous server to have stopped")
	}
	w.start(t)
	w.conn.ResetConnectBackoff()
	w.conn.Connect()
	for state := w.conn.GetState(); state != connectivity.Ready; state = w.conn.GetState() {
		if !w.conn.WaitForStateChange(ctx, state) {
			t.Fatal("Worker channel did not become ready after restart")
		}
	}
}

// instanceFailureProxy 通过真实 WebSocket 转发整条消息，观测 ready/audio_ack。
// 暂扣恢复握手，直到测试确认 Gateway 自然清理；解析后的凭据只保存在夹具内。
type instanceFailureProxy struct {
	url         string
	ready       chan wsprotocol.ReadyMessage
	accepted    chan struct{} // 首条连接的 audio_ack(next_offset=8) 已转发成功。
	resumeSeen  chan struct{} // 已解析首个 resume，但尚未进入 Gateway。
	allowResume chan struct{}
	acceptOnce  sync.Once
	resumeOnce  sync.Once
	mu          sync.Mutex
	front       *websocket.Conn
	back        *websocket.Conn
	requests    []wsprotocol.V2HandshakeKind
	tasks       sync.WaitGroup
	active      atomic.Int32 // 尚未返回的代理 handler，用于观测自然退出。
}

// newInstanceFailureProxy 创建代理及私有取消域，测试结束时清理其全部任务。
func newInstanceFailureProxy(t *testing.T, f *v2EntryFixture) *instanceFailureProxy {
	t.Helper()
	p := &instanceFailureProxy{ready: make(chan wsprotocol.ReadyMessage, 1), accepted: make(chan struct{}), resumeSeen: make(chan struct{}), allowResume: make(chan struct{})}
	ctx, cancel := context.WithCancel(f.ctx)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.tasks.Add(1)
		defer p.tasks.Done()
		p.active.Add(1)
		defer p.active.Add(-1)
		front, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer front.CloseNow()
		typ, data, err := front.Read(ctx)
		if err != nil || typ != websocket.MessageText {
			return
		}
		request, err := wsprotocol.DecodeV2Handshake(data)
		if err != nil {
			return
		}
		p.mu.Lock()
		p.requests = append(p.requests, request.Kind)
		first := len(p.requests) == 1
		p.mu.Unlock()
		if request.Kind == wsprotocol.V2HandshakeResume {
			p.resumeOnce.Do(func() { close(p.resumeSeen) })
			select {
			case <-p.allowResume:
			case <-ctx.Done():
				return
			}
		}
		back, _, err := websocket.Dial(ctx, f.url+"/v2/asr", nil)
		if err != nil {
			return
		}
		defer back.CloseNow()
		if first {
			p.mu.Lock()
			p.front, p.back = front, back
			p.mu.Unlock()
		}
		if err := back.Write(ctx, typ, data); err != nil {
			return
		}
		group, streamCtx := errgroup.WithContext(ctx)
		group.Go(func() error {
			for {
				typ, data, err := front.Read(streamCtx)
				if err != nil {
					return err
				}
				if err := back.Write(streamCtx, typ, data); err != nil {
					return err
				}
			}
		})
		group.Go(func() error {
			for {
				typ, data, err := back.Read(streamCtx)
				if err != nil {
					return err
				}
				var tag struct{ Type wsprotocol.MessageType }
				if err := json.Unmarshal(data, &tag); err != nil {
					return err
				}
				if err := front.Write(streamCtx, typ, data); err != nil {
					return err
				}
				if first && tag.Type == wsprotocol.MessageTypeReady {
					var ready wsprotocol.ReadyMessage
					if err := json.Unmarshal(data, &ready); err != nil {
						return err
					}
					p.ready <- ready
				}
				if first && tag.Type == wsprotocol.MessageTypeAudioAck {
					var ack wsprotocol.AudioAckMessage
					if err := json.Unmarshal(data, &ack); err != nil {
						return err
					}
					if ack.Generation == 1 && ack.NextOffset == 8 && !ack.InputEnded {
						p.acceptOnce.Do(func() { close(p.accepted) })
					}
				}
			}
		})
		_ = group.Wait()
	}))
	p.url = "ws" + strings.TrimPrefix(server.URL, "http")
	t.Cleanup(func() {
		cancel()
		server.Close()
		p.tasks.Wait()
	})
	return p
}

// cutInitial 关闭首代两端 socket，不修改 Gateway 状态；必须在初始 ready 后调用。
func (p *instanceFailureProxy) cutInitial() {
	p.mu.Lock()
	front, back := p.front, p.back
	p.mu.Unlock()
	_ = front.CloseNow()
	_ = back.CloseNow()
}

// requireIdle 在客户端 Run 返回后等待代理自然退出，不先取消代理来制造空闲。
func (p *instanceFailureProxy) requireIdle(t *testing.T, ctx context.Context) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for p.active.Load() != 0 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("proxy handlers did not exit naturally")
		}
	}
	p.tasks.Wait()
}

// waitInstanceSignal 等待明确事实；ctx 只用于防止测试挂住，不测量恢复速度。
func waitInstanceSignal(t *testing.T, ctx context.Context, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("did not observe %s: %v", name, ctx.Err())
	}
}

// waitInstanceDetached 通过协调者命令观察 detached，不直接读取会话的可变字段。
func waitInstanceDetached(t *testing.T, f *v2EntryFixture, ready wsprotocol.ReadyMessage) {
	t.Helper()
	s, ok := f.g.registry.lookup(ready.SessionID)
	if !ok {
		t.Fatal("original session disappeared before Worker failure")
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := s.requestConnectionReady(f.ctx, ready.Generation)
		if errors.Is(err, errSessionNotAttached) {
			return
		}
		if err != nil {
			t.Fatalf("unexpected coordinator query failure: %v", err)
		}
		select {
		case <-ticker.C:
		case <-f.ctx.Done():
			t.Fatal("original session did not detach")
		}
	}
}

func TestV2ClientWorkerInstanceFailure(t *testing.T) {
	for _, detached := range []bool{false, true} {
		name := "attached"
		if detached {
			name = "detached"
		}
		t.Run(name, func(t *testing.T) {
			w := newRestartableInstanceWorker(t)
			// 保留和发送预算超过五秒测试保护期限，排除普通到期引发清理。
			f := newV2EntryFixture(t, asrv1.NewASRServiceClient(w.conn), Config{MaxSessions: 1, WorkerSendTimeout: 30 * time.Second, V2: &V2Config{ResumeWindow: 30 * time.Second}})
			proxy := newInstanceFailureProxy(t, f)
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = writer.Close() })
			source := &clientGatewaySource{reader: reader, close: reader.Close}
			applied := make(chan wsprotocol.SequencedResultMessage, 4)
			client, err := wsclient.NewV2(wsclient.V2Config{
				URL: proxy.url, ChunkBytes: 4,
				Resume: &wsclient.V2ResumeConfig{Window: 4 * time.Second},
				OnResult: func(ctx context.Context, result wsprotocol.SequencedResultMessage, _ time.Time) error {
					select {
					case applied <- result:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				result wsclient.V2RunResult
				err    error
			}
			returned := make(chan outcome, 1)
			go func() { result, err := client.Run(f.ctx, source); returned <- outcome{result, err} }()
			var ready wsprotocol.ReadyMessage
			select {
			case ready = <-proxy.ready:
			case <-f.ctx.Done():
				t.Fatal("initial ready was not forwarded")
			}
			if ready.Type != wsprotocol.MessageTypeReady || ready.Generation != 1 || ready.NextOffset != 0 || ready.InputEnded {
				t.Fatal("unexpected initial ready")
			}
			if _, err := writer.Write([]byte("aabbccdd")); err != nil {
				t.Fatal(err)
			}
			// 保持源打开，采集任务只能由 Run 收尾关闭源来解除阻塞。
			select {
			case result := <-applied:
				if result.Seq != 1 || result.Text != "aabb" || result.IsFinal {
					t.Fatalf("unexpected first applied result: seq=%d text=%q final=%v", result.Seq, result.Text, result.IsFinal)
				}
			case <-f.ctx.Done():
				t.Fatal("first result was not applied")
			}
			waitInstanceSignal(t, f.ctx, w.backend.held, "second chunk received but not processed")
			waitInstanceSignal(t, f.ctx, proxy.accepted, "Gateway audio_ack through byte 8")
			if detached {
				proxy.cutInitial()
				waitInstanceDetached(t, f, ready)
			} else {
				s, ok := f.g.registry.lookup(ready.SessionID)
				if !ok {
					t.Fatal("original session missing before failure")
				}
				current, err := s.requestConnectionReady(f.ctx, ready.Generation)
				if err != nil || current.nextOffset != 8 || current.inputEnded {
					t.Fatalf("original session is not attached with eight accepted bytes: err=%v", err)
				}
			}
			w.stop(t)
			waitInstanceSignal(t, f.ctx, w.backend.firstDone, "original Worker handler exit")
			v2RequireIdle(t, f) // 此前未取消 Gateway，也未调用 StopAccepting。
			waitInstanceSignal(t, f.ctx, proxy.resumeSeen, "original client recovery attempt")
			close(proxy.allowResume)
			var out outcome
			select {
			case out = <-returned:
			case <-f.ctx.Done():
				t.Fatal("original client Run did not return")
			}
			proxy.requireIdle(t, f.ctx)
			var entry *wsclient.V2EntryError
			if !errors.As(out.err, &entry) || entry.Code != wsprotocol.V2ErrorResumeUnavailable || out.result.Completed || out.result.CompletionAckWritten || out.result.AppliedSeq != 1 || out.result.ResumeAttempts != 1 || out.result.ResumedConnections != 0 {
				t.Fatalf("original outcome=%+v err=%v", out.result, out.err)
			}
			if source.closes.Load() != 1 || f.pool.picks.Load() != 1 || w.backend.active.Load() != 0 {
				t.Fatalf("original resources/reselection: source closes=%d picks=%d Worker active=%d", source.closes.Load(), f.pool.picks.Load(), w.backend.active.Load())
			}
			if len(applied) != 0 {
				t.Fatal("unprocessed audio produced an additional applied result")
			}
			proxy.mu.Lock()
			requests := append([]wsprotocol.V2HandshakeKind(nil), proxy.requests...)
			proxy.mu.Unlock()
			if len(requests) != 2 || requests[0] != wsprotocol.V2HandshakeStart || requests[1] != wsprotocol.V2HandshakeResume {
				t.Fatalf("original handshake kinds=%v", requests)
			}
			v2RequireIdle(t, f)
			// 先检查新传输可用，再显式启动独立的新场；旧场失败结果继续保留。
			w.restart(t, f.ctx)
			newInput := []byte("11223344")
			newSource := &clientGatewaySource{reader: bytes.NewReader(newInput)}
			var newResults []wsprotocol.SequencedResultMessage
			newClient, err := wsclient.NewV2(wsclient.V2Config{URL: f.url + "/v2/asr", ChunkBytes: 4, OnResult: func(_ context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
				newResults = append(newResults, r)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			newResult, err := newClient.Run(f.ctx, newSource)
			if err != nil || !newResult.Completed || !newResult.CompletionAckWritten || newResult.FinalOffset != 8 || newResult.AppliedSeq != 3 || newResult.ResumeAttempts != 0 || newSource.closes.Load() != 1 {
				t.Fatalf("new session outcome=%+v err=%v source closes=%d", newResult, err, newSource.closes.Load())
			}
			if len(newResults) != 3 || newResults[0].Seq != 1 || newResults[0].Text != "1122" || newResults[1].Seq != 2 || newResults[1].Text != "3344" || newResults[2].Seq != 3 || newResults[2].Text != "complete" || !newResults[2].IsFinal {
				t.Fatalf("new session results=%+v", newResults)
			}
			v2RequireIdle(t, f)
			w.backend.mu.Lock()
			defer w.backend.mu.Unlock()
			if len(w.backend.records) != 2 || f.pool.picks.Load() != 2 || w.backend.active.Load() != 0 {
				t.Fatal("unexpected replacement RPC or leaked new session")
			}
			old, fresh := w.backend.records[0], w.backend.records[1]
			if string(old.audio) != "aabbccdd" || old.processed != 4 || old.ended || !old.exited {
				t.Fatalf("old stream audio=%q processed=%d ended=%v exited=%v", old.audio, old.processed, old.ended, old.exited)
			}
			if !bytes.Equal(fresh.audio, newInput) || fresh.processed != 8 || !fresh.ended || !fresh.exited {
				t.Fatalf("new stream audio=%q processed=%d ended=%v exited=%v", fresh.audio, fresh.processed, fresh.ended, fresh.exited)
			}
		})
	}
}
