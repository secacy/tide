package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

type v2EntrySelector struct {
	client asrv1.ASRServiceClient
	picks  atomic.Int32
}

func (p *v2EntrySelector) Pick() workerpool.Worker {
	p.picks.Add(1)
	return workerpool.Worker{ID: "v2-test", Client: p.client}
}

// v2EntryBackend 是真实 TCP gRPC 替身，记录每场收到的音频而非猜测发送次数。
type v2EntryBackend struct {
	asrv1.UnimplementedASRServiceServer
	calls atomic.Int32
	mu    sync.Mutex
	audio []string
}

func (b *v2EntryBackend) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	b.calls.Add(1)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "tail", Text: "complete", IsFinal: true})
		}
		if err != nil {
			return err
		}
		b.mu.Lock()
		b.audio = append(b.audio, string(req.Data))
		b.mu.Unlock()
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "audio", Text: string(req.Data)}); err != nil {
			return err
		}
	}
}

type v2EntryFixture struct {
	g        *Gateway
	pool     *v2EntrySelector
	url      string
	ctx      context.Context
	cancel   context.CancelFunc
	returned chan struct{}
}

func newV2EntryFixture(t *testing.T, client asrv1.ASRServiceClient, cfg Config) *v2EntryFixture {
	t.Helper()
	life, cancel := context.WithCancel(context.Background())
	p := &v2EntrySelector{client: client}
	g, err := New(life, p, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	f := &v2EntryFixture{g: g, pool: p, cancel: cancel, returned: make(chan struct{}, 128)}
	var stopTest context.CancelFunc
	f.ctx, stopTest = context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(stopTest)
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
			t.Errorf("fixture cleanup: %v", err)
		}
		server.Close()
	})
	return f
}

func (f *v2EntryFixture) dial(t *testing.T, path string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.Dial(f.ctx, f.url+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func (f *v2EntryFixture) waitReturn(t *testing.T) {
	t.Helper()
	select {
	case <-f.returned:
	case <-f.ctx.Done():
		t.Fatal("handler did not return")
	}
}

func v2WriteJSON(t *testing.T, ctx context.Context, c *websocket.Conn, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
}

func v2ReadJSON(t *testing.T, ctx context.Context, c *websocket.Conn, out any) {
	t.Helper()
	typ, data, err := c.Read(ctx)
	if err != nil || typ != websocket.MessageText {
		t.Fatalf("read: type=%v err=%v", typ, err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatal(err)
	}
}

func v2ReadResult(t *testing.T, ctx context.Context, c *websocket.Conn, seq uint64, text string) {
	t.Helper()
	for {
		var data json.RawMessage
		v2ReadJSON(t, ctx, c, &data)
		var tag struct {
			Type wsprotocol.MessageType `json:"type"`
		}
		if err := json.Unmarshal(data, &tag); err != nil {
			t.Fatal(err)
		}
		if tag.Type == wsprotocol.MessageTypeAudioAck {
			continue
		}
		var result wsprotocol.SequencedResultMessage
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result.Type != wsprotocol.MessageTypeResult || result.Seq != seq || result.Text != text {
			t.Fatalf("unexpected result: %s", data)
		}
		return
	}
}

func v2RequireIdle(t *testing.T, f *v2EntryFixture) {
	t.Helper()
	// 计数读取是线程安全的；短轮询只等实际清理，不衡量恢复性能。
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for f.g.Snapshot().ActiveSessions != 0 || entryHandshakeCount(f.g.gate) != 0 {
		select {
		case <-ticker.C:
		case <-f.ctx.Done():
			t.Fatal("resources did not return to idle")
		}
	}
	f.g.registry.mu.Lock()
	n := len(f.g.registry.entries)
	f.g.registry.mu.Unlock()
	if n != 0 {
		t.Fatalf("registry retained %d sessions", n)
	}
}

func v2ExpectError(t *testing.T, f *v2EntryFixture, c *websocket.Conn, text string) {
	t.Helper()
	var msg wsprotocol.ErrorMessage
	v2ReadJSON(t, f.ctx, c, &msg)
	if msg.Type != wsprotocol.MessageTypeError || msg.Message != text {
		t.Fatalf("unexpected error: %+v", msg)
	}
	f.waitReturn(t)
}

// 两个真实网络层均经过公开 handler；恢复前后 RPC/Pick 与实际音频均有证据。
func TestV2EntryNetworkLifecycle(t *testing.T) {
	for _, recover := range []bool{false, true} {
		name := "new_session"
		if recover {
			name = "resume_at_full_budget"
		}
		t.Run(name, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{MaxSessions: 1, V2: &V2Config{ResumeWindow: 2 * time.Second}})
			c := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
			var ready wsprotocol.ReadyMessage
			v2ReadJSON(t, f.ctx, c, &ready)
			if ready.Type != wsprotocol.MessageTypeReady || ready.Generation != 1 || ready.NextOffset != 0 {
				t.Fatalf("bad initial ready: %+v", ready)
			}
			f.waitReturn(t) // handler 已退出，RPC 和连接必须仍可工作。
			s, ok := f.g.registry.lookup(ready.SessionID)
			if !ok || s.entryGate != f.g.gate || !s.identity.matchesResumeToken(ready.ResumeToken) {
				t.Fatal("ready not bound to registered session")
			}
			if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
				t.Fatal(err)
			}
			v2ReadResult(t, f.ctx, c, 1, "ab")
			if recover {
				// 第二场新建不能消耗第二个逻辑名额或选择另一个 Worker。
				other := f.dial(t, "/v2/asr")
				v2WriteJSON(t, f.ctx, other, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
				v2ExpectError(t, f, other, "session limit exceeded")
				_ = c.CloseNow()
				// 未完成旧连接收割时是明确 busy；每次重试是一条新连接。
				for {
					c = f.dial(t, "/v2/asr")
					v2WriteJSON(t, f.ctx, c, wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: ready.SessionID, ResumeToken: ready.ResumeToken, AppliedSeq: 0})
					var raw json.RawMessage
					v2ReadJSON(t, f.ctx, c, &raw)
					var tag wsprotocol.ErrorMessage
					if err := json.Unmarshal(raw, &tag); err != nil {
						t.Fatal(err)
					}
					f.waitReturn(t)
					if tag.Type == wsprotocol.MessageTypeReady {
						var resumed wsprotocol.ReadyMessage
						if err := json.Unmarshal(raw, &resumed); err != nil {
							t.Fatal(err)
						}
						if resumed.SessionID != ready.SessionID || resumed.ResumeToken != ready.ResumeToken || resumed.Generation != 2 || resumed.NextOffset != 2 {
							t.Fatal("resume did not retain original session")
						}
						ready = resumed
						break
					}
					if tag.Type != wsprotocol.MessageTypeError || tag.Message != "session busy" {
						t.Fatalf("resume rejected: %s", raw)
					}
					_ = c.CloseNow()
					time.Sleep(time.Millisecond)
				}
				v2ReadResult(t, f.ctx, c, 1, "ab") // 未确认结果重放。
				if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
					t.Fatal(err)
				}
			}
			v2WriteJSON(t, f.ctx, c, wsprotocol.ResultAckMessage{Type: wsprotocol.MessageTypeResultAck, Seq: "1"})
			if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(2, "cd")); err != nil {
				t.Fatal(err)
			}
			v2ReadResult(t, f.ctx, c, 2, "cd")
			v2WriteJSON(t, f.ctx, c, wsprotocol.V2EndMessage{Type: wsprotocol.MessageTypeEnd, FinalOffset: "4"})
			v2ReadResult(t, f.ctx, c, 3, "complete")
			readCompletionNetwork(t, f.ctx, c, ready.Generation, 4, 3)
			v2WriteJSON(t, f.ctx, c, map[string]string{"type": "completed_ack", "finalOffset": "4", "lastSeq": "3"})
			v2RequireIdle(t, f)
			backend.mu.Lock()
			audio := append([]string(nil), backend.audio...)
			backend.mu.Unlock()
			if backend.calls.Load() != 1 || f.pool.picks.Load() != 1 || strings.Join(audio, ",") != "ab,cd" {
				t.Fatalf("calls=%d picks=%d audio=%v", backend.calls.Load(), f.pool.picks.Load(), audio)
			}
		})
	}
}

func TestV2EntryBadHandshakeAndAuthentication(t *testing.T) {
	for _, tc := range []struct{ name, message, want string }{
		{"invalid_start", `{"type":"start","version":"v1"}`, "invalid handshake"},
		{"unknown_id", `{"type":"resume","version":"v2","sessionId":"missing","resumeToken":"secret","appliedSeq":"0"}`, "resume unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{V2: &V2Config{}})
			c := f.dial(t, "/v2/asr")
			if err := c.Write(f.ctx, websocket.MessageText, []byte(tc.message)); err != nil {
				t.Fatal(err)
			}
			v2ExpectError(t, f, c, tc.want)
			v2RequireIdle(t, f)
			if f.pool.picks.Load() != 0 || backend.calls.Load() != 0 {
				t.Fatal("bad request started a Worker")
			}
		})
	}
	for _, mode := range []string{"wrong_token", "attached", "ack_ahead", "expired"} {
		t.Run(mode, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{V2: &V2Config{ResumeWindow: 100 * time.Millisecond}})
			c := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
			var ready wsprotocol.ReadyMessage
			v2ReadJSON(t, f.ctx, c, &ready)
			f.waitReturn(t)
			s, _ := f.g.registry.lookup(ready.SessionID)
			want, token, applied := "session busy", ready.ResumeToken, uint64(0)
			if mode == "wrong_token" {
				token = strings.Repeat("x", resumeTokenLength)
				want = "resume unavailable"
			}
			if mode == "ack_ahead" || mode == "expired" {
				_ = c.CloseNow()
				// 显式等待旧 attachment 退出的事实，无需读写可变 resumeState。
				if mode == "expired" {
					select {
					case <-s.controlDone:
					case <-f.ctx.Done():
						t.Fatal("session did not expire")
					}
					v2RequireIdle(t, f)
					want = "resume unavailable"
				} else {
					applied, want = 1, "invalid resume position"
					// 等到后续候选不再 busy，再断言范围拒绝。
				}
			}
			for {
				next := f.dial(t, "/v2/asr")
				v2WriteJSON(t, f.ctx, next, wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: ready.SessionID, ResumeToken: token, AppliedSeq: applied})
				var msg wsprotocol.ErrorMessage
				v2ReadJSON(t, f.ctx, next, &msg)
				f.waitReturn(t)
				if mode == "ack_ahead" && msg.Message == "session busy" {
					_ = next.CloseNow()
					time.Sleep(time.Millisecond)
					continue
				}
				if msg.Type != wsprotocol.MessageTypeError || msg.Message != want {
					t.Fatalf("got %+v want %s", msg, want)
				}
				break
			}
			if f.pool.picks.Load() != 1 {
				t.Fatal("rejected resume picked another Worker")
			}
			if mode != "expired" && f.g.Snapshot().ActiveSessions != 1 {
				t.Fatal("resume failure released original session")
			}
		})
	}
}

func TestV2EntryHandshakeLimitAndTimeout(t *testing.T) {
	backend := &v2EntryBackend{}
	f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{StartTimeout: 100 * time.Millisecond, V2: &V2Config{MaxHandshakes: 1}})
	first := f.dial(t, "/v2/asr") // 首条消息不发送。
	if entryHandshakeCount(f.g.gate) != 1 || f.g.Snapshot().ActiveSessions != 0 {
		t.Fatal("wrong handshake admission")
	}
	c, resp, err := websocket.Dial(f.ctx, f.url+"/v2/asr", nil)
	if c != nil {
		_ = c.CloseNow()
	}
	if err == nil || resp == nil || resp.StatusCode != 503 {
		t.Fatalf("limit: response=%v err=%v", resp, err)
	}
	f.waitReturn(t)             // 超额请求。
	_, _, _ = first.Read(f.ctx) // 库在读取超时后可能直接关闭，错误提示是 best effort。
	f.waitReturn(t)
	v2RequireIdle(t, f)
	if f.pool.picks.Load() != 0 {
		t.Fatal("silent handshake started a Worker")
	}
	// 清理后临时名额可复用；停服在升级前拒绝。
	next := f.dial(t, "/v2/asr")
	_ = next.CloseNow()
	f.waitReturn(t)
	f.g.StopAccepting()
	c, resp, err = websocket.Dial(f.ctx, f.url+"/v2/asr", nil)
	if c != nil {
		_ = c.CloseNow()
	}
	if err == nil || resp == nil || resp.StatusCode != 503 {
		t.Fatalf("stop: response=%v err=%v", resp, err)
	}
}

// 建流替身允许在 ctx 已取消后仍暂停，验证等待真实返回才释放名额。
type v2BlockingOpenClient struct {
	entered chan context.Context
	release chan struct{}
	cause   error
}

func (c *v2BlockingOpenClient) StreamingRecognize(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	c.entered <- ctx
	<-c.release
	return nil, c.cause
}

func TestV2EntryOpenCancellationAndFailure(t *testing.T) {
	for _, mode := range []string{"entry_timeout", "gateway_cancel", "open_failure"} {
		t.Run(mode, func(t *testing.T) {
			client := &v2BlockingOpenClient{entered: make(chan context.Context, 1), release: make(chan struct{}), cause: errors.New("secret backend details")}
			var once sync.Once
			release := func() { once.Do(func() { close(client.release) }) }
			f := newV2EntryFixture(t, client, Config{V2: &V2Config{EntryTimeout: 100 * time.Millisecond}})
			t.Cleanup(release) // 先解除阻塞，再让 fixture 等资源清理。
			c := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
			var rpcCtx context.Context
			select {
			case rpcCtx = <-client.entered:
			case <-f.ctx.Done():
				t.Fatal("Worker not called")
			}
			if mode == "gateway_cancel" {
				f.g.StopAccepting()
				f.cancel()
			}
			if mode != "open_failure" {
				select {
				case <-rpcCtx.Done():
				case <-f.ctx.Done():
					t.Fatal("entry cancellation did not reach original RPC")
				}
				if f.g.Snapshot().ActiveSessions != 1 || entryHandshakeCount(f.g.gate) != 1 {
					t.Fatal("resources released before open returned")
				}
			}
			release()
			if mode == "gateway_cancel" {
				f.waitReturn(t)
			} else {
				want := "worker unavailable"
				if mode == "entry_timeout" {
					want = "entry timeout"
				}
				v2ExpectError(t, f, c, want)
			}
			v2RequireIdle(t, f)
			if rpcCtx.Err() == nil || f.pool.picks.Load() != 1 {
				t.Fatal("failed startup did not cancel exactly one RPC")
			}
		})
	}
}

func TestV2EntrySharedV1Budget(t *testing.T) {
	for _, firstV2 := range []bool{false, true} {
		name := "v1_occupies_budget"
		if firstV2 {
			name = "v2_occupies_budget"
		}
		t.Run(name, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{MaxSessions: 1, V2: &V2Config{}})
			path, version := "/v1/asr", "v1"
			if firstV2 {
				path, version = "/v2/asr", "v2"
			}
			first := f.dial(t, path)
			v2WriteJSON(t, f.ctx, first, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: version})
			if firstV2 {
				var ready wsprotocol.ReadyMessage
				v2ReadJSON(t, f.ctx, first, &ready)
				f.waitReturn(t)
			} else {
				if err := first.Write(f.ctx, websocket.MessageBinary, []byte("ab")); err != nil {
					t.Fatal(err)
				}
				var result wsprotocol.ResultMessage
				v2ReadJSON(t, f.ctx, first, &result)
				if result.Text != "ab" {
					t.Fatal("v1 did not start")
				}
			}
			if firstV2 {
				c, resp, err := websocket.Dial(f.ctx, f.url+"/v1/asr", nil)
				if c != nil {
					_ = c.CloseNow()
				}
				if err == nil || resp == nil || resp.StatusCode != 503 {
					t.Fatal("v1 exceeded shared budget", err)
				}
				f.waitReturn(t)
			} else {
				next := f.dial(t, "/v2/asr")
				v2WriteJSON(t, f.ctx, next, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
				v2ExpectError(t, f, next, "session limit exceeded")
			}
			if f.pool.picks.Load() != 1 || f.g.Snapshot().ActiveSessions != 1 {
				t.Fatal("mixed versions exceeded logical budget")
			}
			f.g.StopAccepting()
			f.cancel()
			_, _, _ = first.Read(f.ctx)
			if err := f.g.Wait(f.ctx); err != nil {
				t.Fatal(err)
			}
			v2RequireIdle(t, f)
		})
	}
}

func TestV2EntryUpgradeAndProtocolFailures(t *testing.T) {
	for _, mode := range []string{"disabled", "upgrade_failure", "binary_handshake", "oversized_handshake"} {
		t.Run(mode, func(t *testing.T) {
			cfg := Config{V2: &V2Config{}, MaxMessageBytes: 128}
			if mode == "disabled" {
				cfg.V2 = nil
			}
			f := newV2EntryFixture(t, &recordingWorker{}, cfg)
			if mode == "disabled" || mode == "upgrade_failure" {
				w := httptest.NewRecorder()
				f.g.ServeV2HTTP(w, httptest.NewRequest(http.MethodGet, "/v2/asr", nil))
				want := http.StatusUpgradeRequired
				if mode == "disabled" {
					want = 404
				}
				if w.Code != want {
					t.Fatalf("status=%d want=%d", w.Code, want)
				}
			} else {
				c := f.dial(t, "/v2/asr")
				if mode == "binary_handshake" {
					if err := c.Write(f.ctx, websocket.MessageBinary, []byte("audio")); err != nil {
						t.Fatal(err)
					}
					v2ExpectError(t, f, c, "invalid handshake")
				} else {
					_ = c.Write(f.ctx, websocket.MessageText, []byte(strings.Repeat("x", 256)))
					_, _, err := c.Read(f.ctx)
					if err == nil {
						t.Fatal("oversized handshake was accepted")
					}
					f.waitReturn(t)
				}
			}
			v2RequireIdle(t, f)
			if f.pool.picks.Load() != 0 {
				t.Fatal("failed handshake selected a Worker")
			}
		})
	}
}
