package gateway

import (
	"bytes"
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
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	"golang.org/x/sync/errgroup"
)

var errClientTestCut = errors.New("controlled websocket cut")

// clientFaultProxy 在真实两条 WebSocket 之间选择性丢失消息并关闭连接。
// Gateway 与 TCP gRPC Worker 均使用生产实现；测试不修改内部会话状态。
type clientFaultProxy struct {
	url         string
	mode        string
	cut         chan struct{}
	allowResume chan struct{}
	cutOnce     sync.Once
	dropReady   atomic.Bool
	connections atomic.Int32
	mu          sync.Mutex
	requests    []wsprotocol.V2Handshake
	generations []uint64
	tasks       sync.WaitGroup
}

func newClientFaultProxy(t *testing.T, f *v2EntryFixture, mode string) *clientFaultProxy {
	t.Helper()
	p := &clientFaultProxy{mode: mode, cut: make(chan struct{}), allowResume: make(chan struct{})}
	ctx, cancel := context.WithCancel(f.ctx)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.tasks.Add(1)
		defer p.tasks.Done()
		front, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer front.CloseNow()
		number := p.connections.Add(1)
		typ, data, err := front.Read(ctx)
		if err != nil {
			return
		}
		request, err := wsprotocol.DecodeV2Handshake(data)
		if err != nil || typ != websocket.MessageText {
			return
		}
		p.mu.Lock()
		p.requests = append(p.requests, request)
		p.mu.Unlock()
		if number > 1 {
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
				if number == 1 && typ == websocket.MessageText {
					input, err := wsprotocol.DecodeV2Control(data)
					if err != nil {
						return err
					}
					cut := input.Kind == wsprotocol.V2InputResultAck && (mode == "audio_and_result_ack_lost" || mode == "resume_ready_lost") || input.Kind == wsprotocol.V2InputEnd && mode == "end_lost" || input.Kind == wsprotocol.V2InputCompletedAck && mode == "completed_ack_lost"
					if cut {
						p.cutOnce.Do(func() { close(p.cut) })
						return errClientTestCut
					}
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
				var tag struct {
					Type wsprotocol.MessageType `json:"type"`
				}
				if err := json.Unmarshal(data, &tag); err != nil {
					return err
				}
				if tag.Type == wsprotocol.MessageTypeReady {
					var ready wsprotocol.ReadyMessage
					if err := json.Unmarshal(data, &ready); err != nil {
						return err
					}
					p.mu.Lock()
					p.generations = append(p.generations, ready.Generation)
					p.mu.Unlock()
					if number == 1 && mode == "initial_ready_lost" || number > 1 && mode == "resume_ready_lost" && p.dropReady.CompareAndSwap(false, true) {
						p.cutOnce.Do(func() { close(p.cut) })
						return errClientTestCut
					}
				}
				if number == 1 && tag.Type == wsprotocol.MessageTypeAudioAck && (mode == "audio_and_result_ack_lost" || mode == "resume_ready_lost") {
					continue
				}
				if number == 1 && tag.Type == wsprotocol.MessageTypeCompleted && mode == "completed_lost" {
					p.cutOnce.Do(func() { close(p.cut) })
					return errClientTestCut
				}
				if err := front.Write(streamCtx, typ, data); err != nil {
					return err
				}
			}
		})
		_ = group.Wait()
	}))
	p.url = "ws" + strings.TrimPrefix(server.URL, "http")
	t.Cleanup(func() { cancel(); server.Close(); p.tasks.Wait() })
	return p
}

type clientGatewaySource struct {
	reader io.Reader
	close  func() error
	closes atomic.Int32
}

func (s *clientGatewaySource) Read(data []byte) (int, error) { return s.reader.Read(data) }
func (s *clientGatewaySource) Close() error {
	s.closes.Add(1)
	if s.close != nil {
		return s.close()
	}
	return nil
}

func TestV2ClientGatewayRecovery(t *testing.T) {
	for _, name := range []string{"audio_and_result_ack_lost", "resume_ready_lost", "end_lost", "completed_lost", "jitter_resume_ready_lost"} {
		t.Run(name, func(t *testing.T) {
			mode := strings.TrimPrefix(name, "jitter_")
			resume := &wsclient.V2ResumeConfig{MaxAttempts: 10, Window: 2 * time.Second}
			if name == "jitter_resume_ready_lost" {
				// 丢掉首个 resume ready，确保实际走过失败后的随机等待再接回原 RPC。
				resume.Jitter, resume.JitterSeed = true, 202610110101
			}
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{MaxSessions: 1, V2: &V2Config{ResumeWindow: 2 * time.Second, ResultRetentionTimeout: 2 * time.Second}})
			proxy := newClientFaultProxy(t, f, mode)
			input := []byte("aabbccddxx")
			source := &clientGatewaySource{reader: bytes.NewReader(input)}
			producerDone := make(chan error, 1)
			if mode == "audio_and_result_ack_lost" || mode == "resume_ready_lost" {
				pr, pw := io.Pipe()
				source.reader = pr
				source.close = pr.Close
				t.Cleanup(func() { _ = pw.Close() })
				go func() {
					if _, err := pw.Write(input[:4]); err != nil {
						producerDone <- err
						return
					}
					select {
					case <-proxy.cut:
					case <-f.ctx.Done():
						producerDone <- f.ctx.Err()
						return
					}
					// 没有允许恢复握手，先完成断线期间的采集；错误关闭源会直接使 Write 失败。
					_, err := pw.Write(input[4:])
					_ = pw.Close()
					close(proxy.allowResume)
					producerDone <- err
				}()
			} else {
				close(proxy.allowResume)
			}
			var applied []uint64
			c, err := wsclient.NewV2(wsclient.V2Config{URL: proxy.url, ChunkBytes: 4, Resume: resume, OnResult: func(_ context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
				applied = append(applied, r.Seq)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := c.Run(f.ctx, source)
			if err != nil || !result.Completed || !result.CompletionAckWritten || result.FinalOffset != uint64(len(input)) || result.ResumedConnections != 1 || result.ResumeAttempts < 1 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if mode == "resume_ready_lost" && (!proxy.dropReady.Load() || result.ResumeAttempts < 2) {
				t.Fatalf("lost-ready not recovered: %+v", result)
			}
			if mode == "audio_and_result_ack_lost" || mode == "resume_ready_lost" {
				select {
				case err := <-producerDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-f.ctx.Done():
					t.Fatal("producer did not finish")
				}
			}
			if source.closes.Load() != 1 {
				t.Fatalf("source Close count=%d", source.closes.Load())
			}
			v2RequireIdle(t, f)
			if backend.calls.Load() != 1 || f.pool.picks.Load() != 1 {
				t.Fatalf("Worker calls=%d picks=%d", backend.calls.Load(), f.pool.picks.Load())
			}
			backend.mu.Lock()
			var actual []byte
			for _, chunk := range backend.audio {
				actual = append(actual, chunk...)
			}
			backend.mu.Unlock()
			if !bytes.Equal(actual, input) {
				t.Fatalf("Worker audio=%q want=%q", actual, input)
			}
			if len(applied) != 4 || result.AppliedSeq != 4 {
				t.Fatalf("applied=%v result=%+v", applied, result)
			}
			for i, seq := range applied {
				if seq != uint64(i+1) {
					t.Fatalf("duplicate or gap: %v", applied)
				}
			}
			proxy.mu.Lock()
			requests := append([]wsprotocol.V2Handshake(nil), proxy.requests...)
			generations := append([]uint64(nil), proxy.generations...)
			proxy.mu.Unlock()
			// 每条已解析并转发的握手都对应一次 Gateway handler；等待真实返回。
			for range requests {
				f.waitReturn(t)
			}
			starts := 0
			for _, request := range requests {
				if request.Kind == wsprotocol.V2HandshakeStart {
					starts++
				} else if request.Kind != wsprotocol.V2HandshakeResume {
					t.Fatal("unexpected handshake")
				}
			}
			if starts != 1 {
				t.Fatalf("start count=%d", starts)
			}
			if mode == "audio_and_result_ack_lost" || mode == "resume_ready_lost" {
				for _, request := range requests[1:] {
					if request.AppliedSeq != 1 {
						t.Fatalf("lost result ACK position=%d", request.AppliedSeq)
					}
				}
			}
			if mode == "resume_ready_lost" && (len(generations) < 3 || generations[len(generations)-1] < 3) {
				t.Fatalf("no skipped generation: %v", generations)
			}
			t.Logf("resume attempts=%d accepted=%d generations=%v; worker calls=%d picks=%d audio bytes=%d applied results=%d; registry/tracker idle",
				result.ResumeAttempts, result.ResumedConnections, generations, backend.calls.Load(), f.pool.picks.Load(), len(actual), len(applied))
		})
	}
}

func TestV2ClientGatewayUnrecoverableBoundaries(t *testing.T) {
	for _, mode := range []string{"initial_ready_lost", "completed_ack_lost"} {
		t.Run(mode, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{V2: &V2Config{ResumeWindow: 200 * time.Millisecond, ResultRetentionTimeout: 200 * time.Millisecond}})
			proxy := newClientFaultProxy(t, f, mode)
			close(proxy.allowResume)
			source := &clientGatewaySource{reader: bytes.NewReader(nil)}
			c, err := wsclient.NewV2(wsclient.V2Config{URL: proxy.url, Resume: &wsclient.V2ResumeConfig{}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := c.Run(f.ctx, source)
			if result.ResumeAttempts != 0 || result.ResumedConnections != 0 || source.closes.Load() != 1 {
				t.Fatalf("result=%+v closes=%d err=%v", result, source.closes.Load(), err)
			}
			if mode == "initial_ready_lost" && (err == nil || result.Completed) {
				t.Fatalf("initial ready loss result=%+v err=%v", result, err)
			}
			if mode == "completed_ack_lost" && !result.Completed {
				t.Fatalf("lost completion fact: %+v err=%v", result, err)
			}
			select {
			case <-proxy.cut:
			case <-f.ctx.Done():
				t.Fatal("fault was not triggered")
			}
			v2RequireIdle(t, f) // 未确认的远端会话由原固定保留窗口清理。
			f.waitReturn(t)
			if f.pool.picks.Load() != 1 || proxy.connections.Load() != 1 {
				t.Fatal("unexpected restart after identity/completion boundary")
			}
		})
	}
}

// 服务端原场已到期后，仍保留凭据的客户端收到明确不可恢复拒绝，不能回退 start。
func TestV2ClientGatewayExpiredSession(t *testing.T) {
	backend := &v2EntryBackend{}
	f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{MaxSessions: 1, V2: &V2Config{ResumeWindow: 200 * time.Millisecond}})
	proxy := newClientFaultProxy(t, f, "audio_and_result_ack_lost")
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })
	source := &clientGatewaySource{reader: pr, close: pr.Close}
	c, err := wsclient.NewV2(wsclient.V2Config{URL: proxy.url, ChunkBytes: 4, Resume: &wsclient.V2ResumeConfig{Window: 2 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result wsclient.V2RunResult
		err    error
	}
	returned := make(chan outcome, 1)
	go func() { result, err := c.Run(f.ctx, source); returned <- outcome{result, err} }()
	if _, err := pw.Write([]byte("aabb")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-proxy.cut:
	case <-f.ctx.Done():
		t.Fatal("fault did not trigger")
	}
	_ = pw.Close()
	// 阻止恢复请求进入 Gateway，等原场实际到期清理，不用睡眠猜测到期。
	v2RequireIdle(t, f)
	close(proxy.allowResume)
	select {
	case out := <-returned:
		var entry *wsclient.V2EntryError
		if !errors.As(out.err, &entry) || entry.Code != wsprotocol.V2ErrorResumeUnavailable || out.result.Completed || out.result.ResumeAttempts != 1 {
			t.Fatalf("result=%+v err=%v", out.result, out.err)
		}
	case <-f.ctx.Done():
		t.Fatal("Run did not return")
	}
	if source.closes.Load() != 1 || f.pool.picks.Load() != 1 || backend.calls.Load() != 1 {
		t.Fatal("expired session restarted or source leaked")
	}
	v2RequireIdle(t, f)
	f.waitReturn(t)
	f.waitReturn(t)
}
