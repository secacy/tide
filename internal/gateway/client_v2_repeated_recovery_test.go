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
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// repeatedRecoveryWorker 记录原 RPC 的实际输入、处理位置、end 与退出事实。
// mu 保护观测；active/calls 与 done 用于在夹具清理前检查自然退出。
type repeatedRecoveryWorker struct {
	asrv1.UnimplementedASRServiceServer
	mu        sync.Mutex
	audio     []byte
	processed uint64
	ended     bool
	active    atomic.Int32
	calls     atomic.Int32
	done      chan struct{}
	doneOnce  sync.Once
}

// StreamingRecognize 每块发布处理进度和对应文本；仅实际 Recv EOF 才记录 end。
func (w *repeatedRecoveryWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	w.calls.Add(1)
	w.active.Add(1)
	defer func() { w.active.Add(-1); w.doneOnce.Do(func() { close(w.done) }) }()
	for {
		req, err := s.Recv()
		if err == io.EOF {
			w.mu.Lock()
			w.ended = true
			w.mu.Unlock()
			return s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "tail", Text: "complete", IsFinal: true})
		}
		if err != nil {
			return err
		}
		w.mu.Lock()
		w.audio = append(w.audio, req.Data...)
		w.processed += uint64(len(req.Data))
		processed := w.processed
		w.mu.Unlock()
		if err := s.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: processed}}); err != nil {
			return err
		}
		if err := s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "audio", Text: string(req.Data)}); err != nil {
			return err
		}
	}
}

// repeatedRecoveryConnection 属于代理的一次连接；测试只操作 permit 并读取事件。
// request 含凭据，仅用于内存核对，禁止整体日志输出。
type repeatedRecoveryConnection struct {
	request wsprotocol.V2Handshake
	permit  chan struct{}                // 测试放行后才建立后端连接，方便观察断线期间的采集。
	ready   chan wsprotocol.ReadyMessage // 原样成功转发 ready 后发布。
	lost    chan wsprotocol.V2Input      // 指定音频被丢弃；Payload 独占副本。
	done    chan struct{}                // 两个转发任务及 socket 实际退出。
}

// repeatedRecoveryProxy 每条连接只在预定 offset 丢弃一块并切断真实 socket。
// 不修改 Gateway 状态。cuts 为不可变配置；attempts 顺序由实际客户端连接决定。
type repeatedRecoveryProxy struct {
	url      string
	attempts chan *repeatedRecoveryConnection
	cuts     map[int]uint64
	count    atomic.Int32
	active   atomic.Int32
}

// newRepeatedRecoveryProxy 预定 cuts 个断线轮次；首轮丢 offset=4，随后每轮增加 8。
// 每次接回会补发丢块与断线期间的新块；测试最终取消和关闭代理。
func newRepeatedRecoveryProxy(t *testing.T, f *v2EntryFixture, cuts int) *repeatedRecoveryProxy {
	t.Helper()
	p := &repeatedRecoveryProxy{attempts: make(chan *repeatedRecoveryConnection, 8), cuts: make(map[int]uint64)}
	for i := 1; i <= cuts; i++ {
		p.cuts[i] = uint64(4 + 8*(i-1))
	}
	ctx, cancel := context.WithCancel(f.ctx)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.active.Add(1)
		var c *repeatedRecoveryConnection
		defer func() {
			p.active.Add(-1)
			if c != nil {
				close(c.done)
			}
		}()
		front, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer front.CloseNow()
		number := int(p.count.Add(1))
		typ, data, err := front.Read(ctx)
		if err != nil || typ != websocket.MessageText {
			return
		}
		request, err := wsprotocol.DecodeV2Handshake(data)
		if err != nil {
			return
		}
		c = &repeatedRecoveryConnection{request: request, permit: make(chan struct{}), ready: make(chan wsprotocol.ReadyMessage, 1), lost: make(chan wsprotocol.V2Input, 1), done: make(chan struct{})}
		select {
		case p.attempts <- c:
		case <-ctx.Done():
			return
		}
		select {
		case <-c.permit:
		case <-ctx.Done():
			return
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
				if offset, cut := p.cuts[number]; cut && typ == websocket.MessageBinary {
					input, err := wsprotocol.DecodeV2Audio(data)
					if err != nil {
						return err
					}
					if input.Offset == offset {
						input.Payload = bytes.Clone(input.Payload)
						c.lost <- input
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
				if err := front.Write(streamCtx, typ, data); err != nil {
					return err
				}
				if tag.Type == wsprotocol.MessageTypeReady {
					var ready wsprotocol.ReadyMessage
					if err := json.Unmarshal(data, &ready); err != nil {
						return err
					}
					select {
					case c.ready <- ready:
					case <-streamCtx.Done():
						return streamCtx.Err()
					}
				}
			}
		})
		_ = group.Wait()
	}))
	p.url = "ws" + strings.TrimPrefix(server.URL, "http")
	t.Cleanup(func() { cancel(); server.Close() })
	return p
}

// repeatedRecoveryReader 记录实际源读取量，不将计划输入或 socket Write 当作采集量。
type repeatedRecoveryReader struct {
	reader *io.PipeReader
	bytes  atomic.Uint64
}

// Read 沿用唯一 PipeReader，计数实际非空返回的字节，源关闭由 Run 负责。
func (r *repeatedRecoveryReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes.Add(uint64(n))
	return n, err
}

// repeatedTake 将所有屏障等待限制在独立测试期限内；不存在就绪事件时明确失败。
func repeatedTake[T any](t *testing.T, ctx context.Context, ch <-chan T, name string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("did not observe %s: %v", name, ctx.Err())
		var zero T
		return zero
	}
}

// TestV2ClientRepeatedGatewayRecovery 验证接回后继续传输再断线的整场预算语义。
// 暂扣握手建立确定顺序；本测试不测量真实采集节奏或恢复性能。
func TestV2ClientRepeatedGatewayRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		budget   int
		cuts     int
		complete bool
	}{
		{"within_budget", 3, 2, true},
		{"last_attempt_then_complete", 2, 2, true},
		{"next_outage_exhausted", 2, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := &repeatedRecoveryWorker{done: make(chan struct{})}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, worker), Config{MaxSessions: 1, V2: &V2Config{ResumeWindow: time.Second, ResultRetentionTimeout: time.Second}})
			proxy := newRepeatedRecoveryProxy(t, f, tc.cuts)
			pr, pw := io.Pipe()
			t.Cleanup(func() { _ = pw.Close(); _ = pr.Close() })
			reader := &repeatedRecoveryReader{reader: pr}
			source := &clientGatewaySource{reader: reader, close: pr.Close}
			applied := make(chan wsprotocol.SequencedResultMessage, 16)
			client, err := wsclient.NewV2(wsclient.V2Config{URL: proxy.url, ChunkBytes: 4, Resume: &wsclient.V2ResumeConfig{MaxAttempts: tc.budget, Window: 2 * time.Second}, OnResult: func(ctx context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
				select {
				case applied <- r:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				result wsclient.V2RunResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() { result, err := client.Run(f.ctx, source); done <- outcome{result, err} }()
			// 每块内容不同，按实际 Worker 字节和结果文本发现重复/跳过。
			input := []byte("aabbccddeeffgghhiijjkkllmmnn")
			written, seen := 0, uint64(0)
			writeChunk := func() {
				t.Helper()
				if n, err := pw.Write(input[written : written+4]); err != nil || n != 4 {
					t.Fatalf("source write: n=%d err=%v", n, err)
				}
				written += 4
			}
			applyChunk := func() {
				t.Helper()
				r := repeatedTake(t, f.ctx, applied, "fresh audio result")
				if r.Seq != seen+1 || r.Text != string(input[seen*4:(seen+1)*4]) || r.IsFinal {
					t.Fatalf("audio result mismatch at seq %d", seen+1)
				}
				seen++
			}
			current := repeatedTake(t, f.ctx, proxy.attempts, "initial start")
			if current.request.Kind != wsprotocol.V2HandshakeStart {
				t.Fatal("initial request was not start")
			}
			close(current.permit)
			identity := repeatedTake(t, f.ctx, current.ready, "initial ready")
			if identity.Generation != 1 || identity.NextOffset != 0 || identity.AckedResultSeq != 0 || identity.InputEnded {
				t.Fatal("unexpected initial positions")
			}
			writeChunk()
			applyChunk()
			for round := 1; round <= tc.cuts; round++ {
				// 已观察这一连接的新音频结果，再发送下一块让代理丢弃并关闭。
				writeChunk()
				lost := repeatedTake(t, f.ctx, current.lost, "dropped audio")
				if lost.Offset != uint64(written-4) || !bytes.Equal(lost.Payload, input[written-4:written]) {
					t.Fatal("wrong dropped audio range")
				}
				repeatedTake(t, f.ctx, current.done, "old proxy handler exit")
				f.waitReturn(t)
				if round > tc.budget {
					break
				}
				waitInstanceDetached(t, f, wsprotocol.ReadyMessage{SessionID: identity.SessionID, Generation: uint64(round)})
				next := repeatedTake(t, f.ctx, proxy.attempts, "resume attempt")
				if next.request.Kind != wsprotocol.V2HandshakeResume || next.request.SessionID != identity.SessionID || next.request.ResumeToken != identity.ResumeToken || next.request.AppliedSeq != seen {
					t.Fatal("resume identity or applied position changed")
				}
				// 握手尚未进入 Gateway 时继续读取新音频，沿用同一个源和位置。
				writeChunk()
				close(next.permit)
				ready := repeatedTake(t, f.ctx, next.ready, "resumed ready")
				if ready.SessionID != identity.SessionID || ready.ResumeToken != identity.ResumeToken || ready.Generation != uint64(round+1) || ready.NextOffset != lost.Offset || ready.AckedResultSeq != seen || ready.InputEnded {
					t.Fatal("resumed positions or generation mismatch")
				}
				applyChunk() // 被丢弃音频由同一客户端重放。
				applyChunk() // 断线期间采集的新音频也到达同一 Worker。
				current = next
			}
			if tc.complete {
				writeChunk()
				applyChunk()
				_ = pw.Close()
				tail := repeatedTake(t, f.ctx, applied, "tail result")
				if tail.Seq != seen+1 || tail.Text != "complete" || !tail.IsFinal {
					t.Fatal("missing or invalid tail")
				}
				seen++
			}
			out := repeatedTake(t, f.ctx, done, "client Run exit")
			if out.result.Completed != tc.complete || out.result.CompletionAckWritten != tc.complete || out.result.ResumeAttempts != 2 || out.result.ResumedConnections != 2 || out.result.AppliedSeq != seen {
				t.Fatalf("unexpected outcome: %+v", out.result)
			}
			if tc.complete {
				if out.err != nil || out.result.FinalOffset != 24 || written != 24 {
					t.Fatalf("completion: result=%+v err=%v", out.result, out.err)
				}
				repeatedTake(t, f.ctx, current.done, "final proxy handler exit")
				f.waitReturn(t)
			} else if !errors.Is(out.err, wsclient.ErrV2RecoveryExhausted) || errors.Is(out.err, context.DeadlineExceeded) || seen != 5 || written != 24 {
				t.Fatalf("expected cumulative attempt exhaustion: result=%+v err=%v", out.result, out.err)
			}
			repeatedTake(t, f.ctx, worker.done, "Worker RPC exit")
			v2RequireIdle(t, f)
			if worker.calls.Load() != 1 || worker.active.Load() != 0 || f.pool.picks.Load() != 1 || proxy.count.Load() != 3 || proxy.active.Load() != 0 || source.closes.Load() != 1 || reader.bytes.Load() != 24 {
				t.Fatal("original RPC, source or handler ownership changed")
			}
			select {
			case <-proxy.attempts:
				t.Fatal("extra connection exceeded budget")
			default:
			}
			select {
			case <-applied:
				t.Fatal("extra result application")
			default:
			}
			worker.mu.Lock()
			actual, processed, ended := bytes.Clone(worker.audio), worker.processed, worker.ended
			worker.mu.Unlock()
			want := 24
			if !tc.complete {
				want = 20
			}
			if !bytes.Equal(actual, input[:want]) || processed != uint64(want) || ended != tc.complete {
				t.Fatal("Worker input duplicated, missing or falsely ended")
			}
			if !tc.complete {
				t.Log("captured=24 processed=20; captured but unprocessed=[20,24), not captured=[24,28); original RPC naturally expired")
			}
			t.Logf("cuts=%d attempts=2 accepted=2 original RPC=1; captured=24 processed=%d applied=%d completed=%t; source closed once, resources naturally idle", tc.cuts, processed, seen, tc.complete)
		})
	}
}
