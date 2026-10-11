package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// switchableGatewayEntry 持有固定 URL，在每次新 HTTP 请求进入时选择目标 Gateway。
// 旧 WebSocket 仍由原 Gateway 持有；切换目标不会迁移已有连接或会话。
type switchableGatewayEntry struct {
	url         string
	target      atomic.Pointer[v2EntryFixture]
	requests    atomic.Int32
	active      atomic.Int32
	tasks       sync.WaitGroup
	resumeSeen  chan struct{} // 第二次 HTTP 请求已到达，尚未升级或进入目标 Gateway。
	allowResume chan struct{} // 测试完成旧实例退出检查后关闭。
}

// newGatewayFailureInstance 创建独立注册表、生命周期和 gRPC 客户端连接。
// 不创建自己的 HTTP Server；由固定测试入口调用公开 ServeV2HTTP。
func newGatewayFailureInstance(t *testing.T, ctx context.Context, workerAddress string) *v2EntryFixture {
	t.Helper()
	conn, err := grpc.NewClient(workerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	life, cancel := context.WithCancel(ctx)
	pool := &v2EntrySelector{client: asrv1.NewASRServiceClient(conn)}
	g, err := New(life, pool, Config{MaxSessions: 1, WorkerSendTimeout: 30 * time.Second, V2: &V2Config{ResumeWindow: 30 * time.Second}})
	if err != nil {
		cancel()
		_ = conn.Close()
		t.Fatal(err)
	}
	f := &v2EntryFixture{g: g, pool: pool, ctx: ctx, cancel: cancel}
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := g.Wait(cleanupCtx); err != nil {
			t.Errorf("Gateway fixture cleanup: %v", err)
		}
		_ = conn.Close()
	})
	return f
}

// newSwitchableGatewayEntry 在首场之后暂扣第二次 HTTP 请求，固定恢复的交接时点。
// 仅按请求次序设置屏障；错误码和握手解析仍由真实 Gateway 与客户端处理。
func newSwitchableGatewayEntry(t *testing.T, ctx context.Context, original *v2EntryFixture) *switchableGatewayEntry {
	t.Helper()
	e := &switchableGatewayEntry{resumeSeen: make(chan struct{}), allowResume: make(chan struct{})}
	e.target.Store(original)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.tasks.Add(1)
		defer e.tasks.Done()
		e.active.Add(1)
		defer e.active.Add(-1)
		if e.requests.Add(1) == 2 {
			close(e.resumeSeen)
			select {
			case <-e.allowResume:
			case <-r.Context().Done():
				return
			case <-ctx.Done():
				return
			}
		}
		e.target.Load().g.ServeV2HTTP(w, r)
	}))
	e.url = "ws" + strings.TrimPrefix(server.URL, "http") + "/v2/asr"
	t.Cleanup(func() { server.Close(); e.tasks.Wait() })
	return e
}

// requireIdle 在客户端 Run 返回后等待 HTTP handler 自然返回，不关闭 Server 兜底。
func (e *switchableGatewayEntry) requireIdle(t *testing.T, ctx context.Context) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for e.active.Load() != 0 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("fixed entry handlers did not return naturally")
		}
	}
	e.tasks.Wait()
}

// soleGatewayFailureSession 只复制注册表中的对象引用，不读取会话可变字段。
func soleGatewayFailureSession(t *testing.T, f *v2EntryFixture) *resumableSession {
	t.Helper()
	f.g.registry.mu.Lock()
	defer f.g.registry.mu.Unlock()
	if len(f.g.registry.entries) != 1 {
		t.Fatalf("expected one original session, got %d", len(f.g.registry.entries))
	}
	for _, s := range f.g.registry.entries {
		return s
	}
	panic("unreachable")
}

func TestV2ClientGatewayInstanceFailure(t *testing.T) {
	for _, replaceBeforeResume := range []bool{false, true} {
		name := "stopping_entry"
		if replaceBeforeResume {
			name = "fresh_instance"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			t.Cleanup(cancel)
			w := newRestartableInstanceWorker(t)
			original := newGatewayFailureInstance(t, ctx, w.address)
			entry := newSwitchableGatewayEntry(t, ctx, original)
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = writer.Close() })
			source := &clientGatewaySource{reader: reader, close: reader.Close}
			applied := make(chan wsprotocol.SequencedResultMessage, 4)
			client, err := wsclient.NewV2(wsclient.V2Config{URL: entry.url, ChunkBytes: 4, Resume: &wsclient.V2ResumeConfig{Window: 4 * time.Second}, OnResult: func(ctx context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
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
			returned := make(chan outcome, 1)
			go func() { result, err := client.Run(ctx, source); returned <- outcome{result, err} }()
			if _, err := writer.Write([]byte("aabbccdd")); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-applied:
				if result.Seq != 1 || result.Text != "aabb" || result.IsFinal {
					t.Fatal("unexpected first result")
				}
			case <-ctx.Done():
				t.Fatal("first result was not applied")
			}
			waitInstanceSignal(t, ctx, w.backend.held, "second audio chunk held before processing")
			s := soleGatewayFailureSession(t, original)
			ready, err := s.requestConnectionReady(ctx, 1)
			if err != nil || ready.nextOffset != 8 || ready.inputEnded {
				t.Fatalf("original Gateway has not accepted eight bytes: %v", err)
			}

			// Worker 服务保持运行，原 RPC 必须由 Gateway 的会话取消解除阻塞。
			original.g.StopAccepting()
			original.cancel()
			if err := original.g.Wait(ctx); err != nil {
				t.Fatalf("original Gateway did not finish cleanup: %v", err)
			}
			waitInstanceSignal(t, ctx, w.backend.firstDone, "Worker RPC exit after Gateway cancellation")
			v2RequireIdle(t, original)
			if !original.g.Snapshot().Stopping || w.backend.active.Load() != 0 {
				t.Fatal("original Gateway or Worker did not stop its session")
			}
			waitInstanceSignal(t, ctx, entry.resumeSeen, "client recovery HTTP request")
			var fresh *v2EntryFixture
			wantCode := wsprotocol.V2ErrorServiceStopping
			if replaceBeforeResume {
				fresh = newGatewayFailureInstance(t, ctx, w.address)
				if fresh.g.registry == original.g.registry {
					t.Fatal("new Gateway shares the old registry")
				}
				entry.target.Store(fresh)
				wantCode = wsprotocol.V2ErrorResumeUnavailable
			}
			close(entry.allowResume)
			var out outcome
			select {
			case out = <-returned:
			case <-ctx.Done():
				t.Fatal("original Run did not return")
			}
			entry.requireIdle(t, ctx)
			var rejected *wsclient.V2EntryError
			if !errors.As(out.err, &rejected) || rejected.Code != wantCode || out.result.Completed || out.result.CompletionAckWritten || out.result.AppliedSeq != 1 || out.result.ResumeAttempts != 1 || out.result.ResumedConnections != 0 {
				t.Fatalf("original outcome=%+v code=%v err=%v", out.result, wantCode, out.err)
			}
			if source.closes.Load() != 1 || original.pool.picks.Load() != 1 || entry.requests.Load() != 2 || len(applied) != 0 {
				t.Fatal("original source leaked, result duplicated, or session restarted")
			}
			if fresh != nil {
				v2RequireIdle(t, fresh)
				if fresh.pool.picks.Load() != 0 {
					t.Fatal("fresh Gateway opened a Worker for an unavailable resume")
				}
			} else {
				// 同一停止入口也拒绝显式新场；不得因为 Worker 仍存活就重新接入。
				probe, err := wsclient.NewV2(wsclient.V2Config{URL: entry.url, ChunkBytes: 4})
				if err != nil {
					t.Fatal(err)
				}
				probeSource := &clientGatewaySource{reader: bytes.NewReader([]byte("zzxx"))}
				probeResult, err := probe.Run(ctx, probeSource)
				rejected = nil
				if !errors.As(err, &rejected) || rejected.Code != wsprotocol.V2ErrorServiceStopping || probeResult.Completed || probeSource.closes.Load() != 1 || original.pool.picks.Load() != 1 {
					t.Fatalf("stopped entry accepted a new session: result=%+v err=%v", probeResult, err)
				}
				entry.requireIdle(t, ctx)
				fresh = newGatewayFailureInstance(t, ctx, w.address)
				entry.target.Store(fresh)
			}
			w.backend.mu.Lock()
			originalCalls := len(w.backend.records)
			w.backend.mu.Unlock()
			if originalCalls != 1 || fresh.pool.picks.Load() != 0 {
				t.Fatal("an old recovery or rejected start created a replacement Worker RPC")
			}

			// URL 不变，只有调用方显式发起的新 Run 可以建立新场。
			newInput := []byte("11223344")
			newSource := &clientGatewaySource{reader: bytes.NewReader(newInput)}
			var results []wsprotocol.SequencedResultMessage
			newClient, err := wsclient.NewV2(wsclient.V2Config{URL: entry.url, ChunkBytes: 4, OnResult: func(_ context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
				results = append(results, r)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := newClient.Run(ctx, newSource)
			if err != nil || !result.Completed || !result.CompletionAckWritten || result.FinalOffset != 8 || result.AppliedSeq != 3 || result.ResumeAttempts != 0 || newSource.closes.Load() != 1 {
				t.Fatalf("new Gateway session outcome=%+v err=%v", result, err)
			}
			if len(results) != 3 || results[0].Seq != 1 || results[0].Text != "1122" || results[1].Seq != 2 || results[1].Text != "3344" || results[2].Seq != 3 || results[2].Text != "complete" || !results[2].IsFinal {
				t.Fatalf("new results mixed with the old session: %+v", results)
			}
			entry.requireIdle(t, ctx)
			v2RequireIdle(t, fresh)
			v2RequireIdle(t, original)
			wantRequests := int32(3)
			if !replaceBeforeResume {
				wantRequests++ // 停止入口上的独立新场探测。
			}
			if entry.requests.Load() != wantRequests || original.pool.picks.Load() != 1 || fresh.pool.picks.Load() != 1 || fresh.g.Snapshot().Stopping || w.backend.active.Load() != 0 {
				t.Fatal("unexpected request, Worker selection, or remaining resources")
			}
			w.backend.mu.Lock()
			defer w.backend.mu.Unlock()
			if len(w.backend.records) != 2 {
				t.Fatal("unexpected Worker RPC count")
			}
			old, current := w.backend.records[0], w.backend.records[1]
			if string(old.audio) != "aabbccdd" || old.processed != 4 || old.ended || !old.exited {
				t.Fatal("original RPC was completed or its cleanup was not observed")
			}
			if !bytes.Equal(current.audio, newInput) || current.processed != 8 || !current.ended || !current.exited {
				t.Fatal("new RPC did not independently process the full input")
			}
		})
	}
}
