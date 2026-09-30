package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

// snapshotNoRPCClient 记录意外 RPC；状态查询不应连接 Worker。
type snapshotNoRPCClient struct{ calls atomic.Int64 }

func (c *snapshotNoRPCClient) StreamingRecognize(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	c.calls.Add(1)
	return nil, errors.New("unexpected RPC from snapshot request")
}

// newSnapshotHTTPGateway 通过正式构造路径创建配置独立的 Gateway。
func newSnapshotHTTPGateway(t *testing.T, client asrv1.ASRServiceClient, limit int) *gateway.Gateway {
	t.Helper()
	pool, err := workerpool.NewRoundRobin([]workerpool.Worker{{ID: "snapshot", Client: client}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g, err := gateway.New(ctx, pool, gateway.Config{MaxSessions: limit})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := g.Wait(ctx); err != nil {
			t.Errorf("gateway cleanup: %v", err)
		}
	})
	return g
}

// checkSnapshotResponse 独立检查响应格式，避免用生产 DTO 解码掩盖错误的标签或字段缺失。
func checkSnapshotResponse(resp *http.Response, active, limit int, stopping bool) error {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Cache-Control") != "no-store" {
		return fmt.Errorf("status=%d headers=%v", resp.StatusCode, resp.Header)
	}
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	var got map[string]any
	if err := decoder.Decode(&got); err != nil {
		return err
	}
	want := map[string]any{"schema_version": json.Number("1"), "active_sessions": json.Number(fmt.Sprint(active)), "max_sessions": json.Number(fmt.Sprint(limit)), "stopping": stopping}
	if len(got) != len(want) {
		return fmt.Errorf("unexpected fields: %v", got)
	}
	for key, value := range want {
		if got[key] != value {
			return fmt.Errorf("field %s=%v, want %v", key, got[key], value)
		}
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing response: %v %v", extra, err)
	}
	return nil
}

// TestSnapshotHTTPContract 验证格式版本、零值字段、实际配置与停止状态；查询不产生 RPC。
func TestSnapshotHTTPContract(t *testing.T) {
	for _, tc := range []struct {
		name              string
		configured, limit int
		stopping          bool
	}{{"default", 0, 64, false}, {"explicit", 3, 3, false}, {"stopped", 2, 2, true}} {
		t.Run(tc.name, func(t *testing.T) {
			client := &snapshotNoRPCClient{}
			g := newSnapshotHTTPGateway(t, client, tc.configured)
			if tc.stopping {
				g.StopAccepting()
			}
			before := g.Snapshot()
			for range 3 {
				w := httptest.NewRecorder()
				routes(g).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/gateway", nil))
				if err := checkSnapshotResponse(w.Result(), 0, tc.limit, tc.stopping); err != nil {
					t.Fatal(err)
				}
			}
			if g.Snapshot() != before || client.calls.Load() != 0 {
				t.Fatal("query changed state or called Worker")
			}
		})
	}
}

// TestSnapshotHTTPRoutes 使用真实 HTTP Server 验证 HEAD 无正文及标准路由行为。
func TestSnapshotHTTPRoutes(t *testing.T) {
	client := &snapshotNoRPCClient{}
	g := newSnapshotHTTPGateway(t, client, 1)
	srv := httptest.NewServer(routes(g))
	t.Cleanup(srv.Close)
	httpClient := &http.Client{Timeout: 3 * time.Second}
	for _, tc := range []struct {
		name, method, path string
		status             int
	}{
		{"head", http.MethodHead, "/debug/gateway", 200},
		{"post", http.MethodPost, "/debug/gateway", 405},
		{"delete", http.MethodDelete, "/debug/gateway", 405},
		{"child", http.MethodGet, "/debug/gateway/child", 404},
		{"health", http.MethodGet, "/healthz", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.status {
				t.Fatalf("status=%d body=%s", resp.StatusCode, body)
			}
			if tc.name == "head" && (len(body) != 0 || resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Cache-Control") != "no-store") {
				t.Fatalf("HEAD headers=%v body=%q", resp.Header, body)
			}
			if tc.status == 405 {
				allow := resp.Header.Get("Allow")
				if !strings.Contains(allow, "GET") || !strings.Contains(allow, "HEAD") {
					t.Fatalf("Allow=%q", allow)
				}
			}
			if tc.name == "health" && string(body) != "ok" {
				t.Fatalf("health=%q", body)
			}
		})
	}
	if got := g.Snapshot(); got.ActiveSessions != 0 || got.Stopping || client.calls.Load() != 0 {
		t.Fatalf("queries changed state: %+v", got)
	}
}

// TestSnapshotHTTPActiveSession 验证真实 ASR 路由占位、满额查询、停止后的尾部与清理。
func TestSnapshotHTTPActiveSession(t *testing.T) {
	addr, backend := startStartupBackend(t, "snapshot")
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	g := newSnapshotHTTPGateway(t, asrv1.NewASRServiceClient(cc), 1)
	appRoutes := routes(g)
	returned := make(chan struct{}, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/asr" {
			defer func() { returned <- struct{}{} }()
		}
		appRoutes.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/asr", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	httpClient := &http.Client{Timeout: 3 * time.Second}
	query := func(active int, stopping bool) error {
		resp, err := httpClient.Get(srv.URL + "/debug/gateway")
		if err != nil {
			return err
		}
		return checkSnapshotResponse(resp, active, 1, stopping)
	}
	if err := query(1, false); err != nil {
		t.Fatal(err)
	}
	// 查询不受已经占满的名额限制，也不会启动等待 start 的连接对应的 RPC。
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				if err := query(1, false); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if backend.streams.Load() != 0 {
		t.Fatal("queries started RPC before start")
	}
	rejected, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/asr", nil)
	if rejected != nil {
		_ = rejected.CloseNow()
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil || resp == nil || resp.StatusCode != 503 {
		t.Fatalf("expected capacity rejection: %v %v", resp, err)
	}
	waitReturn := func() {
		select {
		case <-returned:
		case <-ctx.Done():
			t.Fatal("ASR handler did not return")
		}
	}
	waitReturn()
	if err := query(1, false); err != nil {
		t.Fatal(err)
	}
	g.StopAccepting()
	if err := query(1, true); err != nil {
		t.Fatal(err)
	}
	startupWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
	startupWrite(t, ctx, conn, websocket.MessageBinary, "ab")
	startupResult(t, ctx, conn, "snapshot", "ab", false)
	// 查询与已有会话的收尾并发，允许读到清理前或清理后的活动数。
	wg.Go(func() {
		for range 30 {
			resp, err := httpClient.Get(srv.URL + "/debug/gateway")
			if err != nil {
				t.Error(err)
				return
			}
			var value struct {
				Active   int  `json:"active_sessions"`
				Stopping bool `json:"stopping"`
			}
			err = json.NewDecoder(resp.Body).Decode(&value)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 || !value.Stopping || value.Active < 0 || value.Active > 1 {
				t.Errorf("draining query: %+v err=%v", value, err)
				return
			}
		}
	})
	startupWrite(t, ctx, conn, websocket.MessageText, `{"type":"end"}`)
	startupResult(t, ctx, conn, "snapshot", "tail", true)
	_, _, err = conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("ASR closure: %v", err)
	}
	waitReturn()
	wg.Wait()
	startupExit(t, ctx, backend, codes.OK)
	if err := query(0, true); err != nil {
		t.Fatal(err)
	}
	if backend.streams.Load() != 1 {
		t.Fatalf("streams=%d, want 1", backend.streams.Load())
	}
}

// snapshotResponseWriter 支持失败和阻塞写回，用于验证已经读取的快照及 HTTP 错误边界。
type snapshotResponseWriter struct {
	header           http.Header
	statuses         []int
	writes           int
	body             bytes.Buffer
	err              error
	entered, release chan struct{}
}

func (w *snapshotResponseWriter) Header() http.Header  { return w.header }
func (w *snapshotResponseWriter) WriteHeader(code int) { w.statuses = append(w.statuses, code) }
func (w *snapshotResponseWriter) Write(data []byte) (int, error) {
	// 与真实 ResponseWriter 一样，首次 Write 可隐式发送 200。
	if len(w.statuses) == 0 {
		w.WriteHeader(http.StatusOK)
	}
	w.writes++
	if w.entered != nil {
		close(w.entered)
		<-w.release
	}
	if w.err != nil {
		n := len(data) / 2
		w.body.Write(data[:n])
		return n, w.err
	}
	return w.body.Write(data)
}

// TestSnapshotHTTPWriteError 验证部分写入失败后不追加 http.Error 或第二份响应。
func TestSnapshotHTTPWriteError(t *testing.T) {
	g := newSnapshotHTTPGateway(t, &snapshotNoRPCClient{}, 1)
	w := &snapshotResponseWriter{header: make(http.Header), err: io.ErrClosedPipe}
	gatewaySnapshotHandler(g)(w, httptest.NewRequest(http.MethodGet, "/debug/gateway", nil))
	if w.writes != 1 || len(w.statuses) != 1 || w.statuses[0] != 200 {
		t.Fatalf("writes=%d statuses=%v", w.writes, w.statuses)
	}
	if strings.Contains(w.body.String(), "internal server error") {
		t.Fatal("appended error response")
	}
	if s := g.Snapshot(); s.ActiveSessions != 0 || s.Stopping {
		t.Fatalf("write failure changed state: %+v", s)
	}
}

// TestSnapshotHTTPSlowWriter 验证慢写回不持有 tracker 锁，且不会把后来的状态混入本次响应。
func TestSnapshotHTTPSlowWriter(t *testing.T) {
	g := newSnapshotHTTPGateway(t, &snapshotNoRPCClient{}, 2)
	w := &snapshotResponseWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(w.release) }); <-done })
	go func() {
		defer close(done)
		gatewaySnapshotHandler(g)(w, httptest.NewRequest(http.MethodGet, "/debug/gateway", nil))
	}()
	select {
	case <-w.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("query did not reach Write")
	}
	stopped := make(chan struct{})
	go func() { defer close(stopped); g.StopAccepting() }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("slow Write blocked stopping")
	}
	other := httptest.NewRecorder()
	gatewaySnapshotHandler(g)(other, httptest.NewRequest(http.MethodGet, "/debug/gateway", nil))
	if err := checkSnapshotResponse(other.Result(), 0, 2, true); err != nil {
		t.Fatal(err)
	}
	release.Do(func() { close(w.release) })
	<-done
	resp := &http.Response{StatusCode: 200, Header: w.header, Body: io.NopCloser(bytes.NewReader(w.body.Bytes()))}
	if err := checkSnapshotResponse(resp, 0, 2, false); err != nil {
		t.Fatal(err)
	}
}
