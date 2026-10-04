package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// routingBackend 回显后端 ID 与会话音频，检测同流混入另一会话数据。
// started/exits 是测试观测，不是生产会话计数或容量指标。
type routingBackend struct {
	asrv1.UnimplementedASRServiceServer
	id      string
	started atomic.Int64
	exits   chan error
}

func (w *routingBackend) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) (err error) {
	w.started.Add(1)
	defer func() { w.exits <- err }()
	var sessionID string
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: sessionID, Text: w.id + ":" + sessionID + ":tail", IsFinal: true})
		}
		if recvErr != nil {
			return recvErr
		}
		id, _, ok := strings.Cut(string(req.GetData()), ":")
		if !ok || (sessionID != "" && sessionID != id) {
			return status.Error(codes.InvalidArgument, "audio crossed session boundary")
		}
		sessionID = id
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: id, Text: w.id + ":" + string(req.GetData())}); err != nil {
			return err
		}
	}
}

// routingFixture 运行真实 Gateway handler，handler 完成通知晚于 tracker.leave。
// 所有业务断言发生在服务取消/测试兜底清理之前。
type routingFixture struct {
	g        *Gateway
	pool     WorkerSelector
	url      string
	handlers chan string
	cancel   context.CancelFunc
}

func newRoutingFixture(t *testing.T, workers []workerpool.Worker, cfg Config) *routingFixture {
	t.Helper()
	pool, err := workerpool.NewRoundRobin(workers)
	if err != nil {
		t.Fatal(err)
	}
	return newSelectorRoutingFixture(t, pool, cfg)
}

// newSelectorRoutingFixture 复用真实路由夹具，让不同选择器接受相同的生命周期检查。
func newSelectorRoutingFixture(t *testing.T, pool WorkerSelector, cfg Config) *routingFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	g, err := New(ctx, pool, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	f := &routingFixture{g: g, pool: pool, handlers: make(chan string, 128), cancel: cancel}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { f.handlers <- r.URL.Path }()
		g.ServeHTTP(w, r)
	}))
	f.url = "ws" + strings.TrimPrefix(server.URL, "http")
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := g.Wait(ctx); err != nil {
			t.Error(err)
		}
		server.Close()
	})
	return f
}
func newRoutingBackends(t *testing.T) ([]workerpool.Worker, []*routingBackend) {
	t.Helper()
	var workers []workerpool.Worker
	var backends []*routingBackend
	for _, id := range []string{"A", "B"} {
		backend := &routingBackend{id: id, exits: make(chan error, 128)}
		workers = append(workers, workerpool.Worker{ID: id, Client: newBaselineTCPWorkerClient(t, backend)})
		backends = append(backends, backend)
	}
	return workers, backends
}
func routingContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func (f *routingFixture) dial(t *testing.T, ctx context.Context, path string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, f.url+"/"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}
func routingWrite(t *testing.T, ctx context.Context, conn *websocket.Conn, kind websocket.MessageType, data string) {
	t.Helper()
	if err := conn.Write(ctx, kind, []byte(data)); err != nil {
		t.Fatal(err)
	}
}
func (f *routingFixture) waitHandlers(t *testing.T, ctx context.Context, n int) {
	t.Helper()
	for range n {
		select {
		case <-f.handlers:
		case <-ctx.Done():
			t.Fatal("handler cleanup timed out")
		}
	}
}
func (f *routingFixture) assertActive(t *testing.T, want int) {
	t.Helper()
	f.g.tracker.mu.Lock()
	got := f.g.tracker.active
	f.g.tracker.mu.Unlock()
	if got != want {
		t.Fatalf("active=%d want=%d", got, want)
	}
}
func routingResult(t *testing.T, ctx context.Context, conn *websocket.Conn, id, worker, suffix string, final bool) {
	t.Helper()
	assertRecognitionResult(t, ctx, conn, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: id, Text: worker + ":" + id + ":" + suffix, IsFinal: final})
}
func finishRoutingSession(t *testing.T, ctx context.Context, conn *websocket.Conn, id, worker string) {
	t.Helper()
	for _, part := range []string{"second", "third"} {
		routingWrite(t, ctx, conn, websocket.MessageBinary, id+":"+part)
		routingResult(t, ctx, conn, id, worker, part, false)
	}
	routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"end"}`)
	routingResult(t, ctx, conn, id, worker, "tail", true)
	_, _, err := conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("expected normal close: %v", err)
	}
}
func checkRoutingExits(t *testing.T, ctx context.Context, backends []*routingBackend, want int) {
	t.Helper()
	for _, w := range backends {
		if got := w.started.Load(); got != int64(want) {
			t.Errorf("Worker %s streams=%d want=%d", w.id, got, want)
		}
		for range want {
			select {
			case err := <-w.exits:
				if err != nil {
					t.Errorf("Worker %s exit: %v", w.id, err)
				}
			case <-ctx.Done():
				t.Fatal("Worker did not exit")
			}
		}
	}
}

// runRoutingBeforeStart 执行连接在合法 start 前退出的公共操作，并等待名额释放。
// mode 指定失败场景；选择次数和后端调用断言由各策略测试保留。
func runRoutingBeforeStart(t *testing.T, ctx context.Context, f *routingFixture, mode string) {
	t.Helper()
	switch mode {
	case "stopped_admission":
		f.g.StopAccepting()
		conn, response, err := websocket.Dial(ctx, f.url, nil)
		if conn != nil {
			conn.CloseNow()
		}
		if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected 503: response=%v err=%v", response, err)
		}
	case "failed_upgrade":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http"+strings.TrimPrefix(f.url, "ws"), nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode == http.StatusSwitchingProtocols {
			t.Fatal("unexpected upgrade")
		}
	default:
		conn := f.dial(t, ctx, mode)
		switch mode {
		case "invalid_start":
			routingWrite(t, ctx, conn, websocket.MessageText, `{"type":"end"}`)
		case "disconnect_before_start":
			conn.CloseNow()
		case "start_timeout":
			// 等待配置的启动期限。
		case "shutdown_before_start":
			f.g.StopAccepting()
			f.cancel()
		default:
			t.Fatalf("unknown pre-start scenario: %s", mode)
		}
		if mode != "disconnect_before_start" {
			_, _, err := conn.Read(ctx)
			if err == nil || ctx.Err() != nil {
				t.Fatalf("server did not close independently: %v", err)
			}
		}
	}
	f.waitHandlers(t, ctx, 1)
	f.assertActive(t, 0)
}
