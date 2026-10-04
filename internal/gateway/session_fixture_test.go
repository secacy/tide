package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// recordingWorker 记录是否尝试创建识别流；当前测试不需要真正运行 Worker。
type recordingWorker struct {
	called atomic.Bool // 服务端 goroutine 写入，测试 goroutine 读取。
}

// StreamingRecognize 记录意外的建流调用，并返回错误。
// 未收到 start 时不应该调用此方法。
func (w *recordingWorker) StreamingRecognize(
	_ context.Context,
	_ ...grpc.CallOption,
) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	w.called.Store(true)
	return nil, errors.New("unexpected worker stream creation")
}

// dialTestSession 通过真实 WebSocket 运行单个 session，并暴露 run 的返回值。
// result 容量为 1，确保测试失败时 handler 不会阻塞在发送结果上。
// parent 决定服务生命周期；客户端使用独立 context。
func dialTestSession(t *testing.T, parent context.Context, worker asrv1.ASRServiceClient, timeout time.Duration) (*websocket.Conn, <-chan error) {
	t.Helper()
	return dialSessionWithTimeouts(t, parent, worker, timeout, defaultInputIdleTimeout)
}

// dialSessionWithTimeouts 允许分别设置启动期限和输入空闲期限，其他行为与 dialTestSession 相同。
func dialSessionWithTimeouts(t *testing.T, parent context.Context, worker asrv1.ASRServiceClient, startTimeout, inputIdleTimeout time.Duration) (*websocket.Conn, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			result <- err
			return
		}
		defer ws.CloseNow()
		result <- newSession(ws, singleWorkerPool(t, worker), startTimeout, inputIdleTimeout, defaultWorkerSendTimeout, defaultTailTimeout, defaultResultWriteTimeout, 0).run(ctx)
	}))
	var conn *websocket.Conn
	t.Cleanup(func() {
		cancel()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDial()
	var err error
	conn, _, err = websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	return conn, result
}

// assertRecognitionResult 验证完整结果及其消息类型，连续调用同时验证返回顺序。
func assertRecognitionResult(t *testing.T, ctx context.Context, conn *websocket.Conn, want wsprotocol.ResultMessage) {
	t.Helper()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read result for %s: %v", want.SegmentID, err)
	}
	var got wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if typ != websocket.MessageText || got != want {
		t.Fatalf("result type=%v body=%+v, want %+v", typ, got, want)
	}
}
