package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// inputReadResult 将服务端读取结果传回测试 goroutine，避免在 handler 中调用 Fatal。
type inputReadResult struct {
	typ  websocket.MessageType // 收到的消息类型。
	data []byte                // 收到的完整消息。
	err  error                 // 读取或超时错误。
}

// dialInputReader 为读取函数测试创建真实 WebSocket，fn 在服务端执行。
// 服务端与客户端 context 独立；失败后取消服务端读取并关闭两端资源。
func dialInputReader(t *testing.T, parent context.Context, timeout time.Duration, fn func(context.Context, *session)) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer ws.CloseNow()
		s := newSession(ws, &recordingWorker{}, defaultStartTimeout, timeout, defaultWorkerSendTimeout)
		fn(ctx, s)
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
	return conn
}

// TestReadInput 区分输入期限到期、父 context 结束与成功读取。
// end 和音频均由调用方校验，读取函数只保留类型和数据。
func TestReadInput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     websocket.MessageType
		data    string
		wantErr error
	}{
		{name: "idle_timeout", wantErr: ErrInputIdleTimeout},
		{name: "parent_canceled", wantErr: context.Canceled},
		{name: "parent_deadline", wantErr: context.DeadlineExceeded},
		{name: "audio", typ: websocket.MessageBinary, data: "audio"},
		{name: "end", typ: websocket.MessageText, data: `{"type":"end"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			timeout := time.Second
			if tc.name == "idle_timeout" {
				timeout = 50 * time.Millisecond
			}
			if tc.name == "parent_deadline" {
				var cancelDeadline context.CancelFunc
				parent, cancelDeadline = context.WithTimeout(parent, 50*time.Millisecond)
				defer cancelDeadline()
			}
			results := make(chan inputReadResult, 1)
			conn := dialInputReader(t, parent, timeout, func(ctx context.Context, s *session) {
				typ, data, err := s.readInput(ctx)
				results <- inputReadResult{typ: typ, data: data, err: err}
			})
			if tc.name == "parent_canceled" {
				cancel()
			}
			if tc.wantErr == nil {
				writeCtx, cancelWrite := context.WithTimeout(context.Background(), time.Second)
				defer cancelWrite()
				if err := conn.Write(writeCtx, tc.typ, []byte(tc.data)); err != nil {
					t.Fatalf("write input: %v", err)
				}
			}
			select {
			case result := <-results:
				if !errors.Is(result.err, tc.wantErr) {
					t.Fatalf("read error = %v, want %v", result.err, tc.wantErr)
				}
				if tc.wantErr == nil && (result.typ != tc.typ || string(result.data) != tc.data) {
					t.Fatalf("unexpected message: type=%v data=%q", result.typ, result.data)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("readInput did not return")
			}
		})
	}
}

// TestReadInputDeadlineOnlyAppliesWhileReading 验证成功读取后计时器被释放，
// 两次读取之间耗时超过输入期限，也不会关闭连接；第二次读取获得新的期限。
func TestReadInputDeadlineOnlyAppliesWhileReading(t *testing.T) {
	const timeout = 100 * time.Millisecond
	results := make(chan inputReadResult, 2)
	nextRead := make(chan struct{}) // 第一条消息读取成功后，由测试显式允许下一次读取。
	conn := dialInputReader(t, context.Background(), timeout, func(ctx context.Context, s *session) {
		for i := 0; i < 2; i++ {
			if i == 1 {
				select {
				case <-nextRead:
				case <-ctx.Done():
					return
				}
			}
			typ, data, err := s.readInput(ctx)
			results <- inputReadResult{typ: typ, data: data, err: err}
			if err != nil {
				return
			}
		}
	})
	clientCtx, cancelClient := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClient()
	for i, data := range []string{"first", "second"} {
		if i == 1 {
			// 此等待用于刻意跨过第一轮期限；读取是否完成由 results 同步。
			timer := time.NewTimer(2 * timeout)
			select {
			case <-timer.C:
			case <-clientCtx.Done():
				timer.Stop()
				t.Fatal("test timed out between reads")
			}
			close(nextRead)
		}
		if err := conn.Write(clientCtx, websocket.MessageBinary, []byte(data)); err != nil {
			t.Fatalf("write message %d: %v", i, err)
		}
		select {
		case result := <-results:
			if result.err != nil || result.typ != websocket.MessageBinary || string(result.data) != data {
				t.Fatalf("message %d: type=%v data=%q err=%v", i, result.typ, result.data, result.err)
			}
		case <-clientCtx.Done():
			t.Fatalf("read message %d timed out", i)
		}
	}
}

// TestGatewayInputIdleTimeoutConfig 验证配置默认值、自定义值和负值拒绝。
func TestGatewayInputIdleTimeoutConfig(t *testing.T) {
	for _, timeout := range []time.Duration{0, 2 * time.Second, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			g, err := New(context.Background(), &recordingWorker{}, Config{InputIdleTimeout: timeout})
			if timeout < 0 {
				if err == nil {
					t.Fatal("negative input idle timeout was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := timeout
			if want == 0 {
				want = defaultInputIdleTimeout
			}
			if g.cfg.InputIdleTimeout != want || want <= 0 {
				t.Fatalf("input idle timeout = %v, want positive %v", g.cfg.InputIdleTimeout, want)
			}
		})
	}
}
