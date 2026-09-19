package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestUploadReportsInputIdleTimeout 验证 upload 自己报告准确的事件类型，
// 而不是依赖协调者将“客户端断开”修正为输入超时。
func TestUploadReportsInputIdleTimeout(t *testing.T) {
	results := make(chan sessionResult, 1)
	_ = dialInputReader(t, context.Background(), 50*time.Millisecond, func(ctx context.Context, s *session) {
		// 客户端不发送消息，不会执行任何 Worker 操作，因此不需要 stream。
		results <- s.upload(ctx, nil, make(chan struct{}))
	})
	select {
	case result := <-results:
		if result.kind != resultInputIdleTimeout || !errors.Is(result.err, ErrInputIdleTimeout) {
			t.Fatalf("upload result = {kind:%v err:%v}, want input idle timeout", result.kind, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not exit after input timeout")
	}
}

// TestWaitSessionResultInputIdlePriority 用已到期的 context 和确定的事件顺序，
// 验证写入失败先到达时仍能保留超时原因，不依赖调度碰巧复现竞争。
func TestWaitSessionResultInputIdlePriority(t *testing.T) {
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	canceled, cancelRead := context.WithCancel(context.Background())
	cancelRead()
	writeErr := errors.New("websocket write failed")

	for _, tc := range []struct {
		name        string
		readCtx     context.Context
		stopService bool
		wantKind    sessionResultKind
		wantErr     error
	}{
		{name: "write_failure_first", readCtx: expired, wantKind: resultInputIdleTimeout, wantErr: ErrInputIdleTimeout},
		{name: "service_stop_has_priority", readCtx: expired, stopService: true, wantKind: resultServerStopping, wantErr: context.Canceled},
		{name: "successful_read_cleanup_is_not_timeout", readCtx: canceled, wantKind: resultClientDisconnected, wantErr: writeErr},
		{name: "no_read_is_not_timeout", wantKind: resultClientDisconnected, wantErr: writeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.stopService {
				cancel()
			}
			s := &session{inputReadCtx: tc.readCtx}
			events := make(chan sessionResult, 1)
			// 仅放入 download 的写入失败事件，upload 超时事件尚未上报。
			events <- sessionResult{kind: resultClientDisconnected, err: writeErr}
			result := s.waitSessionResult(ctx, events, make(chan struct{}))
			if result.kind != tc.wantKind || !errors.Is(result.err, tc.wantErr) {
				t.Fatalf("result = {kind:%v err:%v}, want {kind:%v err:%v}", result.kind, result.err, tc.wantKind, tc.wantErr)
			}
		})
	}
}

// TestSessionInputIdleTimeoutCancelsRPC 验证等待首块音频及一轮双向传输后的空闲
// 都能结束会话并取消 RPC；测试没有主动取消服务或提前关闭客户端。
func TestSessionInputIdleTimeoutCancelsRPC(t *testing.T) {
	for _, sendAudio := range []bool{false, true} {
		name := "before_first_audio"
		if sendAudio {
			name = "after_audio"
		}
		t.Run(name, func(t *testing.T) {
			worker := &startProbeWorker{opened: make(chan context.Context, 1)}
			conn, result := dialSessionWithTimeouts(t, context.Background(), worker, time.Second, 100*time.Millisecond)
			clientCtx, cancelClient := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelClient()
			if err := conn.Write(clientCtx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
				t.Fatalf("send start: %v", err)
			}
			var rpcCtx context.Context
			select {
			case rpcCtx = <-worker.opened:
			case <-clientCtx.Done():
				t.Fatal("worker stream was not opened")
			}
			if sendAudio {
				if err := conn.Write(clientCtx, websocket.MessageBinary, []byte("audio")); err != nil {
					t.Fatalf("send audio: %v", err)
				}
				if _, _, err := conn.Read(clientCtx); err != nil {
					t.Fatalf("read recognition result: %v", err)
				}
			}

			// run 返回前会等待 upload/download 退出，因此返回也验证了 I/O 收尾。
			select {
			case err := <-result:
				if !errors.Is(err, ErrInputIdleTimeout) {
					t.Fatalf("session error = %v, want input idle timeout", err)
				}
			case <-clientCtx.Done():
				t.Fatal("session did not exit after input timeout")
			}
			if !errors.Is(rpcCtx.Err(), context.Canceled) {
				t.Fatalf("RPC was not canceled: %v", rpcCtx.Err())
			}
			_, _, err := conn.Read(clientCtx)
			if err == nil || clientCtx.Err() != nil {
				t.Fatalf("expected server closure before client deadline, got: %v", err)
			}
		})
	}
}
