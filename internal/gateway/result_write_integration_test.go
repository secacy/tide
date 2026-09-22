package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestResultWriteTimeoutConfig 验证 New 的默认值、自定义值和非法配置。
func TestResultWriteTimeoutConfig(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value, want time.Duration
	}{
		{"default", 0, 2 * time.Second},
		{"custom", 200 * time.Millisecond, 200 * time.Millisecond},
		{"negative", -time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New(context.Background(), &recordingWorker{}, Config{ResultWriteTimeout: tc.value})
			if tc.value < 0 {
				if err == nil || g != nil || errors.Is(err, ErrResultWriteTimeout) {
					t.Fatalf("invalid config: g=%v err=%v", g, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if g.cfg.ResultWriteTimeout != tc.want {
				t.Fatalf("write timeout=%v want=%v", g.cfg.ResultWriteTimeout, tc.want)
			}
		})
	}
}

// TestResultWriteSessionTimeout 使用受控底层写入，验证完整 run 返回独立超时原因。
// 与优先级单元测试配合，避免只凭连接断开猜测退出分类。
func TestResultWriteSessionTimeout(t *testing.T) {
	s, client, gate := newResultWriter(t, 75*time.Millisecond, true)
	w := &slowReaderWorker{exit: make(chan tailWorkerExit, 1)}
	s.worker = newBaselineTCPWorkerClient(t, w)
	s.startTimeout, s.inputIdleTimeout, s.workerSendTimeout, s.tailTimeout = time.Second, time.Second, time.Second, time.Second
	appCtx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- s.run(appCtx) }()
	joined := false
	defer func() {
		stop()
		if !joined {
			_ = s.ws.CloseNow()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("session did not exit")
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := client.Write(ctx, websocket.MessageBinary, []byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal("result write did not start")
	}
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, ErrResultWriteTimeout) {
			t.Fatalf("session cause=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("session did not time out")
	}
	if appCtx.Err() != nil {
		t.Fatal("session depended on external cancellation")
	}
	if !s.resultWriteExpired() {
		t.Fatal("write deadline evidence was lost")
	}
	select {
	case exit := <-w.exit:
		if exit.err == nil {
			t.Fatal("worker unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal("worker did not exit")
	}
}

// TestResultWriteTimeoutPriority 使用已到期写入状态及先到达的 upload 事件，
// 确定性验证协调者恢复超时原因，同时保留此前约定的错误优先级。
func TestResultWriteTimeoutPriority(t *testing.T) {
	readErr := errors.New("websocket read failed before download reported")
	for _, tc := range []struct {
		name                                           string
		writeExpired, writeCanceled, stop, send, input bool
		want                                           sessionResultKind
		wantErr                                        error
	}{
		{"upload_error_first", true, false, false, false, false, resultResultWriteTimeout, ErrResultWriteTimeout},
		{"service_stop_first", true, false, true, true, true, resultServerStopping, context.Canceled},
		{"send_timeout_first", true, false, false, true, true, resultWorkerSendTimeout, ErrWorkerSendTimeout},
		{"input_timeout_first", true, false, false, false, true, resultInputIdleTimeout, ErrInputIdleTimeout},
		{"successful_write_canceled_context", false, true, false, false, false, resultClientDisconnected, readErr},
		{"no_write_yet", false, false, false, false, false, resultClientDisconnected, readErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rpcCtx, cancelRPC := context.WithCancelCause(ctx)
			defer cancelRPC(nil)
			if tc.send {
				cancelRPC(ErrWorkerSendTimeout)
			}
			if tc.stop {
				cancel()
			}
			s := &session{tailTimeout: defaultTailTimeout}
			if tc.writeExpired {
				writeCtx, stopWrite := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stopWrite()
				s.resultWriteCtx = writeCtx
			} else if tc.writeCanceled {
				writeCtx, stopWrite := context.WithCancel(context.Background())
				stopWrite()
				s.resultWriteCtx = writeCtx
			}
			if tc.input {
				readCtx, stopRead := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stopRead()
				s.inputReadCtx = readCtx
			}
			events := make(chan sessionResult, 1)
			events <- sessionResult{kind: resultClientDisconnected, err: readErr}
			got := s.waitSessionResult(ctx, rpcCtx, events, make(chan struct{}))
			if got.kind != tc.want || !errors.Is(got.err, tc.wantErr) {
				t.Fatalf("got=%+v want kind=%v err=%v", got, tc.want, tc.wantErr)
			}
		})
	}
}
