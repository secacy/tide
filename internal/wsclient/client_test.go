package wsclient

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

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
)

// 服务端持续 Read，但故意不回应 Ping，隔离客户端独立探测及发送取消。
func TestClientHeartbeatFailure(t *testing.T) {
	for _, activeSource := range []bool{false, true} {
		t.Run(map[bool]string{false: "after_end", true: "while_sending"}[activeSource], func(t *testing.T) {
			done := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OnPingReceived: func(context.Context, []byte) bool { return false }})
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.CloseNow()
				for {
					if _, _, err := conn.Read(r.Context()); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			client, err := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Realtime: true,
				Heartbeat: wsheartbeat.Config{Interval: 20 * time.Millisecond, Timeout: 100 * time.Millisecond}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			var source io.Reader = bytes.NewReader(nil)
			if activeSource {
				source = endlessPCM{}
			}
			if err := client.Run(ctx, source); !errors.Is(err, wsheartbeat.ErrFailed) {
				t.Fatalf("expected independent heartbeat failure, got %v", err)
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("server reader not released")
			}
		})
	}
}

type endlessPCM struct{}

func (endlessPCM) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// 让服务端的正常关闭恰好发生于客户端 Ping 等待 Pong 时，不允许误报失败。
func TestClientNormalCloseDuringHeartbeat(t *testing.T) {
	var sessions, normal atomic.Int32
	done := make(chan struct{}, 20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { done <- struct{}{} }()
		defer sessions.Add(-1)
		sessions.Add(1)
		ping, end := make(chan struct{}), make(chan struct{})
		var once sync.Once
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OnPingReceived: func(context.Context, []byte) bool {
			once.Do(func() { close(ping) })
			return false // 确保 Ping 在正常关闭时仍等待 Pong。
		}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			select {
			case <-end:
			case <-r.Context().Done():
				return
			}
			select {
			case <-ping:
			case <-r.Context().Done():
				return
			}
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"result","text":"final","isFinal":true}`)); err == nil {
				if err := conn.Close(websocket.StatusNormalClosure, "completed"); err == nil {
					normal.Add(1)
				}
			}
		}()
		for {
			_, data, err := conn.Read(r.Context())
			if err != nil {
				break
			}
			if string(data) == `{"type":"end"}` {
				close(end)
			}
		}
		<-closed
	}))
	defer server.Close()
	client, err := New(Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"),
		Heartbeat: wsheartbeat.Config{Interval: time.Millisecond, Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		err := client.Run(ctx, bytes.NewReader(nil))
		cancel()
		if err != nil {
			t.Fatalf("normal close misclassified: %v", err)
		}
	}
	for range 20 {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("server handler did not finish")
		}
	}
	server.Close()
	if sessions.Load() != 0 || normal.Load() != 20 {
		t.Fatalf("remaining=%d normal=%d", sessions.Load(), normal.Load())
	}
}

func TestClientHeartbeatConfig(t *testing.T) {
	client, err := New(Config{URL: "ws://example.test"})
	if err != nil || client.cfg.Heartbeat.Interval != 2*time.Second || client.cfg.Heartbeat.Timeout != 3*time.Second {
		t.Fatalf("defaults: %v %v", client, err)
	}
	if _, err := New(Config{URL: "ws://example.test", Heartbeat: wsheartbeat.Config{Timeout: -1}}); err == nil {
		t.Fatal("negative heartbeat timeout accepted")
	}
}
