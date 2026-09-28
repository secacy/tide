package wsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// TestWriteObservationSequence 验证控制消息不算音频、尾块按实际大小计数，
// 并将回调记录与服务端实际收到的消息逐条对照。
func TestWriteObservationSequence(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"exact_chunks", 8},
		{"partial_last_chunk", 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, conn, messages := writeTestPeer(t)
			var events []WriteEvent
			var callbackStarts []time.Time
			client := &Client{cfg: Config{ChunkBytes: 4, OnWrite: func(event WriteEvent) {
				callbackStarts = append(callbackStarts, time.Now())
				events = append(events, event)
			}}}
			input := bytes.Repeat([]byte{0xa5}, tc.size)
			before := time.Now()
			if err := client.send(ctx, conn, bytes.NewReader(input)); err != nil {
				t.Fatal(err)
			}
			wantEvents := 2 + (tc.size+3)/4
			if len(events) != wantEvents {
				t.Fatalf("events = %d, want %d", len(events), wantEvents)
			}
			var received []byte
			for i, event := range events {
				assertWriteTiming(t, event, before, callbackStarts[i])
				if event.Err != nil {
					t.Fatalf("event %d: %v", i, event.Err)
				}
				var msg writeTestMessage
				select {
				case msg = <-messages:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if i == 0 || i == len(events)-1 {
					wantKind := WriteStart
					if i != 0 {
						wantKind = WriteEnd
					}
					var env envelope
					if err := json.Unmarshal(msg.data, &env); err != nil {
						t.Fatal(err)
					}
					if event.Kind != wantKind || event.AudioBytes != 0 || msg.kind != websocket.MessageText || string(env.Type) != string(wantKind) {
						t.Fatalf("control event %+v, wire message %+v", event, msg)
					}
				} else {
					wantBytes := min(4, tc.size-len(received))
					if event.Kind != WriteAudio || event.AudioBytes != wantBytes || len(msg.data) != wantBytes || msg.kind != websocket.MessageBinary {
						t.Fatalf("audio event %+v, wire message %+v, want %d bytes", event, msg, wantBytes)
					}
					received = append(received, msg.data...)
				}
			}
			if !bytes.Equal(received, input) {
				t.Fatal("wire audio differs from input")
			}
		})
	}
}

// TestFailedWriteObservedOnce 用已关闭连接确定性触发 Write 错误；
// 失败尝试仍保留字节数和时间，调用方的错误包装可追溯到事件中的原始错误。
func TestFailedWriteObservedOnce(t *testing.T) {
	for _, kind := range []WriteKind{WriteStart, WriteAudio, WriteEnd} {
		t.Run(string(kind), func(t *testing.T) {
			ctx, conn, _ := writeTestPeer(t)
			conn.CloseNow()
			var events []WriteEvent
			var callbackAt time.Time
			client := &Client{cfg: Config{ChunkBytes: 4, OnWrite: func(event WriteEvent) {
				callbackAt = time.Now()
				events = append(events, event)
			}}}
			before := time.Now()
			var err error
			switch kind {
			case WriteStart:
				err = client.send(ctx, conn, bytes.NewReader(make([]byte, 4)))
			case WriteAudio:
				err = client.sendAudio(ctx, conn, bytes.NewReader(make([]byte, 4)))
			case WriteEnd:
				err = client.writeJSON(ctx, conn, WriteEnd, wsprotocol.EndMessage{Type: wsprotocol.MessageTypeEnd})
			}
			if err == nil || len(events) != 1 {
				t.Fatalf("error = %v, events = %+v; want error and one event", err, events)
			}
			event := events[0]
			wantBytes := 0
			if kind == WriteAudio {
				wantBytes = 4
			}
			if event.Kind != kind || event.AudioBytes != wantBytes || event.Err == nil || !errors.Is(err, event.Err) {
				t.Fatalf("event = %+v, returned error = %v", event, err)
			}
			assertWriteTiming(t, event, before, callbackAt)
		})
	}
}

// TestPreWriteErrorsHaveNoEvent 使用 nil 连接，确保读取/编码失败后
// 根本不会访问连接，也不会虚构写入事件；关闭观察时行为相同。
func TestPreWriteErrorsHaveNoEvent(t *testing.T) {
	sourceErr := errors.New("source failed")
	for _, observe := range []bool{false, true} {
		name := "nil_observer"
		if observe {
			name = "observer"
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			client := &Client{cfg: Config{ChunkBytes: 4}}
			if observe {
				client.cfg.OnWrite = func(WriteEvent) { calls++ }
			}
			err := client.sendAudio(context.Background(), nil, &failingAudioReader{err: sourceErr})
			if !errors.Is(err, sourceErr) {
				t.Fatalf("source error = %v", err)
			}
			err = client.writeJSON(context.Background(), nil, WriteEnd, make(chan int))
			var unsupported *json.UnsupportedTypeError
			if !errors.As(err, &unsupported) {
				t.Fatalf("encoding error = %v", err)
			}
			if calls != 0 {
				t.Fatalf("callbacks before Write = %d", calls)
			}
		})
	}
}

// TestSourceFailureAfterDataPreservesWrites 验证源返回数据和错误时，
// 实际成功写出的部分仍被记录，但不会发送输入正常结束的 end。
func TestSourceFailureAfterDataPreservesWrites(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	sourceErr := errors.New("input interrupted")
	var events []WriteEvent
	client := &Client{cfg: Config{ChunkBytes: 4, OnWrite: func(event WriteEvent) { events = append(events, event) }}}
	err := client.send(ctx, conn, &failingAudioReader{data: []byte{1, 2}, err: sourceErr})
	if !errors.Is(err, sourceErr) {
		t.Fatalf("error = %v, want source error", err)
	}
	if len(events) != 2 || events[0].Kind != WriteStart || events[1].Kind != WriteAudio || events[1].AudioBytes != 2 || events[1].Err != nil {
		t.Fatalf("events = %+v, want successful start and 2-byte audio only", events)
	}
}

// TestPacingCancellationHasNoExtraWrite 在首块写完后取消 context。
// 首块代表 10 秒音频，下一块仍在等待目标发送时间，因此取消发生在 Write 之前。
func TestPacingCancellationHasNoExtraWrite(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	const chunk = 10 * audio.BytesPerSecond
	var events []WriteEvent
	client := &Client{cfg: Config{ChunkBytes: chunk, Realtime: true, OnWrite: func(event WriteEvent) {
		events = append(events, event)
		cancel()
	}}}
	err := client.sendAudio(ctx, conn, bytes.NewReader(make([]byte, chunk+audio.BytesDepth)))
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "wait before sending PCM") {
		t.Fatalf("error = %v, want pacing cancellation", err)
	}
	if len(events) != 1 || events[0].Kind != WriteAudio || events[0].AudioBytes != chunk || events[0].Err != nil {
		t.Fatalf("events = %+v, want only first successful audio write", events)
	}
}

func TestWriteWithoutObserver(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	client := &Client{cfg: Config{ChunkBytes: 4}}
	if err := client.send(ctx, conn, bytes.NewReader(make([]byte, 6))); err != nil {
		t.Fatal(err)
	}
	conn.CloseNow()
	if err := client.sendAudio(ctx, conn, bytes.NewReader(make([]byte, 4))); err == nil {
		t.Fatal("nil observer suppressed a write error")
	}
}

func TestWriteObservedReturnsOriginalError(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	conn.CloseNow()
	var observed error
	calls := 0
	client := &Client{cfg: Config{OnWrite: func(event WriteEvent) {
		calls++
		observed = event.Err
	}}}
	err := client.writeObserved(ctx, conn, WriteAudio, websocket.MessageBinary, []byte{0, 0})
	if err == nil || err != observed || calls != 1 {
		t.Fatalf("returned = %v, observed = %v, callbacks = %d; want same non-nil error and one callback", err, observed, calls)
	}
}

// failingAudioReader 在返回可选数据的同时报告源错误。
type failingAudioReader struct {
	data []byte
	err  error
}

func (r *failingAudioReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, r.err
}

type writeTestMessage struct {
	kind websocket.MessageType
	data []byte
}

// writeTestPeer 创建会持续读取消息的真实本地 WebSocket 对端。
// 有限缓冲只用于这些小测试；清理时关闭连接并等待服务端协程退出。
func writeTestPeer(t *testing.T) (context.Context, *websocket.Conn, <-chan writeTestMessage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	messages := make(chan writeTestMessage, 16)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(1 << 20)
		for {
			kind, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			select {
			case messages <- writeTestMessage{kind, data}:
			case <-ctx.Done():
				return
			}
		}
	}))
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		cancel()
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.CloseNow()
		cancel()
		server.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("WebSocket peer did not exit")
		}
	})
	return ctx, conn, messages
}

func assertWriteTiming(t *testing.T, event WriteEvent, before, callbackAt time.Time) {
	t.Helper()
	if event.StartedAt.Before(before) || event.FinishedAt.Before(event.StartedAt) || event.FinishedAt.After(callbackAt) {
		t.Fatalf("write timing outside call/callback interval: %+v", event)
	}
}

var _ io.Reader = (*failingAudioReader)(nil)
