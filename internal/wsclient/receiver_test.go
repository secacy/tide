package wsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// TestResultHandlerReceivesFieldsAndOriginalTime 验证原始接收时间和结果字段
// 完整传递，片段 final 后仍可收到下一片段。
func TestResultHandlerReceivesFieldsAndOriginalTime(t *testing.T) {
	want := observationResults()
	receivedAt := time.Date(2026, 9, 28, 10, 0, 0, 123, time.UTC)
	var got []wsprotocol.ResultMessage
	client, err := New(Config{
		URL: "ws://unused.test/v1/asr",
		OnResult: func(result wsprotocol.ResultMessage, at time.Time) {
			if at != receivedAt {
				t.Errorf("receivedAt = %v, want unchanged %v", at, receivedAt)
			}
			got = append(got, result)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range want {
		data, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.handleTextMessage(data, receivedAt); err != nil {
			t.Fatal(err)
		}
		// 回调必须在方法返回前执行，不能另启协程异步投递。
		if len(got) != i+1 {
			t.Fatalf("callbacks after message %d = %d", i, len(got))
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("results = %+v, want %+v", got, want)
	}
}

// TestResultObservationPreservesValidation 在有/无观察者两种配置下
// 验证同样的协议检查；错误消息不能作为识别结果投递。
func TestResultObservationPreservesValidation(t *testing.T) {
	for _, observe := range []bool{false, true} {
		t.Run(fmt.Sprintf("observe=%t", observe), func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				data    string
				wantErr string
			}{
				{"valid_result", `{"type":"result","segmentId":"1","text":"你好","isFinal":true}`, ""},
				{"invalid_json", `{"type":`, "decode websocket message envelope"},
				{"invalid_result", `{"type":"result","isFinal":"yes"}`, "decode result message"},
				{"gateway_error", `{"type":"error","message":"worker failed"}`, "gateway error: worker failed"},
				{"invalid_error", `{"type":"error","message":123}`, "decode error message"},
				{"unknown_type", `{"type":"other"}`, "unsupported websocket message type"},
				{"missing_type", `{}`, "unsupported websocket message type"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					calls := 0
					cfg := Config{URL: "ws://unused.test/v1/asr"}
					if observe {
						cfg.OnResult = func(wsprotocol.ResultMessage, time.Time) { calls++ }
					}
					client, err := New(cfg)
					if err != nil {
						t.Fatal(err)
					}
					err = client.handleTextMessage([]byte(tc.data), time.Now())
					checkObservationError(t, err, tc.wantErr)
					wantCalls := 0
					if observe && tc.wantErr == "" {
						wantCalls = 1
					}
					if calls != wantCalls {
						t.Fatalf("callbacks = %d, want %d", calls, wantCalls)
					}
				})
			}
		})
	}
}

// TestResultObservationThroughRun 使用真实本地 WebSocket，覆盖配置经 Run
// 到 receive 的接线。服务端读完 start/audio/end 后才发送结果，避免上传失败
// 与接收错误竞争，便于准确检验本步的接收行为。
func TestResultObservationThroughRun(t *testing.T) {
	for _, tc := range []struct {
		name    string
		observe bool
		kind    websocket.MessageType
		payload string // 空字符串表示发送多个有效结果。
		wantErr string
	}{
		{"results", true, websocket.MessageText, "", ""},
		{"no_observer", false, websocket.MessageText, "", ""},
		{"binary_rejected", true, websocket.MessageBinary, "binary", "unexpected websocket message type"},
		{"gateway_error", true, websocket.MessageText, `{"type":"error","message":"worker failed"}`, "gateway error: worker failed"},
		{"invalid_json_no_observer", false, websocket.MessageText, `{"type":`, "decode websocket message envelope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			serverDone := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				serverDone <- serveObservationSession(ctx, w, r, tc.kind, tc.payload)
			}))
			defer server.Close()
			var got []wsprotocol.ResultMessage
			var times []time.Time
			var callbackStarts []time.Time
			cfg := Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), ChunkBytes: 4}
			if tc.observe {
				cfg.OnResult = func(result wsprotocol.ResultMessage, at time.Time) {
					callbackStarts = append(callbackStarts, time.Now())
					got = append(got, result)
					times = append(times, at)
				}
			}
			client, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			before := time.Now()
			err = client.Run(ctx, bytes.NewReader([]byte{0, 0, 0, 0}))
			checkObservationError(t, err, tc.wantErr)
			select {
			case err := <-serverDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("server did not finish:", ctx.Err())
			}
			if tc.observe && tc.wantErr == "" {
				if !reflect.DeepEqual(got, observationResults()) {
					t.Fatalf("results = %+v, want %+v", got, observationResults())
				}
			} else if len(got) != 0 {
				t.Fatalf("unexpected callbacks: %+v", got)
			}
			for i, at := range times {
				if at.Before(before) || at.After(callbackStarts[i]) {
					t.Errorf("result %d time %v outside Run-start/callback-start interval", i, at)
				}
				if i > 0 && at.Before(times[i-1]) {
					t.Errorf("result timestamps went backwards")
				}
			}
		})
	}
}

// serveObservationSession 校验实际上传协议，再发送测试消息并关闭连接。
func serveObservationSession(ctx context.Context, w http.ResponseWriter, r *http.Request, kind websocket.MessageType, payload string) error {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	for i, wantType := range []websocket.MessageType{websocket.MessageText, websocket.MessageBinary, websocket.MessageText} {
		kind, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read input %d: %w", i, err)
		}
		if kind != wantType {
			return fmt.Errorf("input %d type = %v, want %v", i, kind, wantType)
		}
		switch i {
		case 0:
			var start wsprotocol.StartMessage
			if err := json.Unmarshal(data, &start); err != nil || start.Type != wsprotocol.MessageTypeStart || start.Version != "v1" {
				return fmt.Errorf("invalid start: %s", data)
			}
		case 1:
			if !bytes.Equal(data, []byte{0, 0, 0, 0}) {
				return fmt.Errorf("unexpected audio: %x", data)
			}
		case 2:
			var end wsprotocol.EndMessage
			if err := json.Unmarshal(data, &end); err != nil || end.Type != wsprotocol.MessageTypeEnd {
				return fmt.Errorf("invalid end: %s", data)
			}
		}
	}
	if payload != "" {
		// 客户端读到错误后会取消连接，不要求此路径完成关闭握手。
		return conn.Write(ctx, kind, []byte(payload))
	}
	for _, result := range observationResults() {
		data, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return err
		}
	}
	return conn.Close(websocket.StatusNormalClosure, "complete")
}

// observationResults 特意在第一段 final 后继续第二段，防止把片段定稿当成整场完成。
func observationResults() []wsprotocol.ResultMessage {
	return []wsprotocol.ResultMessage{
		{Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: "第一段", IsFinal: true},
		{Type: wsprotocol.MessageTypeResult, SegmentID: "2", Text: "第二", IsFinal: false},
		{Type: wsprotocol.MessageTypeResult, SegmentID: "2", Text: "第二段", IsFinal: true},
	}
}

func checkObservationError(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want containing %q", err, want)
	}
}
