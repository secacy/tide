package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

func TestV2EntryErrorHTTPNetwork(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		name := "handshake_limit"
		if stopping {
			name = "service_stopping"
		}
		t.Run(name, func(t *testing.T) {
			f := newV2EntryFixture(t, &recordingWorker{}, Config{V2: &V2Config{MaxHandshakes: 1}})
			code, message := wsprotocol.V2ErrorHandshakeLimit, "handshake limit exceeded"
			if stopping {
				f.g.StopAccepting()
				code, message = wsprotocol.V2ErrorServiceStopping, "service is stopping"
			} else {
				if err := f.g.gate.tryEnterHandshake(); err != nil {
					t.Fatal(err)
				}
				defer f.g.gate.leaveHandshake()
			}
			c, resp, err := websocket.Dial(f.ctx, f.url+"/v2/asr", nil)
			if c != nil {
				_ = c.CloseNow()
			}
			if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Content-Type") != "application/json; charset=utf-8" {
				t.Fatal("wrong upgrade refusal", resp, err)
			}
			// 库保留最多 1024 字节的失败响应体，足以读取固定的小错误消息。
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			decodeV2PublicError(t, data, code, message)
			f.waitReturn(t)
			if f.pool.picks.Load() != 0 || f.g.Snapshot().ActiveSessions != 0 {
				t.Fatal("HTTP refusal started a Worker")
			}
		})
	}
}

func TestV2EntryErrorAuthenticationIndistinguishable(t *testing.T) {
	backend := &v2EntryBackend{}
	f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{MaxSessions: 1, V2: &V2Config{}})
	original := f.dial(t, "/v2/asr")
	v2WriteJSON(t, f.ctx, original, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
	var ready wsprotocol.ReadyMessage
	v2ReadJSON(t, f.ctx, original, &ready)
	f.waitReturn(t)
	var first json.RawMessage
	for _, unknown := range []bool{true, false} {
		id, token := ready.SessionID, strings.Repeat("x", resumeTokenLength)
		if unknown {
			id, token = "unknown-session", ready.ResumeToken
		}
		candidate := f.dial(t, "/v2/asr")
		v2WriteJSON(t, f.ctx, candidate, wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: id, ResumeToken: token, AppliedSeq: 0})
		var raw json.RawMessage
		v2ReadJSON(t, f.ctx, candidate, &raw)
		decodeV2PublicError(t, raw, "resume_unavailable", "resume unavailable")
		f.waitReturn(t)
		if first == nil {
			first = raw
		} else if string(first) != string(raw) {
			t.Fatal("lookup and authentication failure disclosed different responses")
		}
	}
	// 两次认证拒绝后，原连接仍能上传并正常完成；不会选择第二个 Worker。
	if err := original.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
		t.Fatal(err)
	}
	v2ReadResult(t, f.ctx, original, 1, "ab")
	v2WriteJSON(t, f.ctx, original, wsprotocol.V2EndMessage{Type: wsprotocol.MessageTypeEnd, FinalOffset: "2"})
	v2ReadResult(t, f.ctx, original, 2, "complete")
	readCompletionNetwork(t, f.ctx, original, 1, 2, 2)
	v2WriteJSON(t, f.ctx, original, map[string]string{"type": "completed_ack", "finalOffset": "2", "lastSeq": "2"})
	v2RequireIdle(t, f)
	if f.pool.picks.Load() != 1 || backend.calls.Load() != 1 {
		t.Fatal("authentication failure changed original Worker")
	}
}

func TestV2EntryErrorResumePositionPreservesOriginal(t *testing.T) {
	for _, gap := range []bool{true, false} {
		name := "replay_gap"
		if !gap {
			name = "invalid_resume_position"
		}
		t.Run(name, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{MaxSessions: 1, V2: &V2Config{ResumeWindow: 2 * time.Second}})
			original := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, original, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
			var ready wsprotocol.ReadyMessage
			v2ReadJSON(t, f.ctx, original, &ready)
			f.waitReturn(t)
			if err := original.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
				t.Fatal(err)
			}
			v2ReadResult(t, f.ctx, original, 1, "ab")
			v2WriteJSON(t, f.ctx, original, wsprotocol.ResultAckMessage{Type: wsprotocol.MessageTypeResultAck, Seq: "1"})
			s, ok := f.g.registry.lookup(ready.SessionID)
			if !ok {
				t.Fatal("original session missing")
			}
			// 等待 reader 实际提交 ACK，避免单凭客户端 Write 返回假设结果已释放。
			for {
				snapshot, err := s.requestConnectionReady(f.ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.ackedResultSeq == 1 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			_ = original.CloseNow()
			applied, code, message := uint64(0), wsprotocol.V2ErrorReplayGap, "result replay gap"
			if !gap {
				applied, code, message = 2, wsprotocol.V2ErrorInvalidResumePosition, "invalid resume position"
			}
			for {
				candidate := f.dial(t, "/v2/asr")
				v2WriteJSON(t, f.ctx, candidate, wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: ready.SessionID, ResumeToken: ready.ResumeToken, AppliedSeq: applied})
				var raw json.RawMessage
				v2ReadJSON(t, f.ctx, candidate, &raw)
				f.waitReturn(t)
				var msg wsprotocol.V2ErrorMessage
				if err := json.Unmarshal(raw, &msg); err != nil {
					t.Fatal(err)
				}
				if msg.Code == wsprotocol.V2ErrorSessionBusy {
					_ = candidate.CloseNow()
					time.Sleep(time.Millisecond)
					continue
				}
				decodeV2PublicError(t, raw, code, message)
				break
			}
			if f.g.Snapshot().ActiveSessions != 1 || f.pool.picks.Load() != 1 {
				t.Fatal("bad resume released original or selected another Worker")
			}
			// 改用已真实应用的位置，只能获得第二代；拒绝没有偷偷推进代次。
			resumed := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, resumed, wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: ready.SessionID, ResumeToken: ready.ResumeToken, AppliedSeq: 1})
			var next wsprotocol.ReadyMessage
			v2ReadJSON(t, f.ctx, resumed, &next)
			f.waitReturn(t)
			if next.Type != wsprotocol.MessageTypeReady || next.SessionID != ready.SessionID || next.ResumeToken != ready.ResumeToken || next.Generation != 2 || next.NextOffset != 2 || next.AckedResultSeq != 1 {
				t.Fatal("rejection corrupted resume state")
			}
			if err := resumed.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(2, "cd")); err != nil {
				t.Fatal(err)
			}
			v2ReadResult(t, f.ctx, resumed, 2, "cd")
			v2WriteJSON(t, f.ctx, resumed, wsprotocol.V2EndMessage{Type: wsprotocol.MessageTypeEnd, FinalOffset: "4"})
			v2ReadResult(t, f.ctx, resumed, 3, "complete")
			readCompletionNetwork(t, f.ctx, resumed, 2, 4, 3)
			v2WriteJSON(t, f.ctx, resumed, map[string]string{"type": "completed_ack", "finalOffset": "4", "lastSeq": "3"})
			v2RequireIdle(t, f)
			backend.mu.Lock()
			audio := strings.Join(backend.audio, ",")
			backend.mu.Unlock()
			if f.pool.picks.Load() != 1 || backend.calls.Load() != 1 || audio != "ab,cd" {
				t.Fatal("rejected resume changed Worker or audio", audio)
			}
		})
	}
}

func TestV2EntryErrorWorkerFailureNetwork(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "opaque_backend_failure"
		if deadline {
			name = "downstream_deadline"
		}
		t.Run(name, func(t *testing.T) {
			cause := errors.New("fake-token fake-backend-details")
			if deadline {
				cause = context.DeadlineExceeded
			}
			opened := make(chan context.Context, 1)
			client := v2OpenFunc(func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				opened <- ctx
				return nil, cause
			})
			f := newV2EntryFixture(t, client, Config{V2: &V2Config{}})
			c := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
			var raw json.RawMessage
			v2ReadJSON(t, f.ctx, c, &raw)
			decodeV2PublicError(t, raw, "worker_unavailable", "worker unavailable")
			f.waitReturn(t)
			v2RequireIdle(t, f)
			if context.Cause(<-opened) == nil || f.pool.picks.Load() != 1 {
				t.Fatal("failed startup RPC not released")
			}
		})
	}
}

func TestV2EntryErrorPeerClosesBeforeFailureReply(t *testing.T) {
	client := &v2BlockingOpenClient{entered: make(chan context.Context, 1), release: make(chan struct{}), cause: errors.New("fake-backend-details")}
	var once sync.Once
	release := func() { once.Do(func() { close(client.release) }) }
	f := newV2EntryFixture(t, client, Config{V2: &V2Config{}})
	t.Cleanup(release)
	c := f.dial(t, "/v2/asr")
	v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
	var rpc context.Context
	select {
	case rpc = <-client.entered:
	case <-f.ctx.Done():
		t.Fatal("startup not entered")
	}
	_ = c.CloseNow()
	release()
	f.waitReturn(t)
	v2RequireIdle(t, f)
	if rpc.Err() == nil || f.pool.picks.Load() != 1 {
		t.Fatal("peer-close failure retained startup RPC")
	}
}
