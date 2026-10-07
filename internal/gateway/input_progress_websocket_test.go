package gateway

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// 使用公开入口、真实 WebSocket 和 TCP gRPC；短期限用于正确性验收，非性能测量。
func TestInputProgressPublicNetwork(t *testing.T) {
	for _, name := range []string{"silent_before_first_audio", "silent_expires", "silent_resumes", "unfinished_message_resumes"} {
		t.Run(name, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{
				MaxSessions: 1, MaxMessageBytes: 16384,
				V2: &V2Config{InputProgressTimeout: 250 * time.Millisecond, ResumeWindow: 2 * time.Second},
			})
			c := f.dial(t, "/v2/asr")
			v2WriteJSON(t, f.ctx, c, wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
			var ready wsprotocol.ReadyMessage
			v2ReadJSON(t, f.ctx, c, &ready)
			f.waitReturn(t)
			if ready.Type != wsprotocol.MessageTypeReady || ready.Generation != 1 {
				t.Fatal(ready)
			}
			if name != "silent_before_first_audio" {
				if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
					t.Fatal(err)
				}
				v2ReadResult(t, f.ctx, c, 1, "ab")
			}
			if name == "unfinished_message_resumes" {
				// Writer 的 Write 发出非 FIN 分片；数据大于库的 4 KiB 写缓冲，
				// 实际发送到 socket，但故意不 Close，不让整条消息读取完成。
				w, err := c.Writer(f.ctx, websocket.MessageBinary)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = w.Close() })
				if _, err := w.Write(readerAudioFrame(2, strings.Repeat("x", 8192))); err != nil {
					t.Fatal(err)
				}
			}
			// 等服务器停止旧连接；并不主动 CloseNow 来制造 detach。
			for {
				_, _, err := c.Read(f.ctx)
				if err != nil {
					if f.ctx.Err() != nil {
						t.Fatal("test deadline, rather than server idle protection, closed connection", err)
					}
					break
				}
			}
			s, ok := f.g.registry.lookup(ready.SessionID)
			if !ok || f.g.Snapshot().ActiveSessions != 1 || backend.calls.Load() != 1 || f.pool.picks.Load() != 1 {
				t.Fatal("idle detach lost registry, logical slot or original Worker")
			}
			if _, err := s.requestConnectionReady(f.ctx, 1); err != errSessionNotAttached {
				t.Fatal("old attachment still accepts commands", err)
			}
			if name == "silent_expires" || name == "silent_before_first_audio" {
				v2RequireIdle(t, f)
				return
			}
			// CloseNow/读写回收未全部完成时允许 busy，候选由本次入口负责清理。
			for {
				c = f.dial(t, "/v2/asr")
				v2WriteJSON(t, f.ctx, c, wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: ready.SessionID, ResumeToken: ready.ResumeToken, AppliedSeq: 0})
				var raw json.RawMessage
				v2ReadJSON(t, f.ctx, c, &raw)
				f.waitReturn(t)
				var tag wsprotocol.ErrorMessage
				if err := json.Unmarshal(raw, &tag); err != nil {
					t.Fatal(err)
				}
				if tag.Type == wsprotocol.MessageTypeReady {
					var resumed wsprotocol.ReadyMessage
					if err := json.Unmarshal(raw, &resumed); err != nil {
						t.Fatal(err)
					}
					if resumed.SessionID != ready.SessionID || resumed.ResumeToken != ready.ResumeToken || resumed.Generation != 2 || resumed.NextOffset != 2 || resumed.InputEnded {
						t.Fatal("resume lost identity or accepted unfinished audio", resumed)
					}
					ready = resumed
					break
				}
				if tag.Type != wsprotocol.MessageTypeError || tag.Message != "session busy" {
					t.Fatal(string(raw))
				}
				_ = c.CloseNow()
				time.Sleep(time.Millisecond)
			}
			v2ReadResult(t, f.ctx, c, 1, "ab")
			if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(0, "ab")); err != nil {
				t.Fatal(err)
			}
			if err := c.Write(f.ctx, websocket.MessageBinary, readerAudioFrame(2, "cd")); err != nil {
				t.Fatal(err)
			}
			v2ReadResult(t, f.ctx, c, 2, "cd")
			v2WriteJSON(t, f.ctx, c, wsprotocol.V2EndMessage{Type: wsprotocol.MessageTypeEnd, FinalOffset: "4"})
			v2ReadResult(t, f.ctx, c, 3, "complete")
			readCompletionNetwork(t, f.ctx, c, 2, 4, 3)
			v2WriteJSON(t, f.ctx, c, map[string]string{"type": "completed_ack", "finalOffset": "4", "lastSeq": "3"})
			v2RequireIdle(t, f)
			backend.mu.Lock()
			audio := strings.Join(backend.audio, ",")
			backend.mu.Unlock()
			if backend.calls.Load() != 1 || f.pool.picks.Load() != 1 || audio != "ab,cd" {
				t.Fatalf("calls=%d picks=%d audio=%s", backend.calls.Load(), f.pool.picks.Load(), audio)
			}
		})
	}
}
