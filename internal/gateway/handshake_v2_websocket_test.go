package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

func TestV2HandshakeWebSocketLeavesNextMessage(t *testing.T) {
	for _, name := range []string{"start", "resume"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
			wire := validV2Start
			want := wsprotocol.V2Handshake{Kind: wsprotocol.V2HandshakeStart}
			if name == "resume" {
				wire = `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"token","appliedSeq":"12"}`
				want = wsprotocol.V2Handshake{Kind: wsprotocol.V2HandshakeResume, SessionID: "id", ResumeToken: "token", AppliedSeq: 12}
			}
			if err := client.Write(ctx, websocket.MessageText, []byte(wire)); err != nil {
				t.Fatal(err)
			}
			next := readerAudioFrame(0, "ab")
			if err := client.Write(ctx, websocket.MessageBinary, next); err != nil {
				t.Fatal(err)
			}
			got, err := readV2Handshake(ctx, legacy.ws, time.Second, 512)
			if err != nil || got != want {
				t.Fatal("unexpected network handshake (credentials redacted)", err)
			}
			// child context 已取消，但原连接仍可由下一阶段读取；首条以后的消息未被消费。
			typ, data, err := legacy.ws.Read(ctx)
			if err != nil || typ != websocket.MessageBinary || !bytes.Equal(data, next) {
				t.Fatal("next input consumed or connection closed", err)
			}
		})
	}
}

func TestV2HandshakeWebSocketRejectsInvalidFirstMessage(t *testing.T) {
	for _, name := range []string{"binary", "malformed", "oversized", "closed_peer"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
			limit := int64(512)
			typ, data := websocket.MessageText, []byte(`{"type":"end"}`)
			want := error(wsprotocol.ErrInvalidV2Handshake)
			switch name {
			case "binary":
				typ, data = websocket.MessageBinary, []byte(validV2Start)
			case "oversized":
				limit = 16
				data = []byte(validV2Start)
				want = websocket.ErrMessageTooBig
			case "closed_peer":
				if err := client.CloseNow(); err != nil {
					t.Fatal(err)
				}
			}
			if name != "closed_peer" {
				if err := client.Write(ctx, typ, data); err != nil {
					t.Fatal(err)
				}
			}
			got, err := readV2Handshake(ctx, legacy.ws, time.Second, limit)
			if name == "closed_peer" {
				if got != (wsprotocol.V2Handshake{}) || err == nil || errors.Is(err, ErrHandshakeTimeout) || errors.Is(err, wsprotocol.ErrInvalidV2Handshake) {
					t.Fatal("peer close classified incorrectly", err)
				}
			} else {
				assertHandshakeReadFailure(t, got, err, want)
			}
		})
	}
}

func TestV2HandshakeWebSocketSilentPeerTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	legacy, _, _ := newLegacyResultWriteFixture(t, time.Second, false)
	got, err := readV2Handshake(ctx, legacy.ws, 100*time.Millisecond, 512)
	assertHandshakeReadFailure(t, got, err, ErrHandshakeTimeout)
}
