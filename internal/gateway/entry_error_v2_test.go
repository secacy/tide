package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

func TestV2EntryErrorCodes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		code    wsprotocol.V2ErrorCode
		message string
	}{
		{"stop", errGatewayStopping, "service_stopping", "service is stopping"},
		{"handshake_limit", errHandshakeLimit, "handshake_limit", "handshake limit exceeded"},
		{"session_limit", errSessionLimit, "session_limit", "session limit exceeded"},
		{"invalid_handshake", wsprotocol.ErrInvalidV2Handshake, "invalid_handshake", "invalid handshake"},
		{"message_limit", websocket.ErrMessageTooBig, "invalid_handshake", "invalid handshake"},
		{"handshake_timeout", ErrHandshakeTimeout, "entry_timeout", "entry timeout"},
		{"entry_timeout", context.DeadlineExceeded, "entry_timeout", "entry timeout"},
		{"unknown_identity", errResumeUnavailable, "resume_unavailable", "resume unavailable"},
		{"closed", errResumeClosed, "resume_unavailable", "resume unavailable"},
		{"expired", errResumeExpired, "resume_unavailable", "resume unavailable"},
		{"generation_exhausted", errResumeGenerationExhausted, "resume_unavailable", "resume unavailable"},
		{"attached", errResumeAlreadyAttached, "session_busy", "session busy"},
		{"retiring", errConnectionRetiring, "session_busy", "session busy"},
		{"replay_gap", errResultReplayGap, "replay_gap", "result replay gap"},
		{"ahead", errResultAckAhead, "invalid_resume_position", "invalid resume position"},
		{"worker", errV2WorkerUnavailable, "worker_unavailable", "worker unavailable"},
		{"unknown", errors.New("fake-token fake-backend-details fake-audio"), "internal_error", "internal error"},
		{"canceled_without_category", context.Canceled, "internal_error", "internal error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				name := "direct"
				if wrapped {
					name = "wrapped"
				}
				t.Run(name, func(t *testing.T) {
					cause := tc.err
					if wrapped {
						cause = fmt.Errorf("fake-token fake-backend-details: %w", cause)
					}
					got := v2EntryError(cause)
					want := wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: tc.code, Message: tc.message}
					if got != want {
						t.Fatalf("public error = %+v, want %+v", got, want)
					}
				})
			}
		})
	}
}

func TestV2EntryErrorCategoryBeforeDeadline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		code  wsprotocol.V2ErrorCode
	}{
		{"worker", errV2WorkerUnavailable, "worker_unavailable"},
		{"stopping", errGatewayStopping, "service_stopping"},
		{"busy", errConnectionRetiring, "session_busy"},
		{"invalid", wsprotocol.ErrInvalidV2Handshake, "invalid_handshake"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := v2EntryError(fmt.Errorf("fake details: %w: %w", tc.cause, context.DeadlineExceeded))
			if got.Code != tc.code {
				t.Fatal("generic deadline hid entry category", got)
			}
		})
	}
}

// decodeV2PublicError 校验独立 HTTP/WS 报文，不借助生产映射生成期望值。
func decodeV2PublicError(t *testing.T, body []byte, code wsprotocol.V2ErrorCode, message string) wsprotocol.V2ErrorMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || len(fields) != 3 {
		t.Fatal("invalid public error schema", err, string(body))
	}
	var got wsprotocol.V2ErrorMessage
	if err := json.Unmarshal(body, &got); err != nil || got.Type != wsprotocol.MessageTypeError || got.Code != code || got.Message != message {
		t.Fatal("unexpected public error", got, err)
	}
	return got
}

func TestV2EntryHTTPErrorSchema(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cause   error
		code    wsprotocol.V2ErrorCode
		message string
	}{
		{"limit", errHandshakeLimit, "handshake_limit", "handshake limit exceeded"},
		{"stop", errGatewayStopping, "service_stopping", "service is stopping"},
		{"timeout", context.DeadlineExceeded, "entry_timeout", "entry timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeV2EntryHTTPError(w, tc.cause)
			if w.Code != http.StatusServiceUnavailable || w.Header().Get("Content-Type") != "application/json; charset=utf-8" {
				t.Fatal(w.Code, w.Header())
			}
			decodeV2PublicError(t, w.Body.Bytes(), tc.code, tc.message)
		})
	}
}

func TestV2EntryHTTPHandlerCancellationAndAdmission(t *testing.T) {
	for _, name := range []string{"handshake_full", "gate_stopped", "gateway_canceled", "request_canceled"} {
		t.Run(name, func(t *testing.T) {
			life, cancelLife := context.WithCancel(context.Background())
			defer cancelLife()
			pool := &v2EntrySelector{client: &recordingWorker{}}
			g, err := New(life, pool, Config{V2: &V2Config{MaxHandshakes: 1}})
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodGet, "/v2/asr", nil)
			code, message := wsprotocol.V2ErrorServiceStopping, "service is stopping"
			switch name {
			case "handshake_full":
				if err := g.gate.tryEnterHandshake(); err != nil {
					t.Fatal(err)
				}
				defer g.gate.leaveHandshake()
				code, message = wsprotocol.V2ErrorHandshakeLimit, "handshake limit exceeded"
			case "gate_stopped":
				g.StopAccepting()
			case "gateway_canceled":
				cancelLife()
			case "request_canceled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r = r.WithContext(ctx)
			}
			w := httptest.NewRecorder()
			g.ServeV2HTTP(w, r)
			if name == "request_canceled" {
				if w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" {
					t.Fatal("request cancellation produced a service refusal", w.Body.String())
				}
			} else {
				if w.Code != 503 || w.Header().Get("Content-Type") != "application/json; charset=utf-8" {
					t.Fatal(w.Code, w.Header())
				}
				decodeV2PublicError(t, w.Body.Bytes(), code, message)
			}
			wantHandshakes := 0
			if name == "handshake_full" {
				wantHandshakes = 1
			}
			if entryHandshakeCount(g.gate) != wantHandshakes || g.Snapshot().ActiveSessions != 0 || pool.picks.Load() != 0 {
				t.Fatal("HTTP refusal leaked or entered Worker path")
			}
		})
	}
}

type failingEntryResponseWriter struct {
	header           http.Header
	statuses, writes int
}

func (w *failingEntryResponseWriter) Header() http.Header { return w.header }
func (w *failingEntryResponseWriter) WriteHeader(int)     { w.statuses++ }
func (w *failingEntryResponseWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, io.ErrClosedPipe
}

func TestV2EntryHTTPErrorWriteFailureDoesNotLeak(t *testing.T) {
	g, err := New(context.Background(), &v2EntrySelector{client: &recordingWorker{}}, Config{V2: &V2Config{}})
	if err != nil {
		t.Fatal(err)
	}
	g.StopAccepting()
	w := &failingEntryResponseWriter{header: make(http.Header)}
	g.ServeV2HTTP(w, httptest.NewRequest(http.MethodGet, "/v2/asr", nil))
	if w.statuses != 1 || w.writes != 1 || entryHandshakeCount(g.gate) != 0 || g.Snapshot().ActiveSessions != 0 {
		t.Fatal("failed response retried or leaked admission", w.statuses, w.writes)
	}
	if strings.Contains(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatal("wrong response type")
	}
}
