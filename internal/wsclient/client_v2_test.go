package wsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// trackedV2Source 使读取/关闭所有权可观测；阻塞源以 pipe.Close 解除 Read。
type trackedV2Source struct {
	reader        io.Reader
	close         func() error
	reads, closes atomic.Int32
	closeErr      error
	entered       chan struct{}
}

func (s *trackedV2Source) Read(p []byte) (int, error) {
	s.reads.Add(1)
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	return s.reader.Read(p)
}
func (s *trackedV2Source) Close() error {
	s.closes.Add(1)
	if s.close != nil {
		return s.close()
	}
	return s.closeErr
}
func staticV2Source(data []byte) *trackedV2Source {
	return &trackedV2Source{reader: bytes.NewReader(data)}
}

func v2TestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// 所有服务器操作都有总期限；handler 的错误经通道返回，不在子协程 Fatal。
func v2TestServer(t *testing.T, handler func(context.Context, *websocket.Conn) error) (string, <-chan error) {
	t.Helper()
	done := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done <- handler(ctx, conn)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http"), done
}
func v2Send(ctx context.Context, conn *websocket.Conn, message any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}
func v2ReadInput(ctx context.Context, conn *websocket.Conn) (wsprotocol.V2Input, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return wsprotocol.V2Input{}, err
	}
	if typ == websocket.MessageBinary {
		return wsprotocol.DecodeV2Audio(data)
	}
	return wsprotocol.DecodeV2Control(data)
}
func v2SendReady(ctx context.Context, conn *websocket.Conn) error {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	handshake, err := wsprotocol.DecodeV2Handshake(data)
	if err != nil || typ != websocket.MessageText || handshake.Kind != wsprotocol.V2HandshakeStart {
		return errors.New("expected v2 start")
	}
	return v2Send(ctx, conn, wsprotocol.ReadyMessage{Type: wsprotocol.MessageTypeReady, SessionID: "session", ResumeToken: "secret", Generation: 1})
}
func v2TestClient(t *testing.T, cfg V2Config) *V2Client {
	t.Helper()
	c, err := NewV2(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func v2CheckClosed(t *testing.T, s *trackedV2Source) {
	t.Helper()
	if s.closes.Load() != 1 {
		t.Fatalf("source Close count=%d", s.closes.Load())
	}
}
func v2WaitServer(t *testing.T, done <-chan error, requireSuccess bool) {
	t.Helper()
	select {
	case err := <-done:
		if requireSuccess && err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestNewV2Config(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://example/v2/asr"})
		if c.cfg.ChunkBytes != 3200 || c.cfg.ReadLimit != 65536 || c.cfg.MaxReplayBytes != 320000 || c.cfg.MaxReplayChunks != 128 || c.cfg.HandshakeTimeout != 10*time.Second || c.cfg.WriteTimeout != 5*time.Second || c.cfg.ApplyTimeout != 5*time.Second || c.cfg.CompletionTimeout != 30*time.Second {
			t.Fatalf("defaults: %+v", c.cfg)
		}
	})
	cases := []struct {
		name   string
		mutate func(*V2Config)
	}{
		{"url", func(c *V2Config) { c.URL = "" }},
		{"negative_chunk", func(c *V2Config) { c.ChunkBytes = -2 }},
		{"odd_chunk", func(c *V2Config) { c.ChunkBytes = 3 }},
		{"header_overflow", func(c *V2Config) { c.ChunkBytes = int(^uint(0)>>1) - 1; c.MaxReplayBytes = ^uint64(0) }},
		{"chunk_over_budget", func(c *V2Config) { c.MaxReplayBytes = 2 }},
		{"negative_read_limit", func(c *V2Config) { c.ReadLimit = -1 }},
		{"negative_count", func(c *V2Config) { c.MaxReplayChunks = -1 }},
		{"negative_handshake", func(c *V2Config) { c.HandshakeTimeout = -1 }},
		{"negative_write", func(c *V2Config) { c.WriteTimeout = -1 }},
		{"negative_apply", func(c *V2Config) { c.ApplyTimeout = -1 }},
		{"negative_completion", func(c *V2Config) { c.CompletionTimeout = -1 }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := V2Config{URL: "ws://example"}
			tt.mutate(&cfg)
			if _, err := NewV2(cfg); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
}

func TestDecodeV2Message(t *testing.T) {
	t.Run("exact_uint64", func(t *testing.T) {
		message, err := decodeV2Message([]byte(`{"type":"result","seq":"18446744073709551615","segmentId":"","text":"","isFinal":false}`))
		if err != nil || message.result.Seq != ^uint64(0) {
			t.Fatalf("message=%+v err=%v", message, err)
		}
	})
	t.Run("unknown_error_code", func(t *testing.T) {
		message, err := decodeV2Message([]byte(`{"type":"error","code":"future_code","message":"secret"}`))
		if err != nil || message.entryError.Code != "future_code" {
			t.Fatalf("message=%+v err=%v", message, err)
		}
		if strings.Contains((&V2EntryError{Code: message.entryError.Code}).Error(), "secret") {
			t.Fatal("error leaked prompt")
		}
	})
	for name, data := range map[string]string{
		"null": "null", "array": "[]", "missing_type": "{}", "unknown_type": `{"type":"future"}`,
		"bad_json":   `{"type":"ready","resumeToken":"secret"`,
		"number_seq": `{"type":"result","seq":1}`, "overflow": `{"type":"result","seq":"18446744073709551616"}`,
		"credential_in_numeric_error": `{"type":"ready","generation":"secret"}`,
		"empty_code":                  `{"type":"error","code":""}`, "trailing": `{"type":"result"} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			msg, err := decodeV2Message([]byte(data))
			if !errors.Is(err, errV2ClientProtocol) || msg.typ != "" || strings.Contains(err.Error(), "secret") {
				t.Fatalf("message=%+v err=%v", msg, err)
			}
		})
	}
}

func TestV2RunNormalNetwork(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("aa"), []byte("aabbccddxx")} {
		t.Run(fmt.Sprintf("bytes_%d", len(data)), func(t *testing.T) {
			var applied []uint64
			var timestamps []time.Time
			var received []byte
			url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if err := v2SendReady(ctx, conn); err != nil {
					return err
				}
				var seq uint64
				for {
					input, err := v2ReadInput(ctx, conn)
					if err != nil {
						return err
					}
					switch input.Kind {
					case wsprotocol.V2InputAudio:
						if input.Offset != uint64(len(received)) {
							return errors.New("audio offset gap")
						}
						received = append(received, input.Payload...)
						if err := v2Send(ctx, conn, wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: uint64(len(received))}); err != nil {
							return err
						}
						seq++
						message := wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq, SegmentID: "same_segment", Text: string(input.Payload)}
						if err := v2Send(ctx, conn, message); err != nil {
							return err
						}
						// 重复序号不再次应用，同片段的新序号仍正常应用。
						if err := v2Send(ctx, conn, message); err != nil {
							return err
						}
					case wsprotocol.V2InputResultAck:
						if input.Seq > seq {
							return errors.New("result ack ahead")
						}
					case wsprotocol.V2InputEnd:
						if input.Offset != uint64(len(received)) {
							return errors.New("wrong end")
						}
						seq++
						if err := v2Send(ctx, conn, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq, SegmentID: "tail", Text: "tail", IsFinal: true}); err != nil {
							return err
						}
						if err := v2Send(ctx, conn, wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1, FinalOffset: input.Offset, LastSeq: seq}); err != nil {
							return err
						}
						for {
							ack, err := v2ReadInput(ctx, conn)
							if err != nil {
								return err
							}
							if ack.Kind == wsprotocol.V2InputCompletedAck {
								if ack.Offset != input.Offset || ack.Seq != seq {
									return errors.New("wrong completed ack")
								}
								return nil
							}
							if ack.Kind != wsprotocol.V2InputResultAck {
								return errors.New("unexpected after end")
							}
						}
					default:
						return errors.New("unexpected input")
					}
				}
			})
			source := staticV2Source(data)
			c := v2TestClient(t, V2Config{URL: url, ChunkBytes: 4, OnResult: func(ctx context.Context, r wsprotocol.SequencedResultMessage, at time.Time) error {
				applied = append(applied, r.Seq)
				timestamps = append(timestamps, at)
				return nil
			}})
			result, err := c.Run(v2TestContext(t), source)
			if err != nil || !result.Completed || !result.CompletionAckWritten || result.FinalOffset != uint64(len(data)) || result.AppliedSeq != uint64((len(data)+3)/4+1) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			v2WaitServer(t, done, true)
			v2CheckClosed(t, source)
			if !bytes.Equal(received, data) || len(applied) != int(result.AppliedSeq) {
				t.Fatalf("audio=%q applied=%v", received, applied)
			}
			for i, seq := range applied {
				if seq != uint64(i+1) || timestamps[i].IsZero() {
					t.Fatal("invalid application order/time")
				}
			}
		})
	}
}

func TestV2RunSourceOwnership(t *testing.T) {
	t.Run("pre_canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		source := staticV2Source(nil)
		c := v2TestClient(t, V2Config{URL: "ws://invalid"})
		_, err := c.Run(ctx, source)
		if !errors.Is(err, context.Canceled) || source.reads.Load() != 0 {
			t.Fatalf("err=%v reads=%d", err, source.reads.Load())
		}
		v2CheckClosed(t, source)
	})
	t.Run("dial_failure", func(t *testing.T) {
		source := staticV2Source(nil)
		c := v2TestClient(t, V2Config{URL: "bad-url"})
		_, err := c.Run(v2TestContext(t), source)
		if err == nil || source.reads.Load() != 0 {
			t.Fatal("expected dial failure without reading")
		}
		v2CheckClosed(t, source)
	})
	t.Run("nil_source", func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://invalid"})
		if _, err := c.Run(context.Background(), nil); err == nil {
			t.Fatal("accepted nil source")
		}
	})
	t.Run("cancel_unblocks_read", func(t *testing.T) {
		url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
			if err := v2SendReady(ctx, conn); err != nil {
				return err
			}
			_, _, err := conn.Read(ctx)
			return err
		})
		pr, pw := io.Pipe()
		defer pw.Close()
		source := &trackedV2Source{reader: pr, close: pr.Close, entered: make(chan struct{}, 1)}
		ctx, cancel := context.WithCancel(v2TestContext(t))
		defer cancel()
		c := v2TestClient(t, V2Config{URL: url, ChunkBytes: 4})
		returned := make(chan error, 1)
		go func() { _, err := c.Run(ctx, source); returned <- err }()
		select {
		case <-source.entered:
		case <-ctx.Done():
			t.Fatal("source read not entered")
		}
		cancel()
		select {
		case err := <-returned:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Run did not join blocked reader")
		}
		v2CheckClosed(t, source)
		v2WaitServer(t, done, false)
	})
}

func TestV2RunEntryErrors(t *testing.T) {
	for _, transport := range []string{"http", "websocket"} {
		t.Run(transport, func(t *testing.T) {
			code := wsprotocol.V2ErrorCode("future_code")
			var url string
			var done <-chan error
			if transport == "http" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(503)
					_ = json.NewEncoder(w).Encode(wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: code, Message: "secret"})
				}))
				t.Cleanup(server.Close)
				url = "ws" + strings.TrimPrefix(server.URL, "http")
			} else {
				url, done = v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
					if _, _, err := conn.Read(ctx); err != nil {
						return err
					}
					return v2Send(ctx, conn, wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: code, Message: "secret"})
				})
			}
			source := staticV2Source([]byte("aa"))
			_, err := v2TestClient(t, V2Config{URL: url, ChunkBytes: 2}).Run(v2TestContext(t), source)
			var entry *V2EntryError
			if !errors.As(err, &entry) || entry.Code != code || strings.Contains(err.Error(), "secret") || source.reads.Load() != 0 {
				t.Fatalf("err=%v reads=%d", err, source.reads.Load())
			}
			v2CheckClosed(t, source)
			if done != nil {
				v2WaitServer(t, done, true)
			}
		})
	}
	for name, body := range map[string]string{"invalid": "not JSON", "truncated": `{"type":"error","message":"` + strings.Repeat("x", 1200) + `","code":"session_limit"}`} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503); _, _ = io.WriteString(w, body) }))
			t.Cleanup(server.Close)
			source := staticV2Source(nil)
			_, err := v2TestClient(t, V2Config{URL: "ws" + strings.TrimPrefix(server.URL, "http")}).Run(v2TestContext(t), source)
			var entry *V2EntryError
			if err == nil || errors.As(err, &entry) {
				t.Fatalf("unexpected entry classification: %v", err)
			}
			v2CheckClosed(t, source)
		})
	}
}

func TestV2RunRejectsProtocol(t *testing.T) {
	for _, phase := range []string{"handshake", "stream"} {
		t.Run(phase, func(t *testing.T) {
			cases := map[string]func() (websocket.MessageType, []byte){
				"binary": func() (websocket.MessageType, []byte) { return websocket.MessageBinary, []byte("aa") },
				"invalid_json": func() (websocket.MessageType, []byte) {
					return websocket.MessageText, []byte(`{"type":"ready","generation":"secret"}`)
				},
				"unknown": func() (websocket.MessageType, []byte) { return websocket.MessageText, []byte(`{"type":"future"}`) },
				"oversized": func() (websocket.MessageType, []byte) {
					return websocket.MessageText, []byte(strings.Repeat("x", 1000))
				},
			}
			if phase == "handshake" {
				cases["result_before_ready"] = func() (websocket.MessageType, []byte) {
					return websocket.MessageText, []byte(`{"type":"result","seq":"1"}`)
				}
			} else {
				cases["repeated_ready"] = func() (websocket.MessageType, []byte) {
					b, _ := json.Marshal(wsprotocol.ReadyMessage{Type: wsprotocol.MessageTypeReady, SessionID: "s", ResumeToken: "t", Generation: 1})
					return websocket.MessageText, b
				}
				cases["wrong_generation"] = func() (websocket.MessageType, []byte) {
					b, _ := json.Marshal(wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: 2})
					return websocket.MessageText, b
				}
				cases["ack_ahead"] = func() (websocket.MessageType, []byte) {
					b, _ := json.Marshal(wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 100})
					return websocket.MessageText, b
				}
				cases["result_gap"] = func() (websocket.MessageType, []byte) {
					return websocket.MessageText, []byte(`{"type":"result","seq":"2"}`)
				}
				cases["late_entry_error"] = func() (websocket.MessageType, []byte) {
					return websocket.MessageText, []byte(`{"type":"error","code":"session_limit"}`)
				}
			}
			for name, frame := range cases {
				t.Run(name, func(t *testing.T) {
					url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
						if phase == "stream" {
							if err := v2SendReady(ctx, conn); err != nil {
								return err
							}
						} else {
							if _, _, err := conn.Read(ctx); err != nil {
								return err
							}
						}
						typ, data := frame()
						if err := conn.Write(ctx, typ, data); err != nil {
							return err
						}
						_, _, err := conn.Read(ctx)
						return err
					})
					pr, pw := io.Pipe()
					defer pw.Close()
					source := &trackedV2Source{reader: pr, close: pr.Close}
					result, err := v2TestClient(t, V2Config{URL: url, ReadLimit: 512}).Run(v2TestContext(t), source)
					if err == nil || result.Completed || strings.Contains(err.Error(), "secret") {
						t.Fatalf("result=%+v err=%v", result, err)
					}
					if phase == "handshake" && source.reads.Load() != 0 {
						t.Fatal("read source before ready")
					}
					v2CheckClosed(t, source)
					v2WaitServer(t, done, false)
				})
			}
		})
	}
}

func TestV2RunBeforeReadyDoesNotRead(t *testing.T) {
	start := make(chan struct{})
	release := make(chan struct{})
	url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, _, err := conn.Read(ctx); err != nil {
			return err
		}
		close(start)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return v2Send(ctx, conn, wsprotocol.V2ErrorMessage{Type: wsprotocol.MessageTypeError, Code: wsprotocol.V2ErrorSessionLimit})
	})
	source := staticV2Source([]byte("aa"))
	ctx := v2TestContext(t)
	returned := make(chan error, 1)
	go func() { _, err := v2TestClient(t, V2Config{URL: url}).Run(ctx, source); returned <- err }()
	select {
	case <-start:
	case <-ctx.Done():
		t.Fatal("missing start")
	}
	if source.reads.Load() != 0 {
		t.Fatal("read source before ready")
	}
	close(release)
	select {
	case err := <-returned:
		var entry *V2EntryError
		if !errors.As(err, &entry) {
			t.Fatalf("err=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("Run did not exit")
	}
	v2CheckClosed(t, source)
	v2WaitServer(t, done, true)
}

func TestV2RunApplicationFailure(t *testing.T) {
	failure := errors.New("application failed")
	for _, mode := range []string{"error", "timeout", "expired_nil", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if err := v2SendReady(ctx, conn); err != nil {
					return err
				}
				if err := v2Send(ctx, conn, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1}); err != nil {
					return err
				}
				input, err := v2ReadInput(ctx, conn)
				if err == nil && input.Kind == wsprotocol.V2InputResultAck {
					return errors.New("failed application was acknowledged")
				}
				return nil
			})
			pr, pw := io.Pipe()
			defer pw.Close()
			source := &trackedV2Source{reader: pr, close: pr.Close}
			ctx, cancel := context.WithCancel(v2TestContext(t))
			defer cancel()
			c := v2TestClient(t, V2Config{URL: url, ApplyTimeout: 30 * time.Millisecond, OnResult: func(appCtx context.Context, _ wsprotocol.SequencedResultMessage, _ time.Time) error {
				if mode == "error" {
					return failure
				}
				if mode == "cancel" {
					cancel()
				}
				<-appCtx.Done()
				if mode == "expired_nil" {
					return nil
				}
				return appCtx.Err()
			}})
			result, err := c.Run(ctx, source)
			want := error(context.DeadlineExceeded)
			if mode == "error" {
				want = failure
			}
			if mode == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || result.Completed || result.AppliedSeq != 0 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			v2CheckClosed(t, source)
			v2WaitServer(t, done, true)
		})
	}
}

// 源返回非 EOF 错误，即使有部分数据，也不能封口成正常 end。
type failingV2Reader struct{}

func (failingV2Reader) Read(p []byte) (int, error) { copy(p, "aa"); return 2, io.ErrClosedPipe }
func TestV2RunInvalidSource(t *testing.T) {
	for _, mode := range []string{"odd_tail", "read_error"} {
		t.Run(mode, func(t *testing.T) {
			url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if err := v2SendReady(ctx, conn); err != nil {
					return err
				}
				for {
					input, err := v2ReadInput(ctx, conn)
					if err != nil {
						return nil
					}
					if input.Kind == wsprotocol.V2InputEnd {
						return errors.New("invalid source sent end")
					}
				}
			})
			source := staticV2Source([]byte("a"))
			if mode == "read_error" {
				source.reader = failingV2Reader{}
			}
			result, err := v2TestClient(t, V2Config{URL: url, ChunkBytes: 4}).Run(v2TestContext(t), source)
			if err == nil || result.Completed {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if mode == "read_error" && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("err=%v", err)
			}
			v2CheckClosed(t, source)
			v2WaitServer(t, done, true)
		})
	}
}

func TestV2RunIncompleteAndCleanupError(t *testing.T) {
	for _, mode := range []string{"normal_close", "audio_ack_only", "source_close_error"} {
		t.Run(mode, func(t *testing.T) {
			url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if err := v2SendReady(ctx, conn); err != nil {
					return err
				}
				for {
					input, err := v2ReadInput(ctx, conn)
					if err != nil {
						return err
					}
					if input.Kind != wsprotocol.V2InputEnd {
						continue
					}
					if mode == "audio_ack_only" {
						if err := v2Send(ctx, conn, wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, InputEnded: true}); err != nil {
							return err
						}
						_, _, err := conn.Read(ctx)
						return err
					}
					if mode == "normal_close" {
						return conn.Close(websocket.StatusNormalClosure, "done")
					}
					if err := v2Send(ctx, conn, wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1}); err != nil {
						return err
					}
					for {
						ack, err := v2ReadInput(ctx, conn)
						if err != nil {
							return err
						}
						if ack.Kind == wsprotocol.V2InputCompletedAck {
							return nil
						}
					}
				}
			})
			closeErr := errors.New("source close failure")
			source := staticV2Source(nil)
			if mode == "source_close_error" {
				source.closeErr = closeErr
			}
			result, err := v2TestClient(t, V2Config{URL: url, CompletionTimeout: 40 * time.Millisecond}).Run(v2TestContext(t), source)
			switch mode {
			case "normal_close":
				if !errors.Is(err, ErrV2Incomplete) || result.Completed {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			case "audio_ack_only":
				if !errors.Is(err, context.DeadlineExceeded) || result.Completed {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			case "source_close_error":
				if !errors.Is(err, closeErr) || !result.Completed || !result.CompletionAckWritten {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
			v2CheckClosed(t, source)
			v2WaitServer(t, done, mode == "source_close_error" || mode == "normal_close")
		})
	}
}

func TestV2RunSharedHandshakeDeadline(t *testing.T) {
	source := staticV2Source(nil)
	budget := 400 * time.Millisecond
	startReceived := make(chan time.Time, 1)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		// 消耗 Dial 的一部分预算；后面的 ready 等待不能重新获得完整预算。
		select {
		case <-time.After(250 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		startReceived <- time.Now()
		// 不发送 ready，读取对端退出，让服务器也实际收尾。
		_, _, _ = conn.Read(ctx)
	}))
	t.Cleanup(server.Close)
	c := v2TestClient(t, V2Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), HandshakeTimeout: budget})
	_, err := c.Run(v2TestContext(t), source)
	if !errors.Is(err, context.DeadlineExceeded) || source.reads.Load() != 0 {
		t.Fatalf("err=%v reads=%d", err, source.reads.Load())
	}
	select {
	case at := <-startReceived:
		if time.Since(at) >= 350*time.Millisecond {
			t.Fatalf("ready received a fresh handshake budget: %v", time.Since(at))
		}
	default:
		t.Fatal("start did not reach server within handshake budget")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not exit")
	}
	v2CheckClosed(t, source)
}

func TestV2RunCompletedThenTransportLoss(t *testing.T) {
	url, done := v2TestServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if err := v2SendReady(ctx, conn); err != nil {
			return err
		}
		input, err := v2ReadInput(ctx, conn)
		if err != nil {
			return err
		}
		if input.Kind != wsprotocol.V2InputEnd {
			return errors.New("expected empty input end")
		}
		if err := v2Send(ctx, conn, wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1}); err != nil {
			return err
		}
		return conn.CloseNow()
	})
	source := staticV2Source(nil)
	result, err := v2TestClient(t, V2Config{URL: url}).Run(v2TestContext(t), source)
	// TCP 关闭与本地写入竞争：ACK 可能写成功，也可能失败；两者都不能撤销完成。
	if !result.Completed || result.AppliedSeq != 0 || result.FinalOffset != 0 || err == nil && !result.CompletionAckWritten {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	v2CheckClosed(t, source)
	v2WaitServer(t, done, false)
}
