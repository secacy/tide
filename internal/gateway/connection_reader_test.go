package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// readerTestConn 将取消/消息注入到真正 reader 的 Read 边界，不替换命令处理。
type readerTestConn struct {
	read  func(context.Context) (websocket.MessageType, []byte, error)
	reads atomic.Int32
	limit atomic.Int64
}

func (c *readerTestConn) SetReadLimit(n int64) { c.limit.Store(n) }
func (c *readerTestConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	c.reads.Add(1)
	return c.read(ctx)
}

type readerTestStep struct {
	typ  websocket.MessageType
	data []byte
	err  error
}

func readerAudioFrame(offset uint64, payload string) []byte {
	data := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(data, offset)
	copy(data[8:], payload)
	return data
}

func scriptedReaderConn(steps ...readerTestStep) *readerTestConn {
	index := 0 // 仅 run 所在 goroutine 访问。
	return &readerTestConn{read: func(context.Context) (websocket.MessageType, []byte, error) {
		if index == len(steps) {
			return 0, nil, io.EOF
		}
		step := steps[index]
		index++
		return step.typ, step.data, step.err
	}}
}

func newTestConnectionReader(t *testing.T, f *workerCoordinatorFixture, generation uint64, conn connectionReadConn, limit int64) *connectionReader {
	t.Helper()
	r, err := newConnectionReader(connectionReaderConfig{session: f.session, generation: generation, conn: conn, controlCtx: f.lifeCtx, maxMessageBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func assertReaderExit(t *testing.T, got connectionReaderExit, kind connectionReaderExitKind, want error) {
	t.Helper()
	if got.kind != kind || got.err == nil || !errors.Is(got.err, want) {
		t.Fatalf("reader exit = %+v, want kind=%d err=%v", got, kind, want)
	}
}

func TestConnectionReaderConstruction(t *testing.T) {
	f, _ := newDuplexWorkerFixture(t, nil, nil)
	conn := scriptedReaderConn()
	cfg := connectionReaderConfig{session: f.session, generation: 1, conn: conn, controlCtx: f.lifeCtx, maxMessageBytes: 9}
	for _, name := range []string{"nil_session", "zero_generation", "nil_conn", "nil_control", "zero_limit", "negative_limit", "header_only_limit", "valid"} {
		t.Run(name, func(t *testing.T) {
			c := cfg
			switch name {
			case "nil_session":
				c.session = nil
			case "zero_generation":
				c.generation = 0
			case "nil_conn":
				c.conn = nil
			case "nil_control":
				c.controlCtx = nil
			case "zero_limit":
				c.maxMessageBytes = 0
			case "negative_limit":
				c.maxMessageBytes = -1
			case "header_only_limit":
				c.maxMessageBytes = 8
			}
			r, err := newConnectionReader(c)
			if name == "valid" {
				if err != nil || r == nil || r.config.conn != conn {
					t.Fatalf("construction = (%v,%v)", r, err)
				}
			} else if r != nil || !errors.Is(err, errInvalidConnectionReaderConfig) {
				t.Fatalf("invalid construction = (%v,%v)", r, err)
			}
			if conn.limit.Load() != 0 || conn.reads.Load() != 0 || context.Cause(f.rpcCtx) != nil {
				t.Fatal("constructor acquired I/O or cancellation ownership")
			}
		})
	}
	t.Run("nil_run_context", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("nil ctx did not panic")
			}
		}()
		r, _ := newConnectionReader(cfg)
		r.run(nil)
	})
}

func TestConnectionReaderReadAndProtocolExits(t *testing.T) {
	transportErr := errors.New("transport failed")
	cases := []struct {
		name string
		step readerTestStep
		kind connectionReaderExitKind
		err  error
	}{
		{"eof", readerTestStep{err: io.EOF}, readerReadFailed, io.EOF},
		{"transport", readerTestStep{err: fmt.Errorf("read: %w", transportErr)}, readerReadFailed, transportErr},
		{"normal_close", readerTestStep{err: websocket.CloseError{Code: websocket.StatusNormalClosure}}, readerReadFailed, nil},
		{"too_big", readerTestStep{err: fmt.Errorf("read: %w", websocket.ErrMessageTooBig)}, readerProtocolFailed, websocket.ErrMessageTooBig},
		{"bad_json", readerTestStep{typ: websocket.MessageText, data: []byte(`{"type":"end"}`)}, readerProtocolFailed, wsprotocol.ErrInvalidV2Input},
		{"empty_audio", readerTestStep{typ: websocket.MessageBinary, data: make([]byte, 8)}, readerProtocolFailed, wsprotocol.ErrInvalidV2Input},
		{"unknown_message_type", readerTestStep{typ: websocket.MessageType(77)}, readerProtocolFailed, wsprotocol.ErrInvalidV2Input},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := newDuplexWorkerFixture(t, nil, nil)
			conn := scriptedReaderConn(tc.step, readerTestStep{typ: websocket.MessageBinary, data: readerAudioFrame(0, "forbidden")})
			r := newTestConnectionReader(t, f, 1, conn, 64)
			ctx, cancel := context.WithCancel(f.lifeCtx)
			defer cancel()
			got := r.run(ctx)
			if tc.err != nil {
				assertReaderExit(t, got, tc.kind, tc.err)
			} else if got.kind != tc.kind || websocket.CloseStatus(got.err) != websocket.StatusNormalClosure {
				t.Fatal(got)
			}
			if conn.reads.Load() != 1 || conn.limit.Load() != 64 || context.Cause(f.rpcCtx) != nil {
				t.Fatal("reader consumed subsequent input or canceled Worker")
			}
		})
	}
}

func TestConnectionReaderStopPriority(t *testing.T) {
	for _, timing := range []string{"before_run", "after_read_error", "after_read_success"} {
		for _, source := range []string{"logical", "connection", "control", "all"} {
			t.Run(timing+"/"+source, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				ctx, cancel := context.WithCancelCause(f.lifeCtx)
				defer cancel(nil)
				logicalCause, connectionCause := errors.New("logical stop"), errors.New("connection stop")
				stop := func() {
					if source == "connection" || source == "all" {
						cancel(connectionCause)
					}
					if source == "logical" || source == "all" {
						f.cancelLife(logicalCause)
					}
					if source == "control" || source == "all" {
						close(f.session.controlDone)
					}
				}
				conn := &readerTestConn{read: func(context.Context) (websocket.MessageType, []byte, error) {
					stop()
					if timing == "after_read_error" {
						return 0, nil, websocket.ErrMessageTooBig
					}
					return websocket.MessageText, []byte(`{"type":"end","finalOffset":"0"}`), nil
				}}
				if timing == "before_run" {
					stop()
				}
				r := newTestConnectionReader(t, f, 1, conn, 64)
				want := logicalCause
				if source == "connection" {
					want = connectionCause
				}
				if source == "control" {
					want = errResumeClosed
				}
				assertReaderExit(t, r.run(ctx), readerStopped, want)
				wantReads := int32(1)
				if timing == "before_run" {
					wantReads = 0
				}
				if conn.reads.Load() != wantReads || timing == "before_run" && conn.limit.Load() != 0 {
					t.Fatal("performed I/O after stop")
				}
			})
		}
	}
}

func TestConnectionReaderEndAckAndIdempotency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, feed := newDuplexWorkerFixture(t, nil, nil)
		f.worker.results, _ = newResultBuffer(6, 1)
		f.start()
		feedDeliveryResult(t, feed, "first") // segmentID=s，加文本恰好 6 字节。
		takeDeliveryResult(t, f.session, 1, 1)
		writeDeliveryResult(t, f.session, 1, 1)
		steps := []readerTestStep{
			{typ: websocket.MessageText, data: []byte(`{"type":"end","finalOffset":"0"}`)},
			{typ: websocket.MessageText, data: []byte(`{"type":"result_ack","seq":"1"}`)},
			{typ: websocket.MessageText, data: []byte(`{"type":"end","finalOffset":"0"}`)},
			{typ: websocket.MessageText, data: []byte(`{"type":"result_ack","seq":"1"}`)},
			{typ: websocket.MessageText, data: []byte(`{"type":"result_ack","seq":"0"}`)},
		}
		conn := scriptedReaderConn(steps...)
		ctx, cancel := context.WithCancel(f.lifeCtx)
		defer cancel()
		assertReaderExit(t, newTestConnectionReader(t, f, 1, conn, 128).run(ctx), readerReadFailed, io.EOF)
		synctest.Wait()
		if conn.reads.Load() != 6 || !f.worker.input.input.ended || f.worker.results.ackedSeq != 1 || f.worker.results.count != 0 || f.worker.results.retainedBytes != 0 {
			t.Fatal("end stopped reads or ACK did not release both budgets")
		}
		feedDeliveryResult(t, feed, "tail!") // 单槽/字节预算已可复用。
		takeDeliveryResult(t, f.session, 1, 2)
		writeDeliveryResult(t, f.session, 1, 2)
		closeDeliveryFixture(t, f)
	})
}

func TestConnectionReaderOldGenerationCannotMutate(t *testing.T) {
	for _, kind := range []string{"audio", "end", "ack"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, feed := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				feedDeliveryResult(t, feed, "one")
				takeDeliveryResult(t, f.session, 1, 1)
				writeDeliveryResult(t, f.session, 1, 1)
				if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
					t.Fatal(d, err)
				}
				if g, err := f.session.requestResume(context.Background(), 0); g != 2 || err != nil {
					t.Fatal(g, err)
				}
				step := readerTestStep{typ: websocket.MessageBinary, data: readerAudioFrame(0, "ab")}
				if kind == "end" {
					step = readerTestStep{typ: websocket.MessageText, data: []byte(`{"type":"end","finalOffset":"0"}`)}
				}
				if kind == "ack" {
					step = readerTestStep{typ: websocket.MessageText, data: []byte(`{"type":"result_ack","seq":"1"}`)}
				}
				conn := scriptedReaderConn(step)
				ctx, cancel := context.WithCancel(f.lifeCtx)
				defer cancel()
				assertReaderExit(t, newTestConnectionReader(t, f, 1, conn, 128).run(ctx), readerStopped, errSessionGenerationMismatch)
				synctest.Wait()
				if f.worker.input.input.nextOffset != 0 || f.worker.input.input.ended || f.worker.results.ackedSeq != 0 || f.session.resume.generation != 2 || conn.reads.Load() != 1 {
					t.Fatal("old-generation command mutated session or continued reading")
				}
				closeDeliveryFixture(t, f)
			})
		})
	}
}

func TestConnectionReaderCommandErrors(t *testing.T) {
	for _, name := range []string{"gap", "overflow", "capacity", "end_mismatch", "ack_ahead", "detached"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newDuplexWorkerFixture(t, nil, nil)
				f.start()
				step, want, fatal := readerTestStep{typ: websocket.MessageBinary, data: readerAudioFrame(1, "ab")}, errAudioGap, true
				switch name {
				case "overflow":
					step.data, want = readerAudioFrame(math.MaxUint64, "ab"), errAudioPositionOverflow
				case "capacity":
					step.data, want = readerAudioFrame(0, "123456789012345678901234567890123"+"45"), errAudioBufferFull
				case "end_mismatch":
					step, want = readerTestStep{typ: websocket.MessageText, data: []byte(`{"type":"end","finalOffset":"1"}`)}, errAudioEndMismatch
				case "ack_ahead":
					step, want, fatal = readerTestStep{typ: websocket.MessageText, data: []byte(`{"type":"result_ack","seq":"1"}`)}, errResultAckAhead, false
				case "detached":
					if d, err := f.session.reportDetach(context.Background(), 1); !d || err != nil {
						t.Fatal(d, err)
					}
					want, fatal = errSessionNotAttached, false
				}
				conn := scriptedReaderConn(step, readerTestStep{typ: websocket.MessageBinary, data: readerAudioFrame(0, "later")})
				ctx, cancel := context.WithCancel(f.lifeCtx)
				defer cancel()
				got := newTestConnectionReader(t, f, 1, conn, 128).run(ctx)
				if fatal {
					// 协调者可以先关闭 controlDone；此时 reader 优先报告停止，
					// 原始不可恢复故障必须由运行器的最终结果保留。
					if !(got.kind == readerCommandFailed && errors.Is(got.err, want)) && !(got.kind == readerStopped && errors.Is(got.err, errResumeClosed)) {
						t.Fatal(got)
					}
					f.assertFinishedAtCurrentTime(t, want)
				} else {
					kind := readerCommandFailed
					if name == "detached" {
						kind = readerStopped
					}
					assertReaderExit(t, got, kind, want)
					closeDeliveryFixture(t, f)
				}
				if conn.reads.Load() != 1 {
					t.Fatal("read continued after command rejection")
				}
			})
		})
	}
}

func TestConnectionReaderBorrowedAudioCopyAndOrderedReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		uploaded := make(chan string, 2)
		f, _ := newDuplexWorkerFixture(t, func(ctx context.Context, req *asrv1.StreamingRecognizeRequest) error {
			select {
			case <-release:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
			uploaded <- string(req.Data)
			return nil
		}, nil)
		f.start()
		data := readerAudioFrame(0, "ab")
		n := 0
		conn := &readerTestConn{read: func(context.Context) (websocket.MessageType, []byte, error) {
			n++
			switch n {
			case 1:
				return websocket.MessageBinary, data, nil
			case 2:
				copy(data[8:], "zz") // 前条命令已回复，借用期结束，可以复用。
				return websocket.MessageBinary, readerAudioFrame(0, "ab"), nil
			case 3:
				return websocket.MessageText, []byte(`{"type":"end","finalOffset":"2"}`), nil
			default:
				return 0, nil, io.EOF
			}
		}}
		ctx, cancel := context.WithCancel(f.lifeCtx)
		defer cancel()
		assertReaderExit(t, newTestConnectionReader(t, f, 1, conn, 128).run(ctx), readerReadFailed, io.EOF)
		close(release)
		synctest.Wait()
		if len(uploaded) != 1 || <-uploaded != "ab" || f.worker.input.input.nextOffset != 2 || !f.worker.input.input.ended {
			t.Fatal("borrowed input overwritten or duplicate uploaded")
		}
		closeDeliveryFixture(t, f)
	})
}
