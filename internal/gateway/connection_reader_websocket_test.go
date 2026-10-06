package gateway

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// observedReaderConn 的第 n 次 Read 开始说明前 n-1 条输入已完成命令回复。
// 该同步点只用于测试；实际产品不添加此观察 channel。
type observedReaderConn struct {
	*websocket.Conn
	started chan int
	n       int
}

func (c *observedReaderConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	c.n++
	c.started <- c.n
	return c.Conn.Read(ctx)
}

type connectionReaderRunFixture struct {
	finished chan struct{}
	cancel   context.CancelCauseFunc
	exit     connectionReaderExit
}

func startNetworkReader(t *testing.T, f *workerCoordinatorFixture, conn *websocket.Conn, limit int64) (*connectionReaderRunFixture, *observedReaderConn) {
	t.Helper()
	observed := &observedReaderConn{Conn: conn, started: make(chan int, 64)}
	r := newTestConnectionReader(t, f, 1, observed, limit)
	ctx, cancel := context.WithCancelCause(f.lifeCtx)
	run := &connectionReaderRunFixture{finished: make(chan struct{}), cancel: cancel}
	go func() { run.exit = r.run(ctx); close(run.finished) }()
	// 夹具显式承担本步尚未接入的连接拥有者责任，并等实际任务返回。
	t.Cleanup(func() { cancel(nil); _ = conn.CloseNow(); <-run.finished })
	return run, observed
}

func awaitNetworkRead(t *testing.T, ctx context.Context, observed *observedReaderConn, want int) {
	t.Helper()
	for {
		select {
		case n := <-observed.started:
			if n == want {
				return
			}
			if n > want {
				t.Fatalf("reader advanced unexpectedly: %d > %d", n, want)
			}
		case <-ctx.Done():
			t.Fatal("reader did not reach next Read", context.Cause(ctx))
		}
	}
}

func waitNetworkReader(t *testing.T, ctx context.Context, run *connectionReaderRunFixture) connectionReaderExit {
	t.Helper()
	select {
	case <-run.finished:
		return run.exit
	case <-ctx.Done():
		t.Fatal("reader cleanup blocked", context.Cause(ctx))
	}
	return connectionReaderExit{}
}

func writeReaderNetworkInput(t *testing.T, ctx context.Context, client *websocket.Conn, observed *observedReaderConn, typ websocket.MessageType, data []byte, nextRead int) {
	t.Helper()
	if err := client.Write(ctx, typ, data); err != nil {
		t.Fatal("write v2 input", err)
	}
	awaitNetworkRead(t, ctx, observed, nextRead)
}

func TestConnectionReaderWebSocketDuplexAndTailAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
	var sends atomic.Int32
	f, feed := newDuplexWorkerFixture(t, func(_ context.Context, req *asrv1.StreamingRecognizeRequest) error {
		sends.Add(1)
		if string(req.Data) != "ab" {
			t.Errorf("audio header leaked or payload changed: %q", req.Data)
		}
		return nil
	}, nil)
	f.worker.results, _ = newResultBuffer(6, 1)
	f.start()
	reader, observed := startNetworkReader(t, f, legacy.ws, 128)
	writer := startTestGenerationWriter(t, newTestGenerationWriter(t, f, 1, legacy.ws, time.Second))
	awaitNetworkRead(t, ctx, observed, 1)
	writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageBinary, readerAudioFrame(0, "ab"), 2)
	writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageBinary, readerAudioFrame(0, "ab"), 3)
	// 同一 Worker 接收路径连续产出；单槽预算必须由真正网络 ACK 释放。
	for i, text := range []string{"first", "next!"} {
		seq := uint64(i + 1)
		sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: text}})
		readSequencedNetworkResult(t, ctx, client, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq, SegmentID: "s", Text: text})
		ack := []byte(`{"type":"result_ack","seq":"1"}`)
		if seq == 2 {
			ack = []byte(`{"type":"result_ack","seq":"2"}`)
		}
		writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageText, ack, 4+i)
		if f.worker.results.count != 0 || f.worker.results.retainedBytes != 0 || f.worker.results.ackedSeq != seq {
			t.Fatal("network ACK did not release slot and bytes")
		}
	}
	writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageText, []byte(`{"type":"end","finalOffset":"2"}`), 6)
	writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageText, []byte(`{"type":"end","finalOffset":"2"}`), 7)
	writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageBinary, readerAudioFrame(0, "ab"), 8)
	select {
	case <-f.worker.uploader.done:
	case <-ctx.Done():
		t.Fatal("audio half-close did not finish")
	}
	if sends.Load() != 1 {
		t.Fatalf("historical resend duplicated Worker input: %d", sends.Load())
	}
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "tail!", IsFinal: true}})
	readSequencedNetworkResult(t, ctx, client, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 3, SegmentID: "s", Text: "tail!", IsFinal: true})
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{err: io.EOF})
	assertNetworkWriterExit(t, ctx, writer, writerResultsComplete, 3, nil)
	writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageText, []byte(`{"type":"result_ack","seq":"3"}`), 9)
	writeReaderNetworkInput(t, ctx, client, observed, websocket.MessageText, []byte(`{"type":"result_ack","seq":"3"}`), 10)
	if f.worker.results.count != 0 || f.worker.results.ackedSeq != 3 {
		t.Fatal("tail ACK lost after end or normal writer return")
	}
	select {
	case <-reader.finished:
		t.Fatal("end or writer completion ended reader")
	default:
	}
	if err := f.session.requestClose(ctx); err != nil {
		t.Fatal(err)
	}
	reader.cancel(nil)
	got := waitNetworkReader(t, ctx, reader)
	if got.kind != readerStopped || got.err == nil {
		t.Fatal(got)
	}
	select {
	case <-f.finished:
	case <-ctx.Done():
		t.Fatal("Worker tasks did not clean up")
	}
	if f.err != nil || f.worker.input != nil || f.worker.results != nil {
		t.Fatal("logical runner did not finish resource cleanup", f.err)
	}
}

func TestConnectionReaderWebSocketMessageLimit(t *testing.T) {
	for _, name := range []string{"audio_at_limit", "audio_header_counts", "control_too_big"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
			f, _ := newDuplexWorkerFixture(t, nil, nil)
			f.start()
			run, observed := startNetworkReader(t, f, legacy.ws, 9)
			awaitNetworkRead(t, ctx, observed, 1)
			typ, data := websocket.MessageBinary, readerAudioFrame(0, "a")
			if name == "audio_header_counts" {
				data = readerAudioFrame(0, "ab")
			}
			if name == "control_too_big" {
				typ, data = websocket.MessageText, []byte(`{"type":"result_ack","seq":"0","extra":"`+strings.Repeat("x", 32)+`"}`)
			}
			if err := client.Write(ctx, typ, data); err != nil {
				t.Fatal(err)
			}
			if name == "audio_at_limit" {
				awaitNetworkRead(t, ctx, observed, 2)
				if f.worker.input.input.nextOffset != 1 {
					t.Fatal("header counted in audio offset")
				}
				run.cancel(nil)
				got := waitNetworkReader(t, ctx, run)
				assertReaderExit(t, got, readerStopped, context.Canceled)
			} else {
				assertReaderExit(t, waitNetworkReader(t, ctx, run), readerProtocolFailed, websocket.ErrMessageTooBig)
				// reader 只汇报错误，不自行终止/取消逻辑会话。
				if advanced, err := f.session.requestResultAck(ctx, 1, 0); advanced || err != nil {
					t.Fatalf("reader failure ended coordinator: (%v,%v)", advanced, err)
				}
			}
			if err := f.session.requestClose(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-f.finished:
			case <-ctx.Done():
				t.Fatal("Worker cleanup blocked")
			}
		})
	}
}

func TestConnectionReaderWebSocketReadTermination(t *testing.T) {
	for _, name := range []string{"connection_cancel", "logical_cancel", "normal_peer_close", "ack_ahead"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
			f, _ := newDuplexWorkerFixture(t, nil, nil)
			f.start()
			run, observed := startNetworkReader(t, f, legacy.ws, 128)
			awaitNetworkRead(t, ctx, observed, 1)
			cause := errors.New("network test owner stopped I/O")
			switch name {
			case "connection_cancel":
				run.cancel(cause)
			case "logical_cancel":
				f.cancelLife(cause)
			case "normal_peer_close":
				if err := client.Close(websocket.StatusNormalClosure, "done"); err != nil {
					t.Fatal(err)
				}
			case "ack_ahead":
				if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"result_ack","seq":"1"}`)); err != nil {
					t.Fatal(err)
				}
			}
			got := waitNetworkReader(t, ctx, run)
			switch name {
			case "connection_cancel", "logical_cancel":
				assertReaderExit(t, got, readerStopped, cause)
			case "normal_peer_close":
				if got.kind != readerReadFailed || websocket.CloseStatus(got.err) != websocket.StatusNormalClosure {
					t.Fatal(got)
				}
			case "ack_ahead":
				assertReaderExit(t, got, readerCommandFailed, errResultAckAhead)
			}
			if name != "logical_cancel" {
				if context.Cause(f.rpcCtx) != nil {
					t.Fatal("connection reader canceled original Worker")
				}
				if err := f.session.requestClose(ctx); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-f.finished:
			case <-ctx.Done():
				t.Fatal("Worker tasks did not exit")
			}
		})
	}
}
