package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// sendWriterNetworkStep 的期限是测试兜底，不充当业务处理期限。
func sendWriterNetworkStep(t *testing.T, ctx context.Context, feed chan workerReadStep, step workerReadStep) {
	t.Helper()
	select {
	case feed <- step:
	case <-ctx.Done():
		t.Fatal("Worker fixture feed blocked", context.Cause(ctx))
	}
}

func assertNetworkWriterExit(t *testing.T, ctx context.Context, run *resultWriterRunFixture, kind resultWriterExitKind, lastSeq uint64, want error) {
	t.Helper()
	select {
	case <-run.finished:
	case <-ctx.Done():
		t.Fatal("writer did not return", context.Cause(ctx))
	}
	if run.exit.kind != kind || run.exit.lastSeq != lastSeq || !errors.Is(run.exit.err, want) {
		t.Fatalf("network writer exit = %+v, want kind=%d seq=%d err=%v", run.exit, kind, lastSeq, want)
	}
}

func readSequencedNetworkResult(t *testing.T, ctx context.Context, client *websocket.Conn, want wsprotocol.SequencedResultMessage) {
	t.Helper()
	typ, data, err := client.Read(ctx)
	if err != nil {
		t.Fatal("read result", err)
	}
	got := decodeWriterMessage(t, typ, data)
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if string(wire["seq"]) != strconv.Quote(strconv.FormatUint(want.Seq, 10)) {
		t.Fatalf("sequence lost string precision: %s", data)
	}
}

func TestResultWriterWebSocketPipeline(t *testing.T) {
	for _, name := range []string{"ordered_updates_and_tail", "maximum_sequence"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
			sent := make(chan struct{})
			f, feed := newDuplexWorkerFixture(t, func(_ context.Context, req *asrv1.StreamingRecognizeRequest) error {
				if string(req.Data) != "ab" {
					t.Errorf("unexpected audio: %q", req.Data)
				}
				close(sent)
				return nil
			}, nil)
			seq := uint64(1)
			if name == "maximum_sequence" {
				seq = math.MaxUint64
				f.worker.results.lastSeq, f.worker.results.ackedSeq = seq-1, seq-1
				f.worker.delivery.cursor, f.worker.delivery.offeredSeq = seq-1, seq-1
			}
			f.start()
			w := newTestGenerationWriter(t, f, 1, legacy.ws, time.Second)
			run := startTestGenerationWriter(t, w)
			a, n, err := f.session.requestAudio(ctx, 1, 0, []byte("ab"))
			assertCoordinatorInput(t, a, n, err, true, 2, nil)
			select {
			case <-sent:
			case <-ctx.Done():
				t.Fatal("audio upload blocked")
			}
			sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 2}}})
			sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "same", Text: "你好", IsFinal: false}})
			readSequencedNetworkResult(t, ctx, client, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq, SegmentID: "same", Text: "你好"})
			if advanced, err := f.session.requestResultAck(ctx, 1, seq); !advanced || err != nil {
				t.Fatalf("ACK after actual read = (%v,%v)", advanced, err)
			}
			a, n, err = f.session.requestEnd(ctx, 1, 2)
			assertCoordinatorInput(t, a, n, err, true, 2, nil)
			if name == "ordered_updates_and_tail" {
				seq++
				sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "same", Text: "你好医生", IsFinal: true}})
				readSequencedNetworkResult(t, ctx, client, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq, SegmentID: "same", Text: "你好医生", IsFinal: true})
			}
			// 等待真正 CloseSend 的成功结果，避免夹具把 EOF 注入到半关闭之前。
			select {
			case <-f.worker.uploader.done:
			case <-ctx.Done():
				t.Fatal("half-close did not finish")
			}
			sendWriterNetworkStep(t, ctx, feed, workerReadStep{err: io.EOF})
			assertNetworkWriterExit(t, ctx, run, writerResultsComplete, seq, nil)
			offer, err := f.session.requestResult(ctx, 1)
			if err != nil || offer.available || !offer.workerCompleted || offer.lastSeq != seq {
				t.Fatalf("normal result sending ended session prematurely: (%+v,%v)", offer, err)
			}
			if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"still-open"}`)); err != nil {
				t.Fatal("writer closed normally completed connection", err)
			}
			if err := f.session.requestClose(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-f.finished:
			case <-ctx.Done():
				t.Fatal("Worker tasks did not clean up")
			}
			if f.err != nil {
				t.Fatal(f.err)
			}
		})
	}
}

func TestResultWriterWebSocketActualWriteExit(t *testing.T) {
	for _, name := range []string{"write_deadline", "parent_cancel", "closed_connection"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			blocked := name != "closed_connection"
			legacy, _, gate := newLegacyResultWriteFixture(t, time.Second, blocked)
			if !blocked {
				if err := legacy.ws.CloseNow(); err != nil {
					t.Fatal(err)
				}
			}
			f, feed := newDuplexWorkerFixture(t, nil, nil)
			f.start()
			budget := 75 * time.Millisecond
			if name == "parent_cancel" {
				budget = time.Minute
			}
			w := newTestGenerationWriter(t, f, 1, legacy.ws, budget)
			run := startTestGenerationWriter(t, w)
			sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{Text: "blocked"}})
			cause := errors.New("connection owner canceled actual Write")
			if blocked {
				select {
				case <-gate.started:
				case <-ctx.Done():
					t.Fatal("actual transport Write not reached")
				}
				if name == "parent_cancel" {
					run.cancel(cause)
				}
			}
			if name == "closed_connection" {
				select {
				case <-run.finished:
				case <-ctx.Done():
					t.Fatal("closed connection Write did not return")
				}
				if run.exit.kind != writerWriteFailed || run.exit.err == nil || errors.Is(run.exit.err, ErrResultWriteTimeout) {
					t.Fatalf("closed connection exit = %+v", run.exit)
				}
			} else {
				kind, want := writerWriteFailed, ErrResultWriteTimeout
				if name == "parent_cancel" {
					kind, want = writerStopped, cause
				}
				assertNetworkWriterExit(t, ctx, run, kind, 0, want)
				select {
				case <-gate.closed:
				default:
					t.Fatal("write cancellation did not close actual transport")
				}
			}
			if _, err := f.session.requestResult(ctx, 1); !errors.Is(err, errResultWriteInFlight) {
				t.Fatalf("failed actual Write reported success: %v", err)
			}
			// 本步夹具明确承担连接拥有者的收尾职责，writer 不自行结束逻辑会话。
			if err := f.session.requestClose(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-f.finished:
			case <-ctx.Done():
				t.Fatal("Worker cleanup after writer failure blocked")
			}
			if f.err != nil {
				t.Fatal(f.err)
			}
		})
	}
}
