package wsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// controlledV2Connection 让测试分别控制 Read 顺序和 Write 的实际返回。
// 它用于协调/任务语义，真实网络由 client_v2_test 和 Gateway 集成覆盖。
type controlledV2Connection struct {
	reads  chan v2ReadEvent
	writes chan *controlledV2Write
	close  func() error // 可控制实际清理完成的屏障。
}
type controlledV2Write struct {
	typ   websocket.MessageType
	data  []byte
	ctx   context.Context
	reply chan error
}

func newControlledV2Connection() *controlledV2Connection {
	return &controlledV2Connection{reads: make(chan v2ReadEvent, 1), writes: make(chan *controlledV2Write, 1)}
}
func (c *controlledV2Connection) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case event := <-c.reads:
		return websocket.MessageText, event.data, event.err
	}
}
func (c *controlledV2Connection) Write(ctx context.Context, typ websocket.MessageType, data []byte) error {
	call := &controlledV2Write{typ: typ, data: bytes.Clone(data), ctx: ctx, reply: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.writes <- call:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-call.reply:
		return err
	}
}
func (c *controlledV2Connection) SetReadLimit(int64) {}
func (c *controlledV2Connection) CloseNow() error {
	if c.close != nil {
		return c.close()
	}
	return nil
}

func (c *controlledV2Connection) send(message any) {
	data, _ := json.Marshal(message)
	c.reads <- v2ReadEvent{data: data}
}

type v2CoordinatorOutcome struct {
	ack bool
	err error
}
type v2CoordinatorFixture struct {
	conn   *controlledV2Connection
	state  *v2ClientState
	cancel context.CancelFunc
	done   chan v2CoordinatorOutcome
}

// 夹具复用生产协调者和三个 I/O 任务，等待全部任务退出才发布结果。
// 仅模拟已经完成握手的连接；不替代公开 Run 的关闭所有权验收。
func newV2CoordinatorFixture(t *testing.T, cfg V2Config, source io.ReadCloser) *v2CoordinatorFixture {
	t.Helper()
	cfg.URL = "ws://test"
	c := v2TestClient(t, cfg)
	run, err := c.newV2Run(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	ready := wsprotocol.ReadyMessage{Type: wsprotocol.MessageTypeReady, SessionID: "s", ResumeToken: "secret", Generation: 1}
	if err := run.state.acceptReady(ready); err != nil {
		t.Fatal(err)
	}
	f := &v2CoordinatorFixture{conn: newControlledV2Connection(), state: run.state, cancel: run.cancel, done: make(chan v2CoordinatorOutcome, 1)}
	run.current = c.startV2Connection(run.ctx, f.conn, ready)
	run.phase = v2Streaming
	run.startCapture()
	go func() {
		err := run.coordinate()
		err = errors.Join(err, run.cleanup())
		f.done <- v2CoordinatorOutcome{ack: run.ackWritten, err: err}
	}()
	return f
}
func (f *v2CoordinatorFixture) take(t *testing.T, kind wsprotocol.V2InputKind) *controlledV2Write {
	t.Helper()
	var call *controlledV2Write
	select {
	case call = <-f.conn.writes:
	case out := <-f.done:
		t.Fatalf("exited before write: %+v", out)
	}
	var input wsprotocol.V2Input
	var err error
	if call.typ == websocket.MessageBinary {
		input, err = wsprotocol.DecodeV2Audio(call.data)
	} else {
		input, err = wsprotocol.DecodeV2Control(call.data)
	}
	if err != nil || input.Kind != kind {
		t.Fatalf("write input=%+v err=%v", input, err)
	}
	return call
}
func (f *v2CoordinatorFixture) finish(t *testing.T, want error) v2CoordinatorOutcome {
	t.Helper()
	out := <-f.done
	if want == nil && out.err != nil || want != nil && !errors.Is(out.err, want) {
		t.Fatalf("outcome=%+v want=%v", out, want)
	}
	return out
}

func TestV2CoordinatorBlockedWriter(t *testing.T) {
	t.Run("ack_releases_while_write_waits", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			f := newV2CoordinatorFixture(t, V2Config{ChunkBytes: 2, MaxReplayBytes: 2, MaxReplayChunks: 1}, pr)
			if _, err := pw.Write([]byte("aa")); err != nil {
				t.Fatal(err)
			}
			first := f.take(t, wsprotocol.V2InputAudio)
			f.conn.send(wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 2})
			synctest.Wait()
			if f.state.audio.ackedOffset != 2 || f.state.audio.count != 0 {
				t.Fatal("ACK blocked behind Write")
			}
			if _, err := pw.Write([]byte("bb")); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if f.state.audio.nextOffset != 4 || f.state.offeredOffset != 2 {
				t.Fatal("capture blocked or authorized second in-flight write")
			}
			select {
			case <-f.conn.writes:
				t.Fatal("more than one write in flight")
			default:
			}
			first.reply <- nil
			second := f.take(t, wsprotocol.V2InputAudio)
			input, err := wsprotocol.DecodeV2Audio(second.data)
			if err != nil || input.Offset != 2 || string(input.Payload) != "bb" {
				t.Fatalf("input=%+v err=%v", input, err)
			}
			f.cancel()
			f.finish(t, context.Canceled)
		})
	})
	t.Run("capture_budget_fails_while_write_waits", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			f := newV2CoordinatorFixture(t, V2Config{ChunkBytes: 2, MaxReplayBytes: 2, MaxReplayChunks: 1}, pr)
			_, _ = pw.Write([]byte("aa"))
			_ = f.take(t, wsprotocol.V2InputAudio)
			_, _ = pw.Write([]byte("bb"))
			f.finish(t, errReplayBufferFull)
			if f.state.audio.nextOffset != 2 || f.state.audio.count != 1 {
				t.Fatal("full buffer overwrote retained audio")
			}
		})
	})
	t.Run("cancel_joins_blocked_tasks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			f := newV2CoordinatorFixture(t, V2Config{ChunkBytes: 2}, pr)
			_, _ = pw.Write([]byte("aa"))
			_ = f.take(t, wsprotocol.V2InputAudio)
			f.cancel()
			f.finish(t, context.Canceled)
		})
	})
}

func TestV2CoordinatorWriteScheduling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		f := newV2CoordinatorFixture(t, V2Config{ChunkBytes: 2}, pr)
		_, _ = pw.Write([]byte("aa"))
		first := f.take(t, wsprotocol.V2InputAudio)
		_, _ = pw.Write([]byte("bb"))
		for seq := uint64(1); seq <= 3; seq++ {
			f.conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq})
		}
		synctest.Wait()
		if f.state.appliedSeq != 3 {
			t.Fatal("did not apply while writer waits")
		}
		first.reply <- nil
		ack := f.take(t, wsprotocol.V2InputResultAck)
		input, _ := wsprotocol.DecodeV2Control(ack.data)
		if input.Seq != 3 {
			t.Fatal("did not coalesce acknowledgments")
		}
		f.conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 4})
		synctest.Wait()
		ack.reply <- nil
		audioWrite := f.take(t, wsprotocol.V2InputAudio) // 待确认与音频同时存在时交替。
		f.conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 5})
		_ = pw.Close()
		synctest.Wait()
		audioWrite.reply <- nil
		end := f.take(t, wsprotocol.V2InputEnd) // end 优先于尚未写出的结果 ACK。
		endInput, _ := wsprotocol.DecodeV2Control(end.data)
		if endInput.Offset != 4 {
			t.Fatal("wrong end offset")
		}
		f.conn.send(wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1, FinalOffset: 4, LastSeq: 5})
		synctest.Wait()
		end.reply <- nil
		completedAck := f.take(t, wsprotocol.V2InputCompletedAck)
		completeInput, _ := wsprotocol.DecodeV2Control(completedAck.data)
		if completeInput.Seq != 5 {
			t.Fatal("wrong final applied position")
		}
		completedAck.reply <- nil
		out := f.finish(t, nil)
		if !out.ack || !f.state.completed {
			t.Fatal("completion missing")
		}
	})
}

func TestV2CoordinatorCompletionDeadline(t *testing.T) {
	for _, mode := range []string{"silent", "trickle_results", "slow_application"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := V2Config{CompletionTimeout: time.Second, WriteTimeout: 5 * time.Second, ApplyTimeout: 5 * time.Second}
				if mode == "slow_application" {
					cfg.OnResult = func(ctx context.Context, _ wsprotocol.SequencedResultMessage, _ time.Time) error {
						<-ctx.Done()
						return nil
					}
				}
				f := newV2CoordinatorFixture(t, cfg, io.NopCloser(bytes.NewReader(nil)))
				end := f.take(t, wsprotocol.V2InputEnd)
				began := time.Now()
				end.reply <- nil
				if mode == "trickle_results" {
					for seq := uint64(1); seq <= 4; seq++ {
						time.Sleep(200 * time.Millisecond)
						f.conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq})
						ack := f.take(t, wsprotocol.V2InputResultAck)
						ack.reply <- nil
					}
				} else if mode == "slow_application" {
					time.Sleep(700 * time.Millisecond)
					f.conn.send(wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1})
				}
				f.finish(t, context.DeadlineExceeded)
				if time.Since(began) != time.Second || f.state.completed {
					t.Fatalf("deadline elapsed=%v", time.Since(began))
				}
				if mode == "slow_application" && f.state.appliedSeq != 0 {
					t.Fatal("late application committed")
				}
			})
		})
	}
}

func TestV2CoordinatorFinalAck(t *testing.T) {
	for _, mode := range []string{"ordered_close", "write_failure", "final_timeout", "pending_writer_window", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newV2CoordinatorFixture(t, V2Config{WriteTimeout: time.Second}, io.NopCloser(bytes.NewReader(nil)))
				end := f.take(t, wsprotocol.V2InputEnd)
				if mode != "pending_writer_window" {
					end.reply <- nil
					synctest.Wait()
				}
				began := time.Now()
				f.conn.send(wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1})
				f.conn.reads <- v2ReadEvent{err: websocket.CloseError{Code: websocket.StatusNormalClosure}}
				synctest.Wait()
				if !f.state.completed {
					t.Fatal("EOF overtook completed")
				}
				if mode == "pending_writer_window" {
					time.Sleep(800 * time.Millisecond)
					end.reply <- nil
				}
				ack := f.take(t, wsprotocol.V2InputCompletedAck)
				var want error
				switch mode {
				case "ordered_close":
					ack.reply <- nil
				case "write_failure":
					want = io.ErrClosedPipe
					ack.reply <- want
				case "final_timeout", "pending_writer_window":
					want = context.DeadlineExceeded
				case "cancel":
					want = context.Canceled
					f.cancel()
				}
				out := f.finish(t, want)
				if !f.state.completed || out.ack != (mode == "ordered_close") {
					t.Fatalf("completed=%v outcome=%+v", f.state.completed, out)
				}
				if (mode == "final_timeout" || mode == "pending_writer_window") && time.Since(began) != time.Second {
					t.Fatalf("final window renewed: %v", time.Since(began))
				}
			})
		})
	}
}

func TestV2WriterRetainsReceiptAfterCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test"})
		conn := newControlledV2Connection()
		jobs := make(chan v2WriteJob, 1)
		receipts := make(chan v2WriteReceipt, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); c.writeV2Jobs(ctx, conn, jobs, receipts) }()
		jobs <- v2WriteJob{kind: v2WriteResultAck, seq: 7}
		call := <-conn.writes
		cancel()
		<-done
		receipt := <-receipts
		if receipt.seq != 7 || receipt.kind != v2WriteResultAck || !errors.Is(receipt.err, context.Canceled) {
			t.Fatalf("lost receipt: %+v", receipt)
		}
		if call.ctx.Err() == nil {
			t.Fatal("write context not canceled")
		}
	})
}

func TestV2ReaderRetainsMessageOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newControlledV2Connection()
		events := make(chan v2ReadEvent, 1)
		done := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { defer close(done); readV2Events(ctx, conn, events) }()
		conn.send(wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1})
		conn.reads <- v2ReadEvent{err: io.EOF}
		synctest.Wait()
		message := <-events
		if message.err != nil || message.receivedAt.IsZero() {
			t.Fatalf("message=%+v", message)
		}
		decoded, err := decodeV2Message(message.data)
		if err != nil || decoded.typ != wsprotocol.MessageTypeCompleted {
			t.Fatal("completion lost")
		}
		terminal := <-events
		if !errors.Is(terminal.err, io.EOF) {
			t.Fatalf("terminal=%+v", terminal)
		}
		<-done
	})
}

func TestV2CollectorFixedCaptureClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test", ChunkBytes: 3200, Realtime: true})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events := make(chan v2SourceEvent, 1)
		done := make(chan struct{})
		source := bytes.NewReader(make([]byte, 4*3200))
		began := time.Now()
		go func() { defer close(done); c.collectV2Audio(ctx, source, events) }()
		synctest.Wait()
		time.Sleep(500 * time.Millisecond) // 让有界交付阻塞，模拟协调者暂停。
		for i := 0; i < 4; i++ {
			event := <-events
			if len(event.data) != 3200 || event.eof || event.err != nil {
				t.Fatalf("event=%+v", event)
			}
		}
		terminal := <-events
		if !terminal.eof || len(terminal.data) != 0 {
			t.Fatalf("terminal=%+v", terminal)
		}
		<-done
		if time.Since(began) != 500*time.Millisecond {
			t.Fatalf("clock reset after delayed handoff: %v", time.Since(began))
		}
	})
}

func TestV2CoordinatorLateCompletedCannotBeatDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 同一时刻激活消息和期限，检查任意 select 选择都不能把迟到完成记成成功。
		for i := 0; i < 32; i++ {
			f := newV2CoordinatorFixture(t, V2Config{CompletionTimeout: time.Second}, io.NopCloser(bytes.NewReader(nil)))
			end := f.take(t, wsprotocol.V2InputEnd)
			end.reply <- nil
			senderDone := make(chan struct{})
			go func() {
				defer close(senderDone)
				time.Sleep(time.Second)
				f.conn.send(wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1})
			}()
			f.finish(t, context.DeadlineExceeded)
			<-senderDone
			if f.state.completed {
				t.Fatal("late completed was accepted")
			}
		}
	})
}
