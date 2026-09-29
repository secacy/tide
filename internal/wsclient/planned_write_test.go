package wsclient

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
)

// TestWritePlansOnlyForRealtimeAudio 检查控制消息无计划，实时音频共享固定时间轴。
func TestWritePlansOnlyForRealtimeAudio(t *testing.T) {
	for _, realtime := range []bool{false, true} {
		name := "unpaced"
		if realtime {
			name = "realtime"
		}
		t.Run(name, func(t *testing.T) {
			ctx, conn, messages := writeTestPeer(t)
			var events []WriteEvent
			client := &Client{cfg: Config{ChunkBytes: 3200, Realtime: realtime, OnWrite: func(e WriteEvent) { events = append(events, e) }}}
			if err := client.send(ctx, conn, bytes.NewReader(make([]byte, 6402))); err != nil {
				t.Fatal(err)
			}
			if len(events) != 5 {
				t.Fatalf("events = %d, want 5", len(events))
			}
			if !events[0].PlannedAt.IsZero() || !events[4].PlannedAt.IsZero() {
				t.Fatal("control message received audio plan")
			}
			if realtime {
				assertAudioPlans(t, events[1:4], 3200)
				if events[1].PlannedAt.Before(events[0].FinishedAt) {
					t.Fatal("audio timeline started before start Write returned")
				}
			} else {
				for _, e := range events {
					if !e.PlannedAt.IsZero() {
						t.Fatalf("unpaced event has plan: %+v", e)
					}
				}
			}
			for i, size := range []int{0, 3200, 3200, 2, 0} {
				select {
				case msg := <-messages:
					if size > 0 && (msg.kind != websocket.MessageBinary || len(msg.data) != size) {
						t.Fatalf("frame %d: kind=%v size=%d", i, msg.kind, len(msg.data))
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}

// TestSlowSourcePreservesOriginalPlan 慢首次读取应体现为首块落后，不重新设定音频起点。
func TestSlowSourcePreservesOriginalPlan(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	source := &delayedPlanReader{reader: bytes.NewReader(make([]byte, 642)), delay: 80 * time.Millisecond}
	var events []WriteEvent
	client := &Client{cfg: Config{ChunkBytes: 320, Realtime: true, OnWrite: func(e WriteEvent) { events = append(events, e) }}}
	if err := client.sendAudio(ctx, conn, source); err != nil {
		t.Fatal(err)
	}
	assertAudioPlans(t, events, 320)
	if events[0].PlannedAt.After(source.startedAt) || events[0].StartedAt.Before(source.finishedAt) {
		t.Fatalf("source time not included: event=%+v source=%+v", events[0], source)
	}
	if lag := events[0].StartedAt.Sub(events[0].PlannedAt); lag < source.delay {
		t.Fatalf("lag=%v, want >= %v", lag, source.delay)
	}
}

// TestSlowCallbackDoesNotShiftPlan 回调延迟应出现在后续块落后中，不能改写原排期。
func TestSlowCallbackDoesNotShiftPlan(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	var events []WriteEvent
	var callbackEnd time.Time
	const delay = 80 * time.Millisecond
	client := &Client{cfg: Config{ChunkBytes: 320, Realtime: true, OnWrite: func(e WriteEvent) {
		events = append(events, e)
		if len(events) == 1 {
			time.Sleep(delay)
			callbackEnd = time.Now()
		}
	}}}
	if err := client.sendAudio(ctx, conn, bytes.NewReader(make([]byte, 642))); err != nil {
		t.Fatal(err)
	}
	assertAudioPlans(t, events, 320)
	if callbackEnd.Sub(events[0].FinishedAt) < delay || events[1].StartedAt.Before(callbackEnd) {
		t.Fatal("write boundaries include callback or second write precedes callback return")
	}
	if lag := events[1].StartedAt.Sub(events[1].PlannedAt); lag < delay-10*time.Millisecond {
		t.Fatalf("second lag=%v, want >=70ms", lag)
	}
}

// TestFailedRealtimeWriteRetainsPlan 真实失败尝试也携带计划，仍只有一次原始错误回调。
func TestFailedRealtimeWriteRetainsPlan(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	conn.CloseNow()
	var events []WriteEvent
	client := &Client{cfg: Config{ChunkBytes: 320, Realtime: true, OnWrite: func(e WriteEvent) { events = append(events, e) }}}
	err := client.sendAudio(ctx, conn, bytes.NewReader(make([]byte, 320)))
	if err == nil || len(events) != 1 {
		t.Fatalf("err=%v events=%+v", err, events)
	}
	e := events[0]
	if e.PlannedAt.IsZero() || e.PlannedAt.After(e.StartedAt) || e.Kind != WriteAudio || e.AudioBytes != 320 || !errors.Is(err, e.Err) {
		t.Fatalf("failed attempt lost facts: %+v, err=%v", e, err)
	}
}

// TestWritePlanPassedUnchanged 检查 helper 不格式化时间或使用写入/回调时刻覆盖原计划。
func TestWritePlanPassedUnchanged(t *testing.T) {
	ctx, conn, _ := writeTestPeer(t)
	planned := time.Now().Add(-time.Second)
	var observed WriteEvent
	client := &Client{cfg: Config{OnWrite: func(e WriteEvent) { observed = e }}}
	if err := client.writeObserved(ctx, conn, WriteAudio, websocket.MessageBinary, []byte{0, 0}, planned); err != nil {
		t.Fatal(err)
	}
	if observed.PlannedAt != planned {
		t.Fatal("plan or monotonic reading changed")
	}
}

// assertAudioPlans 检查三块音频按成功字节推进，末块为两个字节，读取计划不漂移。
func assertAudioPlans(t *testing.T, events []WriteEvent, chunk int) {
	t.Helper()
	if len(events) != 3 {
		t.Fatalf("audio events = %d, want 3", len(events))
	}
	origin := events[0].PlannedAt
	if origin.IsZero() {
		t.Fatal("missing realtime plan")
	}
	var elapsed time.Duration
	for i, e := range events {
		wantBytes := chunk
		if i == 2 {
			wantBytes = 2
		}
		if e.Kind != WriteAudio || e.AudioBytes != wantBytes || e.Err != nil || e.PlannedAt != origin.Add(elapsed) || e.StartedAt.Before(e.PlannedAt) {
			t.Fatalf("event %d has wrong plan or write facts: %+v", i, e)
		}
		elapsed += audio.DurationFromBytes(wantBytes)
	}
}

// delayedPlanReader 只拖慢第一次读取，记录边界供原计划验证使用。
type delayedPlanReader struct {
	reader                io.Reader
	delay                 time.Duration
	startedAt, finishedAt time.Time
}

func (r *delayedPlanReader) Read(p []byte) (int, error) {
	if r.startedAt.IsZero() {
		r.startedAt = time.Now()
		time.Sleep(r.delay)
		r.finishedAt = time.Now()
	}
	return r.reader.Read(p)
}
