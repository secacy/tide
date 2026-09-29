package loadgen_test

import (
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

func TestRecorderZeroValue(t *testing.T) {
	var recorder loadgen.SessionRecorder
	if got := recorder.Snapshot(); !reflect.DeepEqual(got, loadgen.SessionObservation{}) {
		t.Fatalf("initial observation = %+v, want zero value", got)
	}
	// 没有 end 时 Err 同样为 nil，必须用 Kind 区分缺失事件。
	if got := recorder.Snapshot(); got.EndWrite.Kind == wsclient.WriteEnd {
		t.Fatal("missing end was recorded as an attempted end")
	}
}

// TestRecorderAudioAccounting 对比成功写入、末尾失败和首次即失败；
// 失败参与最大写入耗时，但不改变成功音频量及成功写入的时间范围。
func TestRecorderAudioAccounting(t *testing.T) {
	base := recorderBaseTime()
	writeErr := errors.New("write failed")
	first := recorderWrite(wsclient.WriteAudio, 3200, base, 10*time.Millisecond, nil)
	last := recorderWrite(wsclient.WriteAudio, 2, base.Add(time.Second), 5*time.Millisecond, nil)
	failed := recorderWrite(wsclient.WriteAudio, 3200, base.Add(2*time.Second), 50*time.Millisecond, writeErr)
	for _, tc := range []struct {
		name   string
		events []wsclient.WriteEvent
		want   loadgen.SessionObservation
	}{
		{
			name: "success_with_short_tail", events: []wsclient.WriteEvent{first, last},
			want: loadgen.SessionObservation{AudioBytesWritten: 3202, AudioChunksWritten: 2,
				MaxAudioWriteDuration: 10 * time.Millisecond, FirstAudioStartedAt: first.StartedAt, LastAudioFinishedAt: last.FinishedAt},
		},
		{
			name: "failure_after_success", events: []wsclient.WriteEvent{first, last, failed},
			want: loadgen.SessionObservation{AudioBytesWritten: 3202, AudioChunksWritten: 2, WriteFailures: 1,
				MaxAudioWriteDuration: 50 * time.Millisecond, FirstAudioStartedAt: first.StartedAt, LastAudioFinishedAt: last.FinishedAt},
		},
		{
			name: "first_write_failed", events: []wsclient.WriteEvent{failed},
			want: loadgen.SessionObservation{WriteFailures: 1, MaxAudioWriteDuration: 50 * time.Millisecond},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recorder loadgen.SessionRecorder
			for _, event := range tc.events {
				recorder.ObserveWrite(event)
			}
			assertObservation(t, recorder.Snapshot(), tc.want)
		})
	}
}

// TestRecorderControlWrites 保留控制消息的成功/失败事实和原错误，
// 控制消息耗时不能混入音频写入最大耗时。
func TestRecorderControlWrites(t *testing.T) {
	writeErr := errors.New("control write failed")
	for _, kind := range []wsclient.WriteKind{wsclient.WriteStart, wsclient.WriteEnd} {
		for _, failed := range []bool{false, true} {
			name := string(kind) + "/success"
			var err error
			if failed {
				name = string(kind) + "/failed"
				err = writeErr
			}
			t.Run(name, func(t *testing.T) {
				var recorder loadgen.SessionRecorder
				event := recorderWrite(kind, 0, recorderBaseTime(), time.Second, err)
				recorder.ObserveWrite(event)
				want := loadgen.SessionObservation{}
				if kind == wsclient.WriteStart {
					want.StartWrite = event
				} else {
					want.EndWrite = event
				}
				if failed {
					want.WriteFailures = 1
				}
				assertObservation(t, recorder.Snapshot(), want)
				if failed {
					got := recorder.Snapshot().StartWrite
					if kind == wsclient.WriteEnd {
						got = recorder.Snapshot().EndWrite
					}
					if got.Err != writeErr {
						t.Fatal("control error identity changed")
					}
				}
			})
		}
	}
}

// TestRecorderResultsContinueAfterFinal 按收到的消息计数而非按片段去重；
// 后续 partial 更新最后结果时间，但不会覆盖最后 final 的时间。
func TestRecorderResultsContinueAfterFinal(t *testing.T) {
	var recorder loadgen.SessionRecorder
	base := recorderBaseTime()
	for i, final := range []bool{false, true, false, true, false} {
		recorder.ObserveResult(wsprotocol.ResultMessage{
			Type: wsprotocol.MessageTypeResult, SegmentID: "segment", Text: "识别正文", IsFinal: final,
		}, base.Add(time.Duration(i)*time.Second))
		got := recorder.Snapshot()
		wantFinal := int64((i + 1) / 2)
		want := loadgen.SessionObservation{
			ResultCount: int64(i + 1), FinalResultCount: wantFinal,
			FirstResultAt: base, LastResultAt: base.Add(time.Duration(i) * time.Second),
		}
		if wantFinal > 0 {
			want.LastFinalAt = base.Add(time.Duration(2*wantFinal-1) * time.Second)
		}
		assertObservation(t, got, want)
	}
}

// TestRecorderSnapshotsAndSessionsAreIndependent 验证修改快照不影响内部记录，
// 后续记录不改变历史快照，其他会话也不会受到影响。
func TestRecorderSnapshotsAndSessionsAreIndependent(t *testing.T) {
	var first, second loadgen.SessionRecorder
	base := recorderBaseTime()
	endErr := errors.New("end failed")
	first.ObserveWrite(recorderWrite(wsclient.WriteAudio, 3200, base, time.Millisecond, nil))
	first.ObserveWrite(recorderWrite(wsclient.WriteEnd, 0, base.Add(time.Second), time.Millisecond, endErr))
	want := first.Snapshot()
	changed := first.Snapshot()
	changed.AudioBytesWritten = 999
	changed.FirstAudioStartedAt = time.Time{}
	changed.EndWrite.Kind = wsclient.WriteStart
	changed.EndWrite.Err = nil
	assertObservation(t, first.Snapshot(), want)
	assertObservation(t, second.Snapshot(), loadgen.SessionObservation{})
	first.ObserveResult(wsprotocol.ResultMessage{IsFinal: true}, base.Add(2*time.Second))
	if want.ResultCount != 0 || want.EndWrite.Err != endErr {
		t.Fatal("an old snapshot changed after recording a result")
	}
	second.ObserveWrite(recorderWrite(wsclient.WriteAudio, 2, base, time.Millisecond, nil))
	if first.Snapshot().AudioBytesWritten != 3200 || second.Snapshot().AudioBytesWritten != 2 {
		t.Fatal("session byte counts are not independent")
	}
}

// TestRecorderConcurrentSnapshots 模拟一场会话的独立发送/接收协程，
// 第三个协程反复取快照，检查相关字段是同一时刻的一致状态。
func TestRecorderConcurrentSnapshots(t *testing.T) {
	var recorder loadgen.SessionRecorder
	const audioCount = 2000
	const resultCount = 1500
	const snapshots = 2000
	base := recorderBaseTime()
	endErr := errors.New("end write failed")
	start := recorderWrite(wsclient.WriteStart, 0, base.Add(-time.Second), time.Millisecond, nil)
	end := recorderWrite(wsclient.WriteEnd, 0, base.Add(audioCount*time.Millisecond), time.Millisecond, endErr)
	begin := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-begin
		recorder.ObserveWrite(start)
		for i := range audioCount {
			event := recorderWrite(wsclient.WriteAudio, 3200, base.Add(time.Duration(i)*time.Millisecond), time.Microsecond, nil)
			// 每个样本落后递增 1ns，使快照中的样本数与最大值存在可核对关系。
			event.PlannedAt = event.StartedAt.Add(-time.Duration(i + 1))
			recorder.ObserveWrite(event)
			if i%32 == 0 {
				runtime.Gosched()
			}
		}
		recorder.ObserveWrite(end)
	}()
	go func() {
		defer wg.Done()
		<-begin
		for i := range resultCount {
			recorder.ObserveResult(wsprotocol.ResultMessage{IsFinal: (i+1)%3 == 0}, base.Add(time.Duration(i)*time.Millisecond))
			if i%32 == 0 {
				runtime.Gosched()
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-begin
		for range snapshots {
			got := recorder.Snapshot()
			if got.AudioScheduleSamples != got.AudioChunksWritten || got.MaxAudioScheduleLag != time.Duration(got.AudioScheduleSamples) {
				t.Errorf("inconsistent schedule snapshot: %+v", got)
				return
			}
			if got.AudioBytesWritten != got.AudioChunksWritten*3200 || got.FinalResultCount != got.ResultCount/3 {
				t.Errorf("inconsistent counts: %+v", got)
				return
			}
			if got.AudioChunksWritten > 0 && (got.FirstAudioStartedAt != base || got.LastAudioFinishedAt != base.Add(time.Duration(got.AudioChunksWritten-1)*time.Millisecond+time.Microsecond)) {
				t.Errorf("inconsistent audio times: %+v", got)
				return
			}
			if got.ResultCount > 0 && (got.FirstResultAt != base || got.LastResultAt != base.Add(time.Duration(got.ResultCount-1)*time.Millisecond)) {
				t.Errorf("inconsistent result times: %+v", got)
				return
			}
			if (got.EndWrite.Kind == wsclient.WriteEnd) != (got.WriteFailures == 1) {
				t.Errorf("failed end and failure counter disagree: %+v", got)
				return
			}
			runtime.Gosched()
		}
	}()
	close(begin)
	wg.Wait()
	assertObservation(t, recorder.Snapshot(), loadgen.SessionObservation{
		AudioBytesWritten: audioCount * 3200, AudioChunksWritten: audioCount, WriteFailures: 1,
		AudioScheduleSamples: audioCount, MaxAudioScheduleLag: audioCount * time.Nanosecond,
		MaxAudioWriteDuration: time.Microsecond, FirstAudioStartedAt: base,
		LastAudioFinishedAt: base.Add((audioCount-1)*time.Millisecond + time.Microsecond),
		StartWrite:          start, EndWrite: end, ResultCount: resultCount, FinalResultCount: resultCount / 3,
		FirstResultAt: base, LastResultAt: base.Add((resultCount - 1) * time.Millisecond),
		LastFinalAt: base.Add((resultCount - 1) * time.Millisecond),
	})
}

func recorderBaseTime() time.Time {
	return time.Date(2026, 9, 28, 10, 0, 0, 123, time.UTC)
}

func recorderWrite(kind wsclient.WriteKind, size int, at time.Time, elapsed time.Duration, err error) wsclient.WriteEvent {
	return wsclient.WriteEvent{Kind: kind, AudioBytes: size, StartedAt: at, FinishedAt: at.Add(elapsed), Err: err}
}

func assertObservation(t *testing.T, got, want loadgen.SessionObservation) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("observation:\n got: %+v\nwant: %+v", got, want)
	}
}
