package loadgen_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/loadgen"
	"github.com/secacy/tide-artisan/internal/wsclient"
)

// TestRecorderScheduleEligibility 核对缺失/零值/负值及失败尝试的计量口径。
func TestRecorderScheduleEligibility(t *testing.T) {
	base := recorderBaseTime()
	for _, tc := range []struct {
		name             string
		kind             wsclient.WriteKind
		planned, started time.Time
		failed           bool
		count            int64
		lag              time.Duration
	}{
		{"missing_plan", wsclient.WriteAudio, time.Time{}, base, false, 0, 0},
		{"missing_start", wsclient.WriteAudio, base, time.Time{}, false, 0, 0},
		{"zero_lag", wsclient.WriteAudio, base, base, false, 1, 0},
		{"early_is_zero", wsclient.WriteAudio, base.Add(time.Second), base, false, 1, 0},
		{"nanosecond_lag", wsclient.WriteAudio, base, base.Add(7), false, 1, 7},
		{"failed_audio", wsclient.WriteAudio, base, base.Add(40 * time.Millisecond), true, 1, 40 * time.Millisecond},
		{"start_with_plan", wsclient.WriteStart, base, base.Add(time.Second), false, 0, 0},
		{"failed_end_with_plan", wsclient.WriteEnd, base, base.Add(time.Second), true, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recorder loadgen.SessionRecorder
			var writeErr error
			if tc.failed {
				writeErr = errors.New("fixture write failure")
			}
			e := recorderWrite(tc.kind, 4, tc.started, 2*time.Millisecond, writeErr)
			e.PlannedAt = tc.planned
			recorder.ObserveWrite(e)
			got := recorder.Snapshot()
			if got.AudioScheduleSamples != tc.count || got.MaxAudioScheduleLag != tc.lag {
				t.Fatalf("schedule = %d/%v, want %d/%v", got.AudioScheduleSamples, got.MaxAudioScheduleLag, tc.count, tc.lag)
			}
			wantFailures := int64(0)
			if tc.failed {
				wantFailures = 1
			}
			if got.WriteFailures != wantFailures {
				t.Fatal("schedule observation changed failure accounting")
			}
			if tc.kind == wsclient.WriteAudio {
				wantBytes := int64(4)
				if tc.failed {
					wantBytes = 0
				}
				if got.AudioBytesWritten != wantBytes || got.AudioChunksWritten != wantBytes/4 || got.MaxAudioWriteDuration != 2*time.Millisecond {
					t.Fatalf("existing audio accounting lost: %+v", got)
				}
			} else if got.MaxAudioWriteDuration != 0 || got.AudioBytesWritten != 0 {
				t.Fatal("control event entered audio accounting")
			}
		})
	}
}

// TestRecorderScheduleMaximumIncludesFailure 最大落后与最大写入耗时各自统计，不能混淆。
func TestRecorderScheduleMaximumIncludesFailure(t *testing.T) {
	var recorder loadgen.SessionRecorder
	base := recorderBaseTime()
	for i, tc := range []struct {
		lag, write      time.Duration
		bytes           int
		failed, planned bool
	}{
		{5 * time.Millisecond, 2 * time.Millisecond, 4, false, true},
		{40 * time.Millisecond, 50 * time.Millisecond, 4, true, true},
		{2 * time.Millisecond, time.Millisecond, 2, false, true},
		{0, 200 * time.Millisecond, 4, false, false},
	} {
		var err error
		if tc.failed {
			err = errors.New("failed attempt")
		}
		e := recorderWrite(wsclient.WriteAudio, tc.bytes, base.Add(time.Duration(i)*time.Second), tc.write, err)
		if tc.planned {
			e.PlannedAt = e.StartedAt.Add(-tc.lag)
		}
		recorder.ObserveWrite(e)
	}
	got := recorder.Snapshot()
	if got.AudioScheduleSamples != 3 || got.MaxAudioScheduleLag != 40*time.Millisecond || got.MaxAudioWriteDuration != 200*time.Millisecond || got.AudioBytesWritten != 10 || got.AudioChunksWritten != 3 || got.WriteFailures != 1 {
		t.Fatalf("mixed accounting = %+v", got)
	}
}

// TestRecorderScheduleSnapshotIsolation 验证统计随值快照隔离，且不串到其他会话。
func TestRecorderScheduleSnapshotIsolation(t *testing.T) {
	var first, second loadgen.SessionRecorder
	e := recorderWrite(wsclient.WriteAudio, 4, recorderBaseTime(), time.Millisecond, nil)
	e.PlannedAt = e.StartedAt.Add(-5 * time.Millisecond)
	first.ObserveWrite(e)
	old := first.Snapshot()
	changed := old
	changed.AudioScheduleSamples, changed.MaxAudioScheduleLag = 99, time.Hour
	if got := first.Snapshot(); got.AudioScheduleSamples != 1 || got.MaxAudioScheduleLag != 5*time.Millisecond {
		t.Fatalf("snapshot mutation affected recorder: %+v", got)
	}
	e.PlannedAt = e.StartedAt.Add(-40 * time.Millisecond)
	first.ObserveWrite(e)
	if old.AudioScheduleSamples != 1 || old.MaxAudioScheduleLag != 5*time.Millisecond {
		t.Fatal("new event changed historical snapshot")
	}
	if got := first.Snapshot(); got.AudioScheduleSamples != 2 || got.MaxAudioScheduleLag != 40*time.Millisecond {
		t.Fatalf("new maximum lost: %+v", got)
	}
	if got := second.Snapshot(); got.AudioScheduleSamples != 0 || got.MaxAudioScheduleLag != 0 {
		t.Fatal("schedule crossed session boundary")
	}
}

// TestRunSessionScheduleObservation 验证真实客户端回调进入最终报告；不为调度耗时设上界。
func TestRunSessionScheduleObservation(t *testing.T) {
	for _, realtime := range []bool{false, true} {
		name := "unpaced"
		if realtime {
			name = "realtime"
		}
		t.Run(name, func(t *testing.T) {
			url, wait := sessionPeer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if err := readSessionInput(ctx, conn, 642, 320); err != nil {
					return err
				}
				if err := writeSessionJSON(ctx, conn, sessionResult("expected tail", true)); err != nil {
					return err
				}
				return conn.Close(websocket.StatusNormalClosure, "complete")
			})
			cfg := sessionConfig(url)
			cfg.AudioBytes, cfg.ChunkBytes, cfg.Realtime = 642, 320, realtime
			report, err := loadgen.RunSession(context.Background(), cfg)
			wait()
			assertCompletedSession(t, report, err)
			got := report.Observation
			wantSamples := int64(0)
			if realtime {
				wantSamples = 3
			}
			if got.AudioScheduleSamples != wantSamples || got.MaxAudioScheduleLag < 0 || (!realtime && got.MaxAudioScheduleLag != 0) || got.AudioBytesWritten != 642 || got.AudioChunksWritten != 3 {
				t.Fatalf("wired observation = %+v", got)
			}
		})
	}
}
