package gateway

import (
	"errors"
	"math"
	"testing"
	"time"
)

// resumeTestTime 提供固定起点，由测试指定各事件的处理时间，不等待真实时钟。
func resumeTestTime() time.Time {
	return time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
}

func newTestResumeState(t *testing.T) *resumeState {
	t.Helper()
	s, err := newResumeState(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func assertResumeState(t *testing.T, s *resumeState, phase resumePhase, generation uint64, deadline time.Time) {
	t.Helper()
	if s.phase != phase || s.generation != generation || !s.expiresAt.Equal(deadline) {
		t.Fatalf("state = (phase=%d generation=%d expires=%v), want (%d %d %v)",
			s.phase, s.generation, s.expiresAt, phase, generation, deadline)
	}
	if s.window != 10*time.Second {
		t.Fatalf("window changed to %v", s.window)
	}
}

func TestResumeStateConstruction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
	}{
		{"zero", 0},
		{"negative", -time.Nanosecond},
		{"minimum_duration", time.Duration(math.MinInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := newResumeState(tc.window)
			if err == nil || s != nil {
				t.Fatalf("newResumeState(%v) = (%v, %v), want nil and error", tc.window, s, err)
			}
		})
	}
	t.Run("initial_attachment", func(t *testing.T) {
		s := newTestResumeState(t)
		assertResumeState(t, s, resumeAttached, 1, time.Time{})
	})
	t.Run("smallest_positive_window", func(t *testing.T) {
		s, err := newResumeState(time.Nanosecond)
		if err != nil {
			t.Fatal(err)
		}
		now := resumeTestTime()
		if !s.detach(1, now) || !s.expire(now.Add(time.Nanosecond)) {
			t.Fatal("one-nanosecond window must expire at its deadline")
		}
	})
}

func TestResumeStateDetachDoesNotRenewWindow(t *testing.T) {
	s := newTestResumeState(t)
	now := resumeTestTime()
	for _, generation := range []uint64{0, 2, math.MaxUint64} {
		if s.detach(generation, now) {
			t.Fatalf("unexpected detach accepted for generation %d", generation)
		}
		assertResumeState(t, s, resumeAttached, 1, time.Time{})
	}
	if !s.detach(1, now) {
		t.Fatal("current connection must detach")
	}
	deadline := now.Add(10 * time.Second)
	assertResumeState(t, s, resumeDetached, 1, deadline)
	for _, offset := range []time.Duration{time.Second, 9 * time.Second, 11 * time.Second} {
		if s.detach(1, now.Add(offset)) {
			t.Fatal("duplicate disconnect must not renew the window")
		}
		assertResumeState(t, s, resumeDetached, 1, deadline)
	}
	if !s.expire(now.Add(11 * time.Second)) {
		t.Fatal("duplicate events must leave the original deadline effective")
	}
}

func TestResumeStateResumeDeadline(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset time.Duration
		valid  bool
	}{
		{"immediately", 0, true},
		{"one_nanosecond_before", 10*time.Second - time.Nanosecond, true},
		{"exactly_at_deadline", 10 * time.Second, false},
		{"one_nanosecond_after", 10*time.Second + time.Nanosecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestResumeState(t)
			now := resumeTestTime()
			s.detach(1, now)
			generation, err := s.resume(now.Add(tc.offset))
			if tc.valid {
				if err != nil || generation != 2 {
					t.Fatalf("resume = (%d, %v), want (2, nil)", generation, err)
				}
				assertResumeState(t, s, resumeAttached, 2, time.Time{})
				return
			}
			if generation != 0 || !errors.Is(err, errResumeExpired) {
				t.Fatalf("resume = (%d, %v), want (0, expired)", generation, err)
			}
			assertResumeState(t, s, resumeClosed, 1, time.Time{})
			if generation, err = s.resume(now.Add(tc.offset)); generation != 0 || !errors.Is(err, errResumeClosed) {
				t.Fatalf("second resume = (%d, %v), want (0, closed)", generation, err)
			}
		})
	}
}

func TestResumeStateExpireDeadline(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset time.Duration
		want   bool
	}{
		{"before_deadline", 10*time.Second - time.Nanosecond, false},
		{"at_deadline", 10 * time.Second, true},
		{"late_timer", time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestResumeState(t)
			now := resumeTestTime()
			s.detach(1, now)
			if got := s.expire(now.Add(tc.offset)); got != tc.want {
				t.Fatalf("expire = %v, want %v", got, tc.want)
			}
			if !tc.want {
				assertResumeState(t, s, resumeDetached, 1, now.Add(10*time.Second))
				return
			}
			assertResumeState(t, s, resumeClosed, 1, time.Time{})
			if s.expire(now.Add(time.Hour)) {
				t.Fatal("expiration must only win once")
			}
			if generation, err := s.resume(now.Add(tc.offset)); generation != 0 || !errors.Is(err, errResumeClosed) {
				t.Fatalf("resume after timer = (%d, %v), want (0, closed)", generation, err)
			}
		})
	}
}

func TestResumeStateOldConnectionAndTimerCannotCloseNewAttachment(t *testing.T) {
	s := newTestResumeState(t)
	now := resumeTestTime()
	s.detach(1, now)
	if generation, err := s.resume(now.Add(time.Second)); generation != 2 || err != nil {
		t.Fatalf("resume = (%d, %v)", generation, err)
	}
	if s.detach(1, now.Add(2*time.Second)) || s.expire(now.Add(10*time.Second)) {
		t.Fatal("old disconnect or timer affected new attachment")
	}
	assertResumeState(t, s, resumeAttached, 2, time.Time{})
	if generation, err := s.resume(now.Add(11 * time.Second)); generation != 0 || !errors.Is(err, errResumeAlreadyAttached) {
		t.Fatalf("conflicting resume = (%d, %v), want already attached", generation, err)
	}
	assertResumeState(t, s, resumeAttached, 2, time.Time{})

	// 新断开有自己的截止时间；迟到的旧定时器必须依据当前期限判断。
	if !s.detach(2, now.Add(12*time.Second)) {
		t.Fatal("new connection must detach")
	}
	if s.detach(1, now.Add(13*time.Second)) || s.expire(now.Add(14*time.Second)) {
		t.Fatal("stale events affected the new recovery window")
	}
	assertResumeState(t, s, resumeDetached, 2, now.Add(22*time.Second))
	if generation, err := s.resume(now.Add(15 * time.Second)); generation != 3 || err != nil {
		t.Fatalf("second recovery = (%d, %v), want (3, nil)", generation, err)
	}
	assertResumeState(t, s, resumeAttached, 3, time.Time{})
}

func TestResumeStateCloseIsPermanent(t *testing.T) {
	for _, initial := range []string{"attached", "detached", "expired"} {
		t.Run(initial, func(t *testing.T) {
			s := newTestResumeState(t)
			now := resumeTestTime()
			if initial != "attached" {
				s.detach(1, now)
			}
			if initial == "expired" {
				s.expire(now.Add(10 * time.Second))
			}
			s.close()
			s.close()
			if s.detach(1, now.Add(time.Hour)) || s.expire(now.Add(time.Hour)) {
				t.Fatal("closed state must not detach or expire again")
			}
			if generation, err := s.resume(now.Add(time.Hour)); generation != 0 || !errors.Is(err, errResumeClosed) {
				t.Fatalf("resume closed = (%d, %v)", generation, err)
			}
			assertResumeState(t, s, resumeClosed, 1, time.Time{})
		})
	}
}

func TestResumeStateGenerationExhaustion(t *testing.T) {
	s := newTestResumeState(t)
	now := resumeTestTime()
	// 直接设置极值，以检查最后一次合法恢复及随后溢出的边界。
	s.generation = math.MaxUint64 - 1
	s.detach(math.MaxUint64-1, now)
	if generation, err := s.resume(now.Add(time.Second)); generation != math.MaxUint64 || err != nil {
		t.Fatalf("last legal generation = (%d, %v)", generation, err)
	}
	if !s.detach(math.MaxUint64, now.Add(2*time.Second)) {
		t.Fatal("last generation must still be able to detach")
	}
	deadline := now.Add(12 * time.Second)
	for _, offset := range []time.Duration{3 * time.Second, 4 * time.Second} {
		if generation, err := s.resume(now.Add(offset)); generation != 0 || !errors.Is(err, errResumeGenerationExhausted) {
			t.Fatalf("exhausted resume = (%d, %v)", generation, err)
		}
		assertResumeState(t, s, resumeDetached, math.MaxUint64, deadline)
	}
	if generation, err := s.resume(deadline); generation != 0 || !errors.Is(err, errResumeExpired) {
		t.Fatalf("exhausted state at deadline = (%d, %v), want expired", generation, err)
	}
	assertResumeState(t, s, resumeClosed, math.MaxUint64, time.Time{})
}

func TestResumeStateInstancesAreIndependent(t *testing.T) {
	a, b := newTestResumeState(t), newTestResumeState(t)
	now := resumeTestTime()
	a.detach(1, now)
	a.close()
	assertResumeState(t, b, resumeAttached, 1, time.Time{})
	if !b.detach(1, now) {
		t.Fatal("closing a different session must not block detach")
	}
	if generation, err := b.resume(now.Add(time.Second)); generation != 2 || err != nil {
		t.Fatalf("independent resume = (%d, %v)", generation, err)
	}
	assertResumeState(t, a, resumeClosed, 1, time.Time{})
}
