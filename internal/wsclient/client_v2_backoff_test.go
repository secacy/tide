package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestV2JitterConfig(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	const maxBase = maxDuration/3*2 + 1 // 最大满足 B+floor(B/2) <= MaxInt64 的 B。
	for _, tc := range []struct {
		name string
		cfg  V2ResumeConfig
		bad  bool
	}{
		{"default_off", V2ResumeConfig{}, false},
		{"auto_seed", V2ResumeConfig{Jitter: true}, false},
		{"seed_without_jitter", V2ResumeConfig{JitterSeed: 1}, true},
		{"largest_safe_jitter", V2ResumeConfig{Jitter: true, MaxBackoff: maxBase}, false},
		{"first_unsafe_jitter", V2ResumeConfig{Jitter: true, MaxBackoff: maxBase + 1}, true},
		{"disabled_preserves_large_duration", V2ResumeConfig{MaxBackoff: maxDuration}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewV2(V2Config{URL: "ws://test", Resume: &tc.cfg})
			if (err != nil) != tc.bad {
				t.Fatalf("NewV2 err=%v invalid=%v", err, tc.bad)
			}
			if err == nil && (c.cfg.Resume.Jitter != tc.cfg.Jitter || c.cfg.Resume.JitterSeed != tc.cfg.JitterSeed) {
				t.Fatal("normalization changed jitter configuration")
			}
		})
	}
	t.Run("copy", func(t *testing.T) {
		cfg := &V2ResumeConfig{Jitter: true, JitterSeed: 17}
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: cfg})
		cfg.Jitter, cfg.JitterSeed = false, 99
		if !c.cfg.Resume.Jitter || c.cfg.Resume.JitterSeed != 17 {
			t.Fatal("caller changed the saved configuration")
		}
	})
	t.Run("experiment_json_compatibility", func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: &V2ResumeConfig{}})
		data, err := json.Marshal(c.cfg.Resume)
		if err != nil || string(data) != `{"MaxAttempts":5,"Window":5000000000,"InitialBackoff":100000000,"MaxBackoff":1000000000}` {
			t.Fatalf("deterministic report changed: %s err=%v", data, err)
		}
		cfg := *c.cfg.Resume
		cfg.Jitter, cfg.JitterSeed = true, 17
		data, err = json.Marshal(cfg)
		var fields map[string]json.RawMessage
		if err != nil || json.Unmarshal(data, &fields) != nil || string(fields["Jitter"]) != "true" || string(fields["JitterSeed"]) != "17" {
			t.Fatalf("randomized report hides actual strategy: %s err=%v", data, err)
		}
	})
}

func TestV2JitterInterval(t *testing.T) {
	for _, tc := range []struct {
		name         string
		base, lo, hi time.Duration
	}{
		{"one_ns", 1, 1, 1},
		{"odd_ns", 3, 2, 4},
		{"initial", 100 * time.Millisecond, 50 * time.Millisecond, 150 * time.Millisecond},
		{"base_cap", time.Second, 500 * time.Millisecond, 1500 * time.Millisecond},
		{"largest_safe", 6148914691236517205, 3074457345618258603, 9223372036854775807},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, want := range []time.Duration{tc.lo, tc.base, tc.hi} {
				calls := 0
				got := v2JitterDelay(tc.base, func(n int64) int64 {
					calls++
					if n != int64(tc.hi-tc.lo+1) {
						t.Fatalf("sample bound=%d", n)
					}
					return int64(want - tc.lo)
				})
				if got != want || calls != 1 {
					t.Fatalf("got=%v want=%v calls=%d", got, want, calls)
				}
			}
		})
	}
}

// jitterRunForTest 创建真实 Run 状态但不启动 I/O；测试结束仍走实际清理。
func jitterRunForTest(t *testing.T, c *V2Client) *v2ClientRun {
	t.Helper()
	r, err := c.newV2Run(context.Background(), staticV2Source(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.cleanup(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func TestV2JitterRunOwnership(t *testing.T) {
	t.Run("disabled_sequence", func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: &V2ResumeConfig{}})
		r := jitterRunForTest(t, c)
		if r.backoffRand != nil {
			t.Fatal("disabled jitter allocated a random source")
		}
		for i, ms := range []time.Duration{100, 200, 400, 800, 1000, 1000, 1000} {
			if got := r.resumeBackoffDelay(i + 1); got != ms*time.Millisecond {
				t.Fatalf("failure=%d delay=%v", i+1, got)
			}
		}
	})
	t.Run("nil_resume", func(t *testing.T) {
		r := jitterRunForTest(t, v2TestClient(t, V2Config{URL: "ws://test"}))
		if r.backoffRand != nil {
			t.Fatal("single connection client allocated a random source")
		}
	})
	t.Run("auto_seed_ownership", func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: &V2ResumeConfig{Jitter: true}})
		a, b := jitterRunForTest(t, c), jitterRunForTest(t, c)
		if a.backoffRand == nil || b.backoffRand == nil || a.backoffRand == b.backoffRand || c.cfg.Resume.JitterSeed != 0 {
			t.Fatal("automatic random state is not owned by each Run")
		}
		// 不断言随机输出必定不同；只验证合法范围和所有权。
		for _, r := range []*v2ClientRun{a, b} {
			if d := r.resumeBackoffDelay(1); d < 50*time.Millisecond || d > 150*time.Millisecond {
				t.Fatalf("auto-seeded delay=%v", d)
			}
		}
	})
	t.Run("fixed_seed_concurrent_runs", func(t *testing.T) {
		c := v2TestClient(t, V2Config{URL: "ws://test", Resume: &V2ResumeConfig{Jitter: true, JitterSeed: 202610110101}})
		const n = 8
		var runs [n]*v2ClientRun
		var got [n][]time.Duration
		for i := range runs {
			runs[i] = jitterRunForTest(t, c)
		}
		var wg sync.WaitGroup
		for i, r := range runs {
			wg.Go(func() {
				for k := 1; k <= 16; k++ {
					got[i] = append(got[i], r.resumeBackoffDelay(k))
				}
			})
		}
		wg.Wait()
		oracle := rand.New(rand.NewPCG(202610110101, 202610110101^0x9e3779b97f4a7c15))
		for k, gotDelay := range got[0] {
			base := time.Second
			if k < 4 {
				base = (100 * time.Millisecond) << k
			}
			want := base/2 + time.Duration(oracle.Int64N(int64(base+1)))
			if gotDelay != want {
				t.Fatalf("failure=%d seeded delay=%v want=%v", k+1, gotDelay, want)
			}
		}
		for i := 1; i < n; i++ {
			if runs[i].backoffRand == runs[0].backoffRand || !slices.Equal(got[i], got[0]) {
				t.Fatal("concurrent Run changed another Run's seeded sequence")
			}
		}
	})
}

// countedJitterSource 观察实际取随机数动作；单次 Int64N 可能为拒绝采样多次取数。
type countedJitterSource struct {
	pcg   *rand.PCG
	calls int
}

func (s *countedJitterSource) Uint64() uint64 { s.calls++; return s.pcg.Uint64() }

func TestV2JitterCaptureAndCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		source := &trackedV2Source{reader: pr, close: pr.Close}
		random := &countedJitterSource{pcg: rand.NewPCG(7, 11)}
		f := newRecoveryFixture(t, V2Config{ChunkBytes: 2, Resume: &V2ResumeConfig{Jitter: true}}, source, func(f *recoveryFixture) {
			f.run.backoffRand = rand.New(random)
		})
		began := time.Now()
		f.first.reads <- v2ReadEvent{err: io.EOF}
		req := <-f.dials
		if time.Since(began) != 0 || random.calls != 0 {
			t.Fatal("first resume waited or consumed randomness")
		}
		req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
		synctest.Wait()
		until, calls := f.run.backoffUntil, random.calls
		if f.run.phase != v2Backoff || calls == 0 {
			t.Fatal("failure did not schedule random backoff")
		}
		for _, data := range []string{"aa", "bb", "cc"} {
			if _, err := pw.Write([]byte(data)); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if f.run.state.audio.nextOffset != 6 || random.calls != calls || !f.run.backoffUntil.Equal(until) {
			t.Fatal("capture stalled or resampled/extended the pending wait")
		}
		f.run.cancel()
		out := f.finish(t, context.Canceled)
		if out.result.ResumeAttempts != 1 || time.Since(began) != 0 || random.calls != calls {
			t.Fatalf("cancellation waited or started another attempt: %+v", out)
		}
		v2CheckClosed(t, source)
	})
}

func TestV2JitterBudgetLimits(t *testing.T) {
	for _, mode := range []string{"window_before_wait", "window_at_wait", "attempts"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				source := &trackedV2Source{reader: pr, close: pr.Close}
				cfg := V2ResumeConfig{Jitter: true, JitterSeed: 19, MaxAttempts: 2, InitialBackoff: 200 * time.Millisecond}
				oracle := rand.New(rand.NewPCG(19, 19^0x9e3779b97f4a7c15))
				wait := 100*time.Millisecond + time.Duration(oracle.Int64N(200000001))
				if mode == "window_before_wait" {
					cfg.Window = 50 * time.Millisecond
				} else if mode == "window_at_wait" {
					cfg.Window = wait
				}
				f := newRecoveryFixture(t, V2Config{Resume: &cfg}, source, nil)
				began := time.Now()
				f.first.reads <- v2ReadEvent{err: io.EOF}
				req := <-f.dials
				req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
				wantAttempts, elapsed := 1, cfg.Window
				if mode == "attempts" {
					req = <-f.dials
					req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
					wantAttempts, elapsed = 2, wait
				}
				out := f.finish(t, ErrV2RecoveryExhausted)
				// 退出后读取，避免测试读取与下一次握手清零该字段发生竞争。
				if mode != "attempts" && !f.run.backoffUntil.Equal(began.Add(wait)) {
					t.Fatal("scheduled wait differs from seeded sample or was clipped to window")
				}
				if out.result.ResumeAttempts != wantAttempts || time.Since(began) != elapsed || errors.Is(out.err, context.DeadlineExceeded) != (mode != "attempts") {
					t.Fatalf("out=%+v elapsed=%v", out, time.Since(began))
				}
				if f.run.backoffRand.Uint64() != oracle.Uint64() {
					t.Fatal("budget exhaustion sampled an extra wait")
				}
				select {
				case <-f.dials:
					t.Fatal("Dial exceeded the budget")
				default:
				}
				v2CheckClosed(t, source)
			})
		})
	}
}

func TestV2JitterSequenceAcrossRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		f := newRecoveryFixture(t, V2Config{Resume: &V2ResumeConfig{Jitter: true, JitterSeed: 23, MaxAttempts: 4}}, pr, nil)
		oracle := rand.New(rand.NewPCG(23, 23^0x9e3779b97f4a7c15))
		old := f.first
		for generation := uint64(2); generation <= 3; generation++ {
			began := time.Now()
			old.reads <- v2ReadEvent{err: io.EOF}
			req := <-f.dials
			if time.Since(began) != 0 {
				t.Fatal("new outage did not attempt immediately")
			}
			req.reply <- recoveryDialReply{err: io.ErrClosedPipe}
			wait := 50*time.Millisecond + time.Duration(oracle.Int64N(100000001))
			old = f.resume(t, resumedReady(generation, 0, 0, false))
			synctest.Wait()
			if time.Since(began) != wait || f.run.recovery.failures != 0 || !f.run.recovery.deadline.IsZero() {
				t.Fatal("success did not reset failure tier, or reseeded the random sequence")
			}
		}
		old.reads <- v2ReadEvent{err: io.EOF}
		out := f.finish(t, ErrV2RecoveryExhausted)
		if out.result.ResumeAttempts != 4 || out.result.ResumedConnections != 2 || f.run.backoffRand.Uint64() != oracle.Uint64() {
			t.Fatalf("success changed total budget or random state: %+v", out)
		}
	})
}
