package loadgen_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// TestWorkerSamplingEnabledStates 未启用、有效零值和等待都只是观测事实，不自动停止循环。
func TestWorkerSamplingEnabledStates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		states := []loadgen.WorkerState{
			{},
			{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 2}},
			{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 2, InUse: 1, Waiting: 5}},
			{},
		}
		calls := 0
		client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if calls >= len(states) {
				return nil, errors.New("unexpected extra request")
			}
			state := states[calls]
			calls++
			body := `{"schema_version":1,"processing_limit_enabled":false,"processing":null}`
			if state.ProcessingLimitEnabled {
				body = fmt.Sprintf(`{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":%d,"in_use":%d,"waiting":%d}}`, state.Processing.Limit, state.Processing.InUse, state.Processing.Waiting)
			}
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
		}}}
		var samples []loadgen.WorkerSample
		err := loadgen.RunWorkerSampling(ctx, client, workerSamplingConfig(), func(s loadgen.WorkerSample) error {
			i := len(samples)
			if i >= len(states) {
				return errors.New("extra emission")
			}
			if s.Index != i || s.Err != nil || s.State == nil || *s.State != states[i] {
				t.Fatalf("sample %d: %+v", i, s)
			}
			samples = append(samples, s)
			if len(samples) == len(states) {
				cancel()
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) || calls != len(states) || len(samples) != len(states) {
			t.Fatalf("err=%v calls=%d samples=%d", err, calls, len(samples))
		}
		if samples[0].State == samples[3].State {
			t.Fatal("zero states share mutable storage")
		}
		samples[0].State.ProcessingLimitEnabled = true
		if samples[3].State.ProcessingLimitEnabled {
			t.Fatal("changing historical sample changed later sample")
		}
	})
}

// workerSamplingConfig 给出显式测试条件；不是生产默认值或实验 SLA。
func workerSamplingConfig() loadgen.WorkerSamplingConfig {
	return loadgen.WorkerSamplingConfig{Endpoint: "http://worker.invalid/debug/worker", Interval: 100 * time.Millisecond, RequestTimeout: 300 * time.Millisecond}
}

// workerSamplingResponse 返回一份合法 Worker 快照；占用数由测试控制。
func workerSamplingResponse(req *http.Request, active int) *http.Response {
	return &http.Response{StatusCode: 200, Status: "200 OK", Request: req,
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   io.NopCloser(strings.NewReader(fmt.Sprintf(`{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":8,"in_use":%d,"waiting":0}}`, active)))}
}

// TestWorkerSamplingConfig 验证合法地址及前置校验；非法配置不产生查询或样本。
func TestWorkerSamplingConfig(t *testing.T) {
	for _, endpoint := range []string{"http://localhost/debug/worker", "https://example.test/path?q=1", "http://[::1]:8080/debug/worker"} {
		t.Run(endpoint, func(t *testing.T) {
			cfg := workerSamplingConfig()
			cfg.Endpoint = endpoint
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	cases := []struct {
		name   string
		mutate func(*loadgen.WorkerSamplingConfig)
	}{
		{"empty", func(c *loadgen.WorkerSamplingConfig) { c.Endpoint = "" }},
		{"relative", func(c *loadgen.WorkerSamplingConfig) { c.Endpoint = "/debug/worker" }},
		{"scheme", func(c *loadgen.WorkerSamplingConfig) { c.Endpoint = "ws://localhost/debug/worker" }},
		{"host", func(c *loadgen.WorkerSamplingConfig) { c.Endpoint = "http:///debug/worker" }},
		{"syntax", func(c *loadgen.WorkerSamplingConfig) { c.Endpoint = "http://%zz/" }},
		{"zero_interval", func(c *loadgen.WorkerSamplingConfig) { c.Interval = 0 }},
		{"negative_interval", func(c *loadgen.WorkerSamplingConfig) { c.Interval = -time.Second }},
		{"zero_timeout", func(c *loadgen.WorkerSamplingConfig) { c.RequestTimeout = 0 }},
		{"negative_timeout", func(c *loadgen.WorkerSamplingConfig) { c.RequestTimeout = -time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := workerSamplingConfig()
			tc.mutate(&cfg)
			if cfg.Validate() == nil {
				t.Fatal("accepted invalid configuration")
			}
			calls, emits := 0, 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) { calls++; return workerSamplingResponse(req, 0), nil }}}
			err := loadgen.RunWorkerSampling(context.Background(), client, cfg, func(loadgen.WorkerSample) error { emits++; return errors.New("unexpected emission") })
			if err == nil || calls != 0 || emits != 0 {
				t.Fatalf("err=%v calls=%d emits=%d", err, calls, emits)
			}
		})
	}
}

// TestWorkerSamplingPreflight 验证空依赖和启动前已结束 context 不产生观测。
func TestWorkerSamplingPreflight(t *testing.T) {
	for _, mode := range []string{"nil_context", "nil_client", "nil_emit", "canceled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			calls, emits := 0, 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) { calls++; return workerSamplingResponse(req, 0), nil }}}
			ctx := context.Background()
			emit := func(loadgen.WorkerSample) error { emits++; return errors.New("unexpected emission") }
			var want error
			switch mode {
			case "nil_context":
				ctx = nil
			case "nil_client":
				client = nil
			case "nil_emit":
				emit = nil
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "expired":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				want = context.DeadlineExceeded
			}
			err := loadgen.RunWorkerSampling(ctx, client, workerSamplingConfig(), emit)
			if err == nil || calls != 0 || emits != 0 {
				t.Fatalf("err=%v calls=%d emits=%d", err, calls, emits)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("lost parent error: %v", err)
			}
		})
	}
}

// TestWorkerSamplingTimeline 用虚拟时间验证完整节奏、失败后继续、独立状态及及时释放子 context。
func TestWorkerSamplingTimeline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := workerSamplingConfig()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		origin := time.Now()
		failure := errors.New("temporary transport failure")
		durations := []time.Duration{40 * time.Millisecond, 230 * time.Millisecond, 300 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond}
		var requests []context.Context
		var samples []loadgen.WorkerSample
		var active atomic.Int64
		transport := &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if active.Add(1) != 1 {
				t.Error("overlapping queries")
			}
			defer active.Add(-1)
			i := len(requests)
			requests = append(requests, req.Context())
			if i >= len(durations) {
				return nil, errors.New("unexpected extra query")
			}
			if i == 2 {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			time.Sleep(durations[i])
			if i == 1 {
				return nil, failure
			}
			resp := workerSamplingResponse(req, i+1)
			if i == 3 {
				resp.Body = io.NopCloser(strings.NewReader(`{"schema_version":99}`))
			}
			return resp, nil
		}}
		err := loadgen.RunWorkerSampling(ctx, &http.Client{Transport: transport}, cfg, func(s loadgen.WorkerSample) error {
			i := len(samples)
			if i >= len(durations) {
				return errors.New("too many samples")
			}
			wantStart := origin
			if i > 0 {
				wantStart = samples[i-1].FinishedAt.Add(20*time.Millisecond + cfg.Interval)
			}
			if s.Index != i || !s.StartedAt.Equal(wantStart) || s.Duration != durations[i] || s.FinishedAt.Sub(s.StartedAt) != s.Duration {
				t.Errorf("sample %d: %+v want start=%v duration=%v", i, s, wantStart, durations[i])
			}
			if requests[i].Err() == nil {
				t.Error("request context still live at emit")
			}
			if i == 0 || i == 4 {
				if s.Err != nil || s.State == nil || s.State.Processing.InUse != i+1 {
					t.Errorf("success sample: %+v", s)
				}
			} else if s.Err == nil || s.State != nil {
				t.Errorf("failure sample: %+v", s)
			}
			if i == 1 && !errors.Is(s.Err, failure) {
				t.Errorf("lost query error: %v", s.Err)
			}
			if i == 2 && !errors.Is(s.Err, context.DeadlineExceeded) {
				t.Errorf("lost child deadline: %v", s.Err)
			}
			samples = append(samples, s)
			time.Sleep(20 * time.Millisecond) // 交付耗时也必须先完成，之后才等待 Interval。
			if i == 4 {
				cancel()
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) || len(samples) != 5 || len(requests) != 5 || active.Load() != 0 || transport.closed != 0 {
			t.Fatalf("err=%v samples=%d requests=%d active=%d", err, len(samples), len(requests), active.Load())
		}
		if samples[0].State == nil || samples[4].State == nil {
			t.Fatal("missing success states")
		}
		if samples[0].State == samples[4].State || samples[0].State.Processing.InUse != 1 {
			t.Fatal("later sample overwrote history")
		}
		samples[0].State.Processing.InUse = 77
		if samples[4].State.Processing.InUse != 5 {
			t.Fatal("sample pointers alias")
		}
	})
}

// TestWorkerSamplingParentStop 验证取消/父期限中的尝试先交付，间隔等待取消不造新样本。
func TestWorkerSamplingParentStop(t *testing.T) {
	for _, mode := range []string{"inflight_cancel", "inflight_deadline", "wait_cancel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				if mode == "inflight_deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				}
				defer cancel()
				calls := 0
				var samples []loadgen.WorkerSample
				client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
					calls++
					if mode == "wait_cancel" {
						return workerSamplingResponse(req, 0), nil
					}
					if mode == "inflight_cancel" {
						go func() { time.Sleep(50 * time.Millisecond); cancel() }()
					}
					<-req.Context().Done()
					return nil, req.Context().Err()
				}}}
				err := loadgen.RunWorkerSampling(ctx, client, workerSamplingConfig(), func(s loadgen.WorkerSample) error {
					samples = append(samples, s)
					if mode == "wait_cancel" {
						go func() { time.Sleep(50 * time.Millisecond); cancel() }()
					} else if ctx.Err() == nil {
						t.Error("sample emitted before parent ended")
					}
					return nil
				})
				want := context.Canceled
				if mode == "inflight_deadline" {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) || calls != 1 || len(samples) != 1 {
					t.Fatalf("err=%v calls=%d samples=%d", err, calls, len(samples))
				}
				s := samples[0]
				if mode == "wait_cancel" {
					if s.State == nil || s.State.Processing.InUse != 0 || s.Err != nil {
						t.Fatalf("success corrupted: %+v", s)
					}
				} else if s.State != nil || !errors.Is(s.Err, want) || s.Duration != 50*time.Millisecond {
					t.Fatalf("inflight sample: %+v", s)
				}
			})
		})
	}
}

// TestWorkerSamplingSuccessBeforeStop 验证已读取成功的响应不会被稍后的父取消改写为失败。
func TestWorkerSamplingSuccessBeforeStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		resp := workerSamplingResponse(req, 2)
		resp.Body = samplingCancelBody{resp.Body, cancel}
		return resp, nil
	}}}
	var samples []loadgen.WorkerSample
	err := loadgen.RunWorkerSampling(ctx, client, workerSamplingConfig(), func(s loadgen.WorkerSample) error {
		if ctx.Err() == nil {
			t.Error("parent not canceled before emit")
		}
		samples = append(samples, s)
		return nil
	})
	if !errors.Is(err, context.Canceled) || len(samples) != 1 {
		t.Fatalf("err=%v samples=%d", err, len(samples))
	}
	if s := samples[0]; s.State == nil || s.State.Processing.InUse != 2 || s.Err != nil {
		t.Fatalf("success corrupted: %+v", s)
	}
}

// TestWorkerSamplingEmitError 验证交付错误与查询错误分离，不重试交付，且优先于同时发生的父取消。
func TestWorkerSamplingEmitError(t *testing.T) {
	for _, mode := range []string{"success", "query_failure", "parent_cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			deliveryErr, queryErr := errors.New("delivery failed"), errors.New("query failed")
			calls, emits := 0, 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
				calls++
				if mode == "query_failure" {
					return nil, queryErr
				}
				return workerSamplingResponse(req, 0), nil
			}}}
			err := loadgen.RunWorkerSampling(ctx, client, workerSamplingConfig(), func(s loadgen.WorkerSample) error {
				emits++
				if mode == "query_failure" && (s.State != nil || !errors.Is(s.Err, queryErr)) {
					t.Errorf("query fact lost: %+v", s)
				}
				if mode == "parent_cancel" {
					cancel()
				}
				return deliveryErr
			})
			if !errors.Is(err, deliveryErr) || errors.Is(err, context.Canceled) || errors.Is(err, queryErr) || calls != 1 || emits != 1 || !strings.Contains(err.Error(), "0") {
				t.Fatalf("err=%v calls=%d emits=%d", err, calls, emits)
			}
		})
	}
}

// TestWorkerSamplingHTTP 验证循环通过实际 HTTP 查询获得连续样本；不是负载性能测量。
func TestWorkerSamplingHTTP(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":8,"in_use":%d,"waiting":0}}`, n)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg := workerSamplingConfig()
	cfg.Endpoint = srv.URL
	cfg.Interval = time.Millisecond
	var samples []loadgen.WorkerSample
	err := loadgen.RunWorkerSampling(ctx, srv.Client(), cfg, func(s loadgen.WorkerSample) error {
		samples = append(samples, s)
		if len(samples) == 3 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || requests.Load() != 3 || len(samples) != 3 {
		t.Fatalf("err=%v requests=%d samples=%d", err, requests.Load(), len(samples))
	}
	for i, s := range samples {
		if s.Index != i || s.State == nil || s.State.Processing.InUse != i+1 || s.Err != nil || s.Duration < 0 || s.FinishedAt.Sub(s.StartedAt) != s.Duration {
			t.Fatalf("sample: %+v", s)
		}
		if i > 0 && s.StartedAt.Sub(samples[i-1].FinishedAt) < cfg.Interval {
			t.Fatal("next request started before interval")
		}
	}
}
