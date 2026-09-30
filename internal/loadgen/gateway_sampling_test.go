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

// samplingConfig 给出显式测试条件；不是生产默认值或实验 SLA。
func samplingConfig() loadgen.GatewaySamplingConfig {
	return loadgen.GatewaySamplingConfig{Endpoint: "http://gateway.invalid/debug/gateway", Interval: 100 * time.Millisecond, RequestTimeout: 300 * time.Millisecond}
}

// samplingResponse 返回一份合法快照；活动数由各个测试控制。
func samplingResponse(req *http.Request, active int) *http.Response {
	return &http.Response{StatusCode: 200, Status: "200 OK", Request: req,
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   io.NopCloser(strings.NewReader(fmt.Sprintf(`{"schema_version":1,"active_sessions":%d,"max_sessions":8,"stopping":false}`, active)))}
}

// TestGatewaySamplingConfig 验证合法地址及前置校验；非法配置不产生查询或样本。
func TestGatewaySamplingConfig(t *testing.T) {
	for _, endpoint := range []string{"http://localhost/debug/gateway", "https://example.test/path?q=1", "http://[::1]:8080/debug/gateway"} {
		t.Run(endpoint, func(t *testing.T) {
			cfg := samplingConfig()
			cfg.Endpoint = endpoint
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	cases := []struct {
		name   string
		mutate func(*loadgen.GatewaySamplingConfig)
	}{
		{"empty", func(c *loadgen.GatewaySamplingConfig) { c.Endpoint = "" }},
		{"relative", func(c *loadgen.GatewaySamplingConfig) { c.Endpoint = "/debug/gateway" }},
		{"scheme", func(c *loadgen.GatewaySamplingConfig) { c.Endpoint = "ws://localhost/debug/gateway" }},
		{"host", func(c *loadgen.GatewaySamplingConfig) { c.Endpoint = "http:///debug/gateway" }},
		{"syntax", func(c *loadgen.GatewaySamplingConfig) { c.Endpoint = "http://%zz/" }},
		{"zero_interval", func(c *loadgen.GatewaySamplingConfig) { c.Interval = 0 }},
		{"negative_interval", func(c *loadgen.GatewaySamplingConfig) { c.Interval = -time.Second }},
		{"zero_timeout", func(c *loadgen.GatewaySamplingConfig) { c.RequestTimeout = 0 }},
		{"negative_timeout", func(c *loadgen.GatewaySamplingConfig) { c.RequestTimeout = -time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := samplingConfig()
			tc.mutate(&cfg)
			if cfg.Validate() == nil {
				t.Fatal("accepted invalid configuration")
			}
			calls, emits := 0, 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) { calls++; return samplingResponse(req, 0), nil }}}
			err := loadgen.RunGatewaySampling(context.Background(), client, cfg, func(loadgen.GatewaySample) error { emits++; return errors.New("unexpected emission") })
			if err == nil || calls != 0 || emits != 0 {
				t.Fatalf("err=%v calls=%d emits=%d", err, calls, emits)
			}
		})
	}
}

// TestGatewaySamplingPreflight 验证空依赖和启动前已结束 context 不产生观测。
func TestGatewaySamplingPreflight(t *testing.T) {
	for _, mode := range []string{"nil_context", "nil_client", "nil_emit", "canceled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			calls, emits := 0, 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) { calls++; return samplingResponse(req, 0), nil }}}
			ctx := context.Background()
			emit := func(loadgen.GatewaySample) error { emits++; return errors.New("unexpected emission") }
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
			err := loadgen.RunGatewaySampling(ctx, client, samplingConfig(), emit)
			if err == nil || calls != 0 || emits != 0 {
				t.Fatalf("err=%v calls=%d emits=%d", err, calls, emits)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("lost parent error: %v", err)
			}
		})
	}
}

// TestGatewaySamplingTimeline 用虚拟时间验证完整节奏、失败后继续、独立状态及及时释放子 context。
func TestGatewaySamplingTimeline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := samplingConfig()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		origin := time.Now()
		failure := errors.New("temporary transport failure")
		durations := []time.Duration{40 * time.Millisecond, 230 * time.Millisecond, 300 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond}
		var requests []context.Context
		var samples []loadgen.GatewaySample
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
			resp := samplingResponse(req, i+1)
			if i == 3 {
				resp.Body = io.NopCloser(strings.NewReader(`{"schema_version":99}`))
			}
			return resp, nil
		}}
		err := loadgen.RunGatewaySampling(ctx, &http.Client{Transport: transport}, cfg, func(s loadgen.GatewaySample) error {
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
				if s.Err != nil || s.State == nil || s.State.ActiveSessions != i+1 {
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
		if samples[0].State == samples[4].State || samples[0].State.ActiveSessions != 1 {
			t.Fatal("later sample overwrote history")
		}
		samples[0].State.ActiveSessions = 77
		if samples[4].State.ActiveSessions != 5 {
			t.Fatal("sample pointers alias")
		}
	})
}

// TestGatewaySamplingParentStop 验证取消/父期限中的尝试先交付，间隔等待取消不造新样本。
func TestGatewaySamplingParentStop(t *testing.T) {
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
				var samples []loadgen.GatewaySample
				client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
					calls++
					if mode == "wait_cancel" {
						return samplingResponse(req, 0), nil
					}
					if mode == "inflight_cancel" {
						go func() { time.Sleep(50 * time.Millisecond); cancel() }()
					}
					<-req.Context().Done()
					return nil, req.Context().Err()
				}}}
				err := loadgen.RunGatewaySampling(ctx, client, samplingConfig(), func(s loadgen.GatewaySample) error {
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
					if s.State == nil || s.State.ActiveSessions != 0 || s.Err != nil {
						t.Fatalf("success corrupted: %+v", s)
					}
				} else if s.State != nil || !errors.Is(s.Err, want) || s.Duration != 50*time.Millisecond {
					t.Fatalf("inflight sample: %+v", s)
				}
			})
		})
	}
}

// samplingCancelBody 在 Fetch 的关闭响应体阶段取消父 context，模拟成功与停止相邻的边界。
type samplingCancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b samplingCancelBody) Close() error { err := b.ReadCloser.Close(); b.cancel(); return err }

// TestGatewaySamplingSuccessBeforeStop 验证已读取成功的响应不会被稍后的父取消改写为失败。
func TestGatewaySamplingSuccessBeforeStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		resp := samplingResponse(req, 2)
		resp.Body = samplingCancelBody{resp.Body, cancel}
		return resp, nil
	}}}
	var samples []loadgen.GatewaySample
	err := loadgen.RunGatewaySampling(ctx, client, samplingConfig(), func(s loadgen.GatewaySample) error {
		if ctx.Err() == nil {
			t.Error("parent not canceled before emit")
		}
		samples = append(samples, s)
		return nil
	})
	if !errors.Is(err, context.Canceled) || len(samples) != 1 {
		t.Fatalf("err=%v samples=%d", err, len(samples))
	}
	if s := samples[0]; s.State == nil || s.State.ActiveSessions != 2 || s.Err != nil {
		t.Fatalf("success corrupted: %+v", s)
	}
}

// TestGatewaySamplingEmitError 验证交付错误与查询错误分离，不重试交付，且优先于同时发生的父取消。
func TestGatewaySamplingEmitError(t *testing.T) {
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
				return samplingResponse(req, 0), nil
			}}}
			err := loadgen.RunGatewaySampling(ctx, client, samplingConfig(), func(s loadgen.GatewaySample) error {
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

// TestGatewaySamplingObservedStop 验证零活动和停止接入只是观测事实，不替调用方结束采样。
func TestGatewaySamplingObservedStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		states := []loadgen.GatewayState{
			{MaxSessions: 8},
			{ActiveSessions: 2, MaxSessions: 8, Stopping: true},
			{MaxSessions: 8, Stopping: true},
		}
		calls, emits := 0, 0
		client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			if calls >= len(states) {
				return nil, errors.New("unexpected extra observation")
			}
			state := states[calls]
			calls++
			resp := samplingResponse(req, state.ActiveSessions)
			resp.Body = io.NopCloser(strings.NewReader(fmt.Sprintf(`{"schema_version":1,"active_sessions":%d,"max_sessions":8,"stopping":%t}`, state.ActiveSessions, state.Stopping)))
			return resp, nil
		}}}
		err := loadgen.RunGatewaySampling(ctx, client, samplingConfig(), func(s loadgen.GatewaySample) error {
			if emits >= len(states) {
				return errors.New("unexpected extra sample")
			}
			if s.Err != nil || s.State == nil || *s.State != states[emits] {
				t.Errorf("sample %d: %+v", emits, s)
			}
			emits++
			if emits == len(states) {
				cancel()
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) || calls != 3 || emits != 3 {
			t.Fatalf("err=%v calls=%d emits=%d", err, calls, emits)
		}
	})
}

// TestGatewaySamplingHTTP 验证循环通过实际 HTTP 查询获得连续样本；不是负载性能测量。
func TestGatewaySamplingHTTP(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"schema_version":1,"active_sessions":%d,"max_sessions":8,"stopping":false}`, n)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg := samplingConfig()
	cfg.Endpoint = srv.URL
	cfg.Interval = time.Millisecond
	var samples []loadgen.GatewaySample
	err := loadgen.RunGatewaySampling(ctx, srv.Client(), cfg, func(s loadgen.GatewaySample) error {
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
		if s.Index != i || s.State == nil || s.State.ActiveSessions != i+1 || s.Err != nil || s.Duration < 0 || s.FinishedAt.Sub(s.StartedAt) != s.Duration {
			t.Fatalf("sample: %+v", s)
		}
		if i > 0 && s.StartedAt.Sub(samples[i-1].FinishedAt) < cfg.Interval {
			t.Fatal("next request started before interval")
		}
	}
}
