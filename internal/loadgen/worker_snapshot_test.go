package loadgen_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// workerProbeJSON 使用等待数超过名额上限且未满占用的合法状态，避免额外推导瞬时关系。
const workerProbeJSON = `{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":2,"in_use":1,"waiting":5}}`

// TestFetchWorkerSnapshotDecodeCause 保留 JSON 原始错误类型，便于调用方区分协议错误原因。
func TestFetchWorkerSnapshotDecodeCause(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"syntax", "{"},
		{"nested_type", `{"schema_version":1,"processing_limit_enabled":true,"processing":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := workerProbeFetch(t, 200, "application/json", &probeBody{reader: strings.NewReader(tc.body)})
			var syntax *json.SyntaxError
			var kind *json.UnmarshalTypeError
			if tc.name == "syntax" && !errors.As(err, &syntax) || tc.name == "nested_type" && !errors.As(err, &kind) {
				t.Fatalf("lost JSON error type: %v", err)
			}
		})
	}
}

// TestFetchWorkerSnapshotAlreadyCanceled 已取消的真实 HTTP 请求不应产生服务端观测。
func TestFetchWorkerSnapshotAlreadyCanceled(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, workerProbeJSON)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := loadgen.FetchWorkerSnapshot(ctx, srv.Client(), srv.URL)
	if !errors.Is(err, context.Canceled) || got != (loadgen.WorkerState{}) || calls.Load() != 0 {
		t.Fatalf("state=%+v error=%v server calls=%d", got, err, calls.Load())
	}
}

// workerProbeFetch 复用 HTTP 读写探针；假 Content-Length 不能替代真实读取上界。
func workerProbeFetch(t *testing.T, code int, mediaType string, body *probeBody) (loadgen.WorkerState, error) {
	t.Helper()
	calls := 0
	transport := &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodGet || req.URL.String() != "http://worker.invalid/debug/worker?instance=a" {
			t.Errorf("request changed: %s %s", req.Method, req.URL)
		}
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{mediaType}}, Body: body, ContentLength: 1, Request: req}, nil
	}}
	state, err := loadgen.FetchWorkerSnapshot(context.Background(), &http.Client{Transport: transport}, "http://worker.invalid/debug/worker?instance=a")
	if calls != 1 || body.closed != 1 || transport.closed != 0 {
		t.Fatalf("calls=%d body closed=%d pool closed=%d", calls, body.closed, transport.closed)
	}
	if err != nil && state != (loadgen.WorkerState{}) {
		t.Fatalf("partial state on failure: %+v %v", state, err)
	}
	return state, err
}

// TestFetchWorkerSnapshotValues 核对未采集、有效零值、忙碌和未知字段兼容，均通过公开查询入口。
func TestFetchWorkerSnapshotValues(t *testing.T) {
	maximum := int(^uint(0) >> 1)
	for _, tc := range []struct {
		name, body string
		want       loadgen.WorkerState
	}{
		{"disabled", `{"schema_version":1,"processing_limit_enabled":false,"processing":null}`, loadgen.WorkerState{}},
		{"disabled_whitespace", `{"schema_version":1,"processing_limit_enabled":false,"processing":
 null 	 }`, loadgen.WorkerState{}},
		{"idle", `{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":2,"in_use":0,"waiting":0}}`, loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 2}}},
		{"busy_waiting", workerProbeJSON, loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 2, InUse: 1, Waiting: 5}}},
		{"full", `{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":2,"in_use":2,"waiting":0}}`, loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 2, InUse: 2}}},
		{"unknown_fields", `{"schema_version":1,"future":true,"processing_limit_enabled":true,"processing":{"limit":2,"in_use":1,"waiting":5,"future":null}}`, loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 2, InUse: 1, Waiting: 5}}},
		{"integer_boundary", fmt.Sprintf(`{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":%d,"in_use":%d,"waiting":%d}}`, maximum, maximum, maximum), loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: maximum, InUse: maximum, Waiting: maximum}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &probeBody{reader: strings.NewReader(tc.body)}
			got, err := workerProbeFetch(t, 200, "application/json; charset=utf-8", body)
			if err != nil || got != tc.want {
				t.Fatalf("got=%+v err=%v want=%+v", got, err, tc.want)
			}
			if body.read != len(tc.body) {
				t.Fatalf("read=%d want=%d", body.read, len(tc.body))
			}
		})
	}
}

// TestFetchWorkerSnapshotInvalidJSON 检查缺失、显式 null、错误类型和矛盾状态不会成为有效观测。
func TestFetchWorkerSnapshotInvalidJSON(t *testing.T) {
	cases := []struct{ name, body string }{
		{"empty", ""}, {"syntax", "{"}, {"top_null", "null"}, {"top_array", "[]"}, {"trailing", workerProbeJSON + "{}"},
		{"disabled_missing", `{"schema_version":1,"processing_limit_enabled":false}`},
		{"disabled_object", `{"schema_version":1,"processing_limit_enabled":false,"processing":{"limit":0,"in_use":0,"waiting":0}}`},
		{"disabled_array", `{"schema_version":1,"processing_limit_enabled":false,"processing":[]}`},
		{"disabled_string_null", `{"schema_version":1,"processing_limit_enabled":false,"processing":"null"}`},
	}
	add := func(name, field, value string, nested bool) {
		var root map[string]json.RawMessage
		if err := json.Unmarshal([]byte(workerProbeJSON), &root); err != nil {
			t.Fatal(err)
		}
		target := root
		// 新建嵌套 map，避免修改顶层对象。
		if nested {
			target = make(map[string]json.RawMessage)
			if err := json.Unmarshal(root["processing"], &target); err != nil {
				t.Fatal(err)
			}
		}
		if value == "" {
			delete(target, field)
		} else {
			target[field] = json.RawMessage(value)
		}
		if nested {
			data, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			root["processing"] = data
		}
		data, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, struct{ name, body string }{name, string(data)})
	}
	for _, field := range []string{"schema_version", "processing_limit_enabled", "processing"} {
		add("missing_"+field, field, "", false)
		add("null_"+field, field, "null", false)
	}
	for _, field := range []string{"limit", "in_use", "waiting"} {
		for _, v := range []struct{ name, value string }{{"missing", ""}, {"null", "null"}, {"string", `"1"`}, {"float", "1.0"}, {"bool", "false"}, {"overflow", "99999999999999999999999999"}} {
			add(field+"_"+v.name, field, v.value, true)
		}
	}
	for _, v := range []struct{ name, field, value string }{
		{"future_version", "schema_version", "2"}, {"zero_version", "schema_version", "0"}, {"version_string", "schema_version", `"1"`},
		{"enabled_string", "processing_limit_enabled", `"false"`}, {"enabled_number", "processing_limit_enabled", "0"},
		{"processing_array", "processing", "[]"}, {"processing_number", "processing", "1"}, {"processing_string", "processing", `"object"`},
	} {
		add(v.name, v.field, v.value, false)
	}
	for _, v := range []struct{ name, field, value string }{{"zero_limit", "limit", "0"}, {"negative_limit", "limit", "-1"}, {"negative_use", "in_use", "-1"}, {"above_limit", "in_use", "3"}, {"negative_waiting", "waiting", "-1"}} {
		add(v.name, v.field, v.value, true)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := workerProbeFetch(t, 200, "application/json", &probeBody{reader: strings.NewReader(tc.body)})
			if err == nil {
				t.Fatalf("accepted invalid response: %s", tc.body)
			}
		})
	}
}

// TestFetchWorkerSnapshotHTTPBounds 验证协议拒绝、恰好 4KiB 与超限时的读取上界。
func TestFetchWorkerSnapshotHTTPBounds(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		contentType string
	}{
		{"unavailable", 503, "application/json"}, {"redirect", 302, "application/json"},
		{"no_content", 204, "application/json"}, {"missing_type", 200, ""},
		{"wrong_type", 200, "text/plain"}, {"invalid_type", 200, "application/json; broken"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &probeBody{reader: strings.NewReader(workerProbeJSON)}
			_, err := workerProbeFetch(t, tc.status, tc.contentType, body)
			if err == nil || body.read != 0 {
				t.Fatalf("err=%v body read=%d", err, body.read)
			}
			if tc.status != 200 && !strings.Contains(err.Error(), fmt.Sprint(tc.status)) {
				t.Fatalf("missing HTTP status: %v", err)
			}
		})
	}
	for _, size := range []int{4096, 4097, 100000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			body := &probeBody{reader: strings.NewReader(workerProbeJSON + strings.Repeat(" ", size-len(workerProbeJSON)))}
			_, err := workerProbeFetch(t, 200, "application/json", body)
			if (err == nil) != (size == 4096) || body.read != min(size, 4097) {
				t.Fatalf("size=%d err=%v read=%d", size, err, body.read)
			}
		})
	}
}

// TestFetchWorkerSnapshotErrors 验证参数/传输/读取错误和原始错误链。
func TestFetchWorkerSnapshotErrors(t *testing.T) {
	for _, mode := range []string{"nil_context", "nil_client", "invalid_url", "transport", "body_read"} {
		t.Run(mode, func(t *testing.T) {
			sentinel := errors.New("probe I/O failed")
			calls := 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(*http.Request) (*http.Response, error) { calls++; return nil, sentinel }}}
			ctx := context.Background()
			endpoint := "http://worker.invalid/debug/worker"
			switch mode {
			case "nil_context":
				ctx = nil
			case "nil_client":
				client = nil
			case "invalid_url":
				endpoint = "://bad"
			}
			var state loadgen.WorkerState
			var err error
			if mode == "body_read" {
				state, err = workerProbeFetch(t, 200, "application/json", &probeBody{reader: probeErrorReader{sentinel}})
			} else {
				state, err = loadgen.FetchWorkerSnapshot(ctx, client, endpoint)
			}
			if err == nil || state != (loadgen.WorkerState{}) {
				t.Fatalf("state=%+v err=%v", state, err)
			}
			if mode == "transport" || mode == "body_read" {
				if !errors.Is(err, sentinel) {
					t.Fatalf("lost error chain: %v", err)
				}
			} else if calls != 0 {
				t.Fatalf("invalid input made %d requests", calls)
			}
			if mode == "transport" && calls != 1 {
				t.Fatalf("transport attempts=%d", calls)
			}
		})
	}
}

// TestFetchWorkerSnapshotRedirect 验证重定向不跟随且原 client 的策略/Timeout/Transport 保持。
func TestFetchWorkerSnapshotRedirect(t *testing.T) {
	var targetCalls, redirectCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, workerProbeJSON)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer origin.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { redirectCalls.Add(1); return nil }}
	state, err := loadgen.FetchWorkerSnapshot(context.Background(), client, origin.URL)
	if err == nil || state != (loadgen.WorkerState{}) || targetCalls.Load() != 0 || redirectCalls.Load() != 0 {
		t.Fatalf("state=%+v err=%v target=%d policy=%d", state, err, targetCalls.Load(), redirectCalls.Load())
	}
	if client.Transport != transport || client.Timeout != 2*time.Second {
		t.Fatal("caller client changed")
	}
	// 同一个原客户端仍按原策略跟随重定向。
	resp, err := client.Get(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	if targetCalls.Load() != 1 || redirectCalls.Load() != 1 {
		t.Fatal("caller redirect policy changed")
	}
}

// TestFetchWorkerSnapshotConnectionReuse 验证正常响应读完并关闭后复用连接。
func TestFetchWorkerSnapshotConnectionReuse(t *testing.T) {
	var connections, requests atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, workerProbeJSON)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			connections.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	for range 2 {
		if _, err := loadgen.FetchWorkerSnapshot(context.Background(), client, srv.URL); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 2 || connections.Load() != 1 {
		t.Fatalf("requests=%d connections=%d", requests.Load(), connections.Load())
	}
}

// TestFetchWorkerSnapshotCancellation 验证取消与期限同时覆盖等待响应头及读取响应体。
func TestFetchWorkerSnapshotCancellation(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		for _, mode := range []string{"cancel", "deadline"} {
			t.Run(phase+"_"+mode, func(t *testing.T) {
				entered := make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if phase == "body" {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(200)
						_, _ = io.WriteString(w, `{"schema_version":`)
						w.(http.Flusher).Flush()
					}
					close(entered)
					<-r.Context().Done()
				}))
				defer srv.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if mode == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
				}
				defer cancel()
				client := srv.Client()
				client.Timeout = 2 * time.Second
				type result struct {
					state loadgen.WorkerState
					err   error
				}
				done := make(chan result, 1)
				go func() { state, err := loadgen.FetchWorkerSnapshot(ctx, client, srv.URL); done <- result{state, err} }()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("request did not reach server")
				}
				want := context.DeadlineExceeded
				if mode == "cancel" {
					want = context.Canceled
					cancel()
				}
				select {
				case got := <-done:
					if got.state != (loadgen.WorkerState{}) || !errors.Is(got.err, want) {
						t.Fatalf("state=%+v err=%v want=%v", got.state, got.err, want)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("query did not exit")
				}
			})
		}
	}
}
