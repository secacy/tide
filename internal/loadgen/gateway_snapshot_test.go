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

const probeValidJSON = `{"schema_version":1,"active_sessions":2,"max_sessions":3,"stopping":false}`

// probeBody 记录读取量与关闭次数，验证拒绝路径不会无限读取或遗漏关闭。
type probeBody struct {
	reader       io.Reader
	read, closed int
}

func (b *probeBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}
func (b *probeBody) Close() error { b.closed++; return nil }

// probeTransport 为每次请求提供响应，并观察连接池是否被查询函数关闭。
type probeTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
	closed    int
}

func (p *probeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return p.roundTrip(r) }
func (p *probeTransport) CloseIdleConnections()                             { p.closed++ }

// probeFetch 使用内存响应验证客户端语义；真实网络的期限与重定向另行测试。
func probeFetch(t *testing.T, status int, contentType string, body *probeBody) (loadgen.GatewayState, error) {
	t.Helper()
	calls := 0
	transport := &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodGet || req.URL.String() != "http://gateway.invalid/debug/gateway" {
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
		}
		return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)),
			Header: http.Header{"Content-Type": []string{contentType}}, Body: body, Request: req}, nil
	}}
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state, err := loadgen.FetchGatewaySnapshot(ctx, client, "http://gateway.invalid/debug/gateway")
	if calls != 1 || body.closed != 1 || transport.closed != 0 {
		t.Fatalf("calls=%d body closes=%d pool closes=%d", calls, body.closed, transport.closed)
	}
	if err != nil && state != (loadgen.GatewayState{}) {
		t.Fatalf("failure returned state: %+v err=%v", state, err)
	}
	return state, err
}

// TestFetchGatewaySnapshotValues 验证合法空闲、忙碌和停止状态，及可忽略的附加字段。
func TestFetchGatewaySnapshotValues(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		want                    loadgen.GatewayState
	}{
		{"idle", `{"schema_version":1,"active_sessions":0,"max_sessions":64,"stopping":false}`, "application/json", loadgen.GatewayState{MaxSessions: 64}},
		{"busy", probeValidJSON, "application/json", loadgen.GatewayState{ActiveSessions: 2, MaxSessions: 3}},
		{"full_stopping", `{"schema_version":1,"active_sessions":3,"max_sessions":3,"stopping":true}`, "application/json", loadgen.GatewayState{ActiveSessions: 3, MaxSessions: 3, Stopping: true}},
		{"empty_stopping", `{"schema_version":1,"active_sessions":0,"max_sessions":3,"stopping":true}`, "application/json", loadgen.GatewayState{MaxSessions: 3, Stopping: true}},
		{"extra_and_charset", `{"schema_version":1,"active_sessions":2,"max_sessions":3,"stopping":false,"future":{"value":7}}`, "application/json; charset=utf-8", loadgen.GatewayState{ActiveSessions: 2, MaxSessions: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &probeBody{reader: strings.NewReader(tc.body)}
			got, err := probeFetch(t, 200, tc.contentType, body)
			if err != nil || got != tc.want {
				t.Fatalf("state=%+v err=%v want=%+v", got, err, tc.want)
			}
			if body.read != len(tc.body) {
				t.Fatalf("read=%d want=%d", body.read, len(tc.body))
			}
		})
	}
}

// TestFetchGatewaySnapshotInvalidJSON 验证不完整/矛盾响应不会被当成零活动观测。
func TestFetchGatewaySnapshotInvalidJSON(t *testing.T) {
	cases := []struct{ name, body string }{
		{"empty", ""}, {"syntax", "{"}, {"null", "null"}, {"array", "[]"},
		{"trailing_json", probeValidJSON + "{}"},
	}
	fields := []string{"schema_version", "active_sessions", "max_sessions", "stopping"}
	for _, field := range fields {
		for _, kind := range []string{"missing", "null"} {
			var m map[string]any
			if err := json.Unmarshal([]byte(probeValidJSON), &m); err != nil {
				t.Fatal(err)
			}
			if kind == "missing" {
				delete(m, field)
			} else {
				m[field] = nil
			}
			data, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			cases = append(cases, struct{ name, body string }{kind + "_" + field, string(data)})
		}
	}
	for _, change := range []struct{ name, field, value string }{
		{"unknown_version", "schema_version", "2"}, {"zero_version", "schema_version", "0"},
		{"version_bool", "schema_version", "true"}, {"version_string", "schema_version", `"1"`},
		{"active_negative", "active_sessions", "-1"}, {"above_limit", "active_sessions", "4"},
		{"active_float", "active_sessions", "2.0"}, {"active_bool", "active_sessions", "false"},
		{"active_overflow", "active_sessions", "9223372036854775808"},
		{"limit_zero", "max_sessions", "0"}, {"limit_negative", "max_sessions", "-1"},
		{"limit_float", "max_sessions", "3.0"}, {"limit_string", "max_sessions", `"3"`},
		{"stopping_number", "stopping", "0"}, {"stopping_string", "stopping", `"false"`},
	} {
		var m map[string]json.RawMessage
		if err := json.Unmarshal([]byte(probeValidJSON), &m); err != nil {
			t.Fatal(err)
		}
		m[change.field] = json.RawMessage(change.value)
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, struct{ name, body string }{change.name, string(data)})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := probeFetch(t, 200, "application/json", &probeBody{reader: strings.NewReader(tc.body)})
			if err == nil {
				t.Fatalf("accepted invalid JSON: %s", tc.body)
			}
		})
	}
}

// TestFetchGatewaySnapshotHTTPBounds 验证协议拒绝、恰好 4KiB 与超限时的读取上界。
func TestFetchGatewaySnapshotHTTPBounds(t *testing.T) {
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
			body := &probeBody{reader: strings.NewReader(probeValidJSON)}
			_, err := probeFetch(t, tc.status, tc.contentType, body)
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
			body := &probeBody{reader: strings.NewReader(probeValidJSON + strings.Repeat(" ", size-len(probeValidJSON)))}
			_, err := probeFetch(t, 200, "application/json", body)
			if (err == nil) != (size == 4096) || body.read != min(size, 4097) {
				t.Fatalf("size=%d err=%v read=%d", size, err, body.read)
			}
		})
	}
}

type probeErrorReader struct{ err error }

func (r probeErrorReader) Read([]byte) (int, error) { return 0, r.err }

// TestFetchGatewaySnapshotErrors 验证参数/传输/读取错误和原始错误链。
func TestFetchGatewaySnapshotErrors(t *testing.T) {
	for _, mode := range []string{"nil_context", "nil_client", "invalid_url", "transport", "body_read"} {
		t.Run(mode, func(t *testing.T) {
			sentinel := errors.New("probe I/O failed")
			calls := 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(*http.Request) (*http.Response, error) { calls++; return nil, sentinel }}}
			ctx := context.Background()
			endpoint := "http://gateway.invalid/debug/gateway"
			switch mode {
			case "nil_context":
				ctx = nil
			case "nil_client":
				client = nil
			case "invalid_url":
				endpoint = "://bad"
			}
			var state loadgen.GatewayState
			var err error
			if mode == "body_read" {
				state, err = probeFetch(t, 200, "application/json", &probeBody{reader: probeErrorReader{sentinel}})
			} else {
				state, err = loadgen.FetchGatewaySnapshot(ctx, client, endpoint)
			}
			if err == nil || state != (loadgen.GatewayState{}) {
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

// TestFetchGatewaySnapshotRedirect 验证重定向不跟随且原 client 的策略/Timeout/Transport 保持。
func TestFetchGatewaySnapshotRedirect(t *testing.T) {
	var targetCalls, redirectCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, probeValidJSON)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer origin.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { redirectCalls.Add(1); return nil }}
	state, err := loadgen.FetchGatewaySnapshot(context.Background(), client, origin.URL)
	if err == nil || state != (loadgen.GatewayState{}) || targetCalls.Load() != 0 || redirectCalls.Load() != 0 {
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

// TestFetchGatewaySnapshotConnectionReuse 验证正常响应读完并关闭后复用连接。
func TestFetchGatewaySnapshotConnectionReuse(t *testing.T) {
	var connections, requests atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, probeValidJSON)
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
		if _, err := loadgen.FetchGatewaySnapshot(context.Background(), client, srv.URL); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 2 || connections.Load() != 1 {
		t.Fatalf("requests=%d connections=%d", requests.Load(), connections.Load())
	}
}

// TestFetchGatewaySnapshotCancellation 验证取消与期限同时覆盖等待响应头及读取响应体。
func TestFetchGatewaySnapshotCancellation(t *testing.T) {
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
					state loadgen.GatewayState
					err   error
				}
				done := make(chan result, 1)
				go func() { state, err := loadgen.FetchGatewaySnapshot(ctx, client, srv.URL); done <- result{state, err} }()
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
					if got.state != (loadgen.GatewayState{}) || !errors.Is(got.err, want) {
						t.Fatalf("state=%+v err=%v want=%v", got.state, got.err, want)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("query did not exit")
				}
			})
		}
	}
}
