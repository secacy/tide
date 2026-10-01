package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newSnapshotWorker 使用正式构造方法创建实例，不访问私有名额池。
func newSnapshotWorker(t *testing.T, limit int) *mockasr.Worker {
	t.Helper()
	w, err := mockasr.New(mockasr.Config{ProcessingConcurrency: limit, ProcessingDelay: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// checkWorkerSnapshotResponse 独立检查 JSON，避免生产 DTO 的标签错误被测试共享。
// limit=0 表示没有计数，必须保留 null；否则验证三个实际计数，包括零值字段。
func checkWorkerSnapshotResponse(t *testing.T, resp *http.Response, limit, inUse, waiting int) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("status=%d headers=%v", resp.StatusCode, resp.Header)
	}
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	var got map[string]any
	if err := decoder.Decode(&got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"schema_version": json.Number("1"), "processing_limit_enabled": limit > 0, "processing": nil}
	if limit > 0 {
		want["processing"] = map[string]any{
			"limit": json.Number(fmt.Sprint(limit)), "in_use": json.Number(fmt.Sprint(inUse)), "waiting": json.Number(fmt.Sprint(waiting)),
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response=%v want=%v", got, want)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("trailing response: %v %v", extra, err)
	}
}

// queryWorkerSnapshot 重用传入的路由，以发现创建 Handler 时错误缓存状态的实现。
func queryWorkerSnapshot(t *testing.T, handler http.Handler, limit, inUse, waiting int) {
	t.Helper()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/worker", nil))
	checkWorkerSnapshotResponse(t, w.Result(), limit, inUse, waiting)
}

func TestWorkerSnapshotHTTPContract(t *testing.T) {
	for _, limit := range []int{0, 3} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			worker := newSnapshotWorker(t, limit)
			handler := routes(worker)
			for range 3 {
				queryWorkerSnapshot(t, handler, limit, 0, 0)
			}
			got, enabled := worker.ProcessingSnapshot()
			if enabled != (limit > 0) || got != (mockasr.ProcessingSnapshot{Limit: limit}) {
				t.Fatalf("query changed state: %+v enabled=%v", got, enabled)
			}
		})
	}
}

// TestWorkerSnapshotHTTPRoutes 使用真实 HTTP 服务验证 GET、HEAD 及 mux 的拒绝行为。
func TestWorkerSnapshotHTTPRoutes(t *testing.T) {
	srv := httptest.NewServer(routes(newSnapshotWorker(t, 2)))
	defer srv.Close()
	client := srv.Client()
	client.Timeout = 3 * time.Second
	for _, tc := range []struct {
		name, method, path string
		code               int
	}{
		{"get", "GET", "/debug/worker", 200},
		{"head", "HEAD", "/debug/worker", 200},
		{"post", "POST", "/debug/worker", 405},
		{"delete", "DELETE", "/debug/worker", 405},
		{"child", "GET", "/debug/worker/child", 404},
		{"unknown", "GET", "/debug/gateway", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "get" {
				checkWorkerSnapshotResponse(t, resp, 2, 0, 0)
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.code {
				t.Fatalf("status=%d body=%q", resp.StatusCode, body)
			}
			if tc.name == "head" && (len(body) != 0 || resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Cache-Control") != "no-store") {
				t.Fatalf("HEAD headers=%v body=%q", resp.Header, body)
			}
			if tc.code == 405 && (!strings.Contains(resp.Header.Get("Allow"), "GET") || !strings.Contains(resp.Header.Get("Allow"), "HEAD")) {
				t.Fatalf("Allow=%q", resp.Header.Get("Allow"))
			}
		})
	}
}

// snapshotAudioStream 提供一块有效 PCM 和 EOF；通过公开 RPC 方法执行真实 Worker 逻辑。
// 传输为内存替身，不将这些状态检查称为真实 gRPC 网络实验。
type snapshotAudioStream struct {
	grpc.ServerStream
	ctx       context.Context
	read      bool
	responses []*asrv1.StreamingRecognizeResponse // 仅流 goroutine 写，退出后读取。
}

func (s *snapshotAudioStream) Context() context.Context { return s.ctx }
func (s *snapshotAudioStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	if s.read {
		return nil, io.EOF
	}
	s.read = true
	return &asrv1.StreamingRecognizeRequest{Data: make([]byte, 3200)}, nil
}
func (s *snapshotAudioStream) Send(r *asrv1.StreamingRecognizeResponse) error {
	s.responses = append(s.responses, r)
	return nil
}

func startSnapshotAudio(worker *mockasr.Worker, ctx context.Context) (*snapshotAudioStream, <-chan error) {
	s := &snapshotAudioStream{ctx: ctx}
	done := make(chan error, 1)
	go func() { done <- worker.StreamingRecognize(s) }()
	return s, done
}

// checkSnapshotAudioCompletion 确认正常流完整返回进度和尾部，取消流没有确认未处理音频。
func checkSnapshotAudioCompletion(t *testing.T, s *snapshotAudioStream, done <-chan error, want codes.Code) {
	t.Helper()
	if err := <-done; status.Code(err) != want {
		t.Fatalf("stream=%v want=%v", err, want)
	}
	if want == codes.OK {
		if len(s.responses) != 2 || s.responses[0].GetProgress().GetProcessedAudioBytes() != 3200 || !s.responses[1].GetIsFinal() {
			t.Fatalf("incomplete responses: %v", s.responses)
		}
	} else if len(s.responses) != 0 {
		t.Fatalf("canceled audio acknowledged: %v", s.responses)
	}
}

// TestWorkerSnapshotHTTPLifecycle 验证同一路由读取当前状态，查询满额 Worker 不需要处理名额。
func TestWorkerSnapshotHTTPLifecycle(t *testing.T) {
	for _, mode := range []string{"completed", "cancel_waiter", "cancel_holder"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				worker := newSnapshotWorker(t, 1)
				handler := routes(worker)
				queryWorkerSnapshot(t, handler, 1, 0, 0)
				holderCtx, cancelHolder := context.WithCancel(context.Background())
				defer cancelHolder()
				waiterCtx, cancelWaiter := context.WithCancel(context.Background())
				defer cancelWaiter()
				holder, holderDone := startSnapshotAudio(worker, holderCtx)
				synctest.Wait()
				queryWorkerSnapshot(t, handler, 1, 1, 0)
				waiter, waiterDone := startSnapshotAudio(worker, waiterCtx)
				synctest.Wait()
				queryWorkerSnapshot(t, handler, 1, 1, 1)
				first, firstDone, last, lastDone := holder, holderDone, waiter, waiterDone
				want := codes.OK
				switch mode {
				case "completed":
					time.Sleep(10 * time.Millisecond)
				case "cancel_holder":
					cancelHolder()
					want = codes.Canceled
				case "cancel_waiter":
					cancelWaiter()
					want = codes.Canceled
					first, firstDone, last, lastDone = waiter, waiterDone, holder, holderDone
				}
				synctest.Wait()
				checkSnapshotAudioCompletion(t, first, firstDone, want)
				queryWorkerSnapshot(t, handler, 1, 1, 0)
				time.Sleep(10 * time.Millisecond)
				synctest.Wait()
				checkSnapshotAudioCompletion(t, last, lastDone, codes.OK)
				queryWorkerSnapshot(t, handler, 1, 0, 0)
			})
		})
	}
}

func TestWorkerSnapshotHTTPDisabledWhileBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		worker := newSnapshotWorker(t, 0)
		handler := routes(worker)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, done := startSnapshotAudio(worker, ctx)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("processing finished early: %v", err)
		default:
		}
		queryWorkerSnapshot(t, handler, 0, 0, 0)
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		checkSnapshotAudioCompletion(t, stream, done, codes.OK)
		queryWorkerSnapshot(t, handler, 0, 0, 0)
	})
}

// workerSnapshotWriter 控制写回失败或阻塞，只在请求退出后读取记录。
type workerSnapshotWriter struct {
	header           http.Header
	statuses         []int
	writes           int
	body             bytes.Buffer
	err              error
	entered, release chan struct{}
}

func (w *workerSnapshotWriter) Header() http.Header  { return w.header }
func (w *workerSnapshotWriter) WriteHeader(code int) { w.statuses = append(w.statuses, code) }
func (w *workerSnapshotWriter) Write(data []byte) (int, error) {
	if len(w.statuses) == 0 {
		w.WriteHeader(200)
	}
	w.writes++
	if w.entered != nil {
		close(w.entered)
		<-w.release
	}
	if w.err != nil {
		n, _ := w.body.Write(data[:len(data)/2])
		return n, w.err
	}
	return w.body.Write(data)
}

func TestWorkerSnapshotHTTPWriteError(t *testing.T) {
	worker := newSnapshotWorker(t, 1)
	handler := routes(worker)
	w := &workerSnapshotWriter{header: make(http.Header), err: io.ErrClosedPipe}
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/debug/worker", nil))
	if w.writes != 1 || !reflect.DeepEqual(w.statuses, []int{200}) || strings.Contains(w.body.String(), "internal server error") {
		t.Fatalf("writes=%d statuses=%v body=%q", w.writes, w.statuses, w.body.String())
	}
	queryWorkerSnapshot(t, handler, 1, 0, 0)
}

// TestWorkerSnapshotHTTPSlowWriter 阻塞响应写回期间，音频仍能完成并归还名额，其他查询也可继续。
// 旧响应保留读取时的状态，不混入写回期间发生的状态变化。
func TestWorkerSnapshotHTTPSlowWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		worker := newSnapshotWorker(t, 1)
		handler := routes(worker)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, streamDone := startSnapshotAudio(worker, ctx)
		synctest.Wait()
		w := &workerSnapshotWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
		done := make(chan struct{})
		var release sync.Once
		defer func() { release.Do(func() { close(w.release) }); <-done }()
		go func() {
			defer close(done)
			handler.ServeHTTP(w, httptest.NewRequest("GET", "/debug/worker", nil))
		}()
		<-w.entered
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		checkSnapshotAudioCompletion(t, stream, streamDone, codes.OK)
		queryWorkerSnapshot(t, handler, 1, 0, 0)
		release.Do(func() { close(w.release) })
		<-done
		checkWorkerSnapshotResponse(t, &http.Response{StatusCode: 200, Header: w.header, Body: io.NopCloser(bytes.NewReader(w.body.Bytes()))}, 1, 1, 0)
	})
}
