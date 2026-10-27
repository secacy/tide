package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// segmentJSONLog 按序落盘，不积累事件副本；Send 与 Recv 可以并发记录。
type segmentJSONLog struct {
	mu   sync.Mutex
	file *os.File
	err  error
}

func (l *segmentJSONLog) write(value any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err == nil {
		l.err = json.NewEncoder(l.file).Encode(value)
	}
}
func (l *segmentJSONLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return errors.Join(l.err, l.file.Close())
}
func segmentLog(t *testing.T, path string) *segmentJSONLog {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	return &segmentJSONLog{file: f}
}

type segmentSource struct {
	reader         *bytes.Reader
	clock          time.Time
	log            *segmentJSONLog
	offset         int
	firstNS, eofNS int64
	closed         atomic.Int32
}

func (s *segmentSource) Read(p []byte) (int, error) {
	if s.closed.Load() != 0 {
		return 0, io.ErrClosedPipe
	}
	n, err := s.reader.Read(p)
	at := time.Since(s.clock).Nanoseconds()
	if n > 0 && s.firstNS == 0 {
		s.firstNS = at
	}
	if err == io.EOF {
		s.eofNS = at
	}
	s.log.write(map[string]any{"at_ns": at, "offset": s.offset, "bytes": n, "eof": err == io.EOF})
	s.offset += n
	return n, err
}
func (s *segmentSource) Close() error { s.closed.Add(1); return nil }

// segmentObservedClient 复用原始 RPC 摘要；另记录实际 Send 返回和 Recv 的驱动时刻。
type segmentObservedClient struct {
	base  *gatewayObservedClient
	mu    sync.Mutex
	clock time.Time
	log   *segmentJSONLog
}

func (c *segmentObservedClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (asrv1.ASRService_StreamingRecognizeClient, error) {
	c.mu.Lock()
	clock, log := c.clock, c.log
	c.mu.Unlock()
	s, err := c.base.StreamingRecognize(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &segmentObservedStream{ASRService_StreamingRecognizeClient: s, clock: clock, log: log}, nil
}

type segmentObservedStream struct {
	asrv1.ASRService_StreamingRecognizeClient
	clock  time.Time
	log    *segmentJSONLog
	offset int
}

func (s *segmentObservedStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	start := time.Since(s.clock).Nanoseconds()
	err := s.ASRService_StreamingRecognizeClient.Send(req)
	end := time.Since(s.clock).Nanoseconds()
	s.log.write(map[string]any{"event": "send", "start_ns": start, "end_ns": end, "offset": s.offset, "bytes": len(req.Data), "success": err == nil})
	if err == nil {
		s.offset += len(req.Data)
	}
	return err
}
func (s *segmentObservedStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	r, err := s.ASRService_StreamingRecognizeClient.Recv()
	at := time.Since(s.clock).Nanoseconds()
	if err == nil {
		s.log.write(map[string]any{"event": "result", "at_ns": at, "segment_id": r.SegmentId, "text": r.Text, "final": r.IsFinal, "progress": r.Progress != nil})
	} else {
		s.log.write(map[string]any{"event": "terminal", "at_ns": at, "error": err.Error()})
	}
	return r, err
}

type segmentAllocator struct {
	pool  *workerpool.CapacityRoundRobin
	calls atomic.Int32
}

func (a *segmentAllocator) TryAcquire() (*workerpool.WorkerLease, error) {
	a.calls.Add(1)
	return a.pool.TryAcquire()
}

type segmentCase struct {
	Name                     string `json:"name"`
	State                    string `json:"state"`
	StartNS, ReturnNS        int64
	FirstReadNS, SourceEOFNS int64
	InputBytes               int
	SourceCloses             int32
	Updates                  int
	Finals                   []string
	Client                   wsclient.V2RunResult
	RPC                      gatewayRPCSnapshot
	AcquireCalls             int32
	IdleNS                   *int64
	LeaseReusable            bool
	Failure                  string
}

func runSegmentCase(t *testing.T, m *backendMonitor, g *gateway.Gateway, a *segmentAllocator, c *segmentObservedClient, url, out, name string, pcm []byte, id int, budget time.Duration) segmentCase {
	t.Helper()
	slot := segmentCase{Name: name, State: "running", InputBytes: len(pcm)}
	reads := segmentLog(t, filepath.Join(out, name+"-reads.jsonl"))
	results := segmentLog(t, filepath.Join(out, name+"-results.jsonl"))
	rpcLog := segmentLog(t, filepath.Join(out, name+"-rpc.jsonl"))
	source := &segmentSource{reader: bytes.NewReader(pcm), clock: m.start, log: reads}
	c.mu.Lock()
	c.clock, c.log = m.start, rpcLog
	c.mu.Unlock()
	currentID, currentText := "", "" // 只保存当前段，不组装完整历史文本。
	finalized := false
	client, err := wsclient.NewV2(wsclient.V2Config{URL: url, ChunkBytes: 3200, Realtime: true, OnResult: func(_ context.Context, r wsprotocol.SequencedResultMessage, receivedAt time.Time) error {
		slot.Updates++
		if r.SegmentID != currentID {
			if currentID != "" && !finalized {
				return errors.New("new segment before old final")
			}
			currentID = r.SegmentID
			finalized = false
		} else if finalized {
			return errors.New("revision after final")
		}
		currentText = r.Text
		if r.IsFinal {
			slot.Finals = append(slot.Finals, r.SegmentID)
			currentText = ""
			finalized = true
		}
		results.write(map[string]any{"at_ns": receivedAt.Sub(m.start).Nanoseconds(), "message": r, "current_text_bytes": len(currentText)})
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	slot.StartNS = time.Since(m.start).Nanoseconds()
	slot.Client, err = client.Run(ctx, source)
	cancel()
	slot.ReturnNS = time.Since(m.start).Nanoseconds()
	slot.FirstReadNS, slot.SourceEOFNS, slot.SourceCloses = source.firstNS, source.eofNS, source.closed.Load()
	cleanupErr := m.waitCleanup(id, 5*time.Second, &slot.IdleNS)
	trace, traceErr := c.base.traceForCall(id)
	if traceErr == nil {
		slot.RPC = trace.snapshot()
	}
	slot.AcquireCalls = a.calls.Load()
	if cleanupErr == nil {
		lease, leaseErr := a.pool.TryAcquire()
		if lease != nil {
			lease.Release()
			slot.LeaseReusable = true
		}
		cleanupErr = errors.Join(cleanupErr, leaseErr)
	}
	err = errors.Join(err, cleanupErr, traceErr, reads.close(), results.close(), rpcLog.close())
	if err == nil && (!slot.Client.Completed || !slot.Client.CompletionAckWritten || slot.Client.FinalOffset != uint64(len(pcm)) || slot.RPC.AudioBytes != len(pcm) || slot.AcquireCalls != int32(id) || !slot.LeaseReusable) {
		err = errors.New("completion, input or lease mismatch")
	}
	slot.State = "passed"
	if err != nil {
		slot.State = "failed"
		slot.Failure = err.Error()
	}
	writeGatewayJSON(t, filepath.Join(out, name+".json"), slot)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if err := c.base.releaseCompleted(id); err != nil {
		t.Fatal(err)
	}
	return slot
}

// TestRealWorkerSegmented120s 保留明确的两分钟档；普通回归默认跳过。
func TestRealWorkerSegmented120s(t *testing.T) {
	runSegmentExperiment(t, "TIDE_SHERPA_SEGMENT120_DIR", 120)
}

// TestRealWorkerSegmented600s 在相同段长下延长同一会话，不纳入普通回归。
func TestRealWorkerSegmented600s(t *testing.T) {
	runSegmentExperiment(t, "TIDE_SHERPA_SEGMENT600_DIR", 600)
}

// runSegmentExperiment 共享采集语义；时长仅由上述两个显式入口决定。
func runSegmentExperiment(t *testing.T, env string, seconds int) {
	t.Helper()
	root := os.Getenv(env)
	if root == "" {
		t.Skip("set " + env + " to run the frozen segmented experiment")
	}
	bin, model := os.Getenv("TIDE_SHERPA_WORKER_BIN"), os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if bin == "" || model == "" {
		t.Fatal("explicit experiment requires Worker and model paths")
	}
	// 运行前核对实际二进制和源码；不能只冻结一个未使用的程序摘要。
	var frozen struct {
		Worker        string            `json:"worker_sha256"`
		Driver        string            `json:"driver_sha256"`
		Source        map[string]string `json:"source"`
		FormalSeconds int               `json:"formal_seconds"`
	}
	manifest, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(manifest, &frozen); err != nil {
		t.Fatal(err)
	}
	if frozen.FormalSeconds != seconds {
		t.Fatal("experiment duration does not match frozen profile")
	}
	driver, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{bin: frozen.Worker, driver: frozen.Driver} {
		got, err := backendFileSHA(path)
		if err != nil || got != want {
			t.Fatalf("binary identity mismatch: %s %v", path, err)
		}
	}
	for name, want := range frozen.Source {
		got, err := backendFileSHA(filepath.Join("..", "..", "..", "..", name))
		if err != nil || got != want {
			t.Fatalf("frozen source mismatch: %s %v", name, err)
		}
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "-race" && s.Value == "true" {
				t.Fatal("formal driver must be non-race")
			}
		}
	}
	base, err := readTestPCM(filepath.Join(model, "test_wavs", "0.wav"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := freezeBackendAssets(model, base); err != nil {
		t.Fatal(err)
	}
	input, err := os.ReadFile(filepath.Join(root, "input.pcm"))
	if err != nil || len(input) != seconds*32000 {
		t.Fatal("missing frozen PCM for the selected duration", err)
	}
	for i, b := range input {
		if b != base[i%len(base)] {
			t.Fatal("frozen input is not the prescribed repetition")
		}
	}
	out := filepath.Join(root, "data")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	process := startRealWorkerProcess(t, bin, model, out, "-max-audio-bytes="+strconv.Itoa(seconds*32000), "-segment-audio-bytes=960000", "-observe-segments")
	conn, err := grpc.NewClient("passthrough:///"+process.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &segmentObservedClient{base: &gatewayObservedClient{client: asrv1.NewASRServiceClient(conn)}}
	pool, err := workerpool.NewCapacityRoundRobin([]workerpool.WorkerCapacity{{Worker: workerpool.Worker{ID: "sherpa", Client: c}, Limit: 1}})
	if err != nil {
		t.Fatal(err)
	}
	a := &segmentAllocator{pool: pool}
	life, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, err := gateway.NewWithAllocator(life, a, gateway.Config{MaxSessions: 1, MaxMessageBytes: 3208, MaxPendingAudioBytes: 0, V2: &gateway.V2Config{}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(g.ServeV2HTTP))
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := g.Wait(ctx); err != nil {
			t.Error(err)
		}
		server.Close()
	})
	m, err := newBackendMonitor(out, process.cmd.Process.Pid, g)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			if err := m.stop(); err != nil {
				t.Error(err)
			}
		}
	})
	writeGatewayJSON(t, filepath.Join(out, "process.json"), map[string]any{"worker_pid": process.cmd.Process.Pid, "driver_pid": os.Getpid(), "start": m.start.Format(time.RFC3339Nano)})
	m.startSampling()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	var slots []segmentCase
	for i, tc := range []struct {
		name string
		pcm  []byte
		idle time.Duration
	}{{"warmup", base, 5 * time.Second}, {"formal", input, 10 * time.Second}, {"probe", base[:96000], 5 * time.Second}} {
		m.setPhase(tc.name, i)
		budget := 150 * time.Second
		if tc.name == "formal" {
			budget = time.Duration(seconds+30) * time.Second
		}
		slots = append(slots, runSegmentCase(t, m, g, a, c, url, out, tc.name, tc.pcm, i+1, budget))
		writeGatewayJSON(t, filepath.Join(out, "cases.json"), slots)
		m.setPhase(tc.name+"_idle", i)
		time.Sleep(tc.idle)
	}
	if err := m.stop(); err != nil {
		t.Fatal(err)
	}
	stopped = true
	process.stop(t)
	t.Logf("%ds segmented run completed; evidence: %s", seconds, root)
}
