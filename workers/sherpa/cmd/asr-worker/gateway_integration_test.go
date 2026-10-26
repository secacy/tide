package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

type gatewayCaseReport struct {
	Name                string               `json:"name"`
	InputBytes          int                  `json:"input_bytes"`
	InputSHA            string               `json:"input_sha256"`
	ReadBytes           int64                `json:"source_read_bytes"`
	SourceCloses        int32                `json:"source_closes"`
	SourceEOFNS         *int64               `json:"source_eof_ns"`
	RunNS               int64                `json:"run_ns"`
	FirstTextNS         *int64               `json:"run_start_to_first_text_ns"`
	FirstTextBeforeEnd  bool                 `json:"first_text_before_all_source_read"`
	TailNS              *int64               `json:"source_eof_to_run_return_ns"`
	Client              wsclient.V2RunResult `json:"client"`
	Error               string               `json:"error,omitempty"`
	Canceled            bool                 `json:"canceled"`
	CallbackUpdates     int                  `json:"callback_updates"`
	Finals              int                  `json:"finals"`
	Transcript          string               `json:"transcript"`
	Segments            int                  `json:"segments"`
	ActiveAtReturn      int                  `json:"gateway_active_at_run_return"`
	ActiveAfterCleanup  int                  `json:"gateway_active_after_cleanup"`
	CleanupWaitNS       int64                `json:"run_return_to_observed_cleanup_ns"`
	NativeClosed        int                  `json:"native_closed_after_cleanup"`
	RPC                 gatewayRPCSnapshot   `json:"grpc"`
	ProcessedAudioBytes *uint64              `json:"processed_audio_bytes"`
	CER                 *float64             `json:"cer"`
}

func writeGatewayJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

// nativeCloseCount 只观察完整日志行；Close 返回后才写入该事件。
func nativeCloseCount(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	count := 0
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		var tag struct{ Msg string }
		if json.Unmarshal(line, &tag) == nil && tag.Msg == "engine_closed" {
			count++
		}
	}
	return count
}

// waitGatewayNaturalCleanup 不触发关闭；先观测，再由夹具兜底清理。
func waitGatewayNaturalCleanup(g *gateway.Gateway, logPath string, expectedClosed int, budget time.Duration) error {
	timer := time.NewTimer(budget)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if g.Snapshot().ActiveSessions == 0 && nativeCloseCount(logPath) == expectedClosed {
			return nil
		}
		select {
		case <-timer.C:
			return errors.New("natural Gateway/native cleanup did not complete")
		case <-tick.C:
		}
	}
}

// gatewayCaseHooks 仅为实验观察提供本进程时刻；不改变客户端/网关语义。
// 回调随 Run 串行执行，WaitCleanup 在 Run 返回后执行。
type gatewayCaseHooks struct {
	Started     func(time.Time)                // Run 起点，包含后续握手与模型加载。
	Canceled    func(time.Time, int64, uint64) // 取消触发时刻、已读字节、正在回调的结果序号。
	Returned    func(time.Time)                // Run 已等待本地任务退出，但远端可能仍保留会话。
	WaitCleanup func(int, time.Duration) error // 累计预期释放数与本场观察预算。
}

func runGatewayCase(t *testing.T, g *gateway.Gateway, observer *gatewayObservedClient, url, out, name string, pcm []byte, cancelOnText bool, expectedClosed int) (gatewayCaseReport, error) {
	return runGatewayCaseObserved(t, g, observer, url, out, name, pcm, cancelOnText, expectedClosed, gatewayCaseHooks{})
}

func runGatewayCaseObserved(t *testing.T, g *gateway.Gateway, observer *gatewayObservedClient, url, out, name string, pcm []byte, cancelOnText bool, expectedClosed int, hooks gatewayCaseHooks) (gatewayCaseReport, error) {
	t.Helper()
	sum := sha256.Sum256(pcm)
	report := gatewayCaseReport{Name: name, InputBytes: len(pcm), InputSHA: hex.EncodeToString(sum[:])}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	file, err := os.Create(filepath.Join(out, name+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var start time.Time
	source := &gatewayPCMSource{reader: bytes.NewReader(pcm)}
	var consumer gatewayTranscript
	client, err := wsclient.NewV2(wsclient.V2Config{
		URL: url, ChunkBytes: 3200, Realtime: true,
		OnResult: func(ctx context.Context, result wsprotocol.SequencedResultMessage, at time.Time) error {
			read := source.read.Load()
			if err := consumer.apply(ctx, result, at.Sub(start).Nanoseconds(), read, file); err != nil {
				return err
			}
			if report.FirstTextNS == nil && strings.TrimSpace(result.Text) != "" {
				ns := at.Sub(start).Nanoseconds()
				report.FirstTextNS = &ns
				report.FirstTextBeforeEnd = read < int64(len(pcm))
			}
			if cancelOnText && !result.IsFinal && strings.TrimSpace(result.Text) != "" {
				// 回调已更新示例文本，但取消可能先于客户端提交 AppliedSeq。
				if hooks.Canceled != nil {
					hooks.Canceled(time.Now(), source.read.Load(), result.Seq)
				}
				cancel()
			}
			return nil
		},
	})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	start = time.Now()
	source.start = start
	if hooks.Started != nil {
		hooks.Started(start)
	}
	report.Client, err = client.Run(ctx, source)
	returned := time.Now()
	report.RunNS = returned.Sub(start).Nanoseconds()
	if hooks.Returned != nil {
		hooks.Returned(returned)
	}
	report.ActiveAtReturn = g.Snapshot().ActiveSessions
	report.Canceled = errors.Is(err, context.Canceled)
	if err != nil {
		report.Error = err.Error()
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report.ReadBytes, report.SourceCloses = source.read.Load(), source.closed.Load()
	if eof := source.eofNS.Load(); eof != 0 {
		report.SourceEOFNS = &eof
		tail := report.RunNS - eof
		report.TailNS = &tail
	}
	report.CallbackUpdates = len(consumer.updates)
	report.Segments = len(consumer.segments)
	report.Transcript = consumer.segments["1"].Text
	for _, event := range consumer.updates {
		if event.Message.IsFinal {
			report.Finals++
		}
	}
	writeGatewayJSON(t, filepath.Join(out, name+"-client-return.json"), report)
	cleanupBudget := 5 * time.Second
	if cancelOnText {
		cleanupBudget = 15 * time.Second // 包含默认 10 秒恢复窗口。
	}
	waitCleanup := hooks.WaitCleanup
	if waitCleanup == nil {
		waitCleanup = func(expected int, budget time.Duration) error {
			return waitGatewayNaturalCleanup(g, filepath.Join(out, "server.log"), expected, budget)
		}
	}
	cleanupErr := waitCleanup(expectedClosed, cleanupBudget)
	// 从已记录的 Run 返回点计时，包含保存中间记录及等待观察的开销。
	report.CleanupWaitNS = time.Since(start).Nanoseconds() - report.RunNS
	report.ActiveAfterCleanup = g.Snapshot().ActiveSessions
	report.NativeClosed = nativeCloseCount(filepath.Join(out, "server.log"))
	trace, traceErr := observer.traceForCall(expectedClosed)
	if traceErr != nil {
		return report, traceErr
	}
	report.RPC = trace.snapshot()
	writeGatewayJSON(t, filepath.Join(out, name+"-grpc.json"), report.RPC)
	if cleanupErr != nil {
		return report, cleanupErr
	}
	for _, result := range report.RPC.Results {
		if result.Progress || result.SegmentID != "1" {
			return report, errors.New("unexpected Worker result contract")
		}
	}
	for i, applied := range consumer.updates {
		if i >= len(report.RPC.Results) {
			return report, errors.New("application received extra Worker results")
		}
		original := report.RPC.Results[i]
		if applied.Message.SegmentID != original.SegmentID || applied.Message.Text != original.Text || applied.Message.IsFinal != original.Final {
			return report, errors.New("application changed or reordered a Worker snapshot")
		}
	}
	return report, err
}

func checkGatewayCase(report gatewayCaseReport, cancelOnText bool) error {
	if report.SourceCloses != 1 || report.ActiveAfterCleanup != 0 || report.Segments != 1 {
		return errors.New("source, Gateway cleanup or snapshot consumer mismatch")
	}
	if cancelOnText {
		if !report.Canceled || report.Client.Completed || report.Client.CompletionAckWritten || report.Finals != 0 || report.ReadBytes >= int64(report.InputBytes) || report.SourceEOFNS != nil || report.ActiveAtReturn != 1 || report.RPC.HalfClosed {
			return errors.New("cancellation lost original-session retention semantics")
		}
		return nil
	}
	if report.Error != "" || !report.Client.Completed || !report.Client.CompletionAckWritten || report.Client.FinalOffset != uint64(report.InputBytes) || report.Client.AppliedSeq != uint64(report.CallbackUpdates) || report.Finals != 1 || report.ReadBytes != int64(report.InputBytes) || report.RPC.AudioBytes != report.InputBytes || report.RPC.AudioSHA != report.InputSHA || !report.RPC.HalfClosed || report.RPC.Terminal != "EOF" || report.SourceEOFNS == nil || report.CleanupWaitNS > int64(5*time.Second) {
		return errors.New("normal completion or audio forwarding mismatch")
	}
	if len(report.RPC.Results) != report.CallbackUpdates || len(report.RPC.Chunks) == 0 || report.RPC.Chunks[len(report.RPC.Chunks)-1] != (report.InputBytes-1)%3200+1 {
		return errors.New("Worker snapshot count or short tail mismatch")
	}
	return nil
}

// TestRealWorkerGatewayV2 保持生产 Gateway/客户端，通过真实网络连接原生进程。
// 仅显式环境变量启用；普通 race 回归不加载模型。
func TestRealWorkerGatewayV2(t *testing.T) {
	bin, modelDir := os.Getenv("TIDE_SHERPA_WORKER_BIN"), os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if bin == "" || modelDir == "" {
		t.Skip("set Worker binary and pinned model paths to enable Gateway/v2 verification")
	}
	out := os.Getenv("TIDE_SHERPA_GATEWAY_EVIDENCE_DIR")
	if out == "" {
		out = filepath.Join(t.TempDir(), "gateway-evidence")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	process := startRealWorkerProcess(t, bin, modelDir, out)
	conn, err := grpc.NewClient("passthrough:///"+process.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	observer := &gatewayObservedClient{client: asrv1.NewASRServiceClient(conn)}
	pool, err := workerpool.NewRoundRobin([]workerpool.Worker{{ID: "sherpa", Client: observer}})
	if err != nil {
		t.Fatal(err)
	}
	gatewayCtx, cancelGateway := context.WithCancel(context.Background())
	cfg := gateway.Config{MaxSessions: 1, MaxPendingAudioBytes: 0, V2: &gateway.V2Config{}}
	g, err := gateway.New(gatewayCtx, pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/asr", g.ServeV2HTTP)
	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		g.StopAccepting()
		cancelGateway()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.Wait(ctx)
		server.Close()
		_ = conn.Close()
	})
	writeGatewayJSON(t, filepath.Join(out, "conditions.json"), map[string]any{
		"max_sessions": 1, "max_pending_audio_bytes": 0, "resume_window_ns": int64(10 * time.Second),
		"result_retention_ns": int64(30 * time.Second), "realtime": true, "chunk_bytes": 3200,
		"client_resume_enabled": false, "gateway_in_test_process": true, "worker_separate_process": true,
	})
	var reports []gatewayCaseReport
	var speech0 []byte
	for index, name := range []string{"speech0", "speech1", "silence", "cancel_after_partial", "after_cancel"} {
		var pcm []byte
		switch index {
		case 0, 1:
			pcm, err = readTestPCM(filepath.Join(modelDir, "test_wavs", fmt.Sprintf("%d.wav", index)))
			if err != nil {
				t.Fatal(err)
			}
			if index == 0 {
				speech0 = pcm
			}
		case 3:
			pcm = speech0
		default:
			pcm = make([]byte, 65120)
		}
		r, runErr := runGatewayCase(t, g, observer, "ws"+strings.TrimPrefix(server.URL, "http")+"/v2/asr", out, name, pcm, index == 3, index+1)
		reports = append(reports, r)
		writeGatewayJSON(t, filepath.Join(out, "summary.json"), reports)
		t.Logf("case=%s completed=%v callbacks=%d applied_seq=%d active_at_return=%d cleanup_ms=%.3f", name, r.Client.Completed, r.CallbackUpdates, r.Client.AppliedSeq, r.ActiveAtReturn, float64(r.CleanupWaitNS)/1e6)
		if index != 3 && runErr != nil {
			t.Fatal(runErr)
		}
		if index == 3 && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("expected cancellation: %v", runErr)
		}
		if err := checkGatewayCase(r, index == 3); err != nil {
			t.Fatalf("case %s: %v", name, err)
		}
		if index < 2 && (!r.FirstTextBeforeEnd || r.FirstTextNS == nil) {
			t.Fatal("speech did not arrive during input")
		}
		if (index == 2 || index == 4) && (r.FirstTextNS != nil || r.Transcript != "") {
			t.Fatal("silence produced unexpected text")
		}
	}
	g.StopAccepting()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	process.stop(t)
	writeGatewayJSON(t, filepath.Join(out, "process.json"), map[string]any{"process_exit": 0, "models_closed": nativeCloseCount(filepath.Join(out, "server.log")), "gateway_active_before_fixture_cleanup": g.Snapshot().ActiveSessions})
}
