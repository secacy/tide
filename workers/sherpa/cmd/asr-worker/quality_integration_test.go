package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// qualityInput 仅包含已冻结的输入身份；人工参考和评分不进入模型配置。
type qualityInput struct {
	ID           string `json:"id"`
	WAV          string `json:"wav_file"`
	WAVSHA       string `json:"wav_sha256"`
	PCMBytes     int    `json:"pcm_bytes"`
	PCMSHA       string `json:"pcm_sha256"`
	Reference    string `json:"reference_file"`
	ReferenceSHA string `json:"reference_sha256"`
}

// qualityOutcome 保留所有计划槽位；未运行或证据不足不能冒充零错误转录。
type qualityOutcome struct {
	ID      string             `json:"id"`
	Outcome string             `json:"outcome"` // completed / failed / unknown。
	Issue   string             `json:"issue,omitempty"`
	Report  *gatewayCaseReport `json:"report,omitempty"`
}

func qualitySHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// loadQualityInputs 在创建模型/连接之前校验全部六份输入，避免运行后再改参考。
// 文件内容在本次运行内固定；源 WAV 容器和原始 PCM 分别核对摘要。
func loadQualityInputs(dir string) ([]qualityInput, [][]byte, []byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, nil, nil, err
	}
	var manifest struct {
		Schema string         `json:"schema"`
		Cases  []qualityInput `json:"cases"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, nil, nil, err
	}
	if manifest.Schema != "tide.asr-quality-input.v1" || len(manifest.Cases) != 6 {
		return nil, nil, nil, fmt.Errorf("expected six frozen quality inputs")
	}
	var inputs [][]byte
	for i, item := range manifest.Cases {
		if item.ID != fmt.Sprintf("q%02d", i+1) || item.WAV != "pcm/"+item.ID+".wav" || item.Reference != "references/"+item.ID+".txt" {
			return nil, nil, nil, fmt.Errorf("invalid quality case identity or path")
		}
		wavPath := filepath.Join(dir, item.WAV)
		wav, err := os.ReadFile(wavPath)
		if err != nil || qualitySHA(wav) != item.WAVSHA {
			return nil, nil, nil, fmt.Errorf("%s: WAV identity mismatch: %v", item.ID, err)
		}
		pcm, err := readTestPCM(wavPath)
		if err != nil || len(pcm) != item.PCMBytes || qualitySHA(pcm) != item.PCMSHA {
			return nil, nil, nil, fmt.Errorf("%s: PCM identity mismatch: %v", item.ID, err)
		}
		reference, err := os.ReadFile(filepath.Join(dir, item.Reference))
		if err != nil || qualitySHA(reference) != item.ReferenceSHA {
			return nil, nil, nil, fmt.Errorf("%s: reference identity mismatch: %v", item.ID, err)
		}
		inputs = append(inputs, pcm)
	}
	return manifest.Cases, inputs, data, nil
}

// TestRealWorkerQualityV2 复用短音频闭环夹具；每段全新模型、原始快照、不做纠错。
// 只有显式指定本地素材和原生进程才运行，普通回归不访问私人录音。
func TestRealWorkerQualityV2(t *testing.T) {
	dir := os.Getenv("TIDE_SHERPA_QUALITY_INPUT_DIR")
	if dir == "" {
		t.Skip("set TIDE_SHERPA_QUALITY_INPUT_DIR to enable reviewed local recordings")
	}
	bin, model := os.Getenv("TIDE_SHERPA_WORKER_BIN"), os.Getenv("TIDE_SHERPA_MODEL_DIR")
	out := os.Getenv("TIDE_SHERPA_QUALITY_EVIDENCE_DIR")
	if bin == "" || model == "" || out == "" {
		t.Fatal("quality run requires explicit Worker, model and new evidence directory")
	}
	cases, inputs, manifest, err := loadQualityInputs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "input-manifest.json"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	process := startRealWorkerProcess(t, bin, model, out)
	conn, err := grpc.NewClient("passthrough:///"+process.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	observer := &gatewayObservedClient{client: asrv1.NewASRServiceClient(conn)}
	pool, err := workerpool.NewRoundRobin([]workerpool.Worker{{ID: "sherpa", Client: observer}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g, err := gateway.New(ctx, pool, gateway.Config{MaxSessions: 1, MaxPendingAudioBytes: 0, V2: &gateway.V2Config{}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/asr", g.ServeV2HTTP)
	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		waitCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = g.Wait(waitCtx)
		server.Close()
	})
	writeGatewayJSON(t, filepath.Join(out, "conditions.json"), map[string]any{
		"input_manifest_sha256": qualitySHA(manifest), "max_sessions": 1, "max_pending_audio_bytes": 0,
		"resume_window_ns": int64(10 * time.Second), "result_retention_ns": int64(30 * time.Second),
		"realtime": true, "chunk_bytes": 3200, "client_resume_enabled": false,
		"gateway_in_test_process": true, "worker_separate_process": true,
	})
	reports := make([]qualityOutcome, len(cases))
	for i, item := range cases {
		reports[i] = qualityOutcome{ID: item.ID, Outcome: "unknown", Issue: "not run"}
	}
	writeGatewayJSON(t, filepath.Join(out, "summary.json"), reports)
	for i, item := range cases {
		r, runErr := runGatewayCase(t, g, observer, "ws"+strings.TrimPrefix(server.URL, "http")+"/v2/asr", out, item.ID, inputs[i], false, i+1)
		checkErr := checkGatewayCase(r, false)
		reports[i] = qualityOutcome{ID: item.ID, Outcome: "completed", Report: &r}
		if runErr != nil || checkErr != nil {
			reports[i].Outcome = "failed"
			reports[i].Issue = fmt.Sprintf("run=%v; evidence=%v", runErr, checkErr)
			t.Errorf("%s: %s", item.ID, reports[i].Issue)
		}
		writeGatewayJSON(t, filepath.Join(out, "summary.json"), reports)
		t.Logf("case=%s outcome=%s input_bytes=%d callbacks=%d text=%q", item.ID, reports[i].Outcome, r.InputBytes, r.CallbackUpdates, r.Transcript)
		if r.ActiveAfterCleanup != 0 || r.NativeClosed != i+1 || len(observer.traces()) != i+1 {
			break // 清理证据不足时不继续叠加会话，未运行项仍保留为 unknown。
		}
	}
	g.StopAccepting()
	waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := g.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
	process.stop(t)
	writeGatewayJSON(t, filepath.Join(out, "process.json"), map[string]any{
		"process_exit": 0, "models_closed": nativeCloseCount(filepath.Join(out, "server.log")),
		"gateway_active_before_fixture_cleanup": g.Snapshot().ActiveSessions,
	})
}
