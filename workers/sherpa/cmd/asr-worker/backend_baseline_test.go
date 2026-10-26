package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const backendInputSHA = "237ee4752b8a1fb41c6284ae94f2d7baf9a1fc8f28c26d3409c69e86cecbade1"

// backendSlot 是预登记槽位及有界摘要。详细文本只存逐场文件，空闲采样前释放。
// 所有 *NS 时刻相对 monitor 起点；nil 表示未发生/未观察，不以零冒充。
type backendSlot struct {
	Name          string              `json:"name"`
	Round         int                 `json:"round"`
	Cancel        bool                `json:"cancel"`
	State         string              `json:"state"` // not_run/running/passed/failed。
	Failure       string              `json:"failure,omitempty"`
	StartNS       *int64              `json:"start_ns"`
	CancelNS      *int64              `json:"cancel_ns"`
	CancelRead    *int64              `json:"source_read_at_cancel"`
	CancelSeq     *uint64             `json:"callback_seq_at_cancel"` // 此回调提交前，不能当作 AppliedSeq。
	ReturnNS      *int64              `json:"return_ns"`
	GatewayIdleNS *int64              `json:"gateway_idle_observed_ns"`
	IdleStartNS   *int64              `json:"idle_start_ns"`
	IdleEndNS     *int64              `json:"idle_end_ns"`
	Models        []backendModelEvent `json:"model_events"`
}

func backendSlots() []backendSlot {
	slots := []backendSlot{{Name: "warmup", State: "not_run"}}
	for round := 1; round <= 5; round++ {
		for _, cancel := range []bool{true, false} {
			kind := "normal"
			if cancel {
				kind = "cancel"
			}
			slots = append(slots, backendSlot{Name: fmt.Sprintf("r%d_%s", round, kind), Round: round, Cancel: cancel, State: "not_run"})
		}
	}
	return slots
}

func backendFileSHA(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// freezeBackendAssets 在启动进程、建立采样基线前核对模型与固定 PCM。
// 当前仅接受已接入 Worker 的旧模型；源码快照由实验目录的 prepare.py 冻结。
func freezeBackendAssets(modelDir string, pcm []byte) (map[string]string, error) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "experiments", "asrprobe", "model-assets.json"))
	if err != nil {
		return nil, err
	}
	var lock struct {
		Profiles map[string]struct {
			Files map[string]string `json:"files"`
		} `json:"profiles"`
	}
	if err := json.Unmarshal(data, &lock); err != nil {
		return nil, err
	}
	files := lock.Profiles["zipformer-bilingual-2023"].Files
	if len(files) != 4 {
		return nil, errors.New("missing original model asset lock")
	}
	for name, want := range files {
		got, err := backendFileSHA(filepath.Join(modelDir, name))
		if err != nil {
			return nil, err
		}
		if got != want {
			return nil, fmt.Errorf("model digest mismatch: %s", name)
		}
	}
	sum := sha256.Sum256(pcm)
	if len(pcm) != 321700 || hex.EncodeToString(sum[:]) != backendInputSHA {
		return nil, errors.New("unexpected baseline PCM")
	}
	return files, nil
}

// TestRealWorkerBackendBaseline 显式启用约三分钟实验；普通回归不加载模型。
// 一进程、一 Gateway、顺序会话，不重启、不缩短恢复窗口、不强制 GC。
func TestRealWorkerBackendBaseline(t *testing.T) {
	out := os.Getenv("TIDE_SHERPA_BACKEND_EVIDENCE_DIR")
	if out == "" {
		t.Skip("set TIDE_SHERPA_BACKEND_EVIDENCE_DIR to enable repeated real backend experiment")
	}
	bin, modelDir := os.Getenv("TIDE_SHERPA_WORKER_BIN"), os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if bin == "" || modelDir == "" {
		t.Fatal("explicit backend experiment requires Worker binary and model paths")
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Fatal("baseline is pinned to macOS arm64")
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "-race" && setting.Value == "true" {
				t.Fatal("runtime baseline requires non-race driver")
			}
		}
	}
	pcm, err := readTestPCM(filepath.Join(modelDir, "test_wavs", "0.wav"))
	if err != nil {
		t.Fatal(err)
	}
	models, err := freezeBackendAssets(modelDir, pcm)
	if err != nil {
		t.Fatal(err)
	}
	binSHA, err := backendFileSHA(bin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	slots := backendSlots()
	writeGatewayJSON(t, filepath.Join(out, "slots.json"), slots)
	writeGatewayJSON(t, filepath.Join(out, "conditions.json"), map[string]any{
		"schema": "tide.real-backend-baseline.v1", "rounds": 5, "warmup": 1,
		"max_sessions": 1, "max_pending_audio_bytes": 0, "resume_window_ns": int64(10 * time.Second),
		"max_audio_bytes": 960000, "max_chunk_bytes": 3200, "max_text_bytes": 4096,
		"normal_cleanup_budget_ns": int64(5 * time.Second), "cancel_cleanup_budget_ns": int64(15 * time.Second),
		"sample_interval_ns": int64(500 * time.Millisecond), "max_sample_gap_ns": int64(3 * time.Second),
		"warmup_idle_ns": int64(5 * time.Second), "case_idle_ns": int64(2 * time.Second), "final_idle_ns": int64(5 * time.Second),
		"input_bytes": len(pcm), "input_sha256": backendInputSHA, "model_files": models,
		"worker_sha256": binSHA, "go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH,
		"sdk": "sherpa-onnx-go-macos@v1.13.8", "threads": 2, "decoding": "greedy_search",
		"realtime": true, "client_resume": false, "forced_gc": false, "gateway_in_driver": true,
	})
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
	g, err := gateway.New(gatewayCtx, pool, gateway.Config{MaxSessions: 1, MaxPendingAudioBytes: 0, V2: &gateway.V2Config{}})
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
	m, err := newBackendMonitor(out, process.cmd.Process.Pid, g)
	if err != nil {
		t.Fatal(err)
	}
	monitorStopped := false
	t.Cleanup(func() {
		if !monitorStopped {
			if err := m.stop(); err != nil {
				t.Errorf("monitor cleanup: %v", err)
			}
		}
	})
	writeGatewayJSON(t, filepath.Join(out, "process-start.json"), map[string]any{
		"worker_pid": process.cmd.Process.Pid, "driver_pid": os.Getpid(), "started_at": m.start.Format(time.RFC3339Nano),
	})
	m.startSampling()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/v2/asr"
	for index := range slots {
		slot := &slots[index]
		slot.State = "running"
		phase := "normal"
		if slot.Cancel {
			phase = "cancel"
		}
		if index == 0 {
			phase = "warmup"
		}
		m.setPhase(phase, slot.Round)
		writeGatewayJSON(t, filepath.Join(out, "slots.json"), slots)
		hooks := gatewayCaseHooks{
			Started: func(at time.Time) { ns := at.Sub(m.start).Nanoseconds(); slot.StartNS = &ns },
			Canceled: func(at time.Time, read int64, seq uint64) {
				if slot.CancelNS == nil {
					ns := at.Sub(m.start).Nanoseconds()
					slot.CancelNS, slot.CancelRead, slot.CancelSeq = &ns, &read, &seq
				}
			},
			Returned: func(at time.Time) { ns := at.Sub(m.start).Nanoseconds(); slot.ReturnNS = &ns },
			WaitCleanup: func(expected int, budget time.Duration) error {
				return m.waitCleanup(expected, budget, &slot.GatewayIdleNS)
			},
		}
		r, runErr := runGatewayCaseObserved(t, g, observer, url, out, slot.Name, pcm, slot.Cancel, index+1, hooks)
		writeGatewayJSON(t, filepath.Join(out, slot.Name+"-case.json"), r)
		slot.Models = m.modelEvents(index + 1)
		failure := checkGatewayCase(r, slot.Cancel)
		if slot.Cancel {
			if !errors.Is(runErr, context.Canceled) || slot.CancelNS == nil {
				failure = errors.Join(failure, fmt.Errorf("expected callback cancellation, got %v", runErr))
			}
		} else {
			failure = errors.Join(failure, runErr)
			if r.FirstTextNS == nil || !r.FirstTextBeforeEnd {
				failure = errors.Join(failure, errors.New("speech did not arrive during input"))
			}
		}
		if len(slot.Models) != 2 || r.NativeClosed != index+1 {
			failure = errors.Join(failure, errors.New("native lifecycle pair missing"))
		}
		if failure == nil {
			failure = observer.releaseCompleted(index + 1)
		}
		if failure != nil {
			slot.State, slot.Failure = "failed", failure.Error()
			writeGatewayJSON(t, filepath.Join(out, "slots.json"), slots)
			t.Fatal(failure) // 不启动后续槽位；所有原始证据及未执行槽位保留。
		}
		slot.State = "passed"
		// 逐场函数返回后不把 report/consumer/trace 带入空闲窗口。
		r = gatewayCaseReport{}
		writeGatewayJSON(t, filepath.Join(out, "slots.json"), slots)
		runBackendIdle(t, m, out, slots, index)
		t.Logf("slot=%s state=%s model=%d naturally_closed", slot.Name, slot.State, index+1)
	}
	m.setPhase("final_idle", 5)
	time.Sleep(5 * time.Second)
	if err := m.stop(); err != nil {
		monitorStopped = true
		t.Fatal(err)
	}
	monitorStopped = true
	g.StopAccepting()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	process.stop(t)
	writeGatewayJSON(t, filepath.Join(out, "process.json"), map[string]any{
		"process_exit": 0, "models_created": m.created, "models_closed": m.closed,
		"gateway_active_before_fixture_cleanup": g.Snapshot().ActiveSessions,
		"worker_pid":                            process.cmd.Process.Pid, "driver_pid": os.Getpid(),
	})
}

// runBackendIdle 在调用者不再持有逐场 report 的位置观察，避免人为保留全文。
func runBackendIdle(t *testing.T, m *backendMonitor, out string, slots []backendSlot, index int) {
	phase, duration := "idle", 2*time.Second
	if index == 0 {
		phase, duration = "warmup_idle", 5*time.Second
	}
	m.setPhase(phase, slots[index].Round)
	start := time.Since(m.start).Nanoseconds()
	slots[index].IdleStartNS = &start
	time.Sleep(duration)
	end := time.Since(m.start).Nanoseconds()
	slots[index].IdleEndNS = &end
	writeGatewayJSON(t, filepath.Join(out, "slots.json"), slots)
}
