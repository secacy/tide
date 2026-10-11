package gateway

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
)

// formalGroupResumeCases 是 EXP-006-02 五组固定条件，-count=3 顺序执行三轮。
func formalGroupResumeCases() []groupResumeConfig {
	base := groupResumeConfig{Clients: 8, MaxHandshakes: 2, Chunks: 60, ChunkBytes: 3200, BarrierChunk: 20, Processing: 10 * time.Millisecond,
		ServerWindow: 10 * time.Second, RunTimeout: 15 * time.Second, CleanupTimeout: 15 * time.Second, SamplePeriod: 10 * time.Millisecond, SyncLimit: 50 * time.Millisecond,
		Resume: wsclient.V2ResumeConfig{MaxAttempts: 5, Window: 5 * time.Second, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}}
	names := []string{"A_normal", "B_resume_h8", "C_resume_h2", "D_busy_500ms", "E_busy_2s"}
	cases := make([]groupResumeConfig, len(names))
	for i, name := range names {
		cases[i] = base
		cases[i].Case = name
		cases[i].Fault = i != 0
	}
	cases[1].MaxHandshakes = 8
	cases[3].Busy = 500 * time.Millisecond
	cases[4].Busy = 2 * time.Second
	return cases
}

// TestGroupResumeBaselineExperiment 默认跳过；失败仍输出全部已获得事实，不调预算使假设成立。
func TestGroupResumeBaselineExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_GROUP_RESUME_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_GROUP_RESUME_EXPERIMENT=1")
	}
	if groupResumeRaceEnabled {
		t.Fatal("formal recovery measurements require a non-race build")
	}
	for _, cfg := range formalGroupResumeCases() {
		t.Run(cfg.Case, func(t *testing.T) {
			r := groupResumeReport{Version: 2, Config: cfg, ToolErrors: []string{"experiment did not return observations"}}
			for id := 1; id <= cfg.Clients; id++ {
				r.Sessions = append(r.Sessions, groupResumeSession{ID: id})
			}
			defer func() {
				data, err := json.Marshal(r)
				if err != nil {
					t.Errorf("encode formal group report: %v", err)
					return
				}
				t.Logf("GROUP_RESUME_JSON %s", data)
			}()
			r = runGroupResumeProbe(t, cfg)
			requireGroupEvidence(t, r)
			if cfg.Case == "A_normal" {
				for _, s := range r.Sessions {
					if !s.Observation.RunResult.Completed {
						t.Error("normal baseline session did not complete")
					}
				}
			}
		})
	}
}
