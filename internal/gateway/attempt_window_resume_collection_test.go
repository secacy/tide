package gateway

import (
	"errors"
	"os"
	"runtime"
	"testing"
	"time"
)

// attemptWindowCases 只改变恢复计数范围，复用固定音频、故障和记录器。
// 返回独立配置；probe 的 1s 回看不允许泄漏进 formal 的 60s。
func attemptWindowCases(probe bool) []extendedConfig {
	base := extendedCases(probe)[2]
	base.Experiment = "EXP-006-07"
	lifetime := base
	lifetime.Case = "L_lifetime_5"
	sliding := base
	sliding.Case = "S_sliding_5"
	sliding.FaultChunks = append([]int(nil), base.FaultChunks...)
	sliding.AttemptWindow = 60 * time.Second
	if probe {
		sliding.AttemptWindow = time.Second
	}
	return []extendedConfig{lifetime, sliding}
}

// attemptWindowCollectedConfig 固定 mode/case，正式入口必须明确启用。
func attemptWindowCollectedConfig(mode, name, enabled string) (extendedConfig, error) {
	if mode != "probe" && mode != "formal" {
		return extendedConfig{}, errors.New("invalid policy collection mode")
	}
	if mode == "formal" && enabled != "1" {
		return extendedConfig{}, errors.New("formal policy experiment must be explicitly enabled")
	}
	for _, cfg := range attemptWindowCases(mode == "probe") {
		if cfg.Case == name {
			return cfg, nil
		}
	}
	return extendedConfig{}, errors.New("unknown policy case")
}

// TestAttemptWindowResumeCollectedCase 默认跳过；独立采集器逐场启动同一非 race 二进制。
func TestAttemptWindowResumeCollectedCase(t *testing.T) {
	mode := os.Getenv("TIDE_ATTEMPT_WINDOW_MODE")
	if mode == "" {
		t.Skip("explicit policy collection only")
	}
	cfg, err := attemptWindowCollectedConfig(mode, os.Getenv("TIDE_ATTEMPT_WINDOW_CASE"), os.Getenv("TIDE_RUN_ATTEMPT_WINDOW_FORMAL"))
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("TIDE_ATTEMPT_WINDOW_OUTPUT")
	if dir == "" {
		t.Fatal("missing policy output")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Logf("environment go=%s os=%s arch=%s GOMAXPROCS=%d mode=%s case=%s", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), mode, cfg.Case)
	summary := runExtendedProbe(t, cfg, dir)
	if summary.LogError || summary.Forced || !summary.NaturalIdle {
		t.Error("policy collection lacks natural cleanup evidence")
	}
}

func TestAttemptWindowCollectedProfiles(t *testing.T) {
	for _, probe := range []bool{true, false} {
		mode := "formal"
		window := 60 * time.Second
		chunks := 6000
		if probe {
			mode = "probe"
			window = time.Second
			chunks = 120
		}
		for i, cfg := range attemptWindowCases(probe) {
			actual, err := attemptWindowCollectedConfig(mode, cfg.Case, "1")
			if err != nil || actual.validate() != nil || actual.Chunks != chunks || actual.Attempts != 5 || actual.Experiment != "EXP-006-07" {
				t.Fatalf("profile=%+v err=%v", actual, err)
			}
			want := time.Duration(0)
			if i == 1 {
				want = window
			}
			if actual.AttemptWindow != want {
				t.Fatalf("%s window=%v", mode, actual.AttemptWindow)
			}
		}
	}
	for _, tc := range [][3]string{{"formal", "S_sliding_5", "0"}, {"probe", "B_budget_8", "1"}, {"unknown", "L_lifetime_5", "1"}} {
		if _, err := attemptWindowCollectedConfig(tc[0], tc[1], tc[2]); err == nil {
			t.Fatal("invalid policy entry accepted")
		}
	}
	cfg := attemptWindowCases(true)[1]
	cfg.AttemptWindow = -1
	if cfg.validate() == nil {
		t.Fatal("negative window accepted")
	}
	cfg.AttemptWindow = 0
	cfg.Experiment = "unknown"
	if cfg.validate() == nil {
		t.Fatal("unknown experiment accepted")
	}
	copies := attemptWindowCases(true)
	copies[0].FaultChunks[0] = 99
	if copies[1].FaultChunks[0] != 10 || attemptWindowCases(true)[0].FaultChunks[0] != 10 {
		t.Fatal("profiles share mutable faults")
	}
}
