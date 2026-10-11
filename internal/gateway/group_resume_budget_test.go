package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// budgetGroupResumeOrder 固定相邻对照的先后次序；每轮都使用全新夹具。
var budgetGroupResumeOrder = [3][4]string{
	{"D_busy_500ms_a5", "D_busy_500ms_a8", "E_busy_2s_a8", "E_busy_2s_a5"},
	{"D_busy_500ms_a8", "D_busy_500ms_a5", "E_busy_2s_a5", "E_busy_2s_a8"},
	{"D_busy_500ms_a5", "D_busy_500ms_a8", "E_busy_2s_a8", "E_busy_2s_a5"},
}

// budgetGroupResumeCases 仅改变尝试上限；probe 的缩短条件不能作为正式测量。
func budgetGroupResumeCases(round int, formal bool) []groupResumeConfig {
	cases := make([]groupResumeConfig, 0, 4)
	for _, name := range budgetGroupResumeOrder[round] {
		index := 3
		if name[0] == 'E' {
			index = 4
		}
		cfg := formalGroupResumeCases()[index]
		if !formal {
			cfg = groupProbeConfig(name)
			cfg.Fault = true
			cfg.Busy = 40 * time.Millisecond
			if index == 4 {
				cfg.Busy = 300 * time.Millisecond
			}
		}
		cfg.Case = name
		cfg.Resume.MaxAttempts = int(name[len(name)-1] - '0')
		cases = append(cases, cfg)
	}
	return cases
}

// runGroupResumeBudgetRounds 输出独立标记，并保留 setup Fatal 或业务失败的报告。
// rounds 环境变量仅由采集器设置；普通 probe 默认一轮，正式入口固定三轮。
func runGroupResumeBudgetRounds(t *testing.T, formal bool) {
	t.Helper()
	rounds := 1
	if formal {
		rounds = 3
	}
	if value := os.Getenv("TIDE_GROUP_RESUME_BUDGET_ROUNDS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 3 || formal && n != 3 {
			t.Fatal("budget rounds must be 1..3 for probe or exactly 3 for formal")
		}
		rounds = n
	}
	marker := "GROUP_RESUME_BUDGET_PROBE_JSON"
	if formal {
		marker = "GROUP_RESUME_BUDGET_JSON"
	}
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round_%d", round+1), func(t *testing.T) {
			for _, cfg := range budgetGroupResumeCases(round, formal) {
				t.Run(cfg.Case, func(t *testing.T) {
					r := groupResumeReport{Version: 2, Config: cfg, ToolErrors: []string{"budget experiment did not return observations"}}
					for id := 1; id <= cfg.Clients; id++ {
						r.Sessions = append(r.Sessions, groupResumeSession{ID: id})
					}
					defer func() {
						data, err := json.Marshal(struct {
							groupResumeReport
							Experiment string `json:"experiment"`
							Round      int    `json:"round"`
						}{r, "EXP-006-03", round + 1})
						if err != nil {
							t.Errorf("encode budget report: %v", err)
							return
						}
						t.Logf("%s %s", marker, data)
					}()
					r = runGroupResumeProbe(t, cfg)
					requireGroupEvidence(t, r)
				})
			}
		})
	}
}

func TestGroupResumeBudgetExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_GROUP_RESUME_BUDGET_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_GROUP_RESUME_BUDGET_EXPERIMENT=1")
	}
	if groupResumeRaceEnabled {
		t.Fatal("formal budget measurements require a non-race build")
	}
	runGroupResumeBudgetRounds(t, true)
}

func TestGroupResumeBudgetProbe(t *testing.T) {
	runGroupResumeBudgetRounds(t, false)
}

func TestGroupResumeBudgetConditions(t *testing.T) {
	for _, formal := range []bool{false, true} {
		for round := range 3 {
			cases := budgetGroupResumeCases(round, formal)
			for pair := 0; pair < 2; pair++ {
				a, b := cases[pair*2], cases[pair*2+1]
				if a.Resume.MaxAttempts+b.Resume.MaxAttempts != 13 || a.Resume.MaxAttempts == b.Resume.MaxAttempts {
					t.Fatal("pair is not five versus eight")
				}
				a.Case, b.Case = "", ""
				a.Resume.MaxAttempts, b.Resume.MaxAttempts = 5, 5
				if a != b {
					t.Fatal("pair changed more than attempts")
				}
				if err := a.validate(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if formalGroupResumeCases()[4].Resume.MaxAttempts != 5 {
		t.Fatal("historical baseline conditions changed")
	}
}

// 两种保护分开验证：八次仍会耗尽，增加次数也不能越过时间窗口。
func TestGroupResumeBudgetLimits(t *testing.T) {
	for _, name := range []string{"attempts", "window"} {
		t.Run(name, func(t *testing.T) {
			cfg := groupProbeConfig("budget_limit_" + name)
			cfg.Fault, cfg.Busy, cfg.Resume.MaxAttempts = true, 700*time.Millisecond, 8
			if name == "window" {
				cfg.Resume.Window = 70 * time.Millisecond
			}
			r := runGroupResumeProbe(t, cfg)
			requireGroupEvidence(t, r)
			for _, s := range r.Sessions {
				obs := s.Observation
				if obs.RunResult.Completed || obs.RunErrorClass != "recovery_exhausted" {
					t.Fatalf("session %d did not explicitly fail: %+v", s.ID, obs.RunResult)
				}
				if name == "attempts" && obs.RunResult.ResumeAttempts != 8 {
					t.Fatal("did not exercise all eight attempts")
				}
				if name == "window" && obs.RunResult.ResumeAttempts >= 8 {
					t.Fatal("window did not bound attempts")
				}
			}
		})
	}
}
