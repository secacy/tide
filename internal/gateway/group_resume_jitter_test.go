package gateway

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

var jitterGroupResumeOrder = [4][4]string{
	{"D_busy_500ms_none", "D_busy_500ms_jitter", "E_busy_2s_jitter", "E_busy_2s_none"},
	{"D_busy_500ms_jitter", "D_busy_500ms_none", "E_busy_2s_none", "E_busy_2s_jitter"},
	{"D_busy_500ms_none", "D_busy_500ms_jitter", "E_busy_2s_jitter", "E_busy_2s_none"},
	{"D_busy_500ms_jitter", "D_busy_500ms_none", "E_busy_2s_none", "E_busy_2s_jitter"},
}

// jitterGroupResumeCases 保持正式负载；probe 使用独立登记的短条件。
func jitterGroupResumeCases(round int, formal bool) []groupResumeConfig {
	var cases []groupResumeConfig
	for _, name := range jitterGroupResumeOrder[round] {
		index := 3
		if name[0] == 'E' {
			index = 4
		}
		cfg := formalGroupResumeCases()[index]
		if !formal {
			cfg = groupProbeConfig(name)
			cfg.Fault, cfg.Busy = true, 40*time.Millisecond
			if index == 4 {
				cfg.Busy = 300 * time.Millisecond
			}
		}
		cfg.Case = name
		cfg.Resume.MaxAttempts = 8
		cfg.Resume.Jitter = strings.HasSuffix(name, "_jitter")
		cases = append(cases, cfg)
	}
	return cases
}

// groupJitterPlan 是连续失败假设下的候选序列，不是实际休眠或请求时间。
type groupJitterPlan struct {
	ID        int             `json:"id"`
	Strategy  string          `json:"strategy"`
	Algorithm string          `json:"algorithm"`
	Seed      uint64          `json:"seed"`
	Delays    []time.Duration `json:"candidate_delays_ns"`
	Sum       time.Duration   `json:"candidate_sum_ns"`
}

func jitterGroupPlans(cfg groupResumeConfig, round int, formal bool) ([]groupJitterPlan, map[int]uint64) {
	var seeds map[int]uint64
	if cfg.Resume.Jitter {
		seeds = make(map[int]uint64)
	}
	plans := make([]groupJitterPlan, 0, cfg.Clients)
	for id := 1; id <= cfg.Clients; id++ {
		p := groupJitterPlan{ID: id, Strategy: "none", Algorithm: "deterministic_exponential_ns_v1"}
		var random *rand.Rand
		if cfg.Resume.Jitter {
			base := uint64(202610110000)
			if !formal {
				base = 202610120000 // probe 种子不替换正式种子。
			}
			p.Seed = base + uint64(100*round+id)
			p.Strategy, p.Algorithm = "symmetric_50", "pcg_symmetric50_ns_v1"
			seeds[id] = p.Seed
			random = rand.New(rand.NewPCG(p.Seed, p.Seed^0x9e3779b97f4a7c15))
		}
		base := cfg.Resume.InitialBackoff
		for range cfg.Resume.MaxAttempts - 1 {
			delay := base
			if random != nil {
				half := base / 2
				delay = base - half + time.Duration(random.Int64N(int64(2*half+1)))
			}
			p.Delays = append(p.Delays, delay)
			p.Sum += delay
			base = min(base*2, cfg.Resume.MaxBackoff) // 实验基础值固定且远小于溢出边界。
		}
		plans = append(plans, p)
	}
	return plans, seeds
}

func runGroupResumeJitterRounds(t *testing.T, formal bool) {
	t.Helper()
	rounds := 1
	if formal {
		rounds = 4
	}
	if value := os.Getenv("TIDE_GROUP_RESUME_JITTER_ROUNDS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 4 || formal && n != 4 {
			t.Fatal("jitter rounds must be 1..4 for probe or exactly 4 for formal")
		}
		rounds = n
	}
	marker := "GROUP_RESUME_JITTER_PROBE_JSON"
	if formal {
		marker = "GROUP_RESUME_JITTER_JSON"
	}
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round_%d", round+1), func(t *testing.T) {
			for _, cfg := range jitterGroupResumeCases(round, formal) {
				t.Run(cfg.Case, func(t *testing.T) {
					plans, seeds := jitterGroupPlans(cfg, round+1, formal)
					r := groupResumeReport{Version: 2, Config: cfg, ToolErrors: []string{"jitter probe did not return observations"}}
					defer func() {
						data, err := json.Marshal(struct {
							groupResumeReport
							Experiment string            `json:"experiment"`
							Round      int               `json:"round"`
							Plans      []groupJitterPlan `json:"retry_plans"`
						}{r, "EXP-006-04", round + 1, plans})
						if err != nil {
							t.Errorf("encode jitter report: %v", err)
							return
						}
						t.Logf("%s %s", marker, data)
					}()
					r = runGroupResumeWithSeeds(t, cfg, seeds)
					requireGroupEvidence(t, r)
				})
			}
		})
	}
}

func TestGroupResumeJitterExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_GROUP_RESUME_JITTER_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_GROUP_RESUME_JITTER_EXPERIMENT=1")
	}
	if groupResumeRaceEnabled {
		t.Fatal("formal jitter measurements require a non-race build")
	}
	runGroupResumeJitterRounds(t, true)
}

func TestGroupResumeJitterProbe(t *testing.T) { runGroupResumeJitterRounds(t, false) }

func TestGroupResumeJitterConditions(t *testing.T) {
	for _, formal := range []bool{false, true} {
		seen := make(map[uint64]bool)
		for round := range 4 {
			cases := jitterGroupResumeCases(round, formal)
			for pair := range 2 {
				a, b := cases[2*pair], cases[2*pair+1]
				if a.Resume.Jitter == b.Resume.Jitter || a.Resume.MaxAttempts != 8 || b.Resume.MaxAttempts != 8 {
					t.Fatal("pair is not none versus jitter with eight attempts")
				}
				a.Case, b.Case, a.Resume.Jitter, b.Resume.Jitter = "", "", false, false
				if a != b {
					t.Fatal("pair changed additional load settings")
				}
			}
			var first []groupJitterPlan
			for _, cfg := range cases {
				plans, seeds := jitterGroupPlans(cfg, round+1, formal)
				if !cfg.Resume.Jitter {
					if seeds != nil || plans[0].Seed != 0 {
						t.Fatal("deterministic group has seeds")
					}
					continue
				}
				if first != nil {
					for i, p := range plans {
						if p.Seed != first[i].Seed || !slices.Equal(p.Delays, first[i].Delays) {
							t.Fatal("D/E candidate sequence changed")
						}
					}
					continue
				}
				first = plans
				for _, p := range plans {
					if seen[p.Seed] || len(p.Delays) != 7 || p.Seed == 0 || seeds[p.ID] != p.Seed {
						t.Fatal("seed plan not unique or incomplete")
					}
					seen[p.Seed] = true
				}
			}
		}
		if len(seen) != 32 {
			t.Fatal("not four groups of eight distinct seeds")
		}
	}
}

// 随机化后仍可明确失败，不能把采样或时间窗口变成无限恢复。
func TestGroupResumeJitterLimits(t *testing.T) {
	for _, name := range []string{"attempts", "window"} {
		t.Run(name, func(t *testing.T) {
			cfg := jitterGroupResumeCases(0, false)[1]
			cfg.Case, cfg.Busy = "jitter_limit_"+name, 300*time.Millisecond
			if name == "attempts" {
				cfg.Resume.MaxAttempts = 2
			} else {
				cfg.Resume.Window = 10 * time.Millisecond // 第一档至少 10ms，实际清理和握手也消耗窗口。
			}
			_, seeds := jitterGroupPlans(cfg, 1, false)
			r := runGroupResumeWithSeeds(t, cfg, seeds)
			requireGroupEvidence(t, r)
			for _, s := range r.Sessions {
				obs := s.Observation
				if obs.RunResult.Completed || obs.RunErrorClass != "recovery_exhausted" || obs.RunResult.ResumedConnections != 0 {
					t.Fatalf("session %d result=%+v error=%s", s.ID, obs.RunResult, obs.RunErrorClass)
				}
				if name == "attempts" && obs.RunResult.ResumeAttempts != 2 || name == "window" && obs.RunResult.ResumeAttempts >= 8 {
					t.Fatal("random retries escaped configured budget")
				}
				if obs.ClientConfig.Resume.JitterSeed != seeds[s.ID] {
					t.Fatal("actual per-session seed differs from plan")
				}
			}
		})
	}
}
