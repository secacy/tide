package gateway

import (
	"bytes"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsclient"
)

// formalResumeCases 是预登记五组，-count=3 顺序得到三轮，共 15 场。
func formalResumeCases() []resumeExperimentConfig {
	base := resumeExperimentConfig{Chunks: 60, ChunkBytes: 3200, Processing: 10 * time.Millisecond, ClientWindow: 5 * time.Second, ServerWindow: 10 * time.Second, RunTimeout: 15 * time.Second, CleanupTimeout: 15 * time.Second, SamplePeriod: 10 * time.Millisecond, CatchupThreshold: 200 * time.Millisecond}
	cases := make([]resumeExperimentConfig, 5)
	for i := range cases {
		cases[i] = base
	}
	cases[0].Case = "A_normal"
	cases[1].Case = "B_disabled_500ms"
	cases[1].CutChunk = 20
	cases[1].Outage = 500 * time.Millisecond
	cases[2].Case = "C_resume_500ms"
	cases[2].CutChunk = 20
	cases[2].Outage = 500 * time.Millisecond
	cases[2].Resume = true
	cases[3].Case = "D_resume_2s"
	cases[3].CutChunk = 20
	cases[3].Outage = 2 * time.Second
	cases[3].Resume = true
	cases[4].Case = "E_over_window_6s"
	cases[4].CutChunk = 20
	cases[4].Outage = 6 * time.Second
	cases[4].Resume = true
	return cases
}

// TestClientResumeBaselineExperiment 默认跳过，正式性能样本不能启用 race 或虚拟时间。
// 所有场合先输出报告再报错；失败不会从分母中消失，后续组继续执行。
func TestClientResumeBaselineExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_CLIENT_RESUME_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_CLIENT_RESUME_EXPERIMENT=1")
	}
	t.Logf("environment go=%s os=%s arch=%s cpu=%d GOMAXPROCS=%d transport=loopback-TCP same-process", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	for _, cfg := range formalResumeCases() {
		t.Run(cfg.Case, func(t *testing.T) {
			// 即使建环境阶段 Fatal，也保留这次计划尝试及“未取得报告”的事实。
			report := resumeExperimentReport{Version: 1, Config: cfg, ToolErrors: []string{"experiment did not return a report"}}
			defer func() {
				data, err := json.Marshal(report)
				if err != nil {
					t.Errorf("encode report: %v", err)
					return
				}
				t.Logf("CLIENT_RESUME_JSON %s", data)
			}()
			report = runResumeExperiment(t, cfg)
			requireResumeEvidence(t, report)
		})
	}
}

func requireResumeEvidence(t *testing.T, r resumeExperimentReport) {
	t.Helper()
	if len(r.EvidenceErrors) != 0 || len(r.AcceptanceErrors) != 0 {
		t.Errorf("evidence=%v acceptance=%v run=%s", r.EvidenceErrors, r.AcceptanceErrors, r.RunErrorClass)
	}
}

// TestClientResumeExperimentProbe 经过真实公开入口，只验收工具，不提供恢复性能数字。
func TestClientResumeExperimentProbe(t *testing.T) {
	for i, cfg := range formalResumeCases() {
		cfg.Chunks = 8
		cfg.CutChunk = 3
		if i == 0 {
			cfg.CutChunk = 0
		}
		cfg.Processing = time.Millisecond
		cfg.ClientWindow = 500 * time.Millisecond
		cfg.ServerWindow = 800 * time.Millisecond
		cfg.RunTimeout = 5 * time.Second
		cfg.CleanupTimeout = 3 * time.Second
		cfg.SamplePeriod = time.Millisecond
		switch i {
		case 1, 2:
			cfg.Outage = 20 * time.Millisecond
		case 3:
			cfg.Outage = 250 * time.Millisecond
		case 4:
			cfg.Outage = 700 * time.Millisecond
			cfg.ClientWindow = 150 * time.Millisecond
			cfg.ServerWindow = 300 * time.Millisecond
		}
		t.Run(cfg.Case, func(t *testing.T) {
			r := runResumeExperiment(t, cfg)
			requireResumeEvidence(t, r)
			if i == 2 || i == 3 {
				if r.Metrics.RepeatedTransferBytes != 3200 {
					t.Errorf("observed retransmission=%d want 3200", r.Metrics.RepeatedTransferBytes)
				}
				if r.Metrics.FreshAudioEnd == nil || *r.Metrics.FreshAudioEnd <= uint64(cfg.CutChunk*cfg.ChunkBytes) {
					t.Error("old audio treated as fresh")
				}
			}
			if i == 3 {
				capturedDuringOutage := false
				for _, read := range r.SourceReads {
					if read.End > read.Begin && r.CutAt != nil && r.OpenedAt != nil && read.StartedAt.After(*r.CutAt) && read.StartedAt.Before(*r.OpenedAt) {
						capturedDuringOutage = true
					}
				}
				if !capturedDuringOutage {
					t.Error("source did not continue capture while resume was blocked")
				}
			}
			if i == 4 {
				for _, h := range r.Handshakes {
					if h.Kind == "resume" && h.ForwardedAt != nil {
						t.Error("expired client handshake forwarded after reopening")
					}
				}
			}
			encoded, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(encoded, []byte("resume_token")) || bytes.Contains(encoded, []byte("session_id")) {
				t.Error("report contains credentials")
			}
			var decoded resumeExperimentReport
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			analyzeResumeReport(&decoded)
			requireResumeEvidence(t, decoded)
			if decoded.BusinessCompleted != r.BusinessCompleted {
				t.Error("report does not survive JSON round trip")
			}
		})
	}
}

// 自然清理超时后才 stop 的场景，不能因兜底归零而改写自然达标。
func TestClientResumeExperimentForcedCleanup(t *testing.T) {
	cfg := formalResumeCases()[1]
	cfg.Chunks = 8
	cfg.CutChunk = 2
	cfg.Outage = 20 * time.Millisecond
	cfg.Processing = time.Millisecond
	cfg.ServerWindow = 500 * time.Millisecond
	cfg.RunTimeout = 3 * time.Second
	cfg.CleanupTimeout = 20 * time.Millisecond
	cfg.SamplePeriod = time.Millisecond
	r := runResumeExperiment(t, cfg)
	if !r.ForcedCleanup || r.NaturalTasksExited || r.Metrics.NaturalIdleAt != nil || r.BusinessCompleted {
		t.Fatalf("forced cleanup presented as natural: forced=%v natural=%v metrics=%+v", r.ForcedCleanup, r.NaturalTasksExited, r.Metrics)
	}
	if !slicesContain(r.AcceptanceErrors, "natural cleanup did not complete") {
		t.Fatal("missing natural cleanup failure")
	}
	if r.Worker.ExitedAt == nil || r.SourceCloses != 1 {
		t.Fatal("forced cleanup did not join source/Worker")
	}
}
func slicesContain(items []string, needle string) bool {
	for _, s := range items {
		if s == needle {
			return true
		}
	}
	return false
}

func TestClientResumeExperimentRanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ranges []resumeRange
		want   uint64
	}{
		{"empty", nil, 0}, {"contiguous", []resumeRange{{0, 4}, {4, 8}}, 0}, {"exact replay", []resumeRange{{0, 4}, {4, 8}, {4, 8}}, 4}, {"partial overlap", []resumeRange{{0, 4}, {2, 6}, {3, 5}}, 4}, {"unsorted and gap", []resumeRange{{8, 12}, {0, 4}, {10, 16}}, 2}, {"contained", []resumeRange{{0, 12}, {2, 6}}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frames := make([]resumeFrame, len(tc.ranges))
			for i, r := range tc.ranges {
				frames[i].resumeRange = r
			}
			if got := resumeRepeatedBytes(frames); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
			for i, r := range tc.ranges {
				if frames[i].resumeRange != r {
					t.Fatal("input mutated")
				}
			}
		})
	}
}

// syntheticResumeReport 用固定事实检验计时算法；这些数字不是网络测量。
func syntheticResumeReport() resumeExperimentReport {
	cfg := formalResumeCases()[2]
	cfg.Chunks = 8
	cfg.CutChunk = 3
	cfg.Outage = 500 * time.Millisecond
	origin := time.Unix(10000, 0)
	total := uint64(cfg.Chunks * cfg.ChunkBytes)
	input := resumePattern(int(total))
	r := resumeExperimentReport{Version: 1, Config: cfg, StartedAt: origin, ReturnedAt: resumeAt(origin.Add(time.Second)), CutAt: resumeAt(origin.Add(250 * time.Millisecond)), OpenedAt: resumeAt(origin.Add(750 * time.Millisecond)), SourceCloses: 1, WorkerPicks: 1, NaturalTasksExited: true,
		RunResult: wsclient.V2RunResult{Completed: true, CompletionAckWritten: true, FinalOffset: total, AppliedSeq: 9, ResumeAttempts: 1, ResumedConnections: 1}, Worker: resumeWorkerObservation{Calls: 1, Audio: input}, Handshakes: []resumeHandshake{{Kind: "start"}, {Kind: "resume"}}, Readies: []resumeReady{{Connection: 2, WrittenAt: origin.Add(760 * time.Millisecond)}}}
	r.InputSHA256 = resumeDigest(input)
	r.Worker.StartedAt = resumeAt(origin)
	r.Worker.ExitedAt = resumeAt(origin.Add(950 * time.Millisecond))
	r.Worker.TailSentAt = resumeAt(origin.Add(900 * time.Millisecond))
	for i := 0; i < cfg.Chunks; i++ {
		begin, end := uint64(i*cfg.ChunkBytes), uint64((i+1)*cfg.ChunkBytes)
		at := origin.Add(time.Duration(i) * 100 * time.Millisecond)
		r.SourceReads = append(r.SourceReads, resumeSourceRead{resumeRange: resumeRange{begin, end}, StartedAt: at, FinishedAt: at})
		r.Frames = append(r.Frames, resumeFrame{resumeRange: resumeRange{begin, end}, Connection: 1, ReadAt: at, SHA256: resumeDigest(input[begin:end]), Dropped: i == 2})
		r.Worker.Chunks = append(r.Worker.Chunks, resumeWorkerChunk{resumeRange: resumeRange{begin, end}, ReceivedAt: at})
		received := at.Add(10 * time.Millisecond)
		if i == 2 {
			received = origin.Add(780 * time.Millisecond)
		}
		if i >= 3 {
			received = origin.Add(time.Duration(800+(i-3)*10) * time.Millisecond)
		}
		r.Applications = append(r.Applications, resumeApplication{Seq: uint64(i + 1), AudioEnd: end, ReceivedAt: received, AppliedAt: received})
		setSyntheticReception(&r, i, received)
	}
	r.Applications = append(r.Applications, resumeApplication{Seq: 9, AudioEnd: total, Final: true, ReceivedAt: origin.Add(900 * time.Millisecond), AppliedAt: origin.Add(900 * time.Millisecond)})
	r.Samples = []resumeResourceSample{{BeginAt: origin.Add(-time.Millisecond), At: origin}, {BeginAt: origin.Add(time.Second), At: origin.Add(time.Second)}}
	return r
}

// setSyntheticReception 保持对应处理/接收时间有序，构造计时算法所需的固定事实。
func setSyntheticReception(r *resumeExperimentReport, i int, at time.Time) {
	r.Applications[i].ReceivedAt = at
	r.Applications[i].AppliedAt = at
	r.Worker.Chunks[i].ReceivedAt = at.Add(-2 * time.Millisecond)
	r.Worker.Chunks[i].ProcessedAt = resumeAt(at.Add(-time.Millisecond))
	r.Worker.Chunks[i].ProgressSentAt = resumeAt(at)
	r.Worker.Chunks[i].ResultSentAt = resumeAt(at)
}

func TestClientResumeExperimentTiming(t *testing.T) {
	t.Run("fresh audio excludes old buffered results", func(t *testing.T) {
		r := syntheticResumeReport()
		analyzeResumeReport(&r)
		m := r.Metrics
		if m.FreshAudioEnd == nil || *m.FreshAudioEnd != 12800 || m.FreshAfterCutMS == nil || *m.FreshAfterCutMS != 550 || m.FreshAfterOpenMS == nil || *m.FreshAfterOpenMS != 50 {
			t.Fatalf("fresh metrics=%+v", m)
		}
		if m.ReadyAfterOpenMS == nil || *m.ReadyAfterOpenMS != 10 {
			t.Fatal("incorrect ready metric")
		}
		if m.CatchupAfterOpenMS != nil {
			t.Fatal("single low-latency result called catchup")
		}
		if m.CleanupWaitMS == nil || *m.CleanupWaitMS != 0 {
			t.Fatal("pre-session idle used as cleanup")
		}
	})
	t.Run("three consecutive low-latency results", func(t *testing.T) {
		r := syntheticResumeReport()
		r.Config.CatchupThreshold = 50 * time.Millisecond // 人工计时样例；正式组仍固定 200ms。
		setSyntheticReception(&r, 2, r.StartedAt.Add(305*time.Millisecond))
		for i := 3; i < 8; i++ {
			at := r.SourceReads[i].StartedAt.Add(10 * time.Millisecond)
			setSyntheticReception(&r, i, at)
		}
		r.OpenedAt = resumeAt(r.StartedAt.Add(300 * time.Millisecond))
		analyzeResumeReport(&r)
		if r.Metrics.CatchupAfterOpenMS == nil || *r.Metrics.CatchupAfterOpenMS != 210 {
			t.Fatalf("catchup=%v", r.Metrics.CatchupAfterOpenMS)
		}
	})
	t.Run("miss interrupts streak", func(t *testing.T) {
		r := syntheticResumeReport()
		r.Config.CatchupThreshold = 50 * time.Millisecond
		setSyntheticReception(&r, 2, r.StartedAt.Add(305*time.Millisecond))
		r.OpenedAt = resumeAt(r.StartedAt.Add(300 * time.Millisecond))
		for i := 3; i < 8; i++ {
			at := r.SourceReads[i].StartedAt.Add(10 * time.Millisecond)
			if i == 4 {
				at = at.Add(50 * time.Millisecond)
			}
			setSyntheticReception(&r, i, at)
		}
		analyzeResumeReport(&r)
		if r.Metrics.CatchupAfterOpenMS == nil || *r.Metrics.CatchupAfterOpenMS != 410 {
			t.Fatalf("catchup=%v", r.Metrics.CatchupAfterOpenMS)
		}
	})
	t.Run("absent events are null", func(t *testing.T) {
		r := syntheticResumeReport()
		r.CutAt = nil
		r.OpenedAt = nil
		r.Readies = nil
		analyzeResumeReport(&r)
		if r.Metrics.FreshAfterCutMS != nil || r.Metrics.ReadyAfterCutMS != nil || r.Metrics.CatchupAfterOpenMS != nil {
			t.Fatal("missing event presented as zero latency")
		}
		data, _ := json.Marshal(r.Metrics)
		if !strings.Contains(string(data), `"fresh_after_cut_ms":null`) {
			t.Fatal("missing event not encoded as null")
		}
	})
	t.Run("no new captures no fresh sample", func(t *testing.T) {
		r := syntheticResumeReport()
		r.CutAt = resumeAt(r.StartedAt.Add(time.Second))
		analyzeResumeReport(&r)
		if r.Metrics.FreshAudioEnd != nil || r.Metrics.FreshAfterCutMS != nil {
			t.Fatal("invented fresh audio sample")
		}
	})
}

func TestClientResumeExperimentRejectsContradictions(t *testing.T) {
	baseline := syntheticResumeReport()
	analyzeResumeReport(&baseline)
	requireResumeEvidence(t, baseline)
	for _, tc := range []struct {
		name   string
		mutate func(*resumeExperimentReport)
	}{
		{"duplicate application", func(r *resumeExperimentReport) { r.Applications[1].Seq = 1 }},
		{"sequence gap", func(r *resumeExperimentReport) { r.Applications[1].Seq = 3 }},
		{"wrong result audio position", func(r *resumeExperimentReport) { r.Applications[1].AudioEnd += 2 }},
		{"Worker audio reordered", func(r *resumeExperimentReport) { r.Worker.Audio[0] ^= 1 }},
		{"proxy content corrupted", func(r *resumeExperimentReport) { r.Frames[0].SHA256 = "wrong" }},
		{"source range gap", func(r *resumeExperimentReport) { r.SourceReads[1].Begin += 2 }},
		{"dropped but forwarded", func(r *resumeExperimentReport) { r.Frames[2].Forwarded = true }},
		{"multiple Close", func(r *resumeExperimentReport) { r.SourceCloses = 2 }},
		{"second Worker stream", func(r *resumeExperimentReport) { r.Worker.Calls = 2 }},
		{"application before receive", func(r *resumeExperimentReport) {
			r.Applications[0].AppliedAt = r.Applications[0].ReceivedAt.Add(-time.Second)
		}},
		{"receive time reversed", func(r *resumeExperimentReport) {
			r.Applications[1].ReceivedAt = r.Applications[0].ReceivedAt.Add(-time.Millisecond)
		}},
		{"result before processing", func(r *resumeExperimentReport) {
			r.Worker.Chunks[0].ProcessedAt = resumeAt(r.Applications[0].ReceivedAt.Add(time.Millisecond))
		}},
		{"Worker processing before receive", func(r *resumeExperimentReport) {
			r.Worker.Chunks[0].ProcessedAt = resumeAt(r.Worker.Chunks[0].ReceivedAt.Add(-time.Millisecond))
		}},
		{"Worker exit missing", func(r *resumeExperimentReport) { r.Worker.ExitedAt = nil }},
		{"input hash mismatch", func(r *resumeExperimentReport) { r.InputSHA256 = "wrong" }},
		{"completed sequence mismatch", func(r *resumeExperimentReport) { r.RunResult.AppliedSeq = 1 }},
		{"source overrun", func(r *resumeExperimentReport) { r.SourceReads[7].End += 3200; r.Frames[7].End += 3200 }},
		{"resource count negative", func(r *resumeExperimentReport) { r.Samples[0].Sessions = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := syntheticResumeReport()
			tc.mutate(&r)
			analyzeResumeReport(&r)
			if len(r.EvidenceErrors) == 0 || r.BusinessCompleted {
				t.Fatalf("contradiction accepted: %s", tc.name)
			}
		})
	}
}

func TestClientResumeExperimentSnapshot(t *testing.T) {
	r := syntheticResumeReport()
	p := &resumeRecorder{report: r}
	copy := p.snapshot()
	p.change(func(live *resumeExperimentReport) {
		live.Worker.Audio[0] ^= 1
		live.Worker.Chunks[0].End += 2
		live.Frames[0].End += 2
		live.SourceReads[0].End += 2
		live.Applications[0].Seq = 99
		live.Handshakes[0].Kind = "resume"
		live.Readies[0].Generation = 99
		live.Samples[0].Sessions = 1
		live.ToolErrors = append(live.ToolErrors, "later error")
	})
	analyzeResumeReport(&copy)
	requireResumeEvidence(t, copy)
	if !copy.BusinessCompleted {
		t.Fatal("live observations altered frozen report")
	}
}

func TestClientResumeExperimentFailedAudioRanges(t *testing.T) {
	r := syntheticResumeReport()
	r.SourceReads = r.SourceReads[:5]
	r.Frames = r.Frames[:3]
	r.Worker.Audio = r.Worker.Audio[:2*3200]
	r.Worker.Chunks = r.Worker.Chunks[:2]
	r.Applications = r.Applications[:2]
	r.RunResult = wsclient.V2RunResult{}
	r.RunErrorClass = "incomplete"
	analyzeResumeReport(&r)
	if !r.Metrics.WorkerIsPrefix || r.Metrics.MissingCaptured == nil || *r.Metrics.MissingCaptured != (resumeRange{6400, 16000}) || r.Metrics.Uncaptured == nil || *r.Metrics.Uncaptured != (resumeRange{16000, 25600}) {
		t.Fatalf("failed input ranges=%+v", r.Metrics)
	}
	if r.BusinessCompleted {
		t.Fatal("partial input treated as complete")
	}
}

func TestClientResumeExperimentConfiguration(t *testing.T) {
	for _, c := range formalResumeCases() {
		if err := validateResumeConfig(c); err != nil {
			t.Fatal(err)
		}
		if c.Chunks*c.ChunkBytes != 6*audio.BytesPerSecond || c.RunTimeout != 15*time.Second || c.ServerWindow != 10*time.Second || c.ClientWindow != 5*time.Second {
			t.Fatal("formal config diverged from registration")
		}
	}
	for _, mutate := range []func(*resumeExperimentConfig){func(c *resumeExperimentConfig) { c.Chunks = 0 }, func(c *resumeExperimentConfig) { c.ChunkBytes = 2 }, func(c *resumeExperimentConfig) { c.CutChunk = 100 }, func(c *resumeExperimentConfig) { c.Outage = -1 }, func(c *resumeExperimentConfig) { c.SamplePeriod = 0 }, func(c *resumeExperimentConfig) { c.CutChunk = 0 }} {
		c := formalResumeCases()[2]
		mutate(&c)
		if validateResumeConfig(c) == nil {
			t.Fatal("invalid config accepted")
		}
	}
}
