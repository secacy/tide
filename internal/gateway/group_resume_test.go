package gateway

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// groupProbeConfig tests shared admission and retry behavior with shortened input/retention.
// Probe timing and eight-client sample counts are not formal recovery performance results.
func groupProbeConfig(name string) groupResumeConfig {
	return groupResumeConfig{Case: name, Clients: 8, MaxHandshakes: 2, Chunks: 8, ChunkBytes: 3200, BarrierChunk: 3,
		Processing: time.Millisecond, ServerWindow: time.Second, RunTimeout: 5 * time.Second, CleanupTimeout: 2 * time.Second,
		SamplePeriod: 2 * time.Millisecond, SyncLimit: 500 * time.Millisecond,
		Resume: wsclient.V2ResumeConfig{MaxAttempts: 5, Window: 2 * time.Second, InitialBackoff: 20 * time.Millisecond, MaxBackoff: 80 * time.Millisecond}}
}

func logGroupProbe(t *testing.T, report *groupResumeReport) {
	t.Helper()
	data, err := json.Marshal(report)
	if err != nil {
		t.Errorf("encode group probe: %v", err)
		return
	}
	t.Logf("GROUP_RESUME_PROBE_JSON %s", data)
}

func requireGroupEvidence(t *testing.T, report groupResumeReport) {
	t.Helper()
	if len(report.ToolErrors) != 0 || len(report.EvidenceErrors) != 0 {
		t.Fatalf("group evidence tool=%v evidence=%v", report.ToolErrors, report.EvidenceErrors)
	}
}

func TestGroupResumeSharedGatewayProbe(t *testing.T) {
	for _, name := range []string{"A_normal", "B_resume_h8", "C_resume_h2", "D_busy_short", "E_attempt_exhausted"} {
		t.Run(name, func(t *testing.T) {
			cfg := groupProbeConfig(name)
			cfg.Fault = name != "A_normal"
			if name == "B_resume_h8" {
				cfg.MaxHandshakes = cfg.Clients
			}
			if name == "D_busy_short" {
				cfg.Busy = 40 * time.Millisecond
			}
			if name == "E_attempt_exhausted" {
				cfg.Busy = 200 * time.Millisecond
				cfg.Resume.MaxAttempts = 2
			}
			report := groupResumeReport{Version: 2, Config: cfg, ToolErrors: []string{"probe did not return observations"}}
			defer logGroupProbe(t, &report) // Includes unexpected failure and setup Fatal.
			report = runGroupResumeProbe(t, cfg)
			requireGroupEvidence(t, report)
			if len(report.Sessions) != 8 || report.Picks != 8 || report.WorkerCalls != 8 {
				t.Fatal("not eight sessions on the shared Gateway")
			}
			rejections := 0
			for _, a := range report.Attempts {
				if a.Code == wsprotocol.V2ErrorHandshakeLimit && a.CodeWritten {
					rejections++
				}
				if a.Connection > 1 && a.Kind != "resume" {
					t.Fatal("recovery created another start")
				}
			}
			if cfg.Busy > 0 && rejections == 0 {
				t.Fatal("controlled gate occupancy did not reject any actual resume")
			}
			for _, session := range report.Sessions {
				r := session.Observation
				if name == "E_attempt_exhausted" {
					if r.RunResult.Completed || r.RunErrorClass != "recovery_exhausted" || r.RunResult.ResumeAttempts != cfg.Resume.MaxAttempts || r.RunResult.ResumedConnections != 0 || session.LastEntryCode != wsprotocol.V2ErrorHandshakeLimit {
						t.Fatalf("session %d failure=%+v code=%s error=%s", session.ID, r.RunResult, session.LastEntryCode, r.RunErrorClass)
					}
					if len(r.Worker.Audio) != (cfg.BarrierChunk-1)*cfg.ChunkBytes || len(r.Applications) != cfg.BarrierChunk-1 {
						t.Fatal("unexpected failed-session audio/results")
					}
					if r.ReturnedAt.Sub(*report.FaultAt) >= cfg.Resume.Window {
						t.Fatal("did not demonstrate attempt exhaustion before the window")
					}
				} else {
					if r.RunErrorClass != "" || !r.RunResult.Completed || !r.RunResult.CompletionAckWritten {
						t.Fatalf("session %d result=%+v error=%s", session.ID, r.RunResult, r.RunErrorClass)
					}
					if cfg.Fault && r.RunResult.ResumedConnections < 1 {
						t.Fatal("fault batch did not resume original session")
					}
					if !cfg.Fault && r.RunResult.ResumeAttempts != 0 {
						t.Fatal("normal batch resumed")
					}
				}
			}
			t.Logf("group=%s planned=%d picks=%d RPCs=%d gate_rejections=%d natural_cleanup=%t", name, cfg.Clients, report.Picks, report.WorkerCalls, rejections, report.NaturalIdleAt != nil)
		})
	}
}

// Raw-report counterexamples prove that the observer catches incorrect claims, not just happy-path output.
func TestGroupResumeEvidenceCounterexamples(t *testing.T) {
	cfg := groupProbeConfig("counterexample_control")
	report := runGroupResumeProbe(t, cfg)
	requireGroupEvidence(t, report)
	cases := []struct {
		name, expected string
		mutate         func(*groupResumeReport)
	}{
		{"missing_session", "denominator", func(r *groupResumeReport) { r.Sessions = r.Sessions[:7] }},
		{"duplicate_identity", "duplicate session identity", func(r *groupResumeReport) { r.Sessions[1].ID = 1 }},
		{"missing_observation_slots", "slot count", func(r *groupResumeReport) { r.CloseFinishedAt = nil }},
		{"extra_RPC", "assignment", func(r *groupResumeReport) { r.WorkerCalls++ }},
		{"wrong_audio_identity", "session prefix", func(r *groupResumeReport) { r.Sessions[0].Observation.Worker.Audio[0] ^= 1 }},
		{"wrong_frame_digest", "frame digest", func(r *groupResumeReport) { r.Sessions[0].Observation.Frames[0].SHA256 = "wrong" }},
		{"missing_source_read", "source prefix", func(r *groupResumeReport) {
			r.Sessions[0].Observation.SourceReads = r.Sessions[0].Observation.SourceReads[1:]
		}},
		{"missing_processing", "Worker processing", func(r *groupResumeReport) { r.Sessions[0].Observation.Worker.Chunks[0].ProcessedAt = nil }},
		{"duplicate_result", "result order", func(r *groupResumeReport) { r.Sessions[0].Observation.Applications[1].Seq = 1 }},
		{"wrong_result_position", "position", func(r *groupResumeReport) { r.Sessions[0].Observation.Applications[1].AudioEnd++ }},
		{"false_completed", "complete audio", func(r *groupResumeReport) {
			r.Sessions[0].Observation.Worker.Audio = r.Sessions[0].Observation.Worker.Audio[:3200]
		}},
		{"unclosed_source", "lifecycle", func(r *groupResumeReport) { r.Sessions[0].Observation.SourceCloses = 0 }},
		{"fallback_start", "fallback", func(r *groupResumeReport) { r.Attempts = append(r.Attempts, groupResumeAttempt{ID: 1, Kind: "start"}) }},
		{"forced_cleanup", "natural cleanup", func(r *groupResumeReport) { r.ForcedCleanup = true }},
		{"missing_barrier", "missing barrier", func(r *groupResumeReport) { r.BarrierAt[0] = nil }},
		{"startup_spread", "startup spread", func(r *groupResumeReport) {
			r.Sessions[7].Observation.SourceReads[0].StartedAt = r.Sessions[0].Observation.SourceReads[0].StartedAt.Add(time.Second)
		}},
		{"gate_bound", "admission bound", func(r *groupResumeReport) { r.Samples[0].Handshakes = cfg.MaxHandshakes + 1 }},
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var broken groupResumeReport
			if err := json.Unmarshal(data, &broken); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&broken)
			validateGroupResumeReport(&broken)
			if !strings.Contains(strings.Join(broken.EvidenceErrors, "\n"), tc.expected) {
				t.Fatalf("expected %s, errors=%v", tc.expected, broken.EvidenceErrors)
			}
		})
	}
}

func TestGroupResumeForcedCleanupRetainsFailures(t *testing.T) {
	cfg := groupProbeConfig("forced_cleanup")
	cfg.Fault, cfg.Busy, cfg.Resume.MaxAttempts, cfg.CleanupTimeout = true, 200*time.Millisecond, 2, 10*time.Millisecond
	report := runGroupResumeProbe(t, cfg)
	defer logGroupProbe(t, &report)
	if !report.ForcedCleanup || report.NaturalIdleAt != nil || len(report.Sessions) != cfg.Clients || len(report.EvidenceErrors) == 0 {
		t.Fatalf("forced cleanup was concealed: forced=%t idle=%v evidence=%v", report.ForcedCleanup, report.NaturalIdleAt, report.EvidenceErrors)
	}
	for _, s := range report.Sessions {
		if s.Observation.RunResult.Completed || s.Observation.RunErrorClass != "recovery_exhausted" || s.Observation.SourceCloses != 1 {
			t.Fatal("failed session disappeared or changed outcome during teardown")
		}
	}
}

func TestGroupResumeDistinctInputAndConfig(t *testing.T) {
	for id := 1; id <= 8; id++ {
		data := groupPattern(id, 3200)
		if binary.LittleEndian.Uint64(data) != uint64(id) || !bytes.Equal(data, groupPattern(id, 3200)) {
			t.Fatal("input identity is not reproducible")
		}
		if id > 1 && bytes.Equal(data, groupPattern(id-1, 3200)) {
			t.Fatal("session inputs are indistinguishable")
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*groupResumeConfig)
	}{
		{"no_clients", func(c *groupResumeConfig) { c.Clients = 0 }},
		{"too_many_clients", func(c *groupResumeConfig) { c.Clients = 33 }},
		{"no_gate_limit", func(c *groupResumeConfig) { c.MaxHandshakes = 0 }},
		{"short_identity", func(c *groupResumeConfig) { c.ChunkBytes = 4 }},
		{"unaligned_PCM", func(c *groupResumeConfig) { c.ChunkBytes = 9 }},
		{"oversized_input", func(c *groupResumeConfig) { c.Chunks = 257 }},
		{"invalid_barrier", func(c *groupResumeConfig) { c.BarrierChunk = 1 }},
		{"busy_without_fault", func(c *groupResumeConfig) { c.Busy = time.Second }},
		{"zero_observation", func(c *groupResumeConfig) { c.RunTimeout = 0 }},
		{"unbounded_recording", func(c *groupResumeConfig) { c.SamplePeriod = time.Nanosecond }},
		{"zero_attempts", func(c *groupResumeConfig) { c.Resume.MaxAttempts = 0 }},
		{"invalid_backoff", func(c *groupResumeConfig) { c.Resume.MaxBackoff = time.Millisecond }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := groupProbeConfig(tc.name)
			tc.mutate(&cfg)
			if cfg.validate() == nil {
				t.Fatal("unsafe tool configuration accepted")
			}
		})
	}
}
