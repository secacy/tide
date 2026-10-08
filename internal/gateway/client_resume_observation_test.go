package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsclient"
)

// resumeExperimentConfig 是实验条件快照；时间字段的 JSON 单位为纳秒。
// probe 使用较短预算验证工具，正式实验始终使用 formalResumeCases 的条件。
type resumeExperimentConfig struct {
	Case             string        `json:"case"`
	Chunks           int           `json:"chunks"`
	ChunkBytes       int           `json:"chunk_bytes"`
	CutChunk         int           `json:"cut_chunk"` // 1-based，0 不注入；只丢弃首次连接此块。
	Resume           bool          `json:"resume"`
	Outage           time.Duration `json:"outage_ns"`
	Processing       time.Duration `json:"processing_ns"`
	ClientWindow     time.Duration `json:"client_window_ns"`
	ServerWindow     time.Duration `json:"server_window_ns"`
	RunTimeout       time.Duration `json:"run_timeout_ns"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout_ns"`
	SamplePeriod     time.Duration `json:"sample_period_ns"`
	CatchupThreshold time.Duration `json:"catchup_threshold_ns"`
}

// resumeClientSettings 保存实际客户端配置，排除地址与回调等不可复现/敏感字段。
type resumeClientSettings struct {
	ChunkBytes        int                      `json:"chunk_bytes"`
	Realtime          bool                     `json:"realtime"`
	ReadLimit         int64                    `json:"read_limit"`
	MaxReplayBytes    uint64                   `json:"max_replay_bytes"`
	MaxReplayChunks   int                      `json:"max_replay_chunks"`
	HandshakeTimeout  time.Duration            `json:"handshake_timeout_ns"`
	WriteTimeout      time.Duration            `json:"write_timeout_ns"`
	ApplyTimeout      time.Duration            `json:"apply_timeout_ns"`
	CompletionTimeout time.Duration            `json:"completion_timeout_ns"`
	Resume            *wsclient.V2ResumeConfig `json:"resume"`
}

// resumeRange 用半开区间描述逻辑 PCM 字节，不包含 WebSocket offset 头。
type resumeRange struct {
	Begin uint64 `json:"begin"`
	End   uint64 `json:"end"`
}
type resumeSourceRead struct {
	resumeRange
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	EOF        bool      `json:"eof"`
}
type resumeFrame struct {
	resumeRange
	Connection        int        `json:"connection"`
	ReadAt            time.Time  `json:"read_at"`
	ForwardStartedAt  *time.Time `json:"forward_started_at"`
	ForwardReturnedAt *time.Time `json:"forward_returned_at"`
	Forwarded         bool       `json:"forwarded"`
	Dropped           bool       `json:"dropped"`
	SHA256            string     `json:"sha256"` // 实际读取负载的摘要，可独立核对位置内容。
}

// resumeHandshake / resumeReady 仅保留分析字段，绝不保存身份和恢复凭据。
type resumeHandshake struct {
	Connection  int        `json:"connection"`
	Kind        string     `json:"kind"`
	ReadAt      time.Time  `json:"read_at"`
	ForwardedAt *time.Time `json:"forwarded_at"`
}
type resumeReady struct {
	Connection int       `json:"connection"`
	Generation uint64    `json:"generation"`
	NextOffset uint64    `json:"next_offset"`
	AckedSeq   uint64    `json:"acked_seq"`
	WrittenAt  time.Time `json:"written_at"` // 代理 Write 成功返回，不表示客户端接纳。
}
type resumeWorkerChunk struct {
	resumeRange
	ReceivedAt     time.Time  `json:"received_at"`
	ProcessedAt    *time.Time `json:"processed_at"`
	ProgressSentAt *time.Time `json:"progress_sent_at"`
	ResultSentAt   *time.Time `json:"result_sent_at"`
}

// resumeWorkerObservation 是 Worker 退出后的副本；Audio 的 JSON 是实际音频 base64。
type resumeWorkerObservation struct {
	Calls      int                 `json:"calls"`
	StartedAt  *time.Time          `json:"started_at"`
	ExitedAt   *time.Time          `json:"exited_at"`
	ExitClass  string              `json:"exit_class"`
	Audio      []byte              `json:"audio"`
	Chunks     []resumeWorkerChunk `json:"chunks"`
	TailSentAt *time.Time          `json:"tail_sent_at"`
}
type resumeApplication struct {
	Seq        uint64    `json:"seq"`
	AudioEnd   uint64    `json:"audio_end"` // 从实验 Mock 标记解析；不是生产结果字段。
	Final      bool      `json:"final"`
	ReceivedAt time.Time `json:"received_at"`
	AppliedAt  time.Time `json:"applied_at"`
}

// resumeResourceSample 各计数依次读取，非跨组件原子快照；取值落在 BeginAt/At 之间。
type resumeResourceSample struct {
	BeginAt         time.Time `json:"begin_at"`
	At              time.Time `json:"at"`
	Sessions        int       `json:"sessions"`
	Handshakes      int       `json:"handshakes"`
	Registry        int       `json:"registry"`
	WorkerActive    int       `json:"worker_active"`
	GatewayHandlers int       `json:"gateway_handlers"`
	ProxyHandlers   int       `json:"proxy_handlers"`
	FaultTimers     int       `json:"fault_timers"`
}

func (s resumeResourceSample) idle() bool {
	return s.Sessions == 0 && s.Handshakes == 0 && s.Registry == 0 && s.WorkerActive == 0
}
func (s resumeResourceSample) tasksExited() bool {
	return s.GatewayHandlers == 0 && s.ProxyHandlers == 0 && s.FaultTimers == 0
}

type resumeResultWait struct {
	Seq        uint64    `json:"seq"`
	AudioEnd   uint64    `json:"audio_end"`
	PlannedAt  time.Time `json:"planned_at"`
	ReceivedAt time.Time `json:"received_at"`
	WaitMS     float64   `json:"wait_ms"`
}

// resumeMetrics 只从事实推导；没有样本的时间/范围保持 nil，而不是零。
type resumeMetrics struct {
	CapturedBytes         uint64             `json:"captured_bytes"`
	WorkerBytes           uint64             `json:"worker_bytes"`
	WorkerPrefixBytes     uint64             `json:"worker_prefix_bytes"`
	WorkerExact           bool               `json:"worker_exact"`
	WorkerIsPrefix        bool               `json:"worker_is_prefix"`
	MissingCaptured       *resumeRange       `json:"missing_captured"`
	Uncaptured            *resumeRange       `json:"uncaptured"`
	RepeatedTransferBytes uint64             `json:"repeated_transfer_bytes"`
	DuplicateApplications int                `json:"duplicate_applications"`
	FreshAudioEnd         *uint64            `json:"fresh_audio_end"`
	ReadyAfterCutMS       *float64           `json:"ready_after_cut_ms"`
	ReadyAfterOpenMS      *float64           `json:"ready_after_open_ms"`
	FreshAfterCutMS       *float64           `json:"fresh_after_cut_ms"`
	FreshAfterOpenMS      *float64           `json:"fresh_after_open_ms"`
	CatchupAfterOpenMS    *float64           `json:"catchup_after_open_ms"`
	MaxPartialGapMS       *float64           `json:"max_partial_gap_ms"`
	MaxSampleGapMS        *float64           `json:"max_sample_gap_ms"`
	MaxSampleCallMS       *float64           `json:"max_sample_call_ms"`
	NaturalIdleAt         *time.Time         `json:"natural_idle_at"`
	CleanupWaitMS         *float64           `json:"cleanup_wait_ms"`
	Waits                 []resumeResultWait `json:"waits"`
}

// resumeExperimentReport 在业务失败时仍完整输出；证据有效性与业务达标分开。
type resumeExperimentReport struct {
	Version            int                     `json:"version"`
	Config             resumeExperimentConfig  `json:"config"`
	GatewayConfig      Config                  `json:"gateway_config"`
	ClientConfig       resumeClientSettings    `json:"client_config"`
	InputSHA256        string                  `json:"input_sha256"`
	StartedAt          time.Time               `json:"started_at"`
	ReturnedAt         *time.Time              `json:"returned_at"`
	RunResult          wsclient.V2RunResult    `json:"run_result"`
	RunErrorClass      string                  `json:"run_error_class"`
	OuterDeadline      bool                    `json:"outer_deadline"`
	CutAt              *time.Time              `json:"cut_at"`
	OpenedAt           *time.Time              `json:"opened_at"`
	SourceReads        []resumeSourceRead      `json:"source_reads"`
	SourceCloses       int                     `json:"source_closes"`
	Frames             []resumeFrame           `json:"frames"`
	Handshakes         []resumeHandshake       `json:"handshakes"`
	Readies            []resumeReady           `json:"readies"`
	Worker             resumeWorkerObservation `json:"worker"`
	WorkerPicks        int                     `json:"worker_picks"`
	Applications       []resumeApplication     `json:"applications"`
	Samples            []resumeResourceSample  `json:"samples"`
	NaturalTasksExited bool                    `json:"natural_tasks_exited"`
	ForcedCleanup      bool                    `json:"forced_cleanup"` // 自然观察失败后才停止服务兜底。
	ToolErrors         []string                `json:"tool_errors"`
	EvidenceErrors     []string                `json:"evidence_errors"`
	AcceptanceErrors   []string                `json:"acceptance_errors"`
	BusinessCompleted  bool                    `json:"business_completed"`
	Metrics            resumeMetrics           `json:"metrics"`
}

func resumeAt(t time.Time) *time.Time { return &t }
func resumeMS(end, begin *time.Time) *float64 {
	if end == nil || begin == nil {
		return nil
	}
	v := baselineMS(end.Sub(*begin))
	return &v
}
func resumeDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// resumePattern 在每个 16bit 样本位置生成固定伪随机值，避免全零/每块相同掩盖乱序。
func resumePattern(n int) []byte {
	data := make([]byte, n)
	state := uint32(0x1ac3e591)
	for i := 0; i < n; i++ {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		data[i] = byte(state)
	}
	return data
}

// resumeRepeatedBytes 统计范围重叠（含未转发帧），不把首次补发的缓存新音频算作重传。
// 调用方先校验每个非空范围；计算只供有限实验数据，未覆盖 uint64 总量溢出。
func resumeRepeatedBytes(frames []resumeFrame) uint64 {
	ranges := make([]resumeRange, 0, len(frames))
	var total uint64
	for _, f := range frames {
		if f.End > f.Begin {
			total += f.End - f.Begin
			ranges = append(ranges, f.resumeRange)
		}
	}
	slices.SortFunc(ranges, func(a, b resumeRange) int {
		if a.Begin < b.Begin {
			return -1
		}
		if a.Begin > b.Begin {
			return 1
		}
		return 0
	})
	var union, end uint64
	for _, r := range ranges {
		if r.End > end {
			union += r.End - max(end, r.Begin)
			end = r.End
		}
	}
	return total - union
}

// analyzeResumeReport 不依赖预期故障时长填充时间；Mock 标记与实际源读取共同确定新音频。
// 调用时自建任务已退出或事实已冻结；函数可重复调用，覆盖所有派生字段。
func analyzeResumeReport(r *resumeExperimentReport) {
	m := resumeMetrics{Waits: []resumeResultWait{}}
	issues := append([]string{}, r.ToolErrors...)
	cfg := r.Config
	if err := validateResumeConfig(cfg); err != nil {
		r.Metrics = m
		r.EvidenceErrors = append(issues, "invalid report configuration")
		r.AcceptanceErrors = []string{"cannot analyze invalid configuration"}
		r.BusinessCompleted = false
		return
	}
	total := uint64(cfg.Chunks * cfg.ChunkBytes)
	expected := resumePattern(int(total))
	if r.InputSHA256 != resumeDigest(expected) {
		issues = append(issues, "input hash does not match configured pattern")
	}
	if r.ReturnedAt == nil || r.ReturnedAt.Before(r.StartedAt) {
		issues = append(issues, "invalid run return time")
	}
	if r.CutAt != nil && (r.OpenedAt == nil || r.OpenedAt.Before(*r.CutAt)) {
		issues = append(issues, "invalid fault reopening time")
	}
	var origin *time.Time
	for _, read := range r.SourceReads {
		if read.FinishedAt.Before(read.StartedAt) || read.End < read.Begin {
			issues = append(issues, "invalid source read time or range")
		}
		if read.End > read.Begin {
			if origin == nil {
				origin = resumeAt(read.StartedAt)
			}
			if read.Begin != m.CapturedBytes || read.End > total {
				issues = append(issues, "source range is not a contiguous input prefix")
			}
			m.CapturedBytes = read.End
			if r.CutAt != nil && read.StartedAt.After(*r.CutAt) && m.FreshAudioEnd == nil {
				end := read.End
				m.FreshAudioEnd = &end
			}
		}
	}
	m.WorkerBytes = uint64(len(r.Worker.Audio))
	limit := min(len(expected), len(r.Worker.Audio))
	for int(m.WorkerPrefixBytes) < limit && expected[m.WorkerPrefixBytes] == r.Worker.Audio[m.WorkerPrefixBytes] {
		m.WorkerPrefixBytes++
	}
	m.WorkerIsPrefix = m.WorkerPrefixBytes == m.WorkerBytes && m.WorkerBytes <= m.CapturedBytes
	m.WorkerExact = bytes.Equal(expected, r.Worker.Audio)
	if m.WorkerIsPrefix && m.WorkerBytes < m.CapturedBytes {
		m.MissingCaptured = &resumeRange{m.WorkerBytes, m.CapturedBytes}
	}
	if m.CapturedBytes < total {
		m.Uncaptured = &resumeRange{m.CapturedBytes, total}
	}
	for _, f := range r.Frames {
		if f.End <= f.Begin || f.End > m.CapturedBytes || f.End > total {
			issues = append(issues, "proxy frame range outside captured input")
			continue
		}
		if f.SHA256 != resumeDigest(expected[f.Begin:f.End]) {
			issues = append(issues, "proxy frame content does not match input position")
		}
		if f.Dropped && (f.Forwarded || f.ForwardStartedAt != nil) {
			issues = append(issues, "dropped frame was forwarded")
		}
		if f.Forwarded && (f.ForwardStartedAt == nil || f.ForwardReturnedAt == nil) {
			issues = append(issues, "successful forwarding lacks timestamps")
		}
		if f.ForwardStartedAt != nil && f.ForwardStartedAt.Before(f.ReadAt) || f.ForwardReturnedAt != nil && (f.ForwardStartedAt == nil || f.ForwardReturnedAt.Before(*f.ForwardStartedAt)) {
			issues = append(issues, "invalid forwarding time order")
		}
	}
	m.RepeatedTransferBytes = resumeRepeatedBytes(r.Frames)
	for _, ready := range r.Readies {
		if ready.Connection > 1 {
			at := ready.WrittenAt
			m.ReadyAfterCutMS = resumeMS(&at, r.CutAt)
			m.ReadyAfterOpenMS = resumeMS(&at, r.OpenedAt)
			break
		}
	}
	seen := map[uint64]bool{}
	var lastPartial *time.Time
	streak := 0
	finalCount := 0
	for i, a := range r.Applications {
		if seen[a.Seq] {
			m.DuplicateApplications++
		}
		seen[a.Seq] = true
		if a.Seq != uint64(i+1) {
			issues = append(issues, "application sequence has gap or duplicate")
		}
		if a.AppliedAt.Before(a.ReceivedAt) {
			issues = append(issues, "application preceded receive")
		}
		if i > 0 && (a.ReceivedAt.Before(r.Applications[i-1].ReceivedAt) || a.AppliedAt.Before(r.Applications[i-1].AppliedAt)) {
			issues = append(issues, "application or receive time reversed")
		}
		if a.Final {
			finalCount++
			if i != len(r.Applications)-1 || a.AudioEnd != total {
				issues = append(issues, "invalid tail final position")
			}
			continue
		}
		if finalCount != 0 || a.AudioEnd != uint64((i+1)*cfg.ChunkBytes) || a.AudioEnd > m.WorkerBytes || origin == nil {
			issues = append(issues, "partial does not match processed chunk position")
			continue
		}
		if i >= len(r.Worker.Chunks) || r.Worker.Chunks[i].ProcessedAt == nil || a.ReceivedAt.Before(*r.Worker.Chunks[i].ProcessedAt) {
			issues = append(issues, "result preceded corresponding Worker processing")
		}
		planned := origin.Add(audio.DurationFromBytes(int(a.AudioEnd) - cfg.ChunkBytes))
		wait := baselineMS(a.ReceivedAt.Sub(planned))
		m.Waits = append(m.Waits, resumeResultWait{a.Seq, a.AudioEnd, planned, a.ReceivedAt, wait})
		if lastPartial != nil {
			gap := baselineMS(a.ReceivedAt.Sub(*lastPartial))
			if m.MaxPartialGapMS == nil || gap > *m.MaxPartialGapMS {
				m.MaxPartialGapMS = &gap
			}
		}
		lastPartial = resumeAt(a.ReceivedAt)
		if m.FreshAudioEnd != nil && a.AudioEnd >= *m.FreshAudioEnd && m.FreshAfterCutMS == nil {
			at := a.ReceivedAt
			m.FreshAfterCutMS = resumeMS(&at, r.CutAt)
			m.FreshAfterOpenMS = resumeMS(&at, r.OpenedAt)
		}
		if r.OpenedAt != nil && !a.ReceivedAt.Before(*r.OpenedAt) {
			if wait >= 0 && wait <= baselineMS(cfg.CatchupThreshold) {
				streak++
			} else {
				streak = 0
			}
			if streak == 3 && m.CatchupAfterOpenMS == nil {
				at := a.ReceivedAt
				m.CatchupAfterOpenMS = resumeMS(&at, r.OpenedAt)
			}
		}
	}
	for i, s := range r.Samples {
		if s.At.Before(s.BeginAt) || s.Sessions < 0 || s.Sessions > 1 || s.Handshakes < 0 || s.Handshakes > 4 || s.Registry < 0 || s.Registry > 1 || s.WorkerActive < 0 || s.WorkerActive > 1 {
			issues = append(issues, "invalid resource sample")
		}
		call := baselineMS(s.At.Sub(s.BeginAt))
		if m.MaxSampleCallMS == nil || call > *m.MaxSampleCallMS {
			m.MaxSampleCallMS = &call
		}
		if i > 0 {
			gap := baselineMS(s.At.Sub(r.Samples[i-1].At))
			if gap < 0 {
				issues = append(issues, "resource sample time reversed")
			}
			if m.MaxSampleGapMS == nil || gap > *m.MaxSampleGapMS {
				m.MaxSampleGapMS = &gap
			}
		}
		// 只认 Run 返回后独立观察到的空闲，排除尚未创建会话时的初始全零。
		if r.ReturnedAt != nil && !s.BeginAt.Before(*r.ReturnedAt) && s.idle() && m.NaturalIdleAt == nil {
			m.NaturalIdleAt = resumeAt(s.At)
			m.CleanupWaitMS = resumeMS(&s.At, r.ReturnedAt)
		}
	}
	if !m.WorkerIsPrefix {
		issues = append(issues, "Worker audio is not captured input prefix")
	}
	if r.SourceCloses != 1 {
		issues = append(issues, "source Close count is not one")
	}
	if r.Worker.Calls != 1 || r.WorkerPicks != 1 {
		issues = append(issues, "Worker stream or selection count is not one")
	}
	if len(r.Worker.Chunks)*cfg.ChunkBytes != len(r.Worker.Audio) {
		issues = append(issues, "Worker chunk count disagrees with byte count")
	}
	for i, chunk := range r.Worker.Chunks {
		if chunk.Begin != uint64(i*cfg.ChunkBytes) || chunk.End != uint64((i+1)*cfg.ChunkBytes) {
			issues = append(issues, "Worker chunk position is not contiguous")
		}
		if chunk.ProcessedAt != nil && chunk.ProcessedAt.Before(chunk.ReceivedAt) || chunk.ProgressSentAt != nil && (chunk.ProcessedAt == nil || chunk.ProgressSentAt.Before(*chunk.ProcessedAt)) || chunk.ResultSentAt != nil && (chunk.ProgressSentAt == nil || chunk.ResultSentAt.Before(*chunk.ProgressSentAt)) {
			issues = append(issues, "invalid Worker processing time order")
		}
	}
	if r.Worker.StartedAt == nil || r.Worker.ExitedAt == nil || r.Worker.ExitedAt.Before(*r.Worker.StartedAt) {
		issues = append(issues, "Worker exit was not observed")
	}
	if finalCount > 0 && r.Worker.TailSentAt == nil {
		issues = append(issues, "Worker tail send was not observed")
	}
	if r.RunResult.AppliedSeq > uint64(len(r.Applications)) {
		issues = append(issues, "committed results exceed applications")
	}
	if r.RunResult.Completed && r.RunResult.AppliedSeq != uint64(len(r.Applications)) {
		issues = append(issues, "completed state disagrees with applied result position")
	}
	r.Metrics = m
	r.EvidenceErrors = issues
	r.BusinessCompleted = r.RunErrorClass == "" && r.RunResult.Completed && r.RunResult.CompletionAckWritten && r.RunResult.FinalOffset == total && m.WorkerExact && len(r.Applications) == cfg.Chunks+1 && finalCount == 1 && len(issues) == 0
	accept := []string{}
	starts, resumes := 0, 0
	for _, h := range r.Handshakes {
		if h.Kind == "start" {
			starts++
		} else if h.Kind == "resume" {
			resumes++
		} else {
			issues = append(issues, "unknown handshake kind")
		}
	}
	if starts != 1 {
		accept = append(accept, "expected exactly one start")
	}
	if r.CutAt == nil && cfg.CutChunk != 0 {
		accept = append(accept, "fault was not triggered")
	}
	dropped := 0
	for _, f := range r.Frames {
		if f.Dropped {
			dropped++
			if f.Connection != 1 || f.Begin != uint64((cfg.CutChunk-1)*cfg.ChunkBytes) || f.End != uint64(cfg.CutChunk*cfg.ChunkBytes) {
				accept = append(accept, "fault at wrong audio boundary")
			}
		}
	}
	if cfg.CutChunk != 0 && dropped != 1 {
		accept = append(accept, "expected exactly one dropped audio frame")
	}
	if cfg.CutChunk == 0 && (r.CutAt != nil || dropped != 0 || r.OpenedAt != nil) {
		accept = append(accept, "unexpected fault in control")
	}
	if !r.NaturalTasksExited || m.NaturalIdleAt == nil || r.ForcedCleanup {
		accept = append(accept, "natural cleanup did not complete")
	}
	if r.OuterDeadline {
		accept = append(accept, "outer run deadline reached")
	}
	switch {
	case cfg.CutChunk == 0:
		if !r.BusinessCompleted || r.RunResult.ResumeAttempts != 0 || resumes != 0 {
			accept = append(accept, "normal control did not complete without resume")
		}
	case !cfg.Resume:
		if r.BusinessCompleted || r.RunErrorClass == "" || r.RunResult.Completed || resumes != 0 || r.RunResult.ResumeAttempts != 0 {
			accept = append(accept, "disabled recovery did not fail without resume")
		}
	case cfg.Outage > cfg.ClientWindow:
		if r.RunErrorClass != "recovery_exhausted" || r.RunResult.Completed || r.RunResult.ResumeAttempts > 5 || r.RunResult.ResumeAttempts < 1 {
			accept = append(accept, "over-window case did not stop within recovery budget")
		}
	default:
		if !r.BusinessCompleted || r.RunResult.ResumedConnections < 1 || m.ReadyAfterOpenMS == nil || m.FreshAfterOpenMS == nil {
			accept = append(accept, "in-window recovery incomplete or timing absent")
		}
	}
	if r.CutAt != nil && r.OpenedAt == nil {
		accept = append(accept, "fault path reopening was not observed")
	}
	if resumes > r.RunResult.ResumeAttempts {
		issues = append(issues, "proxy saw more resume handshakes than attempts")
	}
	r.EvidenceErrors = issues
	r.AcceptanceErrors = accept
	if len(issues) > 0 {
		r.BusinessCompleted = false
	}
}

// validateResumeConfig 防止 probe/正式实验因配置错误输出可误解的报告。
func validateResumeConfig(c resumeExperimentConfig) error {
	if c.Chunks <= 0 || c.Chunks > 100 || c.ChunkBytes != audio.ChunkBytesDefault || c.CutChunk < 0 || c.CutChunk > c.Chunks || c.Outage < 0 || c.Processing < 0 || c.ClientWindow <= 0 || c.ServerWindow <= 0 || c.RunTimeout <= 0 || c.CleanupTimeout <= 0 || c.SamplePeriod <= 0 || c.CatchupThreshold <= 0 {
		return fmt.Errorf("invalid resume experiment configuration")
	}
	if (c.CutChunk == 0) != (c.Outage == 0) {
		return fmt.Errorf("cut chunk and positive outage must be enabled together")
	}
	return nil
}
