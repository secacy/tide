package loadgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
)

// batchJSON 是输出格式 v1。
// 时长字段使用 _ns 后缀，明确表示整数纳秒。
type batchJSON struct {
	SchemaVersion        int              `json:"schema_version"`         // 固定为 1。
	TailPercentileMethod string           `json:"tail_percentile_method"` // 固定为 nearest_rank。
	Config               batchConfigJSON  `json:"config"`                 // 共同负载配置。
	StartedAt            time.Time        `json:"started_at"`             // 批次开始，UTC。
	FinishedAt           time.Time        `json:"finished_at"`            // 批次结束，UTC。
	BatchError           *string          `json:"batch_error"`            // 无错误时为 null。
	Summary              batchSummaryJSON `json:"summary"`                // 从报告重算的摘要。
	Sessions             []sessionJSON    `json:"sessions"`               // 按原 Index 顺序保存。
}

// batchConfigJSON 保存批次规模和共同会话配置。
type batchConfigJSON struct {
	Sessions int               `json:"sessions"` // 计划会话数。
	Session  sessionConfigJSON `json:"session"`  // 所有会话共用的配置。
}

// sessionConfigJSON 保存单场负载配置。
type sessionConfigJSON struct {
	URL               string `json:"url"`                 // Gateway WebSocket 地址。
	AudioBytes        int64  `json:"audio_bytes"`         // 单场计划音频总字节数。
	ChunkBytes        int    `json:"chunk_bytes"`         // 单块音频字节上限。
	Realtime          bool   `json:"realtime"`            // 是否按实时节奏发送。
	TimeoutNS         int64  `json:"timeout_ns"`          // 单场期限，纳秒。
	ExpectedFinalText string `json:"expected_final_text"` // Mock 预期尾部文本。
}

// sessionJSON 保存一场会话的最终报告。
type sessionJSON struct {
	Index         int             `json:"index"`           // 批次内序号。
	StartedAt     time.Time       `json:"started_at"`      // 会话开始时间，UTC。
	FinishedAt    time.Time       `json:"finished_at"`     // 会话结束时间，UTC。
	Outcome       SessionOutcome  `json:"outcome"`         // 最终分类。
	Error         *string         `json:"error"`           // 单场错误；无错误为 null。
	TailLatencyNS *int64          `json:"tail_latency_ns"` // 完成会话尾部延迟；无样本为 null。
	Observation   observationJSON `json:"observation"`     // 完整观测事实。
}

// observationJSON 保存单场固定数量的观测字段。
type observationJSON struct {
	AudioBytesWritten       int64           `json:"audio_bytes_written"`         // 成功写出的音频字节数。
	AudioChunksWritten      int64           `json:"audio_chunks_written"`        // 成功写出的音频块数。
	WriteFailures           int64           `json:"write_failures"`              // 所有失败 Write 次数。
	MaxAudioWriteDurationNS int64           `json:"max_audio_write_duration_ns"` // 最大音频 Write 耗时，纳秒。
	FirstAudioStartedAt     *time.Time      `json:"first_audio_started_at"`      // 首次成功音频写入开始时间。
	LastAudioFinishedAt     *time.Time      `json:"last_audio_finished_at"`      // 最后一次成功音频写入结束时间。
	StartWrite              *writeEventJSON `json:"start_write"`                 // start 事件；未观察到为 null。
	EndWrite                *writeEventJSON `json:"end_write"`                   // end 事件；未观察到为 null。
	ResultCount             int64           `json:"result_count"`                // 有效结果数。
	FinalResultCount        int64           `json:"final_result_count"`          // final 结果数。
	FirstResultAt           *time.Time      `json:"first_result_at"`             // 第一条结果接收时间。
	LastResultAt            *time.Time      `json:"last_result_at"`              // 最后一条结果接收时间。
	LastFinalAt             *time.Time      `json:"last_final_at"`               // 最后一条 final 接收时间。
}

// writeEventJSON 保存一次已观察到的控制写入事件。
type writeEventJSON struct {
	Kind       wsclient.WriteKind `json:"kind"`        // 写入业务用途。
	AudioBytes int                `json:"audio_bytes"` // 控制消息通常为 0。
	StartedAt  time.Time          `json:"started_at"`  // Write 开始时间，UTC。
	FinishedAt time.Time          `json:"finished_at"` // Write 返回时间，UTC。
	Error      *string            `json:"error"`       // 写入错误；成功为 null。
}

// batchSummaryJSON 保存批次摘要。
type batchSummaryJSON struct {
	PlannedSessions   int                 `json:"planned_sessions"`    // 计划会话数。
	Completed         int                 `json:"completed"`           // 完整完成数。
	Failed            int                 `json:"failed"`              // 普通失败数。
	Canceled          int                 `json:"canceled"`            // 取消数。
	TimedOut          int                 `json:"timed_out"`           // 超时数。
	CompletionRate    float64             `json:"completion_rate"`     // 完成率，0..1。
	PlannedAudioBytes int64               `json:"planned_audio_bytes"` // 计划音频总量。
	AudioBytesWritten int64               `json:"audio_bytes_written"` // 实际成功写出总量。
	ElapsedNS         int64               `json:"elapsed_ns"`          // 批次总耗时，纳秒。
	Tail              *latencySummaryJSON `json:"tail"`                // 无完成样本时为 null。
}

// latencySummaryJSON 保存 completed 会话尾部延迟统计。
type latencySummaryJSON struct {
	Samples int   `json:"samples"` // 样本数。
	MinNS   int64 `json:"min_ns"`  // 最小值，纳秒。
	P50NS   int64 `json:"p50_ns"`  // nearest-rank p50，纳秒。
	P95NS   int64 `json:"p95_ns"`  // nearest-rank p95，纳秒。
	MaxNS   int64 `json:"max_ns"`  // 最大值，纳秒。
}

// errorText 保留错误文本；nil 表示不存在错误，而空文本错误仍有值。
func errorText(err error) *string {
	if err == nil {
		return nil
	}

	text := err.Error()
	return &text
}

// optionalTime 将缺失观察时间转成 nil，其余返回 UTC 时间的独立副本。
func optionalTime(at time.Time) *time.Time {
	if at.IsZero() {
		return nil
	}

	utc := at.UTC()
	return &utc
}

// optionalDurationNS 将可选时长复制为整数纳秒，保留 nil 与零的区别。
func optionalDurationNS(value *time.Duration) *int64 {
	if value == nil {
		return nil
	}

	ns := int64(*value)
	return &ns
}

// toWriteEventJSON 转换已观察到的控制写入；Kind 为空返回 nil。
func toWriteEventJSON(event wsclient.WriteEvent) *writeEventJSON {
	if event.Kind == "" {
		return nil
	}

	return &writeEventJSON{
		Kind:       event.Kind,
		AudioBytes: event.AudioBytes,
		StartedAt:  event.StartedAt.UTC(),
		FinishedAt: event.FinishedAt.UTC(),
		Error:      errorText(event.Err),
	}
}

// toSessionConfigJSON 转换单场负载配置。
func toSessionConfigJSON(cfg SessionConfig) sessionConfigJSON {
	return sessionConfigJSON{
		URL:               cfg.URL,
		AudioBytes:        cfg.AudioBytes,
		ChunkBytes:        cfg.ChunkBytes,
		Realtime:          cfg.Realtime,
		TimeoutNS:         int64(cfg.Timeout),
		ExpectedFinalText: cfg.ExpectedFinalText,
	}
}

// toBatchConfigJSON 转换批次配置。
func toBatchConfigJSON(cfg BatchConfig) batchConfigJSON {
	return batchConfigJSON{
		Sessions: cfg.Sessions,
		Session:  toSessionConfigJSON(cfg.Session),
	}
}

// toObservationJSON 转换单场观测。
func toObservationJSON(observation SessionObservation) observationJSON {
	return observationJSON{
		AudioBytesWritten:       observation.AudioBytesWritten,
		AudioChunksWritten:      observation.AudioChunksWritten,
		WriteFailures:           observation.WriteFailures,
		MaxAudioWriteDurationNS: int64(observation.MaxAudioWriteDuration),
		FirstAudioStartedAt:     optionalTime(observation.FirstAudioStartedAt),
		LastAudioFinishedAt:     optionalTime(observation.LastAudioFinishedAt),
		StartWrite:              toWriteEventJSON(observation.StartWrite),
		EndWrite:                toWriteEventJSON(observation.EndWrite),
		ResultCount:             observation.ResultCount,
		FinalResultCount:        observation.FinalResultCount,
		FirstResultAt:           optionalTime(observation.FirstResultAt),
		LastResultAt:            optionalTime(observation.LastResultAt),
		LastFinalAt:             optionalTime(observation.LastFinalAt),
	}
}

// toSessionJSON 转换一场会话结果。
func toSessionJSON(result SessionResult) sessionJSON {
	return sessionJSON{
		Index:         result.Index,
		StartedAt:     result.Report.StartedAt.UTC(),
		FinishedAt:    result.Report.FinishedAt.UTC(),
		Outcome:       result.Report.Outcome,
		Error:         errorText(result.Err),
		TailLatencyNS: optionalDurationNS(result.Report.TailLatency),
		Observation:   toObservationJSON(result.Report.Observation),
	}
}

// toLatencySummaryJSON 转换尾部延迟摘要。
func toLatencySummaryJSON(summary *LatencySummary) *latencySummaryJSON {
	if summary == nil {
		return nil
	}

	return &latencySummaryJSON{
		Samples: summary.Samples,
		MinNS:   int64(summary.Min),
		P50NS:   int64(summary.P50),
		P95NS:   int64(summary.P95),
		MaxNS:   int64(summary.Max),
	}
}

// toBatchSummaryJSON 转换批次摘要。
func toBatchSummaryJSON(summary BatchSummary) batchSummaryJSON {
	return batchSummaryJSON{
		PlannedSessions:   summary.PlannedSessions,
		Completed:         summary.Completed,
		Failed:            summary.Failed,
		Canceled:          summary.Canceled,
		TimedOut:          summary.TimedOut,
		CompletionRate:    summary.CompletionRate,
		PlannedAudioBytes: summary.PlannedAudioBytes,
		AudioBytesWritten: summary.AudioBytesWritten,
		ElapsedNS:         int64(summary.Elapsed),
		Tail:              toLatencySummaryJSON(summary.Tail),
	}
}

// WriteBatchJSON 将已收尾批次及其批次级错误写为一个 JSON 文档。
// 摘要从 report 重新计算，避免原始记录和摘要不一致。
// 返回值只表示校验、编码或写入是否成功；batchErr 不阻止有效报告输出。
// 函数不创建或关闭文件，不修改 report；写入失败可能留下部分字节。
func WriteBatchJSON(w io.Writer, report BatchReport, batchErr error) error {
	if w == nil {
		return errors.New("write batch json: writer is nil")
	}

	summary, err := SummarizeBatch(report)
	if err != nil {
		return fmt.Errorf("write batch json: summarize batch: %w", err)
	}

	sessions := make([]sessionJSON, len(report.Results))
	for i, result := range report.Results {
		sessions[i] = toSessionJSON(result)
	}

	output := batchJSON{
		SchemaVersion:        1,
		TailPercentileMethod: "nearest_rank",
		Config:               toBatchConfigJSON(report.Config),
		StartedAt:            report.StartedAt.UTC(),
		FinishedAt:           report.FinishedAt.UTC(),
		BatchError:           errorText(batchErr),
		Summary:              toBatchSummaryJSON(summary),
		Sessions:             sessions,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("write batch json: encode: %w", err)
	}

	data = append(data, '\n')

	n, err := w.Write(data)
	if err != nil {
		return fmt.Errorf("write batch json: write: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("write batch json: wrote %d of %d bytes: %w", n, len(data), io.ErrShortWrite)
	}

	return nil
}
