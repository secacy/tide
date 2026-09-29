package loadgen

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// LatencySummary 保存非空尾部样本的排序统计。
// 分位数采用 nearest-rank，展示时必须同时提供 Samples。
type LatencySummary struct {
	Samples int           // 参与统计的 completed 会话数。
	Min     time.Duration // 最小尾部等待。
	P50     time.Duration // 第 ceil(0.50 * Samples) 个样本。
	P95     time.Duration // 第 ceil(0.95 * Samples) 个样本。
	Max     time.Duration // 最大尾部等待。
}

// BatchSummary 保存有限批次的统计，不代表稳定容量。
type BatchSummary struct {
	PlannedSessions int // 计划会话数，也是完成率分母。
	Completed       int // 完整完成数。
	Failed          int // 失败数，不包含取消和超时。
	Canceled        int // 取消数。
	TimedOut        int // 超时数。

	CompletionRate float64 // Completed / PlannedSessions，范围 0..1。

	PlannedAudioBytes int64 // 计划会话数 × 单场计划字节数。
	AudioBytesWritten int64 // 所有会话成功写出的音频，包含失败会话已写出的部分。

	Elapsed time.Duration // 批次结束时间减开始时间，包含拨号和收尾。

	Tail *LatencySummary // 没有 completed 会话时为 nil。
}

// SummarizeBatch 汇总已经收尾的完整批次，不修改输入报告。
// 统计所需字段矛盾、结果缺失或计划量溢出时，返回零摘要和错误。
func SummarizeBatch(report BatchReport) (BatchSummary, error) {
	// 1. 验证批次本身
	if err := report.Config.Validate(); err != nil {
		return BatchSummary{}, fmt.Errorf("invalid batch report config: %w", err)
	}
	if len(report.Results) != report.Config.Sessions {
		return BatchSummary{}, fmt.Errorf("expected %d results, got %d", report.Config.Sessions, len(report.Results))
	}
	if report.StartedAt.IsZero() {
		return BatchSummary{}, errors.New("started_at should not be zero")
	}
	if report.FinishedAt.IsZero() {
		return BatchSummary{}, errors.New("finished_at should not be zero")
	}
	if report.FinishedAt.Before(report.StartedAt) {
		return BatchSummary{}, errors.New("finished_at should be greater than started_at")
	}

	sessions := int64(report.Config.Sessions)
	audioBytesPerSession := report.Config.Session.AudioBytes
	plannedAudioBytes := sessions * audioBytesPerSession

	summary := BatchSummary{
		PlannedSessions:   report.Config.Sessions,
		PlannedAudioBytes: plannedAudioBytes,
		Elapsed:           report.FinishedAt.Sub(report.StartedAt),
	}

	tailSamples := make([]time.Duration, 0, report.Config.Sessions)

	// 2. 验证并累计每场结果。
	for i, result := range report.Results {
		if result.Index != i {
			return BatchSummary{}, fmt.Errorf("invalid session result at position %d: index is %d", i, result.Index)
		}

		if result.Report.Config != report.Config.Session {
			return BatchSummary{}, fmt.Errorf("invalid session result %d: session config does not match batch config", i)
		}

		observation := result.Report.Observation

		if observation.AudioScheduleSamples < 0 {
			return BatchSummary{}, fmt.Errorf("invalid session result %d: audio schedule samples is negative: %d", i, observation.AudioScheduleSamples)
		}

		if observation.MaxAudioScheduleLag < 0 {
			return BatchSummary{}, fmt.Errorf("invalid session result %d: max audio schedule lag is negative: %s", i, observation.MaxAudioScheduleLag)
		}

		if observation.AudioScheduleSamples == 0 && observation.MaxAudioScheduleLag != 0 {
			return BatchSummary{}, fmt.Errorf("invalid session result %d: max audio schedule lag is %s with zero schedule samples", i, observation.MaxAudioScheduleLag)
		}

		written := observation.AudioBytesWritten
		if written < 0 || written > audioBytesPerSession {
			return BatchSummary{}, fmt.Errorf("invalid session result %d: audio bytes written is %d, want range [0, %d]", i, written, audioBytesPerSession)
		}

		// 所有状态都贡献实际成功写出的音频量。
		summary.AudioBytesWritten += written

		switch result.Report.Outcome {
		case SessionCompleted:
			if result.Err != nil {
				return BatchSummary{}, fmt.Errorf("invalid completed session result %d: unexpected error: %w", i, result.Err)
			}

			if result.Report.TailLatency == nil {
				return BatchSummary{}, fmt.Errorf("invalid completed session result %d: tail latency is nil", i)
			}

			if *result.Report.TailLatency < 0 {
				return BatchSummary{}, fmt.Errorf("invalid completed session result %d: negative tail latency %s", i, *result.Report.TailLatency)
			}

			if written != audioBytesPerSession {
				return BatchSummary{}, fmt.Errorf("invalid completed session result %d: wrote %d audio bytes, want %d", i, written, audioBytesPerSession)
			}

			summary.Completed++
			tailSamples = append(tailSamples, *result.Report.TailLatency)

		case SessionFailed:
			if result.Err == nil {
				return BatchSummary{}, fmt.Errorf("invalid failed session result %d: error is nil", i)
			}

			if result.Report.TailLatency != nil {
				return BatchSummary{}, fmt.Errorf("invalid failed session result %d: tail latency is non-nil", i)
			}

			summary.Failed++

		case SessionCanceled:
			if result.Err == nil {
				return BatchSummary{}, fmt.Errorf("invalid canceled session result %d: error is nil", i)
			}

			if result.Report.TailLatency != nil {
				return BatchSummary{}, fmt.Errorf("invalid canceled session result %d: tail latency is non-nil", i)
			}

			summary.Canceled++

		case SessionTimedOut:
			if result.Err == nil {
				return BatchSummary{}, fmt.Errorf("invalid timed out session result %d: error is nil", i)
			}

			if result.Report.TailLatency != nil {
				return BatchSummary{}, fmt.Errorf("invalid timed out session result %d: tail latency is non-nil", i)
			}

			summary.TimedOut++

		default:
			return BatchSummary{}, fmt.Errorf("invalid session result %d: unknown outcome %q", i, result.Report.Outcome)
		}
	}

	// 3. 防止未来修改 switch 时悄悄漏掉某种状态。
	classified := summary.Completed + summary.Failed + summary.Canceled + summary.TimedOut

	if classified != summary.PlannedSessions {
		return BatchSummary{}, fmt.Errorf("invalid batch report: classified %d sessions, want %d", classified, summary.PlannedSessions)
	}

	// 4. 完成率分母始终是全部计划会话。
	summary.CompletionRate = float64(summary.Completed) / float64(summary.PlannedSessions)

	// 5. 仅 completed 会话贡献尾部延迟样本。
	if len(tailSamples) != 0 {
		slices.Sort(tailSamples)

		summary.Tail = &LatencySummary{
			Samples: len(tailSamples),
			Min:     tailSamples[0],
			P50:     nearestRank(tailSamples, 50),
			P95:     nearestRank(tailSamples, 95),
			Max:     tailSamples[len(tailSamples)-1],
		}
	}

	return summary, nil
}

// nearestRank 返回升序排列的非空样本的指定分位数。
// 调用方保证 1 <= percent <= 100；函数不修改输入。
func nearestRank(sorted []time.Duration, percent int) time.Duration {
	n := len(sorted)

	rank := (n/100)*percent + ((n%100)*percent+99)/100

	return sorted[rank-1]
}
