package loadgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// workerProcessingJSON 保存启用限制时的名额计数。
type workerProcessingJSON struct {
	Limit   int `json:"limit"`   // 实际名额上限。
	InUse   int `json:"in_use"`  // 持有名额数。
	Waiting int `json:"waiting"` // 登记等待数。
}

// workerStateJSON 区分查询成功但未采集与已启用的计数。
type workerStateJSON struct {
	ProcessingLimitEnabled bool                  `json:"processing_limit_enabled"` // 共享限制开关。
	Processing             *workerProcessingJSON `json:"processing"`               // 未启用时为 null。
}

// workerSampleJSON 是采样行 v1，全部字段始终输出。
type workerSampleJSON struct {
	SourceKind    string    `json:"source_kind"`    // 固定 worker，避免与其他快照行混淆。
	SchemaVersion int       `json:"schema_version"` // 固定为 1。
	Index         int       `json:"index"`          // 本次运行内的尝试序号。
	StartedAt     time.Time `json:"started_at"`     // 查询开始，UTC。
	FinishedAt    time.Time `json:"finished_at"`    // 查询返回，UTC。
	DurationNS    int64     `json:"duration_ns"`    // 原始查询耗时，整数纳秒。

	State *workerStateJSON `json:"state"` // 查询失败时为 null。
	Error *string          `json:"error"` // 查询成功时为 null。
}

// WriteWorkerSampleJSON 校验并将一条采样记录写为一行 JSON。
// sample.Err 是查询事实，合法失败样本仍可成功写出。
// 返回值只表示校验、编码或写入失败。
// 不修改 sample，不创建、关闭或刷新 writer，不保存跨行状态。
// 写入失败可能留下部分行，调用方应停止继续写入。
func WriteWorkerSampleJSON(w io.Writer, sample WorkerSample) error {
	if w == nil {
		return errors.New("write worker sample json: writer is nil")
	}

	if sample.Index < 0 {
		return fmt.Errorf("write worker sample json: index must not be negative: %d", sample.Index)
	}

	if sample.StartedAt.IsZero() {
		return errors.New("write worker sample json: started_at is zero")
	}

	if sample.FinishedAt.IsZero() {
		return errors.New("write worker sample json: finished_at is zero")
	}

	if sample.Duration < 0 {
		return fmt.Errorf("write worker sample json: duration is negative: %s", sample.Duration)
	}

	if (sample.State == nil) == (sample.Err == nil) {
		return errors.New("write worker sample json: exactly one of state or error must be set")
	}

	var stateJSON *workerStateJSON
	if sample.State != nil {
		state := *sample.State
		if !state.ProcessingLimitEnabled {
			if state.Processing != (WorkerProcessingState{}) {
				return errors.New("write worker sample json: disabled limit has nonzero counts")
			}
			stateJSON = &workerStateJSON{}
		} else {
			p := state.Processing
			if p.Limit <= 0 || p.InUse < 0 || p.InUse > p.Limit || p.Waiting < 0 {
				return fmt.Errorf("write worker sample json: invalid processing counts: %+v", p)
			}
			stateJSON = &workerStateJSON{ProcessingLimitEnabled: true, Processing: &workerProcessingJSON{Limit: p.Limit, InUse: p.InUse, Waiting: p.Waiting}}
		}
	}

	output := workerSampleJSON{
		SourceKind:    "worker",
		SchemaVersion: 1,
		Index:         sample.Index,
		StartedAt:     sample.StartedAt.UTC(),
		FinishedAt:    sample.FinishedAt.UTC(),
		DurationNS:    int64(sample.Duration),
		State:         stateJSON,
		Error:         errorText(sample.Err),
	}

	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("write worker sample json: encode: %w", err)
	}

	data = append(data, '\n')

	n, err := w.Write(data)
	if err != nil {
		return fmt.Errorf("write worker sample json: write: %w", err)
	}

	if n != len(data) {
		return fmt.Errorf("write worker sample json: wrote %d of %d bytes: %w", n, len(data), io.ErrShortWrite)
	}

	return nil
}
