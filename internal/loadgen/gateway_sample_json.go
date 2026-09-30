package loadgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// gatewayStateJSON 保存成功查询到的状态。
type gatewayStateJSON struct {
	ActiveSessions int  `json:"active_sessions"` // 尚未完成清理的会话数。
	MaxSessions    int  `json:"max_sessions"`    // 配置接纳上限。
	Stopping       bool `json:"stopping"`        // 是否已停止接纳。
}

// gatewaySampleJSON 是采样行 v1，全部字段始终输出。
type gatewaySampleJSON struct {
	SchemaVersion int       `json:"schema_version"` // 固定为 1。
	Index         int       `json:"index"`          // 本次运行内的尝试序号。
	StartedAt     time.Time `json:"started_at"`     // 查询开始，UTC。
	FinishedAt    time.Time `json:"finished_at"`    // 查询返回，UTC。
	DurationNS    int64     `json:"duration_ns"`    // 原始查询耗时，整数纳秒。

	State *gatewayStateJSON `json:"state"` // 查询失败时为 null。
	Error *string           `json:"error"` // 查询成功时为 null。
}

// WriteGatewaySampleJSON 校验并将一条采样记录写为一行 JSON。
// sample.Err 是查询事实，合法失败样本仍可成功写出。
// 返回值只表示校验、编码或写入失败。
// 不修改 sample，不创建、关闭或刷新 writer，不保存跨行状态。
// 写入失败可能留下部分行，调用方应停止继续写入。
func WriteGatewaySampleJSON(w io.Writer, sample GatewaySample) error {
	if w == nil {
		return errors.New("write gateway sample json: writer is nil")
	}

	if sample.Index < 0 {
		return fmt.Errorf("write gateway sample json: index must not be negative: %d", sample.Index)
	}

	if sample.StartedAt.IsZero() {
		return errors.New("write gateway sample json: started_at is zero")
	}

	if sample.FinishedAt.IsZero() {
		return errors.New("write gateway sample json: finished_at is zero")
	}

	if sample.Duration < 0 {
		return fmt.Errorf("write gateway sample json: duration is negative: %s", sample.Duration)
	}

	if (sample.State == nil) == (sample.Err == nil) {
		return errors.New("write gateway sample json: exactly one of state or error must be set")
	}

	var stateJSON *gatewayStateJSON
	if sample.State != nil {
		if sample.State.MaxSessions <= 0 {
			return fmt.Errorf("write gateway sample json: max_sessions must be greater than zero: %d", sample.State.MaxSessions)
		}

		if sample.State.ActiveSessions < 0 || sample.State.ActiveSessions > sample.State.MaxSessions {
			return fmt.Errorf("write gateway sample json: active_sessions is %d, want range [0, %d]", sample.State.ActiveSessions, sample.State.MaxSessions)
		}

		stateJSON = &gatewayStateJSON{
			ActiveSessions: sample.State.ActiveSessions,
			MaxSessions:    sample.State.MaxSessions,
			Stopping:       sample.State.Stopping,
		}
	}

	output := gatewaySampleJSON{
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
		return fmt.Errorf("write gateway sample json: encode: %w", err)
	}

	data = append(data, '\n')

	n, err := w.Write(data)
	if err != nil {
		return fmt.Errorf("write gateway sample json: write: %w", err)
	}

	if n != len(data) {
		return fmt.Errorf("write gateway sample json: wrote %d of %d bytes: %w", n, len(data), io.ErrShortWrite)
	}

	return nil
}
