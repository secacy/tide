package realasr

import (
	"errors"
	"fmt"
	"unicode/utf8"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

var (
	errFinalized   = errors.New("snapshot mapper already finalized")
	errInvalidUTF8 = errors.New("snapshot contains invalid UTF-8")
)

// snapshotMapper 将单个模型流的完整快照转换为 Worker 响应。
// 每个 RPC 独占一个实例，串行调用，无需加锁。
type snapshotMapper struct {
	segmentID    string // 本 RPC 内稳定的片段 ID，首版使用 "1"。
	maxTextBytes int    // 单条文本的字节上限，不是字符数。
	lastText     string // 最近一次生成的文本快照。
	finalized    bool   // 已生成 final，不表示发送成功。
}

// newSnapshotMapper 校验非空、合法 UTF-8 的片段 ID 和正数文本字节上限。
func newSnapshotMapper(segmentID string, maxTextBytes int) (*snapshotMapper, error) {
	if segmentID == "" {
		return nil, errors.New("segment ID must not be empty")
	}
	if !utf8.ValidString(segmentID) {
		return nil, errors.New("segment ID contains invalid UTF-8")
	}
	if maxTextBytes <= 0 {
		return nil, errors.New("maxTextBytes must be positive")
	}
	return &snapshotMapper{
		segmentID:    segmentID,
		maxTextBytes: maxTextBytes,
	}, nil
}

// mapSnapshot 转换完整快照；final 表示调用者已完成正常尾部刷新。
// nil,nil 表示 partial 无变化；空文本修订及 final 仍可生成响应。
// 定稿后再次调用、非法 UTF-8 或文本超限均返回错误，不修改原状态。
// 本方法不执行 I/O；生成响应即推进状态，调用者发送失败时应结束 RPC。
// 每次生成独立响应，不设置 Progress；Seq 由 Gateway 分配。
func (m *snapshotMapper) mapSnapshot(text string, final bool) (*asrv1.StreamingRecognizeResponse, error) {
	// 1. final 之后禁止再次更新
	if m.finalized {
		return nil, errFinalized
	}
	// 2. 验证 UTF-8
	if !utf8.ValidString(text) {
		return nil, errInvalidUTF8
	}
	// 3. 按 UTF-8 字节数检查限制
	if len(text) > m.maxTextBytes {
		return nil, fmt.Errorf("snapshot text exceeds limit: %d > %d bytes", len(text), m.maxTextBytes)
	}
	// 4. 只对 partial 去重
	// 初始空 partial 也会自然被抑制
	if !final && text == m.lastText {
		return nil, nil
	}
	// 5. 每次创建新的响应对象
	resp := &asrv1.StreamingRecognizeResponse{
		SegmentId: m.segmentID,
		Text:      text,
		IsFinal:   final,
		Progress:  nil,
	}
	// 6. 只有响应构造成功后才更新状态
	m.lastText = text
	if final {
		m.finalized = true
	}
	return resp, nil
}
