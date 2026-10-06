package wsprotocol

// SequencedResultMessage 是可恢复协议的一条识别更新。
type SequencedResultMessage struct {
	Type      MessageType `json:"type"`       // 固定为 result。
	Seq       uint64      `json:"seq,string"` // 更新序号，以十进制字符串传输。
	SegmentID string      `json:"segmentId"`  // Worker 的片段标识。
	Text      string      `json:"text"`       // 本次更新文本。
	IsFinal   bool        `json:"isFinal"`    // 表示片段定稿。
}
