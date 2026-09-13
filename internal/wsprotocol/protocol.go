package wsprotocol

type MessageType string

const (
	MessageTypeStart      MessageType = "start"
	MessageTypeEnd        MessageType = "end"
	MessageTypeResult     MessageType = "result"
	MessageTypeError      MessageType = "error"
	MessageTypeReady      MessageType = "ready"
	MessageTypeCheckpoint MessageType = "checkpoint"
)

// StartMessage 在音频开始发送前发送
type StartMessage struct {
	SessionID  string      `json:"sessionId,omitempty"`
	AttemptID  string      `json:"attemptId,omitempty"`
	FromSample uint64      `json:"fromSample,omitempty"`
	Type       MessageType `json:"type"`
	Version    string      `json:"version"` // 当前 WebSocket 应用层协议版本
}

// EndMessage 表示客户端已经发送完全部音频。
type EndMessage struct {
	ThroughSample uint64      `json:"throughSample,omitempty"`
	Type          MessageType `json:"type"`
}

// ResultMessage 表示 Gateway 返回给客户端的识别结果。
type ResultMessage struct {
	Type      MessageType `json:"type"`
	SegmentID string      `json:"segmentId"`
	Text      string      `json:"text"`    // Mock Worker 返回的文本结果
	IsFinal   bool        `json:"isFinal"` // 该结果是否为最终结果
}

// ErrorMessage 表示 Gateway 返回的业务或后端错误。
type ErrorMessage struct {
	Type    MessageType `json:"type"`
	Message string      `json:"message"`
}

// RecoveryMessage is a v2 ready or atomic immutable checkpoint envelope.
// Sample offsets refer to the logical visit; identity fences old attempts.
type RecoveryMessage struct {
	Type          MessageType `json:"type"`
	SessionID     string      `json:"sessionId"`
	AttemptID     string      `json:"attemptId"`
	FromSample    uint64      `json:"fromSample"`
	ThroughSample uint64      `json:"throughSample"`
	Text          string      `json:"text,omitempty"`
}
