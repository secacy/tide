package wsprotocol

type MessageType string

const (
	MessageTypeStart     MessageType = "start"
	MessageTypeEnd       MessageType = "end"
	MessageTypeResult    MessageType = "result"
	MessageTypeError     MessageType = "error"
	MessageTypeResultAck MessageType = "result_ack"
)

// StartMessage 在音频开始发送前发送
type StartMessage struct {
	Type    MessageType `json:"type"`
	Version string      `json:"version"` // 当前 WebSocket 应用层协议版本
}

// EndMessage 表示客户端已经发送完全部音频。
type EndMessage struct {
	Type MessageType `json:"type"`
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

// V2EndMessage 表示可恢复协议中客户端已经发送完全部音频。
// FinalOffset 是客户端已发送连续音频的最终字节位置。
type V2EndMessage struct {
	Type        MessageType `json:"type"`
	FinalOffset string      `json:"finalOffset"`
}

// ResultAckMessage 表示客户端已经连续应用到 Seq。
type ResultAckMessage struct {
	Type MessageType `json:"type"`
	Seq  string      `json:"seq"`
}
