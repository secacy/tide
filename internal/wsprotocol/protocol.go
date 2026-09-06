package wsprotocol

type MessageType string

const (
	MessageTypeStart  MessageType = "start"
	MessageTypeEnd    MessageType = "end"
	MessageTypeResult MessageType = "result"
	MessageTypeError  MessageType = "error"
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
	Type  MessageType `json:"type"`
	Text  string      `json:"text"`  // Mock Worker 返回的文本结果
	Final bool        `json:"final"` // 该结果是否为最终结果
}

// ErrorMessage 表示 Gateway 返回的业务或后端错误。
type ErrorMessage struct {
	Type    MessageType `json:"type"`
	Message string      `json:"message"`
}
