package wsprotocol

// AudioAckMessage 确认网关已接纳的连续音频前缀以及合法输入终点。
// 位置是字节数；数值使用十进制字符串，零位置和 false 必须保留。
type AudioAckMessage struct {
	Type       MessageType `json:"type"`              // 固定 audio_ack。
	Generation uint64      `json:"generation,string"` // 当前连接的固定代次。
	NextOffset uint64      `json:"nextOffset,string"` // [0,NextOffset) 已被当前逻辑会话接纳。
	InputEnded bool        `json:"inputEnded"`        // 已接纳合法 end，NextOffset 即最终输入位置。
}
