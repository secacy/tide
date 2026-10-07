package wsprotocol

// ReadyMessage 是已接管连接的第一条服务端应用消息。
// 数值以十进制字符串传输，零位置也必须保留。
type ReadyMessage struct {
	Type           MessageType `json:"type"`                  // 固定 ready。
	SessionID      string      `json:"sessionId"`             // 本场稳定标识。
	ResumeToken    string      `json:"resumeToken"`           // 恢复凭据，不得记录到普通日志。
	Generation     uint64      `json:"generation,string"`     // 当前连接代次。
	NextOffset     uint64      `json:"nextOffset,string"`     // 网关已连续接纳的音频位置。
	InputEnded     bool        `json:"inputEnded"`            // 是否已接纳合法 end。
	AckedResultSeq uint64      `json:"ackedResultSeq,string"` // 已接纳的累计结果确认。
}
