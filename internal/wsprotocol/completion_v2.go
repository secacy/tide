package wsprotocol

import "encoding/json"

// CompletedMessage 描述整场正常识别的固定完成范围。
// 同一场恢复后 FinalOffset/LastSeq 不变，Generation 随连接变化。
type CompletedMessage struct {
	Type        MessageType `json:"type"`
	Generation  uint64      `json:"generation,string"`
	FinalOffset uint64      `json:"finalOffset,string"`
	LastSeq     uint64      `json:"lastSeq,string"` // 无识别结果时为 0。
}

// CompletedAckMessage 表示客户端已应用全部结果并记录成功。
// 连接代次由 reader 提供，不使用客户端自报代次。
type CompletedAckMessage struct {
	Type        MessageType `json:"type"`
	FinalOffset uint64      `json:"finalOffset,string"`
	LastSeq     uint64      `json:"lastSeq,string"`
}

// v2CompletedAckWire 保留必需字段的 JSON 类型，避免缺失被默认零值掩盖。
type v2CompletedAckWire struct {
	FinalOffset json.RawMessage `json:"finalOffset"`
	LastSeq     json.RawMessage `json:"lastSeq"`
}

// decodeV2CompletedAck 解码已验证的单个对象；两个位置均须为 uint64 十进制字符串。
func decodeV2CompletedAck(data []byte) (V2Input, error) {
	var message v2CompletedAckWire
	if err := json.Unmarshal(data, &message); err != nil {
		return V2Input{}, invalidV2Input("decode completed ack", err)
	}
	offset, err := decodeUint64String(message.FinalOffset, "finalOffset")
	if err != nil {
		return V2Input{}, err
	}
	seq, err := decodeUint64String(message.LastSeq, "lastSeq")
	if err != nil {
		return V2Input{}, err
	}
	return V2Input{Kind: V2InputCompletedAck, Offset: offset, Seq: seq}, nil
}
