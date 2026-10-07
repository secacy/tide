package wsprotocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalidV2Handshake 表示首条消息的格式或字段非法。
// 错误文本不得包含恢复凭据或原始报文。
var ErrInvalidV2Handshake = errors.New("invalid v2 handshake")

// ResumeMessage 请求通过新连接接回原逻辑会话。
// 包含恢复凭据，不得整体写入普通日志。
type ResumeMessage struct {
	Type        MessageType `json:"type"`              // 固定 resume。
	Version     string      `json:"version"`           // 固定 v2。
	SessionID   string      `json:"sessionId"`         // 原会话 ID。
	ResumeToken string      `json:"resumeToken"`       // 恢复凭据。
	AppliedSeq  uint64      `json:"appliedSeq,string"` // 连续已应用位置。
}

// V2HandshakeKind 区分新建与恢复；零值非法。
type V2HandshakeKind uint8

const (
	V2HandshakeInvalid V2HandshakeKind = iota
	V2HandshakeStart                   // 请求新建。
	V2HandshakeResume                  // 请求恢复。
)

// V2Handshake 是通过线格式校验的请求。
// 不代表身份有效，也不代表会话允许恢复。
type V2Handshake struct {
	Kind        V2HandshakeKind // 指定下面哪些字段有效。
	SessionID   string          // 仅 resume 有效，原样保留。
	ResumeToken string          // 仅 resume 有效，原样保留。
	AppliedSeq  uint64          // 仅 resume 有效，允许 0。
}

// DecodeV2Handshake 解析一条完整的首条文本消息。
// ID/token 原样保留，身份认证由入口负责；未知字段忽略，重复字段沿用 JSON 后值覆盖。
// 失败返回零值和可用 errors.Is 判断的 ErrInvalidV2Handshake。
func DecodeV2Handshake(data []byte) (V2Handshake, error) {
	raw, err := decodeSingleJSONObject(data)
	if err != nil {
		return V2Handshake{}, invalidV2Handshake("expected one JSON object")
	}
	var wire v2HandshakeWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return V2Handshake{}, invalidV2Handshake("invalid handshake fields")
	}
	typ, err := decodeRequiredString(wire.Type, "type")
	if err != nil {
		return V2Handshake{}, invalidV2Handshake("invalid type")
	}
	version, err := decodeRequiredString(wire.Version, "version")
	if err != nil || version != "v2" {
		return V2Handshake{}, invalidV2Handshake("invalid version")
	}
	switch MessageType(typ) {
	case MessageTypeStart:
		if len(wire.SessionID) != 0 || len(wire.ResumeToken) != 0 || len(wire.AppliedSeq) != 0 {
			return V2Handshake{}, invalidV2Handshake("start contains resume fields")
		}
		return V2Handshake{Kind: V2HandshakeStart}, nil
	case MessageTypeResume:
		id, err := decodeRequiredString(wire.SessionID, "sessionId")
		if err != nil || id == "" {
			return V2Handshake{}, invalidV2Handshake("invalid sessionId")
		}
		token, err := decodeRequiredString(wire.ResumeToken, "resumeToken")
		if err != nil || token == "" {
			return V2Handshake{}, invalidV2Handshake("invalid resumeToken")
		}
		seq, err := decodeUint64String(wire.AppliedSeq, "appliedSeq")
		if err != nil {
			return V2Handshake{}, invalidV2Handshake("invalid appliedSeq")
		}
		return V2Handshake{Kind: V2HandshakeResume, SessionID: id, ResumeToken: token, AppliedSeq: seq}, nil
	default:
		return V2Handshake{}, invalidV2Handshake("unsupported type")
	}
}

// v2HandshakeWire 保留字段的原始 JSON 类型，区分缺失、null 和默认零值。
// 含恢复凭据，仅供解析，不得整体输出到普通日志。
type v2HandshakeWire struct {
	Type        json.RawMessage `json:"type"`        // 必需字符串。
	Version     json.RawMessage `json:"version"`     // 必需字符串 v2。
	SessionID   json.RawMessage `json:"sessionId"`   // resume 必需；start 禁止出现。
	ResumeToken json.RawMessage `json:"resumeToken"` // resume 必需；start 禁止出现。
	AppliedSeq  json.RawMessage `json:"appliedSeq"`  // resume 必需的十进制字符串。
}

// invalidV2Handshake 仅添加由代码提供的固定原因，不透传底层解析错误或报文值。
// JSON/数值错误可能含用户输入，因此只保留握手错误的 errors.Is 身份。
func invalidV2Handshake(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidV2Handshake, reason)
}
