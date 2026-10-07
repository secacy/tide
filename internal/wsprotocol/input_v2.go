package wsprotocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ErrInvalidV2Input 表示已附着阶段的消息不符合 v2 线格式；可用 errors.Is 判断。
var ErrInvalidV2Input = errors.New("invalid v2 input")

// V2InputKind 区分已附着阶段的输入；零值无效。
type V2InputKind uint8

const (
	V2InputInvalid      V2InputKind = iota
	V2InputAudio                    // 二进制音频，含起始字节位置和非空负载。
	V2InputEnd                      // 输入最终位置；不代表识别或连接结束。
	V2InputResultAck                // 客户端已连续应用的结果位置。
	V2InputCompletedAck             // 客户端整场确认，包含最终音频位置和最后结果序号。
)

// V2Input 是一条已完成线格式校验的消息，不代表业务已接纳。
type V2Input struct {
	Kind    V2InputKind // 指定下面哪些字段有效。
	Offset  uint64      // audio 起点或 end/completed_ack 最终位置。
	Payload []byte      // 仅 audio；借用原数据，不得修改。
	Seq     uint64      // result_ack 已应用位置，completed_ack 最后序号。
}

// v2ControlEnvelope 保留原始 type，以区分字段缺失、null 和非字符串。
type v2ControlEnvelope struct {
	Type json.RawMessage `json:"type"`
}

// v2EndWire 在确认消息类型后读取必需的最终音频位置。
type v2EndWire struct {
	Type        json.RawMessage `json:"type"`
	FinalOffset json.RawMessage `json:"finalOffset"`
}

// v2ResultAckWire 保留累计结果位置的原始 JSON 类型，避免默认零掩盖缺失。
type v2ResultAckWire struct {
	Type json.RawMessage `json:"type"`
	Seq  json.RawMessage `json:"seq"`
}

// DecodeV2Audio 解码一个完整二进制消息；失败返回零值和包装后的错误。
// data 的前 8 字节为大端 offset，剩余字节为非空负载；返回 Payload 借用 data。
func DecodeV2Audio(data []byte) (V2Input, error) {
	if len(data) < 9 {
		return V2Input{}, fmt.Errorf("%w: audio requires offset header and non-empty payload", ErrInvalidV2Input)
	}

	return V2Input{
		Kind:    V2InputAudio,
		Offset:  binary.BigEndian.Uint64(data[:8]),
		Payload: data[8:],
	}, nil
}

// DecodeV2Control 解码一个完整文本消息；接受 end/result_ack/completed_ack。
// data 必须只有一个 JSON 对象；位置必须是可解析为 uint64 的十进制字符串。
// 未知字段忽略，同名字段沿用 encoding/json 的后值覆盖规则。
func DecodeV2Control(data []byte) (V2Input, error) {
	raw, err := decodeSingleJSONObject(data)
	if err != nil {
		return V2Input{}, err
	}

	var envelope v2ControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return V2Input{}, invalidV2Input("decode control envelope", err)
	}

	messageType, err := decodeMessageType(envelope.Type)
	if err != nil {
		return V2Input{}, err
	}

	switch messageType {
	case MessageTypeEnd:
		return decodeV2End(raw)

	case MessageTypeResultAck:
		return decodeV2ResultAck(raw)
	case MessageTypeCompletedAck:
		return decodeV2CompletedAck(raw)

	default:
		return V2Input{}, fmt.Errorf(
			"%w: unsupported control type %q",
			ErrInvalidV2Input,
			messageType,
		)
	}
}

// decodeV2End 从已验证的 JSON 对象中读取必需的 finalOffset。
func decodeV2End(data []byte) (V2Input, error) {
	var message v2EndWire
	if err := json.Unmarshal(data, &message); err != nil {
		return V2Input{}, invalidV2Input("decode end", err)
	}

	finalOffset, err := decodeUint64String(message.FinalOffset, "finalOffset")
	if err != nil {
		return V2Input{}, err
	}

	return V2Input{
		Kind:   V2InputEnd,
		Offset: finalOffset,
	}, nil
}

// decodeV2ResultAck 从已验证的 JSON 对象中读取必需的累计 seq。
func decodeV2ResultAck(data []byte) (V2Input, error) {
	var message v2ResultAckWire
	if err := json.Unmarshal(data, &message); err != nil {
		return V2Input{}, invalidV2Input("decode result ack", err)
	}

	seq, err := decodeUint64String(message.Seq, "seq")
	if err != nil {
		return V2Input{}, err
	}

	return V2Input{
		Kind: V2InputResultAck,
		Seq:  seq,
	}, nil
}

// decodeMessageType 要求 type 存在且为字符串；支持哪些值由调用方决定。
func decodeMessageType(raw json.RawMessage) (MessageType, error) {
	value, err := decodeRequiredString(raw, "type")
	if err != nil {
		return "", err
	}

	return MessageType(value), nil
}

// decodeSingleJSONObject 要求只有一个 JSON 对象，允许其前后存在空白。
func decodeSingleJSONObject(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))

	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, invalidV2Input("decode control message", err)
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("%w: control message must be an object", ErrInvalidV2Input)
	}

	var extra json.RawMessage

	switch err := decoder.Decode(&extra); {
	case errors.Is(err, io.EOF):
		return raw, nil

	case err != nil:
		return nil, invalidV2Input("decode trailing data", err)

	default:
		return nil, fmt.Errorf("%w: multiple JSON values", ErrInvalidV2Input)
	}
}

// decodeRequiredString 区分字符串与字段缺失、null 和其他 JSON 类型。
// field 仅用于错误定位；空字符串是否允许由调用方决定。
func decodeRequiredString(raw json.RawMessage, field string) (string, error) {
	if len(raw) == 0 {
		return "", fmt.Errorf("%w: missing %s", ErrInvalidV2Input, field)
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '"' {
		return "", fmt.Errorf("%w: %s must be a string", ErrInvalidV2Input, field)
	}

	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", invalidV2Input("decode "+field, err)
	}

	return value, nil
}

// decodeUint64String 接受非空 ASCII 十进制数字字符串，允许零和前导零。
// 拒绝符号、空白、非数字及 uint64 溢出；field 用于错误定位。
func decodeUint64String(raw json.RawMessage, field string) (uint64, error) {
	value, err := decodeRequiredString(raw, field)
	if err != nil {
		return 0, err
	}

	if value == "" {
		return 0, fmt.Errorf("%w: %s is empty", ErrInvalidV2Input, field)
	}

	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, fmt.Errorf("%w: %s must contain decimal digits only", ErrInvalidV2Input, field)
		}
	}

	valueUint64, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, invalidV2Input("decode "+field, err)
	}

	return valueUint64, nil
}

// invalidV2Input 添加解析位置并保留 ErrInvalidV2Input 的 errors.Is 身份。
// 底层 JSON/数值错误只作为诊断文本，不作为本协议的错误分类契约。
func invalidV2Input(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrInvalidV2Input, operation, err)
}
