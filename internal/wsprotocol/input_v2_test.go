package wsprotocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"testing"
)

func TestV2InputAudio(t *testing.T) {
	for _, offset := range []uint64{0, 0x0102030405060708, math.MaxUint64} {
		t.Run(strconv.FormatUint(offset, 10), func(t *testing.T) {
			data := make([]byte, 11)
			binary.BigEndian.PutUint64(data, offset)
			copy(data[8:], []byte{0, 0xff, 1})
			got, err := DecodeV2Audio(data)
			if err != nil || got.Kind != V2InputAudio || got.Offset != offset || got.Seq != 0 || !bytes.Equal(got.Payload, data[8:]) {
				t.Fatalf("audio = (%+v, %v)", got, err)
			}
			// 解析只借用；取得独立副本属于会话接纳层的职责。
			if &got.Payload[0] != &data[8] {
				t.Fatal("decoder unexpectedly copied payload")
			}
		})
	}
	for _, size := range []int{0, 1, 7, 8} {
		t.Run("short_"+strconv.Itoa(size), func(t *testing.T) {
			got, err := DecodeV2Audio(make([]byte, size))
			assertInvalidV2Input(t, got, err)
		})
	}
}

func assertInvalidV2Input(t *testing.T, got V2Input, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalidV2Input) || got.Kind != V2InputInvalid || got.Offset != 0 || got.Seq != 0 || got.Payload != nil {
		t.Fatalf("invalid input = (%+v, %v)", got, err)
	}
}

func TestV2InputControlValid(t *testing.T) {
	for _, typ := range []MessageType{MessageTypeEnd, MessageTypeResultAck} {
		for _, value := range []string{"0", "00012", "9007199254740993", "18446744073709551615"} {
			t.Run(string(typ)+"/"+value, func(t *testing.T) {
				field, kind := "finalOffset", V2InputEnd
				if typ == MessageTypeResultAck {
					field, kind = "seq", V2InputResultAck
				}
				data := []byte(` {"type":` + strconv.Quote(string(typ)) + `,"` + field + `":` + strconv.Quote(value) + `,"extension":{"x":1}} ` + "\n")
				got, err := DecodeV2Control(data)
				want, _ := strconv.ParseUint(value, 10, 64)
				if err != nil || got.Kind != kind || got.Payload != nil {
					t.Fatalf("control = (%+v, %v)", got, err)
				}
				if kind == V2InputEnd && (got.Offset != want || got.Seq != 0) || kind == V2InputResultAck && (got.Seq != want || got.Offset != 0) {
					t.Fatalf("wrong numeric field: %+v", got)
				}
			})
		}
	}
	for _, data := range []string{
		`{"type":"end","finalOffset":"1","finalOffset":"2"}`,
		`{"type":"result_ack","type":"end","finalOffset":"2"}`,
		`{"type":"end","finalOffset":"\u0032"}`,
	} {
		t.Run("standard_json_"+data, func(t *testing.T) {
			got, err := DecodeV2Control([]byte(data))
			if err != nil || got.Kind != V2InputEnd || got.Offset != 2 {
				t.Fatalf("documented JSON semantics = (%+v, %v)", got, err)
			}
		})
	}
}

func TestV2InputControlRejectsInvalidEnvelope(t *testing.T) {
	cases := []string{"", " ", "null", "[]", `"end"`, "1", "true", "{", "{}",
		`{"type":null}`, `{"type":1}`, `{"type":true}`, `{"type":[]}`, `{"type":{}}`,
		`{"type":""}`, `{"type":"start","version":"v2"}`, `{"type":"resume"}`,
		`{"type":"result"}`, `{"type":"audio"}`, `{"type":"end"}`,
		`{"type":"end","finalOffset":"0"} {}`, `{"type":"end","finalOffset":"0"} null`,
		`{"type":"end","finalOffset":"0"} garbage`,
		`{"type":"end","finalOffset":"0",}`,
	}
	for i, data := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got, err := DecodeV2Control([]byte(data))
			assertInvalidV2Input(t, got, err)
		})
	}
}

func TestV2InputControlRejectsInvalidPositions(t *testing.T) {
	for _, typ := range []MessageType{MessageTypeEnd, MessageTypeResultAck} {
		field := "finalOffset"
		if typ == MessageTypeResultAck {
			field = "seq"
		}
		for _, raw := range []string{"", "null", "0", "true", "[]", "{}", `""`, `" "`, `" 1"`, `"1 "`, `"+1"`, `"-1"`, `"1.0"`, `"1e2"`, `"１２"`, `"18446744073709551616"`} {
			t.Run(string(typ)+"/"+raw, func(t *testing.T) {
				data := `{"type":` + strconv.Quote(string(typ))
				if raw != "" {
					data += `,"` + field + `":` + raw
				}
				got, err := DecodeV2Control([]byte(data + `}`))
				assertInvalidV2Input(t, got, err)
			})
		}
	}
}

func TestV2InputMessageWire(t *testing.T) {
	for _, message := range []any{
		V2EndMessage{Type: MessageTypeEnd, FinalOffset: "18446744073709551615"},
		ResultAckMessage{Type: MessageTypeResultAck, Seq: "9007199254740993"},
	} {
		data, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeV2Control(data); err != nil {
			t.Fatalf("outgoing control cannot decode: %s: %v", data, err)
		}
	}
	data, err := json.Marshal(EndMessage{Type: MessageTypeEnd})
	if err != nil || string(data) != `{"type":"end"}` {
		t.Fatalf("v1 end changed: %s, %v", data, err)
	}
}
