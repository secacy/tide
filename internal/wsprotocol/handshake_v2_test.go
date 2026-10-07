package wsprotocol

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestV2HandshakeValid(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		want       V2Handshake
	}{
		{"start", `{"type":"start","version":"v2"}`, V2Handshake{Kind: V2HandshakeStart}},
		{"start_extensions", ` {"type":"start","version":"v2","extension":{"x":1}} `, V2Handshake{Kind: V2HandshakeStart}},
		{"resume_zero", `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"token","appliedSeq":"0"}`, V2Handshake{Kind: V2HandshakeResume, SessionID: "id", ResumeToken: "token"}},
		{"resume_large", `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"token","appliedSeq":"9007199254740993"}`, V2Handshake{Kind: V2HandshakeResume, SessionID: "id", ResumeToken: "token", AppliedSeq: 9007199254740993}},
		{"resume_maximum", `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"token","appliedSeq":"18446744073709551615"}`, V2Handshake{Kind: V2HandshakeResume, SessionID: "id", ResumeToken: "token", AppliedSeq: math.MaxUint64}},
		{"opaque_identity", `{"type":"resume","version":"v2","sessionId":" id ","resumeToken":" token ","appliedSeq":"0012","generation":"999"}`, V2Handshake{Kind: V2HandshakeResume, SessionID: " id ", ResumeToken: " token ", AppliedSeq: 12}},
		{"duplicate_fields", `{"type":"unknown","type":"start","version":"v1","version":"v2"}`, V2Handshake{Kind: V2HandshakeStart}},
		{"escaped_digits", `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"token","appliedSeq":"1","appliedSeq":"\u0032"}`, V2Handshake{Kind: V2HandshakeResume, SessionID: "id", ResumeToken: "token", AppliedSeq: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeV2Handshake([]byte(tc.data))
			if err != nil || got != tc.want {
				t.Fatal("unexpected handshake or error (credentials redacted)", err)
			}
		})
	}
}

// assertInvalidHandshake 检查失败无部分请求值，不输出含凭据的对象或报文。
func assertInvalidHandshake(t *testing.T, data []byte) error {
	t.Helper()
	got, err := DecodeV2Handshake(data)
	if got != (V2Handshake{}) || !errors.Is(err, ErrInvalidV2Handshake) {
		t.Fatal("invalid handshake returned partial value or wrong error")
	}
	return err
}

func TestV2HandshakeInvalidObject(t *testing.T) {
	for _, tc := range []struct{ name, data string }{
		{"empty", ""}, {"whitespace", " \n"}, {"null", "null"}, {"array", "[]"}, {"string", `"start"`}, {"number", "1"},
		{"incomplete", `{"type":"start"`}, {"empty_object", `{}`},
		{"two_objects", `{"type":"start","version":"v2"} {}`},
		{"trailing_null", `{"type":"start","version":"v2"} null`},
		{"trailing_garbage", `{"type":"start","version":"v2"} x`},
		{"unsupported_type", `{"type":"end","version":"v2"}`},
		{"wrong_version", `{"type":"start","version":"v1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) { assertInvalidHandshake(t, []byte(tc.data)) })
	}
}

func TestV2HandshakeRequiredStrings(t *testing.T) {
	for _, field := range []string{"type", "version", "sessionId", "resumeToken"} {
		for _, value := range []string{"missing", "null", "0", "true", "[]", "{}", `""`} {
			t.Run(field+"/"+value, func(t *testing.T) {
				wire := map[string]json.RawMessage{"type": json.RawMessage(`"resume"`), "version": json.RawMessage(`"v2"`), "sessionId": json.RawMessage(`"id"`), "resumeToken": json.RawMessage(`"token"`), "appliedSeq": json.RawMessage(`"0"`)}
				if value == "missing" {
					delete(wire, field)
				} else {
					wire[field] = json.RawMessage(value)
				}
				data, err := json.Marshal(wire)
				if err != nil {
					t.Fatal(err)
				}
				assertInvalidHandshake(t, data)
			})
		}
	}
}

func TestV2HandshakeStartRejectsResumeFields(t *testing.T) {
	for _, field := range []string{"sessionId", "resumeToken", "appliedSeq"} {
		for _, value := range []string{"null", `"0"`} {
			t.Run(field+"/"+value, func(t *testing.T) {
				assertInvalidHandshake(t, []byte(`{"type":"start","version":"v2","`+field+`":`+value+`}`))
			})
		}
	}
}

func TestV2HandshakeInvalidAppliedSequence(t *testing.T) {
	for i, value := range []string{"missing", "null", "0", `""`, `"-1"`, `"+1"`, `" 1"`, `"1 "`, `"1.0"`, `"１"`, `"18446744073709551616"`} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			body := `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"token"`
			if value != "missing" {
				body += `,"appliedSeq":` + value
			}
			body += `}`
			assertInvalidHandshake(t, []byte(body))
		})
	}
}

func TestV2HandshakeErrorsRedactInput(t *testing.T) {
	const secret = "credential-marker-do-not-log"
	for _, tc := range []struct{ name, data string }{
		{"invalid_json", `{"resumeToken":"` + secret + `",`},
		{"unknown_type", `{"type":"` + secret + `","version":"v2"}`},
		{"unknown_version", `{"type":"start","version":"` + secret + `"}`},
		{"token_wrong_type", `{"type":"resume","version":"v2","sessionId":"id","resumeToken":{"secret":"` + secret + `"},"appliedSeq":"0"}`},
		{"sequence_wrong_type", `{"type":"resume","version":"v2","sessionId":"id","resumeToken":"` + secret + `","appliedSeq":"` + secret + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := assertInvalidHandshake(t, []byte(tc.data))
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), tc.data) {
				t.Fatal("error exposed input")
			}
		})
	}
}

func TestV2HandshakeWirePrecision(t *testing.T) {
	data, err := json.Marshal(ResumeMessage{Type: MessageTypeResume, Version: "v2", SessionID: "id", ResumeToken: "token", AppliedSeq: math.MaxUint64})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["appliedSeq"]) != `"18446744073709551615"` {
		t.Fatal("lost sequence string precision")
	}
	got, err := DecodeV2Handshake(data)
	if err != nil || got.Kind != V2HandshakeResume || got.AppliedSeq != math.MaxUint64 {
		t.Fatal("wire round trip failed")
	}
	data, err = json.Marshal(StartMessage{Type: MessageTypeStart, Version: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	got, err = DecodeV2Handshake(data)
	if err != nil || got != (V2Handshake{Kind: V2HandshakeStart}) {
		t.Fatal("start round trip failed")
	}
}
