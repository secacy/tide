package wsprotocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestCompletedAckDecode(t *testing.T) {
	for _, field := range []string{"finalOffset", "lastSeq"} {
		for _, value := range []string{"", "null", "0", "true", `""`, `"-1"`, `"+1"`, `" 1"`, `"1.0"`, `"１"`, `"18446744073709551616"`} {
			t.Run(field+"/"+value, func(t *testing.T) {
				other := "lastSeq"
				if field == other {
					other = "finalOffset"
				}
				body := fmt.Sprintf(`{"type":"completed_ack","%s":"0"`, other)
				if value != "" {
					body += fmt.Sprintf(`,"%s":%s`, field, value)
				}
				got, err := DecodeV2Control([]byte(body + "}"))
				if got.Kind != V2InputInvalid || got.Offset != 0 || got.Seq != 0 || got.Payload != nil || !errors.Is(err, ErrInvalidV2Input) {
					t.Fatalf("decode = %+v, %v", got, err)
				}
			})
		}
	}
	for _, tc := range []struct {
		name, body  string
		offset, seq uint64
	}{
		{"zero", `{"type":"completed_ack","finalOffset":"0","lastSeq":"0"}`, 0, 0},
		{"leading_zero", `{"type":"completed_ack","finalOffset":"002","lastSeq":"001","generation":"999"}`, 2, 1},
		{"maximum", `{"type":"completed_ack","finalOffset":"18446744073709551615","lastSeq":"18446744073709551615"}`, math.MaxUint64, math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeV2Control([]byte(tc.body))
			if err != nil || got.Kind != V2InputCompletedAck || got.Offset != tc.offset || got.Seq != tc.seq {
				t.Fatalf("decode = %+v, %v", got, err)
			}
		})
	}
}

func TestCompletedMessageWire(t *testing.T) {
	data, err := json.Marshal(CompletedMessage{Type: MessageTypeCompleted, Generation: math.MaxUint64, FinalOffset: math.MaxUint64})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"type":"completed","generation":"18446744073709551615","finalOffset":"18446744073709551615","lastSeq":"0"}` {
		t.Fatal(string(data))
	}
	data, err = json.Marshal(CompletedAckMessage{Type: MessageTypeCompletedAck})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeV2Control(data)
	if err != nil || got.Kind != V2InputCompletedAck || got.Offset != 0 || got.Seq != 0 {
		t.Fatal(got, err)
	}
}
