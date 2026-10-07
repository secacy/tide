package wsprotocol

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"
)

func TestAudioAckMessageExactWire(t *testing.T) {
	for _, offset := range []uint64{0, 1, 1<<53 + 1, math.MaxUint64} {
		t.Run(strconv.FormatUint(offset, 10), func(t *testing.T) {
			message := AudioAckMessage{Type: MessageTypeAudioAck, Generation: math.MaxUint64, NextOffset: offset, InputEnded: offset != 0}
			data, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 4 || string(fields["type"]) != `"audio_ack"` || string(fields["generation"]) != `"18446744073709551615"` || string(fields["nextOffset"]) != strconv.Quote(strconv.FormatUint(offset, 10)) || string(fields["inputEnded"]) != strconv.FormatBool(message.InputEnded) {
				t.Fatalf("unexpected audio ACK wire: %s", data)
			}
			var restored AudioAckMessage
			if err := json.Unmarshal(data, &restored); err != nil || restored != message {
				t.Fatal("audio ACK round trip changed fields", err)
			}
		})
	}
}
