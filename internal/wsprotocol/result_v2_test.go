package wsprotocol

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"
)

func TestSequencedResultMessageExactWire(t *testing.T) {
	for _, seq := range []uint64{1, 1<<53 + 1, math.MaxUint64} {
		t.Run(strconv.FormatUint(seq, 10), func(t *testing.T) {
			message := SequencedResultMessage{Type: MessageTypeResult, Seq: seq, SegmentID: "片段", Text: "你好\n\"医生\"", IsFinal: true}
			data, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 5 || string(fields["seq"]) != strconv.Quote(strconv.FormatUint(seq, 10)) || string(fields["type"]) != `"result"` || string(fields["isFinal"]) != "true" {
				t.Fatalf("unexpected wire fields: %s", data)
			}
			var restored SequencedResultMessage
			if err := json.Unmarshal(data, &restored); err != nil || restored != message {
				t.Fatalf("round trip = (%+v,%v)", restored, err)
			}
		})
	}
}

func TestSequencedResultMessageKeepsV1Wire(t *testing.T) {
	data, err := json.Marshal(ResultMessage{Type: MessageTypeResult, SegmentID: "s", Text: "old", IsFinal: false})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"type":"result","segmentId":"s","text":"old","isFinal":false}` {
		t.Fatalf("v1 wire changed: %s", data)
	}
}
