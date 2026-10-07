package wsprotocol

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"
)

func TestReadyMessageExactWire(t *testing.T) {
	for _, position := range []uint64{0, 1, 1<<53 + 1, math.MaxUint64} {
		t.Run(strconv.FormatUint(position, 10), func(t *testing.T) {
			message := ReadyMessage{Type: MessageTypeReady, SessionID: "session", ResumeToken: "token",
				Generation: 1, NextOffset: position, InputEnded: position != 0, AckedResultSeq: position}
			if position != 0 {
				message.Generation = position
			}
			data, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 7 || string(fields["type"]) != `"ready"` || string(fields["sessionId"]) != `"session"` || string(fields["resumeToken"]) != `"token"` || string(fields["inputEnded"]) != strconv.FormatBool(message.InputEnded) {
				t.Fatal("ready lost a required wire field")
			}
			for key, value := range map[string]uint64{"generation": message.Generation, "nextOffset": position, "ackedResultSeq": position} {
				if string(fields[key]) != strconv.Quote(strconv.FormatUint(value, 10)) {
					t.Errorf("%s was omitted or lost integer precision", key)
				}
			}
			var restored ReadyMessage
			if err := json.Unmarshal(data, &restored); err != nil || restored != message {
				t.Fatal("ready round trip changed fields", err)
			}
		})
	}
}
