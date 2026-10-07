package wsprotocol

import (
	"encoding/json"
	"testing"
)

func TestV2ErrorWireContract(t *testing.T) {
	message := V2ErrorMessage{Type: MessageTypeError, Code: V2ErrorSessionBusy, Message: "session busy"}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"type":"error","code":"session_busy","message":"session busy"}` {
		t.Fatal("wire format changed", string(data))
	}
	var decoded V2ErrorMessage
	if err := json.Unmarshal(data, &decoded); err != nil || decoded != message {
		t.Fatal(decoded, err)
	}
}

func TestV2ErrorKeepsV1WireContract(t *testing.T) {
	data, err := json.Marshal(ErrorMessage{Type: MessageTypeError, Message: "session limit exceeded"})
	if err != nil || string(data) != `{"type":"error","message":"session limit exceeded"}` {
		t.Fatal("v1 error format changed", string(data), err)
	}
}
