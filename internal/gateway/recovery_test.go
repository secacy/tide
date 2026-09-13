package gateway

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

func TestRecoveryAudioAndEndBoundaries(t *testing.T) {
	frame := func(from uint64, pcm []byte) []byte {
		b := make([]byte, 8+len(pcm))
		binary.BigEndian.PutUint64(b, from)
		copy(b[8:], pcm)
		return b
	}
	for _, test := range []struct {
		name string
		from uint64
		data []byte
	}{
		{"header_only", 100, frame(100, nil)}, {"odd_pcm", 100, frame(100, []byte{1})},
		{"gap", 100, frame(101, []byte{0, 0})}, {"repeated_audio", 100, frame(99, []byte{0, 0})},
		{"overflow", math.MaxUint64, frame(math.MaxUint64, []byte{0, 0})},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := newRecoveryState(wsprotocol.StartMessage{FromSample: test.from})
			if _, err := r.decodeAudio(test.data); err == nil {
				t.Fatal("invalid frame accepted")
			}
			if r.received != test.from {
				t.Fatal("invalid frame advanced input")
			}
		})
	}
	r := newRecoveryState(wsprotocol.StartMessage{FromSample: 100})
	if b, err := r.decodeAudio(frame(100, []byte{1, 2, 3, 4})); err != nil || len(b) != 4 {
		t.Fatalf("%x %v", b, err)
	}
	s := &session{recovery: r}
	if err := s.parseEndMessage([]byte(`{"type":"end","throughSample":101}`)); err == nil {
		t.Fatal("short End accepted")
	}
	if err := s.parseEndMessage([]byte(`{"type":"end","throughSample":102}`)); err != nil {
		t.Fatal(err)
	}
}
func TestRecoveryCheckpointContract(t *testing.T) {
	for _, test := range []struct {
		name string
		resp *asrv1.StreamingRecognizeResponse
	}{
		{"hole", &asrv1.StreamingRecognizeResponse{Checkpoint: &asrv1.RecoveryCheckpoint{FromSample: 11, ThroughSample: 20}}},
		{"ahead", &asrv1.StreamingRecognizeResponse{Checkpoint: &asrv1.RecoveryCheckpoint{FromSample: 10, ThroughSample: 21}}},
		{"empty", &asrv1.StreamingRecognizeResponse{Checkpoint: &asrv1.RecoveryCheckpoint{FromSample: 10, ThroughSample: 10}}},
		{"mixed", &asrv1.StreamingRecognizeResponse{Text: "ordinary", Checkpoint: &asrv1.RecoveryCheckpoint{FromSample: 10, ThroughSample: 20}}},
		{"mixed_progress", &asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: 1}, Checkpoint: &asrv1.RecoveryCheckpoint{FromSample: 10, ThroughSample: 20}}},
		{"duplicate_ready", &asrv1.StreamingRecognizeResponse{Ready: true}},
		{"ordinary_final", &asrv1.StreamingRecognizeResponse{Text: "final", IsFinal: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := newRecoveryState(wsprotocol.StartMessage{FromSample: 10})
			r.sent.Store(20)
			r.ready = true
			if _, _, err := r.response(test.resp); err == nil {
				t.Fatal("invalid checkpoint accepted")
			}
			if r.committed != 10 {
				t.Fatal("invalid checkpoint committed")
			}
		})
	}
	r := newRecoveryState(wsprotocol.StartMessage{SessionID: "visit", AttemptID: "try", FromSample: 10})
	r.sent.Store(20)
	if _, _, err := r.response(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: 1}}); err == nil {
		t.Fatal("progress before ready")
	}
	if _, _, err := r.response(&asrv1.StreamingRecognizeResponse{Ready: true}); err != nil {
		t.Fatal(err)
	}
	msg, handled, err := r.response(&asrv1.StreamingRecognizeResponse{Checkpoint: &asrv1.RecoveryCheckpoint{FromSample: 10, ThroughSample: 20}})
	if err != nil || !handled || msg.Text != "" || r.committed != 20 || msg.SessionID != "visit" || msg.AttemptID != "try" {
		t.Fatalf("silence checkpoint: %+v %v", msg, err)
	}
}
