package protocol

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestParseAudioFrame(t *testing.T) {
	data := make([]byte, HeaderBytes+MaxPCMBytes)
	binary.BigEndian.PutUint64(data, 1280)
	frame, err := ParseAudioFrame(data)
	if err != nil {
		t.Fatalf("ParseAudioFrame: %v", err)
	}
	if frame.SampleOffset != 1280 || len(frame.PCM) != MaxPCMBytes {
		t.Fatalf("unexpected frame: offset=%d bytes=%d", frame.SampleOffset, len(frame.PCM))
	}
	data[HeaderBytes] = 42
	if frame.PCM[0] != 0 {
		t.Fatal("PCM must be copied from the WebSocket read buffer")
	}
}

func TestParseAudioFrameRejectsInvalidLengths(t *testing.T) {
	for _, size := range []int{0, 7, 8, 9, HeaderBytes + MaxPCMBytes + 2} {
		if _, err := ParseAudioFrame(make([]byte, size)); !errors.Is(err, ErrInvalidAudioFrame) {
			t.Errorf("size %d: got %v", size, err)
		}
	}
}

func FuzzParseAudioFrame(f *testing.F) {
	f.Add(make([]byte, HeaderBytes+MaxPCMBytes))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := ParseAudioFrame(data)
		if err != nil {
			return
		}
		if len(frame.PCM) < 2 || len(frame.PCM) > MaxPCMBytes || len(frame.PCM)%2 != 0 {
			t.Fatalf("accepted invalid PCM size %d", len(frame.PCM))
		}
	})
}
