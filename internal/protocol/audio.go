package protocol

import (
	"encoding/binary"
	"errors"

	"github.com/secacy/tide/internal/relay"
)

const (
	HeaderBytes  = 8
	MaxPCMBytes  = 1280
	MaxWireBytes = HeaderBytes + MaxPCMBytes
)

var ErrInvalidAudioFrame = errors.New("invalid audio frame")

func ParseAudioFrame(data []byte) (relay.AudioFrame, error) {
	pcmBytes := len(data) - HeaderBytes
	if pcmBytes < 2 || pcmBytes > MaxPCMBytes || pcmBytes%2 != 0 {
		return relay.AudioFrame{}, ErrInvalidAudioFrame
	}
	return relay.AudioFrame{
		SampleOffset: binary.BigEndian.Uint64(data[:HeaderBytes]),
		PCM:          append([]byte(nil), data[HeaderBytes:]...),
	}, nil
}
