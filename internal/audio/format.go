package audio

import (
	"time"
)

const (
	sampleRate = 16000
	channels   = 1
	bitDepth   = 16
	BytesDepth = bitDepth / 8

	BytesPerSecond    = sampleRate * channels * BytesDepth
	ChunkBytesDefault = 3200
)

// DurationFromBytes 根据 PCM 字节数计算其对应的音频时长。
func DurationFromBytes(audioBytes int) time.Duration {
	return time.Duration(audioBytes) * time.Second / time.Duration(BytesPerSecond)
}
