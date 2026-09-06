package streamer

import (
	"time"
)

const (
	sampleRate = 16000
	channels   = 1
	bitDepth   = 16
	bytesDepth = bitDepth / 8

	bytesPerSecond = sampleRate * channels * bytesDepth

	chunkDuration = 100 * time.Millisecond
)

// AudioDuration 根据 PCM 字节数计算其对应的音频时长。
func AudioDuration(audioBytes int) time.Duration {
	return time.Duration(audioBytes) * time.Second / time.Duration(bytesPerSecond)
}
