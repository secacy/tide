package loadgen

import (
	"errors"
	"fmt"
	"io"

	"github.com/secacy/tide-artisan/internal/audio"
)

// NewSilenceSource 创建固定长度的 PCM 静音源。
// totalBytes 必须为正，且是 audio.BytesDepth 的整数倍。
// 每次调用返回独立读取游标；同一个返回值由单个发送协程使用。
func NewSilenceSource(totalBytes int64) (io.Reader, error) {
	if totalBytes <= 0 {
		return nil, errors.New("totalBytes must be greater than zero")
	}
	if totalBytes%audio.BytesDepth != 0 {
		return nil, fmt.Errorf("totalBytes must be a multiple of audio.BytesDepth: totalBytes=%d bytesDepth=%d", totalBytes, audio.BytesDepth)
	}
	return io.LimitReader(silenceReader{}, totalBytes), nil
}

// silenceReader 按需生成零值 PCM，不保存完整音频。
// 总长度由外层 LimitReader 控制，发送节奏由客户端控制。
type silenceReader struct{}

// Read 将 p 全部清零，返回 len(p), nil。
// 它允许任意大小的缓冲区，不负责划分音频块。
func (silenceReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
