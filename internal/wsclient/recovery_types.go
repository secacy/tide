package wsclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

var (
	ErrRecoveryBudget   = errors.New("recovery budget exhausted")
	ErrIncomplete       = errors.New("transcription contains unconfirmed audio")
	ErrRecoveryProtocol = errors.New("invalid recovery protocol")
)

// AudioSource separates capture lifetime from individual network attempts.
// Read must return on cancellation and obey the usual io.Reader count contract.
type AudioSource interface {
	Read(context.Context, []byte) (int, error)
}

// ReaderSource adapts promptly returning file/byte Readers, not blocking devices.
// Closing or interrupting an arbitrary io.Reader remains the caller's responsibility.
type ReaderSource struct{ Reader io.Reader }

func (s ReaderSource) Read(ctx context.Context, p []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.Reader == nil {
		return 0, fmt.Errorf("audio reader is nil")
	}
	return s.Reader.Read(p)
}

// RecoveryConfig configures one live client's bounded recovery policy.
// Observe runs in the state owner; it must return promptly and not retain/mutate input.
type RecoveryConfig struct {
	BufferDuration time.Duration // 未提交 PCM 的保留上限；零值 15 秒。
	MaxChunks      int           // 缓存条目上限；零值 4096，另受 PCM 字节上限约束。
	Budget         time.Duration // 一轮恢复共享的总预算；零值 10 秒。
	RetryDelay     time.Duration // 尝试退出后的重试间隔；零值 100 ms，计入总预算。
	ReadyTimeout   time.Duration // Dial 开始至 Ready 的单次预算；零值 3 秒。
	Observe        func(wsprotocol.RecoveryMessage)
}

func (c RecoveryConfig) normalize() (RecoveryConfig, error) {
	if c.BufferDuration < 0 || c.MaxChunks < 0 || c.Budget < 0 || c.RetryDelay < 0 || c.ReadyTimeout < 0 {
		return c, fmt.Errorf("negative recovery configuration")
	}
	if c.BufferDuration == 0 {
		c.BufferDuration = 15 * time.Second
	}
	if c.MaxChunks == 0 {
		c.MaxChunks = 4096
	}
	if c.Budget == 0 {
		c.Budget = 10 * time.Second
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = 100 * time.Millisecond
	}
	if c.ReadyTimeout == 0 {
		c.ReadyTimeout = 3 * time.Second
	}
	if c.BufferDuration > time.Hour {
		return c, fmt.Errorf("audio buffer duration exceeds one hour")
	}
	return c, nil
}

// Gap is unconfirmed transcript coverage, not a claim that every PCM byte was lost.
// Offsets are global sample positions in [FromSample, ThroughSample).
type Gap struct {
	FromSample, ThroughSample uint64
	Reason                    string
}

// Transcript belongs to the surviving client process. Complete additionally
// requires no gaps; successful closure of the last attempt alone is insufficient.
type Transcript struct {
	SessionID       string
	Segments        []wsprotocol.RecoveryMessage
	Gaps            []Gap
	CapturedThrough uint64
	Complete        bool
	Ended           bool
	Attempts        int
	CachePeakBytes  int
}

func (r *Transcript) addGap(from, to uint64, reason string) {
	if from >= to {
		return
	}
	n := len(r.Gaps)
	if n > 0 && r.Gaps[n-1].ThroughSample == from && r.Gaps[n-1].Reason == reason {
		r.Gaps[n-1].ThroughSample = to
		return
	}
	r.Gaps = append(r.Gaps, Gap{from, to, reason})
}

// capturedFrame is immutable PCM owned by capture, then by the coordinator.
type capturedFrame struct {
	from uint64
	data []byte
	end  bool
	err  error
}

func (f capturedFrame) through() uint64 { return f.from + uint64(len(f.data)/2) }

// pcmCache only retains uncommitted PCM. Trimming at a checkpoint supports a
// boundary inside a client block; offsets therefore do not depend on chunk size.
type pcmCache struct {
	frames                     []capturedFrame
	bytes, maxBytes, maxChunks int
}

func (b *pcmCache) append(f capturedFrame) bool {
	if len(f.data) > b.maxBytes-b.bytes || len(b.frames) >= b.maxChunks {
		return false
	}
	b.frames = append(b.frames, f)
	b.bytes += len(f.data)
	return true
}
func (b *pcmCache) drop(through uint64) {
	for len(b.frames) > 0 && b.frames[0].through() <= through {
		b.bytes -= len(b.frames[0].data)
		b.frames[0] = capturedFrame{}
		b.frames = b.frames[1:]
	}
	if len(b.frames) > 0 && b.frames[0].from < through {
		f := &b.frames[0]
		n := int(through-f.from) * 2
		f.data = f.data[n:]
		f.from = through
		b.bytes -= n
	}
}
func (b *pcmCache) at(from uint64) (capturedFrame, bool) {
	for _, f := range b.frames {
		if f.from <= from && from < f.through() {
			n := int(from-f.from) * 2
			f.data = f.data[n:]
			f.from = from
			return f, true
		}
	}
	return capturedFrame{}, false
}
func (b *pcmCache) clear() { clear(b.frames); b.frames = nil; b.bytes = 0 }
