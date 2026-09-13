package wsclient

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// RunRecoverable owns a logical consultation across sequential network attempts.
// Only this goroutine commits results and retires PCM. Source must honor Context;
// the returned report remains useful on interruption, including unconfirmed ranges.
func (c *Client) RunRecoverable(ctx context.Context, source AudioSource) (report Transcript, err error) {
	if source == nil {
		return report, fmt.Errorf("audio source is nil")
	}
	cfg := c.cfg.Recovery
	cache := pcmCache{maxBytes: int(cfg.BufferDuration*32000/time.Second) / 2 * 2, maxChunks: cfg.MaxChunks}
	if c.cfg.ChunkBytes > cache.maxBytes {
		return report, fmt.Errorf("audio chunk exceeds recovery buffer")
	}
	report.SessionID = rand.Text()
	runCtx, stop := context.WithCancel(ctx)
	frames := make(chan capturedFrame, 1)
	captureDone := make(chan struct{})
	var captured atomic.Uint64
	var captureEnded atomic.Bool
	go func() { defer close(captureDone); c.capture(runCtx, source, frames, &captured, &captureEnded) }()
	events := make(chan attemptEvent, 16)
	var active *recognitionAttempt
	var checkpoint uint64
	// fallback means retained history is insufficient. Only a new Ready can commit
	// the selected gap; receiving End before then forbids skipping to that point.
	fallback := false
	var deadline, retryAt, readyBy time.Time
	beginRecovery := func() {
		if deadline.IsZero() {
			deadline = time.Now().Add(cfg.Budget)
		}
	}
	invalidate := func() {
		if active != nil {
			active.invalid = true
			active.cancel()
		}
	}
	defer func() {
		stop()
		if active != nil {
			active.cancel()
			<-active.joined
		}
		<-captureDone
		report.CapturedThrough = captured.Load()
		report.Ended = captureEnded.Load()
		if err != nil {
			report.addGap(checkpoint, report.CapturedThrough, "interrupted")
		}
	}()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		now := time.Now()
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		if !deadline.IsZero() && !now.Before(deadline) {
			return report, ErrRecoveryBudget
		}
		if active != nil && !active.ready && !active.invalid && !now.Before(readyBy) {
			beginRecovery()
			invalidate()
		}
		if active == nil && (retryAt.IsZero() || !now.Before(retryAt)) {
			from := checkpoint
			if fallback {
				if report.Ended || captureEnded.Load() {
					return report, ErrIncomplete
				}
				if len(cache.frames) == 0 {
					return report, fmt.Errorf("%w: missing fallback audio", ErrIncomplete)
				}
				from = cache.frames[0].from
			}
			attemptCtx, cancel := context.WithCancel(runCtx)
			a := &recognitionAttempt{id: rand.Text(), from: from, sent: from, input: make(chan capturedFrame, 1), cancel: cancel, joined: make(chan struct{})}
			active = a
			report.Attempts++
			readyBy = now.Add(cfg.ReadyTimeout)
			start := wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2", SessionID: report.SessionID, AttemptID: a.id, FromSample: from}
			go func() {
				defer close(a.joined)
				emit := func(e attemptEvent) {
					select {
					case events <- e:
					case <-attemptCtx.Done():
					}
				}
				attemptErr, fatal := c.attemptIO(attemptCtx, start, a.input, emit)
				// Completion survives cancellation of this attempt. It follows all its
				// messages and is suppressed only when the entire logical run is stopping.
				select {
				case events <- attemptEvent{id: a.id, done: true, err: attemptErr, fatal: fatal}:
				case <-runCtx.Done():
				}
			}()
		}
		var input chan capturedFrame
		var next capturedFrame
		if active != nil && active.ready && !active.invalid && !active.ending {
			if f, ok := cache.at(active.sent); ok {
				input, next = active.input, f
			} else if report.Ended && active.sent == report.CapturedThrough {
				input, next = active.input, capturedFrame{from: active.sent, end: true}
			}
		}
		wake := deadline
		earlier := func(t time.Time) {
			if !t.IsZero() && (wake.IsZero() || t.Before(wake)) {
				wake = t
			}
		}
		if active == nil {
			earlier(retryAt)
		} else if !active.ready && !active.invalid {
			earlier(readyBy)
		}
		var tick <-chan time.Time
		if !wake.IsZero() {
			timer.Reset(max(0, time.Until(wake)))
			tick = timer.C
		}
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		case <-tick:
		case input <- next:
			if next.end {
				active.ending = true
			} else {
				active.sent = next.through()
			}
		case f := <-frames:
			if f.err != nil {
				return report, f.err
			}
			if f.end {
				report.Ended = true
				if fallback {
					return report, ErrIncomplete
				}
				continue
			}
			report.CapturedThrough = f.through()
			if !cache.append(f) {
				if report.Ended || captureEnded.Load() {
					return report, ErrIncomplete
				}
				beginRecovery()
				invalidate()
				fallback = true
				cache.clear()
				if !cache.append(f) {
					return report, fmt.Errorf("%w: audio block cannot fit", ErrIncomplete)
				}
			}
			report.CachePeakBytes = max(report.CachePeakBytes, cache.bytes)
		case e := <-events:
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return report, ErrRecoveryBudget
			}
			if active == nil || e.id != active.id {
				continue
			}
			if e.done {
				<-active.joined
				a := active
				active = nil
				a.cancel()
				if a.invalid {
					beginRecovery()
					retryAt = time.Now().Add(cfg.RetryDelay)
					continue
				}
				if e.fatal {
					return report, e.err
				}
				if e.err == nil {
					if !a.ending || checkpoint != report.CapturedThrough {
						return report, fmt.Errorf("%w: premature normal close", ErrRecoveryProtocol)
					}
					report.Complete = len(report.Gaps) == 0
					if !report.Complete {
						return report, ErrIncomplete
					}
					return report, nil
				}
				beginRecovery()
				retryAt = time.Now().Add(cfg.RetryDelay)
				continue
			}
			if active.invalid {
				continue
			}
			msg := *e.message
			if msg.Type == wsprotocol.MessageTypeReady {
				if active.ready || msg.FromSample != active.from || msg.ThroughSample != active.from || msg.Text != "" {
					return report, ErrRecoveryProtocol
				}
				active.ready = true
				if fallback {
					if report.Ended || captureEnded.Load() {
						return report, ErrIncomplete
					}
					report.addGap(checkpoint, active.from, "buffer_exhausted")
					checkpoint = active.from
					cache.drop(checkpoint)
					fallback = false
				}
				continue
			}
			if !active.ready {
				return report, ErrRecoveryProtocol
			}
			if msg.FromSample >= msg.ThroughSample || msg.ThroughSample > active.sent {
				return report, ErrRecoveryProtocol
			}
			if msg.ThroughSample <= checkpoint {
				continue
			} // Already applied, never append twice.
			if msg.FromSample != checkpoint {
				return report, ErrRecoveryProtocol
			}
			report.Segments = append(report.Segments, msg)
			checkpoint = msg.ThroughSample
			cache.drop(checkpoint)
			if cfg.Observe != nil {
				cfg.Observe(msg)
			}
			// Capture may have produced one not-yet-consumed frame; that also counts as
			// backlog, so a temporarily empty coordinator cache is not sufficient.
			if !report.Ended && !captureEnded.Load() && checkpoint == captured.Load() {
				deadline = time.Time{}
			}
		}
	}
}

// capture is paced independently of replay and keeps at most one queued frame.
// AudioSource cancellation is required to join it on every terminal path.
func (c *Client) capture(ctx context.Context, source AudioSource, out chan<- capturedFrame, captured *atomic.Uint64, ended *atomic.Bool) {
	emit := func(f capturedFrame) bool {
		select {
		case out <- f:
			return true
		case <-ctx.Done():
			return false
		}
	}
	pacer := audio.NewPacer()
	var from uint64
	for {
		if c.cfg.Realtime {
			if pacer.WaitBeforeSend(ctx) != nil {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		data := make([]byte, c.cfg.ChunkBytes)
		n, empty := 0, 0
		var err error
		for n < len(data) && err == nil {
			var count int
			count, err = source.Read(ctx, data[n:])
			if count < 0 || count > len(data)-n {
				emit(capturedFrame{err: fmt.Errorf("invalid source read count")})
				return
			}
			n += count
			if count == 0 && err == nil {
				empty++
				if empty >= 100 {
					err = io.ErrNoProgress
				}
			} else {
				empty = 0
			}
			if ctx.Err() != nil {
				return
			}
		}
		if errors.Is(err, io.EOF) {
			ended.Store(true)
		}
		if n%audio.BytesDepth != 0 {
			emit(capturedFrame{err: fmt.Errorf("unaligned PCM input")})
			return
		}
		if n > 0 {
			next := from + uint64(n/audio.BytesDepth)
			if next < from {
				emit(capturedFrame{err: fmt.Errorf("sample offset overflow")})
				return
			}
			captured.Store(next)
			if !emit(capturedFrame{from: from, data: data[:n]}) {
				return
			}
			from = next
			pacer.Advance(n)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				emit(capturedFrame{from: from, end: true})
			} else {
				emit(capturedFrame{err: err})
			}
			return
		}
	}
}
