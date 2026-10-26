package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode"
)

const sampleRate = 16000

// engine is a serial test seam, not a production model adapter. Close runs only
// after all synchronous native calls have returned; context cannot interrupt C.
type engine interface {
	Accept([]float32)
	Ready() bool
	Decode()
	Result() string
	Finish()
	Close()
}

// event times are monotonic offsets from the post-load stream start. Begin/End
// delimit a completed call; they are not audio processing confirmations.
type event struct {
	Kind        string `json:"kind"`
	BeginNS     int64  `json:"begin_ns"`
	EndNS       int64  `json:"end_ns"`
	OffsetBytes int    `json:"input_end_bytes"`
	Samples     int    `json:"samples,omitempty"`
	PlannedNS   *int64 `json:"planned_ns,omitempty"`
	Text        string `json:"text,omitempty"`
}

type metrics struct {
	Status                  string   `json:"status"`
	Error                   string   `json:"error,omitempty"`
	InputBytes              int      `json:"input_bytes"`
	SubmittedBytes          int      `json:"submitted_bytes"`
	LeadingPaddingSamples   int      `json:"leading_padding_samples"`
	TrailingPaddingSamples  int      `json:"trailing_padding_samples"`
	DecodeCalls             int      `json:"decode_calls"`
	TextChanges             int      `json:"text_changes"`
	FinalText               string   `json:"last_text_snapshot"`
	InputFinished           bool     `json:"input_finished"`
	Closed                  bool     `json:"close_returned"`
	FirstTextNS             *int64   `json:"first_nonempty_text_ns"`
	FirstTextBeforeInputEnd bool     `json:"first_text_before_input_end"`
	MaxScheduleLagNS        int64    `json:"max_schedule_lag_ns"`
	EngineCallNS            int64    `json:"engine_call_ns"`
	CallRTF                 *float64 `json:"call_rtf"`
	TailNS                  *int64   `json:"last_input_call_end_to_flush_end_ns"`
	ElapsedNS               int64    `json:"elapsed_ns"`
	ProcessedBytes          *uint64  `json:"processed_audio_bytes"` // nil: unavailable, never inferred from submission/text.
}

// runProbe takes ownership of e. Pacing is anchored to the original schedule;
// inference delays do not shift future plans. Padding never advances offsets.
func runProbe(ctx context.Context, e engine, samples []float32, realtime bool, emit func(event) error) (m metrics, err error) {
	m.InputBytes = len(samples) * 2
	m.Status = "failed"
	start := time.Now()
	var lastInputEnd int64
	defer func() {
		e.Close()
		m.Closed = true
		m.ElapsedNS = time.Since(start).Nanoseconds()
		if len(samples) > 0 && m.Status == "completed_probe" {
			rtf := float64(m.EngineCallNS) / (float64(len(samples)) * 1e9 / sampleRate)
			m.CallRTF = &rtf
		}
		if err != nil {
			m.Error = err.Error()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				m.Status = "canceled"
			}
		}
	}()
	call := func(kind string, n int, plan *int64, f func()) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		begin := time.Since(start).Nanoseconds()
		if plan != nil && begin-*plan > m.MaxScheduleLagNS {
			m.MaxScheduleLagNS = begin - *plan
		}
		f() // Cancellation observed AFTER return; do not free resources from another goroutine.
		end := time.Since(start).Nanoseconds()
		if kind == "input" {
			lastInputEnd = end
		}
		m.EngineCallNS += end - begin
		if err := emit(event{Kind: kind, BeginNS: begin, EndNS: end, OffsetBytes: m.SubmittedBytes, Samples: n, PlannedNS: plan}); err != nil {
			return err
		}
		return ctx.Err()
	}
	drain := func() error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !e.Ready() {
				return nil
			}
			if err := call("decode", 0, nil, func() { e.Decode(); m.DecodeCalls++ }); err != nil {
				return err
			}
			var text string
			if err := call("get_result", 0, nil, func() { text = e.Result() }); err != nil {
				return err
			}
			if text != m.FinalText {
				m.FinalText = text
				m.TextChanges++
				now := time.Since(start).Nanoseconds()
				if m.FirstTextNS == nil && strings.TrimSpace(text) != "" {
					m.FirstTextNS = &now
					m.FirstTextBeforeInputEnd = m.SubmittedBytes < m.InputBytes
				}
				if err := emit(event{Kind: "text_snapshot", BeginNS: now, EndNS: now, OffsetBytes: m.SubmittedBytes, Text: text}); err != nil {
					return err
				}
			}
		}
	}
	if len(samples) == 0 {
		return m, errors.New("empty input")
	}
	if err = call("leading_padding", leadingPaddingSamples, nil, func() {
		e.Accept(make([]float32, leadingPaddingSamples))
		m.LeadingPaddingSamples = leadingPaddingSamples
	}); err != nil {
		return m, err
	}
	for pos := 0; pos < len(samples); {
		plan := int64(pos) * int64(time.Second) / sampleRate
		if realtime {
			if err = waitUntil(ctx, start.Add(time.Duration(plan))); err != nil {
				return m, err
			}
		}
		end := min(pos+chunkSamples, len(samples))
		var planned *int64
		if realtime {
			planned = &plan
		}
		err = call("input", end-pos, planned, func() { e.Accept(samples[pos:end]); m.SubmittedBytes = end * 2 })
		if err != nil {
			return m, err
		}
		if err = drain(); err != nil {
			return m, err
		}
		pos = end
	}
	if err = call("trailing_padding", trailingPaddingSamples, nil, func() {
		e.Accept(make([]float32, trailingPaddingSamples))
		m.TrailingPaddingSamples = trailingPaddingSamples
	}); err != nil {
		return m, err
	}
	if err = call("input_finished", 0, nil, func() { e.Finish(); m.InputFinished = true }); err != nil {
		return m, err
	}
	if err = drain(); err != nil {
		return m, err
	}
	tail := time.Since(start).Nanoseconds() - lastInputEnd
	m.TailNS = &tail
	m.Status = "completed_probe"
	return m, nil
}

func waitUntil(ctx context.Context, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d := time.Until(at)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ctx.Err()
	}
}

// readPCM restricts this experiment to a small RIFF PCM S16LE/16kHz/mono file.
// Container chunks and odd-size padding are skipped; no implicit resampling.
func readPCM(path string) ([]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 16*1024*1024+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 16*1024*1024 {
		return nil, errors.New("probe WAV exceeds 16MiB")
	}
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WAVE" || uint64(binary.LittleEndian.Uint32(data[4:8]))+8 != uint64(len(data)) {
		return nil, errors.New("invalid RIFF WAV length/header")
	}
	var payload []byte
	fmtSeen := false
	for pos := 12; pos < len(data); {
		if pos+8 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		kind := string(data[pos : pos+4])
		pos += 8
		if n > len(data)-pos {
			return nil, io.ErrUnexpectedEOF
		}
		chunk := data[pos : pos+n]
		switch kind {
		case "fmt ":
			if fmtSeen || n < 16 || binary.LittleEndian.Uint16(chunk) != 1 || binary.LittleEndian.Uint16(chunk[2:]) != 1 || binary.LittleEndian.Uint32(chunk[4:]) != sampleRate || binary.LittleEndian.Uint32(chunk[8:]) != 32000 || binary.LittleEndian.Uint16(chunk[12:]) != 2 || binary.LittleEndian.Uint16(chunk[14:]) != 16 {
				return nil, errors.New("require PCM S16LE, mono, 16000Hz")
			}
			fmtSeen = true
		case "data":
			if payload != nil || n == 0 || n%2 != 0 {
				return nil, errors.New("invalid/duplicate PCM data")
			}
			payload = chunk
		}
		pos += n + n%2
		if pos > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
	}
	if !fmtSeen || payload == nil {
		return nil, errors.New("missing fmt/data")
	}
	samples := make([]float32, len(payload)/2)
	for i := range samples {
		samples[i] = float32(int16(binary.LittleEndian.Uint16(payload[i*2:]))) / 32768
	}
	return samples, nil
}

type characterScore struct {
	Policy         string  `json:"policy"`
	Reference      string  `json:"normalized_reference"`
	Hypothesis     string  `json:"normalized_hypothesis"`
	Edits          int     `json:"edits"`
	ReferenceRunes int     `json:"reference_runes"`
	CER            float64 `json:"cer"`
}

// score uses Unicode lowercase and removes whitespace/punctuation only. Digits,
// scripts, and filler words are preserved; no simplified/traditional mapping.
func score(ref, hyp string) (*characterScore, error) {
	normalize := func(s string) string {
		return strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) || unicode.IsPunct(r) {
				return -1
			}
			return unicode.ToLower(r)
		}, s)
	}
	r, h := []rune(normalize(ref)), []rune(normalize(hyp))
	if len(r) == 0 {
		return nil, fmt.Errorf("reference empty after normalization")
	}
	row := make([]int, len(h)+1)
	for j := range row {
		row[j] = j
	}
	for i, x := range r {
		prev := row[0]
		row[0] = i + 1
		for j, y := range h {
			old := row[j+1]
			cost := 0
			if x != y {
				cost = 1
			}
			row[j+1] = min(row[j]+1, old+1, prev+cost)
			prev = old
		}
	}
	return &characterScore{"lowercase; remove Unicode whitespace/punctuation; retain all other runes", string(r), string(h), row[len(h)], len(r), float64(row[len(h)]) / float64(len(r))}, nil
}
