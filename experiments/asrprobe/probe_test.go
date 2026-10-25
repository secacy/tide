package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type fakeEngine struct {
	sizes           []int
	ready           bool
	finished        bool
	closed          int
	inside          bool
	closeDuringCall bool
	text            string
	onDecode        func()
}

func (e *fakeEngine) Accept(s []float32) { e.sizes = append(e.sizes, len(s)); e.ready = true }
func (e *fakeEngine) Ready() bool        { return e.ready }
func (e *fakeEngine) Decode() {
	e.inside = true
	if e.onDecode != nil {
		e.onDecode()
	}
	e.ready = false
	e.text = "初稿"
	if e.finished {
		e.text = "尾部修订"
	}
	e.inside = false
}
func (e *fakeEngine) Result() string { return e.text }
func (e *fakeEngine) Finish()        { e.finished = true; e.ready = true }
func (e *fakeEngine) Close()         { e.closed++; e.closeDuringCall = e.inside }

func TestShortTailAndPaddingAreSeparate(t *testing.T) {
	e := &fakeEngine{}
	var events []event
	m, err := runProbe(context.Background(), e, make([]float32, 1601), false, func(ev event) error { events = append(events, ev); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(e.sizes, []int{4800, 1600, 1, 9600}) {
		t.Fatal(e.sizes)
	}
	if m.SubmittedBytes != 3202 || m.InputBytes != 3202 || m.ProcessedBytes != nil || !m.InputFinished || !m.Closed || m.Status != "completed_probe" || e.closed != 1 || m.FinalText != "尾部修订" || !m.FirstTextBeforeInputEnd || m.TailNS == nil {
		t.Fatalf("%+v", m)
	}
	var offsets []int
	for _, ev := range events {
		if ev.Kind == "input" {
			offsets = append(offsets, ev.OffsetBytes)
			if ev.PlannedNS != nil {
				t.Fatal("unpaced input had a schedule")
			}
		}
		if ev.Kind == "trailing_padding" && ev.OffsetBytes != 3202 {
			t.Fatal(ev)
		}
	}
	if !reflect.DeepEqual(offsets, []int{3200, 3202}) {
		t.Fatal(offsets)
	}
}

func TestCancellationDuringCallWaitsForReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &fakeEngine{onDecode: cancel}
	m, err := runProbe(ctx, e, make([]float32, 3200), false, func(event) error { return nil })
	if !errors.Is(err, context.Canceled) || m.Status != "canceled" || m.InputFinished || e.closeDuringCall || e.closed != 1 || m.SubmittedBytes != 3200 || len(e.sizes) != 2 || m.CallRTF != nil {
		t.Fatalf("%+v %v %+v", m, err, e)
	}
}

func TestCanceledBeforeInputClosesWithoutCallingNative(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &fakeEngine{}
	m, err := runProbe(ctx, e, make([]float32, 1), true, func(event) error { return nil })
	if !errors.Is(err, context.Canceled) || len(e.sizes) != 0 || e.closed != 1 || !m.Closed || m.LeadingPaddingSamples != 0 {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestRecordingFailureStopsAndReleases(t *testing.T) {
	want := errors.New("output full")
	e := &fakeEngine{}
	m, err := runProbe(context.Background(), e, make([]float32, 1), false, func(event) error { return want })
	if !errors.Is(err, want) || e.closed != 1 || m.InputFinished || len(e.sizes) != 1 || m.Status != "failed" {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestPacingUsesOriginalSampleOffset(t *testing.T) {
	e := &fakeEngine{}
	var plans []int64
	m, err := runProbe(context.Background(), e, make([]float32, 1601), true, func(ev event) error {
		if ev.Kind == "input" {
			plans = append(plans, *ev.PlannedNS)
			if len(plans) == 1 {
				time.Sleep(110 * time.Millisecond)
			}
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(plans, []int64{0, 100000000}) || m.MaxScheduleLagNS < 10e6 {
		t.Fatalf("%v %+v %v", plans, m, err)
	}
}

func TestCharacterScorePreservesClinicalTokens(t *testing.T) {
	s, err := score("不发热，120mg。", "发热 12mg")
	if err != nil || s.Reference != "不发热120mg" || s.Hypothesis != "发热12mg" || s.Edits != 2 || s.ReferenceRunes != 8 || s.CER != 0.25 {
		t.Fatalf("%+v %v", s, err)
	}
	if _, err := score("。 ", "x"); err == nil {
		t.Fatal("empty reference accepted")
	}
	s, err = score("Always", "ALWAYS!")
	if err != nil || s.Edits != 0 {
		t.Fatal(s, err)
	}
}

func fixtureWAV() []byte {
	var b bytes.Buffer
	b.WriteString("RIFF")
	binary.Write(&b, binary.LittleEndian, uint32(40))
	b.WriteString("WAVEfmt ")
	binary.Write(&b, binary.LittleEndian, uint32(16))
	for _, v := range []uint16{1, 1} {
		binary.Write(&b, binary.LittleEndian, v)
	}
	for _, v := range []uint32{16000, 32000} {
		binary.Write(&b, binary.LittleEndian, v)
	}
	for _, v := range []uint16{2, 16} {
		binary.Write(&b, binary.LittleEndian, v)
	}
	b.WriteString("data")
	binary.Write(&b, binary.LittleEndian, uint32(4))
	b.Write([]byte{0, 128, 255, 127})
	return b.Bytes()
}

func TestWAVFormatAndSampleConversion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.wav")
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
		valid  bool
	}{
		{"valid", func(b []byte) []byte { return b }, true},
		{"bad_rate", func(b []byte) []byte { b[24] = 0; return b }, false},
		{"stereo", func(b []byte) []byte { b[22] = 2; return b }, false},
		{"float", func(b []byte) []byte { b[20] = 3; return b }, false},
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }, false},
		{"oversized_chunk", func(b []byte) []byte { b[40] = 255; return b }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.mutate(fixtureWAV()), 0600); err != nil {
				t.Fatal(err)
			}
			s, err := readPCM(path)
			if tc.valid {
				if err != nil || !reflect.DeepEqual(s, []float32{-1, 32767.0 / 32768}) {
					t.Fatal(s, err)
				}
			} else if err == nil {
				t.Fatal("invalid WAV accepted")
			}
		})
	}
}
