package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

type segmentObserveFake struct {
	setupTestEngine
	acceptError error
}

func (e *segmentObserveFake) Accept([]float32) error { return e.acceptError }

func TestSegmentObservationTracksReturnedLifecyclesAndSamples(t *testing.T) {
	var log bytes.Buffer
	underlying := &segmentObserveFake{}
	e := &loggedSegmentedEngine{SegmentedEngine: underlying, logger: slog.New(slog.NewJSONHandler(&log, nil)), id: 9, observe: true}
	e.openSegment(true, 123)
	// 独立常量编码：float32 的 0、0.5、-1，以 little-endian 写入。
	if err := e.Accept([]float32{0, 0.5, -1}); err != nil {
		t.Fatal(err)
	}
	underlying.acceptError = errors.New("accept failed")
	if err := e.Accept([]float32{1}); err == nil {
		t.Fatal("lost accept failure")
	}
	e.ReleaseSegment()
	e.ReleaseSegment()
	underlying.failure = errors.New("start failed")
	if err := e.StartSegment(); err == nil {
		t.Fatal("lost start failure")
	}
	underlying.failure = nil
	if err := e.StartSegment(); err != nil {
		t.Fatal(err)
	}
	e.Close()
	var rows []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(log.Bytes()), []byte{'\n'}) {
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if len(rows) != 6 {
		t.Fatalf("events=%s", log.String())
	}
	want := []string{"stream_created", "stream_closed", "stream_creation_failed", "stream_created", "stream_closed", "engine_closed"}
	for i, row := range rows {
		if row["msg"] != want[i] {
			t.Fatal(rows)
		}
	}
	digest := sha256.Sum256([]byte{0, 0, 0, 0, 0, 0, 0, 0x3f, 0, 0, 0x80, 0xbf})
	if rows[1]["samples"] != float64(3) || rows[1]["sample_f32le_sha256"] != hex.EncodeToString(digest[:]) || rows[1]["reason"] != "segment_release" || rows[4]["reason"] != "engine_close" || rows[3]["segment_id"] != float64(2) {
		t.Fatalf("bad observed state: %v", rows)
	}
}

func TestSegmentObservationIsExplicit(t *testing.T) {
	if _, err := parseConfig([]string{"-model-dir=x", "-observe-segments"}, &bytes.Buffer{}); err == nil {
		t.Fatal("observation accepted without segmented mode")
	}
	if cfg, err := parseConfig([]string{"-model-dir=x", "-segment-audio-bytes=3200", "-observe-segments"}, &bytes.Buffer{}); err != nil || !cfg.observeSegments {
		t.Fatal(cfg, err)
	}
}
