package main

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/secacy/tide-artisan/internal/realasr"
)

func TestParseSegmentedConfig(t *testing.T) {
	for _, args := range [][]string{
		{"-max-audio-bytes=0"}, {"-max-audio-bytes=-2"}, {"-max-audio-bytes=3"},
		{"-segment-audio-bytes=-2"}, {"-segment-audio-bytes=3"},
		{"-max-audio-bytes=8", "-segment-audio-bytes=10"},
	} {
		if _, err := parseConfig(append([]string{"-model-dir=x"}, args...), io.Discard); err == nil {
			t.Fatalf("accepted invalid args %v", args)
		}
	}
	for _, segmented := range []bool{false, true} {
		args := []string{"-model-dir=x"}
		if segmented {
			args = append(args, "-max-audio-bytes=3840000", "-segment-audio-bytes=960000")
		}
		cfg, err := parseConfig(args, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if segmented {
			if cfg.maxAudioBytes != 3840000 || cfg.segmentAudioBytes != 960000 {
				t.Fatal(cfg)
			}
		} else if cfg.maxAudioBytes != 960000 || cfg.segmentAudioBytes != 0 {
			t.Fatal(cfg)
		}
	}
}

// setupTestEngine 验证包装是否透传全部方法，以及 Close 返回前不记录释放。
type setupTestEngine struct {
	realasr.Engine
	starts, releases, closed int
	failure                  error
	beforeClose              func()
}

func (e *setupTestEngine) StartSegment() error { e.starts++; return e.failure }
func (e *setupTestEngine) ReleaseSegment()     { e.releases++ }
func (e *setupTestEngine) Close() {
	if e.beforeClose != nil {
		e.beforeClose()
	}
	e.closed++
}

func TestLoggedSegmentedEnginePreservesLifecycle(t *testing.T) {
	var log bytes.Buffer
	failure := errors.New("controlled start failure")
	e := &setupTestEngine{failure: failure}
	e.beforeClose = func() {
		if strings.Contains(log.String(), "engine_closed") {
			t.Fatal("release logged before actual Close")
		}
	}
	w := &loggedSegmentedEngine{SegmentedEngine: e, logger: slog.New(slog.NewJSONHandler(&log, nil)), id: 7}
	if err := w.StartSegment(); err != failure || e.starts != 1 {
		t.Fatal("start method or error not forwarded")
	}
	w.ReleaseSegment()
	w.Close()
	if e.releases != 1 || e.closed != 1 || strings.Count(log.String(), "engine_closed") != 1 || !strings.Contains(log.String(), `"model_id":7`) {
		t.Fatalf("invalid lifecycle log: %s", log.String())
	}
}

func TestConfiguredWorkerDoesNotLoadAssets(t *testing.T) {
	// 文件存在但无有效模型内容；构造不能把它们交给 C。
	for _, segmented := range []bool{false, true} {
		cfg, err := parseConfig([]string{"-model-dir=" + writeSetupAssets(t)}, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if segmented {
			cfg.segmentAudioBytes = 3200
		}
		var output bytes.Buffer
		w, limits, err := configuredWorker(cfg, slog.New(slog.NewJSONHandler(&output, nil)))
		if err != nil || w == nil || limits.MaxAudioBytes != 960000 || output.Len() != 0 {
			t.Fatalf("configuration loaded model: %v %s", err, output.String())
		}
	}
}

func writeSetupAssets(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"encoder-epoch-99-avg-1.int8.onnx", "decoder-epoch-99-avg-1.int8.onnx", "joiner-epoch-99-avg-1.int8.onnx", "tokens.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("configuration-check-only"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
