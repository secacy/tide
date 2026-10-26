package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestModelProfilesMapDistinctAssetsToNativeConfig(t *testing.T) {
	for _, tc := range []struct {
		id, encoder, decoder, joiner, unit string
	}{
		{legacyProfile, "encoder-epoch-99-avg-1.int8.onnx", "decoder-epoch-99-avg-1.int8.onnx", "joiner-epoch-99-avg-1.int8.onnx", ""},
		{"zipformer-zh-2025", "encoder.int8.onnx", "decoder.onnx", "joiner.int8.onnx", "cjkchar"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			p, err := lookupModelProfile(tc.id)
			if err != nil {
				t.Fatal(err)
			}
			cfg := recognizerConfig("models", p, 2, decodingConfig{"greedy_search", 0})
			m := cfg.ModelConfig
			if m.Transducer.Encoder != filepath.Join("models", tc.encoder) || m.Transducer.Decoder != filepath.Join("models", tc.decoder) || m.Transducer.Joiner != filepath.Join("models", tc.joiner) || m.Tokens != filepath.Join("models", "tokens.txt") || m.ModelingUnit != tc.unit || m.ModelType != "" {
				t.Fatalf("wrong native model mapping: %+v", m)
			}
			if m.NumThreads != 2 || m.Provider != "cpu" || cfg.EnableEndpoint != 0 || cfg.FeatConfig.SampleRate != 16000 || cfg.FeatConfig.FeatureDim != 80 || cfg.DecodingMethod != "greedy_search" {
				t.Fatal("profile changed input, execution or search settings")
			}
		})
	}
}

func TestUnknownModelProfileFailsBeforeAssetAccess(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "output")
	err := run([]string{"-model-profile=unknown", "-model-dir=" + filepath.Join(dir, "missing"), "-silence=1s", "-output=" + out})
	if err == nil || !strings.Contains(err.Error(), "unsupported model-profile") {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("invalid profile created output")
	}
}

func TestWrongModelContentFailsBeforeOutputAndNativeLoad(t *testing.T) {
	for _, id := range []string{legacyProfile, "zipformer-zh-2025"} {
		t.Run(id, func(t *testing.T) {
			dir := t.TempDir()
			profile, err := lookupModelProfile(id)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range profile.files() {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("different model bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			out := filepath.Join(dir, "output")
			err = run([]string{"-model-profile=" + id, "-model-dir=" + dir, "-silence=1s", "-output=" + out})
			if err == nil || !strings.Contains(err.Error(), "model SHA256 mismatch") {
				t.Fatalf("got %v", err)
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("wrong model created output")
			}
		})
	}
}
