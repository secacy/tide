package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSearchSettingsReachNativeConfigWithoutChangingModel(t *testing.T) {
	greedy, err := validateDecoding("greedy_search", 0)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := lookupModelProfile(legacyProfile)
	if err != nil {
		t.Fatal(err)
	}
	base := recognizerConfig("fixed-model", profile, 2, greedy)
	for _, paths := range []int{1, 4, 16} {
		beam, err := validateDecoding("modified_beam_search", paths)
		if err != nil {
			t.Fatal(err)
		}
		cfg := recognizerConfig("fixed-model", profile, 2, beam)
		if cfg.DecodingMethod != "modified_beam_search" || cfg.MaxActivePaths != paths || cfg.EnableEndpoint != 0 || cfg.FeatConfig.SampleRate != 16000 || cfg.FeatConfig.FeatureDim != 80 {
			t.Fatalf("search/input settings not forwarded: %+v", cfg)
		}
		cfg.DecodingMethod, cfg.MaxActivePaths = base.DecodingMethod, base.MaxActivePaths
		if !reflect.DeepEqual(cfg, base) {
			t.Fatal("search selection changed unrelated native settings")
		}
	}
	if base.DecodingMethod != "greedy_search" || base.MaxActivePaths != 0 || base.ModelConfig.NumThreads != 2 || base.ModelConfig.Provider != "cpu" {
		t.Fatal("greedy defaults changed")
	}
}

func TestInvalidSearchIsRejectedBeforeReadingAssetsOrCreatingOutput(t *testing.T) {
	for _, flags := range [][]string{
		{"-decoding-method=unknown"},
		{"-decoding-method=modified_beam_search"},
		{"-decoding-method=modified_beam_search", "-max-active-paths=-1"},
		{"-decoding-method=modified_beam_search", "-max-active-paths=17"},
		{"-max-active-paths=4"},
	} {
		t.Run(strings.Join(flags, ","), func(t *testing.T) {
			dir := t.TempDir()
			output := filepath.Join(dir, "must-not-exist")
			args := append([]string{"-model-dir=" + filepath.Join(dir, "absent-model"), "-silence=1s", "-output=" + output}, flags...)
			err := run(args)
			if err == nil || (!strings.Contains(err.Error(), "decoding") && !strings.Contains(err.Error(), "max-active-paths")) {
				t.Fatalf("expected search validation before asset access, got %v", err)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatal("invalid search configuration created output")
			}
		})
	}
}
