package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"time"
)

type report struct {
	Version       int               `json:"version"`
	StartedUTC    time.Time         `json:"started_utc"`
	GoVersion     string            `json:"go_version"`
	Platform      string            `json:"platform"`
	Build         *debug.BuildInfo  `json:"build"`
	Files         map[string]string `json:"file_sha256"`
	Threads       int               `json:"threads"`
	Provider      string            `json:"provider"`
	Decoding      decodingConfig    `json:"decoding"`
	Profile       modelProfile      `json:"model_profile"`
	StreamConfig  streamConfig      `json:"stream_config"`
	Realtime      bool              `json:"realtime"`
	CancelAfterNS int64             `json:"cancel_after_ns"`
	LoadNS        int64             `json:"load_ns"`
	Metrics       metrics           `json:"metrics"`
	Score         *characterScore   `json:"character_score"`
	ScoreNote     string            `json:"score_note"`
}

func digest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func run(args []string) error {
	fs := flag.NewFlagSet("asrprobe", flag.ContinueOnError)
	dir := fs.String("model-dir", "", "pinned model directory matching the selected profile")
	profileID := fs.String("model-profile", legacyProfile, "zipformer-bilingual-2023 or zipformer-zh-2025")
	wav := fs.String("wav", "", "PCM S16LE/mono/16kHz WAV, maximum 16MiB")
	silence := fs.Duration("silence", 0, "generate silence instead of WAV, maximum 30s")
	out := fs.String("output", "", "new evidence directory (must not exist)")
	ref := fs.String("reference", "", "optional UTF-8 human reference file; omit for unknown CER")
	threads := fs.Int("threads", 2, "native CPU threads (1..16)")
	method := fs.String("decoding-method", "greedy_search", "greedy_search or modified_beam_search")
	paths := fs.Int("max-active-paths", 0, "0 for greedy; required beam search budget (1..16)")
	realtime := fs.Bool("realtime", true, "feed at original 100ms schedule")
	cancelAfter := fs.Duration("cancel-after", 0, "cooperative deadline after load; does not interrupt a native call")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *out == "" || *dir == "" || (*wav == "") == (*silence == 0) || *silence < 0 || *silence > 30*time.Second || *threads < 1 || *threads > 16 || *cancelAfter < 0 {
		return errors.New("require model-dir, new output, and exactly one of wav/silence; invalid limits")
	}
	decoding, err := validateDecoding(*method, *paths)
	if err != nil {
		return err // Reject before reading assets, creating output or calling native code.
	}
	profile, err := lookupModelProfile(*profileID)
	if err != nil {
		return err
	}
	cfg := recognizerConfig(*dir, profile, *threads, decoding)
	// Record the same SDK value passed to native construction, using portable
	// filenames. The stream recipe uses the constants also used by runProbe.
	r := report{Version: 3, StartedUTC: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Threads: cfg.ModelConfig.NumThreads, Provider: cfg.ModelConfig.Provider,
		Decoding:     decodingConfig{cfg.DecodingMethod, cfg.MaxActivePaths},
		Profile:      modelProfile{profile.ID, filepath.Base(cfg.ModelConfig.Transducer.Encoder), filepath.Base(cfg.ModelConfig.Transducer.Decoder), filepath.Base(cfg.ModelConfig.Transducer.Joiner), filepath.Base(cfg.ModelConfig.Tokens), cfg.ModelConfig.ModelType, cfg.ModelConfig.ModelingUnit},
		StreamConfig: streamConfig{cfg.FeatConfig.SampleRate, cfg.FeatConfig.FeatureDim, chunkSamples, leadingPaddingSamples, trailingPaddingSamples, cfg.EnableEndpoint},
		Realtime:     *realtime, CancelAfterNS: int64(*cancelAfter), ScoreNote: "No independently verified human reference supplied; CER unknown"}
	r.Build, _ = debug.ReadBuildInfo()
	var samples []float32
	var reference string
	r.Files, err = checkModelFiles(*dir, profile)
	if err != nil {
		return err
	}
	lockHash := sha256.Sum256(assetLock)
	r.Files["model_asset_lock"] = fmt.Sprintf("%x", lockHash)
	if *wav != "" {
		var err error
		samples, err = readPCM(*wav)
		if err != nil {
			return err
		}
		h, err := digest(*wav)
		if err != nil {
			return err
		}
		r.Files["input.wav"] = h
	} else {
		samples = make([]float32, int(int64(*silence)*sampleRate/int64(time.Second)))
		if len(samples) == 0 {
			return errors.New("silence shorter than one sample")
		}
	}
	if *ref != "" {
		b, err := os.ReadFile(*ref)
		if err != nil {
			return err
		}
		reference = string(b)
		r.ScoreNote = "Reference supplied; score omitted unless the full probe completes"
		if _, err = score(reference, ""); err != nil {
			return err
		}
		h, err := digest(*ref)
		if err != nil {
			return err
		}
		r.Files["reference.txt"] = h
	}
	if exe, err := os.Executable(); err == nil {
		h, err := digest(exe)
		if err != nil {
			return err
		}
		r.Files["probe_binary"] = h
	}
	if err := os.Mkdir(*out, 0755); err != nil {
		return err
	}
	events, err := os.OpenFile(filepath.Join(*out, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(events)
	loadStart := time.Now()
	e, runErr := newNative(cfg)
	r.LoadNS = time.Since(loadStart).Nanoseconds()
	if runErr == nil {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		if *cancelAfter > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, *cancelAfter)
			defer cancel()
		}
		r.Metrics, runErr = runProbe(ctx, e, samples, *realtime, func(ev event) error { return encoder.Encode(ev) })
		stop()
	} else {
		r.Metrics.Status = "load_failed"
		r.Metrics.Error = runErr.Error()
	}
	if reference != "" && r.Metrics.Status == "completed_probe" {
		r.Score, err = score(reference, r.Metrics.FinalText)
		runErr = errors.Join(runErr, err)
		r.ScoreNote = "Reference supplied by caller; not verified by the tool"
	}
	closeErr := events.Close()
	f, err := os.OpenFile(filepath.Join(*out, "summary.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Join(runErr, closeErr, err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	writeErr := enc.Encode(r)
	summaryCloseErr := f.Close()
	return errors.Join(runErr, closeErr, writeErr, summaryCloseErr)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
