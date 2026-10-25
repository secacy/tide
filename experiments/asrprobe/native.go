package main

import (
	"fmt"
	"path/filepath"

	sherpa "github.com/k2-fsa/sherpa-onnx-go-macos"
)

// nativeEngine owns one recognizer and one stream. Endpoint reset is disabled:
// results are whole-stream snapshots, not independent transcript deltas.
type nativeEngine struct {
	recognizer *sherpa.OnlineRecognizer
	stream     *sherpa.OnlineStream
}

var modelFiles = []string{"encoder-epoch-99-avg-1.int8.onnx", "decoder-epoch-99-avg-1.int8.onnx", "joiner-epoch-99-avg-1.int8.onnx", "tokens.txt"}

func newNative(dir string, threads int) (engine, error) {
	cfg := sherpa.OnlineRecognizerConfig{}
	cfg.FeatConfig = sherpa.FeatureConfig{SampleRate: sampleRate, FeatureDim: 80}
	cfg.ModelConfig.Transducer = sherpa.OnlineTransducerModelConfig{Encoder: filepath.Join(dir, modelFiles[0]), Decoder: filepath.Join(dir, modelFiles[1]), Joiner: filepath.Join(dir, modelFiles[2])}
	cfg.ModelConfig.Tokens = filepath.Join(dir, modelFiles[3])
	cfg.ModelConfig.NumThreads = threads
	cfg.ModelConfig.Provider = "cpu"
	cfg.DecodingMethod = "greedy_search"
	r := sherpa.NewOnlineRecognizer(&cfg)
	if r == nil {
		return nil, fmt.Errorf("native recognizer creation failed")
	}
	s := sherpa.NewOnlineStream(r)
	return &nativeEngine{r, s}, nil
}

func (e *nativeEngine) Accept(s []float32) { e.stream.AcceptWaveform(sampleRate, s) }
func (e *nativeEngine) Ready() bool        { return e.recognizer.IsReady(e.stream) }
func (e *nativeEngine) Decode()            { e.recognizer.Decode(e.stream) }
func (e *nativeEngine) Result() string     { return e.recognizer.GetResult(e.stream).Text }
func (e *nativeEngine) Finish()            { e.stream.InputFinished() }
func (e *nativeEngine) Close() {
	sherpa.DeleteOnlineStream(e.stream)
	sherpa.DeleteOnlineRecognizer(e.recognizer)
}
