package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"path/filepath"
)

// modelProfile describes how to load one supported model. It owns no native
// resources; filenames are relative to model-dir and the asset lock fixes identity.
type modelProfile struct {
	ID           string `json:"id"`            // Stable name, never inferred from a directory.
	Encoder      string `json:"encoder"`       // Encoder file; quantization is independent per component.
	Decoder      string `json:"decoder"`       // Decoder file, including fp32 where required.
	Joiner       string `json:"joiner"`        // Transducer joiner file.
	Tokens       string `json:"tokens"`        // Vocabulary belonging to this model.
	ModelType    string `json:"model_type"`    // Empty lets the SDK inspect metadata.
	ModelingUnit string `json:"modeling_unit"` // SDK vocabulary unit; legacy configuration leaves it empty.
}

const legacyProfile = "zipformer-bilingual-2023"

//go:embed model-assets.json
var assetLock []byte

// lookupModelProfile rejects unknown names before asset I/O or native calls.
func lookupModelProfile(id string) (modelProfile, error) {
	switch id {
	case legacyProfile:
		return modelProfile{id, "encoder-epoch-99-avg-1.int8.onnx", "decoder-epoch-99-avg-1.int8.onnx", "joiner-epoch-99-avg-1.int8.onnx", "tokens.txt", "", ""}, nil
	case "zipformer-zh-2025":
		return modelProfile{id, "encoder.int8.onnx", "decoder.onnx", "joiner.int8.onnx", "tokens.txt", "", "cjkchar"}, nil
	default:
		return modelProfile{}, fmt.Errorf("unsupported model-profile: %q", id)
	}
}

func (p modelProfile) files() []string {
	return []string{p.Encoder, p.Decoder, p.Joiner, p.Tokens}
}

// checkModelFiles compares observed hashes to the independently pinned embedded
// lock before native construction. Returned hashes describe the files read.
func checkModelFiles(dir string, p modelProfile) (map[string]string, error) {
	var lock struct {
		Profiles map[string]struct {
			Files map[string]string `json:"files"`
		} `json:"profiles"`
	}
	if err := json.Unmarshal(assetLock, &lock); err != nil {
		return nil, fmt.Errorf("invalid embedded asset lock: %w", err)
	}
	expected, ok := lock.Profiles[p.ID]
	if !ok || len(expected.Files) != 4 {
		return nil, fmt.Errorf("incomplete asset lock for %s", p.ID)
	}
	observed := make(map[string]string, 4)
	for _, name := range p.files() {
		got, err := digest(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		if got != expected.Files[name] {
			return nil, fmt.Errorf("model SHA256 mismatch: %s (%s)", p.ID, name)
		}
		observed[name] = got
	}
	return observed, nil
}

// streamConfig records settings actually shared by the SDK and runProbe.
// Sizes are samples; none is a model processing confirmation.
type streamConfig struct {
	SampleRate             int `json:"sample_rate"`
	FeatureDim             int `json:"feature_dim"`
	ChunkSamples           int `json:"chunk_samples"`
	LeadingPaddingSamples  int `json:"leading_padding_samples"`
	TrailingPaddingSamples int `json:"trailing_padding_samples"`
	EnableEndpoint         int `json:"enable_endpoint"`
}

const (
	featureDim             = 80
	chunkSamples           = 1600
	leadingPaddingSamples  = 4800
	trailingPaddingSamples = 9600
)
