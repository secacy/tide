// Package native 适配当前固定的 macOS/cgo Zipformer 后端。
package native

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	sherpa "github.com/k2-fsa/sherpa-onnx-go-macos"
	"github.com/secacy/tide-artisan/internal/realasr"
)

// Config 描述当前固定 int8 Zipformer 后端。
type Config struct {
	ModelDir string // 模型/词表目录，必填；Factory 保存其绝对路径。
	Threads  int    // CPU 线程数；0 默认 2，合法范围 1～16。
}

const (
	encoderFile = "encoder-epoch-99-avg-1.int8.onnx"
	decoderFile = "decoder-epoch-99-avg-1.int8.onnx"
	joinerFile  = "joiner-epoch-99-avg-1.int8.onnx"
	tokensFile  = "tokens.txt"
)

// NewFactory 检查并复制配置，不加载模型。
// 每次成功调用返回的工厂会创建独立 Engine，所有权交给调用方。
func NewFactory(cfg Config) (realasr.EngineFactory, error) {
	cfg, err := validateConfig(cfg)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) (realasr.Engine, error) {
		e, err := newEngine(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return e, nil
	}, nil
}

// NewSegmentedFactory 使用同样的模型配置，显式返回分段能力。
// 每场创建独立 recognizer 和首段流；配置检查不提前加载模型。
func NewSegmentedFactory(cfg Config) (realasr.SegmentedEngineFactory, error) {
	cfg, err := validateConfig(cfg)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) (realasr.SegmentedEngine, error) {
		e, err := newEngine(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return e, nil
	}, nil
}

// validateConfig 检查并复制配置；两个静态类型的工厂共用此流程。
func validateConfig(cfg Config) (Config, error) {
	if strings.TrimSpace(cfg.ModelDir) == "" {
		return Config{}, errors.New("model directory is required")
	}
	if cfg.Threads == 0 {
		cfg.Threads = 2
	}
	if cfg.Threads < 1 || cfg.Threads > 16 {
		return Config{}, errors.New("threads must be between 1 and 16")
	}
	dir, err := filepath.Abs(cfg.ModelDir)
	if err != nil {
		return Config{}, fmt.Errorf("resolve model directory: %w", err)
	}
	cfg.ModelDir = dir
	for _, name := range []string{encoderFile, decoderFile, joinerFile, tokensFile} {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			return Config{}, fmt.Errorf("stat model asset %s: %w", name, err)
		}
		// 在 Open 前拒绝 FIFO/设备等非普通文件，避免配置检查阻塞读取。
		if !info.Mode().IsRegular() || info.Size() == 0 {
			return Config{}, fmt.Errorf("model asset %s must be a nonempty regular file", name)
		}
		f, err := os.Open(path)
		if err != nil {
			return Config{}, fmt.Errorf("open model asset %s: %w", name, err)
		}
		openedInfo, statErr := f.Stat()
		closeErr := f.Close()
		if statErr != nil {
			return Config{}, fmt.Errorf("stat model asset %s: %w", name, statErr)
		}
		if !openedInfo.Mode().IsRegular() || openedInfo.Size() == 0 {
			return Config{}, fmt.Errorf("model asset %s must be a nonempty regular file", name)
		}
		if closeErr != nil {
			return Config{}, fmt.Errorf("close model asset %s: %w", name, closeErr)
		}
	}
	return cfg, nil
}

// newEngine 串行创建原生对象；失败时清理部分资源后才返回。
// ctx 不拥有 cancel，不能中断已经开始的原生创建调用。
func newEngine(ctx context.Context, cfg Config) (_ *engine, err error) {
	if ctx == nil {
		return nil, errors.New("engine context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e := &engine{ctx: ctx}
	defer func() {
		if err != nil {
			e.Close()
		}
	}()
	settings := sherpa.OnlineRecognizerConfig{}
	settings.FeatConfig = sherpa.FeatureConfig{SampleRate: sampleRate, FeatureDim: 80}
	settings.ModelConfig.Transducer = sherpa.OnlineTransducerModelConfig{
		Encoder: filepath.Join(cfg.ModelDir, encoderFile),
		Decoder: filepath.Join(cfg.ModelDir, decoderFile),
		Joiner:  filepath.Join(cfg.ModelDir, joinerFile),
	}
	settings.ModelConfig.Tokens = filepath.Join(cfg.ModelDir, tokensFile)
	settings.ModelConfig.NumThreads = cfg.Threads
	settings.ModelConfig.Provider = "cpu"
	settings.DecodingMethod = "greedy_search"
	settings.EnableEndpoint = 0
	e.recognizer = sherpa.NewOnlineRecognizer(&settings)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.recognizer == nil {
		return nil, errors.New("native recognizer creation failed")
	}
	if err := e.StartSegment(); err != nil {
		return nil, err
	}
	return e, nil
}
