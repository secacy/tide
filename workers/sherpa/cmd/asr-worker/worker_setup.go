package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/secacy/tide-artisan/internal/realasr"
	"github.com/secacy/tide-artisan/workers/sherpa/internal/native"
)

// configuredWorker 选择明确的静态工厂类型；只检查资源，不加载模型。
// 两条分支共用整场限额；模型 ID 只在真正申请创建时分配。
func configuredWorker(cfg config, logger *slog.Logger) (*realasr.Worker, realasr.Config, error) {
	maxAudio := cfg.maxAudioBytes
	if maxAudio == 0 {
		maxAudio = 960000 // 兼容直接构造内部 config 的既有调用。
	}
	limits := realasr.Config{MaxAudioBytes: maxAudio, MaxChunkBytes: 3200, MaxTextBytes: 4096}
	var modelID atomic.Uint64
	if cfg.segmentAudioBytes == 0 {
		factory, err := native.NewFactory(cfg.model)
		if err != nil {
			return nil, limits, fmt.Errorf("configure native factory: %w", err)
		}
		loggedFactory := func(ctx context.Context) (realasr.Engine, error) {
			id, start := modelID.Add(1), time.Now()
			e, err := factory(ctx)
			logCreation(logger, id, start, err)
			if err != nil {
				return nil, err
			}
			return &loggedEngine{Engine: e, logger: logger, id: id}, nil
		}
		w, err := realasr.New(limits, loggedFactory)
		return w, limits, err
	}
	factory, err := native.NewSegmentedFactory(cfg.model)
	if err != nil {
		return nil, limits, fmt.Errorf("configure native factory: %w", err)
	}
	loggedFactory := func(ctx context.Context) (realasr.SegmentedEngine, error) {
		id, start := modelID.Add(1), time.Now()
		e, err := factory(ctx)
		logCreation(logger, id, start, err)
		if err != nil {
			return nil, err
		}
		return &loggedSegmentedEngine{SegmentedEngine: e, logger: logger, id: id}, nil
	}
	w, err := realasr.NewSegmented(limits, cfg.segmentAudioBytes, loggedFactory)
	return w, limits, err
}

// logCreation 只记录模型边界；不把每次分段流创建计作模型加载。
func logCreation(logger *slog.Logger, id uint64, start time.Time, err error) {
	if err != nil {
		logger.Error("model_creation_failed", "model_id", id, "elapsed_ms", float64(time.Since(start))/float64(time.Millisecond), "error", err)
		return
	}
	logger.Info("model_created", "model_id", id, "load_ms", float64(time.Since(start))/float64(time.Millisecond))
}

// loggedEngine 保持旧接口，实际 Close 返回后才记录释放。
type loggedEngine struct {
	realasr.Engine
	logger *slog.Logger
	id     uint64
}

func (e *loggedEngine) Close() {
	start := time.Now()
	e.Engine.Close()
	e.logger.Info("engine_closed", "model_id", e.id, "close_ms", float64(time.Since(start))/float64(time.Millisecond))
}

// loggedSegmentedEngine 保留完整分段方法集合，不经旧 Engine 包装丢失能力。
type loggedSegmentedEngine struct {
	realasr.SegmentedEngine
	logger *slog.Logger
	id     uint64
}

var _ realasr.SegmentedEngine = (*loggedSegmentedEngine)(nil)

func (e *loggedSegmentedEngine) Close() {
	start := time.Now()
	e.SegmentedEngine.Close()
	e.logger.Info("engine_closed", "model_id", e.id, "close_ms", float64(time.Since(start))/float64(time.Millisecond))
}
