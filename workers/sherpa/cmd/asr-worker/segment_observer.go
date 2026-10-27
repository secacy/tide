package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"time"
)

// openSegment 在真实工厂或 StartSegment 成功返回后记录；首段包含模型加载。
// 与 Engine 一样仅由 RPC 串行拥有，不启动后台任务或改变识别协议。
func (e *loggedSegmentedEngine) openSegment(initial bool, elapsed time.Duration) {
	e.segment++
	e.streamOpen = true
	e.samples = 0
	if !e.observe {
		return
	}
	e.sampleHash = sha256.New()
	e.logger.Info("stream_created", "model_id", e.id, "segment_id", e.segment, "initial", initial, "create_ns", elapsed.Nanoseconds())
}

// Accept 观察成功提交给真实 Engine 的原始 float32 样本，不代表解码完成。
// 摘要仅在显式开启时计算；模型内部 padding 不经过此接口。
func (e *loggedSegmentedEngine) Accept(samples []float32) error {
	if err := e.SegmentedEngine.Accept(samples); err != nil {
		return err
	}
	if e.observe {
		data := make([]byte, 4*len(samples))
		for i, sample := range samples {
			binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(sample))
		}
		_, _ = e.sampleHash.Write(data)
		e.samples += uint64(len(samples))
	}
	return nil
}

// StartSegment 不重试；成功才记录下一流，失败仍由整场 Close 清理。
func (e *loggedSegmentedEngine) StartSegment() error {
	start := time.Now()
	err := e.SegmentedEngine.StartSegment()
	if err == nil {
		e.openSegment(false, time.Since(start))
	} else if e.observe {
		e.logger.Info("stream_creation_failed", "model_id", e.id, "create_ns", time.Since(start).Nanoseconds())
	}
	return err
}

// ReleaseSegment 等真实同步释放返回后才记录，不用取消信号代替释放。
func (e *loggedSegmentedEngine) ReleaseSegment() {
	start := time.Now()
	e.SegmentedEngine.ReleaseSegment()
	e.closedSegment("segment_release", time.Since(start))
}

// closedSegment 只结束已经成功移交的一份流，重复清理不产生重复事件。
// engine_close 的耗时包含模型关闭，不能当作纯流释放耗时。
func (e *loggedSegmentedEngine) closedSegment(reason string, elapsed time.Duration) {
	if !e.streamOpen {
		return
	}
	if e.observe {
		e.logger.Info("stream_closed", "model_id", e.id, "segment_id", e.segment, "reason", reason,
			"samples", e.samples, "sample_f32le_sha256", hex.EncodeToString(e.sampleHash.Sum(nil)), "release_ns", elapsed.Nanoseconds())
	}
	e.streamOpen = false
	e.sampleHash = nil
}
