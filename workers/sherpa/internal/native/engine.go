package native

import (
	"context"
	"errors"

	sherpa "github.com/k2-fsa/sherpa-onnx-go-macos"
	"github.com/secacy/tide-artisan/internal/realasr"
)

const sampleRate = 16000

var (
	errClosed   = errors.New("native engine is closed")
	errFinished = errors.New("native input is already finished")
	errNoStream = errors.New("native segment stream is absent")
)

// engine 由单场 RPC 串行拥有；所有原生调用返回后才能 Close。
// 底层没有 error 返回的调用不能通过此包装获得原生故障恢复能力。
type engine struct {
	ctx           context.Context          // 原 RPC context，不拥有 cancel。
	recognizer    *sherpa.OnlineRecognizer // 本场独占，最后释放。
	stream        *sherpa.OnlineStream     // 本场独占，先释放。
	leadingPadded bool                     // 本段已提交前置 padding，不是原始音频进度。
	finished      bool                     // 本段已声明输入结束，尾部仍可继续解码。
	closed        bool                     // 已开始释放，不允许继续使用。
}

var _ realasr.Engine = (*engine)(nil)
var _ realasr.SegmentedEngine = (*engine)(nil)

// check 只检查关闭状态和取消，不调用原生 API，不排斥尾部解码。
func (e *engine) check() error {
	if e.closed {
		return errClosed
	}
	return e.ctx.Err()
}

// checkStream 保留关闭/取消优先级，再拒绝无流状态，避免进入 C。
func (e *engine) checkStream() error {
	if err := e.check(); err != nil {
		return err
	}
	if e.stream == nil {
		return errNoStream
	}
	return nil
}

// Accept 接收非空原始样本；每段首次输入前添加一次 300ms 零 padding。
// 每个同步调用前后检查取消，观察到取消后不再提交下一块。
func (e *engine) Accept(samples []float32) error {
	if err := e.check(); err != nil {
		return err
	}
	if e.finished {
		return errFinished
	}
	if len(samples) == 0 {
		return errors.New("native samples must not be empty")
	}
	if err := e.checkStream(); err != nil {
		return err
	}
	if !e.leadingPadded {
		e.stream.AcceptWaveform(sampleRate, make([]float32, 4800))
		e.leadingPadded = true
		if err := e.check(); err != nil {
			return err
		}
	}
	e.stream.AcceptWaveform(sampleRate, samples)
	return e.check()
}

// Ready 仅表示能否执行下一步解码，不表示连续原始字节处理位置。
func (e *engine) Ready() (bool, error) {
	if err := e.checkStream(); err != nil {
		return false, err
	}
	ready := e.recognizer.IsReady(e.stream)
	if err := e.check(); err != nil {
		return false, err
	}
	return ready, nil
}

// Decode 执行一轮原生解码，Worker 保证此前 Ready=true。
func (e *engine) Decode() error {
	if err := e.checkStream(); err != nil {
		return err
	}
	e.recognizer.Decode(e.stream)
	return e.check()
}

// Result 返回当前段完整可修订快照，不追加旧段、不自行定稿。
func (e *engine) Result() (string, error) {
	if err := e.checkStream(); err != nil {
		return "", err
	}
	result := e.recognizer.GetResult(e.stream)
	if err := e.check(); err != nil {
		return "", err
	}
	return result.Text, nil
}

// Finish 提交 600ms 尾部 padding，再声明输入结束。
// 成功后禁止新输入，Ready/Decode/Result 仍可用于排空尾部。
func (e *engine) Finish() error {
	if err := e.check(); err != nil {
		return err
	}
	if e.finished {
		return errFinished
	}
	if !e.leadingPadded {
		return errors.New("native input has not started")
	}
	if err := e.checkStream(); err != nil {
		return err
	}
	e.stream.AcceptWaveform(sampleRate, make([]float32, 9600))
	if err := e.check(); err != nil {
		return err
	}
	e.stream.InputFinished()
	e.finished = true
	return e.check()
}

// ReleaseSegment 同步释放当前流，保留 recognizer；取消后仍执行清理。
// 允许串行重复调用，不刷新、不重建流。
func (e *engine) ReleaseSegment() {
	if e.stream != nil {
		sherpa.DeleteOnlineStream(e.stream)
		e.stream = nil
	}
}

// StartSegment 要求旧流已释放；创建失败清理局部流，整场应终止。
// 原生创建返回后再次检查取消，不将半成品流交给调用者。
func (e *engine) StartSegment() error {
	if err := e.check(); err != nil {
		return err
	}
	if e.stream != nil {
		return errors.New("native segment stream already exists")
	}
	if e.recognizer == nil {
		return errors.New("native recognizer is absent")
	}
	stream := sherpa.NewOnlineStream(e.recognizer)
	if err := e.ctx.Err(); err != nil {
		if stream != nil {
			sherpa.DeleteOnlineStream(stream)
		}
		return err
	}
	// 绑定不暴露内部 C 指针，非 nil 包装对象不保证底层分配成功。
	if stream == nil {
		return errors.New("native stream creation failed")
	}
	e.stream = stream
	e.leadingPadded = false
	e.finished = false
	return nil
}

// Close 顺序释放当前 stream/recognizer；即使已取消也执行，不补做 Finish。
// 串行重复调用安全；这不允许它与其他方法并发执行。
func (e *engine) Close() {
	if e.closed {
		return
	}
	e.closed = true
	e.ReleaseSegment()
	if e.recognizer != nil {
		sherpa.DeleteOnlineRecognizer(e.recognizer)
		e.recognizer = nil
	}
}
