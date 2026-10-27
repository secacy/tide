package native

import (
	"context"
	"errors"
	"os"
	"testing"
)

// TestRealModelLifecycle 显式启用才加载模型；普通测试不要求下载权重。
func TestRealModelLifecycle(t *testing.T) {
	dir := os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if dir == "" {
		t.Skip("set TIDE_SHERPA_MODEL_DIR to run the pinned native model")
	}
	factory, err := NewFactory(Config{ModelDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, cancelInput := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		e, err := factory(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		// 子函数保证失败时也先 Close，再取消/开始下一场。
		func() {
			defer cancel()
			defer e.Close()
			if err := e.Accept(make([]float32, 1600)); err != nil {
				t.Fatal(err)
			}
			if cancelInput {
				cancel()
				if err := e.Finish(); !errors.Is(err, context.Canceled) {
					t.Fatalf("Finish after cancel=%v", err)
				}
				return
			}
			// 2.035s 静音，包括不足 100ms 的最后 560 样本。
			for remaining := 32560 - 1600; remaining > 0; {
				n := min(1600, remaining)
				if err := e.Accept(make([]float32, n)); err != nil {
					t.Fatal(err)
				}
				if err := drainRealTestEngine(e); err != nil {
					t.Fatal(err)
				}
				remaining -= n
			}
			if err := e.Finish(); err != nil {
				t.Fatal(err)
			}
			if err := drainRealTestEngine(e); err != nil {
				t.Fatal(err)
			}
			if text, err := e.Result(); text != "" || err != nil {
				t.Fatalf("silence result=(%q,%v)", text, err)
			}
			if err := e.Finish(); !errors.Is(err, errFinished) {
				t.Fatalf("repeat Finish=%v", err)
			}
			if err := e.Accept([]float32{0}); !errors.Is(err, errFinished) {
				t.Fatalf("input after Finish=%v", err)
			}
		}()
		owned := e.(*engine)
		if !owned.closed || owned.stream != nil || owned.recognizer != nil {
			t.Fatal("native resources not cleared after Close returned")
		}
		e.Close()
		if _, err := e.Result(); !errors.Is(err, errClosed) {
			t.Fatalf("result after Close=%v", err)
		}
	}
}

// TestRealModelSegmentedLifecycle 验证真实模型复用、逐段流重建和取消清理。
// 短静音只检查生命周期；不作为长时容量或识别质量实验。
func TestRealModelSegmentedLifecycle(t *testing.T) {
	dir := os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if dir == "" {
		t.Skip("set TIDE_SHERPA_MODEL_DIR to run the pinned native model")
	}
	factory, err := NewSegmentedFactory(Config{ModelDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e, err := factory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	owned := e.(*engine)
	model := owned.recognizer
	for segment := 0; segment < 3; segment++ {
		if segment > 0 {
			if err := e.StartSegment(); err != nil {
				t.Fatal(err)
			}
		}
		if owned.leadingPadded || owned.finished || owned.recognizer != model || owned.stream == nil {
			t.Fatal("new segment inherited state or reloaded model")
		}
		for i := 0; i < 10; i++ {
			if err := e.Accept(make([]float32, 1600)); err != nil {
				t.Fatal(err)
			}
			if err := drainRealTestEngine(e); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Finish(); err != nil {
			t.Fatal(err)
		}
		if err := drainRealTestEngine(e); err != nil {
			t.Fatal(err)
		}
		if text, err := e.Result(); text != "" || err != nil {
			t.Fatalf("silence segment %d=(%q,%v)", segment+1, text, err)
		}
		e.ReleaseSegment()
		e.ReleaseSegment()
		if owned.stream != nil || owned.recognizer != model {
			t.Fatal("release did not separate stream and model ownership")
		}
		if _, err := e.Ready(); !errors.Is(err, errNoStream) {
			t.Fatalf("ready after release=%v", err)
		}
		t.Logf("segment %d: one second of silence finished and stream released; recognizer retained", segment+1)
	}
	if err := e.StartSegment(); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(make([]float32, 1600)); err != nil {
		t.Fatal(err)
	}
	cancel()
	e.ReleaseSegment()
	e.Close()
	e.Close()
	if !owned.closed || owned.stream != nil || owned.recognizer != nil {
		t.Fatal("canceled model not actually cleared")
	}
	if err := e.StartSegment(); !errors.Is(err, errClosed) {
		t.Fatalf("start after actual close=%v", err)
	}
	t.Log("fourth stream: canceled after 100ms input; stream and recognizer cleared; repeated Close safe")
}

func drainRealTestEngine(e interface {
	Ready() (bool, error)
	Decode() error
}) error {
	for {
		ready, err := e.Ready()
		if err != nil || !ready {
			return err
		}
		if err := e.Decode(); err != nil {
			return err
		}
	}
}
