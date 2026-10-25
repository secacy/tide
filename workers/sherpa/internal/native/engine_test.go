package native

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeTestAssets(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{encoderFile, decoderFile, joinerFile, tokensFile} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("file-check-only"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestFactoryValidatesBeforeNativeCreation(t *testing.T) {
	dir := writeTestAssets(t)
	for _, cfg := range []Config{{}, {ModelDir: " "}, {ModelDir: dir, Threads: -1}, {ModelDir: dir, Threads: 17}, {ModelDir: filepath.Join(dir, "missing")}} {
		if f, err := NewFactory(cfg); f != nil || err == nil {
			t.Fatalf("invalid config accepted: %+v", cfg)
		}
	}
	for _, mutation := range []string{"missing", "empty", "directory"} {
		t.Run(mutation, func(t *testing.T) {
			dir := writeTestAssets(t)
			path := filepath.Join(dir, tokensFile)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if mutation == "empty" {
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mutation == "directory" {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if f, err := NewFactory(Config{ModelDir: dir}); f != nil || err == nil {
				t.Fatal("invalid model asset accepted")
			}
		})
	}
	// 伪文件只用于配置检查；构造工厂不能提前把它们交给 C 解析。
	f, err := NewFactory(Config{ModelDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e, err := f(ctx); e != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled factory=(%v,%v), want true nil and cancellation", e, err)
	}
	if e, err := f(nil); e != nil || err == nil {
		t.Fatalf("nil context accepted: %v, %v", e, err)
	}
}

func TestEngineStateGuardsBeforeNativeCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, closed := range []bool{false, true} {
		e := &engine{ctx: ctx, closed: closed}
		want := error(context.Canceled)
		if closed {
			want = errClosed
		}
		for _, call := range []func() error{
			func() error { return e.Accept([]float32{0}) },
			func() error { _, err := e.Ready(); return err },
			e.Decode,
			func() error { _, err := e.Result(); return err },
			e.Finish,
		} {
			if err := call(); !errors.Is(err, want) {
				t.Fatalf("guard error=%v, want %v", err, want)
			}
		}
	}
	e := &engine{ctx: context.Background()}
	if err := e.Accept(nil); err == nil || e.leadingPadded {
		t.Fatal("empty input reached native calls")
	}
	if err := e.Finish(); err == nil || e.finished {
		t.Fatal("Finish before input accepted")
	}
	e.finished = true
	if err := e.Accept([]float32{0}); !errors.Is(err, errFinished) {
		t.Fatalf("input after Finish: %v", err)
	}
	if err := e.Finish(); !errors.Is(err, errFinished) {
		t.Fatalf("repeat Finish: %v", err)
	}
	// Close 对部分创建且已取消的对象仍执行，不要求 ctx 活跃。
	e.ctx = ctx
	e.Close()
	e.Close()
	if !e.closed || e.recognizer != nil || e.stream != nil {
		t.Fatal("partial cleanup state invalid")
	}
}
