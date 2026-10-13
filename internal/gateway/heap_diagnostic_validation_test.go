package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeapCheckpointWriterFailures(t *testing.T) {
	for _, name := range []string{"write", "short", "close"} {
		t.Run(name, func(t *testing.T) {
			w := &extendedTestWriter{}
			switch name {
			case "write":
				w.writeErr = errors.New("disk failed")
			case "short":
				w.short = true
			case "close":
				w.closeErr = errors.New("close failed")
			}
			c := captureHeapCheckpointTo(w, func(out io.Writer) error { _, err := out.Write([]byte("profile")); return err }, time.Now(), "unit", nil)
			if c.Valid || w.closes != 1 || (name == "close" && c.CloseError == "") || (name != "close" && c.WriteError == "") {
				t.Fatalf("failure hidden or ownership incorrect: %+v closes=%d", c, w.closes)
			}
		})
	}
}

func TestHeapCheckpointExclusiveFiles(t *testing.T) {
	dir := t.TempDir()
	original := []byte("preserve previous profile")
	if err := os.WriteFile(filepath.Join(dir, "before_fixture.pb.gz"), original, 0600); err != nil {
		t.Fatal(err)
	}
	h := &heapCaseObserver{Origin: time.Now(), Dir: dir}
	h.capture("before_fixture", nil, true)
	got, err := os.ReadFile(filepath.Join(dir, "before_fixture.pb.gz"))
	if err != nil || !bytes.Equal(got, original) {
		t.Fatal("existing profile overwritten")
	}
	if len(h.Rows) != 1 || h.Rows[0].Valid || h.Rows[0].WriteError == "" {
		t.Fatal("open failure hidden")
	}
	if err := writeHeapJSON(filepath.Join(dir, "before_fixture.json"), map[string]bool{"overwrite": true}); err == nil {
		t.Fatal("metadata overwrite accepted")
	}
}

func TestHeapCheckpointRunnerJoins(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r := newHeapCheckpointRunner(func(ctx context.Context) { close(entered); <-release })
	<-entered
	stopped := make(chan struct{})
	go func() { r.stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("cancel treated as completion")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("runner not joined")
	}
	r.stop()
}

func TestHeapActiveCheckpointCancelledBeforeRead(t *testing.T) {
	h := &heapCaseObserver{Origin: time.Now(), Dir: t.TempDir(), Early: time.Second, Late: 2 * time.Second}
	var first atomic.Int64
	first.Store(-1)
	h.start(time.Now(), &first, func() heapResources { return heapResources{Sessions: 1, Registry: 1, WorkerActive: 1} })
	h.returnedNS.Store(0)
	h.detach()
	if h.FirstNS != nil || len(h.Rows) != 0 || h.state != nil || h.runner != nil || !h.Detached {
		t.Fatal("cancel fabricated snapshot or retained fixture closure")
	}
}

// heapKnownLive 阻止优化器把已知分配消除；只在专用验证子进程使用。
var heapKnownLive [][]byte

//go:noinline
func allocateKnownHeap() {
	for i := 0; i < 8; i++ {
		p := make([]byte, 512*1024)
		p[0] = byte(i)
		heapKnownLive = append(heapKnownLive, p)
	}
}

// TestHeapProfileObjectLifecycle 用自有子进程从启动设置 rate=1，核对特定分配栈的释放。
// 既不改变其他测试的全局 rate，也不把全进程 HeapAlloc 当作对象大小。
func TestHeapProfileObjectLifecycle(t *testing.T) {
	if dir := os.Getenv("TIDE_HEAP_OBJECT_CHILD"); dir != "" {
		if runtime.MemProfileRate != 1 {
			t.Fatal("child profile rate differs")
		}
		h := &heapCaseObserver{Origin: time.Now(), Dir: dir}
		allocateKnownHeap()
		h.capture("retained", nil, true)
		runtime.KeepAlive(heapKnownLive)
		heapKnownLive = nil
		h.capture("released", nil, true)
		for _, c := range h.Rows {
			if !c.Valid {
				t.Fatalf("invalid object profile: %+v", c)
			}
		}
		return
	}
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, binary, "-test.run=^TestHeapProfileObjectLifecycle$", "-test.count=1", "-test.timeout=35s")
	child.Env = append(os.Environ(), "TIDE_HEAP_OBJECT_CHILD="+dir, "GODEBUG=memprofilerate=1")
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("object child: %v\n%s", err, output)
	}
	flat := func(name string) int64 {
		cmd := exec.CommandContext(ctx, "go", "tool", "pprof", "-top", "-nodecount=0", "-nodefraction=0", "-edgefraction=0", "-sample_index=inuse_space", "-unit=bytes", binary, filepath.Join(dir, name+".pb.gz"))
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("read object profile: %v\n%s", err, output)
		}
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasSuffix(strings.TrimSpace(line), ".allocateKnownHeap") {
				fields := strings.Fields(line)
				n, err := strconv.ParseInt(strings.TrimSuffix(fields[0], "B"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
		}
		return 0
	}
	before, after := flat("retained"), flat("released")
	if before < 4*1024*1024 || after >= before/100 {
		t.Fatalf("known allocation not visible/released: before=%d after=%d", before, after)
	}
	t.Logf("known allocation stack retained=%dB released=%dB", before, after)
}

// TestHeapFixtureCleanupBoundary 证明外层在所有 Cleanup 返回后才观察关闭事实。
func TestHeapFixtureCleanupBoundary(t *testing.T) {
	var status baselineTCPCleanup
	if !t.Run("owned_fixture", func(t *testing.T) {
		newBaselineTCPWorkerClientTracked(t, &extendedWorker{}, &status)
		if status.ClientClosed || status.ServeExited {
			t.Fatal("premature cleanup status")
		}
	}) {
		t.Fatal("fixture failed")
	}
	if !status.ClientClosed || !status.ServeExited {
		t.Fatal("cleanup not observed after child returned")
	}
}

func TestHeapActiveCheckpointRejectsWrongPhase(t *testing.T) {
	for _, name := range []string{"late", "inactive", "crossed_return", "future"} {
		t.Run(name, func(t *testing.T) {
			origin := time.Now()
			h := &heapCaseObserver{Origin: origin, Dir: t.TempDir(), fixtureOrigin: origin, state: func() heapResources { return heapResources{Sessions: 1, Registry: 1, WorkerActive: 1} }}
			h.returnedNS.Store(-1)
			planned := int64(0)
			switch name {
			case "late":
				h.Origin = origin.Add(-2 * time.Second)
				h.fixtureOrigin = h.Origin
			case "inactive":
				h.state = func() heapResources { return heapResources{} }
			case "crossed_return":
				h.returnedNS.Store(0)
			case "future":
				planned = int64(time.Hour)
			}
			h.capture("early_active", &planned, true)
			if len(h.Rows) != 1 || h.Rows[0].Valid || h.Rows[0].Reason != "active_checkpoint_late_inactive_or_crossed_return" {
				t.Fatalf("invalid phase accepted: %+v", h.Rows)
			}
		})
	}
}
