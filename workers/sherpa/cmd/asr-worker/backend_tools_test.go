package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestBackendLogTailWaitsForCompleteLine(t *testing.T) {
	out := t.TempDir()
	path := filepath.Join(out, "server.log")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	m, err := newBackendMonitor(out, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = m.stop()
		}
	}()
	appendLog := func(s string) {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.WriteString(s); err != nil {
			t.Fatal(err)
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if err := m.refreshLocked(); err != nil {
			t.Fatal(err)
		}
	}
	appendLog("{\"msg\":\"model_created\",\"model_id\":1,")
	if m.created != 0 || len(m.events) != 0 {
		t.Fatal("counted an incomplete event")
	}
	appendLog("\"load_ms\":12.5}\n")
	appendLog("{\"msg\":\"engine_closed\",\"model_id\":1,\"close_ms\":2}\n")
	appendLog("") // EOF 再读不应重复统计。
	if m.created != 1 || m.closed != 1 || len(m.events) != 2 || *m.events[0].LoadMS != 12.5 {
		t.Fatal("lost or duplicated complete model events")
	}
	if m.events[1].ObservedNS < m.events[0].ObservedNS {
		t.Fatal("observation time went backwards")
	}
	appendLog("{\"msg\":\"engine_closed\",\"model_id\":1,\"close_ms\":2}")
	err = m.stop()
	stopped = true
	if err == nil {
		t.Fatal("accepted incomplete final log")
	}
}

func TestBackendLogRejectsDuplicateLifecycle(t *testing.T) {
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "server.log"), []byte("{\"msg\":\"model_created\",\"model_id\":1,\"load_ms\":1}\n{\"msg\":\"model_created\",\"model_id\":1,\"load_ms\":1}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := newBackendMonitor(out, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.stop()
	if err := m.refreshLocked(); err == nil {
		t.Fatal("accepted duplicate native creation")
	}
}

func TestBackendObserverArchiveKeepsCumulativeIdentity(t *testing.T) {
	first := &gatewayRPCTrace{}
	c := &gatewayObservedClient{calls: []*gatewayRPCTrace{first}}
	if err := c.releaseCompleted(1); err == nil {
		t.Fatal("archived live RPC")
	}
	first.terminal = "EOF"
	if err := c.releaseCompleted(1); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 0 || c.archived != 1 {
		t.Fatal("retained completed trace")
	}
	second := &gatewayRPCTrace{terminal: "Canceled"}
	c.calls = append(c.calls, second)
	if trace, err := c.traceForCall(2); err != nil || trace != second {
		t.Fatal("archive corrupted subsequent call numbering")
	}
	if _, err := c.traceForCall(1); err == nil {
		t.Fatal("reused an archived call")
	}
}

func TestBackendRSSMissingProcessIsUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if value, err := readWorkerRSS(ctx, os.Getpid()); err == nil || value != nil {
		t.Fatal("RSS failure was converted to a zero value")
	}
}

// 摘要路径与固定 PCM 的独立已知摘要比较，不读取整个文件进堆。
func TestBackendFileDigest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte("abc"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := backendFileSHA(path)
	if err != nil || got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("digest=%s, err=%v", got, err)
	}
}
