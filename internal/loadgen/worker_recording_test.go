package loadgen_test

import (
	"bytes"
	"encoding/json"
	"github.com/secacy/tide-artisan/internal/loadgen"
	"os"
	"strconv"
	"testing"
)

// readWorkerRecording 检查落盘行数、序号、成功/失败分类与内存计数能相互复核。
func readWorkerRecording(t *testing.T, report loadgen.WorkerRecordingReport) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(report.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(report.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		t.Fatalf("unexpected file mode: %v", info.Mode())
	}
	if report.SamplesWritten != report.SuccessfulSamples+report.FailedSamples {
		t.Fatal("recording count invariant failed")
	}
	if !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatalf("unterminated sample file: %q", data)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	if int64(len(lines)) != report.SamplesWritten {
		t.Fatalf("lines=%d report=%+v", len(lines), report)
	}
	var successes, failures int64
	var docs []map[string]any
	for i, line := range lines {
		doc := decodeWorkerLine(t, append(bytes.Clone(line), '\n'))
		if doc["index"] != json.Number(strconv.Itoa(i)) {
			t.Fatalf("index lost: %#v", doc)
		}
		if doc["error"] == nil {
			successes++
			if doc["state"] == nil {
				t.Fatal("success without state")
			}
		} else {
			failures++
			assertJSONNull(t, doc, "state")
		}
		docs = append(docs, doc)
	}
	if successes != report.SuccessfulSamples || failures != report.FailedSamples {
		t.Fatalf("counts mismatch: successes=%d failures=%d report=%+v", successes, failures, report)
	}
	return docs
}
