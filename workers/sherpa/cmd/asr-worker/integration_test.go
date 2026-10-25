package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// addressCapture 只由 exec 的 stderr 拷贝任务写；通过通道发布启动地址。
type addressCapture struct {
	pending []byte
	address chan string
}

func (w *addressCapture) Write(p []byte) (int, error) {
	w.pending = append(w.pending, p...)
	for {
		i := bytes.IndexByte(w.pending, '\n')
		if i < 0 {
			break
		}
		var event struct{ Msg, Address string }
		if json.Unmarshal(w.pending[:i], &event) == nil && event.Msg == "listening" {
			select {
			case w.address <- event.Address:
			default:
			}
		}
		w.pending = w.pending[i+1:]
	}
	return len(p), nil
}

type realCaseReport struct {
	Name                string   `json:"name"`
	InputBytes          int      `json:"input_bytes"`
	SentBytes           int64    `json:"sent_bytes"`
	Status              string   `json:"status"`
	Error               string   `json:"error,omitempty"`
	Updates             int      `json:"updates"`
	Finals              int      `json:"finals"`
	FirstTextNS         *int64   `json:"rpc_start_to_first_text_ns"`
	FirstTextBeforeEnd  bool     `json:"first_text_before_all_bytes_sent"`
	TailNS              *int64   `json:"close_send_start_to_eof_ns"`
	LastText            string   `json:"last_text_snapshot"`
	ProcessedAudioBytes *uint64  `json:"processed_audio_bytes"`
	CER                 *float64 `json:"cer"`
}

// runRealCase 使用固定排期发送 100ms PCM，接收与发送并行。
// 第一条文字从 RPC 发起计时，包含本场模型加载，不与旧探针的加载后口径混用。
func runRealCase(ctx context.Context, client asrv1.ASRServiceClient, name string, pcm []byte, cancelOnText bool, events io.Writer) (realCaseReport, error) {
	r := realCaseReport{Name: name, InputBytes: len(pcm)}
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		return r, err
	}
	var sent, halfClosed atomic.Int64
	uploaded := make(chan error, 1)
	go func() {
		for pos := 0; pos < len(pcm); pos += 3200 {
			at := start.Add(time.Duration(pos) * time.Second / 32000)
			if delay := time.Until(at); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					uploaded <- ctx.Err()
					return
				case <-timer.C:
				}
			}
			end := min(pos+3200, len(pcm))
			if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: pcm[pos:end]}); err != nil {
				uploaded <- err
				return
			}
			sent.Store(int64(end))
		}
		halfClosed.Store(time.Since(start).Nanoseconds())
		err := stream.CloseSend()
		uploaded <- err
	}()
	enc := json.NewEncoder(events)
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				r.Status = "completed"
				terminal := time.Since(start).Nanoseconds()
				if err := <-uploaded; err != nil {
					return r, err
				}
				r.SentBytes = sent.Load()
				tail := terminal - halfClosed.Load()
				r.TailNS = &tail
				return r, nil
			}
			r.Status = status.Code(recvErr).String()
			cancel()
			<-uploaded
			r.SentBytes = sent.Load()
			return r, recvErr
		}
		if response.Progress != nil || response.SegmentId != "1" || r.Finals != 0 {
			cancel()
			<-uploaded
			return r, errors.New("unexpected progress, segment ID or result after final")
		}
		r.Updates++
		r.LastText = response.Text
		if response.IsFinal {
			r.Finals++
		}
		now := time.Since(start).Nanoseconds()
		if r.FirstTextNS == nil && strings.TrimSpace(response.Text) != "" {
			r.FirstTextNS = &now
			r.FirstTextBeforeEnd = sent.Load() < int64(len(pcm))
		}
		if err := enc.Encode(struct {
			ElapsedNS int64                             `json:"elapsed_ns"`
			SentBytes int64                             `json:"sent_bytes"`
			Response  *asrv1.StreamingRecognizeResponse `json:"response"`
		}{now, sent.Load(), response}); err != nil {
			cancel()
			<-uploaded
			return r, err
		}
		if cancelOnText && !response.IsFinal && strings.TrimSpace(response.Text) != "" {
			cancel() // 只证明观察到文字后取消，不推断正在某个 C 调用内部。
		}
	}
}

// readTestPCM 仅解析本次固定示例；容器头和元数据不进入 Worker。
func readTestPCM(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WAVE" || uint64(binary.LittleEndian.Uint32(data[4:8]))+8 != uint64(len(data)) {
		return nil, errors.New("invalid WAV container")
	}
	var pcm []byte
	formatOK := false
	for pos := 12; pos < len(data); {
		if pos+8 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		kind := string(data[pos : pos+4])
		pos += 8
		if n > len(data)-pos {
			return nil, io.ErrUnexpectedEOF
		}
		chunk := data[pos : pos+n]
		if kind == "fmt " {
			formatOK = n >= 16 && binary.LittleEndian.Uint16(chunk[0:2]) == 1 && binary.LittleEndian.Uint16(chunk[2:4]) == 1 && binary.LittleEndian.Uint32(chunk[4:8]) == 16000 && binary.LittleEndian.Uint16(chunk[12:14]) == 2 && binary.LittleEndian.Uint16(chunk[14:16]) == 16
		}
		if kind == "data" {
			if pcm != nil {
				return nil, errors.New("multiple WAV data chunks")
			}
			pcm = chunk
		}
		pos += n + n%2
		if pos > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
	}
	if !formatOK || len(pcm) == 0 || len(pcm)%2 != 0 || len(pcm) > 960000 {
		return nil, errors.New("expected short mono PCM16 16kHz WAV")
	}
	return pcm, nil
}

// TestRealWorkerProcess 使用非 race 可执行程序，显式启用才执行真实模型。
// 普通测试不依赖模型、下载或预构建程序；超时只用于测试兜底。
func TestRealWorkerProcess(t *testing.T) {
	bin, modelDir := os.Getenv("TIDE_SHERPA_WORKER_BIN"), os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if bin == "" || modelDir == "" {
		t.Skip("set TIDE_SHERPA_WORKER_BIN and TIDE_SHERPA_MODEL_DIR for real process verification")
	}
	out := os.Getenv("TIDE_SHERPA_EVIDENCE_DIR")
	if out == "" {
		out = filepath.Join(t.TempDir(), "evidence")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		name string
		args []string
		ok   bool
	}{
		{"help", []string{"-h"}, true},
		{"invalid_threads", []string{"-model-dir=" + modelDir, "-threads=0"}, false},
		{"missing_assets", []string{"-model-dir=" + filepath.Join(out, "missing")}, false},
	} {
		data, err := exec.Command(bin, check.args...).CombinedOutput()
		if writeErr := os.WriteFile(filepath.Join(out, check.name+".log"), data, 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
		if (err == nil) != check.ok {
			t.Fatalf("process check %s: %v, %s", check.name, err, data)
		}
	}
	logFile, err := os.Create(filepath.Join(out, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	capture := &addressCapture{address: make(chan string, 1)}
	cmd := exec.Command(bin, "-model-dir="+modelDir, "-listen=127.0.0.1:0", "-threads=2")
	cmd.Stdout, cmd.Stderr = logFile, io.MultiWriter(logFile, capture)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(waited) }()
	t.Cleanup(func() {
		select {
		case <-waited:
		default:
			_ = cmd.Process.Kill()
			<-waited
		}
	})
	var address string
	select {
	case address = <-capture.address:
	case <-waited:
		t.Fatalf("process exited during startup: %v", waitErr)
	case <-time.After(5 * time.Second):
		t.Fatal("process did not listen")
	}
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := asrv1.NewASRServiceClient(conn)
	var reports []realCaseReport
	run := func(name string, pcm []byte, cancelOnText bool) (realCaseReport, error) {
		f, err := os.Create(filepath.Join(out, name+".jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		r, runErr := runRealCase(context.Background(), client, name, pcm, cancelOnText, f)
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if runErr != nil {
			r.Error = runErr.Error()
		}
		reports = append(reports, r)
		data, err := json.MarshalIndent(reports, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "summary.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		t.Logf("case=%s input=%d sent=%d status=%s updates=%d final=%d first_text_ns=%v", name, r.InputBytes, r.SentBytes, r.Status, r.Updates, r.Finals, r.FirstTextNS)
		return r, runErr
	}
	var speech0 []byte
	for i := 0; i < 2; i++ {
		pcm, err := readTestPCM(filepath.Join(modelDir, "test_wavs", fmt.Sprintf("%d.wav", i)))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			speech0 = pcm
		}
		r, err := run(fmt.Sprintf("speech%d", i), pcm, false)
		if err != nil || r.Finals != 1 || !r.FirstTextBeforeEnd || r.FirstTextNS == nil || r.SentBytes != int64(len(pcm)) {
			t.Fatalf("speech failed: %+v, %v", r, err)
		}
	}
	silence := make([]byte, 65120)
	r, err := run("silence", silence, false)
	if err != nil || r.Finals != 1 || r.LastText != "" || r.FirstTextNS != nil || r.SentBytes != int64(len(silence)) {
		t.Fatalf("silence failed: %+v, %v", r, err)
	}
	r, err = run("cancel_after_partial", speech0, true)
	if status.Code(err) != codes.Canceled || r.Finals != 0 || r.FirstTextNS == nil || r.SentBytes >= int64(len(speech0)) {
		t.Fatalf("expected partial-input cancellation: %+v, %v", r, err)
	}
	// 客户端先看到取消，原生资源可能稍后才释放；忙时只进行有界探测。
	for attempt := 0; ; attempt++ {
		r, err = run(fmt.Sprintf("after_cancel_%d", attempt), silence, false)
		if err == nil {
			if r.Finals != 1 || r.LastText != "" {
				t.Fatalf("post-cancel probe failed: %+v", r)
			}
			break
		}
		if status.Code(err) != codes.ResourceExhausted || attempt >= 20 {
			t.Fatalf("Worker did not become reusable: %+v, %v", r, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waited:
		if waitErr != nil {
			t.Fatalf("idle shutdown failed: %v", waitErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("idle process failed to exit")
	}
	if err := logFile.Sync(); err != nil {
		t.Fatal(err)
	}
	logs, err := os.ReadFile(filepath.Join(out, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	created, closed := map[uint64]int{}, map[uint64]int{}
	for _, line := range bytes.Split(logs, []byte{'\n'}) {
		var event struct {
			Msg     string `json:"msg"`
			ModelID uint64 `json:"model_id"`
		}
		if json.Unmarshal(line, &event) != nil {
			continue
		}
		if event.Msg == "model_created" {
			created[event.ModelID]++
		}
		if event.Msg == "engine_closed" {
			closed[event.ModelID]++
		}
	}
	if len(created) != 5 || len(closed) != len(created) {
		t.Fatalf("model creation/release mismatch: created=%v closed=%v", created, closed)
	}
	for id, count := range created {
		if count != 1 || closed[id] != 1 {
			t.Fatalf("model %d created %d / closed %d times", id, count, closed[id])
		}
	}
	data, err := json.MarshalIndent(map[string]any{"address": address, "process_exit": 0, "models_created": len(created), "models_closed": len(closed)}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "process.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
