package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestRealWorkerSegmentedProcess 验证显式 CLI、日志包装和实际 gRPC/native 链路。
// 使用短静音检查流生命周期，不测稳定容量、准确率或延迟目标。
func TestRealWorkerSegmentedProcess(t *testing.T) {
	bin, model := os.Getenv("TIDE_SHERPA_WORKER_BIN"), os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if bin == "" || model == "" {
		t.Skip("set Worker binary and pinned model paths for segmented process smoke")
	}
	out := os.Getenv("TIDE_SHERPA_SEGMENTED_EVIDENCE_DIR")
	if out == "" {
		out = filepath.Join(t.TempDir(), "segmented")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	process := startRealWorkerProcess(t, bin, model, out, "-max-audio-bytes=128000", "-segment-audio-bytes=32000")
	conn, err := grpc.NewClient("passthrough:///"+process.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	type report struct {
		Name       string
		InputBytes int
		Results    []*asrv1.StreamingRecognizeResponse
		Finals     []string
	}
	var reports []report
	for _, shortTail := range []bool{false, true} {
		r := report{Name: "boundary", InputBytes: 64000}
		if shortTail {
			r.Name = "short_tail"
			r.InputBytes += 3200
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		func() {
			defer cancel()
			stream, err := asrv1.NewASRServiceClient(conn).StreamingRecognize(ctx)
			if err != nil {
				t.Fatal(err)
			}
			readFinal := func(want string) {
				for {
					response, err := stream.Recv()
					if err != nil {
						t.Fatal(err)
					}
					r.Results = append(r.Results, response)
					if response.Progress != nil || response.SegmentId != want || response.Text != "" {
						t.Fatalf("unexpected silence result: %v", response)
					}
					if response.IsFinal {
						r.Finals = append(r.Finals, response.SegmentId)
						return
					}
				}
			}
			// 不半关闭就读取各满段 final；若 Worker 等 EOF 才刷新会到期失败。
			for segment := 1; segment <= 2; segment++ {
				for i := 0; i < 10; i++ {
					if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 3200)}); err != nil {
						t.Fatal(err)
					}
				}
				readFinal(strconv.Itoa(segment))
			}
			if shortTail {
				if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 3200)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if shortTail {
				readFinal("3")
			}
			if response, err := stream.Recv(); response != nil || err != io.EOF {
				t.Fatalf("EOF generated extra result: %v %v", response, err)
			}
		}()
		want := []string{"1", "2"}
		if shortTail {
			want = append(want, "3")
		}
		if !reflect.DeepEqual(r.Finals, want) {
			t.Fatalf("finals=%v", r.Finals)
		}
		reports = append(reports, r)
	}
	process.stop(t)
	if nativeCloseCount(filepath.Join(out, "server.log")) != 2 {
		t.Fatal("models not actually released")
	}
	writeGatewayJSON(t, filepath.Join(out, "cases.json"), reports)
	t.Logf("two native RPCs completed with 2 and 3 segment finals; models closed twice; evidence: %s", out)
}
