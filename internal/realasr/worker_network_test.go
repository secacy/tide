package realasr

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// workerTestObservedServer 在 handler 真正退出后记录终态。
// 客户端观察到取消并不表示 handler/原生调用已结束。
type workerTestObservedServer struct {
	*Worker
	exited chan error
}

func (s *workerTestObservedServer) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	err := s.Worker.StreamingRecognize(stream)
	s.exited <- err
	return err
}

func startWorkerTestServer(t *testing.T, w *Worker) (asrv1.ASRServiceClient, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(4096))
	observed := &workerTestObservedServer{Worker: w, exited: make(chan error, 16)}
	asrv1.RegisterASRServiceServer(server, observed)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
			t.Error("gRPC Serve did not exit")
		}
	})
	return asrv1.NewASRServiceClient(conn), observed.exited
}

func awaitWorkerTestExit(t *testing.T, exits <-chan error, code codes.Code) {
	t.Helper()
	select {
	case err := <-exits:
		if status.Code(err) != code {
			t.Fatalf("handler error=%v, want %v", err, code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish cleanup")
	}
}

func TestWorkerTCPPartialBeforeHalfCloseAndFinalEOF(t *testing.T) {
	e := newWorkerTestEngine("患者发烧", "患者没有发烧", "患者没有发烧")
	w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) { return e, nil })
	client, exits := startWorkerTestServer(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 每块发送后立即读取，若 Worker 等待输入 EOF 才发送，此处会到期失败。
	for _, text := range []string{"患者发烧", "患者没有发烧"} {
		if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: []byte{0, 0}}); err != nil {
			t.Fatal(err)
		}
		r, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		assertSnapshotResponse(t, r, text, false)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	r, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotResponse(t, r, "患者没有发烧", true)
	if r, err := stream.Recv(); r != nil || err != io.EOF {
		t.Fatalf("after final=(%v,%v), want normal EOF", r, err)
	}
	awaitWorkerTestExit(t, exits, codes.OK)
	assertWorkerTestCleaned(t, w, e)
}

func TestWorkerTCPSilenceAndEmptyInput(t *testing.T) {
	for _, withAudio := range []bool{false, true} {
		name := "empty"
		if withAudio {
			name = "silence"
		}
		t.Run(name, func(t *testing.T) {
			e := newWorkerTestEngine()
			w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) { return e, nil })
			client, exits := startWorkerTestServer(t, w)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := client.StreamingRecognize(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if withAudio {
				if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 2)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			r, err := stream.Recv()
			if !withAudio {
				if r != nil || status.Code(err) != codes.InvalidArgument {
					t.Fatalf("empty input=(%v,%v)", r, err)
				}
				awaitWorkerTestExit(t, exits, codes.InvalidArgument)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertSnapshotResponse(t, r, "", true)
				if _, err := stream.Recv(); err != io.EOF {
					t.Fatalf("silence did not complete: %v", err)
				}
				awaitWorkerTestExit(t, exits, codes.OK)
			}
			assertWorkerTestCleaned(t, w, e)
		})
	}
}

func TestWorkerTCPCancelDoesNotReleaseBusyModel(t *testing.T) {
	decodeEntered, releaseDecode := make(chan struct{}), make(chan struct{})
	closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
	var decodeOnce, closeOnce sync.Once
	// 函数 defer 先于 t.Cleanup 执行，失败时也放开模型阻塞。
	defer decodeOnce.Do(func() { close(releaseDecode) })
	defer closeOnce.Do(func() { close(releaseClose) })
	e := newWorkerTestEngine("partial")
	e.hook = func(name string) error {
		switch name {
		case "decode":
			close(decodeEntered)
			<-releaseDecode
		case "close":
			close(closeEntered)
			<-releaseClose
		}
		return nil
	}
	var calls atomic.Int32
	serverContexts := make(chan context.Context, 1)
	next := newWorkerTestEngine()
	w := newWorkerTestWorker(t, Config{}, func(ctx context.Context) (Engine, error) {
		if calls.Add(1) == 1 {
			serverContexts <- ctx
			return e, nil
		}
		return next, nil
	})
	client, exits := startWorkerTestServer(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: []byte{0, 0}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-decodeEntered:
	case <-ctx.Done():
		t.Fatal("Decode did not start")
	}
	cancel()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("client cancellation=%v", err)
	}
	serverCtx := <-serverContexts
	select {
	case <-serverCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not reach server context")
	}
	probeBusy := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		probe, err := client.StreamingRecognize(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := probe.Recv(); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("busy Worker accepted another RPC: %v", err)
		}
		awaitWorkerTestExit(t, exits, codes.ResourceExhausted)
		if calls.Load() != 1 {
			t.Fatalf("busy RPC created model %d", calls.Load())
		}
	}
	probeBusy()
	select {
	case <-closeEntered:
		t.Fatal("Close overlapped blocked Decode")
	default:
	}
	decodeOnce.Do(func() { close(releaseDecode) })
	select {
	case <-closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not start after Decode returned")
	}
	probeBusy()
	closeOnce.Do(func() { close(releaseClose) })
	awaitWorkerTestExit(t, exits, codes.Canceled)
	assertWorkerTestCleaned(t, w, e)
	if e.finished || e.decoded != 1 {
		t.Fatalf("canceled model continued normal processing: finished=%v decoded=%d", e.finished, e.decoded)
	}
	// 同一个服务实例实际释放后可以重新处理新场。
	nextCtx, nextCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer nextCancel()
	fresh, err := client.StreamingRecognize(nextCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Send(&asrv1.StreamingRecognizeRequest{Data: []byte{0, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := fresh.CloseSend(); err != nil {
		t.Fatal(err)
	}
	r, err := fresh.Recv()
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotResponse(t, r, "", true)
	if _, err := fresh.Recv(); err != io.EOF {
		t.Fatalf("new RPC did not complete: %v", err)
	}
	awaitWorkerTestExit(t, exits, codes.OK)
	assertWorkerTestCleaned(t, w, next)
}
