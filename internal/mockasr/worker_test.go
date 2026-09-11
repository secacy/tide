package mockasr

import (
	"context"
	"io"
	"testing"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockProtocolStream 验证模型替身实际输出的协议消息，不启动网络服务。
type mockProtocolStream struct {
	grpc.ServerStream
	input  []*asrv1.StreamingRecognizeRequest
	output []*asrv1.StreamingRecognizeResponse
}

func (s *mockProtocolStream) Context() context.Context { return context.Background() }
func (s *mockProtocolStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	if len(s.input) == 0 {
		return nil, io.EOF
	}
	r := s.input[0]
	s.input = s.input[1:]
	return r, nil
}
func (s *mockProtocolStream) Send(r *asrv1.StreamingRecognizeResponse) error {
	s.output = append(s.output, r)
	return nil
}

func TestWorkerConfirmsAudioWithoutPartial(t *testing.T) {
	stream := &mockProtocolStream{input: []*asrv1.StreamingRecognizeRequest{{Data: []byte{0, 0}, AudioSeq: 1}, {Data: []byte{0, 0}, AudioSeq: 2}}}
	if err := New(Config{}).StreamingRecognize(stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.output) != 3 {
		t.Fatalf("got %d responses", len(stream.output))
	}
	for i, response := range stream.output[:2] {
		if response.Progress == nil || response.Progress.ProcessedThroughSeq != uint64(i+1) || response.Text != "" || response.IsFinal {
			t.Fatalf("invalid progress: %+v", response)
		}
	}
	if !stream.output[2].IsFinal || stream.output[2].Progress != nil {
		t.Fatal("missing independent final")
	}
}
func TestWorkerRejectsInvalidAudioSequence(t *testing.T) {
	for _, seqs := range [][]uint64{{0}, {2}, {1, 1}, {1, 3}} {
		stream := &mockProtocolStream{}
		for _, seq := range seqs {
			stream.input = append(stream.input, &asrv1.StreamingRecognizeRequest{Data: []byte{0, 0}, AudioSeq: seq})
		}
		if err := New(Config{}).StreamingRecognize(stream); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("sequence %v: %v", seqs, err)
		}
	}
}
