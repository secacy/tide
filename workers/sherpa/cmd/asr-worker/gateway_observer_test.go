package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// gatewayRPCTrace 观察 Gateway 成功交付 gRPC.Send 的负载和收到的快照。
// 此位置不是模型处理确认；保护 Send/Recv/CloseSend 的并发访问。
type gatewayRPCTrace struct {
	mu         sync.Mutex
	audioHash  hash.Hash
	audioBytes int
	chunks     []int
	results    []gatewayWorkerResult
	halfClosed bool
	terminal   string
}

type gatewayWorkerResult struct {
	SegmentID string `json:"segment_id"`
	Text      string `json:"text"`
	Final     bool   `json:"final"`
	Progress  bool   `json:"has_progress"`
}

type gatewayRPCSnapshot struct {
	AudioBytes int                   `json:"audio_bytes"`
	AudioSHA   string                `json:"audio_sha256"`
	Chunks     []int                 `json:"chunks"`
	Results    []gatewayWorkerResult `json:"results"`
	HalfClosed bool                  `json:"half_closed"`
	Terminal   string                `json:"terminal"`
}

func (r *gatewayRPCTrace) snapshot() gatewayRPCSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return gatewayRPCSnapshot{r.audioBytes, hex.EncodeToString(r.audioHash.Sum(nil)), append([]int(nil), r.chunks...), append([]gatewayWorkerResult(nil), r.results...), r.halfClosed, r.terminal}
}

// gatewayObservedClient 只包裹实际 TCP 客户端，不增加 RPC 或修改消息。
type gatewayObservedClient struct {
	client asrv1.ASRServiceClient
	mu     sync.Mutex
	calls  []*gatewayRPCTrace
}

func (c *gatewayObservedClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (asrv1.ASRService_StreamingRecognizeClient, error) {
	stream, err := c.client.StreamingRecognize(ctx, opts...)
	if err != nil {
		return nil, err
	}
	trace := &gatewayRPCTrace{audioHash: sha256.New()}
	c.mu.Lock()
	c.calls = append(c.calls, trace)
	c.mu.Unlock()
	return &gatewayObservedStream{ASRService_StreamingRecognizeClient: stream, trace: trace}, nil
}

func (c *gatewayObservedClient) traces() []*gatewayRPCTrace {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*gatewayRPCTrace(nil), c.calls...)
}

type gatewayObservedStream struct {
	asrv1.ASRService_StreamingRecognizeClient
	trace *gatewayRPCTrace
}

func (s *gatewayObservedStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	err := s.ASRService_StreamingRecognizeClient.Send(req)
	if err == nil {
		s.trace.mu.Lock()
		_, _ = s.trace.audioHash.Write(req.Data)
		s.trace.audioBytes += len(req.Data)
		s.trace.chunks = append(s.trace.chunks, len(req.Data))
		s.trace.mu.Unlock()
	}
	return err
}

func (s *gatewayObservedStream) CloseSend() error {
	err := s.ASRService_StreamingRecognizeClient.CloseSend()
	if err == nil {
		s.trace.mu.Lock()
		s.trace.halfClosed = true
		s.trace.mu.Unlock()
	}
	return err
}

func (s *gatewayObservedStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	r, err := s.ASRService_StreamingRecognizeClient.Recv()
	s.trace.mu.Lock()
	defer s.trace.mu.Unlock()
	if err == nil {
		s.trace.results = append(s.trace.results, gatewayWorkerResult{r.SegmentId, r.Text, r.IsFinal, r.Progress != nil})
	} else if err == io.EOF {
		s.trace.terminal = "EOF"
	} else {
		s.trace.terminal = status.Code(err).String()
	}
	return r, err
}

// gatewayPCMSource 的 Reader 仅采集任务访问；Close 只修改原子状态，不移动游标。
// bytes.Reader 不阻塞，因此关闭无需额外 I/O 解锁。
type gatewayPCMSource struct {
	reader *bytes.Reader
	start  time.Time
	read   atomic.Int64
	eofNS  atomic.Int64
	closed atomic.Int32
}

func (s *gatewayPCMSource) Read(p []byte) (int, error) {
	if s.closed.Load() != 0 {
		return 0, io.ErrClosedPipe
	}
	n, err := s.reader.Read(p)
	s.read.Add(int64(n))
	if err == io.EOF {
		s.eofNS.CompareAndSwap(0, time.Since(s.start).Nanoseconds())
	}
	return n, err
}

func (s *gatewayPCMSource) Close() error { s.closed.Add(1); return nil }

type gatewayAppliedResult struct {
	ReceivedNS int64                             `json:"received_ns"`
	ReadBytes  int64                             `json:"source_read_bytes"`
	Message    wsprotocol.SequencedResultMessage `json:"message"`
}

// gatewayTranscript 是单个 Run 的示例消费者；只接收结果，不接触 ready 凭据。
// 更新同 ID 的文本用替换，单场之间不共享表；应用回调记录不等于协议 ACK。
type gatewayTranscript struct {
	segments map[string]wsprotocol.SequencedResultMessage
	updates  []gatewayAppliedResult
}

func (t *gatewayTranscript) apply(ctx context.Context, r wsprotocol.SequencedResultMessage, at int64, read int64, output io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Type != wsprotocol.MessageTypeResult || r.SegmentID == "" || r.Seq != uint64(len(t.updates)+1) || t.segments[r.SegmentID].IsFinal {
		return errors.New("invalid snapshot sequence")
	}
	event := gatewayAppliedResult{at, read, r}
	if err := json.NewEncoder(output).Encode(event); err != nil {
		return err
	}
	if t.segments == nil {
		t.segments = make(map[string]wsprotocol.SequencedResultMessage)
	}
	t.segments[r.SegmentID] = r
	t.updates = append(t.updates, event)
	return nil
}

func TestGatewayTranscriptReplacesRevisionsAndWithdrawals(t *testing.T) {
	var consumer gatewayTranscript
	for i, text := range []string{"患者发烧", "患者没有发烧", "", ""} {
		msg := wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: uint64(i + 1), SegmentID: "1", Text: text, IsFinal: i == 3}
		if err := consumer.apply(context.Background(), msg, int64(i+1), 2, io.Discard); err != nil {
			t.Fatal(err)
		}
		if len(consumer.segments) != 1 || consumer.segments["1"].Text != text {
			t.Fatal("snapshot was appended instead of replaced")
		}
	}
	if err := consumer.apply(context.Background(), wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 5, SegmentID: "1"}, 5, 2, io.Discard); err == nil {
		t.Fatal("accepted update after final")
	}
	if len(consumer.updates) != 4 || !consumer.segments["1"].IsFinal {
		t.Fatal("rejection changed applied state")
	}
}
