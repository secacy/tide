package gateway

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// 通过公开客户端、真实 WebSocket Gateway 和 TCP gRPC Worker 检查正常闭环。
// 使用包内已有资源夹具，还核对注册表实际归零；不将本地写成功视为远端清理。
func TestV2ClientGatewayEndToEnd(t *testing.T) {
	for _, mode := range []string{"empty", "aligned_tail", "interactive"} {
		t.Run(mode, func(t *testing.T) {
			backend := &v2EntryBackend{}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClient(t, backend), Config{V2: &V2Config{}})
			var input []byte
			var source io.ReadCloser
			var inputEnded atomic.Bool
			firstApplied := make(chan struct{})
			producerDone := make(chan error, 1)
			switch mode {
			case "empty":
				source = io.NopCloser(bytes.NewReader(nil))
			case "aligned_tail":
				input = []byte("aabbxx")
				source = io.NopCloser(bytes.NewReader(input))
			case "interactive":
				input = []byte("aabbccddxx")
				pr, pw := io.Pipe()
				source = pr
				go func() {
					if _, err := pw.Write(input[:4]); err != nil {
						producerDone <- err
						_ = pw.Close()
						return
					}
					select {
					case <-firstApplied:
					case <-f.ctx.Done():
						producerDone <- f.ctx.Err()
						_ = pw.Close()
						return
					}
					_, err := pw.Write(input[4:])
					inputEnded.Store(true)
					_ = pw.Close()
					producerDone <- err
				}()
			}
			var results []wsprotocol.SequencedResultMessage
			c, err := wsclient.NewV2(wsclient.V2Config{URL: f.url + "/v2/asr", ChunkBytes: 4, OnResult: func(ctx context.Context, r wsprotocol.SequencedResultMessage, at time.Time) error {
				if at.IsZero() {
					t.Error("missing receive timestamp")
				}
				results = append(results, r)
				if mode == "interactive" && r.Seq == 1 {
					if inputEnded.Load() {
						t.Error("first result arrived after input ended")
					}
					close(firstApplied)
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := c.Run(f.ctx, source)
			if err != nil || !result.Completed || !result.CompletionAckWritten || result.FinalOffset != uint64(len(input)) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if mode == "interactive" {
				select {
				case err := <-producerDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-f.ctx.Done():
					t.Fatal("producer did not exit")
				}
			}
			f.waitReturn(t)
			v2RequireIdle(t, f) // 同时检查共享 tracker、临时握手及注册表。
			if backend.calls.Load() != 1 || f.pool.picks.Load() != 1 {
				t.Fatal("unexpected worker assignment")
			}
			backend.mu.Lock()
			var actual []byte
			for _, chunk := range backend.audio {
				actual = append(actual, chunk...)
			}
			backend.mu.Unlock()
			if !bytes.Equal(actual, input) {
				t.Fatalf("worker audio=%q want=%q", actual, input)
			}
			if len(results) != (len(input)+3)/4+1 || result.AppliedSeq != uint64(len(results)) {
				t.Fatalf("results=%+v result=%+v", results, result)
			}
			for i, r := range results {
				if r.Seq != uint64(i+1) {
					t.Fatal("result gap")
				}
			}
			tail := results[len(results)-1]
			if tail.SegmentID != "tail" || tail.Text != "complete" || !tail.IsFinal {
				t.Fatalf("tail=%+v", tail)
			}
		})
	}
}
