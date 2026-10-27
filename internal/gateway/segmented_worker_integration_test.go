package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/realasr"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gatewaySegmentEngine 记录模型边界；字段在 handler 返回信号之后读取。
// 历史样本只用于小输入测试，不是生产转录存储方案。
type gatewaySegmentEngine struct {
	parts                    [][]float32
	open, ready, finished    bool
	startFailure             bool
	starts, releases, closed int
}

func (e *gatewaySegmentEngine) Accept(samples []float32) error {
	if !e.open || e.finished {
		return errors.New("invalid input state")
	}
	e.parts[len(e.parts)-1] = append(e.parts[len(e.parts)-1], samples...)
	e.ready = true
	return nil
}
func (e *gatewaySegmentEngine) Ready() (bool, error) { return e.ready, nil }
func (e *gatewaySegmentEngine) Decode() error        { e.ready = false; return nil }
func (e *gatewaySegmentEngine) Result() (string, error) {
	if e.finished {
		return fmt.Sprintf("tail-%d", len(e.parts)), nil
	}
	return fmt.Sprintf("partial-%d", len(e.parts)), nil
}
func (e *gatewaySegmentEngine) Finish() error { e.finished = true; e.ready = true; return nil }
func (e *gatewaySegmentEngine) StartSegment() error {
	e.starts++
	if e.startFailure {
		return errors.New("controlled stream creation failure")
	}
	if e.open {
		return errors.New("stream still open")
	}
	e.parts = append(e.parts, nil)
	e.open = true
	e.finished = false
	return nil
}
func (e *gatewaySegmentEngine) ReleaseSegment() {
	if e.open {
		e.open = false
		e.releases++
	}
}
func (e *gatewaySegmentEngine) Close() { e.ReleaseSegment(); e.closed++ }

func TestGatewaySegmentedWorkerV2CompletionAndLease(t *testing.T) {
	for _, failStart := range []bool{false, true} {
		t.Run(fmt.Sprintf("start_failure=%v", failStart), func(t *testing.T) {
			e := &gatewaySegmentEngine{parts: [][]float32{nil}, open: true, startFailure: failStart}
			var factoryCalls atomic.Int32
			w, err := realasr.NewSegmented(realasr.Config{MaxAudioBytes: 10, MaxChunkBytes: 8}, 4, func(context.Context) (realasr.SegmentedEngine, error) { factoryCalls.Add(1); return e, nil })
			if err != nil {
				t.Fatal(err)
			}
			server := &leaseExitServer{worker: w, exited: make(chan error, 2)}
			a := newLeaseAllocator(t, newBaselineTCPWorkerClient(t, server))
			f := newLeaseGatewayFixture(t, a, Config{MaxSessions: 1, MaxPendingAudioBytes: 0, V2: &V2Config{}})
			var updates []wsprotocol.SequencedResultMessage
			client, err := wsclient.NewV2(wsclient.V2Config{URL: f.url + "/v2/asr", ChunkBytes: 6, OnResult: func(_ context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
				updates = append(updates, r)
				if !failStart && r.IsFinal {
					// 片段 final 不释放整场 Lease；探测失败不改变池游标。
					lease, err := a.pool.TryAcquire()
					if lease != nil {
						lease.Release()
						return errors.New("segment final released logical-session lease")
					}
					if !errors.Is(err, workerpool.ErrNoWorkerCapacity) {
						return fmt.Errorf("unexpected pool error: %v", err)
					}
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			pcm := []byte{1, 0, 2, 0, 3, 0, 4, 0, 5, 0}
			result, runErr := client.Run(f.ctx, io.NopCloser(bytes.NewReader(pcm)))
			select {
			case err := <-server.exited:
				want := codes.OK
				if failStart {
					want = codes.Internal
				}
				if status.Code(err) != want {
					t.Fatalf("worker exit=%v", err)
				}
			case <-f.ctx.Done():
				t.Fatal("worker did not finish actual cleanup")
			}
			v2RequireIdle(t, f)
			requireLeaseReusable(t, a.pool)
			if factoryCalls.Load() != 1 || a.calls.Load() != 1 || e.closed != 1 {
				t.Fatalf("factory=%d acquire=%d close=%d", factoryCalls.Load(), a.calls.Load(), e.closed)
			}
			if failStart {
				if runErr == nil || result.Completed || result.CompletionAckWritten || len(e.parts) != 1 || e.starts != 1 {
					t.Fatalf("failed segment reported completion: %+v %v", result, runErr)
				}
				return
			}
			if runErr != nil || !result.Completed || !result.CompletionAckWritten || result.FinalOffset != 10 || result.AppliedSeq != uint64(len(updates)) {
				t.Fatalf("completion=%+v error=%v", result, runErr)
			}
			var finals []string
			finalized := map[string]bool{}
			for i, r := range updates {
				if r.Seq != uint64(i+1) || finalized[r.SegmentID] {
					t.Fatalf("invalid result order: %v", r)
				}
				if r.IsFinal {
					finals = append(finals, r.SegmentID)
					finalized[r.SegmentID] = true
					if r.Text != "tail-"+r.SegmentID {
						t.Fatal(r)
					}
				}
			}
			if !reflect.DeepEqual(finals, []string{"1", "2", "3"}) || e.starts != 2 || e.releases != 3 {
				t.Fatalf("finals=%v starts=%d releases=%d", finals, e.starts, e.releases)
			}
			var received []byte
			for _, part := range e.parts {
				for _, sample := range part {
					raw := make([]byte, 2)
					binary.LittleEndian.PutUint16(raw, uint16(int16(sample*32768)))
					received = append(received, raw...)
				}
			}
			if !bytes.Equal(received, pcm) {
				t.Fatalf("audio changed: %v", received)
			}
		})
	}
}
