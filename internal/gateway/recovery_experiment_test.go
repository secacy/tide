//go:build tide_recovery

package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// EXP-013 is a test-only recovery protocol model. Each numbered 3200-byte block
// represents 100 ms of synthetic PCM. It is not speech or a proposed wire codec.
const recoveryFrameBytes = 3200

// recoveryCommit is an atomic client application unit supplied by this Mock.
// Restartable is a Worker contract, not inferred from processed_through_seq.
// From/Through are inclusive global block indices, independent of per-RPC seq.
type recoveryCommit struct {
	Attempt       string
	From, Through int
	Tokens        []int
	Silence       bool
	Restartable   bool
}

// recoveryReplica owns the test client's retained PCM and applied results.
// It has one owner, just as a future client coordinator would; this does not
// implement or validate concurrent capture, transport, persistence or UI code.
type recoveryReplica struct {
	policy                          string
	attempt                         string
	checkpoint, processed, captured int
	cache                           map[int][]byte
	peakBytes                       int
	text                            map[int]int
	ignored, commits                int
	exhausted                       bool
}

func newRecoveryReplica(policy string) *recoveryReplica {
	return &recoveryReplica{policy: policy, attempt: "a1", cache: map[int][]byte{}, text: map[int]int{}}
}

func recoveryPCM(frame int) []byte {
	data := make([]byte, recoveryFrameBytes)
	binary.LittleEndian.PutUint64(data, uint64(frame))
	return data
}

// capture stops the trial explicitly on overflow; it never silently evicts PCM.
// The experiment deliberately leaves the post-exhaustion product action undecided.
func (c *recoveryReplica) capture(frame int) bool {
	c.captured = frame
	if len(c.cache) == 150 {
		c.exhausted = true
		return false
	}
	c.cache[frame] = recoveryPCM(frame)
	c.peakBytes = max(c.peakBytes, len(c.cache)*recoveryFrameBytes)
	return true
}

func (c *recoveryReplica) release(through int) {
	for frame := range c.cache {
		if frame <= through {
			delete(c.cache, frame)
		}
	}
}

func (c *recoveryReplica) progress(through int) {
	c.processed = through
	if c.policy == "processed" {
		c.release(through)
	}
}

// apply models the candidate's result+checkpoint application transaction.
// "unfenced" intentionally reproduces blind appending of results from old RPCs.
func (c *recoveryReplica) apply(event recoveryCommit) error {
	if c.policy != "unfenced" && event.Attempt != c.attempt {
		c.ignored++
		return nil
	}
	if c.policy != "unfenced" && event.Through <= c.checkpoint {
		c.ignored++
		return nil
	}
	if !event.Restartable || event.From < 1 || event.Through < event.From {
		return errors.New("unsafe checkpoint")
	}
	if event.Silence {
		if len(event.Tokens) != 0 {
			return errors.New("silence carried text")
		}
	} else {
		if len(event.Tokens) != event.Through-event.From+1 {
			return errors.New("incomplete result coverage")
		}
		for i, token := range event.Tokens {
			if token != event.From+i {
				return errors.New("result coverage mismatch")
			}
		}
	}
	if c.policy == "checkpoint" && event.From != c.checkpoint+1 {
		return errors.New("checkpoint hole")
	}
	// Validate the whole event before mutating any client state.
	for _, token := range event.Tokens {
		c.text[token]++
	}
	c.checkpoint = max(c.checkpoint, event.Through)
	c.commits++
	if c.policy != "processed" {
		c.release(c.checkpoint)
	}
	return nil
}

func (c *recoveryReplica) restart() int {
	c.attempt = "a2"
	if c.policy == "processed" {
		return c.processed + 1
	}
	return c.checkpoint + 1
}

func (c *recoveryReplica) coverage(through int) (missing, duplicates int) {
	for frame := 1; frame <= through; frame++ {
		if c.text[frame] == 0 {
			missing++
		}
		duplicates += max(0, c.text[frame]-1)
	}
	return
}

// recoveryWorker runs through real gRPC/bufconn. The chosen contract emits an
// independently restartable checkpoint every five blocks, including silence.
// interval=0 deliberately models a stream with no stable checkpoint before End.
type recoveryWorker struct {
	asrv1.UnimplementedASRServiceServer
	interval int
	silence  bool
	active   atomic.Int32
	exited   chan struct{}
}

func (w *recoveryWorker) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	w.active.Add(1)
	defer func() { w.active.Add(-1); w.exited <- struct{}{} }()
	md, _ := metadata.FromIncomingContext(stream.Context())
	ids := md.Get("tide-experiment-attempt")
	if len(ids) != 1 {
		return errors.New("missing attempt identity")
	}
	var local uint64
	var first, last int
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		local++
		if req.AudioSeq != local || len(req.Data) != recoveryFrameBytes {
			return errors.New("invalid experimental audio")
		}
		frame := int(binary.LittleEndian.Uint64(req.Data))
		if !bytes.Equal(req.Data, recoveryPCM(frame)) || (last != 0 && frame != last+1) {
			return errors.New("audio changed or reordered")
		}
		if first == 0 {
			first = frame
		}
		last = frame
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: local}}); err != nil {
			return err
		}
		if w.interval > 0 && frame%w.interval == 0 {
			event := recoveryCommit{Attempt: ids[0], From: first, Through: frame, Silence: w.silence, Restartable: true}
			if !w.silence {
				for n := first; n <= frame; n++ {
					event.Tokens = append(event.Tokens, n)
				}
			}
			data, err := json.Marshal(event)
			if err != nil {
				return err
			}
			// Private test envelope carried over existing protobuf; production Gateway
			// does not understand this envelope and is not used in protocol-model trials.
			if err := stream.Send(&asrv1.StreamingRecognizeResponse{Text: string(data), IsFinal: true}); err != nil {
				return err
			}
			first = frame + 1
		}
	}
}

type recoveryRPC struct {
	stream grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	cancel context.CancelFunc
	local  uint64
	worker *recoveryWorker
}

func openRecoveryRPC(t *testing.T, client asrv1.ASRServiceClient, worker *recoveryWorker, attempt string) *recoveryRPC {
	t.Helper()
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(t.Context(), metadata.Pairs("tide-experiment-attempt", attempt)))
	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	rpc := &recoveryRPC{stream: stream, cancel: cancel, worker: worker}
	t.Cleanup(cancel)
	return rpc
}

// exchange injects loss AFTER decoding the complete gRPC result but BEFORE the
// client transaction. This models non-delivery to application state, not TCP loss.
func (r *recoveryRPC) exchange(t *testing.T, c *recoveryReplica, frame int, drop bool) *recoveryCommit {
	t.Helper()
	data, ok := c.cache[frame]
	if !ok {
		t.Fatalf("replay requested unretained frame %d", frame)
	}
	r.local++
	if err := r.stream.Send(&asrv1.StreamingRecognizeRequest{Data: data, AudioSeq: r.local}); err != nil {
		t.Fatal(err)
	}
	response, err := r.stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if response.Progress == nil || response.Progress.ProcessedThroughSeq != r.local {
		t.Fatal("wrong Worker processing progress")
	}
	c.progress(frame)
	if r.worker.interval == 0 || frame%r.worker.interval != 0 {
		return nil
	}
	response, err = r.stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var event recoveryCommit
	if err := json.Unmarshal([]byte(response.Text), &event); err != nil {
		t.Fatal(err)
	}
	if !drop {
		if err := c.apply(event); err != nil {
			t.Fatal(err)
		}
	}
	return &event
}

func (r *recoveryRPC) close(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case <-r.worker.exited:
	case <-time.After(time.Second):
		t.Fatal("old Worker RPC did not exit")
	}
}

func logRecovery(t *testing.T, record map[string]any) {
	t.Helper()
	record["experiment"] = "EXP-013"
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", data)
}

func TestExperimentRecoveryProtocol(t *testing.T) {
	if os.Getenv("TIDE_RUN_RECOVERY_EXPERIMENTS") != "1" {
		t.Skip("explicit recovery experiment opt-in required")
	}
	for _, scenario := range []string{"result_lost", "result_applied", "late_old_result", "duplicate_delivery"} {
		policies := []string{"processed", "checkpoint"}
		if scenario == "late_old_result" || scenario == "duplicate_delivery" {
			policies = []string{"unfenced", "checkpoint"}
		}
		for _, policy := range policies {
			t.Run(scenario+"/"+policy, func(t *testing.T) {
				worker := &recoveryWorker{interval: 5, exited: make(chan struct{}, 2)}
				client := startGatewayWorker(t, worker)
				c := newRecoveryReplica(policy)
				rpc := openRecoveryRPC(t, client, worker, "a1")
				var held *recoveryCommit
				for frame := 1; frame <= 10; frame++ {
					if !c.capture(frame) {
						t.Fatal("unexpected overflow")
					}
					heldEvent := rpc.exchange(t, c, frame, frame == 10 && scenario != "result_applied")
					if heldEvent != nil && frame == 10 {
						held = heldEvent
					}
				}
				before := c.checkpoint
				processedBefore := c.processed
				retained := len(c.cache)
				rpc.close(t) // Model old instance loss: new RPC receives no decoder state.
				replayFrom := c.restart()
				next := openRecoveryRPC(t, client, worker, "a2")
				for frame := 11; frame <= 15; frame++ {
					if !c.capture(frame) {
						t.Fatal("overflow")
					}
				}
				for frame := replayFrom; frame <= 15; frame++ {
					event := next.exchange(t, c, frame, false)
					if scenario == "duplicate_delivery" && event != nil {
						if err := c.apply(*event); err != nil {
							t.Fatal(err)
						}
					}
				}
				if scenario == "late_old_result" {
					if err := c.apply(*held); err != nil {
						t.Fatal(err)
					}
				}
				next.close(t)
				missing, duplicates := c.coverage(15)
				wantMissing, wantDuplicates := 0, 0
				if scenario == "result_lost" && policy == "processed" {
					wantMissing = 5
				}
				if scenario == "late_old_result" && policy == "unfenced" {
					wantDuplicates = 5
				}
				if scenario == "duplicate_delivery" && policy == "unfenced" {
					wantDuplicates = 10
				}
				if missing != wantMissing || duplicates != wantDuplicates || worker.active.Load() != 0 {
					t.Fatalf("missing=%d duplicate=%d active=%d", missing, duplicates, worker.active.Load())
				}
				logRecovery(t, map[string]any{"case": scenario + "/" + policy, "scenario": scenario, "policy": policy, "checkpoint_before_failure": before, "processed_before_failure": processedBefore, "retained_before_failure": retained, "replay_from": replayFrom, "missing_frames": missing, "duplicate_frames": duplicates, "ignored_events": c.ignored, "semantic_ok": missing == 0 && duplicates == 0, "cache_peak_bytes": c.peakBytes, "cache_after": len(c.cache), "worker_active_after": worker.active.Load(), "attempts": 2})
			})
		}
	}
	for _, scenario := range []string{"checkpoint_stalled", "silence_checkpoints"} {
		t.Run(scenario, func(t *testing.T) {
			interval := 0
			if scenario == "silence_checkpoints" {
				interval = 5
			}
			worker := &recoveryWorker{interval: interval, silence: true, exited: make(chan struct{}, 1)}
			client := startGatewayWorker(t, worker)
			c := newRecoveryReplica("checkpoint")
			rpc := openRecoveryRPC(t, client, worker, "a1")
			for frame := 1; frame <= 200; frame++ {
				if !c.capture(frame) {
					break
				}
				rpc.exchange(t, c, frame, false)
			}
			rpc.close(t)
			if scenario == "checkpoint_stalled" && (!c.exhausted || c.captured != 151 || len(c.cache) != 150 || c.checkpoint != 0) {
				t.Fatal("no bounded exhaustion")
			}
			if scenario == "silence_checkpoints" && (c.exhausted || c.checkpoint != 200 || len(c.cache) != 0 || len(c.text) != 0) {
				t.Fatal("silence checkpoint did not cover audio")
			}
			if c.peakBytes > 480000 || worker.active.Load() != 0 {
				t.Fatal("resource bound violated")
			}
			logRecovery(t, map[string]any{"case": scenario, "scenario": scenario, "policy": "checkpoint", "capture_through": c.captured, "checkpoint": c.checkpoint, "explicit_cache_exhausted": c.exhausted, "unresolved_from": c.checkpoint + 1, "cache_frames": len(c.cache), "cache_peak_bytes": c.peakBytes, "worker_active_after": worker.active.Load(), "semantic_ok": !c.exhausted, "audio_frame_ms": 100, "audio_capacity_ms": 15000})
		})
	}
	t.Run("invalid_checkpoint", func(t *testing.T) {
		c := newRecoveryReplica("checkpoint")
		c.capture(1)
		rejected := 0
		for _, event := range []recoveryCommit{
			{Attempt: "a1", From: 1, Through: 1, Tokens: []int{1}},                    // Result does not establish restartability.
			{Attempt: "a1", From: 1, Through: 1, Restartable: true},                   // Checkpoint without complete result.
			{Attempt: "a1", From: 2, Through: 2, Tokens: []int{2}, Restartable: true}, // Prefix hole.
		} {
			if c.apply(event) != nil {
				rejected++
			}
		}
		if rejected != 3 || c.checkpoint != 0 || len(c.text) != 0 || len(c.cache) != 1 {
			t.Fatal("invalid checkpoint mutated state")
		}
		logRecovery(t, map[string]any{"case": "invalid_checkpoint", "rejected": rejected, "checkpoint": c.checkpoint, "cache_frames": len(c.cache), "semantic_ok": true})
	})
}

// Admission trials use the actual production Gateway and WorkerPool. The old
// mock Recv deliberately delays returning after cancellation, keeping its lease.
func TestExperimentRecoveryAdmission(t *testing.T) {
	if os.Getenv("TIDE_RUN_RECOVERY_EXPERIMENTS") != "1" {
		t.Skip("explicit recovery experiment opt-in required")
	}
	for _, exhaust := range []bool{false, true} {
		name := "released_then_admitted"
		if exhaust {
			name = "admission_budget_exhausted"
		}
		t.Run(name, func(t *testing.T) {
			real := startGatewayWorker(t, mockasr.New(mockasr.Config{FinalText: "final"}))
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var opens atomic.Int32
			worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				if opens.Add(1) > 1 {
					return real.StreamingRecognize(ctx)
				}
				return &controlledWorkerStream{ctx: ctx, send: func(*asrv1.StreamingRecognizeRequest) error { return nil }, recv: func() (*asrv1.StreamingRecognizeResponse, error) {
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-release
					return nil, ctx.Err()
				}, closeSend: func() error { return nil }}, nil
			}}
			h := newGatewayHarness(t, worker, 1)
			t.Cleanup(unblock)
			old := h.mustDial(t)
			h.write(t, old, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
			awaitGatewaySignal(t, h.ctx, entered, "old Recv entered")
			_ = old.CloseNow()
			awaitGatewaySignal(t, h.ctx, canceled, "old RPC canceled but cleanup withheld")
			budget := 500 * time.Millisecond // Accelerated correctness budget, not the proposed 10s production value.
			recoveryCtx, cancel := context.WithTimeout(h.ctx, budget)
			defer cancel()
			deadline, _ := recoveryCtx.Deadline()
			rejected := 0
			for {
				conn, response, err := websocket.Dial(recoveryCtx, "ws://gateway.test/v1/asr", &websocket.DialOptions{HTTPClient: h.client})
				if conn != nil {
					_ = conn.CloseNow()
					t.Fatal("old lease was bypassed")
				}
				if recoveryCtx.Err() != nil {
					break
				}
				if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("unexpected admission: %v %v", response, err)
				}
				rejected++
				_ = response.Body.Close()
				if got := h.gateway.pool.Snapshot()[0].Reserved; got != 1 || opens.Load() != 1 {
					t.Fatal("rejection disturbed old reservation or opened RPC")
				}
				if !exhaust {
					break
				}
				// All retry attempts share one context; no retry refreshes the deadline.
				if got, _ := recoveryCtx.Deadline(); got != deadline {
					t.Fatal("retry reset recovery budget")
				}
				select {
				case <-recoveryCtx.Done():
				case <-time.After(50 * time.Millisecond):
				}
				if recoveryCtx.Err() != nil {
					break
				}
			}
			if rejected == 0 {
				t.Fatal("did not exercise admission rejection")
			}
			exhausted := recoveryCtx.Err() != nil
			if exhausted != exhaust {
				t.Fatal("unexpected budget outcome")
			}
			reservedBeforeRelease := h.gateway.pool.Snapshot()[0].Reserved
			unblock()
			// Confirm cleanup, not just cancellation, before considering the slot reusable.
			cleanupCtx, cleanupCancel := context.WithTimeout(h.ctx, time.Second)
			defer cleanupCancel()
			for len(h.gateway.registry.snapshot()) != 0 {
				select {
				case <-cleanupCtx.Done():
					t.Fatal("old session not cleaned")
				case <-time.After(time.Millisecond):
				}
			}
			completed := false
			if !exhaust {
				conn, _, err := websocket.Dial(recoveryCtx, "ws://gateway.test/v1/asr", &websocket.DialOptions{HTTPClient: h.client})
				if err != nil {
					t.Fatal(err)
				}
				defer conn.CloseNow()
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
				h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
				h.expectResult(t, conn, "final", true)
				h.expectClose(t, conn, websocket.StatusNormalClosure)
				completed = true
			}
			h.gateway.StopAccepting()
			if err := h.gateway.Wait(h.ctx); err != nil {
				t.Fatal(err)
			}
			reserved := h.gateway.pool.Snapshot()[0].Reserved
			wantOpens := int32(2)
			if exhaust {
				wantOpens = 1
			}
			if reserved != 0 || opens.Load() != wantOpens {
				t.Fatal("admission cleanup/accounting mismatch")
			}
			logRecovery(t, map[string]any{"case": name, "rejections": rejected, "budget_ms": budget.Milliseconds(), "budget_exhausted": exhausted, "reserved_before_release": reservedBeforeRelease, "reserved_after": reserved, "registered_after": len(h.gateway.registry.snapshot()), "worker_open_calls": opens.Load(), "replacement_completed": completed, "semantic_ok": completed, "cleanup_release_is_injected": true})
		})
	}
}
