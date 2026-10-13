package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// attemptWindowNetworkEvidence 只记录可核对的业务与清理事实，不包含恢复凭据。
// 由子测试主 goroutine 填业务字段；非并行子测试 Cleanup 后父测试补 TCP 关闭事实。
type attemptWindowNetworkEvidence struct {
	Case                 string `json:"case"`
	Cuts                 int    `json:"cuts"`
	MaxAttempts          int    `json:"max_attempts"`
	AttemptWindowNS      int64  `json:"attempt_window_ns"`
	Attempts             int    `json:"attempts"`
	Resumed              int    `json:"resumed"`
	DroppedReady         int    `json:"dropped_ready"`
	Captured             uint64 `json:"captured_bytes"`
	Processed            uint64 `json:"processed_bytes"`
	Planned              uint64 `json:"planned_bytes"`
	Applied              uint64 `json:"applied_results"`
	Completed            bool   `json:"completed"`
	CompletionAckWritten bool   `json:"completion_ack_written"`
	Exhausted            bool   `json:"exhausted"`
	OriginalRPCs         int32  `json:"original_rpcs"`
	WorkerPicks          int32  `json:"worker_picks"`
	ProxyConnections     int32  `json:"proxy_connections"`
	SourceCloses         int32  `json:"source_closes"`
	NaturallyIdle        bool   `json:"naturally_idle"`
	ClientConnClosed     bool   `json:"client_conn_closed"`
	ServeExited          bool   `json:"serve_exited"`
}

// TestV2ClientAttemptWindowGateway 验收生产客户端→真实 WS→Gateway→TCP gRPC 的组合。
// 音频由屏障控制，分散场景额外保持健康传输时间；不据包耗时推断恢复性能。
func TestV2ClientAttemptWindowGateway(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		budget, cuts, attempts, resumed int
		attemptWindow, healthyGap       time.Duration
		dropReady, complete             bool
	}{
		{name: "spaced_recovery_completes", budget: 1, cuts: 3, attempts: 3, resumed: 3, attemptWindow: 300 * time.Millisecond, healthyGap: 600 * time.Millisecond, complete: true},
		{name: "dense_success_exhausted", budget: 2, cuts: 3, attempts: 2, resumed: 2, attemptWindow: 30 * time.Second},
		{name: "lost_ready_attempts_exhausted", budget: 2, cuts: 1, attempts: 2, resumed: 0, attemptWindow: 30 * time.Second, dropReady: true},
	} {
		var tcp baselineTCPCleanup
		evidence := attemptWindowNetworkEvidence{Case: tc.name, Cuts: tc.cuts, MaxAttempts: tc.budget, AttemptWindowNS: int64(tc.attemptWindow)}
		passed := t.Run(tc.name, func(t *testing.T) {
			worker := &repeatedRecoveryWorker{done: make(chan struct{})}
			f := newV2EntryFixture(t, newBaselineTCPWorkerClientTracked(t, worker, &tcp), Config{MaxSessions: 1, V2: &V2Config{ResumeWindow: time.Second, ResultRetentionTimeout: time.Second}})
			proxy := newRepeatedRecoveryProxyOptions(t, f, tc.cuts, tc.dropReady)
			pr, pw := io.Pipe()
			t.Cleanup(func() { _ = pw.Close(); _ = pr.Close() })
			reader := &repeatedRecoveryReader{reader: pr}
			source := &clientGatewaySource{reader: reader, close: pr.Close}
			applied := make(chan wsprotocol.SequencedResultMessage, 16)
			client, err := wsclient.NewV2(wsclient.V2Config{URL: proxy.url, ChunkBytes: 4, Resume: &wsclient.V2ResumeConfig{MaxAttempts: tc.budget, AttemptWindow: tc.attemptWindow, Window: 2 * time.Second}, OnResult: func(ctx context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
				select {
				case applied <- r:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				result wsclient.V2RunResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() { result, err := client.Run(f.ctx, source); done <- outcome{result, err} }()
			input := []byte("aabbccddeeffgghhiijjkkllmmnnoopp")
			written := 0
			seen := uint64(0)
			writeChunk := func() {
				t.Helper()
				if n, err := pw.Write(input[written : written+4]); err != nil || n != 4 {
					t.Fatalf("source write n=%d err=%v", n, err)
				}
				written += 4
			}
			applyChunk := func() {
				t.Helper()
				r := repeatedTake(t, f.ctx, applied, "fresh audio result")
				if r.Seq != seen+1 || r.Text != string(input[seen*4:(seen+1)*4]) || r.IsFinal {
					t.Fatalf("result mismatch at seq %d", seen+1)
				}
				seen++
			}
			current := repeatedTake(t, f.ctx, proxy.attempts, "initial start")
			if current.request.Kind != wsprotocol.V2HandshakeStart {
				t.Fatal("did not start once")
			}
			close(current.permit)
			identity := repeatedTake(t, f.ctx, current.ready, "initial ready")
			if identity.Generation != 1 || identity.NextOffset != 0 || identity.AckedResultSeq != 0 || identity.InputEnded {
				t.Fatal("initial positions")
			}
			writeChunk()
			applyChunk()
			for round := 1; round <= tc.cuts; round++ {
				if round > 1 && tc.healthyGap > 0 {
					// 上轮两个新结果已经应用，证明本地 ready 已接纳；保持连接期间旧额度自然过期。
					timer := time.NewTimer(tc.healthyGap)
					select {
					case <-timer.C:
					case <-f.ctx.Done():
						timer.Stop()
						t.Fatal("healthy interval exceeded test deadline")
					}
				}
				writeChunk()
				lost := repeatedTake(t, f.ctx, current.lost, "dropped audio")
				if lost.Offset != uint64(written-4) || !bytes.Equal(lost.Payload, input[written-4:written]) {
					t.Fatal("wrong cut audio")
				}
				repeatedTake(t, f.ctx, current.done, "old proxy exit")
				f.waitReturn(t)
				if !tc.complete && !tc.dropReady && round > tc.budget {
					break
				}
				waitInstanceDetached(t, f, wsprotocol.ReadyMessage{SessionID: identity.SessionID, Generation: uint64(round)})
				next := repeatedTake(t, f.ctx, proxy.attempts, "resume attempt")
				// 同一个源仍在未放行握手期间读取新音频。
				writeChunk()
				if tc.dropReady {
					for attempt := 1; attempt <= tc.attempts; attempt++ {
						if next.request.Kind != wsprotocol.V2HandshakeResume || next.request.SessionID != identity.SessionID || next.request.ResumeToken != identity.ResumeToken || next.request.AppliedSeq != seen {
							t.Fatal("lost-ready request changed identity/position")
						}
						close(next.permit)
						dropped := repeatedTake(t, f.ctx, next.droppedReady, "Gateway ready dropped before client")
						if dropped.Generation != uint64(attempt+1) || dropped.SessionID != identity.SessionID || dropped.ResumeToken != identity.ResumeToken || dropped.NextOffset != lost.Offset || dropped.AckedResultSeq != seen || dropped.InputEnded {
							t.Fatal("lost-ready Gateway positions")
						}
						evidence.DroppedReady++
						repeatedTake(t, f.ctx, next.done, "failed handshake proxy exit")
						f.waitReturn(t)
						if attempt < tc.attempts {
							waitInstanceDetached(t, f, dropped)
							next = repeatedTake(t, f.ctx, proxy.attempts, "next failed recovery")
						}
					}
					break
				}
				if next.request.Kind != wsprotocol.V2HandshakeResume || next.request.SessionID != identity.SessionID || next.request.ResumeToken != identity.ResumeToken || next.request.AppliedSeq != seen {
					t.Fatal("resume identity/position changed")
				}
				close(next.permit)
				ready := repeatedTake(t, f.ctx, next.ready, "resumed ready")
				if ready.SessionID != identity.SessionID || ready.ResumeToken != identity.ResumeToken || ready.Generation != uint64(round+1) || ready.NextOffset != lost.Offset || ready.AckedResultSeq != seen || ready.InputEnded {
					t.Fatal("resumed positions/generation")
				}
				applyChunk()
				applyChunk()
				current = next
			}
			if tc.complete {
				writeChunk()
				applyChunk()
				_ = pw.Close()
				tail := repeatedTake(t, f.ctx, applied, "tail")
				if tail.Seq != seen+1 || !tail.IsFinal || tail.Text != "complete" {
					t.Fatal("tail missing/duplicated")
				}
				seen++
			}
			out := repeatedTake(t, f.ctx, done, "client Run exit")
			if out.result.Completed != tc.complete || out.result.CompletionAckWritten != tc.complete || out.result.ResumeAttempts != tc.attempts || out.result.ResumedConnections != tc.resumed || out.result.AppliedSeq != seen {
				t.Fatalf("out=%+v err=%v", out.result, out.err)
			}
			if tc.complete {
				if out.err != nil || out.result.FinalOffset != uint64(written) {
					t.Fatalf("completion=%+v err=%v", out.result, out.err)
				}
				repeatedTake(t, f.ctx, current.done, "final proxy exit")
				f.waitReturn(t)
			} else if !errors.Is(out.err, wsclient.ErrV2RecoveryExhausted) || errors.Is(out.err, context.DeadlineExceeded) {
				t.Fatalf("expected attempt exhaustion: %v", out.err)
			}
			repeatedTake(t, f.ctx, worker.done, "natural Worker exit")
			v2RequireIdle(t, f)
			if worker.calls.Load() != 1 || worker.active.Load() != 0 || f.pool.picks.Load() != 1 || proxy.count.Load() != int32(tc.attempts+1) || proxy.active.Load() != 0 || source.closes.Load() != 1 || reader.bytes.Load() != uint64(written) {
				t.Fatal("RPC/source/connection ownership changed")
			}
			select {
			case <-proxy.attempts:
				t.Fatal("extra Dial after exhaustion/completion")
			default:
			}
			select {
			case <-applied:
				t.Fatal("duplicate result application")
			default:
			}
			worker.mu.Lock()
			actual, processed, ended := bytes.Clone(worker.audio), worker.processed, worker.ended
			worker.mu.Unlock()
			wantCaptured, wantProcessed, wantApplied := uint64(32), uint64(32), uint64(9)
			if !tc.complete {
				wantCaptured, wantProcessed, wantApplied = 24, 20, 5
			}
			if tc.dropReady {
				wantCaptured, wantProcessed, wantApplied = 12, 4, 1
			}
			if uint64(written) != wantCaptured || processed != wantProcessed || seen != wantApplied || !bytes.Equal(actual, input[:wantProcessed]) || ended != tc.complete {
				t.Fatalf("input/result mismatch captured=%d processed=%d applied=%d ended=%t", written, processed, seen, ended)
			}
			evidence.Attempts = out.result.ResumeAttempts
			evidence.Resumed = out.result.ResumedConnections
			evidence.Captured = reader.bytes.Load()
			evidence.Processed = processed
			evidence.Planned = uint64(len(input))
			evidence.Applied = seen
			evidence.Completed = out.result.Completed
			evidence.CompletionAckWritten = out.result.CompletionAckWritten
			evidence.Exhausted = errors.Is(out.err, wsclient.ErrV2RecoveryExhausted)
			evidence.OriginalRPCs = worker.calls.Load()
			evidence.WorkerPicks = f.pool.picks.Load()
			evidence.ProxyConnections = proxy.count.Load()
			evidence.SourceCloses = source.closes.Load()
			evidence.NaturallyIdle = true
		})
		// 子测试返回前已执行自己的全部 Cleanup；底层 ClientConn 和 Serve 也必须实际退出。
		if passed {
			if !tcp.ClientClosed || !tcp.ServeExited {
				t.Fatalf("%s TCP fixture not closed", tc.name)
			}
			evidence.ClientConnClosed = tcp.ClientClosed
			evidence.ServeExited = tcp.ServeExited
			data, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("ATTEMPT_WINDOW_NETWORK %s", data)
		}
	}
}
