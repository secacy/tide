package gateway

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// readConnectionReadyNetwork 显式验证本代第一条应用消息和完整状态。
// 不跳过未知消息，不把包含凭据的原始报文输出到测试日志。
func readConnectionReadyNetwork(t *testing.T, ctx context.Context, client *websocket.Conn, f *workerCoordinatorFixture, generation, offset, acked uint64, ended bool) {
	t.Helper()
	typ, data, err := client.Read(ctx)
	if err != nil {
		t.Fatal("read ready", err)
	}
	assertReadyMessage(t, decodeReadyMessage(t, typ, data), f, generation, offset, acked, ended)
}

// readAudioAckNetwork 消费并检查预期累计确认，不跳过其他应用消息。
func readAudioAckNetwork(t *testing.T, ctx context.Context, client *websocket.Conn, generation, offset uint64, ended bool) {
	t.Helper()
	typ, data, err := client.Read(ctx)
	if err != nil {
		t.Fatal("read audio ACK", err)
	}
	decodeAudioAckMessage(t, typ, data, generation, offset, ended)
}

func TestConnectionReadyWebSocketResumeAfterEndWithAppliedResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	legacy1, client1, _ := newLegacyResultWriteFixture(t, time.Second, false)
	legacy2, client2, _ := newLegacyResultWriteFixture(t, time.Second, false)
	server1, server2 := newManagedNetworkConn(legacy1.ws), newManagedNetworkConn(legacy2.ws)
	var uploads, halfCloses atomic.Int32
	f, feed := newDuplexWorkerFixture(t, func(_ context.Context, req *asrv1.StreamingRecognizeRequest) error {
		if string(req.Data) != "ab" {
			t.Error("original Worker received unexpected audio")
		}
		uploads.Add(1)
		return nil
	}, func(context.Context) error { halfCloses.Add(1); return nil })
	startManagedFixture(t, f, server1)
	waitManagedRead(t, ctx, server1, 1)
	readConnectionReadyNetwork(t, ctx, client1, f, 1, 0, 0, false)
	writeManagedInput(t, ctx, client1, server1, websocket.MessageBinary, readerAudioFrame(0, "ab"), 2)
	readAudioAckNetwork(t, ctx, client1, 1, 2, false)
	writeManagedInput(t, ctx, client1, server1, websocket.MessageText, []byte(`{"type":"end","finalOffset":"2"}`), 3)
	readAudioAckNetwork(t, ctx, client1, 1, 2, true)
	select {
	case <-f.worker.uploader.done:
	case <-ctx.Done():
		t.Fatal("original input did not half-close")
	}
	first := wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1, SegmentID: "s", Text: "first"}
	tail := wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 2, SegmentID: "s", Text: "tail", IsFinal: true}
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "first"}})
	readSequencedNetworkResult(t, ctx, client1, first)
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "tail", IsFinal: true}})
	readSequencedNetworkResult(t, ctx, client1, tail)
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{err: io.EOF})
	select {
	case <-f.rpcCtx.Done(): // 此处为正常 Worker 完成释放 RPC，逻辑会话继续 retaining。
	case <-ctx.Done():
		t.Fatal("normal Worker completion not observed")
	}
	if err := client1.CloseNow(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server1.closed:
	case <-ctx.Done():
		t.Fatal("first owner cleanup did not finish")
	}
	// 客户端只连续应用了第一条；提交接管时一起确认，第二条仍需重放。
	if generation := resumeManagedNetwork(t, ctx, f.session, server2, 1); generation != 2 {
		t.Fatal(generation)
	}
	waitManagedRead(t, ctx, server2, 1)
	readConnectionReadyNetwork(t, ctx, client2, f, 2, 2, 1, true)
	readSequencedNetworkResult(t, ctx, client2, tail) // ready 之后紧邻的必须是 seq=2。
	writeManagedInput(t, ctx, client2, server2, websocket.MessageText, []byte(`{"type":"end","finalOffset":"2"}`), 2)
	writeManagedInput(t, ctx, client2, server2, websocket.MessageText, []byte(`{"type":"result_ack","seq":"2"}`), 3)
	snapshot, err := f.session.requestConnectionReady(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertReadySnapshot(t, snapshot, f, 2, 2, 2, true)
	if uploads.Load() != 1 || halfCloses.Load() != 1 {
		t.Fatal("recovery repeated original Worker input or CloseSend", uploads.Load(), halfCloses.Load())
	}
	readCompletionNetwork(t, ctx, client2, 2, 2, 2)
	if err := client2.Write(ctx, websocket.MessageText, []byte(`{"type":"completed_ack","finalOffset":"2","lastSeq":"2"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.finished:
	case <-ctx.Done():
		t.Fatal("managed cleanup did not finish")
	}
	if !f.worker.completion.acknowledged || f.err != nil || f.worker.input != nil || f.worker.results != nil || server1.closes.Load() != 1 || server2.closes.Load() != 1 {
		t.Fatal("recovered retaining session did not release owned resources", f.err)
	}
}
