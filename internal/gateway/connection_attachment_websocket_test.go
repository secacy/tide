package gateway

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// managedNetworkConn 观察真正 socket 的清理与读取边界，不替换 I/O。
type managedNetworkConn struct {
	*websocket.Conn
	readStarted chan int
	readCount   int
	closes      atomic.Int32
	closed      chan struct{}
	closeOnce   sync.Once
}

func (c *managedNetworkConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	c.readCount++
	c.readStarted <- c.readCount
	return c.Conn.Read(ctx)
}
func (c *managedNetworkConn) CloseNow() error {
	c.closes.Add(1)
	err := c.Conn.CloseNow()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}
func newManagedNetworkConn(conn *websocket.Conn) *managedNetworkConn {
	return &managedNetworkConn{Conn: conn, readStarted: make(chan int, 32), closed: make(chan struct{})}
}
func waitManagedRead(t *testing.T, ctx context.Context, conn *managedNetworkConn, want int) {
	t.Helper()
	for {
		select {
		case n := <-conn.readStarted:
			if n == want {
				return
			}
			if n > want {
				t.Fatal("read advanced unexpectedly", n, want)
			}
		case <-ctx.Done():
			t.Fatal("managed reader did not reach next Read", context.Cause(ctx))
		}
	}
}
func writeManagedInput(t *testing.T, ctx context.Context, client *websocket.Conn, server *managedNetworkConn, typ websocket.MessageType, data []byte, next int) {
	t.Helper()
	if err := client.Write(ctx, typ, data); err != nil {
		t.Fatal(err)
	}
	waitManagedRead(t, ctx, server, next)
}

// 每次内部重试使用新的单次候选；未接管 socket 始终由测试调用者持有。
// 重试总期限是测试兜底，不用于测量产品恢复时间。
func resumeManagedNetwork(t *testing.T, ctx context.Context, s *resumableSession, conn sessionConnection, applied uint64) uint64 {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		g, err := s.requestResumeConnection(ctx, newAttachmentCandidate(t, conn), applied)
		if err == nil {
			return g
		}
		if !errors.Is(err, errConnectionRetiring) && !errors.Is(err, errResumeAlreadyAttached) {
			t.Fatal("network attachment rejected", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("network retirement did not finish", context.Cause(ctx))
		}
	}
}

func TestConnectionAttachmentWebSocketResumeOriginalWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	legacy1, client1, _ := newLegacyResultWriteFixture(t, time.Second, false)
	legacy2, client2, _ := newLegacyResultWriteFixture(t, time.Second, false)
	server1, server2 := newManagedNetworkConn(legacy1.ws), newManagedNetworkConn(legacy2.ws)
	uploads := make(chan string, 8)
	f, feed := newDuplexWorkerFixture(t, func(_ context.Context, req *asrv1.StreamingRecognizeRequest) error {
		uploads <- string(req.Data)
		return nil
	}, nil)
	originalResume := f.session.resume // 构造约定保持 resume 指针不变，提交应覆盖其值。
	startManagedFixture(t, f, server1)
	waitManagedRead(t, ctx, server1, 1)
	readConnectionReadyNetwork(t, ctx, client1, f, 1, 0, 0, false)
	writeManagedInput(t, ctx, client1, server1, websocket.MessageBinary, readerAudioFrame(0, "ab"), 2)
	writeManagedInput(t, ctx, client1, server1, websocket.MessageBinary, readerAudioFrame(0, "ab"), 3)
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "first"}})
	first := wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1, SegmentID: "s", Text: "first"}
	readSequencedNetworkResult(t, ctx, client1, first)
	// 客户端实际收到结果，但未发送 ACK，随后丢失第一条连接。
	if err := client1.CloseNow(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server1.closed:
	case <-ctx.Done():
		t.Fatal("old connection was not closed by owner")
	}
	if context.Cause(f.rpcCtx) != nil {
		t.Fatal("disconnect canceled original Worker")
	}
	handshakeCtx, cancelHandshake := context.WithCancel(ctx)
	if g := resumeManagedNetwork(t, handshakeCtx, f.session, server2, 0); g != 2 {
		t.Fatal(g)
	}
	cancelHandshake() // 接纳之后取消请求，不能取消已经转交的连接。
	waitManagedRead(t, ctx, server2, 1)
	readConnectionReadyNetwork(t, ctx, client2, f, 2, 2, 0, false)
	readSequencedNetworkResult(t, ctx, client2, first)
	writeManagedInput(t, ctx, client2, server2, websocket.MessageText, []byte(`{"type":"result_ack","seq":"1"}`), 2)
	writeManagedInput(t, ctx, client2, server2, websocket.MessageBinary, readerAudioFrame(0, "ab"), 3)
	writeManagedInput(t, ctx, client2, server2, websocket.MessageBinary, readerAudioFrame(2, "cd"), 4)
	writeManagedInput(t, ctx, client2, server2, websocket.MessageText, []byte(`{"type":"end","finalOffset":"4"}`), 5)
	select {
	case <-f.worker.uploader.done:
	case <-ctx.Done():
		t.Fatal("original Worker input did not finish")
	}
	if len(uploads) != 2 || <-uploads != "ab" || <-uploads != "cd" {
		t.Fatal("audio replay duplicated or crossed original Worker stream")
	}
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{response: &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "tail", IsFinal: true}})
	readSequencedNetworkResult(t, ctx, client2, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 2, SegmentID: "s", Text: "tail", IsFinal: true})
	sendWriterNetworkStep(t, ctx, feed, workerReadStep{err: io.EOF})
	select {
	case <-f.rpcCtx.Done():
	case <-ctx.Done():
		t.Fatal("normal Worker completion not observed")
	}
	writeManagedInput(t, ctx, client2, server2, websocket.MessageText, []byte(`{"type":"result_ack","seq":"2"}`), 6)
	if err := f.session.requestClose(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.finished:
	case <-ctx.Done():
		t.Fatal("managed runner cleanup did not finish")
	}
	if f.err != nil || f.session.resume != originalResume || server1.closes.Load() != 1 || server2.closes.Load() != 1 || f.worker.input != nil || f.worker.results != nil {
		t.Fatal("final owned resources or outcome incorrect", f.err)
	}
	select {
	case <-server2.closed:
	default:
		t.Fatal("runner returned before actual second CloseNow")
	}
}

func TestConnectionAttachmentWebSocketProtocolFailureTerminates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	legacy, client, _ := newLegacyResultWriteFixture(t, time.Second, false)
	server := newManagedNetworkConn(legacy.ws)
	f, _ := newDuplexWorkerFixture(t, nil, nil)
	startManagedFixture(t, f, server)
	waitManagedRead(t, ctx, server, 1)
	readConnectionReadyNetwork(t, ctx, client, f, 1, 0, 0, false)
	if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.finished:
	case <-ctx.Done():
		t.Fatal("protocol failure did not clean up logical session")
	}
	if !errors.Is(f.err, wsprotocol.ErrInvalidV2Input) || context.Cause(f.rpcCtx) == nil || server.closes.Load() != 1 || f.worker.input != nil || f.worker.results != nil {
		t.Fatal("protocol failure was treated as recoverable detach", f.err)
	}
}
