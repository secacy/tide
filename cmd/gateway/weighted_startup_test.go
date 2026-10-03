package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"
)

// testWeightedGatewayExecutable 使用 CLI 测试构建的 race 可执行程序，验证真实参数到路由与信号退出。
// 只操作本测试创建的进程；后端保持运行，连接关闭在后端清理之前观察。
func testWeightedGatewayExecutable(t *testing.T, binary string) {
	t.Helper()
	reservation, err := net.Listen("tcp", gatewayAddr)
	if err != nil {
		t.Skipf("startup integration requires free %s: %v", gatewayAddr, err)
	}
	reservation.Close()
	addrA, a := startStartupBackend(t, "A")
	addrB, b := startStartupBackend(t, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output := &startupLogSignal{writer: io.Discard, ready: make(chan struct{})}
	command := exec.CommandContext(ctx, binary, "-workers="+addrB+","+addrA, "-worker-strategy=weighted_round_robin", "-worker-weights=2,1", "-max-pending-audio-bytes=32000")
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var processErr error
	go func() { processErr = command.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("owned Gateway process did not exit")
			}
		}
	})
	select {
	case <-output.ready:
	case <-done:
		t.Fatalf("Gateway exited before startup: %v\n%s", processErr, output.output())
	case <-ctx.Done():
		t.Fatalf("Gateway startup timed out: %s", output.output())
	}
	logs := output.output()
	if !strings.Contains(logs, "strategy=weighted_round_robin") || !strings.Contains(logs, "workers=\"["+addrB+" "+addrA+"]\"") || !strings.Contains(logs, "weights=\"[2 1]\"") || strings.Contains(logs, "!BADKEY") {
		t.Fatalf("actual CLI configuration log differs: %s", logs)
	}
	if a.opened.Load() != 0 || b.opened.Load() != 0 {
		t.Fatal("Gateway connected before first RPC")
	}
	backends := []*startupBackend{b, a}
	for i, index := range []int{0, 1, 0, 0, 1, 0} {
		backend := backends[index]
		conn, _, err := websocket.Dial(ctx, "ws://127.0.0.1:8080/v1/asr", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.CloseNow() })
		startupWrite(t, ctx, conn, websocket.MessageText, `{"type":"start","version":"v1"}`)
		for _, part := range []string{"first", "second"} {
			data := fmt.Sprintf("cli-%d:%s", i, part)
			startupWrite(t, ctx, conn, websocket.MessageBinary, data)
			startupResult(t, ctx, conn, backend.id, data, false)
		}
		startupWrite(t, ctx, conn, websocket.MessageText, `{"type":"end"}`)
		startupResult(t, ctx, conn, backend.id, "tail", true)
		_, _, err = conn.Read(ctx)
		if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
			t.Fatalf("normal close: %v", err)
		}
		startupExit(t, ctx, backend, codes.OK)
	}
	if b.streams.Load() != 4 || a.streams.Load() != 2 || a.opened.Load() != 1 || b.opened.Load() != 1 || a.closed.Load() != 0 || b.closed.Load() != 0 {
		t.Fatalf("before shutdown A streams/open/closed=%d/%d/%d B=%d/%d/%d", a.streams.Load(), a.opened.Load(), a.closed.Load(), b.streams.Load(), b.opened.Load(), b.closed.Load())
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		if processErr != nil {
			t.Fatalf("Gateway signal exit failed: %v\n%s", processErr, output.output())
		}
	case <-ctx.Done():
		t.Fatalf("Gateway failed to exit: %s", output.output())
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for a.closed.Load() != 1 || b.closed.Load() != 1 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("connection cleanup incomplete: A=%d B=%d", a.closed.Load(), b.closed.Load())
		}
	}
	t.Log("race executable: routing B,A,B,B,A,B; completed=6; SIGTERM exit=0; each shared gRPC connection opened=1 closed=1 before backend shutdown")
}
