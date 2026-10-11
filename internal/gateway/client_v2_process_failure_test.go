//go:build unix

package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// instanceProcessSpec 只传测试角色、监听地址和私有观测目录，不携带会话凭据。
type instanceProcessSpec struct {
	Role          string
	ListenAddress string
	WorkerAddress string
	Directory     string
	HoldSecond    bool
}

// instanceProcessReady 表示子进程已绑定监听器；业务可用性由实际请求另外验证。
type instanceProcessReady struct {
	Role    string
	Address string
	PID     int
}

// instanceProcessRecord 保存测试 Worker 实际收到/处理的输入，held 文件是故障前事实。
// 被 SIGKILL 的 Worker 不承诺写出最终记录；不能用 held 伪造退出后的内部状态。
type instanceProcessRecord struct {
	PID       int
	RPC       int32
	Audio     string
	Processed uint64
	Ended     bool
	Exited    bool
	Active    int32
}

// instanceProcessState 是存活 Gateway 的测试快照，不包含恢复身份。
type instanceProcessState struct {
	ActiveSessions int
	Handshakes     int
	Registered     int
	Picks          int32
	Stopping       bool
}

// writeInstanceProcessJSON 用同目录临时文件及 rename 发布完整记录。
// 这里只保证测试读取完整 JSON，不宣称文件具备生产持久恢复语义。
func writeInstanceProcessJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err == nil {
		err = os.WriteFile(path+".tmp", data, 0600)
	}
	if err == nil {
		err = os.Rename(path+".tmp", path)
	}
	if err != nil {
		t.Errorf("publish process observation: %v", err)
	}
}

// instanceProcessWorker 每条 RPC 独立计算；可将第一条 RPC 的第二块暂停至取消。
type instanceProcessWorker struct {
	asrv1.UnimplementedASRServiceServer
	t      *testing.T
	spec   instanceProcessSpec
	calls  atomic.Int32
	active atomic.Int32
}

// StreamingRecognize 在 held 处停止处理；正常流发送进度、结果和尾部结果。
func (w *instanceProcessWorker) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	r := instanceProcessRecord{PID: os.Getpid(), RPC: w.calls.Add(1)}
	w.active.Add(1)
	defer func() {
		r.Exited = true
		r.Active = w.active.Add(-1)
		writeInstanceProcessJSON(w.t, filepath.Join(w.spec.Directory, "worker-exit.json"), r)
	}()
	chunks := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			r.Ended = true
			return stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "tail", Text: "complete", IsFinal: true})
		}
		if err != nil {
			return err
		}
		chunks++
		r.Audio += string(req.Data)
		if w.spec.HoldSecond && r.RPC == 1 && chunks == 2 {
			r.Active = w.active.Load()
			writeInstanceProcessJSON(w.t, filepath.Join(w.spec.Directory, "worker-held.json"), r)
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		r.Processed += uint64(len(req.Data))
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: r.Processed}}); err != nil {
			return err
		}
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "audio", Text: string(req.Data)}); err != nil {
			return err
		}
	}
}

// TestV2InstanceProcessHelper 是当前测试二进制的私有子进程入口。
// 正常包回归跳过；父测试通过特定环境与精确 -test.run 激活，最终使用 SIGKILL。
func TestV2InstanceProcessHelper(t *testing.T) {
	if os.Getenv("TIDE_INSTANCE_PROCESS_HELPER") != "1" {
		t.Skip("private process helper")
	}
	var spec instanceProcessSpec
	if err := json.Unmarshal([]byte(os.Getenv("TIDE_INSTANCE_PROCESS_SPEC")), &spec); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", spec.ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ready := instanceProcessReady{Role: spec.Role, Address: listener.Addr().String(), PID: os.Getpid()}
	if spec.Role == "worker" {
		server := grpc.NewServer(grpc.WaitForHandlers(true))
		defer server.Stop()
		asrv1.RegisterASRServiceServer(server, &instanceProcessWorker{t: t, spec: spec})
		writeInstanceProcessJSON(t, filepath.Join(spec.Directory, "ready.json"), ready)
		t.Fatalf("Worker Serve returned unexpectedly: %v", server.Serve(listener))
	}
	if spec.Role != "gateway" {
		t.Fatalf("unknown helper role %q", spec.Role)
	}
	conn, err := grpc.NewClient(spec.WorkerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	life, cancel := context.WithCancel(context.Background())
	pool := &v2EntrySelector{client: asrv1.NewASRServiceClient(conn)}
	g, err := New(life, pool, Config{MaxSessions: 1, WorkerSendTimeout: 30 * time.Second, V2: &V2Config{ResumeWindow: 30 * time.Second}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() {
		g.StopAccepting()
		cancel()
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := g.Wait(ctx); err != nil {
			t.Errorf("helper Gateway cleanup: %v", err)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/asr", g.ServeV2HTTP)
	mux.HandleFunc("GET /test/state", func(rw http.ResponseWriter, _ *http.Request) {
		snapshot := g.Snapshot()
		g.registry.mu.Lock()
		registered := len(g.registry.entries)
		g.registry.mu.Unlock()
		_ = json.NewEncoder(rw).Encode(instanceProcessState{ActiveSessions: snapshot.ActiveSessions, Handshakes: entryHandshakeCount(g.gate), Registered: registered, Picks: pool.picks.Load(), Stopping: snapshot.Stopping})
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	writeInstanceProcessJSON(t, filepath.Join(spec.Directory, "ready.json"), ready)
	t.Fatalf("Gateway Serve returned unexpectedly: %v", server.Serve(listener))
}

// instanceChildProcess 独占自己启动的 Cmd 及唯一 Wait 任务。
// exitErr 和 ProcessState 仅在 done 关闭后读取；清理不会操作任何其他进程。
type instanceChildProcess struct {
	cmd     *exec.Cmd
	spec    instanceProcessSpec
	ready   instanceProcessReady
	done    chan struct{}
	exitErr error
	client  *http.Client // 私有查询传输，不向全局 HTTP 连接池留下测试连接。
}

// startInstanceChild 在临时目录记录输出和就绪信息，启动同一测试二进制的私有入口。
func startInstanceChild(t *testing.T, ctx context.Context, spec instanceProcessSpec) *instanceChildProcess {
	t.Helper()
	spec.Directory = t.TempDir()
	if spec.ListenAddress == "" {
		spec.ListenAddress = "127.0.0.1:0"
	}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(spec.Directory, "process.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	p := &instanceChildProcess{cmd: exec.Command(os.Args[0], "-test.run=^TestV2InstanceProcessHelper$", "-test.timeout=60s"), spec: spec, done: make(chan struct{}), client: &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}}
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "TIDE_INSTANCE_PROCESS_") {
			p.cmd.Env = append(p.cmd.Env, value)
		}
	}
	p.cmd.Env = append(p.cmd.Env, "TIDE_INSTANCE_PROCESS_HELPER=1", "TIDE_INSTANCE_PROCESS_SPEC="+string(data))
	p.cmd.Stdout, p.cmd.Stderr = log, log
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.exitErr = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
			requireInstanceChildCleanLog(t, p)
			return
		default:
			_ = p.cmd.Process.Kill()
		}
		select {
		case <-p.done:
			requireInstanceChildCleanLog(t, p)
		case <-time.After(5 * time.Second):
			t.Errorf("owned child did not exit: role=%s pid=%d", spec.Role, p.cmd.Process.Pid)
		}
	})
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(spec.Directory, "ready.json"))
		if err == nil {
			if err := json.Unmarshal(data, &p.ready); err != nil {
				t.Fatal(err)
			}
			if p.ready.PID != p.cmd.Process.Pid || p.ready.Role != spec.Role {
				t.Fatal("child ready record did not match the owned process")
			}
			return p
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-p.done:
			output, _ := os.ReadFile(filepath.Join(spec.Directory, "process.log"))
			t.Fatalf("child exited before ready: %v\n%s", p.exitErr, output)
		case <-ctx.Done():
			t.Fatal("child did not publish ready")
		case <-ticker.C:
		}
	}
}

// killInstanceChild 强制终止指定自有子进程并核对 OS 退出原因为 SIGKILL。
func killInstanceChild(t *testing.T, ctx context.Context, p *instanceChildProcess) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("test deadline expired before fault injection: %v", err)
	}
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitInstanceSignal(t, ctx, p.done, "owned child process exit")
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if p.exitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child was not killed by SIGKILL: role=%s err=%v", p.spec.Role, p.exitErr)
	}
	requireInstanceChildCleanLog(t, p)
}

// requireInstanceChildCleanLog 在唯一 Wait 完成后检查子进程日志。
// SIGKILL 会覆盖进程退出码，因此另行检查 race/崩溃报告，不能只检查父测试 exit=0。
func requireInstanceChildCleanLog(t *testing.T, p *instanceChildProcess) {
	t.Helper()
	output, err := os.ReadFile(filepath.Join(p.spec.Directory, "process.log"))
	if err != nil {
		t.Errorf("read owned child log: %v", err)
		return
	}
	for _, marker := range []string{"WARNING: DATA RACE", "panic:", "fatal error:", "--- FAIL:", "publish process observation:"} {
		if bytes.Contains(output, []byte(marker)) {
			t.Errorf("owned child reported an error before exit: role=%s pid=%d\n%s", p.spec.Role, p.ready.PID, output)
			return
		}
	}
}

// readInstanceProcessRecord 等待完整 Worker 事实；不从最后样本推断死亡进程的收尾。
func readInstanceProcessRecord(t *testing.T, ctx context.Context, p *instanceChildProcess, name string) instanceProcessRecord {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(p.spec.Directory, name))
		if err == nil {
			var record instanceProcessRecord
			if err := json.Unmarshal(data, &record); err != nil {
				t.Fatal(err)
			}
			return record
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("missing child observation %s", name)
		}
	}
}

// requireProcessGatewayState 从存活子进程查询快照，直到期望状态实际成立。
func requireProcessGatewayState(t *testing.T, ctx context.Context, p *instanceChildProcess, want instanceProcessState) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+p.ready.Address+"/test/state", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := p.client.Do(request)
		if err != nil {
			t.Fatalf("query live Gateway process: %v", err)
		}
		var state instanceProcessState
		err = json.NewDecoder(response.Body).Decode(&state)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("invalid live Gateway observation: status=%d err=%v", response.StatusCode, err)
		}
		if state == want {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("live Gateway state=%+v want=%+v", state, want)
		}
	}
}

// newProcessParentGateway 创建 Worker 进程故障场景中的存活 Gateway 和真实 HTTP 入口。
// ctx 仅作独立观察保护；Gateway 的运行 context 由自身的 cancel 控制。
func newProcessParentGateway(t *testing.T, ctx context.Context, conn *grpc.ClientConn) *v2EntryFixture {
	t.Helper()
	life, cancel := context.WithCancel(context.Background())
	pool := &v2EntrySelector{client: asrv1.NewASRServiceClient(conn)}
	g, err := New(life, pool, Config{MaxSessions: 1, WorkerSendTimeout: 30 * time.Second, V2: &V2Config{ResumeWindow: 30 * time.Second}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(g.ServeV2HTTP))
	f := &v2EntryFixture{g: g, pool: pool, ctx: ctx, cancel: cancel, url: "ws" + strings.TrimPrefix(server.URL, "http")}
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := g.Wait(cleanupCtx); err != nil {
			t.Errorf("live Gateway fixture cleanup: %v", err)
		}
		server.Close()
	})
	return f
}

func TestV2ClientInstanceProcessFailure(t *testing.T) {
	for _, role := range []string{"worker", "gateway"} {
		t.Run(role, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			t.Cleanup(cancel)
			var old, fresh *instanceChildProcess
			var parent *v2EntryFixture
			var parentWorker *restartableInstanceWorker
			var conn *grpc.ClientConn
			if role == "worker" {
				old = startInstanceChild(t, ctx, instanceProcessSpec{Role: role, HoldSecond: true})
				var err error
				conn, err = grpc.NewClient(old.ready.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = conn.Close() })
				parent = newProcessParentGateway(t, ctx, conn)
			} else {
				parentWorker = newRestartableInstanceWorker(t)
				old = startInstanceChild(t, ctx, instanceProcessSpec{Role: role, WorkerAddress: parentWorker.address})
				parent = &v2EntryFixture{ctx: ctx, url: "ws://" + old.ready.Address}
			}
			proxy := newInstanceFailureProxy(t, parent)
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = writer.Close() })
			source := &clientGatewaySource{reader: reader, close: reader.Close}
			applied := make(chan wsprotocol.SequencedResultMessage, 4)
			client, err := wsclient.NewV2(wsclient.V2Config{URL: proxy.url, ChunkBytes: 4, Resume: &wsclient.V2ResumeConfig{Window: 10 * time.Second}, OnResult: func(ctx context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
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
			returned := make(chan outcome, 1)
			go func() { result, err := client.Run(ctx, source); returned <- outcome{result, err} }()
			if _, err := writer.Write([]byte("aabbccdd")); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-applied:
				if result.Seq != 1 || result.Text != "aabb" || result.IsFinal {
					t.Fatal("unexpected first applied result")
				}
			case <-ctx.Done():
				t.Fatal("first result not applied")
			}
			waitInstanceSignal(t, ctx, proxy.accepted, "eight-byte Gateway ACK forwarded")
			if role == "worker" {
				held := readInstanceProcessRecord(t, ctx, old, "worker-held.json")
				if held.PID != old.ready.PID || held.RPC != 1 || held.Audio != "aabbccdd" || held.Processed != 4 || held.Ended || held.Exited || held.Active != 1 {
					t.Fatal("unexpected pre-kill Worker observation")
				}
			} else {
				waitInstanceSignal(t, ctx, parentWorker.backend.held, "Worker held before Gateway kill")
				requireProcessGatewayState(t, ctx, old, instanceProcessState{ActiveSessions: 1, Registered: 1, Picks: 1})
			}
			killInstanceChild(t, ctx, old)
			waitInstanceSignal(t, ctx, proxy.resumeSeen, "client resume after process death")
			if role == "worker" {
				v2RequireIdle(t, parent)
				if parent.pool.picks.Load() != 1 || parent.g.Snapshot().Stopping {
					t.Fatal("live Gateway restarted the original RPC or stopped itself")
				}
				if _, err := os.Stat(filepath.Join(old.spec.Directory, "worker-exit.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("killed Worker unexpectedly published a normal exit observation")
				}
			} else {
				waitInstanceSignal(t, ctx, parentWorker.backend.firstDone, "live Worker handler exit after remote process death")
				if parentWorker.backend.active.Load() != 0 {
					t.Fatal("live Worker retained original RPC")
				}
				fresh = startInstanceChild(t, ctx, instanceProcessSpec{Role: role, ListenAddress: old.ready.Address, WorkerAddress: parentWorker.address})
				requireProcessGatewayState(t, ctx, fresh, instanceProcessState{})
			}
			close(proxy.allowResume)
			select {
			case out := <-returned:
				var entry *wsclient.V2EntryError
				if !errors.As(out.err, &entry) || entry.Code != wsprotocol.V2ErrorResumeUnavailable || out.result.Completed || out.result.CompletionAckWritten || out.result.AppliedSeq != 1 || out.result.ResumeAttempts != 1 || out.result.ResumedConnections != 0 {
					t.Fatalf("original process-fault result=%+v err=%v", out.result, out.err)
				}
			case <-ctx.Done():
				t.Fatal("original Run did not finish")
			}
			proxy.requireIdle(t, ctx)
			if source.closes.Load() != 1 || len(applied) != 0 {
				t.Fatal("original source leaked or an extra result was applied")
			}
			proxy.mu.Lock()
			requests := append([]wsprotocol.V2HandshakeKind(nil), proxy.requests...)
			proxy.mu.Unlock()
			if len(requests) != 2 || requests[0] != wsprotocol.V2HandshakeStart || requests[1] != wsprotocol.V2HandshakeResume {
				t.Fatal("original client restarted instead of resuming")
			}
			if role == "worker" {
				fresh = startInstanceChild(t, ctx, instanceProcessSpec{Role: role, ListenAddress: old.ready.Address})
				conn.ResetConnectBackoff()
				conn.Connect()
				for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
					if !conn.WaitForStateChange(ctx, state) {
						t.Fatal("live Gateway channel did not reconnect to new Worker process")
					}
				}
			} else {
				requireProcessGatewayState(t, ctx, fresh, instanceProcessState{})
			}
			if old.ready.PID == fresh.ready.PID || old.ready.Address != fresh.ready.Address {
				t.Fatal("replacement process did not have a new PID at the original address")
			}
			newSource := &clientGatewaySource{reader: bytes.NewReader([]byte("11223344"))}
			var results []wsprotocol.SequencedResultMessage
			newClient, err := wsclient.NewV2(wsclient.V2Config{URL: parent.url + "/v2/asr", ChunkBytes: 4, OnResult: func(_ context.Context, r wsprotocol.SequencedResultMessage, _ time.Time) error {
				results = append(results, r)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := newClient.Run(ctx, newSource)
			if err != nil || !result.Completed || !result.CompletionAckWritten || result.FinalOffset != 8 || result.AppliedSeq != 3 || result.ResumeAttempts != 0 || newSource.closes.Load() != 1 {
				t.Fatalf("new process session result=%+v err=%v", result, err)
			}
			if len(results) != 3 || results[0].Seq != 1 || results[0].Text != "1122" || results[1].Seq != 2 || results[1].Text != "3344" || results[2].Seq != 3 || results[2].Text != "complete" || !results[2].IsFinal {
				t.Fatal("new process session results were incomplete or mixed")
			}
			if role == "worker" {
				v2RequireIdle(t, parent)
				if parent.pool.picks.Load() != 2 {
					t.Fatal("unexpected live Gateway Worker selection count")
				}
				record := readInstanceProcessRecord(t, ctx, fresh, "worker-exit.json")
				if record.PID != fresh.ready.PID || record.RPC != 1 || record.Audio != "11223344" || record.Processed != 8 || !record.Ended || !record.Exited || record.Active != 0 {
					t.Fatal("replacement Worker did not independently process and release the new RPC")
				}
			} else {
				requireProcessGatewayState(t, ctx, fresh, instanceProcessState{Picks: 1})
				parentWorker.backend.mu.Lock()
				defer parentWorker.backend.mu.Unlock()
				if len(parentWorker.backend.records) != 2 || parentWorker.backend.active.Load() != 0 {
					t.Fatal("unexpected live Worker RPC count or activity")
				}
				before, after := parentWorker.backend.records[0], parentWorker.backend.records[1]
				if string(before.audio) != "aabbccdd" || before.processed != 4 || before.ended || !before.exited || string(after.audio) != "11223344" || after.processed != 8 || !after.ended || !after.exited {
					t.Fatal("live Worker observations did not distinguish old failure and new completion")
				}
			}
			t.Logf("role=%s old_pid=%d signal=SIGKILL new_pid=%d same_address=true original_completed=false new_completed=true", role, old.ready.PID, fresh.ready.PID)
		})
	}
}
