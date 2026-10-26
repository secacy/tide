# Gateway 接入 Worker Lease：获取、交接与归还

日期：2026-10-27。代码参照 `d2e78a2`。状态：指导完成；后续经开发者明确委托，助手已完成核心接入与[集成验收](2026-10-27-gateway-worker-lease-validation.md)，真实并发对照尚未运行。下文保留实施前的修改顺序与取舍。前置组件已通过[验收](2026-10-27-worker-local-capacity-validation.md)。

## 为什么这一步要一起修改生命周期

本地占用池能原子取得名额，但当前 Gateway 仍调用 Pick；服务还没有使用名额。真正接入后，早归还会使同一 Worker 被重复分配，漏归还则使后端明明已空闲却一直被认为满载。

v1 的占用可跟随一次 session.run；v2 的逻辑会话跨越多次 WebSocket 连接，不能把 Release 放在 ServeV2HTTP 的退出 defer 中，否则握手交接完成、handler 一返回就把仍在识别的名额归还了。

本轮选择在 Gateway 原有资源所有权边界持有 Lease：准备函数暂持，准备成功由 sessionWorker 携带，再由入口失败路径或运行器终止路径归还。继续保留到整场本地清理完成，包括结果保留阶段。代价是计算已结束但结果尚未确认时，名额暂不复用；收益是释放位置与现有清理流程一致。

其他方案：将 Release 分散到 Send/Recv 任务容易早还；用 context.AfterFunc 自动释放只能观察取消，不能证明任务退出；提前在 Worker EOF 释放会引入第二个所有权边界。本轮均不采用。Worker 远端真实名额仍由其自身限制，本地归还不证明远端释放。

## 修改 1：显式适配旧选择器

现有 WorkerLease 字段私有，gateway 包不能直接创建无容量凭证。为旧选择器提供一个用途明确的构造函数，放在 `internal/workerpool/worker_lease.go`；不公开任意归还闭包，也不扩大 CapacityRoundRobin 的职责。

```go
// NewUnreservedLease 包装已经选中的有效 Worker，不占用任何本地容量。
// 仅用于旧 Pick 策略的显式适配；Worker 的有效性由选择器契约保证。
// 每次返回新的凭证，Release 可重复调用且无资源归还动作。
func NewUnreservedLease(worker Worker) *WorkerLease {
    return &WorkerLease{worker: worker, release: func() {}}
}
```

新增 `internal/gateway/worker_allocator.go`：

```go
// WorkerAllocator 为新逻辑会话取得一次本地分配，支持并发调用。
// 成功必须返回有效、非 nil 的 Lease 和 nil error；失败返回 nil Lease。
// 不等待容量、不执行网络 I/O；凭证最终由取得者或明确接管者释放。
type WorkerAllocator interface {
    TryAcquire() (*workerpool.WorkerLease, error)
}

// selectorAllocator 适配既有轮询/加权策略；不提供每 Worker 容量限制。
// selector 已由 Gateway.New 校验非 nil，每次申请只调用一次 Pick。
type selectorAllocator struct {
    selector WorkerSelector
}

func (a selectorAllocator) TryAcquire() (*workerpool.WorkerLease, error) {
    return workerpool.NewUnreservedLease(a.selector.Pick()), nil
}

// isNilWorkerAllocator 识别 nil 接口和动态 nil；构造校验不消耗名额。
func isNilWorkerAllocator(allocator WorkerAllocator) bool
```

nil 校验沿用 `isNilWorkerSelector` 的反射范围，不通过调用 TryAcquire 判断有效性。不要将容量池塞进 WorkerSelector 的 Pick：全满时不能返回一个有效 Worker，旧接口没有表达失败的能力。

## 修改 2：两种构造入口，共用一套内部状态

在 `handler.go` 中将 Gateway.pool 改为 `allocator WorkerAllocator`，只保留这一种运行时依赖。构造函数关系：

```go
// New 保留既有选择语义；校验 ctx 和 selector 后，包装 selectorAllocator。
func New(ctx context.Context, pool WorkerSelector, cfg Config) (*Gateway, error)

// NewWithAllocator 显式启用带获取/归还语义的分配器。
// 校验 ctx 和 allocator；构造不调用 TryAcquire，不持有名额。
func NewWithAllocator(ctx context.Context, allocator WorkerAllocator, cfg Config) (*Gateway, error)

// newGateway 承接原 New 的配置归一化、tracker/gate/registry 构造。
// 两个公开入口已校验 ctx 与依赖；不得复制两份配置默认值逻辑。
func newGateway(ctx context.Context, allocator WorkerAllocator, cfg Config) (*Gateway, error)
```

`New` 的调用者和 cmd/gateway 现有装配仍可编译，继续保持旧策略语义。只有使用 NewWithAllocator 的实例才启用每 Worker 本地占用限制；本步不新增 CLI 配置和默认策略迁移。

## 修改 3：v1 会话完整退出后归还

在 `session.go` 中把 session.pool 改成 allocator；`newSession` 的第二个参数改为 WorkerAllocator；ServeHTTP 传入 `g.allocator`。保留合法 start 之后才申请的时机。

原 `selected := s.pool.Pick()` 改为 TryAcquire。流程顺序：

1. RPC/WS context 仍按现有方式创建；合法 start 之前不占 Worker 名额。
2. TryAcquire 失败时按 resultWorkerFailed 调用现有 finish、返回包装后的错误，不发 RPC。若应用 ctx 已取消，保持 resultServerStopping 优先；不要新增“识别完成”分支。
3. 成功后立刻建立归还 defer，再从 lease.Worker() 取得 selected 和 workerID。原 RPC 创建、双向任务及判定保持原逻辑。
4. 因 context 的 defer 已较早注册，单独 `defer lease.Release()` 会先于它们执行。明确写出取消在前、归还在后的兜底顺序：

```go
// 正常运行已在返回前 finish + wg.Wait；这里只兜底取消并归还。
// 不用这个 defer 代替已启动任务的 Wait。
defer func() {
    cancelRPC()
    cancelWS()
    lease.Release()
}()
```

5. 取得 Lease 后、建流之前再观察 ctx 取消，通过既有停服收尾返回；避免已取消请求仍发起不必要的 RPC。建流失败也走 finish，然后由 defer 归还。

已启动 upload/download 的路径必须继续执行 `finish → wg.Wait → return`。不要在 finish 中归还，因为 finish 只负责触发关闭和取消，wg.Wait 才确认本地任务退出。Worker 远端释放不在这个 Wait 的保证范围内。

## 修改 4：v2 准备成功才交出 Lease

在 sessionWorker 上增加字段和小方法；它属于 Gateway 装配资源，不是协议协调状态，不放入 sessionWorkerConfig，也不参与输入/输出状态机。

```go
// lease 由 Gateway.prepareV2Worker 成功准备后绑定；纯部件夹具允许 nil。
// 仅 Gateway 的准备/运行所有者访问，不由 uploader/receiver/连接任务修改。
lease *workerpool.WorkerLease

// releaseLease 由当前 Gateway 所有者在本地资源清理后调用。
// nil 表示纯部件夹具没有申请名额；不执行 stopIO/waitIO/releaseBuffers。
func (w *sessionWorker) releaseLease() {
    if w.lease != nil {
        w.lease.Release()
    }
}
```

不清空 lease 指针，保持装配后只读；重复 Release 由凭证自己保证幂等。这里只处理名额，不把等待和归还偷偷塞进 releaseBuffers。

`prepareV2Worker` 的所有权从以下规则实现：

1. 保留 entryCtx 预检查、RPC context 与入口取消转发。用 `g.allocator.TryAcquire()` 代替 Pick；全满或其他申请错误用 `fmt.Errorf("%w: %w", errV2WorkerUnavailable, acquireErr)` 包装，同时保留 errors.Is 身份。失败时取消本函数创建的 RPC context，不建流。
2. 成功申请后，用 `transferred := false` 管理局部所有权。成功/失败所有返回前都停止入口取消转发并等待已启动的回调；失败必须先取消 RPC，再归还 Lease。
3. 申请完成后及建流返回后均检查 entryCtx/rpcCtx，保留原有取消优先级。TryAcquire 成功不免除随后取消的处理责任。
4. 用 lease.Worker() 建立唯一原 RPC；保留 openErr 分类和 newSessionWorker 校验。任何失败都触发本地回滚，不能创建第二条 RPC 重试。
5. newSessionWorker 成功后赋值 `worker.lease = lease`，设置 transferred=true，再返回 worker。此时没有启动 I/O goroutine；调用方 startV2 接管未运行的 worker。

推荐把失败回滚集中在一个 defer 中，避免分支漏掉。将 prepareV2Worker 改为命名返回 `(prepared *sessionWorker, err error)`，让 defer 使用最终返回的 err；已有取消原因不会被 cancelRPC 覆盖。取得 Lease 后注册：

```go
// 从取得 Lease 到交接之间由准备函数负责。
defer func() {
    stopForwarding() // 等待已经开始的入口取消转发。
    if !transferred {
        cancelRPC(err)
        lease.Release()
    }
}()
```

因尚未调用 startIO，准备失败不能调用 waitIO；其 done 通道永远不会由未启动任务关闭。建流本身已同步返回，失败取消的是原 RPC，不关闭共享 ClientConn。原生远端可能继续清理，这是既定边界。

## 修改 5：v2 两个最终归还位置

**提交前失败：**在 startV2 已有 `!committed` defer 内，若 worker 非 nil，顺序为：

```text
worker.stopIO(err)
worker.releaseBuffers()
worker.releaseLease()
g.tracker.leave()
```

此时 worker 的 uploader/receiver 尚未启动，不调用 waitIO。候选 WebSocket 仍属于 ServeV2HTTP，由其原有失败路径处理；这个 socket 的失败清理有独立握手预算，名额只在取消已准备 RPC、释放 worker 本地缓冲后归还。

**提交后运行退出：**将 runV2Session 的尾部改为：

```go
// runWithConnection 在所有已启动任务退出、缓冲释放后返回。
_ = s.runWithConnection(g.ctx, time.Now, worker, initial)
g.registry.remove(s.identity.id, s)
worker.releaseLease()
g.tracker.leave()
```

顺序保证 Gateway 活跃数归零/Wait 返回前，已先归还其拥有的 Worker 本地名额。runWithConnection 初始化连接失败也会清理接管资源并返回，因此同样由该尾部归还。

resumeV2、普通 detach、连接重建、结果进入 retaining 都不申请或释放 Lease；运行中的同一逻辑会话持续拥有它。不要把释放绑定到某代 WebSocket，也不要修改 runWithWorker 的纯组件接口去强制它依赖本地池。

## 修改 6：兼容旧测试与后续验收

由于内部字段与 newSession 参数改变，少数既有测试夹具需要机械适配：

- 调用 `newSession(ws, selector, cfg)` 的旧测试，把 selector 包成 `selectorAllocator{selector: selector}`。
- 直接访问 `g.pool` 的夹具改用 `g.allocator`；直接给 `s.pool` 赋值的夹具改为给 `s.allocator` 赋显式适配器。
- 许多测试里的 `f.pool` 是自己持有的计数选择器或轮询实例，仍用于验证 Pick 次数/顺序，不应全文替换。
- 现有纯部件 sessionWorker 没有 Lease，允许 nil，原测试不需要凭空申请容量。

这部分由助手在开发者提交核心实现后调整并补新测试。开发者可先用 `go build ./...` 检查生产代码；测试因旧夹具字段暂未迁移的编译错误不代表核心方案失败。新测试将覆盖：

| 场景 | 必须证明 |
| --- | --- |
| 构造入口 | nil/动态 nil 被拒绝；不调用 Pick/TryAcquire；旧入口维持原策略 |
| 池满但 Gateway 总名额未满 | v2 返回 worker_unavailable，原错误可内部辨认，不建立 Worker RPC |
| Gateway 总名额已满 | 保持 session_limit，不能用池满覆盖该原因 |
| 建流失败/提交失败 | 已取得名额归还，取消发生在归还前，后续请求可再取得 |
| v1/v2 正常、异常、停服 | 本地任务仍阻塞清理时名额不归还；真正退出后可复用 |
| v2 detach/resume/retaining | 始终同一 Lease、同一 RPC；不新增申请、不提前归还 |
| L/S/N 错峰 | B 完成并归还后，N 可使用 B，A 原会话不受影响 |
| 本地与远端清理间隙 | 本地可复用时远端仍可能忙；不能假报完成或透明换 Worker 重试 |

本步写好后助手进行代码评审、旧夹具迁移和 race 回归，随后才实现已制定的真实两 Worker 对照。这里没有新增远端预约、队列、处理进度协议或容量实测结论。
