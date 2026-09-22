# 单次 Worker 发送等待期限：实现指导

状态：辅助函数及 Gateway 接入已实现并验证。关联：[改造前停读实验](../../experiments/stalled-worker.md)、[改造后自主清理结果](../../experiments/worker-send-timeout.md)。以下实现步骤保留设计过程，当前完成情况以验证记录为准。

## 问题与本步目标

Worker 停读实验已复现 Gateway Send 持续等待。客户端断开后约两秒内，三个实验会话均未自主退出；服务取消则能够解除发送等待。

本步先给单次 Send 设置可配置的等待期限。到期取消该会话 RPC，使发送与接收退出，再执行会话清理。目标是限制发送阻塞，不是证明已实现断开即时检测、模型处理期限或端到端实时性。

## 方案取舍

最初显式比较的是单次发送期限、有界队列和全 RPC 固定期限。根据开发者要求，本次补充会话进展监测及计时实现方式的比较，区分原有选择与后续扩展分析。

### 第一层：解决什么问题

| 方案 | 收益 | 代价和限制 | 本次判断 |
| --- | --- | --- | --- |
| 单次 Send 超时后取消本会话 RPC | 针对已复现的等待，复用取消与清理；不改变现有串行转发 | 可能终止只是暂时变慢的会话；不能限制 Send 快速返回后的识别落后 | 当前先实施，阈值后续通过配置与实验校准 |
| 独立读取 + 有界队列 + 独立发送 | 能吸收短暂处理抖动，队列未满时可继续读取客户端 | 增加排队和资源；满队列时仍要选择等待、丢弃或终止；继续等待可能再次停止读取 | 后续按抖动与实时性需求评估，不能单独替代等待期限 |
| 会话进展监测：持续无进展超过阈值后结束 | 可覆盖发送停滞，以及发送成功但 Worker 没有推进的情形 | 必须定义进展。没有 partial 不一定是故障，Send 成功也不是识别完成；需要可解释的进度信号和计时规则 | 暂不引入整套监测，先解决已定位的 Send 等待 |
| 整条 RPC 设置固定 deadline | 生命周期上限直观，实现简单 | 按总会话时长结束，可能截断仍健康运行的长问诊；设得很长则不能及时处理早期停滞 | 适合未来最大会话时长策略，不替代本次发送期限 |

这些方案可以组合，并非互斥。选择单次 Send 期限，是依据停读实验已经证明“对应 RPC 的取消可以解除当前等待”，将自动触发条件接到已有清理路径。不是因为队列或进展监测普遍较差。

### 第二层：如何实现单次发送期限

| 实现方式 | 优点 | 代价 / 正确性要求 |
| --- | --- | --- |
| 同步 Send + AfterFunc 到期取消 RPC | 保留现有发送者；正常发送不需要额外启动一个发送 goroutine | 每块分配计时器和完成通道；必须处理 Stop 与回调完成的竞争。尚未测量这部分开销 |
| 新 goroutine 执行 Send，调用者 select 结果或超时 | select 容易表达多个完成条件 | 每块有发送 goroutine 调度；超时后仍必须取消实际 RPC 并等待发送 goroutine 退出，不能只返回超时。正确实现同样可行 |
| 每会话一个监测 goroutine，根据发送开始/结束事件维护计时器 | 可将多次发送的监测集中管理，有复用计时器的空间 | 需要处理事件顺序、发送代次和监测 goroutine 退出；迟到的超时事件不能误取消下一次发送 |

当前优先采用同步 Send + AfterFunc，是为了保留发送所有权，以较小的控制改动验证退出语义。没有 benchmark 支持它在所有负载下性能最优；若以后证据显示每块计时器分配成为瓶颈，再评估复用或集中监测。

仅创建一个与实际 RPC 无取消关联的局部 WithTimeout 无法影响已经存在的 Send。如果将局部超时正确桥接到 RPC 取消，也可以实现相同语义，但仍需要处理取消来源和监测任务的收尾。

## 原理与并发约束

本地 grpc-go v1.83.2 的 Send 使用创建 stream 时的 context，不能给已存在 stream 的每次 Send 另传一个 context。

保持 Send 在原上传 goroutine 中同步调用。使用 `time.AfterFunc` 为本次调用启动计时，到期以 `ErrWorkerSendTimeout` 为原因取消 RPC。计时回调不调用 Send 或 CloseSend。

RPC 计划使用 `context.WithCancelCause`，让 upload 和 download 不论谁先报告退出，协调者都能检查此次取消的业务原因。不能只根据 Send 返回的 io.EOF 或 Recv 返回的 Canceled 分类。

`Timer.Stop()` 返回 false 不表示计时回调已经执行完毕。因此每次发送有独立的回调完成信号；Stop 未能阻止回调时，必须等待回调结束，之后才读取取消原因并返回。否则旧计时器可能在下一次发送开始后取消整个 RPC。

边界采用明确的竞争规则：Send 返回后若成功停止计时器，则本次未由该计时器触发超时；若计时回调已经启动，则等待它完成，以 RPC 已记录的取消原因为准。该规则不是对物理时间先后的精确追溯。服务取消与其他取消原因仍需要统一协调。

## 当前实现任务：只编写发送辅助函数

新建 `internal/gateway/send.go`：

```go
// ErrWorkerSendTimeout 表示单次向 Worker 发送音频的等待超过期限。
var ErrWorkerSendTimeout = errors.New("worker send timeout")

// sendWithTimeout 在原 goroutine 中发送一块音频，并在等待超时后取消整个 RPC。
// rpcCtx 和 cancelRPC 必须来自创建该 stream 的同一组 WithCancelCause。
// timeout 必须为正；本函数不设置默认值。
// stream 必须响应 RPC context 取消，否则不能保证 Send 退出。
// 本函数不允许与该 stream 的另一次 Send 或 CloseSend 并发调用。
func sendWithTimeout(
    rpcCtx context.Context,
    cancelRPC context.CancelCauseFunc,
    stream workerStream,
    request *asrv1.StreamingRecognizeRequest,
    timeout time.Duration,
) error
```

步骤：

1. 非正 timeout 返回配置错误，不启动计时器或 Send；进入前已取消则返回 `context.Cause(rpcCtx)`。
2. 创建只由计时回调关闭的完成通道。
3. 启动 AfterFunc：先 `cancelRPC(ErrWorkerSendTimeout)`，再关闭完成通道。
4. 同步调用 `stream.Send(request)`，保存原始错误。
5. 停止计时器；如果 Stop 返回 false，等待回调完成通道。成功停止时不能等待该通道，因为回调不会执行。
6. 若 RPC 已取消，返回 `context.Cause(rpcCtx)`；否则原样返回 Send 错误，包括 io.EOF。正常返回后不能再有未完成的计时回调。

本小步先不修改配置、构造函数、upload 和会话结果判定，避免尚未测试的计时逻辑直接影响现有路径。开发者实现辅助函数后，由助手补测试。

### 当前验证结果

开发者已实现 [send.go](../../../internal/gateway/send.go)，助手补充 [send_test.go](../../../internal/gateway/send_test.go)。配置非法分支已与真实发送超时分开，不再包装 `ErrWorkerSendTimeout`。

已通过：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
go test -race ./internal/gateway -run '^TestSendWithTimeout' -v -count=1
```

验证包括成功/EOF/普通错误保留、旧计时器不取消下一次发送、期限取消 RPC 后等待底层发送返回、外部取消原因保留、非法参数不发送，以及回调已启动但尚未完成时辅助函数继续等待。测试使用虚拟时钟，时间断言只证明控制语义，不是实际延迟指标。

## 后续接入计划

- 添加 WorkerSendTimeout 配置，规定默认值、拒绝负值，并传入会话；具体默认值与实验设置接入时确定。
- 将创建 stream 的 context 改为 WithCancelCause，向发送路径传递匹配的 context 和取消函数。
- 上传路径调用辅助函数；协调者增加独立发送超时结果，优先保留服务停止，再检查 RPC 发送超时原因，不依赖谁先发送退出事件。
- 超时收尾取消 RPC，尝试以失败关闭码通知客户端，再结束 WebSocket 操作并等待 goroutine 退出。关闭握手耗时单独计量，不宣称整个清理在 Send 期限内完成。
- 不终止共享 gRPC ClientConn，不重试结果不明确的音频块。

### 当前指导：接入一次完整的超时退出流程

状态：以下接入修改已由开发者完成，旧测试已适配，并新增配置、原因判定、上传及整条关闭路径的验证。

本次比较三种原因传递方式：

| 方式 | 优点 | 代价与限制 |
| --- | --- | --- |
| 仅 upload 返回发送超时事件 | 改动小 | download 可能先报告 RPC 取消，协调者会丢失真正原因 |
| 独立超时事件或共享标记 | 能显式表达超时 | 新事件需要协调先后和发送容量，共享标记需要额外同步及与取消的一致性 |
| RPC context 保存取消原因，协调者检查 Cause | 原因与 RPC 取消一起记录，两个方向都可观察；复用现有集中收尾 | 必须匹配创建 stream 的 context，并明确判定优先级 |

选第三种：局部 RPC context 与取消函数都由 `run` 创建，显式传给发送路径与协调者，不另存一套会话超时状态。统一判定顺序为：协调时已观察到服务停止 → RPC 原因为发送超时 → 输入读取超时 → 原始 I/O 事件。该顺序是业务规则，不声称还原并发事件绝对发生顺序。

具体改动：

1. `Config.WorkerSendTimeout`：0 使用开发阶段默认值 2 秒，负值拒绝，正值采用用户配置。2 秒是初始保护参数，不是从已有数据推导的最佳阈值或最终业务 SLA；后续对比正常负载、短暂抖动与故障数据后调整。已有正常配置不变，零值不关闭保护。
2. `session.workerSendTimeout` 保存正值；`newSession` 增加最后一个明确的 duration 参数，Gateway 传入归一化后的值。本步沿用现有构造方式；如果后续配置继续增长，再单独评估配置结构体，避免此次同时重构构造 API。
3. `run` 使用 `rpcCtx, cancelRPCWithCause := context.WithCancelCause(ctx)` 建流。定义 `cancelRPC := func() { cancelRPCWithCause(nil) }`，让已有 `finish` 和 defer 保持普通 CancelFunc 调用方式。发送路径接收原始带原因的取消函数；不改变独立 wsCtx 的职责。
4. `upload` 显式接收 wsCtx、rpcCtx、CancelCauseFunc、stream、inputEnded。读取仍使用 wsCtx，发送调用 `sendWithTimeout`。新增 `resultWorkerSendTimeout`，在普通错误及 EOF 分支之前检查 `errors.Is(err, ErrWorkerSendTimeout)`，避免进入等待最终 EOF 的旧分支。
5. `waitSessionResult` 增加 rpcCtx 参数；在服务停止判断之后检查 `context.Cause(rpcCtx)` 是否为发送超时。继续等待原有 I/O 事件或服务停止信号；已经验证实际 gRPC RPC 取消能够使 I/O 退出并报告事件。
6. `finish` 增加独立超时分支：取消 RPC、尝试关闭 WebSocket（1011，原因 `worker send timeout`）、取消 WebSocket I/O，并返回关闭错误（如有）。内部 goroutine 仍由 `run` 的 wg.Wait 等待。断开或拒绝关闭握手时可能有额外收尾时间，不把 2 秒说成全会话释放硬上限。

上传的取消错误只是观察到的结果，协调者才决定最终会话分类；不在 upload 中发送关闭帧。WorkerSendTimeout 只作用于音频 Send，不覆盖 start、CloseSend、结果 Recv 或模型处理时间。

测试调用点的构造与函数参数适配、新增配置/判定测试和故障实验由助手负责。开发者本步只修改生产代码；参数变更后旧测试可能暂时无法编译，不通过可变参数、隐藏默认值等方式绕过测试适配。

## 验证与量化计划

辅助函数测试：快速成功不会迟到取消下一次发送；超时确实取消底层 RPC 并等待 Send 返回；外部取消保留原因；原始 EOF 不被误判；期限竞争不遗留回调；参数非法不执行发送。优先使用虚拟时钟。

接入后复用停读负载，增加“客户端保持连接且不主动取消”的场景，并再次验证客户端断开。记录单次 Send 开始、超时取消、Send 返回、Worker 退出、handler 返回与是否需要测试兜底。

目标是由“不主动退出、依赖测试取消”变为“配置期限触发发送超时后自主清理”。时间从阻塞 Send 开始计算；不能将其写成客户端断开后的即时检测，也不能把三次测量范围写成所有环境的服务时限。

保留原始故障记录。现有实验等待 Send/Write 超过一秒才触发动作，新实验必须核对这个观察条件与配置期限的关系，避免超时先发生却被错误记录成未复现。
