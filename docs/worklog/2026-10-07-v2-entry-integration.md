# 第六阶段：公开 v2 新建与恢复接线

日期：2026-10-07。状态：开发者随后明确委托助手完成本步，公开实验入口已实现并通过组合验收；空闲保护、错误协议与客户端自动恢复继续待接。下文保留方案依据，末节记录实现与验证事实。

## 能力目标与范围

已完成握手解析、entryGate、注册表、唯一 Worker 运行器、实际连接交接及 completed 确认。下一轮将这些部件接到独立 `/v2/asr`：从公开入口创建会话，断线后用身份和恢复位置接回原会话，最终完成并释放名额。以同一个 Mock RPC 的调用次数、音频记录、结果序号和资源清理为组合证据。

本轮采用显式启用的实验入口。后续一轮补公开链路的输入空闲/断连保护、初始 ready 丢失和错误协议收敛，再接客户端有限重放与自动重连，最后开展故障实验。现有 v2 reader 没有 InputIdleTimeout，不能把本轮组合通过解释为所有长连接保护已经齐全。

## 为什么需要新的入口组织

v1 的 handler 一直等待 session.run 返回，再归还会话名额。可恢复会话在 socket 断开后仍保留原 Worker、结果和恢复资格，因而不能在某条 HTTP 请求结束时释放逻辑名额。

本轮保留两种责任：handler 管一次连接的接入，逻辑运行器管整场会话。新建成功后运行器接管 Worker、连接和逻辑名额；恢复成功只交接新连接。注册表只负责定位，最终恢复资格由协调者判定。

| 方案 | 收益 | 代价与选择 |
| --- | --- | --- |
| 原 v1 handler 同时解释两版协议 | 一个入口 | 占位时机、输入格式和退出责任同时分叉；采用独立方法及路由 |
| 新建 handler 挂到整场退出才返回 | 容易保留原 defer | HTTP 请求、临时握手和逻辑资源责任容易混合；采用短入口加独立运行器 |
| 恢复时重新选择 Worker、建 RPC | 复用新建流程 | 丢失原计算进度，可能重复处理；恢复只查表、认证、提交候选 |
| handler 持 gate 锁等待恢复回复 | 表面上覆盖整个恢复 | 协调者再取 gate 会死锁，慢命令也阻塞其他入口；仅在协调者最终提交处持锁 |
| RPC 直接继承入口超时 context | 建流超时简单 | handler 返回/握手到期会取消已经接管的原 RPC；RPC 继承 Gateway 生命周期，启动时临时转发入口取消 |

## 配置与装配

在 Config 增加 `V2 *V2Config`：nil 表示不启用 v2，非 nil 时 New 复制配置、校验并填默认值。避免运行中继续读取调用者可修改的配置指针。

```go
// V2Config 描述实验 v2 入口的固定预算。
// 各字段零值使用默认值；有符号字段负值非法。
type V2Config struct {
    MaxHandshakes          int           // 尚未完成交接/失败清理的临时连接数。
    EntryTimeout           time.Duration // 从入口取得名额到提交的总操作预算。
    ResumeWindow           time.Duration // 实际断开后的恢复窗口。
    ResultRetentionTimeout time.Duration // Worker 正常完成后的结果保留期限。
    WorkerStatusTimeout    time.Duration // 上传 EOF 后等待 Recv 真实终态的期限。
    MaxAudioBytes          uint64        // 含在途块的音频存储上限。
    MaxAudioChunks         int           // 含在途块的音频条目上限。
    MaxResultBytes         uint64        // 未确认结果字符串字节上限。
    MaxResults             int           // 未确认结果条目上限。
}
```

起始默认值：握手 64、入口 10 秒、恢复窗口 10 秒、结果保留 30 秒、Worker 状态等待 2 秒、音频/结果各 1 MiB 和各 256 条。这些只是可运行的初始预算，不是容量实验结论。复用现有 StartTimeout、WorkerSendTimeout、TailTimeout、ResultWriteTimeout 和 MaxPendingAudioBytes；启用 v2 时 MaxMessageBytes 必须至少为 9。

Gateway 增加 `gate *entryGate`、`registry *sessionRegistry` 和保存归一化配置的私有 `v2 *V2Config`。New 只创建一个 tracker，把同一指针交给 gate；即使未启用 v2，也可用默认握手预算构造 gate，使 StopAccepting/Wait 始终只委托 gate。v1 继续直接申请该 tracker，合计受原 MaxSessions 限制。

`cmd/gateway` 增加默认 false 的 `-enable-v2` 开关，启用时传 `V2: &gateway.V2Config{}`，其余预算本轮先通过 Go 配置供测试设置。routes 增加 `/v2/asr` → `ServeV2HTTP`；未启用时该方法直接 HTTP 404。现有 `/v1/asr` 保持原行为。不能因新增可选配置改变旧命令默认启用范围。

Snapshot.ActiveSessions 仍读共享 tracker：v1 含升级前占位，v2 含合法 start 后占位和断线保留会话；不含 v2 临时握手。更新注释解释口径。

## 建议接口和责任

新增 `internal/gateway/handler_v2.go`；配置可放 `config_v2.go`。

```go
// ServeV2HTTP 管理一次临时连接，完成首条握手后交给新建/恢复分支。
// 成功交接以后不再读写或主动关闭该连接。
func (g *Gateway) ServeV2HTTP(w http.ResponseWriter, r *http.Request)

// startV2 申请逻辑名额、准备原 Worker、登记并启动运行器。
// entryCtx 只限制入口操作；candidate 成功前由 handler 关闭。
// nil 表示 Worker、逻辑名额和连接已交给运行器；错误不转移候选。
func (g *Gateway) startV2(entryCtx context.Context, candidate *connectionCandidate) error

// resumeV2 查找并认证原会话，提交一次候选恢复命令。
// 不 Pick、不建流、不申请逻辑名额；失败不回退新建。
// nil 表示原协调者已经接管 candidate。
func (g *Gateway) resumeV2(entryCtx context.Context, candidate *connectionCandidate, handshake wsprotocol.V2Handshake) error

// prepareV2Worker 同步选择并建立原 RPC，再构造未启动的 sessionWorker。
// RPC 继承 g.ctx；建流期间响应 entryCtx 取消。
// 返回前解除并等待临时取消回调，失败时取消已创建的 RPC。
// 成功后 Worker 尚未启动，由调用者负责取消或交给运行器。
func (g *Gateway) prepareV2Worker(entryCtx context.Context) (*sessionWorker, error)

// runV2Session 只在新建已提交后启动一次。
// 等待 runWithConnection 完成真实清理后，撤销注册并归还逻辑名额。
func (g *Gateway) runV2Session(s *resumableSession, worker *sessionWorker, initial *connectionCandidate)
```

成功转移所有权后的步骤不能再返回普通失败，否则 handler 会错误关闭已被接管的连接。保持准备、提交、运行和回滚的路径可读，避免用多个互相覆盖的 defer 隐藏所有权变化。

## 入口共同流程

1. 未启用返回 404；调用 gate.tryEnterHandshake，满额或停止在升级前返回 503。取得名额后，直到成功交接或者失败清理真正完成才 leaveHandshake。
2. 基于 r.Context 建立一次 EntryTimeout context，并用短取消回调使其响应 g.ctx 取消；退出时解除回调并取消入口 context。最终提交还要检查 g.ctx 的实际取消原因，避免只依赖异步取消回调执行时机。
3. 升级 WebSocket，入口暂时拥有连接。用 owned 标记安排失败时 CloseNow；先完成关闭，再归还握手名额。
4. 调用 readV2Handshake(entryCtx, conn, StartTimeout, MaxMessageBytes)。子读取预算受同一 entryCtx 总期限约束。解析成功后构造 candidate，再按 start/resume 分支执行；首条读取已经返回才能启动 attachment.reader。
5. 分支返回 nil 后取消入口的关闭责任，归还握手名额并返回。ready 由 attachment 的唯一 writer 发，handler 不额外发送 ready，也不等待整场结束。

EntryTimeout 限制新接管的资格及启动操作，不承诺整个 handler 在期限到达的瞬间返回。取消建流后仍须等实际调用退出；失败提示有独立的有界写预算，资源未清理完不得提前减计数。

## 新建：准备完成后才提交

顺序为逻辑名额 → 身份与会话 → Worker → 提交登记 → 启动运行器。

- gate.tryEnterSession 成功后安排失败归还。生产身份使用 crypto/rand.Reader。将共享 gate 绑定到新会话的固定字段 `entryGate *entryGate`，必须在注册/启动前赋值，运行期间不替换。
- prepareV2Worker 只 Pick 一次；RPC context 来自 g.ctx，不来自 entryCtx。构造 sessionWorker 时映射现有四项期限、两组缓冲预算以及未处理音频预算。StreamingRecognize 返回成功只表示流对象已建立，不保证后端已经完成处理。
- 建流期间通过 context.AfterFunc(entryCtx, ...) 转发取消到 cancelRPC。回调结束关闭 done；建流返回后调用 stop，如果回调已开始则在锁外等 done，保证它不会在交接成功后再取消原 RPC。
- 停掉回调后仍要检查 entryCtx/rpcCtx 和最终 gate 提交。回调被解除与提交之间发生入口取消，由 withCommit 拒绝；失败路径取消准备好的 RPC。不能只 stop 回调然后无条件登记。
- gate.withCommit(entryCtx, callback) 内检查 g.ctx/rpcCtx 仍有效并调用 registry.add。登记失败不启动运行器，取消 RPC 并归还逻辑名额；候选连接仍由 handler 关闭。
- 登记成功是新建的所有权提交点。退出锁后无条件启动 `go g.runV2Session(...)`，即使此刻发生取消，也由运行器执行统一清理；这之后 startV2 只能报告已经提交的成功。已有逻辑名额确保停服等待不会在运行器尚未启动时提前完成。
- runV2Session 调用 s.runWithConnection(g.ctx, time.Now, worker, initial)，返回后依次 registry.remove(id, s)、tracker.leave。controlDone 关闭时不能提前做这两件事。

启动前失败不需要等待尚未启动的 uploader/receiver，但必须完成启动调用及取消回调的等待；不关闭共享 gRPC ClientConn。任何已经启动的任务都由唯一运行器等待。

## 恢复：最终检查放进协调者

resumeV2 精确 lookup ID 并 matchesResumeToken。不存在或认证失败统一返回新的 `errResumeUnavailable`，错误文本不含 token/原始报文。认证成功后调用 requestResumeConnection(entryCtx, candidate, appliedSeq)，等待明确回复；命令已交付后不能自行超时返回并猜测候选是否被接管。

禁止在 handler 中写 `gate.withCommit(ctx, func() error { return requestResumeConnection(...) })`。命令等待在锁外，实际提交锁由协调者取得。

修改 runCoordinator 的 controlResumeConnection 分支：保留旧连接收割、恢复范围预检、副本预演和 attachment 准备。把最终有效性检查与正式状态修改包进本地 commit 闭包，再通过 s.entryGate.withCommit(cmd.ctx, commit) 调用。nil entryGate 仅保留已有部件测试/内部运行模式；公开创建的所有会话必须绑定共享 gate。

commit 内依次执行：

1. 获得锁以后重新调用 stopCause，检查逻辑/RPC 取消以及当前绝对期限。之前预演过的状态不能代替这次检查，锁等待期间可能到期。
2. 确认请求仍有效（withCommit 已检查 context.Cause）；在任何所有权转移前完成可能失败的校验。
3. acknowledgeResult，提交已验证的 resume 副本，重置结果投递，安装 connections.current 并重置本代输出字段。该协调段没有处理其他命令，因此旧范围预检仍有效；时间有效性必须通过上一步重新确认。

回调失败时只取消未安装 attachment 的子 context，不关闭候选 socket。区分拒绝本次请求与整场终止：gate 停止、入口取消只拒绝候选；stopCause 确认的逻辑取消/业务到期或内部不变量错误要退出协调循环。可以用单独的 terminalErr 记录，避免按错误文本猜测。

成功后在锁外启动 attachment.run、调整计时器并回复成功。即使刚发生停服/取消，也必须启动并完成已接管资源的清理，不能回到失败分支。检查提交失败时 ACK、generation、恢复期限、原 Worker 和连接所有权均未被错误修改。

## 失败输出与当前边界

升级前使用 HTTP 404/503。升级后仅在入口仍拥有连接时，复用 ErrorMessage 发送固定文本，再 CloseNow。提示写入用 ResultWriteTimeout 的独立收尾预算；服务已取消或明确传输断开时直接清理。不要复用已超时的 entryCtx 导致提示必然立即失败，也不要为了发提示无限等待。

固定分类：非法握手 `invalid handshake`；入口/首条超时 `entry timeout`；会话满额 `session limit exceeded`；停服 `service is stopping`；无会话/错凭据/恢复已结束 `resume unavailable`；原连接尚在或仍退出 `session busy`；恢复位置非法 `invalid resume position`；建流失败 `worker unavailable`；其他内部错误 `internal error`。本轮不新增完整错误码协议，后续客户端重试前再收敛机器可读错误语义。

成功交接后任何 ready、业务输出或失败通知都属于运行器，handler 不再写 socket。初始 ready 丢失时客户端可能没有身份，不能承诺找回该会话；旧会话只按断开后的窗口释放。无数据且无法观察到断开的连接仍需下一轮输入空闲保护，因此实验开关默认关闭。

## 实现顺序与验收

建议按以下顺序写同一组改动，每个阶段保持可编译：配置与装配 → 新建及运行器 → 恢复入口与协调者最终提交 → 固定失败分类和路由。不要把这一组扩展为新的调度策略或客户端缓存实现。

测试由助手完成，验收必须穿过真实公开 handler：

- start → ready → 音频/end → result/completed → completed_ack；注册表移除、逻辑名额归零。ready 身份与注册表对象一致。
- 首连接断开，再用原 ID/token 接回；第二代 ready 在先，原结果重放，历史音频不重复上传，Mock 记录 Pick=1、RPC=1。
- MaxSessions=1 已满时原会话恢复成功，第二场新建拒绝；临时握手超额和静默握手分别受数量/时间限制。
- 不存在的 ID、错误 token、错误结果位置、旧连接未退出和已过期均拒绝；拒绝不建 Worker、不偷偷新建会话、不消费额外逻辑名额。
- 建流受控阻塞时入口取消能传到 RPC；实际建流未返回前不归还名额；成功入口返回后原 RPC 仍活着，停服取消后最终清理。
- 恢复准备后等待 gate 时发生停服或期限到达，候选被拒绝，原状态不被误提交；先提交成功则仅运行器拥有连接并负责清理。
- 注册失败、建流失败、首条非法/超限、ready 写失败均不遗留名额；检查关闭次数和真正任务退出。
- v1/v2 混合接入共用上限，StopAccepting 与 Wait 同时覆盖握手和逻辑会话；实验入口默认不启用，旧入口回归通过。

结果记录实际网络场景、资源计数、唯一 RPC 与去重事实。测试场景数量仍只描述覆盖规模，不作为恢复成功率或生产容量。

## 实现与验收（2026-10-07）

按开发者明确委托实现：新增 config_v2.go、handler_v2.go，接入 Gateway 构造器、共享 tracker/gate、注册表、运行器以及协调者最终恢复提交。命令行 `-enable-v2` 显式启用 `/v2/asr`，默认禁用返回 404。新建成功后原 RPC 不继承 HTTP 请求取消，恢复不重新申请逻辑名额或选择 Worker；唯一 attachment.writer 输出 ready。

入口取消在建流期间临时转发到原 RPC，停止转发时等待已经开始的回调；提交失败取消 RPC 并保留实际失败原因。新建登记成功后无条件启动运行器，运行器实际清理完成才 remove/leave。恢复在获得 gate 锁后重新检查绝对期限/生命周期，再提交结果确认和新代连接；候选拒绝与整场终止分别处理。

新增 **12 顶层/37 叶级**全部通过，其中包含真实公开 handler/WebSocket 与 TCP gRPC 组合，也有同步点控制的建流和提交替身。全项目 `go test -race -p 1 ./...` 共 **613 顶层/2322 叶级：2310 通过、0 失败、12 显式实验跳过**，无数据竞争报告。

| 证据 | 实际结果 |
| --- | --- |
| 正常公开链路 | start→ready→音频/end→结果/completed→completed_ack 后，握手/逻辑计数归零、注册表为空；handler 提前返回后连接和 RPC 仍可工作 |
| 满额公开恢复 | MaxSessions=1 时第二场 start 明确拒绝；原连接断开后凭原身份接回，generation=2、nextOffset=2，未确认 seq=1 重放；最后正常完成 |
| 唯一计算与音频去重 | 正常及恢复两条完整链路各为 Pick=1、真实后端 RPC=1；恢复重发 offset=0 的 ab 后，后端实际音频仍仅 ab、cd 两块；尾部结果 seq=3 |
| 身份与资格拒绝 | 旧版本握手、未知 ID、错 token、仍附着、ACK 越界与过期均明确拒绝，不回退新建；拒绝恢复不额外 Pick，不提前释放原逻辑名额 |
| 双层预算 | MaxHandshakes=1 时静默连接占握手但逻辑数=0，第二条连接升级前 503；首条读取超时后名额可复用。v1 占位阻止 v2 新建，v2 占位阻止 v1 升级，合计上限为 1 |
| 启动取消与实际返回 | 100ms 入口预算到期以及 Gateway 取消均能传给受控建流 RPC；建流仍被阻塞时握手/逻辑各为 1，调用真正返回后才归零；建流错误输出固定文本，不泄露后端细节 |
| 最终提交竞争 | 受控协调者在准备后停止、取得 gate 时到期、等待锁时取消均拒绝候选；候选无读写/关闭，generation 保持 1；业务到期退出整场，单候选拒绝不取消原 Worker |
| 实际共同清理 | 已进入 Recv 的任务在观察到取消后仍被阻塞，controlDone 已关闭但注册仍在、逻辑数=1；20ms 等待返回期限错误，实际 Recv 返回后 Wait 成功且登记撤销 |
| 路由与失败清理 | 默认/显式 false 禁用，显式 true 接到真实 handler；无升级头返回 426。二进制首条、消息超限、升级失败和建流失败不遗留名额 |

这里的 100ms/20ms 是正确性测试设置，非恢复或性能测量。gate 竞争测试使用真实协调者和受控候选，没有把这些内部场景写成网络故障恢复时间。

首次公开网络定向验证 12 叶级通过，扩展边界验证 28 叶级通过。首次全项目回归为 2308 通过、2 失败、12 跳过；两项失败都是测试将无 WebSocket 升级头的响应预期写为 400，当前 coder/websocket 实际返回 426。修正断言，并让新建回滚保留实际取消原因后完整复测通过。之后给实际清理测试增加 Recv 已进入的同步屏障，避免任务启动时序影响测试证据，并定向复验。

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -p 1 ./... -count=1 -timeout=90s -json
```

原始 JSON 本地保留于 `/private/tmp/tide-v2-entry-first.jsonl`、`tide-v2-entry-boundaries.jsonl`、`tide-v2-entry-full.jsonl`、`tide-v2-entry-accepted.jsonl`，清理屏障复验于 `tide-v2-entry-cleanup.jsonl`。本轮没有新增容量、生产恢复成功率或延迟结论。

后续边界：v2 已附着 reader 尚无输入空闲期限；机器可读错误码、初始 ready 丢失的客户端处理以及有界终态查询尚未完成。读取超时/消息超限可能由 WebSocket 库先关闭连接，ErrorMessage 仅 best effort；客户端不能假设所有拒绝都有应用错误帧。随机身份冲突由现有 registry.add 单元测试覆盖，本轮未注入随机碰撞做公开启动回滚实验；ready 写失败及候选共同关闭的受控证据沿用既有 attachment/ready 测试。下一轮收敛公开链路保护及这些协议边界，再接有限重放客户端和故障实验。
