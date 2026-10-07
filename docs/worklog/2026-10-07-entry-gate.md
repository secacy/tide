# 第六阶段：公开入口的准入与停服提交边界

日期：2026-10-07。状态：按开发者明确委托完成实现与测试，独立入口协调部件已验收；公开入口仍待接线。

## 能力目标与实施顺序

首条 start/resume 解析和限时读取已验收。公开 /v2/asr 接线需要共同处理临时连接预算、共享会话名额、注册表、原 Worker 与候选接管。本轮先实现具体的 entryGate，完成入口准入和停服提交边界；随后直接组合 handler、会话运行器和路由，以真实网络的新建/恢复验证这些部件。

本轮新增 internal/gateway/entry_gate.go。测试由助手补充。完成时只能标记入口协调部件通过，不能标记公开路由已启用。后续入口接线还须明确操作总预算、建流取消与 RPC 生命周期脱钩、初始 ready 丢失、输入空闲保护及错误响应语义，不能仅靠本部件宣称公开恢复完整。

## 两个具体问题

### 满额时已有会话仍应能够尝试恢复

假设逻辑会话上限 64，其中一场断线后继续保留。此时仍是 64 场，恢复连接可以进入有限的握手阶段，并接回原会话；它不申请第 65 个逻辑名额。新建第 65 场则必须拒绝。

握手连接也要有独立上限。只限制逻辑会话数，无法限制尚未证明身份的连接；只限制握手数量，也无法限制断线后保留的原 Worker 与缓冲。

复用同一 sessionTracker 计数：v1 继续保持当前的升级前占位；v2 新建在合法 start 后申请该 tracker 名额，直到逻辑会话实际清理完成后归还。v2 恢复不重复申请。两个版本共享现有 MaxSessions 总预算，不为 v2 再复制一份相同上限。Snapshot.ActiveSessions 后续接线时须说明 v1/v2 占位时点不同；握手数单独观测。

### 停服不能只在查表前检查一次

可能发生：恢复请求通过入口检查 → 查到会话 → StopAccepting → 协调者准备接管候选。如果最后没有共同的提交边界，停服检查就过时了。

entryGate.withCommit 与 stopAccepting 使用同一把互斥锁。先取得提交资格的短事务完整结束，stopAccepting 才返回；先提交停止状态，则后续提交不再执行。运行器对已接管资源仍按现有流程清理。这个保证只适用于未来真正通过 withCommit 执行的最后状态提交，不能拿入口提前调用一次 withCommit 替代协调者提交处的检查。

## 方案与权衡

| 方案 | 收益 | 代价及选择 |
| --- | --- | --- |
| 新建/恢复都走当前 tracker.tryEnter | 简单复用 | 满额会挡住原会话恢复，且重复占位；不采用 |
| 只为握手使用 channel 信号量 | 计数直观 | 仍需定义停止、提交及共同退出的原子关系；采用一个小状态对象集中管理 |
| 只用原子 bool 或取消 context 检查停服 | 检查轻量 | 检查与实际接管之间仍有窗口；最终提交与停止使用同一短锁 |
| 给 v2 新建另一份逻辑 tracker | 两版本代码独立 | 两份 MaxSessions 合计超过既有 Gateway 总预算；复用已有 tracker |
| 将升级、读取、建流、恢复命令全过程放在锁内 | 流程容易串行理解 | 慢连接会阻塞其他接入及停服；锁只保护计数和本地提交 |

这是控制路径的短锁。音频转发、结果发送和 Worker I/O 不经过它。暂不做分片锁或复杂调度；后续用并发接入实验判断是否产生可测的锁竞争。

## 类型和接口

```go
var (
    errInvalidEntryGateConfig = errors.New("invalid entry gate config")
    errHandshakeLimit         = errors.New("handshake limit reached")
)

// entryGate 管理 v2 临时握手预算，并串行化最终接管与停服。
// tracker 与 Gateway 原 tracker 为同一对象，共享逻辑会话预算。
// 构造后以指针使用，不得复制；方法允许并发调用。
type entryGate struct {
    mu               sync.Mutex     // 保护以下握手/停止状态及最终提交。
    tracker          *sessionTracker // 共享对象；内部状态仍由 tracker 自己的锁保护。
    maxHandshakes    int            // 临时握手上限，构造后不变。
    activeHandshakes int            // 已占名额、尚未交接或完成失败清理的入口数量。
    stopping         bool           // 永久停止新握手、新建占位和最终接管。
    handshakesDrained chan struct{}  // 停止且握手计数归零时关闭一次。
}

// newEntryGate 必须在 Gateway 开始接入前调用，tracker 须由构造器创建。
// maxHandshakes 必须为正；nil tracker/非法上限返回 nil 和配置错误。
// 不修改 tracker，不启动任务，不消耗任何名额。
func newEntryGate(tracker *sessionTracker, maxHandshakes int) (*entryGate, error)

// tryEnterHandshake 为一次 v2 入口操作占临时名额，满额立即返回 errHandshakeLimit。
// 停止时返回 errGatewayStopping；失败不改变计数，不申请逻辑会话名额。
func (g *entryGate) tryEnterHandshake() error

// leaveHandshake 归还一次成功申请的临时名额。
// 仅在候选已确定移交，或入口拥有的资源实际清理完成后调用。
// 每次申请恰好配对一次；计数为零还调用属于编程错误，应 panic。
func (g *entryGate) leaveHandshake()

// tryEnterSession 仅用于 v2 新建：在停止检查下调用共享 tracker.tryEnter。
// 调用方已经取得握手名额并完成合法 start 解析。
// 不消耗第二个握手名额，也不开始建流；满额保留 errSessionLimit。
// 成功后最终拥有者必须在清理完成后对应 tracker.leave 一次。
func (g *entryGate) tryEnterSession() error

// withCommit 在与停服互斥的短临界区执行最终本地提交。
// ctx 为这次入口/附着操作的生命周期；commit 为调用方提供的短提交函数。
// 调用方在提交结果确定前持续持有临时握手名额。
// nil ctx/commit 属于编程错误，应 panic。
// 停止或请求已取消时不执行 commit；其他情况原样返回 commit 的结果。
// commit 不能做 I/O、等待任务或重新调用 entryGate 方法。
func (g *entryGate) withCommit(ctx context.Context, commit func() error) error

// stopAccepting 永久禁止新握手、新建占位和最终提交，并停止共享 tracker 接入。
// 可以重复调用；不取消已有连接/RPC，不等待任务清理。
func (g *entryGate) stopAccepting()

// wait 等待握手与共享 tracker 两类资源都清理完成。
// 调用方先 stopAccepting；ctx 仅限制本次等待，取消等待不改变计数/所有权。
func (g *entryGate) wait(ctx context.Context) error
```

## 每个方法如何组织

1. 构造器校验依赖和上限，创建 handshakesDrained，握手计数与停止标记初始为零值。
2. tryEnterHandshake 持 mu：先检查 stopping，再检查 activeHandshakes >= maxHandshakes；通过才递增。
3. leaveHandshake 持 mu：零计数 panic，否则递减；stopping 且减到零时关闭 handshakesDrained。
4. tryEnterSession 持 mu：停止时拒绝，其他情况返回 tracker.tryEnter()。不复制 tracker.active，也不替调用者自动归还。
5. withCommit 参数校验后持 mu：先检查 stopping，再检查 context.Cause(ctx)，最后执行 commit 并原样返回。不得在 commit 成功后因请求刚好取消而改报失败；接管事实已经提交，需要按成功路径转移清理责任。
6. stopAccepting 持 mu：已停止直接返回；否则设置 stopping，调用 tracker.stopAccepting()；握手计数为零就关闭 handshakesDrained。它与 withCommit 的执行顺序由同一把锁决定。
7. wait 不持 mu：先等待 handshakesDrained 或 ctx.Done，再调用 tracker.wait(ctx)。两次等待共用传入的同一个 ctx，不重建预算。顺序等待两个已经单调关闭的通知即可，无需再启 goroutine。

锁顺序固定 entryGate.mu → tracker.mu。禁止在持 tracker.mu 时反向调用 entryGate。现有 tracker 方法均在返回前释放内部锁，满足这一约束。后续注册表操作也需维持单向锁顺序；lookup 释放注册表锁后再提交协调命令。

安装 entryGate 后，停止接入统一通过 gate.stopAccepting，不从外部绕过它单独停止 tracker，避免两者停止状态不一致。tracker.leave 与只读 snapshot 继续允许直接调用；gate 不拥有已有会话的取消权。

commit 是最终状态提交点，不是自动回滚事务框架。调用方必须在转移所有权前完成可能失败的校验，失败不能留下已经交接却向入口返回错误的状态。它也不替代会话协调者，不能从 handler 直接修改 resumeState。

## 接线时的清理责任

| 对象/名额 | 取得时机 | 释放或交接时机 |
| --- | --- | --- |
| 临时握手名额 | 升级 WebSocket 前 | 候选已成功交接；或失败后入口拥有的连接/启动资源实际清理完成 |
| 新建逻辑名额 | 合法 start 后，选择 Worker 前 | 启动失败完成回滚；或整场运行器完成实际清理后 |
| 恢复请求的逻辑名额 | 复用原会话名额 | 原逻辑会话最终清理时归还 |
| 候选 socket | 升级成功，入口拥有 | 接管失败由入口关闭；成功后仅 attachment 关闭 |

未来 start 在建流/构造完成后通过 withCommit 完成登记与运行器启动。resume 在已有协调者最终提交候选的地方使用同一 entryGate，最后的期限/请求有效性检查必须发生在获得提交锁后；不能持 gate 锁调用 requestResumeConnection 等待协调者，再让协调者争用同一把锁。网络建流、Read、Write、CloseNow、等待命令回复均在 gate 锁外。

Gateway.StopAccepting 后续统一委托 entryGate.stopAccepting，Gateway.Wait 委托 entryGate.wait，外部 cancelSessions 继续负责通知已有会话停止。v1/v2 共用一个 gate 所关联的 tracker，避免混合接入突破总会话预算。完整接线前，本轮不修改现有公开 handler、配置默认值或 Snapshot 含义。

## 本步验收

验收覆盖以下受控并发与 race 测试：

- 临时握手满额拒绝，释放后可重新进入；握手占位不改变 tracker 活跃数。
- tracker 已满时仍可取得握手名额并进入模拟恢复提交；新建逻辑占位被拒绝，原计数不增加。
- 模拟 v1 直接占 tracker 与 v2 tryEnterSession 合计受同一总上限限制。
- 停止后握手、新建占位及提交均被拒绝；重复停止不会重复关闭通知。
- commit 已进入时停止等待短事务退出；停止已完成时后续 commit 回调绝不执行。
- 请求取消不执行 commit；commit 内已成功提交后再发生取消，仍返回已经提交的成功；回调错误原样保留。
- 握手和逻辑会话的计数分别归零时，wait 仍等待另一类；两者都结束才成功。
- 等待超时不减计数、不释放资源；后续清理完成可再次等待成功。

测试中的模拟提交只证明停止与提交的互斥规则，真实候选交接仍必须在下一轮入口/协调者组合测试中证明。不得把本步通过写成真实满额恢复已经可用。

## 实现与验收（2026-10-07）

开发者明确委托助手实现本步。已补齐 `internal/gateway/entry_gate.go` 的构造器和全部方法，并补充参数、锁顺序、资源归还与提交责任的注释；新增 `internal/gateway/entry_gate_test.go`。保持原 tracker、公开 handler 和配置行为不变，尚未把 Gateway.StopAccepting/Wait 接到 gate。

新增 11 顶层/26 叶级测试全部通过。与现有准入测试一同执行 race 回归：18 顶层/36 叶级，36 通过、0 失败、0 跳过，无数据竞争报告。

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -count=1 -timeout=60s -json ./internal/gateway \
  -run '^Test(EntryGate|Admission)'
```

首次执行中，26 个新增场景通过；两个已有网络准入测试因沙箱禁止 `127.0.0.1:0` 监听而失败。允许本机监听后按同一范围完整复测通过。此失败为运行环境权限问题，未据此修改业务逻辑。

具体证据：

- 握手预算设为 4，64 个并发申请中恰好 4 个成功；其余明确拒绝，释放后可复用，不增加逻辑会话数。
- 逻辑预算设为 4，32 个模拟 v1 与 32 个模拟 v2 新建并发申请合计恰好 4 个成功；每个 v2 请求持有自己的临时握手名额，逻辑占位不再消耗握手名额。
- 逻辑预算 1 已满时，模拟恢复仍可申请握手并提交；新建被拒绝，原逻辑计数始终为 1。这只验证部件规则，没有实际查表、认证或候选接管。
- 受控提交回调持有共同锁期间，停服不能完成；提交返回后停服完成，后续回调不再执行。请求在获得锁前取消时，提交被拒绝并保留取消原因；提交成功后取消不会改报失败。
- 按两种顺序分别归还握手/会话资源，3 个等待者均必须等两类资源都退出才返回；重复停止不重复关闭通知。
- 虚拟总预算 5 秒，握手在第 4 秒释放后，逻辑会话等待仍在第 5 秒超时，没有再获得 5 秒预算。等待超时不改变剩余资源计数，实际清理后再次等待成功。

这些数字是正确性验证的输入及结果，不是生产容量、吞吐或恢复延迟指标。网络、建流和命令等待在提交锁外的约束目前由接口约定保证；后续组合代码必须遵守并验证。下一步组合公开 v2 handler、注册表、Worker 建流、运行器与最终候选提交，补真实网络新建/恢复及停服竞争验收。
