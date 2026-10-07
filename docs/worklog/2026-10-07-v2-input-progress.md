# 第六阶段：v2 输入进展与静默连接保护

日期：2026-10-07。状态：设计与实现指导，待开发者实现；测试由助手完成，尚无本步运行成果。

## 当前位置与能力目标

公开实验入口已经支持新建、满额恢复、音频去重、结果重放和最终清理。但 v2 connectionReader 当前直接 Read(ctx)：如果客户端一直没有新音频，也没有被操作系统报告断开，会话可能长期保持 attached，原 Worker 和逻辑名额持续占用。

本轮让没有输入进展的连接在配置期限到达时进入 detached，并沿用有限恢复窗口。窗口内接回原 Worker，窗口耗尽则终止逻辑会话，实际清理后归还名额。随后收敛恢复错误码、初始 ready 丢失及终态查询边界，再接有限重放客户端与故障实验。

这是输入进展保护，不能据此断言网络一定故障。录音继续上传静音 PCM 时仍有新字节进展；暂停采集且不再上传数据会触发期限。显式暂停/继续录音协议另行设计。

## 方案选择

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 每次 Read 设置超时 | 改动集中，能限制完整消息读取等待 | ACK 或重复音频也能反复刷新；恢复到已 end 会话还需额外同步输入阶段。本轮按业务进展计时，由协调者负责 |
| WebSocket Ping/Pong | 判断对端是否仍响应，适合连接保活 | 对端能回 Pong，不代表仍上传音频；单独使用不能限制业务停滞，后续可作为补充 |
| 定时扫描所有会话 | 可以集中定时管理 | 需要跨会话同步和扫描粒度，当前没有证据表明已有每会话 Timer 是瓶颈；本轮复用协调者现有 Timer |
| 协调者记录新的音频接纳进展 | 正式输入状态、代次与期限在同一拥有者中，可精确排除 ACK/重复块 | 需要让现有期限处理支持“到期后 detach，继续运行”的非终态迁移；采用 |
| 空闲后立即终止整场 | 最快释放计算资源 | 无法给未被及时发现的弱网断线保留恢复机会；本轮先 detach，再使用固定恢复窗口 |

## 明确语义

新增 v2 专用 `InputProgressTimeout`，默认 30 秒。保留现有 v1 InputIdleTimeout 的单次完整消息读取语义。使用不同名称是为了明确区别：v2 只由正式接纳的新音频刷新期限。

| 事件 | 输入进展期限变化 |
| --- | --- |
| 初始连接安装、协调者开始运行 | 未 end 时建立 now + InputProgressTimeout；无需等 ready 写成功 |
| 当前代次成功接纳新音频（accepted=true） | 更新为此次接纳时刻 + InputProgressTimeout |
| 历史音频重复、结果 ACK、Ping/Pong、Worker 进度/结果 | 不刷新 |
| 首次合法 end | 清除输入进展期限，继续使用既有尾部/状态/结果保留期限 |
| 已 end 会话成功恢复 | 不重新建立输入进展期限；仍可读 ACK 和完成确认 |
| 未 end 会话成功恢复 | 从最终安装时刻建立新的输入进展期限 |
| 传输失败导致 detached | 清除输入进展期限；恢复窗口沿用已有的断开观察时刻 |
| 输入进展期限到达 | 停止当前 attachment、进入 detached，恢复截止时间为 inputDeadline + ResumeWindow |
| Worker 上传 EOF 后等待真实 Recv 状态 | 清除输入进展期限，由既有 WorkerStatusTimeout 处理 |

新音频以 Gateway 接纳、接纳位置前进为准，不要求该块已经完成 Send 或模型推理。已接纳音频在 detached 期间继续由原 uploader 排空。下游积压和 Send 超时仍按既有规则处理。

成功恢复给予一个新的输入等待窗口，便于客户端先收到 ready 再重放。这意味着反复主动恢复可以延长整场无进展时间；本轮约束单次静默连接及其恢复窗口，不新增整场最长时长或恢复次数限制。客户端后续还应有独立的重试总预算。

## 配置与字段

在 V2Config 增加：

```go
// InputProgressTimeout 限制 attached 且尚未 end 时等待新音频接纳的时间。
// 0 使用默认 30 秒，负值非法；ACK 和重复音频不刷新。
InputProgressTimeout time.Duration
```

normalizeV2Config 校验/填默认值；prepareV2Worker 将其传入 sessionWorkerConfig。内部构造器仍要求必要期限为正，增加对应校验：

```go
// inputProgressTimeout 是真实连接模式的输入进展期限，必须为正。
// connections=nil 的部件运行模式不启动这个期限。
inputProgressTimeout time.Duration
```

新增原因 `ErrInputProgressTimeout = errors.New("input progress timeout")`，用于停止当前代 attachment。它本身不取消原 RPC，也不作为整场正常完成。恢复窗口最终耗尽仍以既有 errResumeExpired 结束。

在 runCoordinator 增加局部状态：

```go
// inputDeadline 是当前 attached 代次等待新音频的绝对截止时间。
// 零值表示禁用；由协调者独占，detach/end/等待 Worker 状态时清除。
var inputDeadline time.Time
```

不要放入 reader，也不新增扫描 goroutine 或独立 timer。现有 reader 继续报告 I/O 事实，由 attachment.stop 取消它并由 attachment.run 关闭连接、等待读写实际退出。

## 期限处理的组织

当前 nextDeadline 与 checkExpired 各自列举期限，后者把到期统一转换为整场退出错误。本步适合把“选择哪个期限”集中一次，避免两份逻辑的启用条件不一致。

将现有局部 deadlineKind/deadlineCandidate 提前，增加 deadlineInputProgress。以下均可保留为 runCoordinator 内的局部闭包，无须新增通用调度框架：

```go
// nextDeadline 返回此刻适用的最早绝对期限；零期限不参与选择。
// 时间相同时，沿用已有终态期限优先级，输入进展排在最后。
nextDeadline := func() (deadlineCandidate, bool) { /* ... */ }

// armWakeTimer 仅按 nextDeadline 的 at 唤醒，不修改业务截止时间。
armWakeTimer := func() { /* ... */ }

// checkExpired 处理截至 at 已到期的业务期限。
// 输入进展到期只执行 detach，再重新检查其他期限；终态期限返回错误。
// 一次调用中 at 固定，不能为迟到处理重新赠送一个恢复窗口。
checkExpired := func(at time.Time) error { /* ... */ }
```

输入期限只在 connections != nil、worker != nil、workerRunning、resumeAttached、尚未 end 且没有等待 Worker 状态时适用。显式迁移时清零，选择函数仍检查适用条件；避免一次遗漏清零把旧代期限用于新状态。

为了复用断开逻辑，把现有 detachGeneration 中的状态变化提取为接受显式时刻的局部函数，放在期限选择/处理闭包之前：

```go
// detachGenerationAt 提交指定代次的断开，at 决定恢复窗口起点。
// 成功时清除 inputDeadline，重置投递并通知本代停止。
// 不重排 Timer、不做 CloseNow、不等待任务；重复/旧代断开不续期。
detachGenerationAt := func(generation uint64, cause error, at time.Time) bool { /* ... */ }
```

已有 detachGeneration 保留为小包装：调用 detachGenerationAt(generation, cause, now())，成功时 armWakeTimer。传输事件和 controlDetach 继续用它。输入超时调用 detachGenerationAt(currentGeneration, ErrInputProgressTimeout, selected.at)，随后重排唤醒并继续检查期限。这样避免 checkExpired 与带重排的 detachGeneration 互相依赖。

checkExpired 使用循环处理：

1. 选择最早适用期限，没有期限或 at 尚未到达则返回 nil。
2. 若为输入进展，到期点作为 detach 起点；清除该期限，安排恢复期限，然后继续第 1 步。一次调用最多执行一次有效的输入 detach。
3. 若为恢复/尾部/状态/结果保留等终态期限，沿用现有错误与状态推进规则返回。

例：输入期限 t=5、窗口 10 秒，协调者在 t=8 观察到超时，恢复截止仍是 t=15；如果直到 t=20 才运行，这一次调用就同时处理输入 detach 和已过期的恢复窗口。不能将截止时间写成 t=18 或 t=30。

stopCause 继续保持逻辑 context → 运行中 RPC 取消 → 业务期限的优先级，仍在各事件/命令处理前调用。仅在 wakeC 分支检查不够：忙碌的 ACK/Worker 事件不能让过期连接不断续命。输入 detach 返回 nil 后，命令分支继续通过当前附着状态和 generation 拒绝旧代音频。

## 接纳与切换位置

1. 初始化：只在真实连接模式、输入未结束时设置 inputDeadline，并通过已有 armWakeTimer 安排唤醒。首次初始化也必须 arm，不能只对 resumeDetached 初始化 Timer。
2. controlAudio：保留现有前置期限检查。仅 offerAudio 成功且 accepted=true 时用 now()+inputProgressTimeout 刷新，并重排 Timer。历史重发不更新；音频即使早已进入 socket 缓冲，到处理时已经过期也不能复活旧代。
3. controlEnd：首次合法接纳后清除 inputDeadline，再建立既有 tailDeadline 并重排 Timer；重复 end 不产生新的期限。
4. Worker 上传 EOF：切到 uploadAwaitingStatus 时清除 inputDeadline，再建立/使用原状态等待期限。不要把已不允许新输入的阶段判为客户端空闲。
5. 普通 detach：在统一 detachGenerationAt 中清除；重复读写退出不重建窗口。原来“已报告协议错误即使先 detach 仍终止”的规则继续保留。
6. controlResumeConnection：只在 gate 内的最终提交成功时，为未 end 会话建立新 inputDeadline；准备、认证或范围校验失败都不能刷新。已 end 恢复保持零值。锁外启动 attachment，并沿用现有 armWakeTimer。
7. 清理：输入超时先取消当前 attachment；原 Worker、注册表和逻辑名额在恢复窗口内继续保留。窗口耗尽时仍由原运行器取消 Worker、等待全部实际退出，再 remove/leave。

同一条事件被选中不代表可以越过期限；沿用现有各处理点的 stopCause 检查。不要取消整场 context 来模拟输入超时，也不要在协调循环中等待 CloseNow。

## 实现与验收安排

开发者修改 config_v2.go、handler_v2.go、session_worker.go、session_control.go，补上字段和行为注释。已有内部测试构造器需要新增必要期限，助手在评审时统一更新测试夹具并补测试；纯部件模式保持不启动输入进展计时。

助手将验证以下可观察行为：

- 公开 ready 后一直不发送音频：进入 detached、旧连接实际关闭，RPC/注册/名额仍保留；恢复窗口耗尽后真正清理归零。
- 窗口内通过公开入口恢复并继续音频：同一个 sessionId、递增 generation、Pick=1/RPC=1，最后正常完成。
- 合法新音频刷新；连续 ACK、重复历史音频和 Worker 响应不刷新；分片迟迟不完成不能占住连接无限等待。
- 截止点同时到来的旧代音频/end 不推进输入；已接受的输入不被回滚，恢复位置仍正确。
- 已 end 会话等待尾部结果，以及在 retaining 阶段恢复等待 ACK，均不受输入进展期限误伤。
- 虚拟时间验证迟到观察：I=5 秒、R=10 秒，在 t=8 时截止仍为 t=15；在 t=20 时观察则直接走最终终止；以上为设定值，尚无实测结果。
- 旧连接清理阻塞时仍处理协调请求并明确拒绝提前安装；逻辑期限耗尽不提前释放仍在实际清理的资源。
- 与逻辑取消、RPC 失败、状态等待、其他期限及候选最终提交竞争时，沿用既定优先级；到期状态不被迟到命令覆盖。

测试将区分虚拟期限证据与真实网络证据。I+R 是无人恢复时的逻辑终止预算，实际资源释放还取决于 I/O 响应取消及关闭完成，不能承诺墙钟恰好 I+R 时所有资源已释放。本轮不新增恢复成功率或容量结论，也不提前启用生产默认 v2。
