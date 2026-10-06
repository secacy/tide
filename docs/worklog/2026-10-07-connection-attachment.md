# 第六阶段：连接附着、候选交接与共同清理

日期：2026-10-07。状态：实现指导，尚未实现或验收。依据 `488f1bd` 的 reader/writer 与[资源所有权约定](2026-10-05-session-resource-ownership.md)。开发者实现核心代码，助手负责评审、测试与记录。

## 本步能力与边界

固定连接上的音频、结果和网络 ACK 已组合验收，连接启动、取消、关闭和等待目前仍由测试夹具承担。本步把这些责任放入真实连接附着对象，并接入同一个会话协调循环：首次连接安装、断线保留、带候选连接的恢复、旧代事件隔离和整场资源等待。

验收应能使用两条真实 WebSocket：第一条断开后原 Worker 继续运行；第二条通过内部附着命令接回，音频重放不重复上传，未确认结果可以重发；最终运行器等待已接管的全部任务退出。内部候选交接不等于公开握手或客户端自动恢复已经实现。

后续两组工作：统一 ready/音频接纳确认/终态输出，并接握手、注册表及双层准入、输入空闲与静默断网检测；再实现客户端有限缓存与恢复实验。本步不在 v1 handler 上启用新入口。

## 决策与取舍

| 选择 | 收益 | 代价与本次决定 |
| --- | --- | --- |
| reader/writer 各自关闭连接、报告 detach | 局部代码少 | 两边都需要理解终态，容易重复清理或旧代误伤；不采用 |
| 每条连接单独拥有 socket 和读写任务，事件回到现有协调者 | 一处决定业务状态，一处等待本代资源退出 | 每个已接管连接增加一个负责关闭和等待的 goroutine；采用 |
| 给整场会话再增加一个连接调度循环 | 可单独组织连接消息 | 与既有唯一协调者重复管理附着状态；不采用 |

清理与恢复还有两种选择：

- 允许旧连接清理和新连接运行重叠：能减少恢复等待，但要限制退出中连接数量，并持有它们直到真正退出。
- 旧连接清理完成后才接受新候选：只需一个 current 指针即可追踪所有已接管连接；代价是恢复等待可能增加。本次采用此方案，旧连接仍在退出时返回可重试的 errConnectionRetiring，不延长原恢复窗口。不能在协调者里等待它退出。

该约束最多限制一场会话已经接管的连接；同时握手或被拒绝的候选仍属入口所有，必须在后续入口配置独立连接预算。不能宣称整个进程因此只有“会话数那么多连接”。

## 两个不同的结束信号

```text
连接：停止信号 → 取消本代 ctx → CloseNow 返回 + reader/writer 返回 → attachment.done
会话：停止资格 → controlDone → 取消并等待 Worker/连接 → 释放缓冲 → 运行器返回
```

controlDone 保留现有语义，requestClose 仍只等待它。一个连接的 done 只表示本连接拥有的任务和关闭调用已结束，不能命名为整个会话 cleanupDone。现阶段用新运行器的返回表示内部资源清理完成；以后注册表及名额由外层运行器接管时，再在真正最终释放之后提供整场清理通知。

coder/websocket v1.8.15 的 CloseNow 包含 waitGoroutines，因此它不属于协调者可直接调用的非阻塞操作。协调者只发停止信号，由连接拥有者执行关闭和等待。

## 建议结构与注释

新增 `internal/gateway/connection_attachment.go`，实际字段可按实现需要调整，保持以下契约。

```go
// sessionConnection 是一条固定 WebSocket 的 I/O 与关闭能力。
// *websocket.Conn 直接满足；CloseNow 允许阻塞，必须在连接拥有者中执行。
type sessionConnection interface {
    connectionReadConn
    resultWriteConn
    CloseNow() error
}

// connectionCandidate 是已完成入口校验、尚未被会话接管的连接。
// 构造后不可修改。一次候选只能提交一次，不得并发复用或包装已被接管的连接。
type connectionCandidate struct {
    conn sessionConnection    // 接纳前由提交者负责关闭。
    writeTimeout time.Duration // 本连接单次写期限，必须为正。
    maxMessageBytes int64     // 完整输入消息上限，至少 9。
}

// newConnectionCandidate 校验配置，不启动任务、不设置连接参数、不关闭连接。
// 非法配置返回 nil、errInvalidConnectionCandidate；清理责任仍属调用者。
func newConnectionCandidate(conn sessionConnection, writeTimeout time.Duration,
    maxMessageBytes int64) (*connectionCandidate, error)

// connectionTaskKind 标识退出事件来自哪个任务；零值无效。
type connectionTaskKind uint8
const (
    connectionTaskInvalid connectionTaskKind = iota
    connectionReaderTask
    connectionWriterTask
)

// connectionEvent 只传递已经发生的退出事实，不自行决定会话终态。
type connectionEvent struct {
    generation uint64          // 固定服务端代次，不能来自客户端消息。
    task connectionTaskKind    // 决定下面哪个退出值有效。
    reader connectionReaderExit
    writer resultWriterExit
}

// connectionAttachment 拥有一条已接管连接及其任务；构造后以指针使用。
// 控制字段在启动前设置，此后不替换 conn/generation/reader/writer。
type connectionAttachment struct {
    generation uint64           // 安装时分配的固定代次。
    conn sessionConnection       // 固定句柄，唯一主动关闭者为本对象的 run。
    controlCtx context.Context   // 逻辑生命周期，判定停止原因时优先于本代 ctx。
    ctx context.Context          // 逻辑生命周期的子 context。
    cancel context.CancelCauseFunc // 停止本代，不向上取消 Worker。
    controlDone <-chan struct{}  // 控制结束可先于逻辑 ctx 取消。
    reader *connectionReader     // 本代唯一读取任务。
    writer *resultWriter         // 本代唯一结果写任务。
    events chan connectionEvent  // 容量恰好 2，两个任务各报告一次。
    done chan struct{}           // run 在实际关闭和读写全部返回后关闭。
    closeErr error               // 仅 run 写；其他调用者只能在 done 后读取。
}

// newConnectionAttachment 准备固定代次的 context 和真实 reader/writer。
// controlCtx 为逻辑会话生命周期，generation 非零；candidate 必须有效。
// 返回前不启动任务，不操作 socket，不转移 socket 的清理责任。
// 构造失败应释放已创建的子 context；保留配置错误身份。
func newConnectionAttachment(s *resumableSession, controlCtx context.Context,
    generation uint64, candidate *connectionCandidate) (*connectionAttachment, error)

// stop 幂等地取消本代 ctx，只发停止信号，不执行 CloseNow 或等待。
// cause 可以为 nil；context 会将这种情况记为 context.Canceled。
func (a *connectionAttachment) stop(cause error)

// run 在一个由会话启动的 goroutine 中执行一次。
// 启动真实 reader/writer，等待停止，然后关闭 socket 并等待两个任务返回。
// done 表示实际收尾结束；本方法不修改 resume、Worker 或准入状态。
func (a *connectionAttachment) run()
```

若准备好的 attachment 最终没有安装，只取消其子 context；不要运行 run，不要关闭候选 socket。此时提交者仍是 socket 拥有者。不要为了未安装对象“补齐 done”而假装执行过资源清理，未安装对象直接丢弃即可。

## attachment.run 的执行顺序

1. 创建 readerDone/writerDone 两个本地信号，各启动一次真实 run。它们共享 a.ctx；writer 的 controlCtx 继续指向逻辑会话，用来报告已经成功的 Write。
2. 每个任务返回后，把固定代次和退出值写入 a.events，再关闭自己的完成信号。容量 2 对应两个任务各一次；即使协调者已经退出，两次发送也都能完成，不启动额外发送 goroutine。
3. 拥有者等待 a.ctx.Done 或 controlDone。唤醒后按逻辑/连接停止原因选择 cause，调用 a.stop；controlDone 已关但 ctx 未取消时使用 errResumeClosed。循环关闭后不能让 reader 继续阻塞。
4. 同步调用一次 a.conn.CloseNow，记录结果，再等待 readerDone 和 writerDone。库或 reader 内部也可能因取消关闭底层连接，拥有者仍须完成自己的关闭/等待职责。
5. 所有等待完成后关闭 a.done。每个任务只有一个事件，不关闭 events，避免协调者 select 反复收到零值；协调者通过 a.done 判断生产者已经结束。

writerResultsComplete 只投递事件，run 仍等待停止，reader 可以继续接收 ACK。不能使用“任一任务返回就取消另一任务”的默认规则。

CloseNow 返回 errors.Is(net.ErrClosed) 的重复关闭结果视为已关闭；其他关闭错误保留为清理失败。这里的 done 证明关闭调用和任务已返回，不声称错误情况下底层资源必然全部成功释放。测试替身的 Read/Write 应响应 context；另可控制 CloseNow 的返回时刻验证不会提前报告 done。

## 接入同一个协调者

建议在 `internal/gateway/session_connections.go` 放连接模式的字段和事件处理辅助方法。它们由现有 runCoordinator 串行调用，不再启动第二个状态协调循环。

```go
// sessionConnections 由本场运行器创建，运行期间仅协调者修改。
// 协调者返回后，外层运行器接续读取 current 并执行最终等待。
type sessionConnections struct {
    current *connectionAttachment // 含正在退出的旧连接；done 和事件处理完前不丢弃。
    outputComplete bool           // 当前代 writer 已正常发完所需结果。
    lastSeq uint64                // 仅 outputComplete 时有效；不代表客户端已 ACK。
}
```

给 runCoordinator 增加可选的 `connections *sessionConnections` 参数。nil 为已有独立部件运行模式；非 nil 为管理真实连接模式。runControl/runWithWorker 传 nil，新入口传对象。助手在测试阶段适配现有直接调用该私有方法的测试，不要求开发者自行修改测试。

协调者的 select 新增当前 attachment.events 和 attachment.done 两个分支。current=nil 时置对应接收 channel 为 nil。current 仍在清理时保留它和事件读取能力；done 已处理并清空 current 后，不再监听旧 channel。

每个事件先经过既有 stopCause（逻辑取消、Worker 失败及绝对期限）检查，再验证 current 存在、事件代次等于 current.generation 且等于 resume.generation。旧代事件不改变附着、期限或结果状态；非法任务标签按内部错误终止。不得调用无代次约束的 requestClose 来转发连接失败。

### 事件处理规则

| 事件 | 协调者动作 |
| --- | --- |
| writerResultsComplete | 记录 outputComplete/lastSeq，保持连接和 reader，等待后续终态协议 |
| readerReadFailed / writerWriteFailed | 可恢复传输故障：当前 attached 转 detached；重置本代结果投递、通知、建立恢复截止时间；a.stop 原因。Worker 保留 |
| readerProtocolFailed / readerCommandFailed / writerControlFailed | 当前代不可恢复失败，返回原错误，进入整场清理 |
| readerStopped / writerStopped | 如果本代已在停止、已 detached 或控制生命周期结束，属于预期退出；当前代仍 attached、ctx 活跃却无故停止，按 errUnexpectedConnectionStop 终止，避免留下没有读取任务的 attached 会话 |

识别出的 WebSocket 关闭码 1002（协议错误）、1007（非法负载）、1008（策略违反）、1009（消息过大）也作为不可恢复连接错误，不应仅因来自 Read/Write 就解释为弱网。普通传输错误、对端正常关闭及单次 Write 超时按断线处理；这意味着 v2 写超时先停止本代连接，仍由恢复期限/结果预算限制保留。其他远端错误关闭可保守按不可恢复处理；规则固定为仅 1000/1001 与无 CloseStatus 的一般 I/O 错误允许恢复。库没有给出可识别协议身份的一般错误无法精确细分，不能据此宣称已识别全部底层协议故障。应用消息违规和消息大小已有 reader 分类。

断开处理应复用同一 helper，既供上述事件使用，也供已有 controlDetach 使用；有效 detach 同时停止 current，重复/旧代不重置截止时间。生命周期失效后产生的 I/O 错误不能反过来覆盖已经确定的会话原因。

一个容易漏掉的竞争：writer 先报告网络失败，协调者已进入 detached；reader 之前已经报告的协议错误稍后才被取出。只要还是这一代、这一条已接管连接，**已经报告的不可恢复错误仍必须终止会话**，不能因 detached 就忽略。取消发生后被 reader 自身优先分类为 stopped 的结果，按既有任务契约处理。

### 退休连接的回收

处理 done 时，先确认 done 已关闭，再排空剩余事件（最多两个），仍按上述规则裁决。因为所有生产者已返回，这时排空是确定且有限的。然后检查 closeErr，异常清理错误终止会话；正常才清空 current。

恢复命令处理时也执行同样的非阻塞检查：如果 current.done 已关，先处理剩余事件并回收；尚未关闭则返回 errConnectionRetiring。不能仅判断 done 后就直接换代，否则可能丢掉 channel 中已经报告的协议错误。不能依赖 select 一定先选 done 或事件分支。

若上述处理决定整场终止，仍保留 current 给外层等待；该等待可能立即返回，但所有权不能丢失。

## 首次启动和最终等待

新增入口：

```go
// runWithConnection 运行一场带实际连接所有权的逻辑会话。
// 必须用于未启动、初始 attached/generation=1 的 session；参数均已构造有效。
// 进入运行即接管原 Worker 与 initial；即使 ctx 已取消，也执行统一清理。
// 返回前等待全部已接管任务，并释放会话缓冲；返回值保留整场原因。
func (s *resumableSession) runWithConnection(ctx context.Context, now func() time.Time,
    worker *sessionWorker, initial *connectionCandidate) error
```

参数 nil、重复启动或非初始状态属于内部调用违约，采用与现有运行入口一致的编程错误处理，不设计成握手失败。生产入口先完成构造/校验，再调用一次运行器。候选/Worker 一旦交给此入口，调用者不能再用 defer 关闭同一 socket 或取消同一个 RPC。HTTP 请求 ctx 不得作为这里的逻辑 ctx。

提取与 runWithWorker 共用的私有 Worker 启动和收尾路径，复用同一 runCoordinator；不要复制一份千行协调循环。首次连接用 generation=1 预构造并安装 current，启动它的 run 和原 Worker 双向任务，然后进入协调循环；任务提前提交命令会等待同一个循环接收。准备过程中发生内部构造失败也必须清理已经接管的 initial/Worker；不丢弃所有权。

协调者返回之后：

1. 保存其退出原因，取消 RPC，并对仍有的 current 发 stop。
2. 先发出所有停止通知，再等待 uploader.done、receiver.done、current.done；顺序等待不会影响已经并行进行的退出。
3. 检查非预期 closeErr，必要时 errors.Join 到主因，保留 errors.Is。此前回收的 attachment 已等过 done，无需无界历史列表。
4. 全部任务退出后解除 input/results 引用并返回。保持已有 requestClose 只等 controlDone 的语义。

## 候选恢复的原子提交

新增 controlResumeConnection、命令 candidate 字段和内部入口：

```go
// requestResumeConnection 提交一条由调用者持有的候选连接。
// appliedSeq 为客户端连续应用位置；成功返回非零代次，并转移候选清理责任。
// 失败返回 0，候选仍由调用者关闭；不能并发复用候选。
// 请求交付后必须等明确回复，即使 ctx 此时取消也不能自行猜测是否接管。
func (s *resumableSession) requestResumeConnection(ctx context.Context,
    candidate *connectionCandidate, appliedSeq uint64) (uint64, error)
```

仅真实连接管理模式支持该命令，nil 模式返回 errConnectionManagementUnavailable。真实连接管理模式拒绝旧的无候选 requestResume，返回 errConnectionCandidateRequired；否则可以绕过实际连接安装而只改代次。现有组件模式的 requestResume 和测试行为保留。

协调者处理命令时按以下顺序操作；所有拒绝必须回复一次，继续沿用 submitCommand 的缓冲回复和交付后等待规则：

1. 现有 stopCause、命令 ctx 校验，确认管理模式及候选配置有效。
2. attached 拒绝抢占；closed/到期拒绝。若 detached 仍持有旧 current，按上节检查并回收；仍清理中就拒绝本次候选，不等待、不入队、不延长恢复期限。
3. 检查 ackedSeq <= appliedSeq <= offeredSeq。拒绝时不改变累计 ACK、generation、cursor 或候选所有权。
4. 可用 resumeState 的值副本计算下一代，调用其 resume(now) 做状态/期限/溢出检查；不要提前修改正式状态。副本检测到到期时回复并终止真实协调者，使正式恢复资格也结束。
5. 用下一代预构造 attachment。此时仍没有 socket I/O 或任务。最终提交前再次检查停止/期限及命令取消；拒绝时只取消预构造的子 context，候选留给提交者。
6. 同一串行段提交累计 ACK、正式恢复状态、结果投递重置、新 current 与 outputComplete 重置。这里的 ACK 范围已经验证，不能留可预期失败在状态切换之后；内部不变量失败应终止并按已经转移的所有权清理。
7. 安装 current 即转移清理责任，由协调者启动 attachment.run，然后回复新 generation。中间没有网络 I/O、CloseNow 或任务等待。成功后即便请求 ctx 取消，返回仍说明已经接纳；不能再把候选还给提交者。

附着 context 始终继承逻辑会话，不能继承本次请求 ctx。网络 ready 在后续统一输出中发送；本步内部调用测试先提供已知协议/身份，不把 generation 返回值伪装成完整网络握手。当前已知结果可以由 writer 开始发送，公开入口启用前必须补齐 ready 在先的输出顺序。

## 验收清单与实现顺序

建议本次按以下三个部分连续实现，最终一起组合验收：

1. connectionCandidate/connectionAttachment：真实任务启动、两个有界退出事件、stop、CloseNow 和共同 done。
2. sessionConnections、协调者事件分支和新运行入口：有效断线保留原 Worker，正常 writer 返回保留 reader，整场结束等待实际清理。
3. 带候选恢复命令：一次提交绑定状态和 socket，旧连接未清理时明确拒绝，拒绝不接管候选。

助手负责补充测试和必要的旧测试调用适配。必须验证：

- 构造无 I/O，非法候选拒绝；成功接管后，即使请求取消，socket 仍由会话负责关闭。
- 两个任务都退出后才有 attachment.done；CloseNow 卡住时协调者仍处理命令/期限，恢复返回 retiring，整场运行器仍等待，不能提前释放缓冲。
- 正常 writer 返回不关闭 reader，网络 ACK 继续接纳；reader 或 writer 传输失败有效 detach，原 Worker 不取消、不额外 CloseSend，重复事件不续期。
- 已报告协议错误与传输错误竞争时，不因先 detach 或先选择 done 而被遗忘；旧代事件不能影响新代状态。普通断网与显式协议关闭码按固定策略分类。
- 多个候选竞争只接纳一个；attached、错误 appliedSeq、代次耗尽、恰好到期、旧连接退出中拒绝，均不接管被拒 socket、不推进 ACK/代次。到期会正常终止整个会话。
- 两条真实 WebSocket 接到同一个原 Worker：窗口内恢复，未确认结果重放、已接纳音频重发去重、尾部仍完整；测试调用内部安装命令，明确尚无网络握手/自动重连。
- 循环多次断开/附着时，已接管连接的同时存活数不超过 1，实际关闭次数与成功接管数一致；最后 reader/writer/Worker 均退出。计数属于受控条件下正确性证据，不包装为稳定容量或生产恢复耗时。

本次仅形成方案和进度记录，没有新增运行能力或测试成果。
