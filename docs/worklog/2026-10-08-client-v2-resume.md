# 第六阶段：客户端有限重试与原场恢复

日期：2026-10-08。状态：已按开发者明确委托实现，有限原场恢复及全项目 race 验收通过；量化恢复性能实验尚未执行。前置依据为已验收的单连接客户端（099f03c）及公开 v2 恢复入口。下文保留实现前方案，文末记录实际接口与证据。

## 本步能力与后续顺序

目标：客户端在已经接纳初始 ready 后遭遇短暂传输故障，保留本场身份、音频缓存、连续结果应用位置和采集任务，在有限预算内重新连接同一 Gateway，以 resume 接回原 Worker，补发必要音频并完整结束。

本步是一组可验收能力：会话/连接所有权调整、恢复握手、有限重试、重放与退出一并完成。无需先提交一批尚未接线的微型部件。随后由助手补可控断网及真实 Gateway/TCP gRPC 验收；再进入固定故障条件的量化实验，记录实际恢复耗时、重放字节、缺口和重复。CLI/负载观测按实验需求接入。

## 问题与备选方案

现在 Run 退出会关闭 source、取消三个 I/O 任务并销毁状态。直接在外面循环调用 Run，会重新 start，失去原音频位置和结果位置。把 Dial/resume 塞进现有协调循环同步执行，则在等待连接时无法接收采集事件，文件源会被暂停，也不能证明真实采集仍然持续。

| 方案 | 优点 | 代价 / 选择 |
| --- | --- | --- |
| Run 出错后从头重建会话 | 调用简单 | 重复建立 Worker，结果上下文及去重失去原会话依据，不能用于原场恢复 |
| 暂停源、同步重连，成功后继续读文件 | 改动较小 | 文件可以暂停，真实说话不能暂停；本步需要覆盖断线期间新采集音频，因此不采用 |
| 保留整场协调者与采集任务，替换连接任务 | 已有缓存/序号可跨连接继续使用，断线期间仍按预算接纳音频 | 需要连接清理、握手结果交接与总预算；本步采用 |

首版保持一次只有一条候选/活动/退出中的连接。旧连接完全关闭且读写任务退出后才开始下一次 Dial。相比并行竞争多个连接，恢复多花旧连接清理的时间，但所有权和事件来源明确，也符合服务端尚有连接时返回 session_busy 的规则。

## 先固定恢复边界

1. `Resume == nil` 保持单连接行为。显式开启后，只恢复已接纳初始合法 ready 的本场会话。
2. 初始 ready 尚未接纳时的失败直接返回。服务端可能已经创建会话而 ready 丢失；客户端没有可信 ID/token，不能猜测原场身份。当前协议没有客户端创建幂等键，不自动再 start。
3. 恢复请求固定同一 URL、原 sessionID/token 和当前 appliedSeq；失败不回退 start、不重新选 Worker。本步限同一存活 Gateway/Worker，实例状态丢失时明确失败。
4. 仅在 Completed=false 时恢复。已经核实 completed 后，确认失败保留 Completed=true 并返回原错误；服务端可能已经因确认而删除会话，首版不新增完成查询或确认后重连。
5. 源错误、奇数音频、缓存满、结果缺口、协议错误、回调错误/超时、调用方取消、整场完成期限到期均结束本场。缓存满不覆盖旧块，不停读等待新连接腾出空间。
6. 普通传输断开、单次 Write 超时和恢复握手的传输失败可以尝试恢复；操作超时必须先检查整场 context、恢复期限及完成期限仍有效。
7. 有明确入口错误时按 code 判断：session_busy、handshake_limit、entry_timeout 可在剩余预算内重试；其他码（含未知码）终止。service_stopping 不在同实例上反复尝试，resume_unavailable 不推断其内部认证/过期/状态丢失原因。

不能只写 `errors.Is(err, context.DeadlineExceeded) => retry`：回调、写入和整场完成都可能返回同一个错误，但处理方式不同。保留错误产生的阶段及类别；解析和业务校验直接标为终止，只有 I/O 边界进入传输分类。WebSocket ErrMessageTooBig 和已知协议/策略 CloseStatus 终止；无 CloseStatus 的普通 I/O 错误及 NormalClosure/GoingAway 在业务尚未完成时允许有限恢复，沿用当前 Gateway 的保守传输分类。这个规则不宣称所有底层读错误都能恢复成功。

HTTP 恢复入口：有效 error JSON 仍使用 V2EntryError；没有明确错误码的 503 可在预算内作为临时入口失败重试。其他已明确的 HTTP 拒绝不根据提示文本猜测重试。解析始终有界，不输出凭据或原文。

## 配置、结果与预算

在 V2Config 增加 Resume 字段，构造时复制所指配置并填充默认值，避免调用方随后修改指针影响运行。

```go
// V2ResumeConfig 控制同一 Run 内对原会话的恢复尝试。
// 通过 V2Config.Resume 非 nil 显式启用；零字段采用默认值，负值非法。
type V2ResumeConfig struct {
    MaxAttempts int // 默认 5，整场 Run 的 resume Dial 总次数；首次 start 不计。
    Window time.Duration // 默认 5s，每轮断线从被协调者观察到开始的固定总预算。
    InitialBackoff time.Duration // 默认 100ms，本轮第一次失败后的等待。
    MaxBackoff time.Duration // 默认 1s，指数退避上限，必须 >= InitialBackoff。
}

// 在 V2Config 中追加：
// Resume *V2ResumeConfig // nil 不自动恢复；NewV2 保存独立配置副本。

// 在 V2RunResult 中追加：
// ResumeAttempts int // 实际启动的恢复拨号数，包含失败和 ready 未接纳的尝试。
// ResumedConnections int // 本地成功接纳的恢复 ready 数；不是丢包次数。

// ErrV2RecoveryExhausted 表示恢复的次数或窗口预算耗尽。
// 返回时保留最近失败原因；窗口到期另外保留 context.DeadlineExceeded。
var ErrV2RecoveryExhausted = errors.New("v2 recovery budget exhausted")
```

首个恢复尝试在旧连接实际清理后立即进行。连续尝试失败后依次等待 100、200、400、800、1000ms，随后封顶；计算需防 duration 溢出，不按实际等待结束重置本轮截止点。等待 Timer 期间继续处理源事件和整场取消。

一次合法恢复 ready 接纳后结束本轮恢复窗口，清零本轮退避次数；后续再次断线可开始新窗口，但整场 ResumeAttempts 不清零。第五次尝试成功后可以继续正常传输，只有再需要第六次尝试时才因次数耗尽退出。这意味着长期频繁抖动也会用尽五次默认尝试；选择总次数是为了明确限制首版反复重连行为，未来可按实验需要改为滑动窗口/每轮预算。

首版采用确定性退避，便于固定条件复测；多客户端同时断线可能同拍重试，群体恢复实验前应评估并加入有上界的随机抖动，单客户端验收不作为群体重连安全证据。

客户端的 5s 是本地停止尝试预算，不能推算服务端还剩多少恢复时间。服务端普通恢复窗口当前默认 10s，但其观测起点和尾部结果保留期限不同，服务端可能更早拒绝。所有时间值为初始实验配置，不是容量/网络恢复结论。

时间所有权：

- 本轮 `recoveryDeadline`：观察断线时固定，包含旧连接清理、退避、Dial、resume Write 和 ready Read；失败尝试不续期。
- 每次握手：`min(now + HandshakeTimeout, recoveryDeadline, completionDeadline（若已启动）, caller deadline)`。
- `completionDeadline`：仍从本场第一次 end 授权固定；搬到整场运行字段中，重连、重复 end、ready.inputEnded=true 都不重置。
- 已接纳 completed 后沿用固定最终确认窗口，本步不走恢复分支。
- 清理必须实际完成再返回；超时取消是停止业务的预算，不能承诺所有依赖任意时刻都已关闭。

## 资源归属与运行阶段

整场 Run 持有 source、唯一采集任务、sourceEvents、v2ClientState、完成期限、恢复预算和最终结果。第一次合法 ready 后启动采集一次；后续连接变化不重新启动、不重新定位源，也不重置采集时间轴。只在本场最终退出时关闭 source 并等待采集结束。

每次连接持有 socket、连接 context、reader/writer、各自容量 1 的事件/请求通道、writerBusy 和本连接结果确认写出位置。连接退出时取消连接 context、CloseNow、等待两个任务完成；不取消 Run context，不关闭源。

恢复后新建连接任务，清零 writerBusy/上一写类型，将 lastResultAckWritten 初始化为合法 ready.AckedResultSeq（等于恢复请求 appliedSeq）。旧连接失败写的授权事实仍在 state，不能回滚 offeredOffset/endOffered/appliedSeq。

```go
// v2RunPhase 由整场协调者独占；phase 不代替实际任务 done。
type v2RunPhase uint8
const (
    v2Handshaking v2RunPhase = iota // 首次 start 或后续 resume；尚无可用读写任务。
    v2Streaming // 当前连接正常处理音频/结果。
    v2Closing // 旧连接正在实际关闭和等待；继续接纳源事件。
    v2Backoff // 等待下一次尝试；继续接纳源事件。
    v2Finishing // 已记录 Completed；仅处理最终确认与清理。
)

// v2RecoveryState 只记录预算，由协调者读写；不自行 Sleep 或启动任务。
type v2RecoveryState struct {
    deadline time.Time // 当前一轮断线的固定截止点；非恢复中为零。
    attempts int // 整场累计实际 resume Dial 次数，成功恢复也不清零。
    failures int // 当前一轮已失败的恢复尝试数，用于计算下次退避。
    lastErr error // 最近连接/握手失败原因，不包含凭据打印。
}

// v2HandshakeResult 在 attempt.done 关闭后才可读取。
// conn 非 nil 时表示已成功读出候选 ready，仍由 attempt 持有，等待协调者取走。
type v2HandshakeResult struct {
    conn *websocket.Conn
    ready wsprotocol.ReadyMessage
    failure v2AttemptFailure // 成功为零值；区分传输、入口拒绝及不可恢复失败。
}

// v2HandshakeAttempt 独占一次 Dial/写握手/读首帧任务。
// cancel 只取消握手子 context，不能取消候选的未来业务 context。
// result 仅由该任务写一次，读取必须在观察 done 后；含身份，不整体记录日志。
type v2HandshakeAttempt struct {
    cancel context.CancelFunc
    done chan struct{}
    result v2HandshakeResult
}

// v2AttemptFailure 保留失败类别，不能通过裸 error 猜测是否重试。
type v2AttemptFailure struct {
    kind v2AttemptFailureKind // 零值成功；其他为 transport、entry、terminal。
    err error // 原始原因；entry 使用可 errors.As 的 V2EntryError。
}
```

```go
// v2AttemptFailureKind 在产生错误的边界确定，不通过错误文本猜测来源。
type v2AttemptFailureKind uint8
const (
    v2AttemptOK v2AttemptFailureKind = iota // 无失败。
    v2AttemptTransport // I/O 故障，仍须排除消息过大和协议/策略关闭。
    v2AttemptEntry // 收到有效 V2EntryError，按 Code 决定。
    v2AttemptTerminal // 协议、状态等不可恢复错误。
)
```

应用错误一律由协调者终止，不送入连接重试分类器；关闭旧连接导致的取消回执也不会再开启新一轮恢复。

连接句柄建议组织为 v2ClientConnection，包含固定 generation、socket、cancel、readerEvents/writeJobs/writeReceipts、两个任务的 WaitGroup、清理 done 和清理错误。只保留一个当前句柄，清理完成前不能替换；单写在途规则沿用已验收实现。该类型不拥有 source 或 v2ClientState。

```go
// v2ClientConnection 使用后不复制；协调者拥有句柄和调度字段。
// I/O 任务仅使用固定 conn/context/通道，不访问业务状态或调度字段。
type v2ClientConnection struct {
    conn *websocket.Conn // 本次 socket，beginClose 负责实际关闭。
    generation uint64 // 已接纳的服务端代次，业务 reader 事件绑定该值。
    cancel context.CancelFunc // 只取消本连接，不影响采集。
    readEvents chan v2ReadEvent // 容量 1，消息与终态错误按读取顺序交付。
    writeJobs chan v2WriteJob // 容量 1，最多一个待写或在途任务。
    writeReceipts chan v2WriteReceipt // 容量 1，保留每次实际写入结果。
    tasks sync.WaitGroup // 只等待本连接 reader/writer。
    closing bool // 仅由协调者修改，避免重复启动清理。
    done chan struct{} // 实际 CloseNow 和 tasks.Wait 完成后关闭。
    closeErr error // 清理任务写入；观察 done 后才能读取。
    writerBusy bool // 协调者派发前置 true，处理回执后置 false。
    lastWriteKind v2WriteKind // 本连接公平派发依据，不跨连接继承。
    lastResultAckWritten uint64 // 初始取合法 ready.AckedResultSeq，之后只按成功回执推进。
}
```

候选 ready 校验失败时也可使用此清理句柄：尚无 reader/writer，tasks 为零；cancel 允许为空，beginClose 按是否存在处理。正常连接的 lastResultAckWritten 由协调者在启动后的首次派发前初始化。

握手和清理都由短生命周期任务执行，协调者通过 done 观察结果。不能在协调者里直接等待 Dial、CloseNow 或 WaitGroup.Wait，否则断线期间的源事件再次被堵住。整个运行至多一个源任务、一组连接读写任务及一个清理任务，或一个握手任务；不会每次重试留下旧任务。

## 握手/清理交接的具体规则

建议增加 client_v2_connection.go，承接以下职责；client_v2.go 保留公开 API 与整场协调循环，io_v2.go 继续提供有界 I/O。

```go
// startV2Handshake 启动一次有绝对期限的握手，不读写共享 state。
// request 是协调者构造的 StartMessage 或 ResumeMessage 值快照，含凭据勿记录。
// 任务解码首帧；ready 的业务校验由协调者调用 state.acceptReady 完成。
func (c *V2Client) startV2Handshake(
    ctx context.Context, request any, deadline time.Time,
) *v2HandshakeAttempt

// startV2Connection 启动固定连接的 reader/writer；连接业务 ctx 继承 Run，
// 不继承即将结束的握手 context。仅在 ready 校验接纳后调用。
func (c *V2Client) startV2Connection(
    ctx context.Context, conn *websocket.Conn, generation uint64,
) *v2ClientConnection

// beginClose 只发起一次实际关闭/等待；协调者通过 done 继续观察。
// 重复调用不再启动任务；不关闭源，不改业务状态。
func (a *v2ClientConnection) beginClose()

// retryableV2Failure 根据明确的来源及稳定入口 code 判断候选是否可重试。
// 调用前先检查 Run 取消、Completed、恢复启用、可信身份及各项绝对预算。
func retryableV2Failure(failure v2AttemptFailure) bool
```

request 使用现有协议结构体的值快照，不新增协议字段。也可以让 startV2Handshake 接收预编码的独立字节切片，关键是不能让任务读取正在变化的 state。

握手任务独占候选 socket，失败先关闭候选再发布 done；成功把 socket 和解析的 ready 保存到 result 后关闭 done。协调者观察 done 后检查取消/绝对预算，再 acceptReady，成功才取走 socket 并清空 attempt 引用。如果 ready 被拒绝，候选也必须实际关闭；可以交给同一异步清理句柄处理（尚无 reader/writer 时 WaitGroup 为零），清理期间保持源事件处理。失败清理错误保留，不能隐藏后继续产生新的候选；沿用单连接实现，CloseNow 的预期 net.ErrClosed 可以忽略，其余错误保留。

最终退出若仍有握手尝试，先 cancel，再等 done，关闭任何未被取走的候选。done 发布后取消与事件选择竞争也不能丢失连接所有权。因为始终只有一个 attempt 句柄，且完成处理后才换下一个，本步无需额外引入 attempt ID；服务端 generation 仍用于业务状态校验，允许恢复 ready 跳代。

## 协调循环的改造顺序

1. **迁移整场资源。** 把采集启动和 WaitGroup 从 coordinateV2 的连接局部逻辑提到 Run 生命周期；源在第一次 ready 后启动一次。提升 state、completionDeadline；保留 nil Resume 时已有行为与验收。
2. **封装一次连接。** 把 reader/writer、通道、busy/确认位置放入连接句柄；连接失败立即停止新派发，state.detach 当前代，并启动异步实际清理。Closing 阶段忽略旧连接业务事件，仍处理源和预算。
3. **封装一次握手。** 初次发送 start，后续只发送 `ResumeMessage{Type:resume, Version:v2, SessionID:原ID, ResumeToken:原token, AppliedSeq:state.appliedSeq}`。握手任务只交付候选，协调者唯一修改 state。
4. **接入有限重试。** 第一次可恢复故障固定本轮 deadline；清理 done 到达、无清理错误且预算仍有效后，立即或经退避启动恢复尝试。Dial 启动时才增加 attempts，失败不延长窗口。所有等待阶段继续 appendAudio 或 sealInput，满则明确失败。
5. **接纳 ready 后继续调度。** state.acceptReady 原子校准确认位置和本代发送游标，清理前一轮恢复窗口，再启动新的 reader/writer；从 ready.NextOffset 开始发送缓存。先补原来保留的连续块，再发送断线期间新增的块，沿用 FIFO 和单写授权。
6. **完成与统一收尾。** end 在新连接必要时重发，整场完成期限只设置一次。Completed 后进入已有最终确认流程。最终退出停止所有 Timer、取消 Run/握手/连接、关闭源与所有尚持有的候选/连接、等待全部实际任务，保留确认写成功事实和清理错误。

旧连接已经入队但尚未应用的结果可以舍弃，恢复请求的 appliedSeq 只报告真正提交的位置，服务端会重放后续结果。已经应用但 ACK 丢失的结果不会再次应用；恢复握手本身就提交 appliedSeq。连接报错抢先于未处理 completed 时，尚未记录的完成可通过恢复重新取得；已经记录的完成永不回滚。

done/Timer/源事件同时就绪时都要在状态提交前复核取消与绝对截止点，沿用上一轮边界修正。恢复 deadline 到达后收到候选 ready，也必须关闭候选并结束，不因 select 恰好选择 ready 而延长预算。

## 一个具体恢复例子

客户端已经采集到字节 12800，曾向旧连接授权发送到 9600，最后收到 ACK=6400。断网期间再采集到 16000。恢复后 Gateway 返回 nextOffset=9600：

- [6400,9600) 已在 Gateway 接纳，只是旧 ACK 未送达；按 ready 释放本地对应缓存。
- 按原块从 9600 继续到 16000，覆盖旧连接尚未接纳及断线新采集音频。
- 不重读 source，不回滚本地采集位置，不重建 Worker。
- 若 ready 声明 nextOffset 超过旧授权 9600，或者早于本地已释放前缀，则拒绝，不能凭空补齐。

## 预先固定的验收

实现前预先固定以下验收；随后开发者明确委托助手完成本组实现与测试，实际结果见文末。

- 保持既有单连接正常/退出场景，Resume=nil 不出现隐式重连。
- 真实 Gateway 与原 TCP gRPC Worker：上传中断后源继续产生音频，恢复后 Worker 字节完全一致、无重复上传，Pick/RPC 仍各一次，结果连续应用，最终 registry/tracker 归零。
- 故意丢失音频 ACK：ready 校准减少重放；故意丢失 result_ack：恢复携带 appliedSeq，不重复应用；恢复 ready 丢失后可跳代接回。
- end 已发送但未获确认，以及 end 已被接纳但 completed 未到的两种故障：必要时重发 end，原完成期限不重置，尾部结果完整。
- 初始 ready 丢失无凭据时不自动 start；已完成后的最终确认失败不新建/不重做；resume_unavailable/协议错误/回调错误/缓存满均明确终止。
- 退避、拨号、握手和旧连接清理期间仍接纳源事件；慢清理不会与新 Dial 重叠，反复失败不累计 reader/writer；源只启动/关闭一次。
- session_busy/handshake_limit/entry_timeout 退避重试；非法/未知入口码停止；固定恢复窗口、全场次数、调用方取消及尾部期限分别触发；再次成功连接不重置总次数。
- 取消与候选 ready/done 同时发生时连接不泄漏；旧代事件不修改新代状态；到期 ready 不被接纳。虚拟时间用于预算和竞争，真实网络用于闭环，两类证据分别记录。

验收通过后才制定量化故障实验：固定音频节奏、断点、断网时长和负载，记录尝试分母、成功/明确失败、源字节/Worker 字节、缺口/重复、恢复到首个新结果的时间，以及资源归零情况。当前没有这些量化实验结果；正确性验收见文末。


## 实际实现与接口（2026-10-08）

按明确委托完成本组能力；实际代码将公开 API、连接任务和整场协调分开，便于审查所有权：

- [client_v2.go](../../internal/wsclient/client_v2.go)：增加 Resume 配置、独立配置复制与默认值，以及 ResumeAttempts/ResumedConnections；Resume=nil 沿用单连接行为。
- [client_v2_connection.go](../../internal/wsclient/client_v2_connection.go)：异步限时 start/resume 握手、稳定拒绝分类、唯一候选所有权、固定 reader/writer 与异步实际清理。私有 v2ManagedConnection/v2Dialer 支持可控替身，公开 Run 使用真实 coder/websocket。
- [client_v2_run.go](../../internal/wsclient/client_v2_run.go)：整场拥有源、唯一采集任务和状态；关闭/退避/握手期间持续接纳源事件。旧连接 done 后才开始下一次 Dial；失败不回退 start，初始身份缺失不重建本场，Completed 后不再恢复。
- 既有 [io_v2.go](../../internal/wsclient/io_v2.go) 和 v2ClientState/缓存逻辑继续使用，没有另建严格字段解码器。旧协调测试夹具迁到真实整场协调和清理方法，原有断言保留。

实际 startV2Connection 接收合法 ReadyMessage 值快照，同时初始化 generation 和 lastResultAckWritten，避免握手已确认的位置被重复当成未写 ACK。startV2Handshake 额外接收私有 dialer，方便确定性测试；业务状态仍只由协调者提交。每个恢复尝试独立缩短握手预算，整场 completionDeadline 不随新连接重置。

首次构建因重构后遗留的未使用 fmt 导入失败，测试尚未运行；删除导入后原有配置/并发与真实网络定向回归通过。首轮新增恢复 57 个叶级与真实故障代理 6 个场景通过；随后补充旧代事件隔离、旧连接清理错误、过期原场及空失败分类，并复核错误只保留一次。最终结果如下，无功能测试失败或数据竞争报告。

## 最终验收证据

新增 **17 个顶层 / 67 个叶级场景全部通过**：wsclient 60 个（配置、分类及可控协调/任务），Gateway 7 个真实网络故障场景。wsclient 包累计 79 顶层 / 305 叶级全部通过。全项目串行 race：**2666 叶级通过、0 失败、12 个显式实验跳过**；不将无测试文件包计入实验跳过。

[客户端恢复测试](../../internal/wsclient/client_v2_resume_test.go)使用虚拟时间和同步屏障验证：

- Closing、Dial、ready 握手及 Backoff 四种等待中，采集仍从 2 字节推进到 6 字节，授权仍为 2；源未关闭。慢关闭结束前没有新 Dial，取消后源恰好关闭一次。
- 断线缓存满明确失败，保留原有块；恢复请求报告实际 appliedSeq，跳代 ready 校准 ACK 丢失位置，只发送未接纳后缀，重复结果不再次应用。
- 固定恢复窗口涵盖连续失败和挂起尝试；总次数在成功恢复后继续保留；取消退避、终止拒绝、HTTP 503/403 和清理错误按约定退出。
- end 未确认需重发、以及 inputEnded=true 不重发两种情况下，700ms 时恢复后仍在原 1s 完成期限结束；这属于虚拟时间中的预算正确性，不是恢复延迟数据。
- 回调错误/超时、协议/消息过大、已完成后确认错误、无初始身份均不自动重试；取消与候选/ready 竞争时 Close 恰好一次，过期 ready 拒绝，旧连接排队结果不影响新代。

[真实 Gateway 故障验收](../../internal/gateway/client_v2_resume_integration_test.go)在生产客户端和公开 Gateway 之间加入两条真实 WebSocket 的消息代理，Worker 使用真实 TCP gRPC。代理选择性丢弃消息后关闭连接，未修改 Gateway 内部会话状态。四个恢复场景各执行一场，最终均完成：

| 故障场景 | 本次恢复尝试 / 已接纳恢复连接 | 服务端 ready 代次 | 原 Worker 建流 / 选择 | Worker 音频 / 输入 | 连续应用结果 |
| --- | --- | --- | --- | --- | --- |
| 音频 ACK、已应用结果的 ACK 丢失 | 1 / 1 | 1 → 2 | 1 / 1 | 10 / 10 字节，逐字一致 | seq 1–4，各一次 |
| 首个恢复 ready 丢失 | 2 / 1 | 1 → 2（丢失）→ 3 | 1 / 1 | 10 / 10 字节，逐字一致 | seq 1–4，各一次 |
| end 未到达 Gateway | 1 / 1 | 1 → 2 | 1 / 1 | 10 / 10 字节，逐字一致 | seq 1–4，各一次 |
| end 接纳后 completed 丢失 | 1 / 1 | 1 → 2 | 1 / 1 | 10 / 10 字节，逐字一致 | seq 1–4，各一次 |

前两个场景在恢复请求被代理阻挡期间继续从 pipe 读取剩余音频；只有采集完成才允许恢复进入 Gateway。因此旧源若随断线被关闭，生产者写入会失败，不会把“暂停文件后重连”当成持续采集。四场均核对源关闭一次、最终 handler/活动计数/握手计数及注册表清理；尝试数来自本次最终日志，边界竞争可能出现 session_busy 后额外尝试，不能推断所有运行都只有该次数。

另外三个真实边界场景通过：初始 ready 丢失时无凭据、不再次 start；completed_ack 丢失时保留业务完成、不重做；阻挡恢复至原场实际到期后，收到 resume_unavailable，原 Worker 只建立一次。未确认的远端会话由原恢复/结果保留期限清理，本地 Run 返回不代表远端已经立即归零。

最终命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -p 1 ./... -count=1 -json
```

全项目回归后补上每条 Gateway 入口 handler 的明确返回断言，七个真实故障场景定向 race 复验全部通过；业务代码此后仅调整注释。该次日志为 `/private/tmp/tide-v2-resume-handler-final.jsonl`。

本机原始证据：`/private/tmp/tide-v2-resume-project-final.jsonl`。首次构建、单连接定向、首轮恢复、网络故障及补充定向日志保留在同前缀的 `/private/tmp` 文件；不作为仓库业务产物提交。

## 当前边界与下一步

已接通同一存活 Gateway/Worker 上的有限原场恢复。跨实例身份/Worker 状态重建、初始创建幂等键、完成后终态查询、客户端进程重启后的持久恢复没有实现。源 Close 及结果回调仍须遵守合作取消契约；缓存预算不是总客户端内存上限。

当前 4/4 是四个受控功能场景的结果，不是弱网恢复成功率估计；没有新增真实麦克风、长时恢复、高并发重连或恢复延迟数据。下一步制定固定输入、故障断点/时长和判据的量化实验，接入必要 CLI/故障工具与采集观测；确定性退避在群体恢复前仍需评估随机抖动。
