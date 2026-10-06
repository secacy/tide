# 第六阶段：结果投递授权与累计确认

日期：2026-10-06。状态：实现指导，等待开发者编码。依据 `76d36d8` 的内部双向协调验收。本轮核心由开发者实现，助手负责测试及旧测试调用的迁移。

## 本轮能力与后续顺序

在现有协调循环中接入结果取出、写入成功报告、客户端累计确认，以及带已应用位置的恢复请求。形成真实 receiver 保存结果 → 内部写任务取得一条结果 → 客户端确认释放预算 → 恢复时继续投递的内部组合。

后续接入实际 WebSocket 写任务、终态投递/确认与候选连接原子安装，再完成客户端音频缓存及端到端故障验证。本步的调用方是受信任的内部连接适配器或测试夹具，尚无网络握手、凭据校验入口或真实客户端应用行为。

## 要解决的三个问题

1. 保存过的结果未必已经交给客户端写任务。仅检查 ACK<=lastSeq，会允许清理尚未交付的结果。
2. 客户端可能已读到消息并发送 ACK，但网关还没处理 Write 返回。若只在写入成功报告到达后提高 ACK 上限，会误拒绝有效确认。
3. 断开时客户端可能已应用结果，只是 ACK 丢失。恢复声明必须与代次切换一起校验/提交，否则错误声明可能先夺得连接资格，或释放不该释放的数据。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 写入成功后删除结果 | 状态少 | Write 成功不能证明客户端已应用；无法补齐断线时的未知状态，不采用 |
| ACK 上限等于保存位置 | 实现简单 | 允许确认从未交给写任务的结果，不采用 |
| ACK 上限只在 Write 成功报告后推进 | 易理解为已发送 | 无法处理 ACK 比成功报告更早被协调者观察的顺序，不采用 |
| 交付给内部写任务时登记上限，客户端 ACK 后释放 | 支持上述竞争及 ACK 丢失 | 要维护投递授权、单个在途记录和客户端应用位置，采用 |

requestResult 将“读取下一条”和“授权本次写入”合为一个串行操作。它有状态变化，不能拿它做后台预览。内部唯一写任务必须先得到该操作的回复，再执行实际 Write；协调者发布回复前登记授权上限，因此客户端不可能在合法写出后仍看见旧上限。这里记录的是授权交付，不是网络送达证明；客户端 ACK 仍依赖认证后的客户端诚实报告已连续应用的位置。

本步每代最多保留一个尚未报告写入成功的结果授权。这样不用为每条消息创建独立任务，也不会在内部再建无限结果队列。旧代写任务退出时可能还持有借出的值，其资源上限及取消等待仍须在实际连接接入时保证。

## 状态与不变量

在 sessionWorker 增加 delivery resultDeliveryState。新增文件 session_results.go 放置以下类型和串行方法，仍由同一个 runCoordinator 调用，不增加 goroutine 或锁。

```go
// resultDeliveryState 保存结果交付状态，由唯一协调者串行操作。
// newSessionWorker 初始化 changed；使用后不得复制或并发读取。
type resultDeliveryState struct {
    offeredSeq uint64 // 跨代次保留：曾授权交给内部写任务的最大连续序号。
    cursor uint64 // 当前代已写成功或已获应用确认的位置；下一次从此处读取。
    inFlightSeq uint64 // 当前代已借出且未报告写成功的序号；0 表示没有。
    changed chan struct{} // 协调者独占关闭/替换；调用方仅等待。
}

// resultOffer 是一次取结果操作的值快照。
// available=true 时同时建立唯一写入授权，调用方负责写入或报告连接失效。
// 字符串只读，确认释放缓冲后已经取得的值仍可使用。
type resultOffer struct {
    result retainedResult // available=true 时有效；否则为零值。
    available bool // 本次是否实际取得并授权了一条结果。
    workerCompleted bool // Worker 已正常完成，不表示客户端已收到终态。
    lastSeq uint64 // 快照时最后已保存的结果序号。
    ackedSeq uint64 // 快照时已接纳的累计应用确认。
    changed <-chan struct{} // 一次性状态变化通知；唤醒后重新查询。
}

// offerResult 读取并授权 cursor 后紧邻的一条结果。
// 已有 inFlight 时返回零值和 errResultWriteInFlight。
// 没有下一条时返回 available=false 的快照，不等待、不移动位置。
// 成功交付前登记 inFlightSeq 和 offeredSeq，不释放存储或推进 cursor。
func (w *sessionWorker) offerResult() (resultOffer, error)

// completeResultWrite 接纳当前代的一次写成功报告。
// seq 必须非零且等于 inFlightSeq；失败返回 errResultWriteMismatch，不修改状态。
// 成功清除 inFlightSeq，并将 cursor 推进到 max(cursor,seq)，不释放结果。
func (w *sessionWorker) completeResultWrite(seq uint64) error

// acknowledgeResult 接纳客户端已连续应用至 seq 的声明。
// seq>offeredSeq 返回 errResultAckAhead，不修改状态。
// 其余交给 resultBuffer.acknowledge；推进确认后令 cursor>=ackedSeq。
// 不清除在途写入，即使 ACK 已覆盖它也要等待写任务的报告或断开。
func (w *sessionWorker) acknowledgeResult(seq uint64) (bool, error)

// resetResultDelivery 在有效断开或成功恢复时重置本代写入状态。
// cursor 回到当前 ackedSeq，inFlightSeq 清零；offeredSeq 跨代次保留。
// 只撤销本代授权元数据，旧写任务持有值的释放仍由该任务负责。
func (w *sessionWorker) resetResultDelivery()

// notifyResultChange 关闭旧通知并创建新的未关闭通道，唤醒已有观察者。
// 只能在协调者运行期间调用；最终退出关闭当前通道且不再替换。
func (w *sessionWorker) notifyResultChange()
```

newSessionWorker 成功时创建 delivery.changed。offeredSeq、cursor、inFlightSeq 初始均为 0。新增有注释的错误 errResultWriteInFlight、errResultWriteMismatch、errResultAckAhead。

核心关系：

```text
ackedSeq <= cursor <= offeredSeq <= lastSeq
inFlightSeq == 0，或者它是一条本代已经授权的结果序号
```

offerResult 首先检查没有 inFlight，再调用 results.peekAfter(cursor)。有结果时登记 inFlightSeq=result.seq、offeredSeq=max(offeredSeq,result.seq)，cursor 等写成功或累计确认后再前进。没有结果时仍返回阶段/序号/通知快照。不要自己计算 cursor+1，沿用 peekAfter 对最大序号的处理。

ACK 可以早于当前写成功报告，甚至跨过当前在途结果：旧代曾授权至 6，新代正在重放 3，客户端此时补报已应用至 6。只要符合代次与授权上限，确认合法；cursor 提升到 6，在途 3 仍保留。随后 3 的写成功报告不能把 cursor 从 6 降回 3。客户端必须按结果序号去重应用。

上述方法只操作状态，不负责通知。协调者在下面指定的状态变化成功提交后统一 notify，避免一次恢复产生多次中间通知。

## 命令与调用入口

在 sessionControlKind 末尾追加 controlTakeResult、controlResultWritten、controlResultAck。已有 controlResume 增加已应用结果位置，构成一个原子恢复命令。

sessionControlCommand 新增字段：

```go
resultSeq uint64 // resume 的客户端已应用位置；written/ack 的结果序号。其他命令忽略。
```

sessionControlResult 新增字段：

```go
offer resultOffer // take 成功时的快照，available 可以为 false。
handled bool // written 是否接纳了当前代的匹配报告。
advanced bool // ACK 是否使累计确认前进；重复/旧 ACK 为 false。
```

沿用 submitCommand 的交付契约：交付前可取消；协调者取得命令后调用方等待唯一回复，不能因为请求 ctx 取消丢掉已建立的授权。入口可以放在 session_results.go；requestResume 仍放现有文件。

```go
// requestResult 请求当前代下一条结果，并取得本次写入授权。
// ctx 只控制本次命令；generation 必须是当前 attached 代次。
// 成功返回的 available=false 表示暂时没有下一条，调用方可等待 changed。
// 错误返回零值；调用方不得把该入口当成无副作用的窥视操作。
func (s *resumableSession) requestResult(ctx context.Context, generation uint64) (resultOffer, error)

// reportResultWritten 报告此前取得的 seq 已成功写入该代连接。
// 仅成功 Write 后调用；失败由连接拥有者按错误类别报告断开或结束会话。
// stale/detached 报告返回 false,nil；当前代序号不匹配返回错误。
// 报告使用独立的有效请求 ctx；不能默默丢弃已经成功交付的授权。
func (s *resumableSession) reportResultWritten(ctx context.Context, generation, seq uint64) (bool, error)

// requestResultAck 报告客户端已连续应用至 seq。
// 必须先校验当前附着资格和 generation，再进行幂等/上限判定。
// 新确认返回 true；旧/重复确认 false,nil；错误返回 false。
func (s *resumableSession) requestResultAck(ctx context.Context, generation, seq uint64) (bool, error)

// requestResume 在同一串行处理段校验结果恢复位置并切换连接代次。
// appliedSeq 是客户端已连续应用的位置；不得低于 ackedSeq 或超过 offeredSeq。
// 成功时接纳该累计确认并以它作为新代投递起点，返回新 generation。
// 纯控制模式没有 Worker，只允许 appliedSeq=0。
// 真实候选连接的安装与凭据检查在后续接入时纳入同一个提交边界。
func (s *resumableSession) requestResume(ctx context.Context, appliedSeq uint64) (uint64, error)
```

既有 requestResume 测试调用由助手在验收时补充参数 0；已有测试未做客户端结果授权/ACK，因此原结果位置为 0。纯控制测试通过 submitControl 提交 controlResume 时 resultSeq 零值保持有效。核心实现后可先运行 go build ./internal/gateway。

## 协调分支的具体处理

所有新分支仍沿用 stopCause 和 cmd.ctx.Err 检查，以及已接收命令恰好回复一次的契约。worker=nil 返回 errSessionWorkerUnavailable，纯控制恢复位置为 0 的情况除外。

**take 与 ACK：** 先检查 attached，否则 errSessionNotAttached；再检查当前代次，否则 errSessionGenerationMismatch。通过后调用对应方法，回复结果。错误只拒绝本次操作，保持原状态并继续循环；包括 ACK 越界和并发 take。即使旧代 ACK 的序号小于已确认位置，也必须先被代次检查拒绝，不能走幂等成功。

**written：** worker 存在，但已经 detached 或 generation 不是当前代时，回复 handled=false,nil 并忽略；不能误清新代的 inFlight。当前有效代调用 completeResultWrite，成功回复 handled=true 并通知变化；重复或错误序号返回 errResultWriteMismatch，保持状态。该命令只描述写成功，不能把客户端断开后的失败写报告伪装成成功来释放授权。

**有效 detach：** 只有 resume.detach 返回 true 时，resetResultDelivery 并通知变化，再沿用恢复 Timer 处理。旧代或重复 detach 不清理当前代状态、不续期。写失败时连接拥有者先按已定错误分类报告 detach 或 close；本步不把任何错误一律改成可恢复断开。

**resume：** 本轮扩展已有 controlResume，按以下顺序执行：

1. 纯控制模式要求 resultSeq==0；否则回复 Worker 不可用。
2. 有 Worker 且当前 detached 时，先校验 appliedSeq>=results.ackedSeq，低于返回 errResultReplayGap；再校验 appliedSeq<=delivery.offeredSeq，超过返回 errResultAckAhead。校验失败回复错误并继续等待，代次、期限、结果内容和交付状态均不变化。
3. 调用已有 resume.resume(now())，继续保留 attached 拒绝、到期和代次耗尽规则。失败不接纳任何新的结果确认；若恢复状态已因到期关闭，沿用退出路径。
4. 成功后，worker 存在则 acknowledgeResult(appliedSeq)，再 resetResultDelivery、通知变化。前置范围校验与该提交之间没有并发状态变更，ACK 按不变量不会再发生预期范围错误；意外内部错误须终止协调并明确报告，不能伪装成未发生代次切换的普通拒绝。
5. 按既有逻辑重排 Timer 并回复新代次。tail/status/retention 的绝对期限不变。

本轮没有安装真实候选 WebSocket。未来真实附着必须把候选连接安装与上述提交放在同一个协调处理段，不能先调用这个状态入口再由其他 goroutine 修改连接。

## 通知与生命周期

协调者不能为了等待下一条结果而阻塞。无结果时立即返回包含 changed 的快照；写任务随后可以在 changed、连接取消和会话退出之间等待，再提交下一次请求。

采用关闭并替换通道，避免检查为空与开始等待之间丢失通知：即使新结果在调用方开始等待前已到达，返回的旧通道已经关闭，等待仍会立即返回。它是“状态可能变化”的通知，不是一条结果；多次变化可以合并。调用方每次唤醒必须重新查询并取得新的通道，不得反复监听旧的已关闭通道。

以下成功变化调用 notifyResultChange：结果 append、进入 workerRetaining、有效 detach、成功 resume、匹配的写成功报告、ACK 真正前进。重复/旧 ACK 和被拒绝操作不通知。offerResult 本身只建立授权，不需要额外通知。

协调者最终 defer 中关闭当前 delivery.changed（不替换），再发布 controlDone；worker=nil 跳过。外层仍等待 Worker 双向任务后清理 input/results。已经借出的只读结果值由写任务释放，ACK 不修改其内容。

写任务示意：requestResult → 有结果则 Write → 成功 reportResultWritten → 请求下一条；失败则按类别报告 detach/close。无结果且 workerCompleted=false 时等待 changed；workerCompleted=true 且已无下一条时，应进入后续终态投递流程，本步不编造终态协议或直接判整个会话已清理。

ACK 释放对应前缀并允许后续 Worker 结果复用预算，但不重置任何期限。即使当前所有结果均已确认，也不能直接退出：Worker 可能继续产生结果，或者客户端尚未收到整场完成通知。终态确认与提前结束结果保留留下一步接入。

## 助手验收计划

使用实际 runWithWorker 及可控 Worker 流，在同一协调者上验证：

- 未授权结果不能被 ACK；读到但尚未写成功的授权可以被合法 ACK，确认才释放存储。
- 同代并发取结果只有一个成功建立授权；未完成写入前不借出第二条。值借出后 ACK/槽位复用不改变它。
- ACK 先于写成功报告；写成功报告不得倒退已由 ACK 推进的 cursor；旧/重复 ACK 幂等。
- 正常阶段、等待尾部和结果保留阶段均可取结果和确认，阶段/位置快照符合事实。
- 断开后旧代 ACK 被拒绝、旧代写成功被忽略；恢复后按已应用位置继续，旧回调不清新代在途项。
- ACK 丢失后恢复声明推进确认，减少不必要重放；声明低于已清理位置或高于授权上限时，结果、代次和期限都不变化。
- ACK 释放字节和槽位预算，实际 receiver 随后可继续保存结果；所有结果 ACK 不提前关闭恢复资格。
- 无结果返回立即完成；append/正常完成/代次变化/关闭唤醒旧通知，不漏唤醒、不忙循环。
- 序号与代次边界不回绕，预取消请求无副作用，已交付请求仍返回明确授权，既有双向清理与期限回归通过。

这些是内部数据连续性与所有权检查；真实 Write、凭据绑定、客户端去重持久性和网络恢复效果还需后续端到端验证。本次仅记录设计，没有新增运行指标。
