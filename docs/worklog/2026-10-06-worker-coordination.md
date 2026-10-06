# 第六阶段：Worker 双向协调与结果保留

日期：2026-10-06。状态：开发者已修正首轮问题，内部双向协调验收通过；真实连接恢复尚待接入。依据 `165c117` 的接收任务验收及已有内部上传协调；本轮核心由开发者实现，助手负责测试及既有测试夹具迁移。以下保留设计与首轮评审，最终结果见文末。

## 本轮能力与后续顺序

将实际 uploader、receiver、音频缓冲和结果缓冲接入同一个 runCoordinator。内部命令输入音频后可以收到并保存真实 receiver 交付的更新；断开期间仍读取 Worker；正常 Worker 完成后在固定期限内保留恢复资格和结果。异常或期限到达后取消原 RPC，等待两个 I/O 任务真实退出，再释放缓冲。

随后接结果读取/投递范围、客户端累计应用确认和终态确认，再接真实 WebSocket 连接交接与恢复协议，最后进行端到端故障实验。本轮仍是内部组合，不增加网络恢复率或容量结论。

## 为什么一起接这几部分

receiver.done 仅表示读取任务退出。若此时立即让当前 runCoordinator 返回，其 defer 会关闭恢复资格，外层清理也会结束 RPC；这样客户端断开期间收到的尾部结果虽然保存过，却再也无法恢复读取。另一方面，永远保留已计算完成的会话会占用内存和会话名额。

因此区分两个阶段：Worker 仍在运行；Worker 已正常完成、结果有期限地保留。第二阶段继续处理内部恢复与断开命令，后续接入结果投递和确认。达到固定保留期限或明确关闭后才退出协调者。本轮尚无客户端确认入口，所以没有“确认全部结果后提前退出”路径。

| 方案 | 好处 | 代价与选择 |
| --- | --- | --- |
| receiver 直接写结果缓冲，加锁和 ACK 入口协调 | 少一次事件交接 | 结果序号、释放、关闭与代次分散在多个所有者，不采用 |
| Worker 完成后将结果转移到独立保留服务 | 可独立释放计算会话、扩展持久化 | 需要额外的身份/恢复资格交接与双份生命周期，本阶段不采用 |
| 同一协调者串行管理输入、响应和结果保留 | 已有代次校验、恢复窗口与结果状态在同一处裁决 | 协调者需要明确两个阶段及不同绝对期限，采用 |

无缓冲 receiver.events 保持现状；收到事件就在该 select 分支内同步处理，处理完才回到下一轮。不要为每条结果启动保存 goroutine，也不要同步 Recv 阻塞协调循环。

## 文件与资源结构

将 session_upload.go 中 sessionUploadConfig、sessionUpload、newSessionUpload、runWithUpload 改名为 sessionWorkerConfig、sessionWorker、newSessionWorker、runWithWorker，文件改为 session_worker.go。已有上传阶段 sessionUploadPhase 继续使用，因为它仍只描述上传方向。runCoordinator 的第三个参数改为 worker *sessionWorker，nil 保留纯控制入口。

已有上传专项测试的符号迁移、正值配置和可取消 Recv 替身由助手在验收时调整；核心实现后可先使用 go build ./internal/gateway 检查编译。旧上传专项测试需要更新，不保留生产“只上传不接收”开关来迁就夹具。

```go
// sessionWorkerConfig 描述一场逻辑会话的原 RPC、缓冲预算和等待期限。
// rpcCtx/cancelRPC 必须属于 stream，不绑定某代 WebSocket。
type sessionWorkerConfig struct {
    rpcCtx context.Context // 原 Worker RPC 的生命周期。
    cancelRPC context.CancelCauseFunc // 仅取消该 RPC，不关闭共享 ClientConn。
    stream workerStream // 有效且响应原 RPC 取消的双向流。
    sendTimeout time.Duration // 单次 Send 期限，必须为正。
    tailTimeout time.Duration // 首次合法 end 到 Worker 完成的预算，必须为正。
    statusTimeout time.Duration // Send/CloseSend 返回 EOF 后等待 Recv 终态的预算，必须为正。
    resultRetentionTimeout time.Duration // Worker 正常完成后的结果保留预算，必须为正。
    maxAudioBytes uint64 // 含在途块的音频存储预算，必须为正。
    maxAudioChunks int // 含在途块的槽位预算，必须为正。
    maxResultBytes uint64 // 未确认结果的字符串字节预算，必须为正。
    maxResults int // 未确认结果的条目预算，必须为正。
    maxPendingAudioBytes uint64 // 接纳但未确认处理的音频预算；0 关闭。
}

// sessionWorkerPhase 表示计算与结果保留阶段，不描述连接是否 attached。
type sessionWorkerPhase uint8
const (
    workerRunning sessionWorkerPhase = iota // 上传/识别尚未正常完成。
    workerRetaining // 正常计算完成；继续保留结果及恢复资格，期限不续期。
)

// sessionWorker 组合原 Worker RPC 的双向任务与协调者独占状态。
// 通过构造器创建，以指针使用，一次运行；运行中禁止外部读取可变字段。
type sessionWorker struct {
    config sessionWorkerConfig // 构造后不变。
    input *audioInputBuffer // 协调者独占；I/O 任务全退出后清理。
    results *resultBuffer // 协调者独占；整个协调路径结束后清理。
    uploader *workerUploader // 唯一 Send/CloseSend 任务。
    receiver *workerReceiver // 唯一 Recv 任务。
    phase sessionWorkerPhase // 计算成功只推进一次到 retaining。
    dispatchedBytes uint64 // 已交付 uploader 的音频末端，可能仍在 Send 中。
    processedBytes uint64 // Worker 合法累计处理位置。
}

// newSessionWorker 校验 RPC、四项正期限及两组缓冲预算，创建资源但不启动任务。
// 失败返回 nil；保留音频/结果预算错误的 errors.Is 身份。
// 构造不转移 RPC 清理责任；只有 runWithWorker 启动后接管。
func newSessionWorker(config sessionWorkerConfig) (*sessionWorker, error)

// runWithWorker 恰好启动一次 uploader、receiver 与同一协调循环。
// ctx 属于逻辑会话；now 与 Timer 同步，不可阻塞。
// 返回前取消原 RPC，等待两个 done，再解除 input/results 引用。
// 返回值是生命周期退出原因；nil 仍可表示明确 controlClose，不能单独视为 ASR 成功。
func (s *resumableSession) runWithWorker(ctx context.Context, now func() time.Time, worker *sessionWorker) error

// runCoordinator 串行拥有连接恢复、输入、Worker 事件与结果保留状态。
// worker=nil 保留纯控制行为；退出时关闭恢复资格与 controlDone。
// 不在循环内等待 I/O 任务退出，收尾等待由 runWithWorker 完成。
func (s *resumableSession) runCoordinator(ctx context.Context, now func() time.Time, worker *sessionWorker) error

// acknowledgeProgress 校验累计处理位置，不操作 I/O 或分配结果序号。
// n 不得倒退或超过 dispatchedBytes；重复确认合法。失败不修改状态。
func (w *sessionWorker) acknowledgeProgress(n uint64) error

// offerAudio 先检查音频范围与未处理预算，再接管存储。
// offset 是字节起点，payload 在调用期间只读；失败不推进接纳位置。
// 完整历史重发不增加任何计量；代次/附着资格由协调者在调用前校验。
func (w *sessionWorker) offerAudio(offset uint64, payload []byte) (bool, error)
```

新增带注释的错误：errInvalidSessionWorkerConfig、errInvalidWorkerProgress、errWorkerEndedEarly、errWorkerStatusTimeout、errResultRetentionExpired、errWorkerInputStopped。复用已有 errResultBufferFull、ErrAudioBacklogExceeded 和 ErrTailTimeout；本路径的尾部期限截止于 Worker 正常完成，后续客户端交付由结果保留期限约束，应在注释中区分两个运行路径。uploadUnavailable 的内部名称可同步调整，但不改变 nil 控制模式拒绝音频/end 的含义。

## 输入与进度计量

保持 processedBytes <= dispatchedBytes <= input.input.nextOffset。

nextOffset 是网关已接管的位置。dispatchedBytes 在 jobs <- pending 成功的分支内推进，使用 pending.chunk.offset+len(pending.chunk.data)，然后才清掉 pending.chunk.data。准备好 pending 但尚未交付时不能推进。音频范围此前已验证不溢出，仍应明确检查连续交付起点等于旧 dispatchedBytes，以捕获内部不变量损坏。

不能拿“已成功返回的 Send”当处理确认上限：Worker 可能已经收到音频并报告进度，而 Send goroutine 尚未返回。也不能只拿 nextOffset 当上限：缓冲中尚未交付的音频不可能已被 Worker 处理。

offerAudio 先调用现有 classifyAudio。历史完整重发直接交给 input.offer 处理幂等；新数据才检查候选未处理量 (offset+size)-processedBytes。classifyAudio 已证明 offset+size 不溢出且新块连续；超过启用的 maxPendingAudioBytes 时在 offer 前拒绝，因此触发块不被接纳。范围或预算错误仍按当前内部输入错误规则，回复后退出协调路径。通过检查后调用 input.offer，存储预算仍由该缓冲负责。重复一次纯范围校验可以接受，先保证接纳和预算的原子语义。

Worker progress 更新只交给 acknowledgeProgress；非法进度终止协调。结果 isFinal 只表示片段定稿；result 事件只调用 results.append，失败则明确退出，成功才获得序号。等待事件、保存结果和进度不能改变音频重复判断。

## 协调循环：普通事件与 Worker 结束

保留已验证的输入、上传派发和控制命令逻辑，新增 receiver.events 与 receiver.done 两个 select 分支。只在 workerRunning 且接收终态尚未处理时启用。done 分支先置对应本地 channel 为 nil，之后读取 receiver.err；不关闭 events，不 range events，不反复处理已关闭的 done。

上传结果依然严格匹配 pending；成功音频才 complete，成功 CloseSend 才进入 uploadHalfClosed。新增 uploadAwaitingStatus：上传返回 EOF 后上传停止，只等接收终态及期限，不再派发音频或半关闭。

| 观察到的情况 | 行为 |
| --- | --- |
| receiver.err 非 nil | 返回该错误，由外层统一取消和等待两个方向 |
| 上传返回普通非 EOF 错误 | 返回原错误；取消后等待接收退出 |
| 上传返回 EOF | 不 complete，不直接取消 RPC；进入 uploadAwaitingStatus，首次记录 statusDeadline，继续接收普通响应及最终状态 |
| 等待状态时 Recv 最终报错 | 返回 Recv 的错误身份，不用上传 EOF 遮盖它 |
| 等待状态时 Recv 正常 EOF | 返回 errWorkerEndedEarly；已有上传失败，不能判为完整计算成功 |
| 正常 Recv EOF，但未接纳 end，或尚未交付 CloseSend | 返回 errWorkerEndedEarly，包括还有待发音频的情况 |
| 正常 Recv EOF，CloseSend 已交付但其结果尚未处理 | 记住 recvEOF，继续等匹配的 CloseSend 结果；尾部期限仍生效 |
| 正常 Recv EOF，且匹配的 CloseSend 已成功 | 进入 workerRetaining |

“已交付 CloseSend”包括当前上传阶段为 uploadWaiting 且 pending.kind 为 workerUploadCloseSend；uploadPending 尚未交付不满足。Recv EOF 与 CloseSend 的成功结果处理顺序可以相反，两种都应收敛为同一个正常计算完成状态。此规则沿用 Worker 消费请求流到 EOF 后才正常完成的协议；若模型后端支持主动提前结束，未来应增加明确协议而非放宽此处判断。

uploadAwaitingStatus 期间，当前代次的新 audio/end 请求回复 errWorkerInputStopped，但协调者继续等真实 Worker 状态，不能让这类后续请求覆盖等待中的终态。恢复/断开/关闭命令仍可处理。普通结果仍按预算保存。

除取消/期限及上传 EOF 这一特殊规则外，独立故障以协调者首次观察并接纳的原因为准；不声称能确定不同 goroutine 中错误发生的物理先后。任何错误出口都通过统一收尾等待两个 I/O 任务。

## 绝对期限与结果保留

使用绝对截止时间，Timer 只负责唤醒。可以将现有 expiryTimer 改为一个 wakeTimer，唤醒时间取以下有效期限的最小值；保存当前已设置的截止时间，没有变化时不重复重置 Timer。

- 恢复期限：使用 resumeState.expiresAt；只有真实 detach 建立，沿用已有规则。
- tailDeadline：首次成功接纳合法 end 时设置 now()+tailTimeout；重复 end、detach、resume、进度或文本均不重置。
- statusDeadline：首次处理上传 EOF 时设置 now()+statusTimeout；不能拿它延长已经存在的尾部期限。
- retentionDeadline：正常进入 workerRetaining 时设置 now()+resultRetentionTimeout，只设置一次；同时停用 tail/statusDeadline。

每轮准备派发前检查逻辑 ctx、计算仍在运行时的原 RPC cause、恢复到期及各适用截止时间。select 选择事件后，在推进业务状态前再次执行同一检查，尤其包括接收 EOF 和 CloseSend 成功分支，防止在已过尾部期限时进入 retaining 并删掉原期限。若选中的是控制命令，已经接收的命令必须先取得唯一回复，再退出；若已向 uploader 交付任务，则不能假装没有交付，外层仍取消并等待任务结束。这样 Timer、命令及完成事件同时就绪时，已到期条件不会被偶然选中的分支绕过。

取消原因优先级为逻辑 ctx、运行中的 RPC cause；没有取消时，已过的多个期限取最早绝对截止时间，同一时刻按恢复、尾部、状态等待、结果保留顺序判断。期限一旦观察为到期便不被本轮刚到达的完成事件反转。这是协调者的观察语义，不承诺物理到达时间相同的事件存在全局顺序。

workerRetaining 的含义：两方向已经满足正常完成条件，所有读取事件已经由协调者同步处理，结果仍未获得客户端应用确认。进入时取消已完成 RPC 以释放 context 资源；此后禁用 rpcDone 分支和原 RPC cause 检查，避免把自己为释放资源执行的 cancel 当作会话失败。仍检查逻辑 ctx、恢复期限与保留期限。

保留阶段不安排新的 Worker I/O，仍允许当前代次历史音频和重复 end 按既有规则得到幂等回复；不延长期限。对真正的新音频仍按 ended 规则拒绝。resultRetentionTimeout 在 attached/detached 两种状态都生效，恢复资格取两个期限共同允许的范围。

保留期限到达返回 errResultRetentionExpired。它表示结果交付/保留窗口耗尽，不能被描述为 ASR 推理失败；worker.phase 仍记录此前正常完成。未来加入客户端终态确认后可提前结束保留，本步不从 final 或单次写成功推导应用确认。

## 统一收尾与本轮限制

runWithWorker 完成入口校验后启动两个任务，只调用一次 runCoordinator。协调者结束后，先 cancelRPC(退出原因)，再等待 uploader.done 和 receiver.done，最后将 input/results 置 nil。顺序等待两个已启动任务即可，不需要新增等待 goroutine；取消必须先于等待。协调者的 controlDone 仍只代表控制退出，不能改称完整资源清理结束。

保留阶段内 results 不能提前清空；最终协调者退出后清空。构造预算失败不启动任务、不取消调用方 RPC。父取消、恢复到期、结果超限、非法进度、尾部/状态等待到期和明确关闭均走同一收尾。

此版本保持现有 v1 网络入口；Worker 名额、注册表/准入最终释放、真实客户端结果投递、ACK 与终态确认后续再接。内存结论仍局限于应用缓冲预算，不能把它等同于 gRPC 解码、框架或全进程内存上限。

## 助手验收计划

1. 实际 runWithWorker 同时运行上传和接收；文本/进度交错，断开期间仍保存结果，恢复后继续原流。
2. 处理确认可先于 Send 返回；未交付的缓冲音频不能被确认；重复确认/音频重发不重复计量，非法进度拒绝。
3. 未处理预算严格超限块不接纳，处理进度释放预算；结果满额后取消并等待双向任务退出。
4. 合法 end、CloseSend 成功与 Recv EOF 两种观察顺序；提前 EOF、Send EOF 后真实 gRPC 错误/正常 EOF/永久等候分别裁决。
5. tail/statusDeadline 不因重复 end、断开、恢复或进度更新移动；到期与命令竞争不绕过期限。
6. 正常计算完成后仍可恢复并保留全部结果；保留期限固定，附着状态也受限；内部 RPC 清理取消不结束保留。
7. controlDone 可以先于 I/O 清理；只有两个任务真实退出后 runWithWorker 才返回、缓冲才释放。

测试由助手编写和迁移。该设计记录没有新增测试通过数或业务指标。

## 首轮实现评审与组合测试（2026-10-06）

开发者完成 sessionWorker、双向任务启动/等待、响应处理、处理进度校验、EOF 裁决、绝对期限与结果保留阶段。助手迁移原上传专项测试到实际双向入口，给没有接收行为的上传替身增加可取消的阻塞 Recv；原直接调用 Recv 的尾部检查改为检查真实 receiver 交给协调者保存的结果。原纯控制两组测试用请求 Err 检查建立同步点，取代依赖旧时钟调用次数的暂停。

新增 [session_worker_duplex_test.go](../../internal/gateway/session_worker_duplex_test.go)：17 个顶层、42 个叶级场景。与已有相关部件共 133 个顶层、371 个叶级场景；最终定向 race 运行中 363 个叶级通过，8 个叶级失败，未报告数据竞争。本步未验收通过，核心尚未由助手修改或提交。

已通过的覆盖包括实际双向流、断开期间结果保存、进度先于 Send 返回、未交付音频不得确认、重复确认/音频重发、积压拒绝原子性、结果双预算、两种正常完成观察顺序、提前 EOF、上传 EOF 等待真实状态、固定尾部与状态等待期限、正常完成后保留/恢复，以及两个方向真实退出后才释放缓冲。

失败对应三类修正：

1. **统一取消与期限的停止判定（3 个叶级失败）。** 原 RPC 已取消且恢复期限同时到期时，当前循环先判到期，丢失原取消原因。接收正常 EOF 的分支仅检查期限，没有重新检查逻辑/RPC 取消；若提交正常完成前取消，会误入 retaining，RPC 取消还会被该阶段屏蔽。改为每轮及每个选中分支使用同一停止判定：先取得 at=now()，再检查逻辑 ctx、运行中 RPC cause，最后检查到期。命令退出前仍回复一次；已交付上传任务的事实必须登记并统一收尾。
2. **最早截止时间决定到期原因（3 个叶级失败）。** checkExpired 按恢复/尾部/状态/保留固定类别优先判断，只有期限相同时才应按此顺序。受控夹具暂停协调者至两个期限均过期，分别复现尾部被恢复覆盖、状态等待被尾部覆盖、结果保留被恢复覆盖。应先在适用期限中选择最早时间，平局按固定顺序，再判断是否已到期；选中恢复期限后才调用 resume.expire。
3. **等待真实状态时的输入拒绝不能终止等待（2 个叶级失败）。** CloseSend 返回 EOF 后，新音频被 classifyAudio 的 ended 检查转成 errAudioInputEnded，错误 end 被转成 errAudioEndMismatch，两者都会直接结束循环并取消 RPC。为 uploadAwaitingStatus 增加专门输入分支：资格和代次检查后，完整历史音频/完全相同的已有 end 可返回幂等结果，其他输入回复 errWorkerInputStopped 并继续等待；不得新增输入位置、修改 end 或覆盖 Worker 最终状态。

可选优化：armWakeTimer 目前会停止并重建相同截止时间的 Timer；保存当前已设置时间，最早期限未变化时可直接返回。绝对期限已保证不会续期，这项是减少定时器创建的改进，不是本轮正确性阻塞项。

首次测试运行中的旧时钟暂停点已迁移；新增结果保留测试也曾在读取 results 指针后直接推进虚拟时间，与后续清空指针缺少同步。助手将缓冲引用移到启动前取得，并在保留状态检查后提交无状态变化的旧代 detach 命令建立读取到后续清理的同步；最终 race 运行没有该问题。测试同步问题与上述核心失败分别记录。

复验命令：

```sh
go test -race ./internal/gateway \
  -run '^Test(SessionWorker|SessionControl|WorkerReceiver|DecodeWorkerResponse|ResultBuffer|WorkerUploader|SendWithTimeout|AudioInputBuffer|AudioInputState|ResumableSession|SessionRegistry|ResumeState|SessionIdentity)' \
  -count=1 -timeout=45s
```

本轮为受控内部正确性检查，没有真实 WebSocket 恢复、负载实验或恢复耗时数据。修正后再执行同范围复验，通过后更新验收状态并提交。

## 双向协调验收（2026-10-06）

开发者修正三类问题：每轮及事件分支统一使用 stopCause，先取时间再检查逻辑/RPC 取消及到期；按最早绝对期限选择原因，平局按约定顺序；uploadAwaitingStatus 单独拒绝新输入，保留历史音频和相同 end 的幂等回复，继续等待 Recv 的真实状态。此前 8 个失败场景全部通过。开发者另补充 Recv EOF 已先被观察、随后 CloseSend 返回 EOF 时立即报告 errWorkerEndedEarly，避免无意义等待已知的接收终态。

助手增加该 EOF 顺序及三个相同截止时间的场景，补充局部函数/期限结构注释，并明确 ErrTailTimeout 在 v1 转发路径和内部恢复协调路径的完成边界。核心逻辑由开发者完成；最早期限不变时复用 Timer 仍为可选优化。

最终新增双向测试共 **19 个顶层、46 个叶级场景**；同上述命令运行，与相关恢复部件合计 **135 个顶层、375 个叶级场景（含父测试共 447 项）全部通过定向 race**，无失败、跳过或数据竞争报告。没有据此声称全项目回归或性能收益。

当前实际能力是：内部音频命令驱动原 Worker 双向任务；断开期间继续保存有序结果；进度确认受已交付边界约束；输入和结果预算生效；正常计算完成后在固定期限内保留结果及恢复资格；异常、关闭或到期后取消并等待两个任务退出，才解除缓冲引用。

下一步为同一协调者增加结果读取/投递范围与累计应用确认，再接真实 WebSocket 交接、终态协议和端到端故障验证。当前 v1 网络入口尚未调用这条恢复路径；客户端 ACK、注册表/准入及 Worker 名额的完整清理仍需后续接入。测试里的断开/恢复是内部控制命令，尚无真实网络恢复率、重放量或恢复耗时数据。
