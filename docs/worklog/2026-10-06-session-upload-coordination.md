# 第六阶段：逻辑会话内部的音频接纳与上传协调

日期：2026-10-06。状态：开发者实现已通过内部协调组合验收；Worker 接收与真实 WebSocket 恢复尚未接入。依据 `b0d7f35`、[上传任务验收](2026-10-05-worker-uploader.md)和[资源所有权约定](2026-10-05-session-resource-ownership.md)。以下保留实现指导，实际验收见文末。

## 要解决的问题与方案取舍

目前恢复状态、音频缓冲和上传任务分别成立，但生产 runControl 尚未协调音频。需要让同一个协调者决定：哪代连接可以交付音频、音频何时进入缓冲、何时交给上传任务，以及发送结果如何归还预算。

例如 A 块正在等待 Worker，客户端此时断开。协调者仍应接受 detach、启动恢复期限；已经接纳的 A 块继续归逻辑会话所有。新连接恢复后，旧连接迟到的音频或 end 不能改变输入状态。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 音频命令处理时直接调用 Send，或交付后同步等上传结果 | 代码顺序直观 | 控制循环被 Worker 耗时占住，无法及时处理 detach、resume、期限；不采用 |
| 控制循环和另一数据协调循环分别管理状态，加锁或跨循环请求 | 两条路径可独立运行 | 代次校验与实际接纳之间容易被 detach/resume 插入；保持原子关系需要额外协议；当前不采用 |
| 一个协调循环管理状态，一个已有上传任务执行阻塞 I/O | 校验代次、接管音频和推进位置在同一串行处理段完成；发送期间仍能处理控制事件 | 要显式维护待交付、等待结果和半关闭状态；采用 |

单协调者只串行处理一场会话自己的状态，不是所有会话共用一个全局循环。这里不承诺严格的控制事件优先级，也不凭部件结构推断高并发容量。

## 本步实现范围

修改 resumable_session.go 和 session_control.go，新增 session_upload.go。复用 workerUploader、audioInputBuffer、现有控制请求提交契约与恢复 Timer。

保留 runControl(ctx, now) 作为已有纯控制部件的入口；把它的循环提取到唯一的 runCoordinator(ctx, now, upload)。runControl 调用 upload=nil；新增 runWithUpload 传入实际上传资源。两种入口对同一个 resumableSession 只能选一个启动一次，不能同时运行两套循环。nil 模式用于保持现有部件测试及调用兼容，不是生产会话的配置选项；未来真实运行入口使用带上传资源的路径。

本步先验证内部命令 → 缓冲 → 上传任务的组合。Worker 接收、最终 RPC 状态裁决、结果保留、真实候选 WebSocket 安装、准入与注册表清理仍需后续接入。特别是 Send 返回 io.EOF 不能代表识别成功；本轮遇到上传错误时停止这条内部运行路径并返回原错误，不输出任何整场识别成功/失败协议消息。实际双向运行器接入时，必须加入接收结果的裁决规则，不能把本步上传错误直接当成最终服务端状态。

## 资源组合与启动入口

使用具体类型组合资源，不新增通用任务框架或启动回调。

```go
// sessionUploadConfig 描述一场逻辑会话已经建立的 Worker RPC 及输入预算。
// rpcCtx/cancelRPC 必须属于创建 stream 的同一 RPC，继承逻辑会话生命周期。
// 不能绑定某一代 WebSocket；Go 类型无法验证这层关联，启动方负责保证。
type sessionUploadConfig struct {
    rpcCtx         context.Context         // 原 Worker RPC 的 context。
    cancelRPC      context.CancelCauseFunc // 取消原 RPC，不关闭共享 gRPC ClientConn。
    stream         workerStream            // 有效、响应 RPC 取消的流。
    sendTimeout    time.Duration           // 单次 Send 期限，必须为正。
    maxAudioBytes  uint64                  // 缓冲字节预算，包含在途块，必须为正。
    maxAudioChunks int                     // 缓冲槽位预算，包含在途块，必须为正。
}

// sessionUpload 组合协调者独占的缓冲和唯一上传任务。
// 构造后只交给一个 runWithUpload；不得复用或并发读取可变字段。
type sessionUpload struct {
    config   sessionUploadConfig // 初始化后不变。
    input    *audioInputBuffer   // 只有协调者调用；上传退出后解除引用。
    uploader *workerUploader     // 通过命令/结果通道交接，run 只启动一次。
}

// newSessionUpload 校验配置并创建空缓冲及任务通道，不启动 I/O 或 goroutine。
// 失败返回 nil 和错误；必要配置缺失使用 errInvalidSessionUploadConfig，
// 缓冲预算错误保留 errInvalidAudioBufferLimits 的 errors.Is 身份。
// stream 的 typed nil 属于调用方违反有效流契约，不要求反射检测。
// 构造成功或失败均不接管 RPC 清理；只有交给 runWithUpload 才转交责任。
func newSessionUpload(config sessionUploadConfig) (*sessionUpload, error)

// runWithUpload 运行同一个协调循环及唯一上传任务，恰好调用一次。
// ctx 属于逻辑会话，now 与 Timer 同步；必要参数 nil 属于编程错误。
// 开始运行后接管 RPC 的取消和 uploader 的等待责任。
// 返回前已取消 RPC、观察到 uploader.done，并解除剩余缓冲引用。
// 返回值是本步协调/上传路径的退出原因，不是整场 ASR 结论。
func (s *resumableSession) runWithUpload(
    ctx context.Context, now func() time.Time, upload *sessionUpload,
) error

// runCoordinator 是唯一状态循环，串行处理控制命令、输入与上传结果。
// upload=nil 时保留已有纯控制行为，并拒绝音频/end 操作。
// 退出时停止 Timer、关闭恢复资格并关闭 controlDone；不在循环内等待 I/O 退出。
func (s *resumableSession) runCoordinator(
    ctx context.Context, now func() time.Time, upload *sessionUpload,
) error
```

newSessionUpload 配置失败时创建者仍负责取消已经建立的 RPC；未启动的对象不得提前登记为正在服务的会话。runWithUpload 在所有入口参数校验完成后启动 uploader；注册统一收尾，确保每条正常返回路径都取消 RPC，再等待 uploader.done，最后将 upload.input 置 nil。该对象只使用一次，不尝试复位后重启。

runCoordinator 的 defer 先完成控制退出，外层 runWithUpload 再执行 I/O 收尾。requestClose 继续只等待 controlDone，不能改成等待 uploader；测试方需要观察 runWithUpload 返回，才能断言本步拥有的上传资源已经退出。将来的整场 cleanupDone 还需要覆盖接收、连接、注册表与准入，本步不提前定义为整场已清理。

## 输入命令及提交契约

沿用 s.commands，新增 controlAudio 与 controlEnd。扩展已有命令/结果，不再开第二条永久协调路径。

```go
// sessionControlCommand 新增字段：其他已有字段及语义保留。
offset  uint64 // audio 的起点，end 的最终接纳位置；控制命令忽略。
payload []byte // 仅 audio 使用；从请求开始到回复前只读，调用方不得修改。

// generation 的注释扩展为：detach/audio/end 的连接代次。

// sessionControlResult 新增字段：只用于成功的 audio/end 回复。
accepted   bool   // 本次是否新接纳；完整重发或重复 end 为 false。
nextOffset uint64 // 回复时 Gateway 已连续接纳的位置，不代表 Worker 处理位置。

// requestAudio 提交当前连接代次的一块音频。
// payload 在函数返回前必须保持只读；成功后缓冲拥有独立副本。
// 返回 accepted、nextOffset、error；任何错误时前两项均为零值。
func (s *resumableSession) requestAudio(
    ctx context.Context, generation, offset uint64, payload []byte,
) (bool, uint64, error)

// requestEnd 提交当前连接代次的输入终点。
// 成功只表示接纳 end；重复 end 返回 false、当前接纳位置、nil。
// 不代表 CloseSend 已执行，更不代表识别完成。
func (s *resumableSession) requestEnd(
    ctx context.Context, generation, finalOffset uint64,
) (bool, uint64, error)

// submitCommand 为一次不可复用命令安装 ctx 和独立容量 1 的 reply。
// 交付前可取消；交付后只等待唯一回复，不能因 ctx 取消提前归还 payload 所有权。
// cmd 的 ctx/reply 由该函数设置，调用者仅提供操作及业务字段。
func (s *resumableSession) submitCommand(
    ctx context.Context, cmd sessionControlCommand,
) sessionControlResult
```

原 submitControl(ctx, kind, generation) 保留为 submitCommand 的轻量包装，requestResume/reportDetach/requestClose 保持签名和已验收语义。由 submitCommand 统一执行当前的“交付后等唯一回复”代码，避免复制三套取消逻辑。不要在提交方直接读取 s.resume 或 upload.input。

处理 audio/end 的顺序：

1. 先检查逻辑会话/RPC 是否已取消；已接收命令仍回复一次，再退出。
2. 检查请求 ctx；请求已取消则回复取消错误，不接纳、不终止其他有效操作。
3. upload=nil 返回 errSessionUploadUnavailable，不改变纯控制循环。
4. 当前必须 attached，否则返回 errSessionNotAttached；再校验 generation，错误返回 errSessionGenerationMismatch。两类请求均不得改变位置、缓冲或 end 状态，也不终止现有会话。
5. audio 调用 input.offer；end 调用 input.acceptEnd。在同一串行处理段完成状态校验、接纳和回复；中间不得向上传任务阻塞交付或等待结果。
6. 成功回复 accepted 和 input.input.nextOffset。失败回复零值及原错误。当前合法代次的范围/终点错误或容量耗尽按不可恢复输入错误处理：先回复，再退出协调循环并统一清理；不默默丢弃或无限等待容量。

判定以协调者处理顺序为准：detach 之前已经被接纳的音频继续属于会话；detach 之后旧连接迟到的输入被拒绝。恢复后的重复音频仍先通过代次检查，再由缓冲判断重复，不能以“反正是重发”为理由绕过代次。

这里的 payload 生命周期依赖“交付后不能放弃等待”。缓冲的副本只在接纳后持有；回复后提交方可以修改原切片。接入 WebSocket 时仍需限制消息尺寸及每连接未完成请求数，不能靠当前缓冲预算推断所有调用方等待中的音频也已受限。

## 上传派发状态及 select

在 runCoordinator 内维护一个本地状态和 pending 命令即可，不给每块音频启动 goroutine。

```go
// sessionUploadPhase 表示协调者眼中的上传操作阶段，不是整场会话状态。
type sessionUploadPhase uint8

const (
    uploadIdle sessionUploadPhase = iota // 无未完成操作，可检查缓冲。
    uploadPending                       // 已准备任务，尚未交付给 uploader。
    uploadWaiting                       // 已交付，等待并处理其唯一结果。
    uploadHalfClosed                    // CloseSend 成功，上传结束；响应仍可能继续。
)
```

每轮先检查逻辑 ctx 和 RPC context.Cause；已知终止时不再准备新任务。只有 uploadIdle 才调用一次 input.take：

- 有块：保存 audio 命令，进入 uploadPending。take 已标记在途，预算仍占用。
- 无块且 input.inputDrained()：保存零 chunk 的 CloseSend 命令，进入 uploadPending。
- 否则保持 uploadIdle，等待输入或控制事件。

根据阶段设置本轮 select 使用的局部通道：仅 uploadPending 启用 jobs 发送通道，仅 uploadWaiting 启用 results 接收通道，其余时候赋 nil。upload=nil 时这两条分支与 RPC 取消分支均禁用。nil channel 的 select 分支不会被选择。

```go
// 示意：省略现有命令、Timer、ctx、RPC 取消分支；不是完整实现。
select {
case jobs <- pending:
    // 到这里 uploader 已接收；同一个任务不能再次交付。
    phase = uploadWaiting
    pending.chunk.data = nil // 保留 kind/offset 供匹配，解除本地 payload 借用。
case result := <-results:
    // 先核对 result.kind/offset 与 pending，再处理原错误或成功结果。
}
```

禁止先执行 `uploader.jobs <- pending` 再进入主 select；即使已有上传 goroutine，也可能阻塞在任务交付上。也不能在 audio 命令处理函数里同步等 results。

结果处理：

1. kind/offset 必须与当前已交付任务一致，否则返回 errWorkerUploadResultMismatch。它表示内部交接约束被破坏，不能调用 complete 继续推进。
2. result.err 非 nil：保留错误身份，退出本步协调路径；不 complete、不重试、不另外递交 CloseSend。
3. audio 成功：调用 input.complete(result.offset)，成功后清空 pending，回到 uploadIdle。complete 错误按内部错误终止。
4. CloseSend 成功：清空 pending，进入 uploadHalfClosed；不取消 RPC、不退出协调者。重复 end 可获得幂等回复，但不能再触发一次 CloseSend。

detach 只改变连接附着状态和恢复 Timer。即使 detached，已接纳音频仍继续派发；已接纳的 end 也可在排空后半关闭。新输入必须等待有效恢复代次。恢复不会清空缓冲、重建 uploader 或重新派发正在等待结果的任务。

主 select 无需增加 uploader.done 分支：当前 uploader 的合法退出原因已由 RPC 取消或已交付任务的结果表达。正常半关闭后 done 会一直就绪，不能把它当成整场完成或让它导致忙循环。收尾阶段才等待 done；主循环处理完成事件使用 results，避开 done 与已发布结果同时就绪的竞争。

## 退出、原因和剩余数据

runCoordinator 返回值约定：显式 controlClose 返回 nil；逻辑 ctx 或 RPC 取消返回对应 context.Cause；恢复过期返回 errResumeExpired；当前代次输入错误、上传错误或内部结果不匹配返回对应错误。nil 只表示明确请求关闭，本步不存在“Worker 正常完成”分支。

收到控制命令后，再次检查逻辑/RPC 取消；请求方沿用 errResumeClosed 作为循环已经结束的回复，运行函数保留实际 cause。命令已经接收时，无论哪个分支退出都先给出唯一回复。与取消同时发生的成功操作以串行处理点为准，不承诺取消发生瞬间之后绝对没有新事件执行。

先完成控制退出，再由 runWithUpload 取消 RPC、等待 uploader.done、解除缓冲引用。在正常半关闭后，只有后续明确关闭/到期/取消等终止原因才进入这个收尾。等待期间没有新的任务交付，因此结果通道容量 1 足以容纳已接收任务的最后一个结果；无需在收尾时通过循环读取结果帮助 uploader 退出。

全局取消与结果同时就绪时可以选择取消并停止数据推进，未消费结果仍由 uploader 完整发布；不要把收尾中遗留的一条成功结果当成整场成功。取消不能强杀不合作的 Send，runWithUpload 必须继续等待真正退出，不能提前释放缓冲或宣称资源已释放。

## 助手后续验收计划

用户完成核心后，助手新增组合测试并继续运行原有控制/Timer 与恢复部件测试。本次只有设计记录，不新增测试数量、恢复耗时或容量结论。

| 场景 | 必须观察到的结果 |
| --- | --- |
| FIFO 与半关闭 | 多块音频按序上传；合法 end 后所有成功发送完成才 CloseSend，且只调用一次；协调者和 RPC 仍保留 |
| Send 阻塞期间 detach/resume | 生产 runCoordinator 能处理断开和恢复请求；期限按实际 detach 启动；已接纳块不丢失、不重复交付 |
| 旧代次 audio/end | 新代次已建立后，旧代请求被拒绝，连续位置和 end 状态不变 |
| 待交付阶段的控制操作 | uploader 尚未接收任务时，协调者仍能处理关闭或取消，不阻塞在 jobs 发送 |
| 重复输入与副本 | 当前代重复块不增加预算/Send 次数；请求返回后修改原 payload 不影响已接纳数据 |
| 协议/容量失败 | 当前代次收到明确错误，位置不越过失败输入；控制关闭，RPC 取消，上传真正退出后运行函数返回 |
| 超时与恢复期限 | Send 阻塞时仍可触发恢复期限；取消原因可追溯，未成功发送的块不 complete |
| 控制退出和上传收尾 | 可控 Send 在取消后延迟返回；controlDone 和 requestClose 先完成，runWithUpload 等 Send 真正返回后才结束 |
| 半关闭后取消/关闭 | 不忙循环、不重复 CloseSend；后续终止能完成本步上传收尾 |
| 原有行为回归 | 纯控制模式、交付后唯一回复、代次隔离、绝对恢复期限及边界竞争规则保持成立 |

本步验证完成后，下一步接入唯一 Worker 接收任务与有界结果保留，并补齐双向完成/错误裁决及尾部期限；随后才能把这些组件用于真实连接恢复。既有 v1 网络路径继续承担当前运行行为。

## 内部协调组合验收（2026-10-06）

开发者完成 [session_upload.go](../../internal/gateway/session_worker.go) 和 [session_control.go](../../internal/gateway/session_control.go)。评审确认输入资格校验、有界接管、select 派发、发送成功释放及半关闭/终止的分工符合本步约定，核心逻辑无需修改。助手补充 submitControl 注释，修正两处遗留指导占位注释与一个注释笔误，执行 gofmt，并新增 [session_upload_test.go](../../internal/gateway/session_worker_test.go)。

本步能力目标是“发送阻塞时，逻辑会话仍可处理控制事件并保持输入所有权”。主体测试启动实际 runWithUpload/runCoordinator，仅 Worker I/O 使用可控替身。为准确覆盖尚未交付的任务及损坏的内部结果，两组测试直接启动实际 runCoordinator，刻意不启动真实 uploader，由测试方管理 RPC 清理/一次性任务结果。这些组不冒充运行入口的资源收尾验收。交付后等待回复的两组则使用命令接收夹具，只验证提交契约。

执行最终验证：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race ./internal/gateway \
  -run '^Test(SessionUpload|WorkerUploader|SendWithTimeout|AudioInputBuffer|AudioInputState|SessionControl|ResumableSession|SessionRegistry|ResumeState|SessionIdentity)' \
  -count=1 -timeout=45s -json
```

新增 **18 个顶层测试、47 个叶级场景**，含父测试共 58 项。与已有相关测试合计 **97 个顶层、259 个叶级场景**，含父测试共 308 项，最终全部通过，无失败、跳过或数据竞争报告。包耗时 4.383 秒，仅为测试运行耗时。

| 行为 | 本轮证据 |
| --- | --- |
| 输入接管与 FIFO | 接纳 2/3/1 字节三块到位置 6；入口回复后复用第一块原切片仍发送原始 ab；最终 Worker 操作恰为 ab/cde/f/CloseSend，无重复发送或提前半关闭 |
| 发送阻塞时恢复 | ab 发送暂停，实际协调者仍完成 detach，在虚拟 3 秒内恢复为代次 2，接纳新块 c；旧代历史块、新块及 end 均被拒绝；最终只发送 ab/c，旧 t=10 恢复期限不结束新代次 |
| 断开后继续处理已接纳数据 | 已接纳音频和 end 后 detach；即使没有连接，仍按 FIFO 发送并半关闭，之后可恢复并获得重复 end 确认 |
| 待交付也能处理控制 | jobs 无接收者时，实际协调者仍可处理 detach/resume、明确关闭或取消；在途块继续占预算，不伪造上传结果 |
| 并发完整重放 | 32 个同位置请求中恰好 1 个新增接纳、31 个重复确认；均返回连续位置 2，一个阻塞 Send，无逐请求上传任务 |
| 双预算与非法输入 | 空块、缺口、部分重叠、位置溢出、错误 end、新输入越过 end、字节满额、槽位满额共八组均先回复原错误再终止，失败输入不推进连续位置；取消在途发送不 complete |
| 预算复用 | 两字节/一槽预算被 ab 占满时完整重放仍可确认；Send 成功且协调者处理结果后，cd 才能接纳；最终发送 ab/cd，一次半关闭 |
| 控制退出与资源退出 | 明确 close、逻辑取消、RPC 取消三组：requestClose/controlDone 可以先结束；Send 仍持数据时 input 保留、运行函数未返回；测试放行 Send 后才退出并解除 input 引用，最后一个取消结果仍可发布 |
| 恢复到期与发送超时 | 发送阻塞的命令 detach/初始 detached 两组，在虚拟 10 秒期限触发 errResumeExpired；单次 Send 虚拟 1 秒触发 ErrWorkerSendTimeout，底层额外收尾 10ms 期间运行入口仍等待，两种底层 nil/EOF 均保留取消原因 |
| 半关闭与错误 | 正常半关闭不取消 RPC、不结束协调者，重复 end 不再半关闭；替身仍可返回尾部。发送 EOF/错误及自定义 CloseSend 错误均保留操作错误、不重试，不作为最终 ASR 状态证据 |
| 请求取消与内部交接 | 接纳前取消不修改音频或 end；已经交付的调用方不因取消放弃唯一回复；错误结果类型/offset 不释放预算 |
| 会话隔离与回归 | 一场发送阻塞或关闭不妨碍另一场发送/半关闭，不取消另一场 RPC；已有纯控制、绝对期限和竞争场景共同通过 |

首次组合运行发现助手超时夹具的一个同步问题：在阶段断言中读取 upload.input，之后虚拟时间推进使运行任务清空该指针，二者缺少显式同步顺序。已在替身 Send 的收尾段增加 channel 放行，建立“断言完成 → Send 返回 → uploader.done → 清空缓冲”的先后关系；最终重新执行相关测试通过。修正没有改动核心逻辑。虚拟时间本身不能替代共享状态的同步边界。

本地临时日志：已有部件基线 `/private/tmp/tide-session-upload-baseline-2026-10-06.jsonl`、首次组合 `/private/tmp/tide-session-upload-first-2026-10-06.jsonl`、最终 `/private/tmp/tide-session-upload-final-2026-10-06.jsonl`。它们是本地复核日志，可能被清理，不作为永久实验归档。

边界：本轮没有运行真实 gRPC/WebSocket 断线实验，未测量生产控制延迟、恢复耗时或高并发容量。没有 Worker 接收任务及最终结果裁决、结果确认/有界保留、候选连接原子安装、完整注册表/准入清理或终态通知。现有 v1 入口未调用本组件，正常半关闭后的内部协调者仍等待明确关闭/取消/恢复到期，不能据此宣称第六阶段完成。

下一轮以可验收能力组织：当前已完成内部输入协调；接下来依次补 Worker 接收与有界结果保留、双向完成和错误裁决及尾部期限、真实连接交接与恢复协议。组合部件后安排端到端恢复验证，不以连续增加独立部件替代业务能力验收。
