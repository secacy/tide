# 第五阶段起步：可控的 Worker 共享处理容量

日期：2026-09-27。状态：processingSlots 已接入 Worker 逐块处理，全项目 race 回归通过。本文记录方案、实现和验证依据，尚无第五阶段负载实验数据。

## 为什么先做这一项

第四阶段已有单实例准入、操作期限、音频预算与重复清理证据。第五阶段要评估多 Worker 会话分配、稳定容量与瓶颈，首先需要一个能表达共享处理资源的实验后端。

改造前 Mock 的 ProcessingDelay 在各条 StreamingRecognize 流里独立 wait，没有跨流共享的处理名额。增加会话时，各条流都可以同时等待；除了运行时与传输资源，模型内部没有配置的共享处理瓶颈。因此已有 Mock 适合验证单流变慢、停读与恢复，不能仅据其扩展曲线推断模型容量或调度收益。

多 Worker 地址配置仍需补齐，但共享容量模型先以独立小部件实现，避免在调度之前缺少可解释的负载条件。

## 阶段路线与验收口径

1. 为 Mock 增加 Worker 实例共享的处理名额，验证争用、取消及名额归还。
2. 接入每块处理路径，再支持不同端口与处理配置启动多个 Mock Worker。
3. 引入 Worker 列表与新会话分配。一次 ASR 会话固定到一个 Worker；不逐块更换 Worker，不在当前阶段实现流迁移。
4. 先建立简单分配基线，再根据长短会话混合或 Worker 能力差异，比较轮询与按活跃会话等策略。活跃会话数只是负载代理，不能等同推理负载。
5. 在相同输入、总处理名额、网关保护配置与明确成功/延迟条件下做负载实验；区分增加资源带来的扩容和相同资源下的调度改善。

计划记录：每 Worker 活跃会话、处理名额使用、等待处理的时间、未确认音频、客户端结果延迟分布、完成/拒绝/保护失败数、网关与 Worker 资源趋势。稳定容量定义需要在正式实验前固定负载、持续时间、预热、延迟/失败阈值；暂不预填目标数字。

第四阶段尚未证明的全链路缓冲和严格总内存边界仍明确保留，不能因为开始第五阶段就改记为已验证。

## 可选方案与当前选择

| 方案 | 优点 | 代价与当前选择 |
| --- | --- | --- |
| 维持每流独立延迟 | 简单，现有故障可复现 | 没有可配置的 Worker 共享处理瓶颈，不足以单独支撑本阶段容量实验 |
| 一条会话全程占用一个名额 | 易于模拟固定会话槽位，适合某些有状态模型 | 包含静默、网络等待与整个会话驻留，不能区分实际处理与仅保持连接 |
| 每块模拟处理期间占用共享名额 | 能明确模拟多个会话争用并行处理能力 | 需要定义等待/取消与释放规则，也不等于真实 GPU 调度；本阶段选择 |
| 固定执行 goroutine 加音频任务队列 | 可以统一排队并控制队列 | 要额外处理任务取消、音频所有权、结果路由和流内顺序；当前无需先增加这套结构 |
| 直接接真实 ASR | 真实性更强 | 难固定处理速度与瓶颈，也会混入模型/设备细节；仍留第七阶段验证 |

实现选用带缓冲 channel 表示名额。已有每流 goroutine 继续串行执行，每次只在模拟处理期间占用一个名额；名额共享于整个 Worker 实例，不能在每次 RPC 内新建，否则不能约束跨流并行度。不创建额外处理 goroutine 池，不宣称 channel 提供严格公平调度。

已实现的处理顺序为：Recv 一块 → 等待名额 → 模拟处理 → 归还名额 → 更新处理量并发送进度/结果。取消等待时直接退出，不得把未处理块计入处理确认。网络发送、暂停/停读故障注入不占用计算名额，避免将这些等待误计为模型计算；这是实验模型的约定，不是对所有真实 ASR 实现的断言。

处理名额只限制同时进行的模拟处理，不限制活跃 RPC 或等待名额的会话数量。每个等待者仍可能持有一块已接收音频；客户端、gRPC 缓冲和模型状态也不由该名额直接限制。因此不能把它写成完整队列或内存上限。

## 首步任务：独立 processingSlots（已完成）

首步范围为新增 internal/mockasr/processing_slots.go；当时不接入 Worker、不修改配置或启动命令。由开发者写业务部件，助手补测试。后续逐块接入及验收见下文。

```go
// processingSlots 限制一个 Worker 同时进行的模拟处理数量。
// 同一 Worker 的所有会话共享它；它不保存音频，也不统计活跃会话。
// 必须通过 newProcessingSlots 创建，创建后不得复制。
type processingSlots struct {
    tokens chan struct{} // 容量是处理名额上限，元素数是当前占用量。
}

// newProcessingSlots 创建固定容量的处理名额池。
// limit 必须大于 0；0 或负数返回错误。
func newProcessingSlots(limit int) (*processingSlots, error)

// acquire 等待取得一个处理名额，等待期间响应 ctx 取消。
// ctx 必须非 nil。成功时返回归还名额的函数；调用方处理结束后必须调用。
// 归还函数允许重复或并发调用，实际仅归还一次；失败返回 nil 和 ctx.Err()。
func (s *processingSlots) acquire(ctx context.Context) (release func(), err error)
```

行为要求：

1. 先检查 context；已经取消时不取得名额。
2. 在向 tokens 写入一个空值与 ctx.Done 之间 select；写入成功表示占位，满额时等待，不忙轮询。
3. 取得名额后再次检查 context：若此时观察到取消，先归还名额，再返回错误。该检查解决名额与取消同时可用的情况，但不能保证 context 在函数返回后不再取消；后续处理本身仍须响应取消。
4. 成功时返回一个归还闭包，用 sync.Once 确保只从 tokens 取出一次，防止重复调用扣掉他人的名额。
5. 仅返回/归还名额，不关闭 tokens，不创建计时器，不执行音频处理，也不在后台替调用方归还名额。取得后自动因 context 取消而归还，可能早于实际处理退出，从而让实际并行数越界；因此成功取得后的名额由处理调用方负责释放。

构造函数这里要求正值。后续 Worker 配置如保留 0=关闭共享限制，应由 Worker 组装层决定是否创建 processingSlots，不将“无限制”混进这个部件。

预定测试：容量内立即取得、满额等待、归还后继续、等待取消、已取消 context 不占位、重复/并发归还不多扣、非法容量，以及竞争条件下同时持有名额不超过上限。测试验证名额语义，不测模型性能。

## 2026-09-27 独立部件实现验收

本轮按当前源码重新核对并验收：开发者新增 [processing_slots.go](../../../internal/mockasr/processing_slots.go)，已实现正容量校验、可取消获取、取得后再次检查取消，以及 sync.Once 幂等归还。助手未修改业务逻辑，新增 [processing_slots_test.go](../../../internal/mockasr/processing_slots_test.go)。

验证命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
go test -race ./internal/mockasr -count=1 -timeout=30s -v
```

Mock 包测试通过（1.610s），未报告数据竞争。新增八个顶层测试，覆盖：

- 非法容量与容量内获取；满额等待、归还后继续。
- 已取消 context、等待中取消和期限到期，不遗留占位或释放他人的名额。
- 获取成功后的 context 取消不会提前归还；仍由调用方实际结束后释放。
- 旧 release 在后续调用者持有名额时被 32 个 goroutine 重复调用，不会误释放后续占位。
- 100 轮取消与归还竞争，接受成功或取消两种合法结果，并核对所有名额仍可重新取得。
- 24 个调用者共 600 次获取/归还，通过独立持有量计数检查同时持有数不超过容量 3，最后全部名额可复用。

满额等待和取消顺序使用 testing/synctest 验证，不用真实 sleep 猜测等待者是否已经阻塞。并发持有测试仅用 runtime.Gosched 增加交叠，不作吞吐或公平性结论。

本次替换了此前与当前仓库文件不一致的验收文字。当前没有 capacity_experiment_test.go、loadProcessingPool 或第五/六阶段已完成的源码证据，不沿用这些旧描述；以上结论均以本次实际代码和测试为准。

## Worker 接入边界

接入时先明确 Worker 配置的默认/关闭/非法值语义，再让同一 Worker 的各流共享一个 processingSlots。当前 StreamingRecognize 已通过 processChunk 使用该实例共享池；默认配置为 0，仍关闭限制。

逐块集成应只覆盖模拟处理区间：获取名额、执行可取消处理、归还名额，然后更新处理计数并发送进度/文本。无论处理成功或取消都必须归还；不能在循环体中直接累积 defer 到整场会话结束，网络发送、暂停与停读也不应占用计算名额。集成后再验证多流争用及取消，尚不填写容量或调度收益数据。

## 接入 Worker 配置与逐块处理（已验收）

### 配置与 API 的取舍

增加 `ProcessingConcurrency int`：0 关闭共享处理限制，正值表示同一 Worker 同时模拟处理的音频块数量，负值属于配置错误。0 保留既有每流独立延迟行为，便于在相同源码上建立开关对照；不将 1 或某个实验值默认为所有 Worker 的容量。

负值不能静默退化为关闭限制。将 `New(cfg Config)` 改为 `New(cfg Config) (*Worker, error)`，在构造时明确拒绝负值并传播 newProcessingSlots 的错误。相比 panic，错误返回适合未来命令行配置；相比新增第二个构造入口，可集中默认值和校验逻辑。代价是调用点需要适配：开发者改唯一生产调用 cmd/asr-worker，助手在验收时统一适配测试调用，不要求开发者重写测试。

Worker 保存 `slots *processingSlots`，nil 表示共享限制关闭；仅在 New 中为正值创建一次，每条 StreamingRecognize 流使用同一指针。不同 Worker 实例拥有不同的名额池，不使用包级全局池。

### 处理边界

增加 `processChunk(ctx context.Context) error`，只模拟当前有效音频块的处理过程，不更新计数、不收发网络消息：

1. 检查 context，已经取消则返回 ctx.Err()，包括 ProcessingDelay 为 0 的情况。
2. slots 非 nil 时获取名额；失败直接返回原始 context 错误，成功后在本辅助方法内 defer release。
3. 调用现有 wait(ctx, w.cfg.ProcessingDelay)，失败直接返回。
4. 成功等待后返回 ctx.Err()，避免在已观察到取消时报告成功；这不保证返回以后 context 不再取消。

名额的持有范围到本方法返回为止。将 `defer release()` 写在辅助方法里可以同时覆盖成功和取消路径；如果直接写在 StreamingRecognize 的循环体中，defer 会积累到整条流结束才执行，可能使单个流耗尽池后等待自己归还名额。

StreamingRecognize 保留 Recv、空块跳过和 PCM 合法性检查，只将当前 wait(ctx, ProcessingDelay) 改为 processChunk(ctx)，继续在 RPC 边界用 status.FromContextError 转换错误。仅成功后增加 totalBytes/processedChunks，并发送进度和文本。等待名额或模拟处理中取消，不得为该块增加确认量。

进度/结果发送、ResponseDelay、尾部发送以及原有暂停/永久停读位于处理范围外，不持有名额。本 Mock 的容量模型只模拟逐块 ProcessingDelay，不能用于推断这些等待也是实际模型计算。启用并发限制而 ProcessingDelay 为 0 仍合法，但不能据此形成有代表性的模拟处理吞吐瓶颈。

### 生产入口与验收分工

cmd/asr-worker 先调用新的 New 并处理错误，再建立监听器、注册服务和启动 gRPC；正常结束或启动失败时仍应关闭监听器。此小步保留现有启动参数和配置值，不新增多端口/命令行配置入口。共享限制由测试显式配置正值来验证，日常启动继续默认关闭。

开发者修改 worker.go 及 cmd/asr-worker/main.go。完成后助手适配所有受 New 签名影响的测试调用，并补集成验证：

- 0 关闭、正值启用、负值构造失败；同实例共享、不同实例独立。
- 两个流争用一个名额时串行处理，等待者取消不产生进度。
- 持有者处理中取消会归还名额，后续流能继续处理。
- 进度/文本发送阻塞、暂停及停读不占用处理名额。
- 单个长流连续多块不会因 defer 累积而自锁；计数只包含成功处理块。
- 既有 Mock 行为和 Gateway 链路回归保持通过。

以上接入代码现已完成并通过回归，尚无第五阶段负载实验结果。

## 接入初稿检查（2026-09-27，历史记录）

开发者已实现 ProcessingConcurrency、实例共享的 slots 和 processChunk，StreamingRecognize 已调用该辅助方法。助手适配既有 Mock/Gateway 测试中的 New 调用，新增 processing_capacity_test.go 的五个顶层测试、22 个子用例：

- 配置负值拒绝、零关闭、正值启用及既有默认值保留。
- 两条流各处理两块，每块虚拟耗时 10ms：同实例容量 1 总耗时 40ms，容量 2、关闭限制或独立 Worker 各为 20ms；逐流进度独立累计且正常返回 final。
- 分别取消等待者和持有者，覆盖主动取消与期限到期；未完成块无进度，其他流继续处理，退出后池可复用。
- 已取消 context 在零耗时和关闭限制时仍返回取消。
- 进度、partial、final 发送阻塞，以及 ResponseDelay、暂停、停读期间，另一条流仍可处理并确认音频。

上述时间使用 testing/synctest 虚拟时钟，是并发行为证据，不是实测吞吐或稳定容量数据。

首次 Mock 包 race 回归有 6 个子用例失败：新增 4 个取消子用例与既有 TestProcessingDelayCancellation 的 2 个子用例。资源释放和进度断言通过，失败点是 StreamingRecognize 直接返回 context 错误，直接检查 status.Code 得到 Unknown，未保留原有的 Canceled/DeadlineExceeded 方法契约。应在调用 processChunk 失败处恢复 status.FromContextError(err).Err()。当前 grpc-go v1.83.2 的服务端在网络边界另有 context 错误转换，所以这不等于真实客户端一定收到 Unknown。

另有两项代码审查收尾：New 不应丢弃 newProcessingSlots 返回的错误（目前正值检查使该错误分支不可达，但显式传播更便于变更）；cmd/asr-worker 应先构造并校验 Worker，再监听端口，且监听成功后 defer Close。业务代码由开发者修正，测试保持原有契约，不降低断言来通过。此步尚未验收完成。

全项目 `go test -race ./... -count=1 -timeout=180s` 已执行：cmd/gateway 和 internal/gateway 通过（分别 1.581s、12.998s），Mock 包仍仅上述 6 个状态语义子用例失败，未报告数据竞争。未开启额外的可选负载实验。待生产代码修正后再次执行验收；当前不标记完成。

## 接入修正后验收（2026-09-27）

开发者已恢复 StreamingRecognize 在 processChunk 失败处的 gRPC 状态转换，显式传播名额池构造错误，并将 Worker 构造和配置校验移到 net.Listen 之前。助手只整理 Go 格式，未改动业务逻辑。

监听器清理的审查结论修正：当前监听成功后没有提前返回的错误分支，直接进入 grpcServer.Serve；本地依赖 grpc-go v1.83.2 的 Serve 约定并实现返回时关闭传入监听器。因此当前未加 defer listener.Close 不构成已知泄漏，不列为验收阻塞项。若以后在 Listen 与 Serve 之间加入可失败的启动步骤，应明确这些路径的清理责任；显式 defer 可作为入口防护。

执行：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
go test -race ./... -count=1 -timeout=180s
```

全项目通过，无数据竞争报告：cmd/gateway 1.651s、internal/gateway 13.039s、internal/mockasr 2.395s。新增五个顶层测试的 22 个子用例全部通过，此前失败的新增 4 个及既有 2 个取消子用例全部恢复通过。验证入口：[processing_capacity_test.go](../../../internal/mockasr/processing_capacity_test.go)。没有开启额外可选负载实验。

本步收益是 Mock 具备可配置、实例共享且可取消的处理瓶颈，为后续多 Worker 容量对照提供可控条件；不能据虚拟时钟测试推断实际稳定容量或真实 ASR 性能。下一步支持 Worker 监听地址和处理参数的启动配置，再推进多 Worker 新会话分配。

## 下一小步：Worker 启动参数解析（待实现）

目标是用同一份程序启动监听地址和处理能力不同的 Worker，并保留可复制的实验启动命令。此小步先新增 cmd/asr-worker/config.go 的纯参数解析，下一小步再接 main/run 与启动日志。当前可执行程序尚不支持以下参数，不能将示例作为已可用命令。

选择标准库 flag.FlagSet + ContinueOnError：命令行适合少量参数和本地实验，已有 Gateway 采用同样方式；环境变量较隐蔽，YAML/TOML 会引入文件格式和多来源优先级，当前暂不增加。进程配置包含 ListenAddr 和 Mock mockasr.Config；监听地址属于启动入口，不放进模型行为配置。

计划参数：

| 参数 | 默认值 | 语义 |
| --- | --- | --- |
| -listen | :50051 | TCP 监听地址；下一步由 net.Listen 检查可绑定性 |
| -processing-concurrency | 0 | 0 关闭共享限制，正值为处理名额数；负值由 mockasr.New 拒绝 |
| -processing-delay | 0s | 每个有效音频块模拟处理耗时；CLI 拒绝负值 |
| -response-delay | 50ms | 每条文本响应的额外等待；CLI 拒绝负值，可显式设 0s 以隔离处理瓶颈 |

ResponseDelay 不占处理名额，但会延后同一流读取后续音频，容量实验需能显式配置并记录。默认保持当前启动行为，不在此步暴露全部故障注入参数。内部 Mock 对非正延迟的兼容行为保持不变，命令行负值作为输入错误拒绝；并行度的语义校验继续集中在 mockasr.New。

待开发者实现的接口：workerConfig{ListenAddr string; Mock mockasr.Config}；parseWorkerConfig(args []string) (workerConfig,error)。args 不含程序名，解析不监听端口、不启动 goroutine、不创建 Worker、不退出进程。初始化既有 PartialEvery、PartialTexts、FinalText 默认值，使用 StringVar/IntVar/DurationVar 绑定参数；原样返回 flag.ErrHelp 和语法错误；拒绝位置参数、空白监听地址以及负数处理/响应延迟。非空监听地址的语法与绑定由后续 net.Listen 处理。

助手随后补默认值、显式覆盖、持续时间格式、帮助、未知参数、位置参数、空地址和负延迟测试，并组合 mockasr.New 验证负并行度拒绝；后续接线时检查帮助成功退出、配置错误在监听之前失败、双端口启动及启动日志。当前不声称多 Worker 会话分配已实现。
