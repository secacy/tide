# 第五阶段：并发负载工具与实验口径

状态：有限静音 PCM 源、客户端收发观察、单会话记录器、RunSession、有限并发 RunBatch、批次摘要及 JSON 输出已实现并验收；命令参数、共享校验、运行入口与文件输出已验收。已有多 Worker 配置、会话轮询与连接回收验收；已完成小规模完整链路基线，尚无稳定容量结论。

当前小步：gateway-sampler 参数解析已验收，包名已统一为 main，duration 帮助文案明确文件收尾可能超过预算；45 项参数检查及三个相关包共 448 项检查通过 race。下一步接命令运行、信号与退出码；尚未提供可运行入口或正式采样实验。

## 为什么先准备负载与观测

多个连接能成功建立，只能说明链路可用。容量实验还需要知道每场计划发送多少音频、实际发送多少、结果何时返回，以及失败或提前退出的数量。否则，少发音频、发送被拖慢或提前失败，都可能让延迟看起来更低。

现有 internal/wsclient 已处理 start、二进制音频、end 和双向收发；音频来自 io.Reader，识别结果直接打印。优先复用这些协议能力，后续增加可关闭打印的观测出口；当前不复制一套 WebSocket 协议实现。若后续观测需求侵入普通客户端过多，再评估独立客户端内核。

## 推进顺序

1. 有限静音 PCM 源：每场独立读取固定字节数，复用现有客户端的分块与节奏。
2. 单会话观测：记录计划/实际输入、连接与结果事件、结束原因，验证记录与真实链路一致。
3. 有限并发批次：每批 N 场，每场固定输入长度，结束后不自动补充新会话；保存逐会话原始记录和汇总。
4. 小规模基线与观测补齐：验证负载实际到达、客户端发送落后、服务端活动会话与资源采样；按需要添加结果音频位置对应关系。
5. 正式容量实验：提前确定配置、时长、重复次数、延迟/失败标准，逐级增加负载，保留瓶颈与限制。

固定批次的并发数是计划启动数，不保证全程保持 N 个活动会话。后续持续负载、稳定区间与会话到达模型需另行定义，不能用一次短批次直接宣称稳定容量。

## 首步：有限静音 PCM 源

方案比较：

| 方案 | 好处 | 代价与适用范围 |
| --- | --- | --- |
| 每场打开真实 PCM 文件 | 贴近真实输入，可延续至真实 ASR | 需要准备素材和文件生命周期，时长受素材约束 |
| 预先生成完整音频放入内存 | 简单；只读切片可在会话间共享 | 内存随素材长度增长；必须为每场创建独立读取游标 |
| 按读取请求生成零值 PCM | 无素材依赖、长度精确、源对象占用不随时长增长 | 只能验证当前 Mock 的字节处理与系统行为，不能评估识别质量或真实模型容量 |

本轮选择按需生成。当前 Mock 依据音频字节数和配置延迟处理，不依据语音内容；后续真实 ASR 的静音优化可能使结果失真，届时必须使用代表性真实音频。

新增 internal/loadgen/source.go，提供：

```go
// NewSilenceSource 创建固定长度的 PCM 静音源。
// totalBytes 必须为正，且是 audio.BytesDepth 的整数倍。
// 每次调用返回独立读取游标；一个返回值由单个发送协程使用。
func NewSilenceSource(totalBytes int64) (io.Reader, error)

// silenceReader 按需填充零值 PCM，不保存完整音频，也不负责发送节奏。
type silenceReader struct{}

// Read 将 p 全部清零，返回 len(p), nil；长度限制由外层负责。
func (silenceReader) Read(p []byte) (int, error)
```

NewSilenceSource 先校验长度，再组合 io.LimitReader(silenceReader{}, totalBytes)。不要 make([]byte, totalBytes)，也不要把有读取位置的同一个 io.Reader 交给多场会话共享。可以共享不可变参数，每场分别调用构造函数。Read 必须清零调用方提供的缓冲区，不能假设它本来就是零。

源不分配 goroutine、channel、timer，不持有连接，也不等待 context；它立即生成请求的数据，网络发送与取消仍归客户端负责。PCM 样本对齐在总长度及发送块配置处约束，Reader 自身允许任意大小读取，不把一次 Read 当成一个音频块。

以现有格式 16000Hz、单声道、16bit 计算，每秒 32000 字节，60 秒是 1920000 字节。若客户端 ChunkBytes=3200，则为 600 块，标称每块 100ms。源只保证输入内容与长度，实际发送节奏仍由客户端 Realtime/Pacer 决定。

现有 Pacer 第一块立即发送，之后按累计音频时长排期；因此理想情况下 600 块在约 59.9 秒发完，而不是等待满 60 秒才发完。音频时长与发送墙钟时长必须分别记录。落后时当前 Pacer 会追赶发送，后续观测应记录计划与实际发送时间，不能自动当作严格实时采集。

助手后续检查：非法长度拒绝、已有非零缓冲区确实清零、分次读取精确 EOF、不足一块的尾部、不同源游标独立，以及大计划长度的小量读取不需要预先分配整段内容。本步不启动负载实验，不修改 Gateway/Worker。

## 预先固定的统计原则

- 保存计划会话数、实际尝试数、升级成功数、完整完成数、拒绝/失败/取消数；不能只汇总成功者。HTTP 503 在当前系统也可能表示服务停止，不能不经服务端证据就全部标为容量拒绝。
- 计划音频字节数与客户端成功 Write 的字节数分别记录。Write 成功不表示 Gateway 已读到，更不表示 Worker 已处理。
- 正常完成要同时核对完整输入、end 和预期结束协议；单条 is_final 是片段定稿，不是整场完成。Mock 特定验收可以核对约定尾部结果，不能据此泛化真实 ASR 协议。
- 尾部等待以 end 写入开始到预期尾部结果到达计时，与既有实验口径衔接。未发送 end 或缺少尾部结果时记录缺失，不能记为 0；错误/取消后的结果不混入成功延迟分布。
- 当前 WebSocket 结果没有对应音频位置，Mock 默认 partial 数量也有限。首条结果延迟、尾部等待与整场耗时分别命名；不将它们称为全程逐段转录延迟。需要全程实时性指标时，再补结果对应音频位置及持续结果样本。
- 记录发送落后与实际输入速率。背压导致客户端慢发时，负载已经改变，不能只根据低错误率判定系统扛住了目标负载。
- 客户端活动连接、Gateway 活跃会话、Worker 在处理/等待的流不是同一个指标；客户端结束也不证明服务端资源已经回收。
- 原始记录应允许重算分位数，并记录样本数、失败比例与统计方法；少量成功样本的 p99 不作为可靠容量依据。

正式实验记录代码版本、硬件/进程部署、Worker 数量和各自处理名额/耗时、Gateway 保护参数、音频块/时长/发送节奏、并发模型、预热与观测窗口、重复次数。增加 Worker 同时增加总名额得到的收益属于扩容；相同总资源下比较调度才可用于评价调度策略。

上述是待实现的测量约定，不是已具备的观测能力，也不预先承诺稳定容量数值。

## 首步验收（2026-09-28）

开发者实现 internal/loadgen/source.go，构造函数拒绝非正数和非采样对齐长度，使用 io.LimitReader 包装按需清零的 silenceReader。代码无需修正；助手新增 source_test.go。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/loadgen -count=1 -timeout=30s -v`，5 个顶层测试、12 项叶级检查全部通过，耗时 1.636s，未报告数据竞争。包括：

- 5 类非法输入拒绝；最小采样、完整块、尾部不足一块、60 秒音频的 io.ReadFull 读取与持续 EOF 正确。
- 60 秒输入精确读取 1920000 字节；每轮预先污染缓冲区，确认返回区域清零且尾部未返回区域未被修改。
- 奇数字节及空缓冲区读取不丢失或额外消耗输入。
- 16 个源先耗尽其中一个，另外 15 个并发读取，各自仍取得完整输入，没有共享读取游标。
- 接近 int64 上界的合法逻辑长度能够构造并只读取 3200 字节，不要求分配完整输入；该检查不测量进程总内存上界。

本步只新增独立音频源，未修改客户端、Gateway 或 Worker，因此仅运行新包测试。测试直接读取音频，没有按实时节奏等待 60 秒，也没有执行网络或容量实验。下一步设计单会话观测，再接入并发批次。

## 第二步之一：结果观察出口（2026-09-28，已验收）

当前 receiver.handleTextMessage 解析结果后直接 fmt.Printf。普通示例需要输出，但并发负载需要结构化计数与时间；逐条终端输出可能影响客户端消费速度。本步先允许调用方决定如何消费结果，不实现完整指标报告，也不据此判定会话成功。

### 方案与权衡

| 方案 | 优点 | 代价 / 本步选择 |
| --- | --- | --- |
| 解析 stdout 或日志 | 无需改变调用接口 | 文本格式脆弱、计时点不明确，输出还会影响负载；不采用 |
| Run 完成后返回报告 | 调用方集中读取，适合整场汇总 | 需一并定义发送量、收尾和并发汇总责任；后续仍会设计，目前先解开结果输出 |
| 同步结果回调 | 收到即可观察，保持接收顺序，无额外队列与协程 | 回调阻塞就拖慢接收；本步采用，约束回调只做短操作 |
| 通过 channel 异步投递事件 | 可以让消费者独立工作 | 满队列时仍需选择阻塞或丢事件，并管理退出；当前没有证据需要该复杂度 |

### 接口与实现责任

在 internal/wsclient/client.go 增加命名函数类型及 Config 字段：

```go
// ResultHandler 观察一个成功解析的识别结果。
// receivedAt 是客户端 conn.Read 成功返回后立即记录的本地时间，
// 不是 Worker 生成结果的时间，也不表示该结果对应音频的采集时间。
// 回调在接收协程中同步执行；应快速返回，避免阻塞或进行耗时 I/O。
// 同一场会话内按接收顺序调用；多个 Run 共用回调时，调用方负责并发安全。
type ResultHandler func(result wsprotocol.ResultMessage, receivedAt time.Time)

// OnResult 在识别结果成功解析后调用；nil 表示不观察，也不打印。
// 是否观察不影响协议校验及错误处理。
OnResult ResultHandler // 加入 Config
```

receiver.receive 在 conn.Read 成功后、消息类型检查和 JSON 解码前捕获一次 time.Now()，将其传入 handleTextMessage(data []byte, receivedAt time.Time)。保留当前 Read 错误和关闭码处理。只有完整解码成功的 result 才调用 OnResult；nil 时继续读取，错误消息、非法 JSON、未知类型仍走原错误路径，不能为了禁用输出而跳过解码校验。

handleTextMessage 删除硬编码 fmt.Printf，改为有回调则调用。该回调不返回 error，仅作为观察出口；协议错误仍由客户端判断，外部取消仍用 context。不给每条结果额外启动 goroutine，以保持顺序和有界开销。

cmd/ws-client/main.go 在 Config.OnResult 中显式提供原来的打印行为，保持命令行体验。负载工具将自行构造观察回调，避免逐条打印。本步不把一个可变观察器保存成全局变量；后续每场会话有独立记录状态。

### 计时与范围

receivedAt 包含消息完整到达、网络读取和客户端被调度的影响，不包含本条消息后续 JSON 解码或回调耗时；前一条回调过慢仍会推迟下一次 Read，因此不是完全无开销的观测。数据暂存在回调中不意味着已经计算出转录延迟，当前协议缺少结果与输入音频位置的对应关系。

is_final 仅表示片段定稿。看到 final 不能停止接收或标记整场完成；当前正常关闭与上传完成的业务核对，后续需要在完整会话报告及退出测试中处理。本步保留现有 Run/errgroup 行为，不把观测接口改动当作退出语义验收。

计划验收：partial/final 顺序与字段传递、时间参数传递、nil 回调仍校验协议、错误/非法消息不触发回调，以及实际读取路径和普通命令入口编译。执行结果如下。

### 结果观察出口验收

开发者已在 Config 添加 OnResult 和 ResultHandler；receive 在 Read 成功后记录时间并向解码方法传递；handleTextMessage 只对成功解析的结果同步调用观察者。打印移入 cmd/ws-client 的配置回调，nil 回调仍执行原有协议校验。业务代码无需修正，助手新增 internal/wsclient/receiver_test.go。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/wsclient ./cmd/ws-client -count=1 -timeout=45s -v`：客户端 3 个顶层测试、20 项叶级检查全部通过（1.592s），未报告数据竞争；命令入口编译及默认 vet 通过，该入口没有测试文件。

1. 字段、顺序和传入时间完整保留，回调在解码方法返回前执行。
2. 有/无回调各 7 类消息检查，覆盖有效结果、非法 JSON、结果字段类型错误、Gateway 错误、错误字段类型错误、未知及缺少类型；错误消息不触发结果回调。
3. 5 个真实本地 WebSocket 场景经过 New → Run → receive，服务端校验 start/音频/end 后返回消息。正常有/无回调均完成，二进制消息、Gateway 错误和无观察者时的非法 JSON 均按预期返回错误。
4. 正常链路返回第一段 final、第二段 partial、第二段 final，三条全部按顺序到达；每条时间落在 Run 开始与对应回调开始之间。精确记录位置通过源码审查确认，不将时间区间断言当作解析前后耗时的性能测量。

本步使用测试 WebSocket 服务端验证客户端接线，没有启动 Gateway/Worker，未运行真实音频示例程序或并发容量实验。命令行打印行为完成代码审查和入口编译，未声称有终端输出端到端测试。现有 Run 的退出语义未改动，完整会话成功判定留下一步。

## 第二步之二：发送观察出口（2026-09-28，已验收）

### 问题与选择

音频源被读取不意味着网络 Write 成功，Write 成功也不意味着 Gateway 已读取或 Worker 已处理。只观察成功写入还会遗漏失败的 end 尝试，因此统一记录每次实际 Write 尝试的结果，再由报告层统计成功音频量与失败情况。

| 方案 | 好处 | 代价 / 选择 |
| --- | --- | --- |
| 包装 io.Reader 计数 | 改动集中在音频源 | 只能测取出量，不能测网络成功量或 end；不采用 |
| 分别提供 OnAudioSent、OnEndSent | 成功路径直观 | 丢失失败尝试，新增类别需继续增加接口；不采用 |
| 统一 OnWrite 事件 | 一套接口记录三类写入及失败，计时位置一致 | 调用方需按类型与 Err 分类；本步采用 |
| Client 内置完整统计器 | 调用方直接得到报告 | 汇总规则与传输层耦合，仍需明确并发收尾语义；汇总留后续负载层 |

保持与 OnResult 一致的同步回调方式。只包含小型元数据，不暴露 data 切片，不为事件分配额外 goroutine 或 channel。回调耗时仍会影响下一次发送；观测不等于零开销。

### 接口

新增 internal/wsclient/write_observer.go：

```go
// WriteKind 区分一次客户端写入的业务用途，与 WebSocket 帧类型不同。
type WriteKind string

const (
    WriteStart WriteKind = "start" // 会话开始控制消息。
    WriteAudio WriteKind = "audio" // PCM 二进制音频消息。
    WriteEnd   WriteKind = "end"   // 输入结束控制消息。
)

// WriteEvent 描述一次已经返回的 conn.Write 调用，不代表服务端处理完成。
type WriteEvent struct {
    Kind       WriteKind // 写入用途。
    AudioBytes int       // 本次尝试写入的 PCM 字节数；控制消息为 0。
    StartedAt  time.Time // 紧邻 conn.Write 调用前采样，不含读取、节奏等待和 JSON 编码。
    FinishedAt time.Time // conn.Write 返回后立即采样，不含观察回调执行时间。
    Err        error     // conn.Write 原始错误；nil 表示本次客户端写入成功。
}

// WriteHandler 在发送协程内同步观察写入结果，应快速返回。
// 同一会话内依写入顺序调用；可能与 OnResult 并发执行。
// 多场共用回调也可能并发，调用方负责共享状态的同步。
type WriteHandler func(event WriteEvent)
```

Config 增加 OnWrite WriteHandler，nil 表示不观察、不输出，不能跳过实际写入或错误处理。事件只保存本次字节数，不在 Client 字段保存累计量，避免不同 Run 混账。AudioBytes 表示尝试量；仅 Kind == WriteAudio 且 Err == nil 时才可计入成功写出量。失败 Write 可能已有部分数据进入网络，但 API 没有返回可靠字节数，不能猜测部分成功量。

### 接线顺序

在 sender.go 新增私有 helper：

```go
// writeObserved 执行一次 WebSocket 写入，随后同步报告结果并返回原始错误。
// kind 表示业务用途，messageType 表示 WebSocket 消息类型，data 仅在调用内使用。
func (c *Client) writeObserved(ctx context.Context, conn *websocket.Conn,
    kind WriteKind, messageType websocket.MessageType, data []byte) error
```

helper 顺序为：StartedAt = time.Now → conn.Write → FinishedAt = time.Now → 如有回调则传入事件 → 返回同一个 err。只有 WriteAudio 填入 len(data)，控制消息 AudioBytes 为 0。成功失败均报告一次；不在这里包裹错误，保留现有调用方错误说明和错误链。

sendAudio 在现有 WaitBeforeSend 之后用 helper 替换直接 conn.Write，传 WriteAudio / MessageBinary / buf[:n]；原错误返回、成功后 pacer.Advance、源错误处理顺序保持。节奏等待或读取在调用 Write 前失败，不生成虚构 WriteEvent。

writeJSON 新增 kind WriteKind 参数，先完成 json.Marshal，再调用 helper（MessageText）；编码失败直接返回原编码错误，不产生 Write 事件。send 中 start/end 两处分别传 WriteStart/WriteEnd。end 的开始时间由 helper 紧邻 conn.Write 记录，不从调用 writeJSON 之前计时，避免把 JSON 编码算入 end 写入等待。

命令入口不增加逐块打印。后续负载报告配置 OnWrite；本步仅新增观察能力，保留原正常/异常收发行为。

### 并发与测量边界

OnWrite 由发送协程调用，OnResult 由接收协程调用。Worker 的尾部结果可能已被接收回调观察到，而 end 的写入回调尚未执行；不能按两个回调的到达顺序推断网络因果。报告层应同步保存两侧事实，等待 Run 返回后综合判断，不在结果回调里提前宣布成功或把缺失的 end 时间当成 0。

本次 Write 耗时 = FinishedAt - StartedAt，包含本地写入路径及阻塞等待，不是识别延迟。成功音频量只计 Err == nil 的音频事件。尾部计时可以使用 WriteEnd.StartedAt，但是否存在合法完成、预期尾部以及 end 成功，仍需最终报告核对；WriteEnd 成功不表示识别完成。

本步还没有记录每块计划发送时刻，不能仅凭 Write 耗时证明输入符合实时节奏；发送落后与实际速率留后续负载记录。回调不保留每块大对象，未来汇总也应控制观测记录的内存量。

计划验收：start/audio/end 顺序及控制消息零音频量、完整块和尾部实际字节数、成功/失败时间边界、失败 Write 恰好报告一次且原错误可追溯、读取/节奏等待/编码失败不虚构写入、nil 回调行为不变，并运行此前结果回调回归。执行结果如下。

### 发送观察出口验收

开发者已新增 write_observer.go、Config.OnWrite 和 writeObserved，并接入音频及两类控制消息。时间采样紧邻 conn.Write 前后，JSON 编码和节奏等待在其外部；控制消息 AudioBytes 为 0，成功失败均同步报告，helper 返回原始错误。生产代码无需修正。

助手新增 sender_test.go，并在既有 Run 网络测试中增加写入事件断言。执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/wsclient ./cmd/ws-client -count=1 -timeout=60s -v`，客户端共 10 个顶层测试、32 项叶级检查全部通过（1.578s），未报告数据竞争；命令入口编译及默认 vet 通过，无入口测试文件。

- 新增发送检查共 12 项：空输入、完整块、尾块的事件与实际 WebSocket 消息逐条对照；三种用途的失败写入；有/无观察者时读取及 JSON 编码失败；源在返回部分数据时出错；节奏等待取消；nil 观察者实际成功/失败写入；helper 原错误原样返回。
- 10 字节输入以 4 字节分块，实际音频事件为 4、4、2，start/end 均为 0；源返回 2 字节后报错时只出现 start 和成功的 2 字节音频，没有 end。
- 已关闭连接确定性触发 start/audio/end 三类 Write 失败，各恰好记录一次，失败音频仍保留尝试量，调用方包装错误可以追溯到事件中的原始错误。计时均落在测试调用开始与同步回调开始之间；精确采样位置另经源码检查。
- 节奏取消测试首块对应 10 秒音频，但不等待 10 秒：首块成功回调立即取消 context，下一块在等待目标时刻时退出，只记录第一块成功写入。它不测量长期发送精度或慢网阻塞耗时。
- 原有 20 项接收检查继续通过，其中 5 个真实 WebSocket 场景同时验证 Run 接线输出 start/audio/end，接收错误不抹除此前的成功写入事实。测试分别保存收发记录，等待 Run 返回后断言，不声称任意共享回调实现都并发安全。

本步只验收客户端观测事实，没有实现成功字节数累计或最终成功分类；未运行 Gateway/Worker 容量实验。失败字节如何排除、缺失 end 如何表示以及完整会话报告，仍由下一步汇总设计落实。

## 第三步之一：单会话事实汇总（2026-09-28，已验收）

### 目标与权衡

在 internal/loadgen 新增独立 SessionRecorder，把 OnWrite/OnResult 的观察汇总为 SessionObservation。它不建立连接、不决定会话成败、不保存全部事件或识别正文；后续 runner 再组合计划输入、Run 起止时间和返回错误形成最终报告。当前只提供内存快照，不定义 JSON 导出格式。

| 方案 | 好处 | 代价 / 选择 |
| --- | --- | --- |
| 将每条事件追加到切片 | 细节完整，方便事后分析 | 长时、多会话下记录量持续增长，可能干扰被测系统；不作为默认汇总 |
| 多个 atomic 字段 | 独立计数简单 | 时间、控制消息和相关字段的整体一致读取更复杂；当前不采用 |
| channel 加专门汇总协程 | 一处更新数据 | 新增队列、满队列策略和退出等待；当前没有需要这些复杂度的证据 |
| 每场一个 mutex 和固定字段 | 状态简单、快照一致，跨会话不共享锁 | 同场收发回调短暂竞争；采用，锁内只做赋值、加法和比较 |

保存固定数量的统计字段，不保留音频和结果正文，记录空间不随事件条数增长。这是实现结构上的性质，不是整个负载进程内存上界或零开销的实测结论。后续若需逐块分位数或发送落后轨迹，另行设计有界直方图/采样，当前最大值无法替代分布。

### 类型与接口

新增 internal/loadgen/recorder.go：

```go
// SessionObservation 保存一场会话的观测事实，不表示最终成功或失败。
// 未出现的时间为零值；控制写入 Kind 为空表示尚未观察到该写入。
type SessionObservation struct {
    AudioBytesWritten    int64         // Err == nil 的音频写入累计字节数。
    AudioChunksWritten   int64         // Err == nil 的音频写入次数。
    WriteFailures        int64         // 所有用途的失败 Write 次数，不包含读源/编码错误。
    MaxAudioWriteDuration time.Duration // 所有音频 Write（含失败）的最大耗时。
    FirstAudioStartedAt  time.Time     // 首次成功音频 Write 的开始时间。
    LastAudioFinishedAt  time.Time     // 最后一次成功音频 Write 的结束时间。
    StartWrite           wsclient.WriteEvent // 开始控制消息事件，保留失败事件。
    EndWrite             wsclient.WriteEvent // 输入结束控制消息事件，保留失败事件。
    ResultCount          int64         // 成功解析的结果消息数。
    FinalResultCount     int64         // 其中 IsFinal 的消息数，不是会话完成数。
    FirstResultAt        time.Time     // 第一条结果的 receivedAt。
    LastResultAt         time.Time     // 最后一条结果的 receivedAt。
    LastFinalAt          time.Time     // 最后一条 IsFinal 结果的 receivedAt，不保证是整场尾部。
}

// SessionRecorder 汇总单场会话的有序收发事件。
// 零值可用，首次使用后不得复制；不同会话必须分别创建实例。
// ObserveWrite、ObserveResult 与 Snapshot 可以并发调用。
type SessionRecorder struct {
    mu sync.Mutex
    observation SessionObservation
}

// ObserveWrite 记录来自 wsclient 的一次实际写入结果。
// 同场写入由一个发送协程按顺序提供，成功/失败均可传入。
func (r *SessionRecorder) ObserveWrite(event wsclient.WriteEvent)

// ObserveResult 记录成功解析的结果及其原始接收时间，不保留正文。
// 同场结果由一个接收协程按顺序提供，不在此处重新 time.Now。
func (r *SessionRecorder) ObserveResult(result wsprotocol.ResultMessage, receivedAt time.Time)

// Snapshot 返回锁保护下的一致值副本；运行中快照可能尚未包含全部事件。
// Run 返回后且不再向该实例写入时，才可将快照作为完整观察记录。
func (r *SessionRecorder) Snapshot() SessionObservation
```

本步方法只接受当前 wsclient 产生的有效、有序事件，不作为通用事件导入/纠错接口。每个实例只对应一次 Run，无 Reset、合并或跨会话复用。预期有限音频任务的总量不超过 int64；本步不扩展至无限输入或任意恶意事件导致的计数溢出处理。

### 更新规则

ObserveWrite 加锁后，若 Err != nil 则增加 WriteFailures，但不要直接 return，否则会丢掉失败的 start/end 或失败音频的耗时。

- WriteStart/WriteEnd：原样保存到对应字段，成功失败都保存。当前客户端每场各至多一次，不增加重试或去重策略。
- WriteAudio：先计算 FinishedAt.Sub(StartedAt)，更新 MaxAudioWriteDuration，包含失败尝试。仅 Err == nil 时增加 AudioBytesWritten 和 AudioChunksWritten；首次成功用此前 AudioChunksWritten == 0 判断并记录 StartedAt，每次成功更新 LastAudioFinishedAt。

ObserveResult 加锁；用此前 ResultCount == 0 判断首条，保存传入 receivedAt；每次增加 ResultCount 并更新 LastResultAt。若 IsFinal，再增加 FinalResultCount 并更新 LastFinalAt。不能因已经看到 final 而停止记录后续片段。

Snapshot 加同一把锁，直接返回 observation 值副本；不返回内部指针。当前字段没有 slice/map 或可变记录指针，因此修改返回值的计数、时间或控制事件字段不会改写记录器。错误值按现有客户端只读错误语义保留，不要求对 error 的任意动态类型进行深拷贝。

### 缺失数据与成功判定

EndWrite 的零值 Err 也是 nil，因此单看 EndWrite.Err == nil 无法区分没发送 end 和发送成功。必须先验证 EndWrite.Kind == wsclient.WriteEnd，再检查 Err；StartWrite 同理。没有结果的时间保持零值，后续不能把零时间或缺失样本当作零延迟。

例如一条 3200 字节音频成功，下一条 3200 字节失败，应为 AudioBytesWritten=3200、AudioChunksWritten=1、WriteFailures=1。即使收到 final，也不能仅凭这份记录判定成功。

OnResult 可能早于 end 的 OnWrite 回调取得锁；只分别保存事实，不在线计算尾部等待或设置 Success。下一步等 Run 完成，再结合计划量、实际量、控制消息及协议结束判断，保留“尚无尾部证据”的情况。LastFinalAt 不能无条件当成整场尾部时间。

### 验收计划与结果

助手补测试：零值/缺失控制事件、成功与失败计数、失败控制消息保存、最大耗时包含失败而成功时间范围不包含失败、final 后继续记录、输入时间原样保留、修改快照不影响内部状态、两个实例隔离，以及一个发送协程/一个接收协程/并发快照的 race 检查。不启动网络或正式负载；通过后再指导 runner 接线与完整报告。

开发者已实现 recorder.go，所有更新和快照使用同一把 mutex；失败次数与控制事件保留、音频成功量和最大耗时的口径符合设计，Snapshot 返回值副本。生产代码无需修正，助手新增 recorder_test.go。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/loadgen -count=1 -timeout=30s -v`：全包 11 个顶层测试、23 项叶级检查通过（1.832s），包括新增记录器 6 个顶层测试/11 项检查，以及原音频源 12 项检查；未报告数据竞争。

- 两次成功音频写入 3200、2 字节，随后 3200 字节写入失败，最终成功量 3202 字节、2 块、失败 1 次。失败的 50ms 写入参与最大耗时，成功输入时间范围仍止于此前成功块。
- 首次音频写入即失败时，成功量和成功音频起止时间保持零值；start/end 成功与失败均原样保留，控制写入不混入音频最大耗时，缺失 end 的 Kind 保持空值。
- 连续 5 条结果中两条 final，最终消息数 5、final 数 2；final 后继续记录 partial，LastResultAt 更新而 LastFinalAt 保留最近 final 的原接收时间。
- 修改已返回快照的计数、时间、控制事件字段不影响记录器，后续记录不改变历史快照，两个记录器的字节数相互独立。
- 并发测试用一个发送协程记录 2000 次成功音频写入和一次失败 end，一个接收协程记录 1500 条结果（500 条 final），第三个协程取 2000 次快照。相关计数/时间字段保持一致，最终成功量 6400000 字节、失败 1 次，快照与最终预期相符。

这些数值来自直接构造事件的行为测试，不是实际音频发送、网络吞吐、负载容量或资源上界实验。当前仍未将记录器接入真实 Run，也未实现最终报告或成功分类；下一步完成单会话执行与汇总。

## 第三步之二：单会话执行与最终报告（2026-09-28，已验收）

### 范围与选择

新增 internal/loadgen/session.go，连接有限静音源、独立记录器和 wsclient.Run，产生一次尝试的最终报告。当前用于确定性 Mock；不增加并发批次、命令行或跨会话共享状态，不改 wsclient 的退出逻辑。

完成标准可选：只看 Run 返回 nil（无法证明输入和尾部完整）；再要求任意 final（可能只是较早片段）；校验当前 Mock 的预期尾部并同时核对输入/正常结束（本步选择）；新增通用 session_done 协议（可供未来模型使用，但本步没有必要扩展整个服务端协议）。选择 Mock 明确的完成契约，不把特定文本断言推广为真实 ASR 质量校验。

结果通过 `(SessionReport, error)` 返回：前置配置或构造失败返回零报告和错误，尚未开始尝试；一旦调用 Run，成功失败都返回带起止时间与观测的报告，非完整完成时 error 非 nil。调用方遇错仍应保留报告；未来并发批次不能因单场失败直接丢弃统计。报告不重复存储同一个 error，最终导出时由汇总层把返回错误转换为可保存字段。

### 数据结构

```go
// SessionConfig 定义一次 Mock 负载会话的输入与验证条件。
// 不在执行器里隐式调整块大小、音频量或超时。
type SessionConfig struct {
    URL               string        // Gateway WebSocket 地址。
    AudioBytes        int64         // 计划 PCM 总字节数，正值且采样对齐。
    ChunkBytes        int           // 每次音频写入上限，正值且采样对齐。
    Realtime          bool          // true 按现有 Pacer 节奏发送；false 尽快发送。
    Timeout           time.Duration // 整场期限，涵盖拨号、发送和等待关闭，必须为正。
    ExpectedFinalText string        // 本次 Mock 配置的尾部文本，必须非空，不内置默认文本。
}

// SessionOutcome 表示一次已开始尝试的最终分类；空值表示尚未开始。
type SessionOutcome string
const (
    SessionCompleted SessionOutcome = "completed" // 完成当前 Mock 的全部验收条件。
    SessionFailed    SessionOutcome = "failed"    // 运行错误或完整性检查失败。
    SessionCanceled  SessionOutcome = "canceled"  // 运行失败，观察到取消。
    SessionTimedOut  SessionOutcome = "timed_out" // 运行失败，观察到期限到达。
)

// SessionReport 保存一次已开始尝试的配置、收尾时间和观察记录。
// 它不是服务端资源回收证明，也不是识别质量评估。
type SessionReport struct {
    Config      SessionConfig      // 本次实际采用的负载参数。
    StartedAt   time.Time          // 建立会话 context 前、调用 Run 前记录，包含拨号阶段。
    FinishedAt  time.Time          // Run 返回后立即记录，尚未做报告整理。
    Outcome     SessionOutcome     // 最终分类。
    Observation SessionObservation // Run 返回后读取的完整快照。
    TailLatency *time.Duration     // 仅完整完成时填写；nil 表示缺失，不是零延迟。
}

// RunSession 执行一场有限输入的 Mock 会话，并在双向收发结束后形成报告。
// 前置失败返回零报告；已开始的尝试即使失败也返回完整观察及非 nil 错误。
func RunSession(ctx context.Context, cfg SessionConfig) (SessionReport, error)
```

前置检查 Timeout > 0、ChunkBytes > 0、ExpectedFinalText 非空；音频总量和采样对齐由 NewSilenceSource 检查，块采样对齐与 URL 非空由 wsclient.New 检查。ChunkBytes 要提前检查，避免普通客户端的非正值默认逻辑改变实验输入。非空但不可拨号的 URL 或 HTTP 拒绝等仍作为实际 Run 失败报告，不在本步实现完整 URL 静态诊断。Timeout 可以小于音频发送时长，用于有意的取消/超时验证。

### 执行步骤

1. 完成前置检查，构造 source；声明零值 SessionRecorder 和一个局部 lastResult wsprotocol.ResultMessage。
2. 创建 wsclient.Config，URL/ChunkBytes/Realtime 来自本次参数，ReadLimit 保留当前默认。OnWrite 绑定 recorder.ObserveWrite；OnResult 使用一个短闭包调用 recorder.ObserveResult，并将 result 赋给 lastResult。只保留最后一条消息，不保存结果历史。
3. source 与 client 构造都成功后记录 StartedAt，创建 context.WithTimeout(ctx, cfg.Timeout)，defer cancel。每次调用都拥有独立 source/client/recorder/lastResult；没有全局实例。
4. 调用 client.Run；返回后立即记录 FinishedAt，再读取 runCtx.Err()，最后 Snapshot。必须在 defer cancel 执行前读取 context 状态，不能把自己的清理取消误当成运行取消。
5. 填写 Config、时间和 Observation，再分类。结果回调只由接收协程修改 lastResult，主协程只在 Run/errgroup.Wait 完成后读取，因此不需要为 lastResult 再加锁；不可提前或在 OnWrite 中读取它。

### 运行错误与分类

若 Run 返回非 nil 错误，以返回错误及 Run 返回时采样的会话 context 错误共同分类：任一 errors.Is(..., context.DeadlineExceeded) 为真则 timed_out；否则任一 errors.Is(..., context.Canceled) 为真则 canceled；否则 failed。用 errors.Join(runErr, ctxErr) 保留两者，再按需要用 %w 增加说明。

这里是确定的报告分类规则，不声称还原并发错误的最早根因：网络失败与截止/取消可能接近同时发生。原始错误链保留用于诊断。记录器里的失败事件也保留；不要只返回 context 错误而丢掉网络错误。

若 Run 返回 nil，不因为随后到来的 context 取消改写结果，继续以下完整性检查。检查失败均 classified failed 并返回描述缺口的非 nil 错误。任何非 completed 报告 TailLatency 为 nil。

### 完整完成条件（全部满足）

- Run 返回 nil；当前客户端语义意味着两个方向均正常返回、接收侧观察到 1000 关闭，而不是只收到一个结果。
- StartWrite.Kind == WriteStart 且 Err == nil。
- AudioBytesWritten == cfg.AudioBytes，且 WriteFailures == 0。
- EndWrite.Kind == WriteEnd 且 Err == nil。
- ResultCount > 0，最后收到的消息 lastResult.IsFinal 为真且 Text == cfg.ExpectedFinalText。
- 最后一条结果的接收时间非零，且不早于 EndWrite.StartedAt。先核实控制消息存在、时间非零，再比较；相同时间允许。

满足后 Outcome = completed，TailLatency 指向 `Observation.LastResultAt.Sub(Observation.EndWrite.StartedAt)`。使用 end 写入开始而非完成，是因为尾部可能在 end 的 OnWrite 回调执行前已被接收。该值包含 end 写入及传输/处理等待，是当前 Mock 的客户端尾部等待，不是模型纯计算时延或全程转录 p95。

最后消息是预期 final 的检查故意严格：如果正常关闭却尾部缺失、文本不符，或预期 final 后又收到其他片段，不把它算入当前 Mock 的完整完成数。这不验证 Worker 的实际处理字节数或识别质量；这些仍需服务端测量及未来真实模型评估。lastResult 只留一条受客户端 ReadLimit 限制的消息，退出后释放，不写入报告正文。

### 验收计划与结果

计划补配置拒绝、正常有限输入和尾块、缺少/错误/提前 final、结果错误、异常关闭、取消与超时的本地 WebSocket 验证，确认失败也保留观测、缺失尾部时延为 nil、错误链可诊断，并复验现有 loadgen 测试；再通过现有 Gateway/Mock 实际链路验证一场完整会话。这些属于单会话正确性验收，不是并发容量数据。

开发者已完成 session.go：前置失败返回零报告；独立 source/client/recorder 配合整场 context；Run 返回后采样结束时间与 context 状态，读取快照和最后结果；分类和完整性检查均按约定执行。原始错误链通过 errors.Join 和 %w 保留，只有 completed 生成尾部等待指针。实现无需修改，助手新增 session_test.go。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/loadgen -count=1 -timeout=60s -v`：全包 17 个顶层测试、46 项叶级检查通过（3.777s），包括 RunSession 新增 23 项及原记录器/音频源 23 项，未报告数据竞争。

- 10 类无效配置返回零报告和错误，未进入 Run；HTTP 503 则返回带配置与起止时间的 failed 报告，观测为空，不将其直接命名为容量拒绝。
- 7 类上传完成后的响应/关闭组合：正常 10 字节输入分为 4/4/2，完成 start/audio/end，收到 partial 和预期尾部，报告 completed 且尾部计时等于 LastResultAt - EndWrite.StartedAt；无结果、只有 partial、错误尾部、final 后又有 partial、final 后异常关闭、partial 后 Gateway 错误都返回 failed，保留实际输入与结果数量，TailLatency 为 nil。异常关闭状态可以从返回错误链提取。
- 提前 final 测试以实时节奏发送两块各 32000 字节，首块后服务端返回预期 final，随后仍读完输入并正常关闭；完整输入及 final 都存在，但接收时间早于 end 写入开始，因此判为 failed。约 1 秒块间隔用于构造该场景，不是发送精度或容量测量。
- 等待尾部时的父级取消、500ms 会话期限、500ms 父级期限分别报告 canceled/timed_out/timed_out，保留 10 字节成功输入、错误链及 nil 尾部计时。未把这些预算或测试运行时长当作精确退出延迟指标。
- 实际 RunSession → WebSocket Gateway → TCP gRPC Mock：Mock 单处理名额、无额外模拟延迟，预算 32000 字节；输入 16002 字节分为 6 块，收到 1 条 partial、1 条 final，报告 completed。随后停止接纳，Gateway.Wait 在主动取消 Gateway 之前成功返回，验证正常会话清理。

所有网络服务均在同一测试进程的本地临时端口运行，不是程序入口或多进程部署实验。本步未改动客户端、Gateway、Worker，因此只运行新增执行器所在包及其真实链路测试；未重复全项目测试。尚未运行并发批次或得出稳定容量，后续推进有限并发运行和原始报告保留。

## 第四步之一：有限并发批次（2026-09-28，已验收）

### 负载模型与取舍

一次配置 N 场同参数会话，每场只调用一次 RunSession；无补位、自动重试、到达速率或稳态窗口。共同开始信号只约束最早放行时刻，goroutine 调度和拨号仍有偏差；配置 Sessions=N 不代表 N 个连接已经建立，更不保证全程 N 个活动会话。后续以实际开始时间、服务端观测和输入速率检验负载。

| 方案 | 优点 | 代价 / 选择 |
| --- | --- | --- |
| 顺序调用 N 次 | 最简单，便于单会话诊断 | 无法观察会话争用；不用于本批次 |
| 每场一个 goroutine + WaitGroup | 与有限批次直接对应，无任务队列 | goroutine 和结果记录数量随 N 增长；本步采用 |
| 固定 worker pool 消费更多任务 | 适合总任务量大于并发度的持续运行 | 引入排队和补位，改变负载模型；后续有需要再加 |
| errgroup.WithContext，直接返回每场错误 | 首错后统一终止方便 | 某场被拒绝会取消健康会话，扭曲失败率；不采用这种失败传播方式 |

errgroup 并非不能用，也可以吞掉子任务错误再存储；但本步只有等待语义，WaitGroup 更直接。每场已有独立超时，整批共用调用方 context。内存为 O(N) 的 goroutine 和报告，不在执行期间累计每块音频事件；N 必须由实验人员按客户端资源合理配置，不宣称任意 N 都安全。

### 先统一前置校验

SessionConfig 新增私有 validate() error，在任何网络操作之前检查现有全部参数约束：Timeout > 0、ChunkBytes > 0 且采样对齐、AudioBytes > 0 且采样对齐、ExpectedFinalText 非空、URL 非空。使用 audio.BytesDepth，不增加 URL 在线探测、总量与期限关系等新限制。

RunSession 开头调用 cfg.validate()；source/client 的构造与错误处理保留。这些组件仍独立维护自身约束，而批次层借助共同的会话配置校验，避免同一无效配置启动 N 个必然失败的任务。允许改进错误说明，保持前置失败返回零报告的契约。

### 类型与接口

新增 internal/loadgen/batch.go：

```go
// BatchConfig 定义一次有限批次，不会在会话结束后补充新会话。
type BatchConfig struct {
    Sessions int           // 本批计划调用 RunSession 的次数，必须为正。
    Session  SessionConfig // 所有会话使用的相同负载条件。
}

// SessionResult 保存一个计划位置对应的报告和错误。
// 即使 Err 非 nil，也必须保留 Report 中已发生的事实。
type SessionResult struct {
    Index  int           // 从 0 开始的批次内序号，不是服务端 session ID。
    Report SessionReport // RunSession 原样返回的报告。
    Err    error         // RunSession 原样返回的错误，completed 时为 nil。
}

// BatchReport 保存全部单会话结果，不在本步计算成功率或分位数。
type BatchReport struct {
    Config     BatchConfig     // 本批输入参数。
    StartedAt  time.Time       // 放行所有任务前立即记录，不包含构造 goroutine 的时间。
    FinishedAt time.Time       // 等待所有任务返回后立即记录。
    Results    []SessionResult // 按 Index 保存，长度固定为 Sessions，不按完成顺序排列。
}

// RunBatch 启动一个有限批次，等待每场 RunSession 返回并保存所有结果。
// 前置失败返回零报告；运行中单场失败不作为批次级错误。
// 父 context 取消时仍等待所有任务退出，返回完整报告和父 context 错误。
func RunBatch(ctx context.Context, cfg BatchConfig) (BatchReport, error)
```

### 执行与并发责任

1. 检查 Sessions > 0，再调用 cfg.Session.validate()，最后检查 ctx.Err()。任一步失败返回 BatchReport{} 和可追溯错误，不分配结果切片或启动会话。调用方传入非 nil context。
2. 预分配长度为 N 的 []SessionResult，建立 start := make(chan struct{}) 和局部 WaitGroup。结果切片不得在运行期间 append 或重新赋值。
3. 循环 N 次，每次在启动 goroutine 前 wg.Add(1)；通过参数明确传入 index。协程 defer wg.Done()，先 <-start，再调用 RunSession(ctx, cfg.Session)，将报告和错误一起写到 results[index]。每场只有一个写者，不修改共享计数器。
4. N 个 goroutine 创建完毕后设置 BatchReport.Config 和 StartedAt，close(start) 放行。它只是共同放行信号，不保证同一纳秒执行或网络连接同时建立。
5. wg.Wait() 后立刻记录 FinishedAt，此后主协程才能读取所有结果。采样父 ctx.Err()：若非 nil，返回完整报告及该错误；否则返回报告和 nil。

每个协程写不同切片元素，且主协程在 Wait 返回后才读；不需要为写结果额外加 mutex。不要并发 append，也不要在运行中遍历未完成结果。如果后续要实时展示进度，需另设同步机制。

start 放行前或创建协程期间收到取消，仍会放行已规划的全部 N 个任务；它们调用 RunSession 时使用已取消的 context，通常无需建立连接即可返回 canceled/timed_out 报告。本步不引入 skipped 分类。因此 N 表示 RunSession 调用次数，不是成功拨号或进入服务端的次数。

### 两层错误与取消边界

- 某场的网络错误、尾部缺失、单场 Timeout 到期等，保存在该 SessionResult.Err；其他会话继续，不触发父 context 取消。
- 批次运行完毕且父 context 有效时，RunBatch 的 error 为 nil，即使部分或全部会话失败。nil 表示批次收集工作完成，不表示所有会话成功。
- 父 context 取消或到期时，所有会话共享该停止请求；仍须 Wait 后返回报告，不能 select 到 ctx.Done 就立即返回半填充的结果。
- 父 context 状态在 Wait 后采样，边界上可能所有会话已完成、父取消刚好到来，批次仍返回 context 错误；原单会话 completed 状态不被改写。批次错误用于说明最终观察到的父级停止状态，不宣称是每场结束的原因。
- 单场报告中的 Timeout 从各场开始运行时计算；不是整个批次的额外全局期限。需要限制整批总时长时由调用方提供带期限的父 context。

当前不添加 JSON 输出、成功率/p95 统计、命令行参数或 Worker 调度策略改动；先保证每个结果不遗漏、不互相覆盖，再在下一步整理可保存的批次统计。

### 后续验收

助手验证：无效批次/会话配置及预取消 context 不发请求；服务端屏障证明多个会话确实重叠；混合成功与失败时健康会话继续，报告数为 N 且 Index 唯一；单场期限不取消其他会话；父取消后仍等待全部任务返回且保留各场报告；结果元素隔离及 race 检查。保留现有 RunSession 配置/网络回归，尚无本步新增测试或容量结论。

### 初稿检查与待修正项

RunBatch 使用固定结果切片、按索引写入、统一开始信号和 WaitGroup 收尾；单场错误保存在结果，父 context 错误在全部任务退出后返回，主体符合方案。助手只格式化 batch.go 并新增 batch_test.go，没有修改生产逻辑。

共享 SessionConfig.validate 缺少 AudioBytes 的采样对齐检查。输入 AudioBytes=3、Sessions=3 时，批次前置校验通过，随后每场由 NewSilenceSource 拒绝，最终 RunBatch 返回非零批次报告、三个零值单场报告和 nil 批次错误。虽然没有发出网络请求，但无效实验参数被当作已执行批次收集，违反前置失败返回零报告的约定。应在 AudioBytes > 0 检查后补 `cfg.AudioBytes % int64(audio.BytesDepth) != 0` 的错误返回。

检查期间 session.go 另新增 validateSessionCompletion，目前 RunSession 仍使用原来的内联检查，没有调用 helper；helper 中又保留两套重复条件。若继续此抽取，应保留一套检查（含 end 开始时间非零），补函数说明，并由 RunSession 调用 helper 替代原整段内联判断、统一设置 SessionFailed 和包装错误。未将未调用 helper 的逻辑算作已验证的运行路径。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/loadgen -count=1 -timeout=60s -v`：全包 22 个顶层测试、66 项叶级检查中 65 通过、1 失败（3.772s），未报告数据竞争。失败项为 TestRunBatchPreflight/unaligned_audio；旧 46 项回归全部通过。新增批次共 20 项，结果包括：

- 14 项前置检查中仅音频总量未对齐失败；检查期间 HTTP 请求数为 0，失败在于报告/返回错误契约，而非已经发送非法 PCM。
- 服务端屏障等 8 场全部上传结束后才释放尾部，8 场均 completed，证明真实重叠；每场 10 字节、3 块，索引、时间范围及尾部样本存储独立。
- 5 场混合结果中，错误尾部场景为 4 completed/1 failed；单场期限场景为 4 completed/1 timed_out；批次错误均为 nil。期限场景健康会话先完成，不声称检验了在它到期之后继续运行的同期限会话。
- 父取消和父期限各运行 4 场，均保留 4 份包含完整 10 字节输入的 canceled/timed_out 报告并返回可追溯父级错误；测试服务端处理均结束。
- 3 场 HTTP 503 全部失败，保留 3 个结果，RunBatch 仍返回 nil 批次错误，符合单场错误与批次收集结果分离的约定。

本次属于可控本地 WebSocket 并发正确性测试；旧单会话 Gateway/Mock 回归也通过，但没有进行多 Worker 并发容量实验。待前置校验和未完成的 helper 抽取整理后复验；本次不提交未通过的整体实现。

同轮最新复验：开发者已去掉 validateSessionCompletion 的重复检查，并在 RunSession 中调用它替代内联逻辑；助手补函数注释。按最新代码重新执行同一包 race（省略 -v），仍仅 TestRunBatchPreflight/unaligned_audio 失败，耗时 3.749s；其余检查通过，未报告数据竞争。当前只需补 AudioBytes 采样对齐的共享前置校验，helper 抽取不再是待修正项。测试和文档保留在工作区，尚未提交。

### 修正后验收

开发者已在 SessionConfig.validate 中补齐 `cfg.AudioBytes % int64(audio.BytesDepth) != 0` 的错误返回。助手仅运行 gofmt 规范新增校验行的格式，没有修改逻辑。

再次执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/loadgen -count=1 -timeout=60s -v`：22 个顶层测试、66 项叶级检查全部通过（4.842s），未报告数据竞争。原失败用例现在在批次开始前返回零报告与非 nil 错误，14 项前置检查均不产生 HTTP 请求；8 场重叠、混合结果、父取消/期限收尾及全部拒绝的完整收集继续通过。原 46 项音频源、记录器和 RunSession 检查（含实际 Gateway/Mock 单会话）也全部通过。

本步验收的是有限批次执行、错误隔离与完整结果保留，未计算成功率/分位数、导出原始记录或开展正式容量实验。有限批次已经可以作为下一步统计与输出的输入，仍不把计划 N 当作持续 N 个活动会话。

## 第四步之二：批次统计口径（2026-09-28 设计，2026-09-29 已验收）

### 目的与方案选择

将已收尾的 BatchReport 转成可重复计算的摘要，保持原逐会话记录不变。只有完成率和成功延迟同时展示，才能避免失败越多、剩余少量成功会话的延迟反而越低所产生的误读。本步只实现纯函数，不写文件、不启动网络或新增并发，不在 RunBatch 运行途中计算。

| 方案 | 优点 | 代价 / 选择 |
| --- | --- | --- |
| 只统计平均耗时 | 简单 | 容易掩盖尾部，混入快速失败会造成误导；不采用 |
| 保存成功会话的尾部样本并排序 | 样本精确、算法透明，方便对原报告复算 | O(N) 临时空间、O(N log N) 排序；当前已有 O(N) 报告且每场仅一个样本，本步采用 |
| 固定直方图或近似分位数算法 | 适合大量持续采样 | 需定义桶/精度与合并规则；以后逐块长期观测时再评估 |

默认提供 p50、p95、最小值、最大值和样本数；暂不增加 p99 或均值。当前统计的是 completed 会话的客户端尾部等待，不是逐段转录延迟、纯推理耗时或全部尝试的响应时间。

### 类型与接口

新增 internal/loadgen/summary.go：

```go
// LatencySummary 是非空尾部延迟样本的精确排序摘要。
// 分位数采用 nearest-rank；Samples 必须随延迟一起展示。
type LatencySummary struct {
    Samples int           // 纳入统计的 completed 会话数量。
    Min     time.Duration // 最小尾部等待。
    P50     time.Duration // 排序后第 ceil(0.50 * Samples) 个值。
    P95     time.Duration // 排序后第 ceil(0.95 * Samples) 个值。
    Max     time.Duration // 最大尾部等待。
}

// BatchSummary 保存有限批次的统计，不是稳定容量判定。
type BatchSummary struct {
    PlannedSessions  int           // 本批计划会话数，也是完成率分母。
    Completed        int           // completed 数量。
    Failed           int           // failed 数量，不包括取消/超时。
    Canceled         int           // canceled 数量。
    TimedOut         int           // timed_out 数量。
    CompletionRate   float64       // Completed / PlannedSessions，范围 0..1。
    PlannedAudioBytes int64        // 计划会话数 × 单场计划音频字节数。
    AudioBytesWritten int64        // 所有会话中成功 Write 的音频字节数，包含失败会话已写出的部分。
    Elapsed          time.Duration // 批次 FinishedAt - StartedAt，包含拨号和收尾。
    Tail             *LatencySummary // 只有 completed 会话有样本；无 completed 时为 nil。
}

// SummarizeBatch 汇总已结束的完整批次，不修改输入报告或其指针字段。
// 统计必要字段矛盾、批次缺失结果或发生整数溢出时，返回零摘要和错误。
// 无效配置/预取消产生的零 BatchReport 不作为一个 0% 完成的实验。
func SummarizeBatch(report BatchReport) (BatchSummary, error)

// nearestRank 返回已经升序排列的非空样本的指定分位数。
// 调用方保证 1 <= percent <= 100，函数不排序也不修改输入。
func nearestRank(sorted []time.Duration, percent int) time.Duration
```

### 完成率与样本选择

分母固定为 Config.Sessions，先验证正值且 Results 长度相等。Completed + Failed + Canceled + TimedOut 必须等于 PlannedSessions，不把取消或超时排除后重新计算一个更高的完成率。它衡量当前批次全部计划尝试的完成比例，不是 Worker 对已接纳会话的成功率。

每场 Outcome 为 completed 且 Err == nil，TailLatency 非 nil 且值非负，才符合成功样本契约。任一条件矛盾直接返回错误，不悄悄跳过样本或改写 Outcome。其他三类 Outcome 要求 Err != nil、TailLatency == nil；未知/空 Outcome 返回错误，不归入普通失败以掩盖报告缺项。

失败会话中的成功音频写入仍累加到 AudioBytesWritten；不要只累计 completed 会话的字节数。该值只代表客户端成功 Write，不代表服务端处理量。失败时尝试但未成功的块已经由 SessionRecorder 排除，摘要不从计划量或块数重新推算。

批次级错误不参与该函数：即使 RunBatch 因父 context 返回错误，只要完整报告已收集，仍可汇总。未来输出同时保存批次错误与摘要，不把摘要存在等同于整个实验正常结束。

### 输入检查与执行顺序

1. 检查 Config.Sessions > 0、调用 Config.Session.validate()、Results 长度等于计划数；检查批次 StartedAt/FinishedAt 非零且结束不早于开始。零时长允许，不在这里计算吞吐率。
2. 检查计划音频乘法溢出：单场字节数已经为正，如果 int64(Sessions) > math.MaxInt64 / Config.Session.AudioBytes，返回错误，再进行乘法。
3. 遍历 Results。要求 result.Index 等于当前槽位、result.Report.Config 等于 Config.Session；按上述 Outcome/Err/TailLatency 契约检查并计数。completed 会话的实际成功字节数还必须等于单场计划量。
4. 所有会话的 AudioBytesWritten 必须在 [0, 单场计划量] 内，再累加，保留失败会话已写出的部分。前面已经确认 N × 单场计划量可表示，且恰好只有 N 份结果，因此总实际量也不会溢出，不必再增加重复的累计溢出分支。统计检查不重跑完整协议验收，不对原始错误正文做分类。
5. 将 completed 的 *TailLatency 值复制到新的 []time.Duration，不能重排 report.Results 或改写原指针。遍历完成后计算 Elapsed、CompletionRate。统计结果只保存在局部变量，任一步失败都返回 BatchSummary{} 和错误，不返回半成品摘要。
6. 样本为空时 Tail=nil；否则对新切片 slices.Sort，填 Samples/Min/P50/P95/Max。合法的零延迟样本保留为 0，不能当成缺失值。

本步输入契约是由当前 RunBatch 产生的已收尾报告，不设计通用、不可信 JSON 报告导入器。上面的检查针对计算依赖和基本一致性，不能代替服务端处理进度证据或原始完成协议校验。

### 分位数算法与解释

nearest-rank 使用从 1 开始的位置 rank = ceil(percent * n / 100)，返回 sorted[rank-1]。只调用 p50/p95，所有样本按实际 time.Duration 排序，不先转毫秒截断精度。可用整数计算避免浮点取整：

```go
n := len(sorted)
rank := (n/100)*percent + ((n%100)*percent+99)/100
```

这种分拆避免先做 n*percent 的整数溢出。调用方保证非空和合法百分位，不为此私有 helper 增加通用输入解析。

举例（仅说明规则，非实验结果）：10 场中 8 completed、1 failed、1 timed_out，完成率 0.8；成功尾部样本为 10/20/30/40/50/60/70/80ms，p50=40ms、p95=80ms、Samples=8。失败会话即使 1ms 就返回，也不能混入这些尾部样本。8 个样本时该算法的 p95 就是最大值，必须保留样本数，不以少量成功样本推断稳定容量。

### 验收计划与结果

计划验证四类数量及固定分母、失败会话部分音频计量、全失败时 Tail=nil、合法零延迟、单样本/偶数样本/20 样本分位数、乱序输入和不修改原报告、缺失或矛盾报告被拒绝、计划量和累计量的边界保护，以及对真实 RunBatch 报告的接线。

2026-09-29，开发者已实现 summary.go。初次检查发现缺少 result.Index == i 的槽位校验，检查期间开发者已补齐；正式运行测试时，负序号、重复序号、越界序号和交换槽位四类检查均通过。助手新增 summary_test.go，并在既有混合批次/全部拒绝网络测试中验证摘要接线，随后仅对 summary.go 做 gofmt。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/loadgen -count=1 -timeout=60s -v`：28 个顶层测试、107 项叶级检查全部通过（4.784s），未报告数据竞争。包括新增摘要 6 个顶层测试/41 项检查，及原有 66 项回归。完整日志保留于本地临时文件 `/private/tmp/tide-summary-review.log`，可重现的测试源码在仓库中。

- 人工构造五场混合报告：2 completed、failed/canceled/timed_out 各 1，完成率 0.4；计划 5000 字节，实际 2700 字节，包含三场未完成会话已写出的 400/200/100 字节。成功尾部 20/80ms，样本数 2、p50=20ms、p95=80ms。
- 无 completed 时 Tail=nil；合法 0 纳秒尾部仍形成非空单样本摘要。单样本、纳秒精度、最大 duration、乱序双样本、重复样本、20 样本和 101 样本均按 nearest-rank 计算正确；20 个 1..20ns 样本 p95=19ns，101 个 1..101ns 样本 p95=96ns。
- 29 类无效/矛盾报告均返回零摘要和错误，覆盖配置/数量/时间/计划量溢出、序号/配置不符、实际量越界，以及各 Outcome 与错误/尾部样本契约冲突。多数错误位于第二条记录，确认不返回已累计的部分摘要。
- 汇总不改变原结果顺序、尾部值和指针，修改摘要也不影响输入。接近 int64 上界的合法计划/实际字节数没有溢出，批次零时长可接受。
- 真实 WebSocket 批次的两种 5 场混合场景均汇总为 4 completed、完成率 0.8、实际 50 字节和 4 个尾部样本，分别保留 1 failed 或 1 timed_out。3 场 HTTP 503 全部拒绝时得到失败 3、完成率 0、实际字节 0、Tail=nil。

人工样本数字只验证算法，不是性能实验数据；真实 WebSocket 接线验证也不是稳定容量实验。当前摘要计算已可用，但原始报告、错误及配置的可保存输出尚未实现，后续先补输出再组织可复核实验。

## 第四步之三：可保存的 JSON 输出（2026-09-29，已验收）

### 目的与取舍

将同一批次的负载配置、逐会话观测、单场/批次错误及摘要放在一个文档，支持离线核对与对比；不能只保留终端上的成功率/p95。本步新增输出能力，不运行实验，也不从客户端猜测 Worker/网关配置。正式容量实验还需要代码版本、硬件和服务端参数清单，后续命令入口及实验记录再补。

| 格式 / 实现 | 优点 | 代价 / 选择 |
| --- | --- | --- |
| 终端日志 | 便于临时阅读 | 难以完整、稳定地复算；不作为实验记录 |
| CSV | 表格分析便利 | 配置、摘要和嵌套写入事件通常要拆多份文件；后续可从 JSON 派生 |
| JSON Lines | 适合持续流式落盘 | 需要开始/结束记录和不完整批次规则；当前运行完才汇总，本步不采用 |
| 每批一个 JSON 对象 | 配置、原始统计事实与摘要集中，容易核对 | 编码时暂存整个对象，O(N) 额外空间；本步采用 |

使用独立的私有输出结构体，不给 wsclient.WriteEvent 等运行类型添加 MarshalJSON，也不修改业务结构来适配文件。普通 error 直接编码可能成为空对象；必须转换为明确的字符串/空值。输出类型字段均添加固定 snake_case JSON 标签和含义说明，不使用 map[string]any 拼装整份文档。

### 唯一公开入口

新增 internal/loadgen/json_output.go：

```go
// WriteBatchJSON 将已收尾批次及其批次级错误写为一个 JSON 文档。
// 摘要由 report 重新计算，不接收另一份可能不匹配的摘要。
// 返回值只表示校验/编码/写入是否成功；batchErr 非 nil 不阻止有效报告输出。
// 函数不创建或关闭文件，不修改 report；写入失败可能留下部分字节。
func WriteBatchJSON(w io.Writer, report BatchReport, batchErr error) error
```

调用方必须等 RunBatch 返回后再调用，不允许边修改报告边输出。w 为 nil 时返回错误；不尝试通过反射识别接口中的 typed-nil 指针。无效配置/预取消产生的零报告会被 SummarizeBatch 拒绝，不输出伪造实验结果。

### v1 顶层对象

```go
// batchJSON 是输出格式 v1；所有时长字段以 _ns 明确标记整数纳秒。
type batchJSON struct {
    SchemaVersion       int               `json:"schema_version"`         // 固定为 1。
    TailPercentileMethod string           `json:"tail_percentile_method"` // 固定为 nearest_rank。
    Config              batchConfigJSON   `json:"config"`                 // 共同负载配置，保存一次。
    StartedAt           time.Time         `json:"started_at"`             // 批次开始，转换到 UTC。
    FinishedAt          time.Time         `json:"finished_at"`            // 批次结束，转换到 UTC。
    BatchError          *string           `json:"batch_error"`            // nil 错误输出 null。
    Summary             batchSummaryJSON  `json:"summary"`                // 本次重算的摘要。
    Sessions            []sessionJSON     `json:"sessions"`               // 保持原 Index 顺序。
}
```

不使用 omitempty 隐藏错误、缺失尾部或时间。单会话配置与共同配置的一致性由 SummarizeBatch 检查，因此每条 session 不重复保存配置。

### 嵌套对象字段契约

每一行对应一个私有输出结构体；括号内为有必要明确的类型/单位，其余沿用原字段类型。结构体自身及字段需要简短注释。

| 类型 | JSON 字段 |
| --- | --- |
| batchConfigJSON | sessions；session（sessionConfigJSON） |
| sessionConfigJSON | url、audio_bytes、chunk_bytes、realtime、timeout_ns（int64）、expected_final_text |
| sessionJSON | index、started_at（UTC time.Time）、finished_at（UTC time.Time）、outcome、error（*string）、tail_latency_ns（*int64）、observation（observationJSON） |
| observationJSON | audio_bytes_written、audio_chunks_written、write_failures、max_audio_write_duration_ns（int64）、first_audio_started_at（*time.Time）、last_audio_finished_at（*time.Time）、start_write（*writeEventJSON）、end_write（*writeEventJSON）、result_count、final_result_count、first_result_at（*time.Time）、last_result_at（*time.Time）、last_final_at（*time.Time） |
| writeEventJSON | kind、audio_bytes、started_at（UTC time.Time）、finished_at（UTC time.Time）、error（*string） |
| batchSummaryJSON | planned_sessions、completed、failed、canceled、timed_out、completion_rate、planned_audio_bytes、audio_bytes_written、elapsed_ns（int64）、tail（*latencySummaryJSON） |
| latencySummaryJSON | samples、min_ns、p50_ns、p95_ns、max_ns（四个时长为 int64） |

所有时间戳用 time.Time.UTC() 后交给 encoding/json 输出 RFC3339 格式，不转 Unix 秒，不使用 time.String()。所有 duration 直接转 int64 纳秒，不转整数毫秒，以免截断小样本。CompletionRate 保留 0..1；显示百分号属于界面层。

零值观察时间用 nil 指针输出 null；缺失 start/end 由事件 Kind 为空识别，整个事件输出 null。存在但失败的 start/end 必须保留事件与错误，不能只凭 Err 是否为空决定事件存在。

TailLatency == nil 输出 null；合法 0ns 输出数字 0。摘要无样本时 tail=null。nil error 输出 null，非 nil error 用 Error() 字符串，即使 Error() 是空字符串也不能改成 null。JSON 会转义 errors.Join 产生的换行；文件不保留 error 的 Go 类型/解包结构，Outcome 仍负责稳定分类。写入函数自身的错误则继续通过 %w 保留链。

### 建议的转换 helper

```go
// errorText 保留错误文本；nil 表示不存在错误，而空文本错误仍有值。
func errorText(err error) *string

// optionalTime 将缺失观察时间转成 nil，其余返回 UTC 时间的独立副本。
func optionalTime(at time.Time) *time.Time

// optionalDurationNS 将可选时长复制为整数纳秒，保留 nil 与零的区别。
func optionalDurationNS(value *time.Duration) *int64

// toWriteEventJSON 转换已观察到的控制写入；Kind 为空返回 nil。
func toWriteEventJSON(event wsclient.WriteEvent) *writeEventJSON
```

其他配置、观察、单场、摘要转换按表逐字段赋值，可各自拆私有 helper 并补注释。不能对 report 中的指针执行就地转换，不能改变会话顺序、原始时间的时区或错误对象。输出对象不包含识别正文历史或音频数据；expected_final_text 是当前 Mock 验收配置。

### 写入步骤与失败边界

1. 检查 w != nil；调用 SummarizeBatch(report)，失败则包装错误返回，writer 不收到任何字节。
2. 按字段契约构造输出对象，其中 Summary 只能用步骤 1 的结果，BatchError 来自单独传入的 batchErr。BatchError 非 nil 不视为输出失败，例如整批被取消但报告完整，仍应保存。
3. json.MarshalIndent（两个空格缩进），编码失败包装返回，尚未调用 writer。完成编码后追加一个换行。
4. 调用 w.Write(data)。若 err != nil，通过 %w 返回写入错误；若 err == nil 但 n != len(data)，返回包装后的 io.ErrShortWrite。不要假设 writer 总会一次写满，也不要在这个函数里自动重跑批次。
5. 只有完整写出才返回 nil。不关闭 w，不承诺磁盘持久化，也不承诺写入失败时 writer 中没有半份内容。后续若要保证文件可见时必然完整，再由文件层实现临时文件和最终发布策略。

先构造并完整编码再写，能保证参数/报告/JSON 编码失败不会产生部分输出；不能因此声称底层写入也是原子的。暂存 JSON 的额外空间随 N 增长，是当前有限批次方案的明确取舍。

### 预定验收范围

助手使用 bytes.Buffer 解码验证版本、字段、纳秒/UTC、共同配置、每场观察与错误、重新计算的摘要及顺序；验证 nil/0、空文本错误、errors.Join 文本、失败控制事件、输入不被修改；使用故障 writer 验证错误链、短写及校验失败零写入；使用真实批次报告走完整输出路径。本段为实现前的验收约定，实际结果见下文；本步不发布命令行参数或容量结果。

### JSON 输出验收（2026-09-29）

实现入口：[json_output.go](../../../internal/loadgen/json_output.go)；测试入口：[json_output_test.go](../../../internal/loadgen/json_output_test.go)。本轮未修改生产逻辑。

新增 6 个顶层测试、13 项叶子检查；loadgen 包共 34 个顶层测试、120 项叶子检查全部通过 `go test -race ./internal/loadgen -count=1 -timeout=60s -v`，包耗时 4.755s，未报告数据竞争。该耗时是测试执行耗时，不是链路延迟。

- 用独立 JSON 期望值检查 v1 全部字段、共同配置仅保存一次、完整观察与重算摘要；固定 UTC+8 时间输出 UTC，保留 123ns 精度，导出后输入时区、字段、指针和值不变。
- 检查 0ns、7ns、int64 最大值的整数精度；缺失尾部和时间显式为 null，无完成样本时摘要 tail=null。测试用极值仅验证编码精度，不代表实际延迟。
- 三类失败报告保留原序号、分类及错误；空文本错误保留空字符串，errors.Join 的换行/引号解码后仍一致；失败的 end 写入保留事件，批次错误不阻止导出。
- nil writer 拒绝；报告校验失败和时间编码失败均不调用 Write；底层错误、部分写入伴随错误、无错误短写均正确返回，错误链可用 errors.Is 判断；成功或失败均不关闭调用方 writer。
- 真实本机 WebSocket 两场批次，一场完成、一场尾部文本校验失败；导出保留两场观测，每场 10 字节、3 块、1 条 final，摘要完成率 0.5、实际成功写出总量 20 字节。该场景验证接线与失败留存，不能推导稳定容量。

当前通过 io.Writer 写出完整 JSON 文档；测试使用内存 buffer 和故障 writer，尚未验收文件创建、关闭、覆盖或原子发布。编码暂存仍需 O(N) 额外空间，底层写入失败可能留下部分内容；正式容量测量还需命令入口及环境/服务端配置记录。


## 第五步之一：命令参数与共享校验（2026-09-29，已验收）

### 为什么与方案取舍

现有 RunBatch 和 WriteBatchJSON 已能执行并输出结果，但尚无用户命令入口。先将实验条件解析为明确配置，使坏参数在创建文件、连接和 goroutine 前返回；下一小步再接运行与输出。

- 独立 cmd/loadgen 复用 internal/loadgen，保留 cmd/ws-client 的单文件调试用途。把两类用途并入旧命令会增加运行模式、参数与输出分支，当前选择独立命令。
- 本轮用标准 flag，参数少且与已有服务命令一致；YAML/JSON 配置文件可保存复杂配置，但会引入两套来源的优先级规则；后续批量实验有需要再增加。
- 暂时直接配置每场 audio-bytes，复用已有精确字节口径；audio-duration 更直观，但要另外定义不足一个 PCM 采样的取整、溢出及与字节参数冲突规则。本步采用字节数，60 秒对应 1920000 字节。
- 每处各写校验会产生规则漂移，依赖 RunBatch 校验又太晚。提取无副作用 BatchConfig.Validate，命令解析、RunBatch、SummarizeBatch 共用；每个入口继续检查自身负责的条件。

### 本步实现接口

在 internal/loadgen/batch.go 增加：

```go
// Validate 检查批次参数及计划总音频量是否可用 int64 表示。
// 不分配会话、不建立连接，也不检查目标服务是否可达。
func (cfg BatchConfig) Validate() error
```

顺序：Sessions > 0；调用已有 cfg.Session.validate()；随后检查 int64(cfg.Sessions) > math.MaxInt64/cfg.Session.AudioBytes 时拒绝。必须先校验单场字节数为正，再做除法。只验证可表达性，不把 int64 上界当成可运行容量。

RunBatch 将原来的批次/会话参数检查替换成 cfg.Validate()，放在 context 检查及资源创建前。SummarizeBatch 在最前调用 report.Config.Validate()，移除重复的批次/会话校验与计划字节乘积溢出检查，再安全计算计划总量；其他报告一致性检查保留。SessionConfig.validate 仍保持私有，RunSession 继续使用。

新增 cmd/loadgen/config.go，package main：

```go
// loadConfig 描述一次负载命令的输入与输出位置。
type loadConfig struct {
    Batch loadgen.BatchConfig // 有限批次负载条件。
    OutputPath string         // 报告文件路径；本步仅解析，不创建文件。
}

// parseLoadConfig 解析不含程序名的参数并校验，不运行负载或访问文件。
// 请求帮助时返回 flag.ErrHelp；其他错误返回零配置。
func parseLoadConfig(args []string) (loadConfig, error)
```

使用 flag.NewFlagSet("loadgen", flag.ContinueOnError)，帮助与解析诊断沿用标准 flag 输出。本步不写 main，不调用 os.Exit；不连接服务器、不创建结果文件，不检查目录是否存在或目标是否已经存在。

| 参数 | flag 类型 | 默认值 / 含义 |
| --- | --- | --- |
| -url | StringVar | ws://localhost:8080/v1/asr |
| -sessions | IntVar | 1，计划尝试数，不表示全程活跃连接数 |
| -audio-bytes | Int64Var | 1920000，每场 60 秒静音 PCM 对应字节数 |
| -chunk-bytes | IntVar | 3200，100ms 音频块 |
| -realtime | BoolVar | true；关闭时使用 -realtime=false |
| -session-timeout | DurationVar | 90s，含连接、发送、等待尾部和收尾 |
| -expected-final-text | StringVar | 空，必须显式提供与 Mock 对应的非空尾部文本 |
| -output | StringVar | 空，必须显式提供报告文件路径 |

fs.Parse 失败返回零配置与原错误，保留 flag.ErrHelp；拒绝任何位置参数；调用 cfg.Batch.Validate；额外用 net/url.Parse 校验 URL scheme 为 ws/wss 且 Hostname 非空，不进行 DNS 或可达性检查；OutputPath 的 TrimSpace 非空且不为 "-"，本轮仅接受文件路径。检查时不要修改路径和预期文本，空白也可能是文件名或 Mock 文本的一部分。URL 不静默补协议或修复输入。

预期文本仅要求非空，与库校验一致；必须显式提供，避免默认值掩盖不同 Mock 配置。不要强制 session-timeout 大于音频时长，较短期限可以是有意的失败实验。

### 后续运行与文件层边界

本步只确定 -output 指向文件；拒绝覆盖已有结果是后续文件层的预定方向，需要通过独占创建等文件操作实现，不能用解析阶段的 exists 检查保证。运行开始后的取消/失败仍应尽量保存已收尾报告，并区分负载失败与保存失败；具体接线与退出规则下一小步设计。本步不发布可运行命令，不增加标准输出 JSON、自动命名、重试或持续补充负载。

### 预定验收

开发者实现后，助手补参数与共享校验测试：显式必填参数下的默认配置、全部覆盖值、Bool=false、帮助、未知/位置参数、数字与 duration 格式错误、零负值、PCM 对齐、总量溢出、URL 与输出路径拒绝规则；确认非法配置在批次启动前返回，摘要原有校验仍生效。检查解析不修改尾部文本或路径。暂不启动正式容量实验。


### 参数解析与共享校验验收（2026-09-29）

实现入口：[config.go](../../../cmd/loadgen/config.go)、[batch.go](../../../internal/loadgen/batch.go)、[summary.go](../../../internal/loadgen/summary.go)。本轮生产逻辑审查无须修正。

- 新增命令参数测试 [config_test.go](../../../cmd/loadgen/config_test.go)：4 个顶层测试、44 项叶子检查通过 `go test -race ./cmd/loadgen -count=1 -timeout=60s -v`，包耗时 1.461s。覆盖全部默认/覆盖值、false 与短期限、必填参数、帮助、格式/范围/采样对齐错误、总量溢出、URL 与输出路径规则；保留原始尾部文本及路径，不修改传入参数。
- 新增 [batch_config_test.go](../../../internal/loadgen/batch_config_test.go) 的 5 项边界检查，并扩展批次启动前检查的溢出场景：两场每场 4611686018427387902 字节可通过数值校验，每场 4611686018427387904 字节被拒绝；零/负字节数先返回错误，未发生除零。极值只做 Validate，不实际分配或运行对应负载。
- `RunBatch` 溢出场景与原有启动前失败场景均返回零报告，HTTP 请求总数为 0。loadgen 包共 35 个顶层测试、126 项叶子检查通过 `go test -race ./internal/loadgen -count=1 -timeout=60s -v`，包耗时 4.691s；原有运行、摘要与 JSON 输出回归通过，未报告数据竞争。
- 用临时目录验证：解析可接受父目录尚不存在的输出路径，且不创建文件；已有文件内容保持不变。`.invalid` 地址仍能解析通过，网络可达性留给运行阶段。

本轮合计新增 50 项检查，两个包共 170 项通过。首次参数测试中，助手对标准 flag 的布尔错误文本预期不准确；修正测试为 `invalid boolean value` 后命令包回归通过，未改生产逻辑。当前 cmd/loadgen 只有参数解析，还没有 main/run，因此尚不能用 go run 启动负载；文件创建/关闭/覆盖与取消后保存报告留下一步。本轮没有新增容量实验结果。


## 第五步之二：运行入口与文件保存（2026-09-29，已验收）

### 目的与选择

把已验收的参数、RunBatch 和 WriteBatchJSON 串成可执行命令。运行失败不等于没有报告，输出成功也不等于负载全部完成；这两个结果必须独立处理。

| 方案 | 收益 | 代价与本次决定 |
| --- | --- | --- |
| 标准输出 JSON，由 shell 重定向 | 入口少管文件 | 当前约定显式 -output；覆盖和文件关闭错误难由程序统一控制，不选 |
| 运行结束后才创建结果文件 | 执行期间没有空文件 | 可能跑完才发现目录不存在、路径已占用；不选 |
| 运行前独占创建目标文件 | 已有文件不覆盖，常见路径错误在负载前发现 | 执行期间可能为空、写入失败可能不完整；本步采用 |
| 同目录临时文件，成功后发布 | 可以让最终文件仅在写完后出现 | 需另外定义不覆盖的发布、清理与崩溃规则，普通 Rename 不自动满足不覆盖；后续按需要增加 |

独占创建用 os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)。不使用 os.Create，不先 Stat 再 Create，不自动创建父目录。O_EXCL 解决两个进程争用同一路径时只允许一个成功创建的问题，不证明后续写入必然成功。

当前文件只在命令收尾后读取：执行中可能为空；写入/关闭失败可能留有部分或完整 JSON，保留供检查，不自动删除或重试。有效 JSON 也不表示整个批次成功，需要读取分类与 batch_error。没有 fsync、掉电持久化或原子发布承诺。

### 新增 cmd/loadgen/main.go

```go
// main 解析参数、监听停止信号，并将运行结果转换为进程退出状态。
func main()

// run 使用已经通过 parseLoadConfig 校验的配置执行一轮有限负载。
// 独占创建并关闭输出文件；已开始的批次即使失败或取消也尝试保存报告。
// 返回错误合并运行、非完整会话、输出及关闭失败；不退出进程、不自动重试。
func run(ctx context.Context, cfg loadConfig) (err error)
```

run 的配置前提是来自 parseLoadConfig；不在本函数重复解析 URL 或裁剪路径。只按实际报告与 RunBatch 返回值决定运行成败，不在批次完成后额外读取 ctx.Err 来改写既有成功结果。

### run 的顺序与资源所有权

1. 检查 ctx.Err，预取消时直接返回包装错误，不创建文件也不启动批次。
2. 独占打开 cfg.OutputPath；失败返回包含路径的包装错误，不执行 RunBatch。成功后立即登记 defer 关闭，所有后续路径均只关闭一次。
3. 调用 RunBatch(ctx, cfg.Batch)，取得 report 与 batchErr。不要看到 batchErr 就立即返回。
4. 若 report.StartedAt.IsZero()，依据 RunBatch 的零报告契约判为批次尚未开始，不调用 WriteBatchJSON。返回 batchErr 的包装；若出现不符合当前契约的零报告且 nil 错误，返回明确内部错误。此分支可能发生在检查 ctx 后、RunBatch 前收到取消；新建文件保持为空，仍关闭它，不生成伪报告。
5. 对有报告的批次，统计 Outcome != SessionCompleted 的结果个数，若大于零生成独立的 outcomeErr（包含未完整完成数和计划总数）。无需为此再算分位数，也无需合并每场长错误；原始单场错误已保存在 JSON。单场失败不改写 batchErr。
6. 无论 batchErr/outcomeErr 是否存在，都调用 WriteBatchJSON(file, report, batchErr)。使用本来就不接收 context 的输出接口，不能因本轮负载 context 已取消而跳过保存。这里保存的是全部会话退出后的报告，不允许与报告写入并发。
7. 输出失败生成带路径的 saveErr，以 errors.Join(batchErr, outcomeErr, saveErr) 返回，包装保留 %w。nil 项不会产生错误；不要用取消错误覆盖保存错误，也不要在 JSON batch_error 中放入命令层生成的 outcomeErr。
8. defer 中检查 file.Close() 的错误，存在则经 %w 包装路径并合入命名返回值 err。不能仅 defer file.Close() 忽略返回值，也不能用 defer 的局部 err 遮蔽命名返回值。

采用单一 defer 关闭，返回前不再显式 Close。文件归 run 所有；WriteBatchJSON 仍不关闭调用方 writer。没有新 goroutine 或 channel，复用 RunBatch 现有并发与退出等待。

### main 与退出语义

main 先 parseLoadConfig(os.Args[1:])：flag.ErrHelp 正常返回；其他解析错误打印到 stderr 并退出 1。成功后 signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)，传给 run。run 返回后显式调用 stop()，再决定是否 os.Exit(1)，避免依赖 os.Exit 不执行的 defer。run 中的文件 defer 此时已经执行完毕。

成功时打印简短报告路径到 stderr；失败时打印错误及路径，不宣称报告已保存成功。日志沿用标准 log/slog 默认 stderr 即可，本步不再设计终端摘要布局。

| 结果 | 文件行为 | 进程退出 |
| --- | --- | --- |
| 帮助 | 不创建文件、不运行 | 0 |
| 参数错误或预取消 | 不创建文件、不运行 | 1 |
| 文件已存在/父目录不存在 | 原文件不动、不运行 | 1 |
| 所有会话完成且写入、关闭成功 | 完整报告 | 0 |
| 部分或全部会话失败/超时 | 仍保存全批报告；batch_error 可能为 null | 1 |
| 批次运行中收到停止信号 | RunBatch 等待退出，仍保存完整收尾报告及 batchErr | 1 |
| 写入/关闭失败 | 返回文件错误，保留当次新建文件供检查 | 1 |

退出 1 表示本轮未满足“全部完成且报告写入、关闭成功”；刻意过载实验也可以合理地返回 1，分析时仍读取完整 JSON。批次级 nil 错误不能替代逐会话完成检查。

停止信号只取消负载；文件收尾不使用已取消的 context。当前无法承诺磁盘阻塞时固定时限退出、SIGKILL/崩溃后保留报告，未来持续采集如需要中途持久化再考虑增量格式。

### 预定验收

开发者完成后，助手补本地文件与真实 WebSocket/进程测试：正常完成的可解码文件、混合/全失败仍保存且返回错误、运行中取消后的报告、预取消无文件、已有文件原样保留且零请求、缺失父目录零请求、文件正常关闭，以及帮助/参数错误/正常/失败的实际命令退出。写入错误链与短写由既有 WriteBatchJSON 故障测试继续覆盖；本步 Close 错误合并必须代码审查，若没有可控故障夹具，不声称已实测该分支。验收后再发布运行命令；当前仍为设计，暂无新增容量结果。


### 运行入口与文件保存验收（2026-09-29）

实现：[main.go](../../../cmd/loadgen/main.go)。新增测试：[main_test.go](../../../cmd/loadgen/main_test.go)、[process_test.go](../../../cmd/loadgen/process_test.go)。生产逻辑无需修改。

执行 `go test -race ./cmd/loadgen ./internal/loadgen -count=1 -timeout=120s -v`：命令包 8 个顶层测试、62 项叶子检查通过（5.141s）；loadgen 包 35 个顶层测试、126 项叶子检查通过（5.107s）。本轮新增 4 个顶层测试、18 项叶子检查，总计 188 项通过，未报告数据竞争。包耗时不是业务延迟指标。

- 函数级 9 项：两场正常/混合失败/全部失败/单场超时分别保存完整 JSON；在两场输入确实到达后取消，返回可由 errors.Is 判断的 context.Canceled，仍保存两场 canceled 记录；已有文件内容不变、父目录缺失、预取消、预超时均不发起 HTTP 请求，后三种不创建文件。
- 真实进程 9 项：测试构建 `go build -race` 可执行程序，直接启动二进制；正常退出 0，混合和全部失败退出 1，均保存两场记录。SIGINT、SIGTERM 分别在两场输入到达后发送，程序正常退出 1（不是被信号强杀），报告保留两场 canceled 与批次取消错误。帮助退出 0，非法参数、已有文件和缺失父目录退出 1，均零请求且不改已有结果。
- 网络夹具严格检查每场 start、4/4/2 三块静音音频共 10 字节与 end。保存后的报告可解码为恰好一个 JSON 文档，序号、成功/失败错误及尾部样本一致，计划的两场记录均存在，实际写出总量为 20 字节。日志在 stderr，不污染 stdout。
- 代码审查确认文件成功打开后立即 defer Close，命名返回值合并关闭错误，所有后续路径执行同一关闭操作。未注入实际文件 Close 失败或磁盘写入失败；不声称这些文件故障已经实测。既有 WriteBatchJSON 故障 writer 测试继续覆盖写入错误链与短写。预取消检查后、批次启动前的精确取消竞争窗口及零报告 nil 错误防御分支仅审查，未通过夹具强制触发。

这些是本机受控 WebSocket 对端的正确性与命令验收，并非真实进程 Gateway/Worker 的容量测量。既有 loadgen 测试继续覆盖真实 Gateway/Mock 单会话。当前有限批次命令可用，使用说明已补至 [command.md](../../command.md)；下一步先做小规模完整链路基线，补齐负载到达、发送落后和服务端观察，再确定容量实验条件。


## 第六步：小规模完整链路基线（2026-09-29，已完成）

见 [EXP-005-01](../../experiments/loadgen-baseline.md)。单 Worker 固定共享名额 1、每块模拟处理 10ms、文本响应 5ms，Gateway 预算 32000 字节；独立真实进程、非 race 构建。1/2/4 场各三批、每场 5 秒静音 PCM，正式 21/21 完成，共 3360000 字节、1050 块、84 partial + 21 final，所有首条结果早于 end。各组尾部 p50 为 17.202/18.362/28.529ms，p95（本组最大值）为 17.851/28.459/49.855ms；发送跨度 4.900815–4.902324 秒。

首次预热暴露实验脚本的时钟假设错误，未开始正式测量。Go Sub 的单调时长不能从 JSON 墙钟戳逐纳秒重建；保留首次记录，在新目录用修正脚本完成九批。没有修改业务代码，也没有将本轮结果写作性能改进。

目前只有客户端固定字段和启动/退出日志，无进程级服务端活跃会话、Worker 排队和资源采样。优先补逐块发送排期落后，防止总跨度掩盖中途停顿与追赶；其后再补服务端观察及稳定区间，不能直接扩大并发并宣称容量。


## 第七步之一：暴露音频计划发送时间（2026-09-29，已验收）

### 问题、指标与取舍

EXP-005-01 的发送总跨度接近 4.9 秒，但它只有首尾两个点，无法排除中途停顿后追赶。每块计划时刻必须沿用实际控制发送的 Pacer，才能判断负载是否按预期到达客户端的 Write 调用；不从结果返回时刻反推发送节奏。

指标定义为有计划的音频尝试的 `max(0, StartedAt.Sub(PlannedAt))`，即计划时刻到实际开始 Write 的落后。它与 FinishedAt.Sub(StartedAt) 的单次写入耗时不同：源读取、客户端调度、之前 Write/观察回调的阻塞会反映在落后中，而本次 Write 阻塞主要体现在本次写入耗时及后续块的落后中。它不是服务端排队或端到端识别时延。

| 方案 | 优点 | 限制 / 决定 |
| --- | --- | --- |
| 看总发送跨度 | 已有指标、无需改动 | 看不到中间停顿与追赶，保留但不足以替代逐块观察 |
| 用相邻两块实际发送间隔 | 能看到突发与间隔不均 | 无法表达相对原时间轴累计落后，本步不作为主指标 |
| 记录 Pacer 原计划与实际 Write 开始时刻 | 计划与发送控制共用来源，能观测累计落后 | 需要把计划时间传给现有回调，本步采用 |
| 保存每块完整时间线 | 可精确分析突发过程 | 空间随音频块数增长；先逐块观察、后续固定空间汇总，诊断需要时再采样 |

Pacer 的计划是构造时的 start + 已成功发送音频时长。3200 字节为 100ms，因此前几块计划为 T、T+100ms、T+200ms；首块立即发送。不能改成上次 Write 返回后再等 100ms，也不能把首块实际 Write 开始作为新的原点，否则会改变原发送负载或隐藏首块源读取落后。

落后之后仍沿用既有追赶行为，本步只观察，不增加丢帧、重排、休眠策略或因落后而失败的阈值。正常计时/调度也可能产生小幅正值，因此不能把所有正值都解释成异常。

### 本小步范围

只修改 internal/audio/pacer.go、internal/wsclient/write_observer.go、internal/wsclient/sender.go；暂不接 SessionRecorder、JSON 输出、命令参数或正式容量实验。最终统计会区分无样本与零落后，并保留样本数；输出格式与统计具体字段留下一小步明确。

### 1. Pacer 只读访问器

```go
// NextSendAt 返回下一块音频的计划发送时刻：起点加已成功发送音频的时长。
// 不等待、不推进时间轴；保留 time.Time 的单调时钟信息。
// 与 WaitBeforeSend、Advance 一样，由同一发送协程顺序调用。
func (p *Pacer) NextSendAt() time.Time
```

内部返回 p.start.Add(p.audioElapsed)。WaitBeforeSend 将内部 target 的获取改成 p.NextSendAt()，计时器和取消行为保持。调用者不能并发 Advance；不为当前单发送协程用途增加锁。

为 NewPacer 补注释：以构造时刻作为音频计划起点，第一块计划立即发送。Advance 的注释补充按实际成功写出的字节数推进，支持不足整块的尾部。现有参数校验与字节/时长换算不在本步扩展。

### 2. WriteEvent 添加 PlannedAt

```go
// 仅实时音频写入设置，来自 Pacer 原始计划，保留单调时钟信息。
// start/end 控制消息及 Realtime=false 的音频使用零值，表示不适用。
// 与 StartedAt 相减可计算开始发送前的落后；零值不表示准时。
PlannedAt time.Time
```

使用 time.Time 零值表达是否存在计划，与已有时间字段口径一致。事件按值传递，不需要指针或新的同步。每次实际 Write 返回时通知观察者，包括失败尝试；等待取消、源读取失败且未调用 Write 时不生成事件。回调执行在 Write 返回之后，但不能用回调时刻计算落后。

### 3. sender 传递原计划

在 sendAudio 每轮 n>0 时声明 plannedAt 的零值，仅 Realtime=true 时先取 pacer.NextSendAt()，再调用原 WaitBeforeSend；等待失败保持原错误返回，不调用 Write。随后将 plannedAt 传给 writeObserved，成功后仍 pacer.Advance(n)。首次 io.ReadFull 之前创建 Pacer 的现有位置保持，因此源读取时间自然进入计划落后。

私有 helper 增加末尾参数：

```go
// writeObserved 执行实际写入，按原始时间与错误通知观察者。
// plannedAt 仅用于实时音频的计划时刻，其他情况传 time.Time{}。
// 不在内部等待、修正或重新计算计划，data 不传给观察者。
func (c *Client) writeObserved(
    ctx context.Context,
    conn *websocket.Conn,
    kind WriteKind,
    messageType websocket.MessageType,
    data []byte,
    plannedAt time.Time,
) error
```

writeJSON 调用该 helper 时传 time.Time{}。writeObserved 保持 StartedAt 紧贴 conn.Write 之前、FinishedAt 紧贴返回之后，构造事件时增加 PlannedAt，原始错误及无观察者行为保持。不要对运行中时间调用 UTC/UnixNano/Format 再解析；后续统计在同进程中用 Sub 计算耗时，避免重现基线的墙钟/单调时钟误用。

### 验收与边界

用户编写生产代码；助手负责同步现有 sender_test.go 中直接调用私有 helper 的一个位置，并补新测试。预定检查：计划起点固定、重复读取不推进、累计成功字节决定下一计划、尾块推进正确、Wait 使用同一计划；实时事件计划递增、源读取/慢回调导致实际落后但计划不漂移；非实时/控制消息无计划；失败 Write 保留计划，等待取消不虚构 Write 事件；原错误及收发回归通过。

慢源/慢观察回调作为可控测试扰动，不当作真实网络背压实验。仍在阻塞中的 Write 尚未生成返回事件，取消发生在 Write 之前也不会贡献样本；后续分析需要结合超时/失败及样本数，不能把缺失解释成准时。

本步完成后事件能携带计划，但现有 SessionRecorder 不保存逐块音频事件，因此 JSON 中还看不到新指标。下一小步再接固定空间统计、明确 JSON 演进和复测，避免把回调已支持误记为实验报告已支持。


### 发送计划事件验收（2026-09-29）

实现：[pacer.go](../../../internal/audio/pacer.go)、[sender.go](../../../internal/wsclient/sender.go)、[write_observer.go](../../../internal/wsclient/write_observer.go)。生产逻辑无需修正，助手补充 Advance 的实际字节/尾块注释与 writeObserved 的 plannedAt 参数说明。

新增 [pacer_test.go](../../../internal/audio/pacer_test.go) 两个顶层测试、5 项叶子检查；新增 [planned_write_test.go](../../../internal/wsclient/planned_write_test.go) 五个顶层测试、6 项叶子检查。同步已有直接调用 helper 的测试签名，强化原等待取消测试的 PlannedAt 非零断言。

- 使用 synctest 验证首块计划为构造时刻；连续推进 3200/1600/2 字节后计划分别增加 100ms/50ms/62500ns，重复读取与时间流逝不改计划。虚拟时间下，目标在 200ms 后时等待准确到期；目标已过立即返回；取消和 50ms 期限中断等待且不推进计划。
- 真实 WebSocket 检查 3200/3200/2 字节三块及控制消息：实时音频计划按 100ms 递增，实际 Write 不早于计划；非实时音频与 start/end 的 PlannedAt 为零。
- 首次源读取注入 80ms 延迟，首块计划仍早于读取开始，实际首块落后至少 80ms；首个写入回调注入 80ms 延迟、块间计划为 10ms，第二块落后至少 70ms，计划未随回调漂移。它们是可控客户端扰动，不是网络或 Worker 性能数据。
- 已关闭连接的真实 Write 失败仍携带原计划及错误；helper 按值保留计划与单调时钟信息。原等待取消场景只产生第一块事件，没有虚构尚未执行的下一块 Write。

执行 `go test -race ./internal/audio ./internal/wsclient ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -v`：audio 5 项（1.494s）、wsclient 38 项（2.155s）、loadgen 126 项（5.565s）、命令包 62 项（5.616s），共 231 项全部通过，未报告数据竞争。耗时为测试执行时间，不作业务延迟指标。

本步共新增 11 项检查，JSON v1 契约和既有命令行为回归通过。SessionRecorder 尚未汇总 PlannedAt，因此没有新的落盘排期指标或基线数据；下一步在同进程使用 Sub 计算并固定空间汇总，再明确输出演进与复测。


## 第七步之二：会话内排期统计（2026-09-29，已验收）

### 问题与方案取舍

上一小步提供 PlannedAt，但音频回调结束后没有长期保存逐块事件。需要将这些事实变成每场报告可携带的统计，使长会话也保持固定空间。

| 方案 | 收益 | 代价 / 决定 |
| --- | --- | --- |
| 保存所有块的落后样本 | 可以回看完整轨迹、精确计算分位数 | 内存与会话时长增长，本步不采用 |
| 固定桶直方图 | 固定空间，可估计分布 | 需要提前确定桶边界与精度，本步先不增加 |
| 样本数 + 最大落后 | 更新简单、固定空间，能检查是否曾明显落后 | 无法说明落后频率、持续时间或 p95；本步采用 |

只记录最大值会混淆未观察到和观察到零落后；采用样本数与 time.Duration 值的组合，保留 SessionObservation 按值复制的快照性质。无需可变时长指针，不增加锁、channel、goroutine 或逐块容器。

### 唯一生产文件改动：internal/loadgen/recorder.go

在 SessionObservation 增加字段：

```go
// 有计划且有实际开始时间的音频 Write 样本数，包含失败尝试。
// 为 0 表示没有可用排期样本，不能解释成发送准时。
AudioScheduleSamples int64

// max(0, StartedAt.Sub(PlannedAt)) 的最大值，包含失败尝试。
// 仅 AudioScheduleSamples > 0 时有意义；无样本时保持 0。
MaxAudioScheduleLag time.Duration
```

这是每场会话的音频发送开始落后，不是本次 Write 耗时、Worker 排队或结果延迟。仍在进行而未返回的 Write 尚未有回调，因此不在这些已观察样本中。

### ObserveWrite 更新规则

在现有锁内、case wsclient.WriteAudio 分支中增加排期统计，放在只处理 event.Err == nil 的成功量分支之外。事件满足 `!event.PlannedAt.IsZero() && !event.StartedAt.IsZero()` 时：

1. 计算 `lag := event.StartedAt.Sub(event.PlannedAt)`，负值归零；仍在同进程使用 Sub，不转换 UTC/Unix 时间或格式化再解析。
2. AudioScheduleSamples 增加 1。是否成功写出不影响此计数。
3. 如果 lag 大于当前 MaxAudioScheduleLag，更新最大值。

其他情况不产生排期样本：控制消息即使意外带 PlannedAt 也忽略该字段；PlannedAt 缺失包括正常非实时发送；StartedAt 缺失时无法计算，跳过排期统计，不能将缺失当成有效零落后。

仅跳过新增排期统计，不能从 ObserveWrite 提前 return，原有音频成功量、写入失败数、控制事件与最大 Write 耗时继续按原规则更新。记录器依据事件字段工作，不额外读取 Realtime 配置或推断音频时长。

实际开始早于计划的输入，按非负“落后”定义记为零并贡献一个样本；这不表示提前发送被证明符合节奏要求。当前正常 Pacer 不会主动提前发送，负值分支用于明确统计定义，不增加独立早发检测器。

### 缺失值、并发与一致性

| 已观察事实 | AudioScheduleSamples | MaxAudioScheduleLag | 含义 |
| --- | --- | --- | --- |
| 尚无音频尝试，或全部非实时 | 0 | 0 | 无可用样本 |
| 一次有计划音频，实际与计划相同 | 1 | 0 | 有样本，本次没有晚发 |
| 一次落后 5ms 成功，另一次落后 40ms 写入失败 | 2 | 40ms | 失败尝试保留已发生的落后；成功音频量只计前者 |

计数与最大值在现有同一把 r.mu 下更新，Snapshot 仍锁内直接返回 observation 值，不需要改签名或深拷贝。运行中快照的二者必须来自同一时刻；之后的新事件、其他会话或调用方修改快照不影响已取出的值。

保持不变量：计数非负，最大落后非负；计数为 0 时最大落后为 0。计数不一定等于成功块数：有计划失败尝试计入排期而不计成功块数，无计划成功尝试则相反。不要从两个计数的差值推导失败次数。

给 ObserveWrite 补注释说明排期样本含失败尝试；字段注释注明计数为零时最大值没有测量意义。本步不在 RunSession 结束时二次计算，不根据 lag 更改 Outcome，也不引入判定异常的阈值。

### 后续与验收

本步暂不修改 SummarizeBatch、JSON DTO、schema_version 或命令参数；现有 JSON v1 尚不导出新字段。下一步明确输出演进：无样本的最大落后需要输出 null，有样本且没有晚发输出 0；不能通过重新相减序列化时间来恢复运行时统计。最大值不能推导每块落后的分位数。

助手后续编写测试：零值；缺失计划/开始时间；零、负和纳秒级差值；多个样本取最大值；失败尝试参与样本但不计成功音频；控制事件不污染统计；原有账目保持；快照值隔离、跨会话隔离与并发一致性；真实 Realtime RunSession 的样本数与报告接线。测试完成后再记录数据，不预写性能改进结论。


### 会话内排期统计验收（2026-09-29）

生产逻辑符合设计，仅补 ObserveWrite 注释明确失败尝试参与统计。新增 [schedule_recorder_test.go](../../../internal/loadgen/schedule_recorder_test.go) 四个顶层测试、12 项叶子检查，增强既有并发快照测试。

- 覆盖缺失 PlannedAt/StartedAt、零落后、提前归零、7ns 精度、有计划失败写入，以及意外带计划的控制事件。跳过排期统计不影响原音频量或失败账目。
- 混合事件得到 3 个排期样本、最大落后 40ms、最大 Write 耗时 200ms、成功音频 10 字节/3 块、失败 1 次，验证落后与写入耗时独立，失败样本不计成功音频。
- 修改快照不改变内部记录，后续事件不改历史快照，另一会话不受影响。原并发夹具中 2000 次音频写入的落后按 1ns 递增，与 1500 条结果及 2000 次快照并发；所有快照的排期样本数等于本夹具成功块数，最大落后等于样本数纳秒，最终 2000 样本/2000ns。此等式只适用于该构造夹具，不是通用统计规则。
- 真实 WebSocket 的 Realtime 与非实时会话各发送 642 字节、3 块并完整完成；前者产生 3 个排期样本，后者保持 0 样本/0 最大值。

`go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -v` 通过：loadgen 39 个顶层测试、138 项检查（5.319s）；命令包 8 个顶层测试、62 项检查（5.291s），共 200 项，未报告数据竞争。测试耗时不是负载性能数据。JSON v1 仍不导出新增字段，下一步接输出。


## 第七步之三：排期统计的 JSON v2 输出（2026-09-29，已验收）

### 目的与格式选择

会话内存报告已有排期统计，当前 JSON v1 仍丢弃这两个字段。必须落盘，才能核对实验中的样本覆盖和最大落后；不从序列化的首尾时间戳推算运行时最大值。

方案比较：沿用 v1 添加可选字段通常可以兼容忽略未知字段的读者，但必须额外约定“缺字段表示旧版未知”；本项目读者规模小且已有严格版本检查，为清楚区分老报告不具备观测能力与新报告确实没有样本，本步选择 schema_version=2。新程序只写 v2，历史 v1 原始实验文件保持不动，不增加命令格式选择开关。

助手验收时同步测试及实验脚本：读取端明确接受 v1/v2，未知版本拒绝；v1 排期指标标记缺失，不能自动补为零；v2 要求新字段存在并校验数值与 null 规则。旧版基线复算不能因为新增指标失效，不改写历史原始 JSON、旧汇总或原清单哈希；复核使用内存或临时输出。

### 用户生产改动一：json_output.go

将 batchJSON 的版本说明、SchemaVersion 字段注释和 WriteBatchJSON 的赋值由 1 更新为 2；旧字段名、类型、单位、错误语义及 nearest-rank 规则保持。

在 observationJSON 中增加：

```go
// 有计划且有实际开始时间的音频 Write 样本数，包含失败尝试。
AudioScheduleSamples int64 `json:"audio_schedule_samples"`

// 最大开始发送落后，整数纳秒；无样本为 null，有样本可以为 0。
MaxAudioScheduleLagNS *int64 `json:"max_audio_schedule_lag_ns"`
```

不加 omitempty。toObservationJSON 直接复制样本数；仅样本数 > 0 时，把 observation.MaxAudioScheduleLag 转为局部 int64 值，输出它的指针；否则输出 nil。不要取输入可变字段指针，也不要根据最大值是否为零决定 null。

| 内存统计 | JSON v2 |
| --- | --- |
| Samples=0，MaxLag=0 | audio_schedule_samples: 0，max_audio_schedule_lag_ns: null |
| Samples=1，MaxLag=0 | audio_schedule_samples: 1，max_audio_schedule_lag_ns: 0 |
| Samples=3，MaxLag=40ms | audio_schedule_samples: 3，max_audio_schedule_lag_ns: 40000000 |

字段属于每场 observation，包括最终失败、取消和超时会话；不只导出 completed。writeEventJSON 当前只保存 start/end，二者没有音频计划，不增加 PlannedAt 字段。batchSummaryJSON 本步不增加全批排期分位数：每场最大值不能复原每块落后的 p95。

### 用户生产改动二：summary.go

SummarizeBatch 已在输出前核对报告一致性。在遍历每场结果时增加对排期字段的三个检查，任一不满足均返回零 BatchSummary 与包含会话序号的错误：

1. AudioScheduleSamples 不得小于 0。
2. MaxAudioScheduleLag 不得小于 0。
3. AudioScheduleSamples == 0 时，MaxAudioScheduleLag 必须为 0。

放在当前会话贡献统计之前；沿用现有返回零摘要的错误路径。这防止负计数或“无样本但非零最大值”被转换成貌似合法的 JSON。合法负载报告由 recorder 自然满足，无需更改 RunSession。

不要求样本数等于成功块数，不从计数差值推导失败数；不增加与会话结果无关的强制完整性检查。本步未对实时配置与样本覆盖建立额外推断规则，实际覆盖在实验核对阶段观察。

WriteBatchJSON 已先调用 SummarizeBatch，因此此类矛盾报告会在调用 writer 前被拒绝，不用在 DTO 转换里再写一遍验证。

### 验收与边界

助手补 v2 字段/版本、零样本 null、有样本零值、纳秒精度、失败会话保留、输入不变及矛盾报告零写入测试，保留既有 writer 错误覆盖；用真实实时/非实时批次核对最终 JSON。同步已有 v1 精确期望为当前 v2 期望，并适配分析脚本读取历史 v1（未知）及新 v2（实际统计），历史基线不能补造排期数据。

本步只接输出与读取兼容，不自动给最大落后设置通过阈值，也不声称已经复测。验收后再按相同负载条件采集新指标，比较时明确源码与观测字段变化。


### JSON v2 与读取兼容验收（2026-09-29）

初次检查发现 WriteBatchJSON 的实际 SchemaVersion 仍为 1，版本契约测试失败；开发者改为 2 后复验通过。新报告输出 v2，不改写历史 v1 文件。

新增 [schedule_json_test.go](../../../internal/loadgen/schedule_json_test.go) 三个顶层测试、12 项叶子检查，并更新原有完整 JSON 契约期望：

- 区分零样本 null 与有样本零落后，保留 7ns 及 int64 边界值精度；边界值为格式测试夹具，不是实测负载。
- failed/canceled/timed_out 均保留排期事实，转换不修改输入。
- 负样本数、负最大值、零样本非零最大值返回零摘要及带会话序号的错误；输出函数在这些情况下不调用 writer。
- 真实 WebSocket 的实时/非实时批次各运行两场，每场发送 10 字节、3 块；最终 JSON 分别包含 3 个样本/运行时最大值和 0 个样本/null。

`go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json` 通过：loadgen 42 个顶层测试、150 项检查（5.270s），命令包 8 个顶层测试、62 项检查（5.146s），共 212 项，未报告数据竞争。测试耗时不作为性能指标。

实验读取脚本接受 v1/v2、拒绝未知版本；v1 新指标为未知，v2 必须有完整合法字段。按组仅汇总样本数、有样本会话数与最大落后；混入旧版未知数据时整组排期统计为 null，不从会话最大值推导逐块 p95。

`python3 docs/experiments/scripts/test_loadgen_reports.py -v` 的 6 项测试通过：验证缺失/非法字段、整数范围、版本拒绝、旧基线重算及合成 v2/混合版本。旧基线仅在临时副本上重算，原指标一致，原证据目录所有文件哈希不变；合成 v2 数据仅用于脚本测试，不能写入实验 Result。

本步完成观测输出，不包含新的性能实验。下一步在独立结果目录按原 1/2/4 场、各三批、每场 5 秒条件复测，记录源码版本与新指标，之后再推进服务端观测。


## 排期观测同条件复测（2026-09-29）

[EXP-005-02](../../experiments/loadgen-schedule-baseline.md) 已完成，源码 ec4fc6d，业务与脚本均未修改。沿用单 Worker、1/2/4 场各三批、每场 5 秒条件，21/21 场完整完成，1050/1050 块有排期样本，成功写出 3360000 字节，首条结果均早于 end。三组最大排期落后为 34.686/8.600/4.386ms，发送跨度整体为 4.900692–4.901533 秒。

最大落后出现在第一轮单场，该会话总跨度仍约 4.901 秒，说明总跨度不能替代逐块落后观测。保留该样本，不重跑替换；最大值不包含发生位置和原因，不能推导逐块 p95。旧基线无此指标，不形成优化 A/B 结论。下一小步设计 Gateway 活动会话快照，复用 tracker 作为唯一计数来源。


## 第八步之一：Gateway 活动会话快照（2026-09-29 设计，2026-09-30 验收）

### 目的与方案权衡

客户端计划启动 N 场不等于服务端当前登记 N 场。部分请求可能仍未到达、被拒绝、提前失败或已经结束。服务端还需要自己的活动量记录，以后才能描述负载运行期间的实际占用及测后是否归零。

当前 sessionTracker 已在 tryEnter 成功时计数，并由 handler 最后的 leave 归还；登记早于 WebSocket 升级，释放晚于会话运行和连接兜底关闭。因此 ActiveSessions 表示本 Gateway 已接纳但尚未完成清理的数量，含升级、等待 start、识别和收尾，不等于成功升级连接数、Worker 正在处理数或整个进程的连接数。升级失败可能短暂占位后归还，被拒绝的请求不计入。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 同一把 tracker.mu 下复制已有字段 | 计数只有一份，三个字段来自同一观察时刻，改动小 | 读取短暂参与锁竞争；本步选用，锁内仅复制标量 |
| 额外维护原子活动计数 | 单字段读取方便 | 多一套增减路径，容易遗漏；多个原子读取也不自动形成一致快照，本步不采用 |
| 维护每场记录的注册表 | 能查询具体会话与状态 | 增加登记/删除、内存和遍历成本；本步只需总量，暂不引入 |

Prometheus 是后续暴露和采集方式，不替代计数口径。本步先建立稳定、可测试的进程内读取接口；HTTP JSON、采样周期与格式下一步设计。

### 实现范围

新增 internal/gateway/snapshot.go，声明以下值类型（暂不加 JSON 标签）：

```go
// GatewaySnapshot 是单个 Gateway 在一次加锁读取时的活动状态。
// 返回值不引用内部可变数据；取出后不会随 Gateway 继续运行而变化。
type GatewaySnapshot struct {
    // ActiveSessions 是已接纳但尚未完成清理的会话数。
    // 包括连接升级、等待 start、识别和收尾，不等于 Worker 正在处理数。
    ActiveSessions int
    // MaxSessions 是本 Gateway 实际采用的接纳上限，已包含默认值处理。
    MaxSessions int
    // Stopping 表示已永久停止接纳新会话，不表示已有会话已退出。
    Stopping bool
}

// Snapshot 返回本 Gateway 的一致状态快照，可与会话运行及停止接入并发调用。
// 不等待会话结束，也不改变接入或清理行为；g 必须由 New 成功创建。
func (g *Gateway) Snapshot() GatewaySnapshot
```

在 tracker.go 增加带注释的方法：

```go
// snapshot 在同一临界区复制活动数、接纳上限与停止状态。
// 可并发调用；只观察，不修改计数、不关闭 drained，也不等待清理完成。
func (t *sessionTracker) snapshot() GatewaySnapshot
```

tracker.snapshot 用现有 mu 加锁，复制 active/maxActive/stopping 到上述值类型并解锁返回；Gateway.Snapshot 只委托 g.tracker.snapshot。无需新增计数、goroutine、channel、定时器或单独 DTO。这里快照直接采用包内统一的返回类型，避免为三个标量维护两套映射。沿用 Mutex，不为本次读取先行更换 RWMutex 或原子方案；是否优化需有锁竞争测量依据。

不改变 tryEnter/leave/stopAccepting/wait 的行为。读取只在瞬间一致，返回后服务可继续变化；调用方不能用它替代 tryEnter 做准入，也不能通过轮询替代 Wait 协调退出。停止且活动数为零表示 tracker 已无登记，不承诺 Go 堆或 goroutine 全部归零。MaxSessions 是配置上限，不是实测稳定容量。

本步不增加采样时间戳、累计接纳/拒绝数或历史峰值。以后外部定期读取时，只能报告采样到的最大活动数，不能据此证明两个采样点之间没有更高峰值；新快照本身也不能证明持续并发数。

### 验收计划

由助手在实现后补测试：初始值与默认/显式上限、成功登记与拒绝、释放归零、停止但仍有会话、停止并清空、旧快照不随新变化而改变、修改返回值不污染内部、不同 Gateway 隔离、并发读取与登记/清理无数据竞争，以及真实 handler 路径的快照接线。验证过程中不直接把测试调用量当作容量实验。通过后再接对外查询和实验采样，本步不预写新增性能数据。


### Gateway 快照验收（2026-09-30）

生产实现符合设计，未修改开发者的业务逻辑。Gateway.Snapshot 委托 tracker.snapshot，三个标量在同一把现有 Mutex 下复制返回，无额外计数、后台任务或生命周期变化。

新增 [snapshot_test.go](../../../internal/gateway/snapshot_test.go) 五个顶层测试、七项叶子检查：

- 默认上限 64 与显式上限 2；逐次登记、满额拒绝、停止接入、停止状态拒绝、逐次释放与 drained 通知。快照读取不提前释放名额或结束等待。
- 修改返回值、后续会话变化与另一个 Gateway 均不污染旧快照或内部状态。
- 八个登记调用者各执行 200 次登记/归还，与四个读取者各 1000 次快照并发；随后另有八个待归还名额，停止接入与释放、4000 次快照并发。验证活动数边界、固定上限，以及排空期间活动数不增加、停止状态不回退。这是竞争测试夹具，不是吞吐实验。
- 升级失败的 HTTP 响应写出时已占用一个名额，handler 返回后归零，验证登记早于升级的口径。
- 两个真实 WebSocket 场景均在等待 start 时占用一个名额，额外连接被拒绝不增加计数；分别验证停止接入后仍可完成 start/audio/end/final，以及客户端在 start 前断开。handler 返回后快照归零，检查时未取消服务上下文。

`go test -race ./internal/gateway ./cmd/gateway -count=1 -timeout=180s -json` 通过：gateway 包 82 个顶层测试、178 项叶子检查（12.918s），命令包 8 个顶层测试、86 项叶子检查（4.140s），共 264 项，未报告数据竞争。12 个需显式环境开关的实验测试按默认跳过，本轮未重跑这些负载实验。测试运行时间不是性能测量。

当前只有进程内快照，还未接 HTTP 或定时采样，暂无新增活跃会话曲线、采样峰值或稳定容量数据。下一步明确查询接口和编码边界，再由实验端采样；不在 Gateway 内新增轮询 goroutine。


## 第八步之二：Gateway 快照 HTTP 查询（2026-09-30，已验收）

### 目的与备选方案

独立实验进程无法直接调用 Go 对象的 Snapshot；需要一个进程外读取入口。查询只暴露现有事实，不改变会话处理，也不在服务端启动定时任务。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| HTTP JSON 查询 | 复用已有监听器与 Python 读取能力，容易保留原始响应 | 后续需由实验端管理采样时刻和失败；本步采用 |
| 每次会话进出写日志 | 能保留事件时间，可能重建变化过程 | 并发日志重排、完整性与解析另需设计，按事件写日志也有开销 |
| Prometheus 指标端点 | 适合后续持续采集和监控生态 | 当前只有三个状态字段，暂不引入额外依赖和指标命名约定；仍需明确同样的统计口径 |

路由层已负责组装 Gateway。routes 参数从 http.Handler 改为 *gateway.Gateway，使其同时能注册 ASR handler 和读取快照；当前只有一个具体实现，不为此新增接口或运行时类型断言。HTTP 返回格式由 cmd/gateway 私有 DTO 定义，内部 GatewaySnapshot 保持进程内语义，不增加 JSON 标签。

### HTTP 契约

在现有监听器注册 `GET /debug/gateway`。使用标准 ServeMux 方法模式；根据本地 Go net/http.ServeMux 文档，GET 模式也匹配 HEAD，真实 HTTP Server 对 HEAD 不发送响应体。其他方法返回 405，由 ServeMux 处理；路径不带末尾斜杠，未注册子路径。

正常响应为 200，Content-Type 为 application/json，Cache-Control 为 no-store。全部字段始终存在，不使用 omitempty：

```json
{
  "schema_version": 1,
  "active_sessions": 2,
  "max_sessions": 64,
  "stopping": false
}
```

schema_version=1 是本接口自己的格式版本，与负载报告的 v2 独立。三个状态字段来自同一次 Snapshot 调用；数字使用 int，停止标志使用 bool。不输出时间戳、会话列表、历史峰值或累计请求数。后续采样脚本记录请求开始/结束时刻；一个响应只代表服务端读取时的状态，不表示整个请求期间状态不变。

只要 HTTP 服务仍可处理请求，活动数满额或 Stopping=true 都返回 200 和实际状态。查询不调用 tryEnter/leave，不经过 Gateway.ServeHTTP，不创建 Worker 流；仍保留 /healthz 的现有语义。实际服务关闭时查询可能失败，不能承诺停止期间端点一直可达，也不能把失败当作 active=0。

本步复用当前 :8080 监听范围，不建立独立观测端口或访问控制；它是当前实验查询入口，不把 /debug 前缀解释为仅本机可达。

### 用户实现任务

1. 新增 cmd/gateway/snapshot.go，声明私有 gatewaySnapshotJSON，字段为 SchemaVersion int、ActiveSessions int、MaxSessions int、Stopping bool，JSON 标签依次为 schema_version、active_sessions、max_sessions、stopping；字段及类型补充含义注释。
2. 实现 `func gatewaySnapshotHandler(g *gateway.Gateway) http.HandlerFunc`，返回只读 HTTP 处理函数。注释说明 g 必须由 gateway.New 成功创建，每请求只读一次 Snapshot，查询不占用名额。
3. 处理函数调用一次 g.Snapshot，将值映射至 DTO，SchemaVersion 固定 1；用 json.Marshal 先完成编码。此时 tracker 锁已释放，序列化与网络 I/O 都不在锁内。编码出错时记录错误并返回 HTTP 500，不先发送 200；成功后设置上述两个响应头，调用 Write 输出 JSON，省略显式 WriteHeader 时默认 200。Write 出错只记录日志，不再调用 http.Error 追加第二份响应。当前 DTO 只有 int/bool，正常编码不会失败，但保留常规错误处理；不通过修改生产类型专门制造编码故障。
4. 修改 routes 为 `func routes(wsGateway *gateway.Gateway) http.Handler`，保留原 ASR/healthz 注册，并增加 `mux.HandleFunc("GET /debug/gateway", gatewaySnapshotHandler(wsGateway))`。run 已传入对应 Gateway，无需新增依赖注入层。

可用 slog.Error 记录 encoding/write 错误，日志仅包含操作和错误。不新建 goroutine，不缓存响应，不增加计数或调用 Wait/StopAccepting。

### 验收计划与范围

实现后由助手补测试及使用文档：精确字段/版本/零值保留、默认与显式上限、JSON 与 no-store 头、GET/真实 HTTP HEAD/405/未知路径、满额或停止状态仍能查询、查询不占用会话名额、与真实 WebSocket 接入/退出的接线、原 healthz/ASR 路由回归，以及响应 Write 失败不二次写错误页。状态变化与查询并发时运行 race 检测。

本步不运行新的容量实验，不生成活动曲线。下一步再设计采样周期、请求期限、失败样本和停止采样条件；定期样本的最大值只能称采样峰值，不能冒充真实瞬时峰值。


### HTTP 查询验收（2026-09-30）

开发者实现符合方案，业务代码无需修改。routes 接收具体 Gateway，查询在获取一次值快照后编码；显式 WriteHeader(200) 位于成功编码和设置响应头之后，符合接口约定。

新增 [cmd/gateway/snapshot_test.go](../../../cmd/gateway/snapshot_test.go) 五个顶层测试、11 项叶子检查：

- 独立按 JSON 字段检查版本、精确字段集合、数字/布尔类型、0/false 保留、默认上限 64、显式上限及停止状态；不复用生产 DTO 解码。重复查询不改状态，不调用 Worker。
- 真实 HTTP GET/HEAD 路由、HEAD 无响应体、POST/DELETE 返回 405 并包含 Allow、未注册子路径返回 404，以及 /healthz 仍返回 ok。正常查询 Content-Type 与 Cache-Control 符合约定。
- 真实 WebSocket/gRPC 路由中，等待 start 的一场占满名额；四个查询调用者各执行 20 次查询，均返回 active=1，Worker 尚未建流。第二场被 503 拒绝，查询仍可读取；停止接入后查询返回 active=1、stopping=true，已有会话继续完成 start/audio/end/final。30 次查询与收尾并发，handler 退出后查询返回 active=0、stopping=true；全程仅创建一条业务 Worker 流。
- 注入部分响应写入和 io.ErrClosedPipe，确认仅一次 Write、一次 200 状态，没有追加错误页。
- 阻塞响应 Write 时，StopAccepting 仍能完成，另一请求读取到停止状态；解除阻塞后，原响应仍保留其读取时的 stopping=false，证明响应不混用后来状态。固定 int/bool DTO 的编码失败分支仅代码审查，未伪造生产编码错误。

`go test -race ./cmd/gateway ./internal/gateway -count=1 -timeout=180s -json` 通过：命令包 13 个顶层测试、97 项叶子检查（3.569s），Gateway 包 82 个顶层测试、178 项叶子检查（13.597s），共 275 项，未报告数据竞争。12 个需显式启用的实验默认跳过；此处请求次数与运行时间仅为行为验证，不是性能实验。

[使用说明](../../command.md)已补查询示例。尚未接实验采样，下一步固定采样周期、单次请求期限、开始/结束时刻与失败样本口径，然后再采集活动曲线。


## 第八步之三：客户端单次 Gateway 查询（2026-09-30，已验收）

### 目标与分工

HTTP 查询已可用，但实验端必须能区分“合法观测到零活动”和“本次未获得有效观测”。先实现单次查询与响应校验；循环、计时、保存分别接入，不一次修改负载执行、报告版本及实验编排。

采样组件继续使用 Go，放在负载工具 internal/loadgen 中，生产实现由开发者完成、测试由助手补齐。后续在负载客户端一侧独立串行运行，Python 脚本继续负责进程编排和分析。这里的“外部”指 Gateway 进程之外，不要求每次查询另起进程。Python 直接采集也可减少 Go 代码，但本项目客户端已有 context 和 Go 测试体系；选择 Go 便于明确请求取消并复用 HTTP 连接。接线时不会放进音频发送/接收回调，以免观测请求直接阻塞音频流。

### 后续周期采样的原则（本步不实现）

- 当前实验候选采样间隔 100ms、单次完整查询期限 200ms，均需显式记录；属于实验配置，不是服务 SLA。
- 同时最多一个查询。请求变慢时不积压待执行任务、不并发补发漏掉的采样；具体定时推进与跳过记录在下一小步固定。
- 每次尝试记录开始/结束墙钟、单调耗时和序号，失败保留错误且状态为 null；停止时取消在途请求并等待采样退出。
- 覆盖负载前、负载运行中及负载结束后的有限观察窗口。尚未固定窗口与停止条件，不据此宣称退出或持续并发。
- 周期采样可能遗漏短暂峰值，只能称“采样到的最大活动数”。成功请求不是零成本，后续实验需记录频率与开销。

### 本步接口

新增 internal/loadgen/gateway_snapshot.go：

```go
// GatewayState 是从一个合法 HTTP 快照响应读取的 Gateway 状态。
// 只有 FetchGatewaySnapshot 返回 nil 错误时，该值才代表有效观测。
type GatewayState struct {
    // ActiveSessions 是已接纳但尚未完成清理的会话数。
    ActiveSessions int
    // MaxSessions 是接纳配置上限，不是实测容量。
    MaxSessions int
    // Stopping 表示已永久停止接纳新会话。
    Stopping bool
}

// FetchGatewaySnapshot 向 endpoint 执行一次 GET，校验 HTTP 与快照 v1 格式。
// endpoint 是完整查询 URL；ctx 控制本次完整请求及响应读取的期限和取消。
// client 由调用方创建并复用，本函数不修改它、不关闭其连接池。
// 不自动跟随重定向、不主动重试；失败返回零 GatewayState 和可追溯错误。
// ctx 与 client 不能为 nil；调用方应为每次查询设置期限。
func FetchGatewaySnapshot(ctx context.Context, client *http.Client, endpoint string) (GatewayState, error)
```

GatewayState 不导入 internal/gateway：客户端通过 HTTP 契约读取，不依赖服务实现包。JSON 读取用私有 gatewaySnapshotResponse，字段为 SchemaVersion *int、ActiveSessions *int、MaxSessions *int、Stopping *bool；标签与 HTTP 契约一致。指针用于区分合法的 0/false 与字段缺失/null。不要给业务返回类型加指针以传播这层解析细节。

### 执行流程与边界

1. nil ctx/client 返回错误；用 http.NewRequestWithContext 创建 GET，可设置 Accept: application/json。保持调用方 client 不变。
2. 浅拷贝 http.Client，复用 Transport/Timeout 等配置，仅在副本上设置 CheckRedirect 返回 http.ErrUseLastResponse。这样仍共享连接池，但不让重定向把观测带到另一个地址；3xx 当作非 200 错误。不要每次新建 Transport，也不调用 CloseIdleConnections。
3. 对副本调用一次 Do；出错用 %w 包装原错误。取得响应后及时 defer Body.Close，所有后续路径都关闭 Body。正常 200 要读取到 EOF；错误路径不为复用连接而无限排空响应。
4. 非 200 返回含状态码的错误。用 mime.ParseMediaType 检查 Content-Type 为 application/json，允许合法参数（例如 charset=utf-8）。
5. 响应体上限固定 4096 字节，用 io.LimitReader(resp.Body, 4097) 后 io.ReadAll；读取失败包装 %w，超过 4096 拒绝。上限是本小型快照协议的保护值，不是全链路内存保证。
6. 对完整字节串 json.Unmarshal 到私有响应类型，拒绝非法 JSON、错误类型、小数整数、整数溢出和第二份尾随 JSON。未知附加字段允许忽略，格式版本仍须为 1；不额外实现重复键检测，本步使用 Go 标准 JSON 解码行为。
7. 四个指针都必须非 nil；版本必须 1；MaxSessions > 0；0 <= ActiveSessions <= MaxSessions。Stopping=true 且 ActiveSessions>0 合法。只在全部检查通过后构造返回 GatewayState。

零值响应并非成功：有效的零活动状态仍需完整四字段及合法正上限。错误时 GatewayState 零值仅满足函数约定，采样层必须检查 error，不能落盘为观测到 active=0。保留取消/超时错误链，后续可用 errors.Is 判断。这里不新增 Sample 类型、时间戳、goroutine、ticker、落盘、命令参数或负载报告版本。

### 验收计划

助手在实现后补测试：合法空闲/忙碌/停止状态、缺失/null、未知版本、类型和范围、非法与尾随 JSON、媒体类型、非 200、响应体边界/读取错误、错误返回零值及关闭响应体；真实本地 HTTP 的请求取消、响应体读取超时、重定向不跟随及调用方 client 不被修改。时间期限覆盖完整响应读取，不只等待响应头；不通过 sleep 估算生产性能。通过后再明确周期采样和样本记录结构。


### 单次 Gateway 查询验收（2026-09-30）

开发者实现符合约定，生产逻辑未修改。私有指针 DTO 校验缺失/null，全部检查通过后才返回 GatewayState；HTTP 客户端副本禁用重定向并继续复用 Transport，所有取得响应后的返回路径均关闭 Body。

新增 [gateway_snapshot_test.go](../../../internal/loadgen/gateway_snapshot_test.go) 七个顶层测试、53 项叶子检查：

- 合法空闲、忙碌、满额停止及清空停止；允许附加字段和 application/json 的合法 charset 参数。
- 四个字段逐个缺失/null、未知版本、错误类型、小数整数、溢出、非法计数关系，以及空响应、非法 JSON、null/数组和尾随第二份 JSON 均拒绝，错误返回零 GatewayState。
- 非 200 响应、缺失/错误/非法媒体类型均拒绝且关闭响应体，不为排空错误响应而继续读取。恰好 4096 字节合法响应可接受；4097 及 100000 字节响应拒绝，最多实际读取 4097 字节。
- nil 参数和非法 URL 不发请求；传输失败不主动重试；传输/响应体读取错误保留 errors.Is 错误链，取得响应后恰好关闭一次 Body，函数不关闭调用方连接池。
- 真实 HTTP 重定向目标未被查询，调用方的自定义 CheckRedirect 不被调用或修改；随后原 client 仍按原策略跟随重定向，Transport 与 Timeout 保持不变。
- 真实 HTTP 连续两次成功查询仅建立一条 TCP 连接，验证正常响应完整读取和连接复用。
- 等待响应头及已经开始读取响应体时，分别验证主动取消和 200ms context 期限；均返回零状态并保留 context.Canceled/DeadlineExceeded。200ms 是测试超时配置，不是性能测量。

`go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json` 通过：loadgen 包 49 个顶层测试、203 项叶子检查（5.728s），命令包 8 个顶层测试、62 项叶子检查（5.240s），共 265 项，未报告数据竞争。

当前只完成一次请求及校验，还未接周期采样、时间记录、文件或实验命令，不产生服务端活动曲线或新的容量结论。下一步明确样本结构、串行执行、超时/取消及漏过时刻的处理方式。


## 第八步之四：串行采样与样本交付（2026-09-30，已验收）

### 目的、节奏与方案权衡

单次查询已能返回可靠状态或明确错误。本步连续观察变化，同时保证观测自身不会堆积请求、无限保存样本，且可以停止。成功的零活动与失败的未知状态必须在记录结构上区分。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 每次查询并交付后等待固定间隔 | 一个串行循环，无补发/积压，慢请求自然降低频率，停止逻辑清楚 | 不是固定起点间隔；本步选用并记录实际时刻 |
| 固定起点时间表，忙时跳过 | 更接近固定时间网格，方便描述漏过的计划时刻 | 需定义错过时刻、跳过数与调度延迟；当前无需这些字段 |
| 每个周期并发发起一次查询 | 可以维持更密集的尝试 | 慢请求导致并发增长、响应乱序，观测可能干扰被测系统，本轮不采用 |

本步将 Interval 明确定义为“上条样本交付结束后，到下次查询之前的等待时长”。首次立即尝试。例如查询从 0ms 开始，230ms 返回，样本立即交付，Interval=100ms，则下一次最早约 330ms 开始；不补发 100ms/200ms 的请求，也没有固定时间网格的漏采计数。回调耗时也影响采样频率。100ms/200ms（间隔/请求期限）仍是后续实验候选配置，不在本函数内设默认值、不承诺精确 10Hz。

样本存储比较：返回持续增长的 slice 会使长时间采样占用越来越多内存；通过 channel 异步交付又需要定义缓冲和满时策略。因此选同步 emit 回调：循环只持有当前样本，调用方决定保存方式。emit 必须及时返回；阻塞的任意回调无法由本函数强行中断，context 不提供此保证。落盘、异步化或回调期限留后续设计，不新增后台 goroutine 掩盖阻塞。

### 本步类型与函数

新增 internal/loadgen/gateway_sampling.go：

```go
// GatewaySamplingConfig 指定串行采样条件，两个时长都必须为正。
type GatewaySamplingConfig struct {
    Endpoint string // 完整的 http/https 查询 URL，必须包含主机名。
    Interval time.Duration // 上条样本交付完成后的等待时长，不是固定起点周期。
    RequestTimeout time.Duration // 每次查询（含响应读取）的子 context 期限。
}

// Validate 只检查配置，不创建请求、连接、计时器或后台任务。
func (cfg GatewaySamplingConfig) Validate() error

// GatewaySample 保存一次已开始的查询尝试，包含失败尝试。
type GatewaySample struct {
    Index int // 本次运行内从 0 递增的尝试序号。
    StartedAt time.Time // 调用单次查询前的本地时间，保留单调读数。
    FinishedAt time.Time // 查询返回后立即记录的本地时间。
    Duration time.Duration // FinishedAt.Sub(StartedAt)，不包含 emit 和后续等待。
    State *GatewayState // 查询成功时非 nil；失败时 nil，不能补造零活动。
    Err error // 查询原始错误；成功时 nil，保留 errors.Is 错误链。
}

// RunGatewaySampling 在调用方当前 goroutine 中串行查询并交付样本。
// ctx 控制整体运行，client 由调用方创建并复用，emit 必须非 nil 且及时返回。
// 每次查询结果同步交付一次；查询失败不会自行终止循环。
// 父 ctx 结束或 emit 返回错误才退出；本函数不保存样本历史，也不关闭 client。
// 配置、nil 参数或启动前取消会在首次尝试前返回，不交付样本。
func RunGatewaySampling(
    ctx context.Context,
    client *http.Client,
    cfg GatewaySamplingConfig,
    emit func(GatewaySample) error,
) error
```

Validate 要求 Endpoint 由 url.Parse 成功解析，Scheme 为 http 或 https，Hostname 非空，Interval/RequestTimeout > 0；不探测服务是否可达，不限定调用方使用某个固定路径。nil ctx/client/emit 属于前置错误。前置校验完成后检查父 context；已经结束则返回 ctx.Err()，不发起查询。

### 循环顺序与停止规则

1. 每轮开始检查 ctx.Err()，结束则直接返回。
2. 保存 StartedAt=time.Now()，创建 context.WithTimeout(ctx,cfg.RequestTimeout)，用它调用 FetchGatewaySnapshot。
3. 查询返回后立即保存 FinishedAt=time.Now()，马上 cancel 子 context，不在循环内累积 defer cancel。若 client.Timeout 更短或父期限更早，实际请求会更早结束。
4. 构造当前样本。Duration 使用两个原始 time.Time 的 Sub；不先转 UTC，不通过序列化墙钟相减重算。成功才把本轮独立 GatewayState 值的地址放入 State；失败 State=nil、Err 为查询返回的错误。每轮使用独立局部值，后续查询不能覆盖旧样本。
5. 同步 emit(sample) 一次；若交付失败，返回含样本序号、以 %w 包装的交付错误，不重试，避免重复记录。此次交付错误优先于同时发生的父取消错误；不能把交付失败伪装成 HTTP 查询失败。
6. 交付成功后检查父 ctx。父 context 已结束则返回 ctx.Err()；否则等待 Interval，再进入下一轮。等待使用可取消 Timer/select，退出时停止 Timer，不使用不可取消的 time.Sleep。

不因查询 HTTP 错误、解析错误、单次请求超时、State.Stopping=true 或 active=0 自动退出。父 context 仍有效时，这些都只是需要保留的事实；是否负载已结束由之后的协调层决定。函数没有“达到某个样本数即成功”的隐含条件；调用方取消后返回 context.Canceled 是运行结束原因，后续协调层可区分主动停止与异常，不在这里统一吞掉。

取消边界：

- 初始或两次尝试之间取消：不新增样本。
- 进行中的查询因父取消返回错误：先交付该次失败样本，再返回父错误；即使父已经取消也调用 emit，以保留最后一次尝试。
- 查询已成功但交付前父被取消：仍交付成功样本，不用后来的取消推翻已经拿到的有效状态，然后返回父错误。
- 检查 context 与真正开始请求之间仍可能发生取消；已经进入查询尝试就保留其结果，不声称取消时绝对没有新的网络调用。

调用方若需要并发运行负载和采样，应在后续协调层启动并等待这个同步函数，采样器本身不创建 goroutine。历史记录由 emit 的接收方拥有；这里仅保证自身不累计 slice，不承诺接收方存储也有界。暂不增加 JSON 标签、输出文件、命令参数、采样峰值统计或批次报告字段。

### 验收计划

由助手补测试，优先使用 Go testing/synctest 与受控 HTTP transport 验证时间顺序，不依赖很短的真实 sleep：前置失败零请求/零样本、首次立即、间隔包含交付后等待、慢查询不重叠或补发、查询失败/子期限后继续、父取消时的在途记录、等待取消不造样本、成功与取消边界、emit 失败停止且保留错误链、连续序号、时间/耗时、独立历史值。另验证实际 FetchGatewaySnapshot 接线。通过后再设计输出与负载生命周期协调，暂无新的负载实验或容量数字。


### 串行采样验收（2026-09-30）

开发者实现符合设计，生产逻辑无需修改。配置、nil 参数和已结束的父 context 均前置拒绝；查询成功的状态使用每轮独立副本，失败保留原错误，子 context 在交付前取消。等待可由父取消中断，未引入内部 goroutine 或持续增长的样本 slice。

新增 [gateway_sampling_test.go](../../../internal/loadgen/gateway_sampling_test.go) 八个顶层测试、27 项叶子检查：

- HTTP/HTTPS/IPv6 地址及九类非法配置；配置校验失败、nil 依赖和启动前取消/过期均零请求、零交付。
- 虚拟时间串行五次尝试，查询耗时分别为 40/230/300/10/10ms，含传输失败、子期限和格式错误；每次交付耗时 20ms、间隔 100ms。首轮立即开始，后续精确在上次查询结束后 120ms 开始，无补发或请求重叠；Duration 只包含查询时间，序号连续，失败后继续。
- 每次交付时子 context 已结束；超时和传输错误保留 errors.Is 链。成功样本的状态指针不共享，后续查询与修改历史样本均不污染其他结果。
- 在途父取消或父期限均先交付持续 50ms 的失败尝试，再返回父错误；等待间隔取消不新增样本。响应已成功读取但关闭 Body 时父被取消，仍交付成功状态，然后退出。
- 交付失败只交付一次，返回交付错误及样本序号，不与查询错误混淆；同时取消时仍以交付错误为退出原因。
- 依次观察空闲、仍有会话的停止状态、清空停止状态，均继续，直到调用方取消。真实 HTTP 连续三个合法响应生成三个独立样本，序号、状态、时间关系符合约定。

先运行 `go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json` 通过；补充停止状态不终止循环的测试后，再运行 `go test -race ./internal/loadgen -count=1 -timeout=120s -json` 通过。最终覆盖 loadgen 57 个顶层测试、230 项叶子检查（5.288s），命令包 8 个顶层测试、62 项叶子检查（前次回归 5.851s），共 292 项，未报告数据竞争。上述毫秒数为受控虚拟时间，测试运行耗时也不作为性能指标。

本步证明采样循环的行为，不代表采样已接入实验；仍没有样本文件、活动曲线或新的容量数据。下一步明确输出格式与错误/缺失值表达，随后再接命令和负载协调。同步 emit 阻塞仍由调用方负责，不声称 context 能强行中断任意回调。


## 第八步之五：采样 JSON Lines 输出（2026-09-30，已验收）

### 格式选择与目标

采样循环已通过同步 emit 交付记录，但仍不能作为实验文件复核。本步实现一条样本到 io.Writer 的转换，复用现有负载 JSON 的纳秒/UTC/null 口径，不修改 BatchReport 或批次 JSON v2。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 收集完整 slice 后输出 JSON 数组 | 整份文档易于一次读取 | 采样越久内存越多，结束前无法保存完整样本；本步不选 |
| 流式写 JSON 数组 | 可以逐条写出，整体格式常见 | 首项逗号、关闭括号和中断恢复需额外状态 |
| 每条样本一行 JSON（JSON Lines） | 当前样本编码后即可写出，无历史积累，完整前缀可逐行解析 | 需要另行描述运行元信息与结束状态；本步选用 |

文件名未来使用 .jsonl，当前函数只接收 writer。Endpoint、采样间隔、期限、源码等共同配置及运行结束原因计划由后续运行清单记录，暂不重复到每行。只有若干完整行不能证明整场采样完整；文件截断、最后一行不完整或运行失败必须在后续读者与清单中显式处理，不能悄悄过滤后宣布成功。

### 单行 v1 契约

每次调用输出紧凑 JSON 加恰好一个换行符；字段全部存在，不使用 omitempty：

```json
{"schema_version":1,"index":0,"started_at":"2026-09-30T00:00:00Z","finished_at":"2026-09-30T00:00:00.012Z","duration_ns":12000000,"state":{"active_sessions":0,"max_sessions":64,"stopping":false},"error":null}
```

失败样本保持同样字段，state=null，error 为查询错误文本；不是字符串 "null"，也不是一个所有值都为零的 state。非 nil 但 Error() 返回空字符串的错误仍输出空字符串，不转成 null。错误文本里的换行由 JSON 编码转义，不能生成额外记录行。schema_version=1 为采样行格式自己的版本，与 HTTP 查询 v1、负载报告 v2 相互独立。

时间戳使用 UTC 的 time.Time JSON 编码；duration_ns 是 int64(sample.Duration)，保留运行时记录的整数纳秒。不要从 UTC/序列化墙钟重新相减计算或强制验证与 duration 相等。墙钟可能校正，单调时长和序列化墙钟差值也未必逐纳秒一致；单行校验要求时间非零、记录耗时非负，不依据墙钟顺序裁掉合法观测。

### 用户实现任务

新增 internal/loadgen/gateway_sample_json.go，定义私有 DTO 并补注释：

```go
// gatewayStateJSON 是采样行中的成功状态；外层为 nil 时输出 null。
type gatewayStateJSON struct {
    ActiveSessions int `json:"active_sessions"` // 已接纳但尚未完成清理的会话数。
    MaxSessions int `json:"max_sessions"` // 配置接纳上限。
    Stopping bool `json:"stopping"` // 是否已停止接纳。
}

// gatewaySampleJSON 是采样行 v1；全部字段显式输出，时长为整数纳秒。
type gatewaySampleJSON struct {
    SchemaVersion int `json:"schema_version"` // 固定为 1。
    Index int `json:"index"` // 本次运行内的尝试序号。
    StartedAt time.Time `json:"started_at"` // 查询开始，UTC。
    FinishedAt time.Time `json:"finished_at"` // 查询返回，UTC。
    DurationNS int64 `json:"duration_ns"` // 原样保留的查询耗时。
    State *gatewayStateJSON `json:"state"` // 查询失败时为 null。
    Error *string `json:"error"` // 查询成功时为 null。
}

// WriteGatewaySampleJSON 校验并将一条采样记录写为一行 JSON。
// sample.Err 是查询事实，合法失败样本仍可成功写出；返回值只表示输出失败。
// 函数不修改 sample、不创建/关闭/刷新 writer，不保存跨行状态。
// 校验/编码失败不写入；写入错误可能留下部分行，调用方应停止继续写入。
func WriteGatewaySampleJSON(w io.Writer, sample GatewaySample) error
```

按以下顺序实现：

1. nil writer 拒绝；Index >= 0；StartedAt/FinishedAt 均非零；Duration >= 0。
2. 成功必须 Err==nil 且 State!=nil；失败必须 Err!=nil 且 State==nil。两者同时有值或同时为空均拒绝。
3. State 非 nil 时检查 MaxSessions > 0，0 <= ActiveSessions <= MaxSessions；Stopping=true 且有活动会话合法。单行函数无历史，不能验证序号从零连续；不要硬编码 Index 必须为零。
4. 通过校验后映射私有 DTO：时间使用 .UTC() 的返回值，耗时直接 int64，状态逐字段复制至新值。Error 复用包内 errorText，保持 nil 与空文本错误的区别。不要修改输入时间或输入 State。
5. json.Marshal（不用 MarshalIndent），编码成功后 append '\n'。时间超出 JSON 支持范围等编码错误也必须在任何 Write 前返回。
6. 对 writer 调用一次 Write，出错用 %w 包装原错误；err==nil 但 n!=len(data) 返回包装的 io.ErrShortWrite。不循环续写或重试，不吞写入错误。成功返回 nil。

校验错误注明字段或样本序号即可，无需新增错误分类体系。无需额外公开转换函数，也不用改变 GatewaySample/GatewayState 或既有 errorText。函数不加锁，同一 writer 的调用由外部串行协调；未来可直接作为 RunGatewaySampling 的 emit 实现，不另建异步写队列。

### 错误与所有权边界

查询超时、取消、服务错误都是合法失败样本：保存成功就返回 nil，使父 context 仍有效的采样能够继续。相反，磁盘或 writer 出错属于样本无法交付，返回错误后 RunGatewaySampling 会停止；不要自动重试部分行，否则可能产生重复或拼接坏数据。

一次 Write 并不保证文件原子写入或掉电持久化；writer 如果是缓冲器，Flush/Close 由后续文件层负责。当前仍可能被阻塞的 writer 阻塞，保持同步 emit 的既有边界；本函数不声称强行取消任意 I/O。完整运行的文件创建、关闭、元信息和异常结束记录留下一步。

### 验收计划

由助手补单行精确格式、UTC、纳秒精度、0/false/null、空文本和多行错误、成功/失败/取消样本、连续调用逐行可解析、输入不变；非法样本零写入、编码失败零写入、原始 writer 错误及短写、不关闭 writer，以及真实 RunGatewaySampling emit 到 writer 的接线。当前只定义格式，没有活动曲线、采样性能或新容量结果。


### 采样 JSON Lines 验收（2026-09-30）

开发者实现符合设计，生产代码无需修改。状态与错误恰好一个非 nil，时间非零、耗时/序号非负及状态范围均在输出前校验；私有 DTO 独立复制状态，UTC 转换不修改原输入，查询错误通过 errorText 保留。

新增 [gateway_sample_json_test.go](../../../internal/loadgen/gateway_sample_json_test.go) 七个顶层测试、29 项叶子检查：

- 精确字段集合、v1 版本、非零序号、UTC、纳秒时间、零活动/false/null，以及满额停止状态；原样本时间、状态指针和值保持不变。
- 空文本错误、取消、期限和含中文/引号/换行的组合错误均可成功写出，state 为 null，错误换行被转义，每次恰好一条带换行的 JSON 记录。
- 0/7ns/int64 最大值精度保留；模拟墙钟回退时保留原时间和非负记录耗时，不强制从墙钟重算。边界值与回退样本是输出测试夹具，不是实测事件。
- nil writer、非法样本与超出 time.Time JSON 编码范围的年份均拒绝，校验/编码错误不调用 Write，编码错误保留错误链。
- 底层错误、部分写入错误和无错误短写均正确返回，只调用一次 Write，不调用 Flush/Close，不续写重试。
- 真实 HTTP 连续成功、503 和在途取消生成三条可独立解析的记录，序号、耗时和原始错误保留；查询失败没有被当作交付失败。另验证部分写入错误沿 emit 传播并停止采样，没有第二次查询或写入。

首次回归仅助手的两项字符串期望失败：RFC3339Nano 会把小数末尾的零省略，130ns 编码为 .00000013，而非 .000000130。修正测试期望后复验通过，生产实现未改动。

初次 `go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json` 中命令包通过（8 个顶层测试、62 项叶子检查，5.222s）；修正期望后 `go test -race ./internal/loadgen -count=1 -timeout=120s -json` 通过（64 个顶层测试、259 项叶子检查，5.214s）。最终两包共 321 项检查通过，未报告数据竞争；测试耗时不作为性能数据。

本步只完成 writer 层与采样回调接线；未创建正式样本文件或运行清单，尚无活动曲线或新容量实验。下一步设计独占创建、关闭/错误传播及整场运行完整性记录，再接命令和负载协调。


## 第八步之六：采样文件与内存运行报告（2026-09-30，已验收）

### 目的与拆分

单行 writer 已验收，下一步需要把文件所有权与整场结果明确下来。分两小步推进：本步独占创建样本文件、连接 emit、关闭并返回 GatewayRecordingReport；后续将这个报告和配置写成运行清单，再接命令与负载协调。本步返回报告还只在内存，不能仅凭 JSONL 文件末尾判断完整结束，强杀后可能没有报告。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 覆盖已有输出 | 使用简单 | 可能销毁旧证据，不采用 |
| 追加到已有文件 | 能接着保存 | 不同运行的配置/序号混合，恢复与去重未设计，不采用 |
| 独占创建新文件 | 路径冲突立即暴露，保留一次运行的证据 | 调用方每次选择新路径；本步采用，与负载报告一致 |

先直接写 os.File，不增加 bufio.Writer，避免本轮再引入 Flush 失败及未刷新的数据。直接写入也不保证掉电持久化，且写入耗时会影响同步采样频率。此时请求量小，是否缓冲应由后续测量决定。

不把查询失败等同记录失败。比如两条成功观测和一条查询超时均写出，则 SamplesWritten=3、SuccessfulSamples=2、FailedSamples=1；查询超时行不是缺失。如果第三条输出失败，则它不增加任何“写出成功”计数，OutputErr 保留失败，采样停止；即使文件看起来可解析，也不能忽略该错误宣布记录完整。

### 用户实现接口

新增 internal/loadgen/gateway_recording.go：

```go
// GatewayRecordingReport 保存一次文件记录的配置、时间、计数和退出事实。
// StartedAt 为零表示文件记录尚未启动；本报告本步不直接序列化。
type GatewayRecordingReport struct {
    Config GatewaySamplingConfig // 实际使用的采样配置。
    OutputPath string // 实际使用的输出路径，不自动改名或规范化。
    StartedAt time.Time // 文件创建成功后、调用采样循环前记录。
    FinishedAt time.Time // 文件关闭尝试完成后记录，不仅是最后查询的时间。
    SamplesWritten int64 // WriteGatewaySampleJSON 返回 nil 的记录数。
    SuccessfulSamples int64 // 上述已写出记录中，查询成功的数量。
    FailedSamples int64 // 上述已写出记录中，查询失败的数量。
    SamplingErr error // RunGatewaySampling 返回的原错误，含取消或输出传播错误。
    OutputErr error // emit 中 WriteGatewaySampleJSON 的原错误；没有输出错误时为 nil。
    CloseErr error // 本函数关闭样本文件的原错误；成功时 nil。
}

// RunGatewaySamplingToFile 独占创建输出文件，串行采样并在返回前关闭文件。
// cfg/client/ctx/path 在创建文件前校验；前置失败返回零报告且不查询。
// 文件创建后即使取消或失败，也返回已记录事实并保留文件，不自动删除或重试。
// 返回错误合并采样与关闭错误，保留 errors.Is 链；client 仍由调用方拥有。
func RunGatewaySamplingToFile(
    ctx context.Context,
    client *http.Client,
    cfg GatewaySamplingConfig,
    outputPath string,
) (report GatewayRecordingReport, err error)
```

### 顺序与错误所有权

1. 检查 nil ctx/client，cfg.Validate，strings.TrimSpace(outputPath) 非空，拒绝 outputPath=="-"（本函数不输出到 stdout）；检查 ctx.Err()。这些失败返回零报告、零文件创建、零请求。只用 TrimSpace 判断空白，不擅自修改实际路径。
2. 使用 os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)。不创建父目录；已有文件、目录或符号链接均由独占创建拒绝，不先 Stat 再普通创建。创建失败也返回零报告，不能运行采样。
3. 文件创建成功后初始化 report 的 Config、OutputPath、StartedAt；立即登记 defer 关闭文件。采用具名返回值，让 defer 可以填写原始 CloseErr、在关闭尝试后填写 FinishedAt，并以 errors.Join 把包装后的关闭错误合入最终 err。无论运行因何返回，都只关闭一次，不让调用方再次关闭本函数拥有的文件。
4. 调用现有 RunGatewaySampling，不复制查询循环。emit 调用 WriteGatewaySampleJSON(file,sample)：失败时保存 report.OutputErr 原错误并返回它；成功时 SamplesWritten++，按 sample.Err 是否为 nil 增加 SuccessfulSamples 或 FailedSamples。不能在写之前增加计数。
5. 保存 report.SamplingErr 为 RunGatewaySampling 的原返回错误，并返回报告与该错误；关闭 defer 在真正返回前补齐。不要再次把 OutputErr 加入 errors.Join：它已通过 emit 被 SamplingErr 包装保留。CloseErr 则是独立错误，需要合并。

完整运行中保持 SamplesWritten=SuccessfulSamples+FailedSamples。计数是 WriteGatewaySampleJSON 成功返回的事实，不是独立磁盘持久化计数；关闭失败不回滚此前计数，而是通过 CloseErr 说明收尾失败。

不能仅因 errors.Is(err,context.Canceled) 就忽略整个返回错误，因为 errors.Join 可能还包含 CloseErr。本层也不吞掉父取消：控制流主动取消与异常如何映射为命令退出码，留后续协调层结合报告单独判断。单次查询超时可以写成 FailedSamples 并继续，不是 OutputErr。

创建文件与启动采样之间取消的竞争允许留下空的新文件：报告 StartedAt 非零，写出数为零，SamplingErr 保留父错误。这里 StartedAt 表示记录流程已拥有输出文件，不承诺已经发起网络查询。启动前就已观察到父取消，则不创建文件。对任意同步文件 I/O 不承诺 context 能强行中断。

本步没有 manifest/status/complete 字段，没有原子发布、重连追加或失败删除。后续运行清单要保存此报告、配置与结束原因；读取端需同时检查清单、计数和 JSONL，而不能把完整行的存在当作整场成功证据。

### 验收计划

助手补测试：前置失败/已有路径/缺少父目录零请求、不覆盖旧内容；实际文件与真实 HTTP 成功/失败样本统计、取消后的最后样本及错误链、时间边界、空或部分记录保留、输入配置不变。验证 SamplesWritten 的计数口径及输出错误沿 emit 停止的接线。单行 writer 和 emit 的故障注入已在前步覆盖；真实 os.File 的写入/关闭故障如未注入必须明确只做代码审查，不把普通临时文件测试说成磁盘故障验收。文件层通过后再设计运行清单的格式与保存方式，不预写新的容量结果。


### 文件层验收（2026-09-30）

开发者实现符合设计，生产代码无需修改。前置校验先于独占创建，文件创建后立即登记关闭 defer；成功写出后才增加分类计数，原始采样/输出/关闭错误分别保留，关闭错误通过具名返回值与采样错误合并。

新增 [gateway_recording_test.go](../../../internal/loadgen/gateway_recording_test.go) 五个顶层测试、19 项叶子检查：

- nil 依赖、非法配置、空白/空/stdout 路径及启动前取消/过期均零报告、零请求，临时输出目录保持空。
- 已有文件、目录、指向现有文件的符号链接及悬空链接全部拒绝，保留旧内容与链接；父目录不存在时不创建父目录，错误保留 os.ErrExist/os.ErrNotExist 链。
- 真实 HTTP 与临时文件的三组结果：混合场景写出三行（查询成功 1、查询失败 2，含最终在途取消）；全失败场景保留三条失败行；父期限中断在途请求后保留一条失败行。各组逐行序号、状态/错误分类和报告计数一致，关闭无报错，配置与路径未改写，开始/结束时间在函数调用范围内。
- 成功读取后父取消仍写出一条成功记录，路径中的有效首尾空格未被删除；调用方连接池未被关闭。
- 两个调用同时争用同一路径，仅一个创建成功并发起一次查询，另一个零报告返回 os.ErrExist；取消获胜调用后仍保留一条失败样本。文件为普通文件，组和其他用户无访问权限。

`go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json` 通过：loadgen 69 个顶层测试、278 项叶子检查（6.016s），命令包 8 个顶层测试、62 项叶子检查（5.128s），共 340 项，未报告数据竞争。这些是临时目录行为测试，不是正式负载实验。

真实 os.File 写入/关闭故障本轮未注入，相关路径仅代码审查：输出失败计数不增加、OutputErr 经 SamplingErr 传播；defer 保存 CloseErr 并通过 errors.Join 合并，不会覆盖先前错误。前步 io.Writer 注入证明的是行输出和 emit 传播，不冒充本轮磁盘故障验证。文件创建后、首次尝试前发生取消而留下空文件的竞争窗口也未做确定性注入；依据现有控制流保留设计约定。

目前内存报告返回后尚未保存为运行清单，单独 JSONL 不足以证明整场记录完整；下一步设计清单格式与写入流程，再接命令及负载协调。没有新增活动曲线或容量数据。


## 第八步之七：采样运行清单 JSON（2026-09-30，已验收）

### 目标与表达方式

GatewayRecordingReport 目前只在内存。清单保存一次记录的配置、路径、时间、计数和错误，使后续读者能够结合 JSONL 复核运行结果。本步只实现 io.Writer 输出，不在 RunGatewaySamplingToFile 里面隐式创建第二个文件；文件与命令层在后续明确两份产物的所有权和失败处理。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 只保存总体 success/complete | 展示简单 | 混淆主动取消、查询失败、写入失败和关闭失败，容易过度解释 |
| 只保存错误文本 | 保留信息 | 读者必须解析错误字符串判断取消/期限，脆弱 |
| 结构化停止原因 + 各类原始错误与计数 | 容易分析，保留诊断信息，各种失败不会互相覆盖 | 字段稍多；本步采用 |

停止原因仅解释采样循环为什么返回，不代替文件收尾状态。主动取消 + 关闭失败仍输出 stop_reason=canceled 和非空 close_error；全部查询失败但记录成功时，failed_samples 可以等于 samples_written。不能仅看到 canceled 或完整 JSON 就宣布负载实验成功。

### 清单契约

独立格式 schema_version=1，一个清单对应一份采样 JSONL（当前采样行格式 v1）。使用易读缩进 JSON，末尾一个换行；全部字段存在，不用 omitempty：

```json
{
  "schema_version": 1,
  "config": {
    "endpoint": "http://127.0.0.1:8080/debug/gateway",
    "interval_ns": 100000000,
    "request_timeout_ns": 200000000
  },
  "output_path": "samples.jsonl",
  "started_at": "2026-09-30T00:00:00Z",
  "finished_at": "2026-09-30T00:00:03Z",
  "samples_written": 3,
  "successful_samples": 2,
  "failed_samples": 1,
  "stop_reason": "canceled",
  "sampling_error": "context canceled",
  "output_error": null,
  "close_error": null
}
```

配置数值为整数纳秒，计数为 int64；时间转 UTC 副本，保留输出路径原文。错误用已有 errorText 保留 nil/null 与空文本错误的区别；不从文本判断类型。清单未加入性能结论、活动数峰值或覆盖时长，也不新增总体 complete 布尔值。

### 用户实现范围

新增 internal/loadgen/gateway_recording_json.go：

- 私有 gatewaySamplingConfigJSON：Endpoint string、IntervalNS int64、RequestTimeoutNS int64，标签对应 config 内三个字段。
- 私有 gatewayRecordingJSON：SchemaVersion int、Config gatewaySamplingConfigJSON、OutputPath string、StartedAt/FinishedAt time.Time、SamplesWritten/SuccessfulSamples/FailedSamples int64、StopReason string、SamplingError/OutputError/CloseError *string。标签与契约一致，各字段注明含义。
- 私有 gatewayRecordingStopReason(report GatewayRecordingReport) string，注释说明只分类循环退出原因，不判定关闭是否成功。
- 导出函数如下：

```go
// WriteGatewayRecordingJSON 将已收尾的记录报告导出为清单 v1。
// 报告中的运行/输出/关闭错误是待保存事实，不阻止有效清单输出。
// 返回错误仅表示校验、编码或清单写入失败。
// 不修改 report，不访问其 OutputPath，也不创建/关闭/刷新 writer。
// 校验/编码失败零写入；写入失败可能留下部分清单，调用方负责处理。
func WriteGatewayRecordingJSON(w io.Writer, report GatewayRecordingReport) error
```

### 校验与分类规则

先完成校验，再编码和写入：

1. w 非 nil；report.Config.Validate() 通过。
2. OutputPath 非空白，且不为 "-"；只检查文本，不 Stat/读取 JSONL、不推断路径相对于清单的位置。本步不转换相对路径。
3. StartedAt 与 FinishedAt 均非零，表示报告已启动并完成关闭尝试；不从序列化墙钟判断单调顺序，不新增重算耗时。
4. 三个计数均非负。先检查 SuccessfulSamples <= SamplesWritten，再检查 FailedSamples == SamplesWritten - SuccessfulSamples；使用减法避免直接相加 int64 溢出。0/0/0 合法，代表可能已创建文件却尚无成功写出的行。
5. OutputErr 非 nil 时，SamplingErr 也必须非 nil：当前输出错误通过 emit 使采样退出。无需比较错误文本或验证 errors.Is(SamplingErr,OutputErr)，不拒绝 CloseErr 非 nil 的报告。

stop_reason 使用如下优先级（判断错误本身，不看字符串）：

| 条件 | stop_reason |
| --- | --- |
| OutputErr != nil | output_error |
| errors.Is(SamplingErr, context.DeadlineExceeded) | deadline_exceeded |
| errors.Is(SamplingErr, context.Canceled) | canceled |
| SamplingErr != nil | sampling_error |
| SamplingErr == nil | returned |

输出错误优先，是因为写入失败也可能包着 context.Canceled；不能把交付失败误报为普通取消。DeadlineExceeded 优先于 Canceled，若一个构造错误链同时包含二者，分类明确。returned 只表示采样无错误返回；当前无限采样循环通常以取消结束，但清单允许没有采样错误的已收尾报告，不把 returned 当作关闭成功。CloseErr 不参与该分类，单独保存。

校验通过后构造私有 DTO，转换 UTC、纳秒并使用 errorText；json.MarshalIndent 后追加换行，调用一次 Write。底层错误使用 %w 包装；err==nil 但 n!=len(data) 返回包装的 io.ErrShortWrite。不续写或重试，不返回报告自身的 SamplingErr/OutputErr/CloseErr 作为清单写入结果：错误报告写成功同样返回 nil。

### 完整性与后续接线

本函数只序列化报告，不能证明实际文件与报告匹配。后续分析应核对样本行数、序号与分类计数，并检查 output_error/close_error；再结合外层运行清单与负载时间窗口确认采样覆盖。相对路径需要由后续运行目录约定解释。没有清单、清单截断或清单保存失败都不能默认为运行成功；原 JSONL 仍应保留。

这一步既不创建清单文件，也不修改采样记录函数。下一步统一设计两个产物的路径、独占创建与关闭顺序、保存失败返回规则，以及停止原因与命令退出码的关系，避免在序列化层隐式承担文件编排。

### 验收计划

助手补精确格式、版本/单位/UTC/null、空错误、三类错误同时保留、分类优先级、零样本/int64 边界与防溢出、矛盾报告零写入、编码失败/短写/错误链及输入不变测试；用真实 RunGatewaySamplingToFile 返回报告输出清单，并与临时 JSONL 行数核对。负载实验与清单发布完整性仍未实现，不预写容量结果。


### 运行清单 JSON 验收（2026-09-30）

开发者实现符合设计，生产代码无需修改。计数按非负、成功数不大于总数、失败数等于差值校验，避免直接相加溢出；三类错误分别输出，停止原因按约定优先级分类。有效的错误报告写出成功时返回 nil。

新增 [gateway_recording_json_test.go](../../../internal/loadgen/gateway_recording_json_test.go) 七个顶层测试、40 项叶子检查：

- 精确字段、清单 v1、配置单位、UTC/纳秒、路径原文、null 和缩进/结尾换行；编码不修改原报告。
- 12 种错误组合验证 returned、取消、期限、其他采样错误、输出错误的分类与优先级；错误文本恰好是“context canceled”但不具备相应类型时仍分类为 sampling_error。空错误文本、多行错误及三类错误同时出现均完整保留，关闭错误不改变循环停止原因。
- 零样本、全成功/全失败/混合的 int64 最大值、配置时长最大值和 1ns 精度保留；墙钟回退夹具不被重排，不能将这些构造值当作实测事件。
- 非法配置/路径/时间/计数、溢出风险组合、输出错误缺少采样错误及不支持编码的年份均零写入，编码错误保留错误链；底层错误、部分写入错误和短写只调用一次 Write，不接管 Flush/Close。
- 样本路径不存在也可编码清单，不创建文件或目录。清单编码不承担数据文件校验。
- 真实 HTTP 采样、JSONL 文件、内存报告再到清单，三条记录（查询成功 1、失败 2，含最终取消）计数一致，stop_reason=canceled、采样错误保留，输出/关闭错误为 null；原 JSONL 字节不变。

`go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json` 通过：loadgen 76 个顶层测试、318 项叶子检查（5.907s），命令包 8 个顶层测试、62 项叶子检查（6.045s），共 380 项，未报告数据竞争。测试耗时和构造样本不作为性能或容量数据。

本步清单输出仍写入调用方 writer，没有实现清单文件创建、关闭或发布。下一步统一处理 JSONL 与清单路径、独占创建和错误合并，再接命令与负载协调。临时文件接线测试不等于已经完成正式采样实验。


## 第八步之八：两个记录产物的文件编排（2026-09-30 设计，2026-10-01 验收）

### 目标与方案选择

已有两个输出函数和样本文件层。本步组合它们，形成一次运行的目录；不复制采样循环，不在已有 RunGatewaySamplingToFile 内隐式追加清单副作用。

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 用户分别指定两个文件路径 | 灵活 | 路径可能相同或别名冲突，文件归属与迁移更难管理 |
| 一个新目录、两个固定文件名 | 同一次运行的文件聚合，避免参数间冲突，可整体移动 | 每次运行使用新目录；本步采用 |
| 文件都结束后再统一发布目录 | 读者易识别最终产物 | 需要临时目录、发布与崩溃恢复协议，当前先不实现 |

目录必须新建，固定文件名 samples.jsonl 与 manifest.json。父目录必须已存在；不覆盖、不追加、不复用已有目录，即使旧目录为空也拒绝。先创建清单文件，尽早发现路径问题；这不保证之后磁盘一定可写，运行中的保存错误仍必须保留。

清单中的 output_path 固定为相对于清单所在目录的 samples.jsonl，方便整体移动。函数返回的记录报告仍保留实际绝对样本路径：写清单时复制报告再改副本，不修改返回报告。此处是对上一小步暂未固定的相对路径解释作出明确选择。

### 新类型与函数

新增 internal/loadgen/gateway_recording_run.go：

```go
// GatewayRecordingResult 描述样本记录和清单保存两个层次的结果。
type GatewayRecordingResult struct {
    Recording GatewayRecordingReport // 样本文件层的原始报告，OutputPath 为实际绝对路径。
    ManifestPath string // 清单的绝对路径；目录创建成功后填写。
    ManifestErr error // 清单创建、编码/写入、关闭错误，多个错误用 errors.Join 保留。
    ManifestSaved bool // 仅清单写入及关闭均成功时为 true，不表示实验成功或掉电持久化。
}

// RunGatewayRecording 在新的 outputDir 中记录样本并保存清单。
// ctx/client/cfg/outputDir 前置检查失败返回零结果，不创建目录、不查询。
// 目录创建后保留所有已有产物，失败不删除；函数返回前关闭所拥有的清单文件。
// result.Recording 仍只描述样本文件，清单错误单独记录；err 合并两个层次的错误。
// 不吞掉父取消，不修改或关闭 client；本函数本身不启动后台 goroutine。
func RunGatewayRecording(
    ctx context.Context,
    client *http.Client,
    cfg GatewaySamplingConfig,
    outputDir string,
) (result GatewayRecordingResult, err error)
```

ManifestSaved=true 不能取代 Recording.OutputErr/CloseErr：可能成功保存的是一份“样本输出失败”的报告。ManifestErr=nil 也不单独意味着清单已保存：记录流程未启动时，清单可能保持空文件，ManifestSaved=false。不要把清单错误写进 Recording.CloseErr 或 SamplingErr，否则清单自己的错误会冒充样本文件的错误。

### 执行顺序

1. 检查 nil ctx/client、cfg.Validate、outputDir 非空白且不为 "-"，用 filepath.Abs 解析绝对目录，检查 ctx.Err()。不 TrimSpace 后替换实际目录字符串。前置失败零结果、零文件系统创建、零查询。
2. os.Mkdir(absDir,0700)，不使用 MkdirAll；已有目录/文件/链接均失败。创建失败返回零结果。创建成功后计算固定绝对路径，填写 result.ManifestPath。
3. os.OpenFile(manifestPath,os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600)，此时只是空文件。打开失败记录带路径且 %w 包装的 result.ManifestErr，返回该错误，不调用采样函数，已创建目录保留。
4. 清单打开后立即登记关闭 defer，声明局部 manifestWritten=false。defer 中关闭一次清单，关闭错误同时合入 result.ManifestErr 和具名 err；最后 result.ManifestSaved=manifestWritten && closeErr==nil。不要在 Write 成功后就提前宣布已保存。
5. 调用 RunGatewaySamplingToFile(ctx,client,cfg,samplesPath)，将其报告放进 result.Recording，保留返回的 recordingErr。该调用返回时样本文件的关闭尝试已完成，清单可以记录样本 CloseErr。
6. 如果 Recording.StartedAt 为零，说明样本文件记录尚未启动：不将零报告交给清单序列化，直接返回 result 和 recordingErr；空清单及目录保留，由 defer 关闭，ManifestSaved 保持 false。根据现有子函数契约，此时 recordingErr 必须非 nil。
7. 已启动则无论 recordingErr 是否非 nil，均尝试保存最终清单。复制 manifestReport:=result.Recording，把副本 OutputPath 设为 "samples.jsonl"，调用 WriteGatewayRecordingJSON(manifestFile,manifestReport)。成功设置 manifestWritten=true；失败以 %w 和清单路径包装，保存到 result.ManifestErr。
8. 返回 result 与 errors.Join(recordingErr,result.ManifestErr)，随后关闭 defer 补齐关闭结果。recordingErr 已含样本关闭错误，不再单独 join Recording.SamplingErr/OutputErr/CloseErr，避免重复。清单关闭错误只在 defer 中添加一次。

父取消是正常协调过程中可能出现的停止原因，但本函数继续保留 context.Canceled/DeadlineExceeded 错误链；后续命令层必须结合 Recording.OutputErr、Recording.CloseErr、ManifestSaved 与 ManifestErr 决定退出码，不能仅凭 errors.Is(err,context.Canceled) 忽略其余错误。清单保存不再受已经取消的请求 context 控制，以便尽力记录最后事实；普通同步文件写入仍可能阻塞，不新增强行中断或后台写入承诺。

### 文件状态与完整性解释

- 运行前已经取消：零结果，不创建目录。
- 创建目录后、样本文件创建前取消：可能只留下空清单，Recording 为零、ManifestSaved=false；本步不尝试编造一次采样报告。
- 样本文件已经创建、首次查询前取消：允许零行 JSONL 与合法零计数最终清单，依据已有记录报告写出。
- 采样因取消/期限/样本保存错误退出：先完成样本关闭，再尽力保存包含这些事实的清单。
- 清单编码/写入/关闭失败：保留样本文件与空或部分清单，ManifestErr 非 nil、ManifestSaved=false，返回错误；不重试、不删除证据。
- 进程强杀可能留下空清单、截断清单或恰好可解析的内容；清单不是跨文件事务或原子发布。本轮只保证函数正常返回路径上的错误与关闭处理，不承诺掉电持久化。

最终读者还需结合外层进程退出结果、清单错误事实、样本行数/序号/分类计数和实验时间窗口核对。尤其清单自身关闭错误只能由本函数返回结果及后续命令报告，不能靠清单自证其关闭成功。代码提交、完整环境和文件哈希继续由已有实验编排层归档，本步不重复塞入采样核心。

### 验收计划

助手补前置拒绝不创建目录、已有目录/文件/链接保护、缺少父目录、同目录并发竞争、实际 HTTP 样本与清单计数和相对路径一致、主动取消仍保存最终清单、返回报告绝对路径不变、两份产物移至另一目录后按清单相对路径可读取等测试。核对异常路径不覆盖旧数据、保存结果不冒充查询成功。

普通临时文件测试不证明真实磁盘故障；无法确定性注入的清单创建/写入/关闭或竞争窗口按代码审查明确记录，不为了注入而随意关闭其他进程文件描述符。验收通过后再接命令入口、信号和退出码，最后接基线实验。当前仍无正式采样曲线或容量数据。


### 实现与验收（2026-10-01）

开发者实现 [RunGatewayRecording](../../../internal/loadgen/gateway_recording_run.go)，本轮审查未发现需要修改的生产逻辑。助手新增 [文件编排测试](../../../internal/loadgen/gateway_recording_run_test.go)，共 5 个顶层测试、23 项叶子检查：

- 前置拒绝 10 项：nil context/client、无效 URL/间隔/查询期限、空目录/空白目录/标准输出占位、已取消/已到期。均返回零结果，不查询、不在测试输出父目录中创建产物。
- 路径保护 6 项：已有空目录、含旧样本与清单的目录、普通文件、目录链接、悬空链接、缺少父目录。均不进入查询，旧内容与链接保持不变，不自动创建父目录。
- 文件编排与迁移 3 项：绝对路径、相对路径和含首尾空格的目录名。请求开始时两个空文件已存在；成功响应读取完成后取消父 context，成功样本仍落盘并生成最终清单。返回报告保留绝对路径和实际配置，清单仅将样本路径改为 samples.jsonl；文件内容与最终报告一致。整体重命名目录后仍可按清单相对路径读到相同样本，调用方 client 与连接池所有权保持不变。
- 真实 HTTP 3 项：混合结果写出 3 行（1 成功、2 失败），全失败写出 3 行，父期限到期写出 1 行失败记录；均保留相应取消/到期错误链，同时 ManifestSaved=true。逐行核对序号、分类和报告计数，说明“清单保存成功”不能等同“查询成功”。
- 同目录竞争 1 项：两个调用同时启动，只有一个进入查询；另一个返回 os.ErrExist 和零结果。取消后获胜者保存 1 行失败样本与最终清单，没有覆盖或混写。

运行 `go test -race ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json`：loadgen 包 81 个顶层测试、341 项叶子检查通过（6.349s）；命令包 8 个顶层测试、62 项叶子检查通过（5.201s），共 403 项，未报告数据竞争。以上是正确性验收与回归数据，不代表负载性能或稳定容量。

本轮没有确定性注入目录已创建后的清单创建失败、真实磁盘写入/关闭失败，或文件创建期间的取消窗口；这些异常分支仅完成代码审查。检查了清单关闭 defer、错误合并、未启动报告不编码，以及已有产物不删除的路径，不能将其表述为磁盘故障实验已通过。普通关闭成功也不提供 fsync、跨文件事务或崩溃恢复保证。

本步使一次采样具备可搬移、可核对的两份文件。命令入口、信号处理、退出码和与负载实验的协调仍待实现；正式活动会话采样曲线及容量结果尚未取得。


## 第八步之九：采样命令参数（2026-10-01，已验收）

### 问题与方案选择

文件编排已经可以保存一次采样，实验操作者仍需要稳定的启动方式，明确查询哪里、多久查一次、何时停止和证据保存位置。参数留在临时代码中不利于重复实验；命令参数则能被实验脚本记录与复用。

| 方案 | 优点 | 代价与本次选择 |
| --- | --- | --- |
| 每次写临时 Go 程序调用 | 接线快 | 配置和退出处理重复，不作为持续使用入口 |
| 在 loadgen 中增加采样选项 | 单进程容易协调负载与采样 | 当前入口同时增加两类运行及错误处理；独立观察网关不方便，暂不采用 |
| 新增 gateway-sampler 命令 | 复用现有采样核心，可独立运行，可覆盖负载前后观察窗口 | 后续脚本需协调两个进程；本次采用 |

按进程职责选择独立命令，不新增重复采样循环或改动已验收的核心函数。实验窗口同步与退出状态记录仍由后续实验编排实现，独立命令不自动知道负载何时结束。

### 本小步实现边界

仅新增 cmd/gateway-sampler/config.go，package main；先实现参数解析与校验，不创建 main/run 占位函数，不发请求、不创建目录、不监听信号。该命令在下一小步接入 main 后才能运行。

```go
// samplerConfig 描述一次采样命令的配置；Duration 属于命令运行策略。
type samplerConfig struct {
    Sampling loadgen.GatewaySamplingConfig // 地址、查询间隔和单次查询期限。
    Duration time.Duration                 // 从后续 run 建立运行 context 起计算的采样预算，必须为正。
    OutputDir string                       // 运行时独占创建的新目录；解析阶段保持用户输入。
}

// parseSamplerConfig 解析不含程序名的参数，复用采样核心的配置校验。
// 不访问网络或检查/创建输出目录，不注册信号，不退出进程。
// 帮助请求返回 flag.ErrHelp；任意错误均返回零配置。
func parseSamplerConfig(args []string) (samplerConfig, error)
```

| 参数 | 字段 | 默认值 | 含义 |
| --- | --- | --- | --- |
| -url | Sampling.Endpoint | http://localhost:8080/debug/gateway | 网关快照 HTTP 地址 |
| -interval | Sampling.Interval | 100ms | 上条查询及样本交付完成后的等待时间，不是固定 10Hz |
| -request-timeout | Sampling.RequestTimeout | 1s | 单次查询预算，包括响应体读取 |
| -duration | Duration | 30s | 整体采样运行预算；不是包含文件收尾的进程退出硬期限 |
| -output-dir | OutputDir | 空，必须填写 | 新产物目录，父目录须在实际运行时存在 |

默认值用于开发操作，不代表经过容量实验确定的生产配置。选有限时长便于脚本运行，防止遗忘停止；提前终止将由后续信号处理支持。Duration 与 RequestTimeout、Interval 不强制大小关系：剩余运行预算不足一次查询期限时，由后续父子 context 协作限制查询；短运行可能没有成功样本，应如实记录。

### 解析顺序与错误契约

1. 初始化默认配置，建立 flag.NewFlagSet("gateway-sampler", flag.ContinueOnError)，绑定五个参数。
2. 调用 Parse(args)；任何错误返回 samplerConfig{} 和原错误，保留 flag.ErrHelp。不调用 os.Exit。
3. 拒绝剩余位置参数，错误包含意外参数值。
4. 调用 cfg.Sampling.Validate()，用 %w 包装错误；不在命令层复制 URL 和采样时长校验。
5. 检查 cfg.Duration > 0，错误说明是整体运行时长。
6. 用 strings.TrimSpace 仅判断 OutputDir 非空白，拒绝精确的 "-"；不改写合法目录名中的空格，不转绝对路径。
7. 成功返回 cfg,nil；所有失败分支返回零配置。

不调用 os.Stat 预判路径可用性：检查和实际创建之间目录仍可能变化。已有 RunGatewayRecording 的独占创建负责最终判断，因此解析阶段允许已有目录或缺少父目录的路径字符串，实际运行再拒绝。帮助文本说明 interval 是完成后等待、duration 不包含保存完成保证。

### 后续运行层要解决的问题

后续 main/run 会创建和回收命令自己的 HTTP 客户端资源，组合有限运行预算与 SIGINT/SIGTERM，并在 RunGatewayRecording 返回后汇总运行结果。整体按计划到期与单次查询超时含义不同，前者触发采样停止，后者是可记录并继续的失败样本。核心层继续保留原始错误；命令层决定退出状态，必须同时检查样本与清单保存事实，不能仅凭 errors.Is(err, context.DeadlineExceeded/Canceled) 忽略同一错误树里的保存失败。具体退出策略在下一小步单独确定，本步不实现错误吞并。

当前清单保存采样核心配置，尚不包含命令的 Duration 与信号来源；后续命令/实验编排需记录启动参数与退出事实，不将已有清单描述为完整的进程运行记录。

### 验收计划

开发者完成后，助手补参数测试：默认值（显式给输出目录）、全部覆盖、帮助、未知参数、位置参数、URL/时长解析及非正值、缺失/空白/标准输出目录、保留合法路径空格、解析不检查或创建目录、失败返回零配置。运行定向测试并按变更范围回归；当前仅设计，无新测试结果或容量数据。


### 初审与行为验证（2026-10-01，待包名修正）

参数绑定、共享校验、错误时零配置返回和路径保持符合约定。助手新增 [config_test.go](../../../cmd/gateway-sampler/config_test.go)，暂时与当前实现使用相同的 package gateway_sampler，以便独立验证解析行为：4 个顶层测试、45 项叶子检查，通过 `go test -race ./cmd/gateway-sampler -count=1 -timeout=30s -json`（1.474s）。

覆盖 9 项默认值/覆盖值/时间独立性/路径保持检查、28 项非法输入、4 项必填与帮助、4 项文件系统无副作用检查。已有目录和普通文件在解析时不被拒绝或修改，缺少父目录时也不创建文件；这些可用性判断仍交给实际运行层。

尚未完成本小步验收：config.go 首行为 package gateway_sampler，与独立命令需要的 package main 不符；需将实现与测试文件的包名一起改为 main，再复验。当前通过仅说明解析行为正确，不代表命令已可运行。另外建议把 duration 参数帮助文本明确为采样预算，说明文件收尾可能超过该时长，与字段注释保持一致。未添加 main/run，未运行网络实验，无新增容量数据。


### 修正后验收（2026-10-01）

开发者已将 config.go 与 config_test.go 的包名统一为 main，并将 duration 帮助文案改为 `sampling time budget; file finalization may take longer`。参数解析行为保持不变，格式检查通过。

复验 `go test -race ./cmd/gateway-sampler ./internal/loadgen ./cmd/loadgen -count=1 -timeout=120s -json`：采样命令包 4 个顶层测试、45 项叶子检查通过（1.515s）；loadgen 包 81 个顶层测试、341 项叶子检查通过（6.172s）；负载命令包 8 个顶层测试、62 项叶子检查通过（6.159s）。共 448 项检查通过，无跳过或数据竞争报告。

本小步完成参数解析验收；main/run 尚未实现，因此当前不能把 gateway-sampler 作为完整命令运行。下一步明确运行预算、停止信号与产物错误的退出策略后接入入口。此次为正确性回归，无新增正式采样曲线或容量结果。
