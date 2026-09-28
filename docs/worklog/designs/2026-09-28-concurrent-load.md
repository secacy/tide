# 第五阶段：并发负载工具与实验口径

状态：有限静音 PCM 源、客户端结果和发送观察回调已实现并验收；完整单会话报告与并发运行待实现。已有多 Worker 配置、会话轮询与连接回收验收；本方案尚无容量实测数据。

当前小步：OnWrite 发送观察已验收，新增 12 项检查与原有 20 项接收检查通过定向 race。开始实现每场独立的 SessionRecorder，汇总两侧观测并提供一致快照；等待开发者实现。完整会话执行与成功判定留后续步骤。

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

## 第三步之一：单会话事实汇总（2026-09-28，待实现）

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

### 后续验收

助手补测试：零值/缺失控制事件、成功与失败计数、失败控制消息保存、最大耗时包含失败而成功时间范围不包含失败、final 后继续记录、输入时间原样保留、修改快照不影响内部状态、两个实例隔离，以及一个发送协程/一个接收协程/并发快照的 race 检查。不启动网络或正式负载；通过后再指导 runner 接线与完整报告。
