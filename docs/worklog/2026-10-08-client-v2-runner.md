# 第六阶段：单连接 v2 客户端运行器

日期：2026-10-08。状态：设计与实现指导，等待开发者实现；本轮没有新增业务代码或测试结果。

## 目标与实施范围

已有 audioReplayBuffer 和 v2ClientState 通过串行组合验收。按最新范围调整，本步直接连接真实 WebSocket，消息解析作为运行器中的小函数完成。验收能力是一次 start → ready → 双向音频/结果 → end → completed → completed_ack → 本地任务全部退出。

实现独立 V2Client/NewV2/Run，既有 v1 入口继续用于原负载工具。建议代码组织为 client_v2.go（API/配置/主循环）、io_v2.go（采集、reader、writer 和必要解析），两个文件组成同一可验收步骤。下一组工作加入有限重试与原场恢复，随后做故障实验；本步仅拨号一次，不发送 resume，也不重新新建替代失败会话。

## 为什么需要协调循环

写 socket 可能等待，此时仍需处理音频接纳确认、识别结果和取消。输入 EOF 只表示音频结束，还要等 Worker 的尾部结果。多个任务发生错误后，还必须实际关闭源和连接，等待任务退出，不能仅发出取消信号就返回。

| 方案 | 收益 | 代价与选择 |
| --- | --- | --- |
| 一个循环直接读源、写音频、读结果 | 初始代码直观 | 任一阻塞调用都会阻止另一个方向前进，无法持续双向处理 |
| reader/writer 分别更新状态、用锁保护 | 每个方向可独立 I/O | 缓存释放、end 顺序、结果应用和完成裁决需要跨任务协调 |
| 一个协调循环拥有状态，三个任务负责阻塞 I/O | 状态提交串行，网络写慢时仍可处理其他事件，清理责任集中 | 需要明确有界事件与退出顺序；本步采用 |

协调循环在 Run 调用者 goroutine 内执行。source、reader、writer 各一个 goroutine；三者均不读取或修改 v2ClientState。握手在这些任务启动前同步执行，因此同一时刻仍只有一个 WebSocket 读者/写者。

结果回调本步在协调循环中同步执行，要求快速且响应取消，另给每次应用一个超时 context。这样保留已验收的检查/应用/提交串行约定。慢回调会暂缓其他事件处理，这是首版明确的限制；以后只有出现实际慢业务需求，才增加有界异步结果应用任务。

## API 骨架

```go
// V2Config 是单连接 v2 客户端配置；零值使用下述默认值，负值拒绝。
type V2Config struct {
    URL string // 实验 /v2/asr 的 WebSocket 地址，必填。
    ChunkBytes int // PCM 分块，默认 3200；必须为正且对齐 audio.BytesDepth。
    Realtime bool // 按固定音频时间轴模拟采集；false 表示尽快读取。
    ReadLimit int64 // 服务端完整消息上限，默认 64 KiB。
    MaxReplayBytes uint64 // 未确认音频负载上限，默认 10*audio.BytesPerSecond。
    MaxReplayChunks int // 未确认块数上限，默认 128。
    HandshakeTimeout time.Duration // Dial/start/首帧 ready 共享预算，默认 10s。
    WriteTimeout time.Duration // 单次 Write 预算；也用作完成后确认总窗口，默认 5s。
    ApplyTimeout time.Duration // 单次结果业务应用预算，默认 5s，需回调合作取消。
    CompletionTimeout time.Duration // 首次 end 授权起到核实 completed 的固定预算，默认 30s。
    OnResult V2ResultHandler // nil 表示有意消费并丢弃结果，仍推进应用确认。
}

// V2ResultHandler 成功返回后才允许推进 appliedSeq 并确认给 Gateway。
// 按序同步调用，必须快速或响应 ctx；不得重入同一 Run 的状态操作。
// 失败/超时即终止本次运行，不自动重试可能已有外部副作用的应用。
type V2ResultHandler func(ctx context.Context, result wsprotocol.SequencedResultMessage, receivedAt time.Time) error

// V2RunResult 区分已核实的业务完成与完成确认写入。
// Completed=true 时，即使 err 非 nil，也不撤销已知完整结果。
type V2RunResult struct {
    Completed bool // 已经通过 acceptCompleted 记录本场成功。
    FinalOffset uint64 // 仅 Completed=true 时有效，最终音频位置。
    AppliedSeq uint64 // 返回时已连续成功应用的位置，允许 0。
    CompletionAckWritten bool // completed_ack 的实际 Write 返回 nil；不证明对端已处理。
}

// V2Client 只保存归一化配置；每个 Run 创建自己独占的状态和任务。
// 多个 Run 共用 OnResult 时，调用方保证回调所用共享数据的并发安全。
type V2Client struct { cfg V2Config }

// NewV2 校验/填充配置；ChunkBytes 不得超过 MaxReplayBytes，须能安全加 8 字节头。
func NewV2(cfg V2Config) (*V2Client, error)

// Run 对一个音频源执行一次新建会话，返回前等待本次创建的任务退出。
// 非 nil source 交入后由 Run 负责且仅负责一次 Close，包括拨号失败/预先取消。
// source.Close 必须可与 Read 并发，及时返回并解除阻塞 Read；Run 期间调用方不再使用它。
// nil error 表示完成已核实、完成确认写成功且本地清理成功；其他情况结合 result.Completed 判断。
func (c *V2Client) Run(ctx context.Context, source io.ReadCloser) (V2RunResult, error)

// V2EntryError 保存完整入口拒绝的 code；未知非空 code 也保留。
// Error 返回固定安全说明，调用方可用 errors.As 取得 Code，不依赖提示文字。
type V2EntryError struct { Code wsprotocol.V2ErrorCode }
func (e *V2EntryError) Error() string

// ErrV2Incomplete 表示对端正常关闭但本地尚未核实 completed，不能把它当作成功。
var ErrV2Incomplete = errors.New("v2 session ended before completion")
```

校验 URL、正数和分块对齐即可，不新增一套通用配置框架。超时/读写错误使用阶段说明包装并保留 errors.Is 原因。既有 uint64,string 消息字段直接使用；end 和 result_ack 的字符串位置用 strconv.FormatUint 构造。

Run 的 source 合约是实现前提：任意 io.Reader 并不保证可取消，io.NopCloser 包装阻塞 Reader 也不会自动获得解除阻塞能力。测试会提供遵守关闭合约的源；对违反合约的源/回调，Go 无法强制终止 goroutine，不通过“等待超时即返回”伪装清理完成。

## 任务与有界事件

建议三个事件通道容量均为 1，加容量为 1 的 writer 请求通道。通道由本次 Run 独占；不依赖关闭事件通道表示业务完成，避免 select 反复收到零值。source EOF、reader 错误和 writer 回执均显式表达。另用 WaitGroup 等待实际退出。

```go
// v2SourceEvent 交付独立 PCM 块或源终态；EOF 可与非空尾块一起出现。
// err 非 nil 表示采集失败；协调者不把该事件封口成正常输入结束。
type v2SourceEvent struct {
    data []byte
    eof bool
    err error
}

// v2ReadEvent 按实际读取顺序交付完整消息与最终读取错误。
// data 有效时 err=nil；receivedAt 紧邻 Read 成功后记录。
type v2ReadEvent struct {
    data []byte
    receivedAt time.Time
    err error
}

// v2WriteKind 区分音频、end、累计结果确认和整场完成确认。
type v2WriteKind uint8
const (
    v2WriteAudio v2WriteKind = iota + 1
    v2WriteEnd
    v2WriteResultAck
    v2WriteCompletedAck
)

// v2WriteJob 是唯一待写或在途任务；音频 data 为独立副本。
// offset 用于音频起点/end 或 completed_ack 终点；seq 用于两个结果确认。
type v2WriteJob struct { kind v2WriteKind; offset uint64; seq uint64; data []byte }

// v2WriteReceipt 表示 Write 已经返回；不包含音频或凭据。
// result_ack 用 seq 更新实际写出的累计位置，不能用较新的 appliedSeq 代替。
type v2WriteReceipt struct { kind v2WriteKind; seq uint64; err error }

// collectV2Audio 独立于网络写入读取音频，交付事件时可被 ctx 取消。
// 阻塞源 Read 由拥有者 Close 解除；不关闭源，不操作状态。
func (c *V2Client) collectV2Audio(ctx context.Context, source io.Reader, events chan<- v2SourceEvent)

// readV2Events 读取完整文本帧，原始消息和最终错误按同一通道有序交付。
// 停止时不关闭连接、不更改状态；事件交付监听 ctx。
func readV2Events(ctx context.Context, conn *websocket.Conn, events chan<- v2ReadEvent)

// writeV2Jobs 串行编码并限时写入；每个实际取得的任务产生一个回执。
// 第一次写失败后退出，不继续处理后续任务，不修改协调状态。
func (c *V2Client) writeV2Jobs(ctx context.Context, conn *websocket.Conn, jobs <-chan v2WriteJob, receipts chan<- v2WriteReceipt)

// v2ServerMessage 是协调者内部解析结果；typ 指定唯一有效字段。
// 含 ready 凭据，不整体记录到日志。
type v2ServerMessage struct {
    typ wsprotocol.MessageType
    ready wsprotocol.ReadyMessage
    audioAck wsprotocol.AudioAckMessage
    result wsprotocol.SequencedResultMessage
    completed wsprotocol.CompletedMessage
    entryError wsprotocol.V2ErrorMessage
}

// decodeV2Message 按 type 分派并解到已有消息类型，出错返回零值和固定错误。
// 不另建逐字段校验层；消息阶段和业务合法性由握手/协调者判断。
func decodeV2Message(data []byte) (v2ServerMessage, error)
```

reader 和 source 正常结束前先投递终态事件；投递可因整体取消退出。reader 的正常消息、completed 和随后 Close/EOF 必须走同一有序通道，不能让独立错误通道越过已经读到的 completed。

writerBusy 在派发任务前置 true，直到协调者处理对应回执才清零。包括控制帧在内，总共最多一个待写/在途任务；只有 busy=false 才授权/构造下一份副本。回执通道容量 1 保证唯一任务结束后能报告实际结果；writer 实际取得任务后即使取消，也应报告结果，回执不通过 ctx 分支随意丢弃。整体退出时尚未取走的请求可直接废弃，不算实际写入。

三类任务的退出通知只用于等待，不直接裁定识别成功，也不因 source EOF 取消其他任务。

## 运行流程

### 1. 所有权与握手

非 nil source 从 Run 开始由它接管关闭责任。创建独立 state 和 runCtx，准备统一收尾；已取消的调用也关闭源并返回原因。配置在 NewV2 已验证。

HandshakeTimeout 创建一次绝对预算，覆盖 Dial、写 start(version=v2) 和读第一条应用消息，不能每个步骤重新计时。握手期间尚未启动三个任务，也不读取或发送源音频。设置 ReadLimit，首帧只允许 ready 或入口 error；合法 ready 经 state.acceptReady 后才进入双向运行。握手结束取消它的子 context，后续任务使用 runCtx。

Dial 失败但带 HTTP 响应时，可尝试解析 503 的 error JSON，得到 V2EntryError；不符合格式仍保留原拨号错误。入口 error 必须 Type=error 且 Code 非空，未知非空 Code 保留，Message 不参与控制判断。已核对本地 coder/websocket v1.8.15：失败响应体由库最多保留前 1024 字节，网络体已由库关闭；应用只解析这份有界响应，不再开展无界读取。截断或无 JSON 不代表入口未提交。

### 2. 音频采集

合法 ready 后才启动采集任务。每次使用独立缓冲调用 io.ReadFull，正常完整块交付 data；io.EOF 或 io.ErrUnexpectedEOF 表示正常输入结束，尾块非空时与 eof=true 同一个事件交付；其他读取错误作为失败事件，不发合法 end。任何音频块都须按 PCM 样本字节对齐。

Realtime=true 使用本任务创建时刻加累计音频时长作为固定计划：等待下一块时刻、读取、推进音频时长并交付。该时间轴只按新采集字节前进，和 Write 成功/失败无关；不要把 v1 Pacer 的“按写成功前进”逻辑搬到 writer。false 允许快速读取，超过确认释放速度时可能耗尽缓存，这是明确失败，不覆盖旧音频。

事件交付允许在容量 1 的通道上等待，等待监听 ctx。协调者即使 writer 忙也继续接纳 source 事件并 appendAudio；到缓存上限明确失败，不能把 source 通道禁用来等待网络确认。同步慢回调会暂缓交付，固定模拟时间轴仍不重置，恢复执行时按原计划追赶；这不等于真实麦克风永不丢采样，真实源需要自己的采集/过载合约，留后续接入验证。

协调者先检查 source 事件读取错误/样本对齐，再 append 非空数据；成功后若 eof 则 sealInput，并停止选择该 source 通道。源 EOF 不关闭 WebSocket。

### 3. 消息解析与结果应用

reader 只接收 WebSocket Text，记录接收时刻并交付原字节。协调者内部用简短的 decodeV2Message：先反序列化 type，再解到对应现有结构体。解析错误使用固定说明，避免底层 JSON/数值错误回显凭据或原文。沿用当前双方受控范围，不新增全字段 RawMessage 校验器。

ready 后仅接受 audio_ack、result、completed；重复 ready、入口 error 或未知类型作为阶段/协议错误结束本次运行。音频确认调用 acceptAudioAck。结果先 checkResult，重复序号跳过应用；新结果调用 OnResult，成功后 commitApplied。

每次 OnResult 派生 ApplyTimeout，父 context 为 runCtx，并受已有完成等待截止点约束。回调返回后再检查该应用 context 的错误；已经超时的调用即使返回 nil 也不推进 appliedSeq。错误或超时结束运行，禁止为同一结果偷偷新起 goroutine 或自动重试。nil 回调表示显式消费并丢弃，仍提交应用位置。

### 4. 唯一写任务的调度

维护 writerBusy、lastResultAckWritten、上次写任务是否为 result_ack 等少量运行字段。状态提供事实，运行字段管理 I/O。每轮有待处理事件时正常处理，writer 空闲时最多派发一个任务：

1. 已核实 completed：完成确认优先。
2. 本地已封口、所有音频已派发、之前任务实际返回且 end 尚需发送：调用 authorizeEnd 并派发 end。
3. 有累计结果需确认，且上次任务不是 result_ack 或当前没有音频待派发：发送 appliedSeq 的快照。
4. 有音频待派发：调用 authorizeAudio，立刻把唯一副本交给 writer。

这一顺序让结果确认与音频在两者都有待处理工作时交替，不让持续结果更新无限推迟音频。end 优先于待合并 result_ack，completed_ack 自身会在服务端累计确认最后结果，不需要先补完所有 result_ack。

result_ack 合并成当前位置，不逐条排队。只在 state.appliedSeq>lastResultAckWritten 时需要发送；实际成功后按回执 seq 更新 lastResultAckWritten。写失败不能推进该值，也不能回滚已经应用的结果。

音频帧是 8 字节大端 offset + 原块负载。writer 内组帧，控制消息使用已有 V2EndMessage、ResultAckMessage、CompletedAckMessage。每次 conn.Write 使用 WriteTimeout 子 context；写结束后释放音频/组帧引用，回执只含 kind/seq/err。

### 5. 完成等待与最终确认

首次 authorizeEnd 成功时固定 completionDeadline=now+CompletionTimeout，覆盖 end 写入及剩余结果/应用等待。结果更新不续期。协调者按绝对时间复核截止点，不能因 select 恰好选中消息而无限推迟超时；回调 context 同样受此截止点约束。

acceptCompleted 成功后立即记录业务完成，停止完成等待 Timer，进入有固定 WriteTimeout 总窗口的最终确认阶段。窗口从接纳 completed 时计算，包含等待当前 writer 任务实际返回及写 completed_ack 的时间，不能在每次事件到来时重置。

最终确认阶段不再处理新业务输入。已经排队的读取 Close/EOF 不能抢先取消尚在完成的确认 Write；等待当前回执及 completed_ack 回执，或最终确认截止/调用者取消。确认写失败、超时或连接已无法写入时保留 Completed=true，并返回相应错误；CompletionAckWritten 仅依据实际写成功回执。

Completed=false 时遇到对端正常关闭，返回 ErrV2Incomplete；其他传输错误保留原因。本步直接结束本次运行，恢复留到下一组工作。

### 6. 共同收尾

所有退出路径集中执行：停止本地 Timer、取消 runCtx、恰好一次 source.Close、conn.CloseNow、等待已经启动的三个任务全部退出。不能只调用 cancel 就返回。CloseNow 的预期 net.ErrClosed 可忽略，其他清理错误和源 Close 错误保留并与主错误合并。尚未建立 conn 或尚未启动任务的握手失败也走同一所有权规则。

等待结束后若还留有唯一写回执，补记 completed_ack 是否实际写成功，避免取消与回执同时就绪时把已成功写入记成未写；不因该回执抹掉已经裁定的主错误。state.detach 只改变业务附着标志，不能代替关闭/等待。

Run 最后从状态构造 V2RunResult。nil error 只在已核实完成、完成确认写成功且清理成功时返回。Completed=true 与非 nil error 可以同时存在：调用方保留完整识别结果，并展示确认/清理异常；不要因非 nil error 自动重做原问诊。

## 资源边界与后续空间

MaxReplayBytes 只限制缓存拥有的未确认音频；还存在容量 1 的源事件、正在读取的源块、协调者处理中的源块、一个写副本及组帧、reader 消息和 JSON 解码结果。各层都要避免无界排队，不能把缓存字节预算宣称为全客户端内存上限。整个 Run 始终只有上述三个自建 I/O 任务，底层 WebSocket/HTTP 的内部任务另计。

本步暂不增加 OnWrite/指标框架或新的 CLI，测试可直接调用 NewV2.Run。后续故障实验再按实际需要补采集/授权/确认/重放量和时间观测，避免把 v1 的成功写出字节与恢复确认混用。

## 预先固定的验收

开发者完成这组运行器实现，助手负责测试。验收分为真实可控 WebSocket 场景和真实 Gateway + TCP gRPC 可控 Worker 组合：

- 正常双向场景：源尚未 EOF 时已经收到并应用结果；Worker 最终收到的字节与输入逐字相等、顺序一致；尾部结果应用后 Completed=true，FinalOffset 等于输入长度，最后结果序号连续。
- 空输入和不足整块但样本对齐的尾块完成；奇数尾字节/非 EOF 源错误不发送合法 end，不误报成功。
- ready 前不读源/发音频，握手共享期限；HTTP/WS 入口拒绝保留 code；非法首帧、二进制帧、超限消息、重复 ready、错误代次或确认超前均拒绝。
- 慢 writer 时协调者仍接收 ACK/取消和源事件；未确认缓存耗尽明确失败，绝不覆盖。可控源和写阻塞需同步屏障，不能依赖随机睡眠猜测时序。
- 结果重复序号只应用一次，同片段不同序号照常应用；回调报错或合作式超时不提交/确认该结果，并进入实际清理。
- end 后静默触发固定完成期限，持续结果不续期；只有音频 ACK 或正常 socket close 均不能产生成功。
- completed 紧随连接关闭时按 reader 顺序核实成功；completed_ack 写入失败/最终窗口超时后仍保留 Completed=true；写成功只证明本地写完成。
- 取消时源正在阻塞 Read，Close 使其退出且恰好执行一次；source/reader/writer 的完成信号在 Run 返回前全部到达。对合作式回调同样验证取消退出，不声称能强制终止违约回调。
- 真实 Gateway 正常完成确认后，最终注册/活动计数归零。确认未送达场景按已有恢复/保留期限清理，不把本地 Run 返回当作远端已清理。
- 回归既有 v1 客户端和已验收缓存/状态。记录实际场景、通过/失败及修正原因；本轮尚无上述验收结果或自动恢复性能数据。
