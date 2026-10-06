# 第六阶段：固定连接输入读取与网络结果确认

日期：2026-10-07。状态：固定连接 reader 及真实网络输入/结果 ACK 组合已验收；候选连接安装与共同清理尚未接入。设计源码依据 `2dc690a`；以下保留实现前方案，实际验收见文末。核心代码由开发者完成，助手负责评审、测试和记录。

## 能力目标与顺序调整

上一小步已让 Worker 结果经过真实 WebSocket 发给客户端，但网络输入仍缺少 v2 音频位置、end 终点和结果 ACK 的读取任务。下一组能力仍是候选连接交接与读写共同清理；先补真实 reader，避免用占位读回调验收连接管理。

本次实现固定连接、固定代次的输入任务，并组合已有 Worker 协调者和结果 writer 验证双向数据。接下来依次完成：候选连接原子安装及读写共同清理；统一 ready/音频接纳确认/终态输出及握手、双层准入；客户端有限重放与真实断连实验。这些是能力组，不代表每组只需一次提交。

本次不把 reader 接进现有 v1 handler。网络 ACK 可释放已确认结果，但没有音频接纳确认、终态协议和真实候选安装时，仍不能标记可恢复客户端链路完成。

## 问题和方案取舍

end 表示没有新的音频，不表示连接读取结束。尾部结果仍在返回，客户端还要发结果 ACK；同一条连接的 reader 必须继续工作。连接代次由服务端安装时绑定，不能从客户端消息取值。

| 选择 | 优点 | 代价与决定 |
| --- | --- | --- |
| 音频和控制全部 JSON，音频 Base64 | 编码一致、便于查看 | Base64 增加约三分之一负载及编解码成本；暂不采用 |
| 音频二进制，控制 JSON | 保留音频直接传输，控制易读 | 两种解析路径；采用，音频加固定长度位置头 |
| 所有消息自定义二进制或 Protobuf | 统一强类型编码，便于更多字段扩展 | 客户端与协议工具改动更多；当前三种输入尚不需要 |

读取后直接同步提交已有会话命令，不另加 reader 队列：一条消息处理完才读下一条，避免第二套缓冲预算。代价是协调者忙时暂停读取；协调者本就不能阻塞在网络 I/O 中。音频接纳位置、重放去重、end 和 ACK 合法性继续由协调者唯一判定，reader 不复制业务状态。

reader 不写音频确认，否则它和 resultWriter 会各自安排输出顺序。后续确认消息纳入单一连接输出流程；本次不增加无人消费的确认 channel。

## v2 已附着阶段的线格式

本协议只用于握手并安装连接之后。start/resume 属于后续握手入口；reader 遇到它们按不支持的输入拒绝。

| 输入 | 编码 | 含义 |
| --- | --- | --- |
| audio | 一个 WebSocket 二进制消息：8 字节大端 uint64 offset，随后为非空原始音频 | offset 为音频负载的字节起点；8 字节头不计入音频位置/预算 |
| end | `{"type":"end","finalOffset":"32000"}` | 最终连续音频位置，允许 `"0"` |
| result_ack | `{"type":"result_ack","seq":"12"}` | 客户端已经连续应用的结果序号，允许 `"0"` |

JSON 中 uint64 必须使用十进制字符串，与结果输出的 seq 一致，避免 JavaScript 数字精度丢失。要求一个或多个 ASCII 数字且 ParseUint 不溢出；允许前导零，不接受空串、空白、正负号、小数、指数、裸 JSON 数字、null 或缺失。不能用 uint64 默认零值区分缺失和合法零。

文本必须是一个 JSON 对象，不接受数组/null/多个对象或尾部非空白数据。按 type 解码必需字段；未知字段采用标准 encoding/json 的忽略行为，同名字段沿用其后值覆盖行为，不新增自定义 JSON 解析器。必需字段和 type 仍须满足上述约束。未知 type 拒绝；v1 EndMessage 不直接复用，因为它没有终点。

二进制消息长度至少 9；解码头使用 encoding/binary.BigEndian.Uint64。位置加长度的溢出、缺口、部分重叠等由已有 requestAudio 路径校验。协议 helper 不复制 payload，返回输入切片的只读视图；调用者必须等 requestAudio 返回后才能复用它。

建议在 `internal/wsprotocol/input_v2.go` 提供下列类型和函数，并定义可 errors.Is 判断的 ErrInvalidV2Input。包只处理字节，不依赖 websocket。

```go
// V2InputKind 区分已附着阶段的输入；零值无效。
type V2InputKind uint8
const (
    V2InputInvalid V2InputKind = iota
    V2InputAudio
    V2InputEnd
    V2InputResultAck
)

// V2Input 是一条已完成线格式校验的消息，不代表业务已接纳。
type V2Input struct {
    Kind V2InputKind // 指定下面哪些字段有效。
    Offset uint64   // audio 的起点或 end 的最终位置。
    Payload []byte  // 仅 audio；借用原数据，不得修改。
    Seq uint64      // 仅 result_ack；累计已应用位置。
}

// DecodeV2Audio 解码一个完整二进制消息；失败返回零值和包装后的错误。
func DecodeV2Audio(data []byte) (V2Input, error)

// DecodeV2Control 解码一个完整文本消息；只接受 end/result_ack。
func DecodeV2Control(data []byte) (V2Input, error)
```

## reader 结构和职责

建议新增 `internal/gateway/connection_reader.go`。

```go
// connectionReadConn 是固定连接的读取能力；*websocket.Conn 直接满足。
// Read 必须响应 ctx 取消；同一连接只允许一个应用读取任务。
type connectionReadConn interface {
    Read(context.Context) (websocket.MessageType, []byte, error)
    SetReadLimit(int64)
}

// connectionReaderConfig 构造后不可变，不转移连接关闭责任。
type connectionReaderConfig struct {
    session *resumableSession // 已运行的逻辑会话，所有状态通过命令访问。
    generation uint64        // 服务端分配的固定代次，非零。
    conn connectionReadConn  // 本次连接的固定句柄。
    controlCtx context.Context // 逻辑生命周期，用于识别停止原因。
    maxMessageBytes int64    // 完整消息上限，二进制包括 8 字节头；至少 9。
}

// connectionReader 由拥有者启动一次，不自行创建 goroutine。
type connectionReader struct { config connectionReaderConfig }

type connectionReaderExitKind uint8
const (
    readerStopped connectionReaderExitKind = iota // 生命周期/控制结束/代次失效。
    readerReadFailed       // 传输读取失败；包括对端正常关闭，不等于问诊完成。
    readerProtocolFailed   // 消息过大、不支持的类型或非法线格式。
    readerCommandFailed    // 协调者拒绝业务命令或内部控制错误；保留原错误。
)

// connectionReaderExit 是任务退出事实，所有退出均带非 nil 原因。
// 由后续连接拥有者决定 detach 或终止；reader 不做生命周期决策。
type connectionReaderExit struct {
    kind connectionReaderExitKind
    err error
}

// newConnectionReader 校验非 nil 依赖、非零代次和消息上限。
// 失败返回 nil、errInvalidConnectionReaderConfig；不操作连接或启动任务。
func newConnectionReader(config connectionReaderConfig) (*connectionReader, error)

// run 使用本代连接 ctx；ctx 必须非 nil，且继承 controlCtx。
// 所有者负责在 controlDone 时取消 ctx，主动关闭连接并等待本任务返回。
// end 成功仍继续读取，直到失败或停止；不会写消息、关闭连接或取消 RPC。
func (r *connectionReader) run(ctx context.Context) connectionReaderExit
```

maxMessageBytes 是单条完整消息的保护，不等于整个进程内存上界。run 在首次 Read 前 SetReadLimit；握手入口以后还须设置自己的读取上限，不能等握手结束才限制首条消息。当前固定依赖 coder/websocket v1.8.15 在本地读取超限时返回包装了 websocket.ErrMessageTooBig 的错误，用 errors.Is 归为 protocolFailed；不能只依赖 CloseStatus，因为发送给对端的关闭码不等于本地返回错误的类型。其他 Read 错误保留原始身份，不做字符串匹配。

## run 的执行顺序

1. 入口验证 ctx；先检查停止，再设置读取上限。
2. 每轮按 controlCtx、连接 ctx、controlDone 的顺序判定停止原因。停止后不开始下一次 Read。
3. 同步 Read(ctx)。返回后再次检查停止原因，避免自己取消连接产生的 I/O 错误被当作断网。Read 成功也需要这次检查，不能继续提交已经停用的输入。
4. 没有停止时，Read 错误按上述分类返回；EOF/正常关闭都没有 readerCompleted 语义。
5. MessageBinary 调用 DecodeV2Audio，MessageText 调用 DecodeV2Control，其他应用消息类型拒绝。解析错误前再次尊重已观察到的生命周期停止原因。
6. 按类型同步调用 requestAudio(ctx, generation, offset, payload)、requestEnd(ctx, generation, offset) 或 requestResultAck(ctx, generation, seq)。禁止直接读取 resume/input/delivery 可变字段；不调用 Worker Send/CloseSend。
7. 已有命令交付规则不变：交付后等唯一回复。成功的状态提交不会因随后取消而回滚；回复后先检查停止，再进入下一次 Read。成功返回中的 accepted=false/advanced=false 都是合法幂等操作，不是失败。
8. 命令错误先检查停止原因；errSessionGenerationMismatch、errSessionNotAttached、errResumeClosed 归 readerStopped，其余归 readerCommandFailed。用 errors.Is，包装错误保留身份。缺口、预算超限及 ACK 超前等由后续连接协调策略终止，不能一律当作可恢复断网。

stopCause/helper 可参照 resultWriter 的实现和错误优先级。不要让 reader 自己调用 requestDetach/requestClose，也不要为了监听 controlDone 再启动一条无人等待的 goroutine。

收到合法 end 后继续读，允许合法重复 end、历史音频重发和结果 ACK；能否接纳仍以协调者判断为准。恢复后的 reader 不需要自己猜测音频是否已结束。

本任务暂不添加每次 Read 的空闲计时。现有 v1 空闲保护继续有效，但 v2 尚未提供等价保护，不能接为生产入口。后续连接管理必须区分音频输入空闲与 end 后等待 ACK，并明确静默断网的检测期限；不能因为任意 ACK 到达就无限延长音频输入期限。底层 Read 响应取消不等于能自动检测所有弱网。

## 验收依据与后续所有权

开发者完成后由助手补测试，预先明确以下行为：

- 编码覆盖合法零/最大 uint64、必需字段缺失、非法字符串、短二进制、空音频、多 JSON 值及未知消息；验证大端 offset 与 payload 不混计。
- 音频经真实 WebSocket reader → 协调者 → 原 Worker；结果经现有 writer 回到客户端。历史音频重发只送 Worker 一次，合法 end 后尾部结果仍可收到。
- 客户端在同一真实连接上发 result_ack，释放结果预算；ACK 超前拒绝，重复 ACK 幂等；end 后 reader 不提前返回。
- 旧代音频/end/ACK 不能修改新代状态。并发停止优先级符合约定，原始命令错误可 errors.Is，收到异常消息后不继续消费后续业务输入。
- 完整消息上限覆盖头部；真实取消解除 Read 阻塞。测试拥有者显式关闭连接、等待 reader/writer 和 Worker，不能只等控制循环退出。

这一轮真实网络测试是固定连接双向组合证据，不是候选连接原子恢复、音频 ACK 或端到端重连验收。不会把测试数量当作恢复耗时/恢复率。

下一组把真实 reader/writer 纳入连接附着对象。尤其注意 coder/websocket v1.8.15 的 CloseNow 仍会等待内部 goroutine，不能在会话协调循环同步调用；关闭和等待必须由明确拥有者完成。writerResultsComplete 不应取消仍在读 ACK 的 reader；整个连接清理完成和 controlDone 必须分开。

## 实现与验收（2026-10-07）

开发者完成 [输入解析](../../internal/wsprotocol/input_v2.go)、[控制消息定义](../../internal/wsprotocol/protocol.go)和 [connectionReader](../../internal/gateway/connection_reader.go)。评审未发现需要修改的核心逻辑；助手补充注释、gofmt 和测试，未代为改动业务或并发控制。

实际 reader 固定连接与 generation，同步 Read/解码/提交命令，接纳完成后才读取下一条；合法 end、重复 end、历史重发及旧/重复 ACK 均不会提前结束读取。消息上限在首次 Read 前设置，超限用 errors.Is(websocket.ErrMessageTooBig) 分类。逻辑取消、连接取消及控制结束按既定顺序判定，传输关闭不视为问诊完成。reader 本身没有关闭连接、取消 RPC 或修改附着状态的权力。

### 验证范围与结果

新增 [协议测试](../../internal/wsprotocol/input_v2_test.go)、[reader 受控测试](../../internal/gateway/connection_reader_test.go)和 [真实网络组合测试](../../internal/gateway/connection_reader_websocket_test.go)。新增共 **15 个顶层、122 个叶级场景全部通过**，其中 **8 个真实 WebSocket 场景全部通过**。

- 解析覆盖 uint64 零/最大值、超过 JavaScript 精确数字范围的位置、大端头、非空借用负载、必需字段、十进制字符串、溢出、多个 JSON 值、扩展字段与后值覆盖；保留 v1 end 输出格式。
- 受控测试覆盖构造无副作用、Read 前/成功后/失败后停止原因优先级、消息/协议错误、三类旧代命令隔离、ACK 超前、音频范围及容量错误。复用已结束借用期的消息切片后，Worker 仍收到会话接管的原始音频副本；重复输入只上传一次。
- 真实组合运行现有 uploader、receiver、协调者、writer 和新 reader。客户端发送两次相同 `ab` 及 end 后历史重发，Worker **实际 Send 1 次、负载 2 字节**，未混入 8 字节头。结果缓冲设置为 **1 槽、6 字节**，三次结果连续使用同一预算，网络 ACK 均释放条目及字节占用。
- 客户端通过真实连接收到两个中途更新及一个尾部结果；end 后仍可 ACK，writer 正常返回后 reader 仍可接纳尾部 ACK 和重复 ACK。另验证完整消息恰好 9 字节可接受、头部计入超限、控制消息超限、连接/逻辑取消解除真实 Read、对端正常关闭分类和网络 ACK 超前拒绝。
- 测试拥有者显式取消/关闭连接并等待 reader/writer；逻辑会话关闭后等待 runWithWorker 返回，确认 Worker 任务已退出、输入/结果缓冲引用已释放。这里仍是测试夹具的所有权，不是生产连接管理已经完成。

最终相关回归命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race ./internal/gateway ./internal/wsprotocol \
  -run '^Test(V2Input|ConnectionReader|ResultWriterTask|ResultWriterWebSocket|SequencedResultMessage|ResultWrite|SessionProgressIntegration|SessionAudioBacklogFailure|ResumeState|SessionIdentity|SessionRegistry|SessionControl|AudioInput|WorkerUploader|SessionWorker|ResultBuffer|WorkerReceiver|SessionResults|DecodeWorkerResponse|SendWithTimeout|ResumableSession)' \
  -count=1 -timeout=45s -json
```

共 **198 个顶层、591 个叶级场景，其中 590 通过、0 失败、1 显式实验跳过**；含父测试共 693 项 pass，无数据竞争报告。跳过的是仍需显式开启的 TestResultWriteTimeoutExperiment。本机原始输出 `/private/tmp/tide-connection-reader-accepted.jsonl`；该临时文件未纳入 Git，以上命令可重跑对应检查。没有重新开展性能实验，测试数量和包耗时不作为恢复率、延迟或容量收益。

### 首次验证与退出语义说明

第一轮非网络测试全部通过。首次完整网络运行在 audio_header_counts 用例中，助手误用包含 synctest.Wait 的 assertControlAlive，导致真实时钟测试 panic（goroutine is not in a bubble）。已改为同步提交零 ACK，证明协调者仍可接受命令；这是测试夹具问题，没有修改核心实现。修正后新测试和最终相关回归全部通过；首次失败保留于 `/private/tmp/tide-connection-reader-first.jsonl`。

音频缺口、溢出、容量或 end 不一致等会让协调者按已有策略终止。它可能先回复命令再关闭 controlDone，reader 恢复执行时已能观察到关闭。此时遵循现有停止优先级，reader 可返回 readerStopped/errResumeClosed，而不是 readerCommandFailed/原始错误；如果控制尚未退出，则保留命令错误。**整场故障原因由 runWithWorker 的返回值保留**，本次逐项验证该身份，没有只检查 reader 是否结束。后续连接拥有者必须以会话终态作为最终故障依据，不能用 reader 的“控制已结束”覆盖原始故障，也不能在这里重新 detach 一个已终止会话。

当前仍无候选连接原子安装、实际握手、音频接纳确认、统一终态输出、v2 空闲/静默断网检测、双层准入或客户端有限重放。本步证明固定连接上双向数据及网络结果确认能组合工作；下一步将这些真实任务纳入连接附着对象的启动、停止和清理，并接入同一协调者的候选安装及旧代事件隔离。
