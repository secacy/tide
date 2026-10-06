# 第六阶段：固定连接输入读取与网络结果确认

日期：2026-10-07。状态：实现指导，尚未实现或验收。源码依据 `2dc690a`。核心代码由开发者完成，助手负责评审、测试和记录。

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
