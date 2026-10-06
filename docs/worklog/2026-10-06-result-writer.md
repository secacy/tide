# 第六阶段：固定连接的结果写任务

日期：2026-10-06。状态：实现指导，等待开发者编码。依据 `ba81c7e` 的结果投递与累计确认验收。本轮核心由开发者实现，助手负责受控 I/O、真实 WebSocket 组合测试和文档维护。

## 当前目标与后续顺序

本轮形成实际 runWithWorker → 有界结果保存 → 结果写任务 → WebSocket 客户端的可验证通路。一次写任务固定使用一个连接及代次，从协调者取授权，写成功后报告，再取下一条；没有结果时等待通知。测试中的 ACK 仍通过内部命令提交，网络 ACK 读取入口留给连接接入。

随后完成候选连接的原子安装、连接读写任务的共同监督、接纳确认及终态消息的发送顺序，再接 v2 握手/准入/注册表运行器与客户端有限重放，进行完整网络恢复验收。本步不启用 v2 HTTP 入口，不将已有无候选连接的 requestResume 当作真实连接交接。

## 问题、备选方案与选择

结果缓冲已经解决“保存什么、可以确认到哪里、重连从哪里取”。慢 WebSocket Write 仍可能阻塞；如果让协调者直接 Write，音频、ACK、恢复和期限都无法及时处理。如果多个任务各自取结果并写，发送顺序、在途授权与清理责任会变复杂。

| 方案 | 优点 | 代价及决定 |
| --- | --- | --- |
| 协调者直接 Write | 调用路径短 | 网络阻塞会占住唯一控制循环，不采用 |
| 每条结果启动独立写任务 | 单次调用容易拆开 | 需额外管理并发、顺序和在途任务；与单授权规则冲突，不采用 |
| 在协调者与写任务之间另加结果队列 | 能预取 | 与现有结果缓冲重复持有数据，增加预算及断线回退状态；本轮没有需求，不采用 |
| 每代一个固定连接写任务，逐条取得授权并同步 Write | 顺序直接、有界、阻塞与控制分离 | 需明确停止原因及调用方的清理职责，采用 |

写任务不读取可变的“当前连接指针”，不直接读取 resultBuffer，也不自行重连。底层库是否允许并发 Write 不决定应用消息顺序；应用层仍由同一写任务串行发送。当前依赖固定为 coder/websocket v1.8.15，已检查本地模块源码的 Write/context 行为。

## 本轮文件与类型

新增 `internal/wsprotocol/result_v2.go`，定义带序号的结果消息。v2 协商后才使用；原 v1 ResultMessage 继续供既有入口使用。

```go
// SequencedResultMessage 是可恢复协议的一条识别更新。
// Seq 标识更新，不是 segmentId；IsFinal 只表示该片段定稿。
type SequencedResultMessage struct {
    Type MessageType `json:"type"`       // 固定为 MessageTypeResult。
    Seq uint64 `json:"seq,string"`        // 正序号，十进制 JSON 字符串，保留完整 uint64 精度。
    SegmentID string `json:"segmentId"`   // Worker 的片段标识。
    Text string `json:"text"`             // 此次更新文本。
    IsFinal bool `json:"isFinal"`         // 不表示整场完成。
}
```

示例：`{"type":"result","seq":"3","segmentId":"s","text":"你好","isFinal":true}`。JSON number 对 Go 客户端方便，但浏览器常用 number 无法精确表示全部 uint64；选用字符串为后续音频位置/代次的线格式提供一致方向。不要把序号先转成 float64。

新增 `internal/gateway/result_writer.go`：

```go
// resultWriteConn 是写任务所需的最小 I/O 能力；*websocket.Conn 直接满足。
// Write 必须响应 ctx；连接关闭及读任务由连接拥有者负责。
type resultWriteConn interface {
    Write(ctx context.Context, typ websocket.MessageType, data []byte) error
}

// resultWriterConfig 在构造后保持不变。
type resultWriterConfig struct {
    session *resumableSession       // 已运行的逻辑会话，通过命令访问其状态。
    generation uint64              // 固定非零代次；不能在重连时原地修改。
    conn resultWriteConn            // 本代连接；不在每轮读取共享的“当前连接”。
    writeTimeout time.Duration     // 正值；仅约束实际单次 Write。
    controlCtx context.Context     // 逻辑会话控制生命周期，不使用连接取消的 ctx。
}

// resultWriter 只运行一次；自身不启动 goroutine、不拥有连接关闭权。
// 调用方保证同一连接最多启动一个结果写任务。
type resultWriter struct {
    config resultWriterConfig
}

// resultWriterExitKind 说明任务为何返回，供连接拥有者做后续处理。
type resultWriterExitKind uint8

const (
    writerResultsComplete resultWriterExitKind = iota // Worker 正常完成，当前需要投递的结果已写完或被确认跳过。
    writerStopped                                    // 生命周期取消、控制结束或本代附着资格已失效。
    writerWriteFailed                                // 实际 WebSocket Write 出错或达到单次期限。
    writerControlFailed                              // 命令/编码等内部错误，不能一律解释为断网。
)

// resultWriterExit 在 run 返回时交给连接拥有者，不等同于整场会话终态。
type resultWriterExit struct {
    kind resultWriterExitKind
    lastSeq uint64 // 仅 writerResultsComplete 有效；允许正常计算没有任何结果时为 0。
    err error      // 完成时 nil；其他退出必须带原因，保留 errors.Is 身份。
}

// newResultWriter 校验 session、conn、controlCtx 非 nil，generation 非零及期限为正。
// 非法参数返回 nil 和 errInvalidResultWriterConfig；不启动任务或操作/关闭连接。
func newResultWriter(config resultWriterConfig) (*resultWriter, error)

// run 在调用方提供的 goroutine 中执行固定代次的结果发送循环。
// ctx 属于本代连接，必须继承 config.controlCtx 的逻辑会话生命周期；nil 为编程错误。
// 连接拥有者还须在 controlDone 关闭时取消本代 ctx，并负责关闭连接和等待读写任务。
// 正常结果发送结束仍保留连接，后续终态投递/确认由连接输出流程继续完成。
func (w *resultWriter) run(ctx context.Context) resultWriterExit

// writeMessage 同步写入已经编码的一条文本消息。
// 每次建立独立期限，返回前读取该期限的状态，再取消计时资源。
// 父 ctx 取消优先，其次本次超时 ErrResultWriteTimeout，最后保留 Write 原始错误。
// 不启动另一个 goroutine 竞速超时；须等实际 Write 返回才结束调用。
func (w *resultWriter) writeMessage(ctx context.Context, data []byte) error
```

新增带注释的哨兵错误 `errInvalidResultWriterConfig`，以及 `errResultWriterSuperseded`：后者表示写成功报告返回 handled=false，本代授权已因断开或换代失效。已有 ErrResultWriteTimeout 复用。错误文案不作为调用方分类依据。

## run 的处理顺序

先固定理解两个 context：连接 ctx 控制查询、等待和实际 Write；controlCtx 用于 Write 成功后的状态报告。连接刚被取消时，仍应能向存活的协调者提交已经发生的事实。controlCtx 由逻辑会话拥有者管理，不能改成永不过期的 Background 逃避清理。

1. 每轮及取得 offer 后检查退出：controlCtx 的取消原因优先，其次连接 ctx 的取消原因，最后已关闭的 controlDone（用 errResumeClosed）。命中则返回 writerStopped。已收到授权后发生取消也必须归还明确退出结果，不能丢弃任务结果；连接拥有者随后负责撤销本代资格或结束会话。
2. 调用 `session.requestResult(ctx, generation)`，不直接读 buffer。命令返回 errResumeClosed/errSessionNotAttached/errSessionGenerationMismatch 时返回 writerStopped；其余错误返回 writerControlFailed。若当前已有取消，优先返回相应 writerStopped。errResultWriteInFlight 表示内部错误地启动了竞争写任务，不能忙重试或假装断网。
3. available=true 时，用 Seq、SegmentID、Text、IsFinal 构造 SequencedResultMessage 并 json.Marshal。编码失败归 writerControlFailed；编码发生在单次 Write 期限之外。
4. 调用 writeMessage。失败时，若连接或控制生命周期已取消，返回 writerStopped；否则返回 writerWriteFailed，保留超时或底层错误。不要调用 reportResultWritten，也不要继续取下一条。
5. writeMessage 返回成功后，用 `controlCtx` 调用 `reportResultWritten(controlCtx, generation, seq)`。即使随后连接刚被取消，也先尝试报告已判定成功的 Write；若取消在 writeMessage 内已经被观察，则按第 4 项退出。handled=false,nil 返回 writerStopped + errResultWriterSuperseded；命令错误按第 2 项分类。handled=true,nil 才继续下一轮。写成功不会发送 ACK 或释放结果，客户端应用确认仍走独立入口。
6. available=false 且 workerCompleted=true 时返回 writerResultsComplete，lastSeq=offer.lastSeq。该位置可以为 0，也可能包含客户端已确认而本代无需重发的前缀。
7. available=false 且 workerCompleted=false 时，在 offer.changed、连接 ctx.Done、controlCtx.Done、session.controlDone 之间等待；唤醒后回到循环重新检查资格/退出并取快照。不能连续查询，也不能反复使用旧的已关闭通知。

writeMessage 在调用底层前检查父 ctx；建立 WithTimeout 后同步 Write，取得 writeCtx.Err，再 cancel。返回分类采用：父 ctx 已取消 → context.Cause(ctx)；writeCtx 已 DeadlineExceeded → ErrResultWriteTimeout；底层错误 → 包装并保留 errors.Is；否则 nil。检查要在 cancel 前保存期限状态，避免自行取消改变原因。不要在循环中积累 defer cancel。

本轮不增加 err/done 常驻对象或事件 channel：run 的返回值已经是任务终止结果。连接拥有者以后以一个容量为 1 的出口发布结果，保证协调者停止收事件也不阻塞写任务退出；只有 run 真正返回才表示其持有的借出结果/编码字节可被释放。

## 所有权、错误与完成边界

writer 不调用 CloseSend、不取消 Worker RPC、不自行调用 requestClose 或 reportDetach。退出结果由连接拥有者统一处理：否则旧 writer 迟到的错误可能结束新代会话。下一步会将读写退出事件都绑定 generation，并在协调者里检查后决定断开或终止。

writerWriteFailed 只标明错误来自输出 I/O，不等于必然可以恢复。现有写超时属于保护退出，协议错误/停服/Worker 错误也不能统一转成可恢复断线。网络错误分类与读写竞争在连接监督接入时集中完成。当前测试夹具必须在失败后显式报告有效 detach 或终止并等待清理，不能留下 inFlight 授权。

ctx 取消是否立即使底层 Write 返回取决于连接实现。本轮实际使用 coder/websocket 的可取消 Write；测试还应检查“已经发出取消但底层尚未返回”时 run 不会谎报退出。当前 runWithWorker 尚不拥有这个新任务，不能将其返回当成连接写任务已被等待的证据。

controlDone 关闭时，空闲 writer 能自行醒来；阻塞中的 Write 需要连接拥有者取消 ctx/关闭连接才能立即解除，单次期限提供有限等待。本轮夹具显式承担这一责任；下一步真实连接监督必须接线，不能遗漏。

writerResultsComplete 仅说明需要发送的识别结果已处理完毕，不证明客户端应用了全部结果，也不证明客户端收到了整场完成通知。不得据此 Close 正常连接或直接 requestClose。后续同一串行输出任务发送带 lastSeq 的整场完成消息，再接客户端终态确认。真实入口启用前还须把 ready、音频接纳确认与终态消息纳入单一发送顺序；不能让读任务直接并发 Write。

本轮测试开始写结果前由夹具确保附着与握手就绪；该前置条件不能替代未来“候选连接安装 + 代次切换”原子提交。新连接创建新的 writer，旧 writer 只持有旧连接。收到 writerStopped 后也不能立即归还逻辑会话名额，最终资源清理由会话运行器负责。

## 助手验收计划

- 构造失败无网络动作；固定连接与代次，不调用另一连接。
- 实际 receiver 产生结果，经 runWithWorker、writer 和真实 WebSocket 到客户端，序号/文本/片段更新顺序正确；最大 uint64 序号通过字符串精确传输。
- 第一条 Write 阻塞时不写第二条，协调者仍可处理 ACK/控制命令。客户端快速 ACK 可以先于写成功报告，后续游标不倒退。
- 无结果时阻塞等待，追加后唤醒；空结果正常完成、尾部完整投递均返回确定 lastSeq；写完但未确认的结果仍能重放。
- 超时、父取消、底层 Write 错误分类及错误身份；没有成功报告的写不能清除授权或继续写下一条。
- Write 成功后连接取消，仍使用独立 controlCtx 报告；旧代报告被忽略时正常停止，不影响新代授权；非法双 writer 按控制错误退出。
- 等待结果时逻辑/连接取消或协调结束能退出；底层不返回时不提前宣称退出。夹具接收退出结果后撤销资格/终止，并等待任务和 RPC 清理。
- 保持原 v1 线格式与相关恢复部件回归。本轮尚无网络 ACK 读取、候选连接安装或自动重连，不产生恢复率/耗时结论。

本次仅记录实现方案，没有修改运行代码或新增测试结果。
