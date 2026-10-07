# 第六阶段：先发送 ready，再投递结果

日期：2026-10-07。状态：设计与实现指导，等待开发者实现；没有新增运行或测试成果。

## 能力目标与当前位置

上一步已经验收已接管连接的读写监督、原 Worker 保留、候选原子安装和共同清理。但安装后 writer 可以立即发送 result，客户端尚不知道这条连接对应的会话身份和音频接纳位置。内部测试掌握这些状态，公开恢复客户端不能依赖测试夹具。

本步完成一个组合闭环：每条已接管连接以 ready 作为第一条服务端应用消息，写成功后才开始结果授权和投递。ready 使用协调者提供的一致值快照；发送失败遵循已有连接失败和清理规则。

后续两组：先在同一写任务中增加持续音频接纳确认、正常终态投递及确认，再接公开 start/resume 握手、入口生命周期/准入与客户端有限重放。音频确认和终态不能另开并发写任务。结果循环将随持续控制输出接入而演进；本步保留现有组件接口，不新增完整通用输出队列。

## 问题与方案比较

例：客户端发出 [0,6400) 字节后断线，网关只接纳 [0,3200)。新连接如果只有识别文本，客户端无法确定哪些音频还需要重发。ready.nextOffset=3200 给出服务端确定接纳的位置。该位置不同于 Worker 已处理量，也不同于客户端上次 Write 成功位置。

| 方案 | 好处 | 代价与本步选择 |
| --- | --- | --- |
| 握手处理函数先写 ready，再启动 writer | 可以安排顺序，适用于握手完全拥有首次写入的设计 | 现有候选提交已转移连接所有权并启动任务，需要额外启动屏障和失败交接；不采用 |
| 多个消息生产者加共享写锁 | 能避免同时写 socket | 锁不保证 ready 抢在 result 前面；还要解决顺序、等待、取消和所有权；不采用 |
| 有界通用发送队列、单一消费者 | 消息统一串行，适合消息种类很多的实现 | 要增加排队容量、优先级、唤醒和清理规则；当前 ready 每代只有一次，无需单独队列 |
| 现有唯一写任务先写 ready，再进入结果循环 | 顺序由同步调用保证，沿用写期限与退出汇报 | 后续持续 ACK 需接入同一输出循环；本步采用 |

状态读取也有两种选择：安装时生成不可变快照，或写任务启动后向协调者查询。前者可行，但需要让首次启动和恢复安装两条路径都传递输出数据。选择后者：复用命令通道，在实际准备输出时取得快照。明确这是命令处理时的状态，不宣称是安装瞬间或网络到达时的状态。

## ready 的线格式与含义

在 internal/wsprotocol/protocol.go 增加 MessageTypeReady，新增 ready_v2.go：

```go
// ReadyMessage 是已接管连接的第一条服务端应用消息。
// 数值以十进制字符串发送；零位置仍必须出现在 JSON 中。
// NextOffset 仅代表当前网关逻辑会话已接纳的连续音频，不是持久化或推理完成承诺。
type ReadyMessage struct {
    Type           MessageType `json:"type"`                  // 固定 ready。
    SessionID      string      `json:"sessionId"`             // 本场稳定标识。
    ResumeToken    string      `json:"resumeToken"`           // 本场恢复凭据，不得记录整条消息到普通日志。
    Generation     uint64      `json:"generation,string"`     // 当前连接代次，非零。
    NextOffset     uint64      `json:"nextOffset,string"`     // [0,NextOffset) 已被网关接纳。
    InputEnded     bool        `json:"inputEnded"`            // 已接纳合法 end；为 true 时 NextOffset 即最终输入位置。
    AckedResultSeq uint64      `json:"ackedResultSeq,string"` // 网关已接纳的客户端累计结果确认。
}
```

首次和恢复连接都发送同一种 ready；恢复不重新生成 ID/token。ackedResultSeq 是已接纳的确认位置，不是最后生成/已发送的结果序号。恢复请求中的 appliedSeq 已在安装时提交，因此 ready 至少反映该提交；正常客户端在 ready 前等待，通常等于该位置。

本步不加入 workerCompleted：输入结束和整场完成不同，后续独立终态协议负责说明整场成功。也不加入独立 finalOffset：InputEnded=true 时 NextOffset 已固定，可表达最终位置。

协议客户端先等待 ready，再开始本代音频/end/ACK。当前 reader 可以与 writer 同时运行，不新增输入启动屏障；如果提前输入已被协调者合法接纳，快照可以反映它。如果输入在快照之后推进，旧快照仍是真实接纳前缀，重发通过已有去重处理。不能绕过既有范围/容量/代次校验，也不把服务端 ready 在先误称为客户端输入已被强制门控。

token 只向已接管连接输出；公开入口接入前必须先完成恢复凭据校验。本步内部候选入口仍由可信调用者提供，不声称已经有公网认证。首次 ready 丢失且客户端尚不知道身份时如何回收未被认领会话，属于后续握手与空闲期限组合验收。

## 协调者快照

新增 internal/gateway/session_ready.go：

```go
// connectionReady 是协调者一次处理命令时取得的值快照。
// 字段均为值或不可变字符串，不携带 Worker、缓冲或 resumeState 指针。
type connectionReady struct {
    sessionID      string // 本场稳定 ID。
    resumeToken    string // 本场恢复凭据，不进入普通日志。
    generation     uint64 // 本次查询验证通过的当前代次。
    nextOffset     uint64 // 网关连续接纳位置。
    inputEnded     bool   // 是否已接纳合法 end。
    ackedResultSeq uint64 // 已接纳的累计结果确认。
}

// requestConnectionReady 查询当前 attached 代次的 ready 快照。
// ctx 只控制命令请求；generation 来自固定写任务，不能使用客户端自报代次。
// 复用 submitCommand 的交付后等待明确回复规则。
// 不授权结果、不推进游标、不修改恢复期限；错误返回零值快照。
func (s *resumableSession) requestConnectionReady(
    ctx context.Context, generation uint64,
) (connectionReady, error)
```

在 sessionControlKind 增加 controlConnectionReady，在 sessionControlResult 增加 `ready connectionReady`。更新 command.generation 注释，说明本命令也使用固定代次。

协调分支复用已有命令处理前的 stopCause 和 cmd.ctx 检查。按以下次序回复：

1. worker=nil：errSessionWorkerUnavailable。
2. 非 attached：errSessionNotAttached。
3. cmd.generation 不等于当前代次：errSessionGenerationMismatch。
4. 返回 s.identity、s.resume.generation、worker.input.input.nextOffset、worker.input.input.ended、worker.results.ackedSeq 的值快照。

与 requestResult 一样允许 runWithWorker 部件模式使用，便于独立组合验证；不要求 connections 非 nil。生产已接管连接通过 attachment 唯一 writer 使用。正常 Worker 完成后处于 retaining 也允许查询，随后照常重放剩余结果。

命令只读状态，不能调用 offerResult；ready 未写成功时不得提前占用 inFlightSeq 或提升 offeredSeq。不改变 ACK、cursor、generation 或任何期限。不需要 readyWritten 控制命令：本步没有依赖 ready 写成功的协调者状态，后续动作由唯一写任务自身同步排序。

## 写任务接入

在 result_writer.go 增加：

```go
// runWithReady 是已接管连接唯一写任务的运行入口，只能运行一次。
// ctx 为本代连接 context，必须继承 config.controlCtx。
// 先查询并限时写出 ready，成功后同步调用已有 run 发送结果。
// 不启动额外 goroutine，不关闭连接，不自行 detach。
func (w *resultWriter) runWithReady(ctx context.Context) resultWriterExit
```

这里保留 run 作为已有结果部件运行入口。不是让两个入口同时运行：attachment 的 writer goroutine 只调用 runWithReady，后者直接调用 run，整个期间只有一个任务。以后控制输出增多时可以统一命名和循环，不需要现在为了名字迁移全部部件测试。不能在 connectionCandidate 增加可关闭 ready 的生产配置开关。

实现流程：

1. nil ctx 按现有运行入口处理为编程错误；用 stopCause 检查停止。
2. 用本代 ctx 和固定 generation 调用 requestConnectionReady。错误交给 classifyControlError。
3. 命令返回后先检查 stopCause，再将快照映射到 wsprotocol.ReadyMessage 并 json.Marshal；编码错误为 writerControlFailed，错误中不包含凭据或完整报文。
4. 编码后、实际写之前再检查 stopCause。复用 writeMessage 同步限时 Write；不要再启动 goroutine 竞速超时。
5. Write 失败时，先用 stopCause 判断生命周期停止；仍活跃则返回 writerWriteFailed 并保留错误身份。这与现有 result 的分类一致。
6. Write 成功后同步 `return w.run(ctx)`；其第一步仍会重新检查停止原因，然后才 requestResult。没有 reportResultWritten，也不等待客户端另发 ready ACK。

把 writeMessage 注释和错误包装从“只写结果”改为“写连接文本消息”，保留 ErrResultWriteTimeout 的现有身份，本步不连带迁移期限配置和错误名。任务事件仍只在整个 runWithReady 返回时由 attachment 报告一次，事件容量仍为 2。

在 connection_attachment.go 唯一 writer goroutine 内，把 `a.writer.run(a.ctx)` 改为 `a.writer.runWithReady(a.ctx)`。构造阶段仍不访问协调者、不做网络操作；候选被接纳前不得发送 ready。

候选安装成功是所有权提交，ready 写成功是之后的传输事实。ready 写失败不得回滚 generation、已接纳的 appliedSeq 或候选所有权；由既有事件裁决停止本代并保留/结束逻辑会话。即使对端可能收到失败 Write 的部分或全部数据，也不会在本代继续发送 result。恢复后新代重新发送一次 ready。

## 助手负责的验收

开发者只需实现上述协议、快照命令、写任务入口与 attachment 接线；测试及必要的旧夹具适配由助手完成。

- wire：0 和 MaxUint64 无精度丢失，位置是字符串，false/0 不因 omitempty 消失。
- 快照：首次、恢复、end 后、Worker retaining 的字段正确；无 Worker、detached、旧代次、取消和结束命令拒绝；读取不会改变交付授权、确认或期限。
- 顺序：预先已有结果时仍 ready 第一；阻塞 ready Write 期间没有结果授权，协调者仍能处理命令/期限。不同字段来自同一次协调处理，快照取得后状态推进不会修改已取值。
- 失败：ready 写失败/超时/取消后不进入结果循环，原结果保留；有效传输失败进入 detached 并保留原 Worker，整场清理仍等待实际任务与 socket。
- 真实双 WebSocket：两代第一条消息均为 ready，ID/token 不变、代次递增、接纳位置和累计确认正确；之后按已有序号重放未确认结果，原 Worker 不重建。
- 旧组件测试继续只测结果部件；managed connection 的测试必须显式消费并检查 ready，不能不分消息类型地跳过首帧来凑通过。

验收关注消息顺序、位置正确性与失败后的资源/数据归属。不以新增测试数包装成恢复耗时或业务吞吐改进；尚未完成公开握手、持续音频确认、整场完成确认或客户端自动恢复。
