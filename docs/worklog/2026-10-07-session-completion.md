# 第六阶段：整场正常完成通知与确认

日期：2026-10-07。状态：设计与实现指导，等待实现。上一小步音频累计确认已经验收；此次不新增运行成果。

## 能力目标和边界

当前链路可以发送 ready、audio_ack 和有序 result。Worker 正常完成且结果发送完后，writerResultsComplete 仅通知服务端协调者，客户端没有整场成功的应用消息，运行器也只能等外部关闭或保留期限结束。

本步实现一个内部端到端闭环：合法 end 和 Worker 正常完成 → 发完本代所需结果 → completed → 客户端确认 completed_ack → 统一清理。完成通知或确认丢失时，沿用同一逻辑会话的保留和恢复机制；恢复后重新发送完成通知。

之后接公开 start/resume 握手、注册表/名额生命周期与 v2 空闲保护，再接客户端有限缓存、重连与故障实验。本步不把上一轮“由助手实现音频确认”的单步授权扩大为后续全部核心开发授权：核心实现继续由开发者完成，助手负责评审、测试和记录。

本步只定义正常 completed。Worker 失败、协议违规、积压超限等仍沿用既有失败退出，不发送成功通知。失败终态的稳定线格式、活动会话撤销后的有界终态查询记录和实例重启恢复继续作为后续事项；不能仅凭本步宣称完整终态查询或生产恢复完成。

## 语义与方案选择

- end 表示输入终点；audio_ack.inputEnded 表示网关接纳了它。
- result.isFinal 表示某个片段定稿。
- completed 表示原 Worker 正常结束，本场最终音频位置和最后结果序号已固定。
- completed_ack 表示客户端已连续应用全部结果，并记录这场识别成功，允许网关释放剩余保留资源。

| 方案 | 优点 | 代价及选择 |
| --- | --- | --- |
| 正常关闭 WebSocket 表示完成 | 协议简单 | 难以区分传输关闭与业务完成，也缺少最终音频/结果范围；不采用 |
| 发一次 completed 后立即销毁会话 | 回收快 | 通知丢失后无法在原对象上重发；不采用 |
| 把 completed 放入识别结果序号流 | 可复用累计 ACK/重放存储 | resultBuffer、消费端和 seq 语义都要扩展为多种事件；可行，但当前只有一个不可变正常终态，选择单独小状态 |
| **单独 completed/completed_ack，复用固定保留窗口** | 输入、结果、成功声明语义清晰；完成通知可跨代重发 | 需要终态发送授权和确认校验；本步采用 |

completed_ack 同时累计确认到 lastSeq，不强制客户端先发单独 result_ack(lastSeq)。两条消息经同一 reader 有序处理，但一次确认就能表达“所有结果已应用且整场成功已记录”。

客户端的成功判定发生在：收到合法 completed，校验 finalOffset 与本地输入终点一致，且连续应用位置达到 lastSeq，并记录成功。此后发送 completed_ack 用于服务端回收；不等待 ACK-of-ACK 才把这场识别标为成功。

因此：completed 丢失时，客户端尚不知道成功，窗口内恢复后重放；completed_ack 丢失时，客户端已知道成功，服务端继续保留到固定期限或再次确认。客户端不能因为 ACK Write 失败就重建问诊或改判识别失败。服务端保留到期表示交付未获确认，不反过来证明 Worker 计算失败。

## 线格式

新增 MessageTypeCompleted="completed" 和 MessageTypeCompletedAck="completed_ack"；建议在 wsprotocol/completion_v2.go 定义：

```go
// CompletedMessage 是一场正常识别的固定完成范围。
// 同一场恢复后 FinalOffset/LastSeq 不变，Generation 属于本代连接。
type CompletedMessage struct {
    Type        MessageType `json:"type"`               // 固定 completed。
    Generation  uint64      `json:"generation,string"`  // 服务端本代代次。
    FinalOffset uint64      `json:"finalOffset,string"` // 已接纳的合法输入终点。
    LastSeq     uint64      `json:"lastSeq,string"`     // 最后一条识别结果序号；无结果为 0。
}

// CompletedAckMessage 表示客户端已应用全部结果并记录整场成功。
// 代次使用 reader 固定的服务端 generation，不接纳客户端自报代次。
type CompletedAckMessage struct {
    Type        MessageType `json:"type"`               // 固定 completed_ack。
    FinalOffset uint64      `json:"finalOffset,string"` // 必须匹配 completed。
    LastSeq     uint64      `json:"lastSeq,string"`     // 必须匹配 completed，同时累计确认结果。
}
```

所有位置保持十进制字符串、必需字段和严格 uint64 校验，零值合法。扩展 V2InputKind 为 V2InputCompletedAck；V2Input.Offset/Seq 分别携带 finalOffset/lastSeq，更新注释。增加保留两个 json.RawMessage 的 wire 类型，沿用 decodeSingleJSONObject 和 decodeUint64String；缺失/null/JSON 数值/溢出均拒绝。generation 不从报文读取，未知字段继续沿用已有忽略规则。

reader 新分支同步调用 requestCompletionAck(ctx, config.generation, input.Offset, input.Seq)。命令失败沿用 classifyCommandError；成功后按既有循环尾部检查停止，不自行 close/cancel RPC。reader 在正常 writer 退出后仍须能读取此确认。

## 协调者状态与接口

新增 internal/gateway/session_completion.go：

```go
// completionSnapshot 是正常计算完成后的固定业务范围值。
// 由协调者读取，不能用片段 final 或客户端请求自行构造成功。
type completionSnapshot struct {
    finalOffset uint64 // 合法 end 对应的音频终点。
    lastSeq     uint64 // 正常 Worker 最后结果序号，允许 0。
}

// completionState 由协调者独占，嵌入 sessionWorker。
// 使用 generation 标记授权，避免 lastSeq=0 被误判为未发送。
type completionState struct {
    offeredGeneration uint64 // 当前代已获发送 completed 授权；0 表示没有。
    acknowledged      bool   // 已接纳完整 completed_ack；之后协调者立即结束。
}

// requestCompletion 在当前 attached 代次取得完成范围及发送授权。
// ctx 为本次请求；成功回复前协调者已登记 offeredGeneration。
// 失败返回零值快照，不改变确认、代次或期限。
func (s *resumableSession) requestCompletion(
    ctx context.Context, generation uint64,
) (completionSnapshot, error)

// requestCompletionAck 校验本代完成确认并允许整场清理。
// finalOffset/lastSeq 必须匹配服务器终态，lastSeq 同时累计确认结果。
// nil 表示确认已接纳，不表示 socket/任务/缓冲已经清理完毕。
// 命令一旦交付，沿用 submitCommand 等待明确回复的规则。
func (s *resumableSession) requestCompletionAck(
    ctx context.Context, generation, finalOffset, lastSeq uint64,
) error
```

给 sessionWorker 增加 `completion completionState`，零值可用；不要将它放到 attachment 或候选对象中。现有 resultDeliveryState.offeredSeq 仍只描述识别结果授权。

在 sessionControlKind 增加 controlCompletion 和 controlCompletionAck，在 sessionControlResult 增加 `completion completionSnapshot`。确认命令沿用 cmd.offset/cmd.resultSeq，补齐字段注释。两个命令均先经过现有 stopCause/请求取消检查，再检查 Worker、attached 和 generation，与其他操作保持一致。部件模式 runWithWorker 也可使用这两个命令，便于独立测试。

可以提取以下私有 helper，避免大循环继续堆业务判断：

```go
// offerCompletion 只能由协调者在附着资格校验后调用。
// 仅正常 retaining 且结果发送授权已清空时登记本代完成通知。
func (w *sessionWorker) offerCompletion(generation uint64) (completionSnapshot, error)

// acknowledgeCompletion 只能由协调者在附着资格校验后调用。
// 完整校验之后才提交累计结果确认和 acknowledged；失败无部分提交。
func (w *sessionWorker) acknowledgeCompletion(
    generation, finalOffset, lastSeq uint64,
) error
```

### 发送授权

offerCompletion 依次要求：

1. worker.phase == workerRetaining 且 input.ended=true。
2. delivery.inFlightSeq==0 且 delivery.cursor==results.lastSeq：本代需要的结果已写成功，或此前已由客户端累计确认跳过。
3. 从 input.nextOffset/results.lastSeq 取得固定范围，然后设置 completion.offeredGeneration=generation，再回复。

前两项不满足返回 errCompletionNotReady。同代重复内部查询可以返回同一快照，保持既有授权；网络 writer 每代只执行一次完成发送。查询不推进结果 ACK，不释放结果，也不改变任何截止时间。

必须在实际 Write 之前登记授权：客户端读到消息并发回 ACK，可能早于服务端 Write 返回和 writer 退出事件。如果等 writerCompletionSent 才允许确认，就会错误拒绝合法的快速客户端。

### 完成确认

acknowledgeCompletion 在不修改任何状态的情况下先检查：

1. workerRetaining/inputEnded；否则 errCompletionNotReady。
2. completion.offeredGeneration==generation；否则 errCompletionNotOffered。
3. finalOffset==input.nextOffset 且 lastSeq==results.lastSeq；否则 errCompletionMismatch。
4. 验证仍无结果在途且 cursor==lastSeq，确认范围不超过 delivery.offeredSeq。合法路径应满足；不满足按 errCompletionNotReady 拒绝。

全部通过后调用已有 acknowledgeResult(lastSeq)，成功再设置 completion.acknowledged=true。即使所有结果早已单独 ACK，本次完成确认仍有效，不能因为 acknowledgeResult 返回 advanced=false 就忽略它。空结果 lastSeq=0 也必须先有本代 completed 授权。

协调者对有效 controlCompletionAck 给出唯一成功回复，然后 return nil，沿用现有 defer 关闭 controlDone、外层运行器停止/等待已接管资源。无效确认回复错误并继续控制循环；实际 reader 会将业务拒绝作为 readerCommandFailed 汇报，按现有规则结束错误客户端。测试直接调用内部命令时应先验证拒绝没有改变正式状态。

记录 acknowledged 用于区分 completed_ack 正常收尾与显式 requestClose（后者也可能返回 nil）。它只由协调者写，外层只能在协调结束之后读取，不能借这个字段并发窥视状态。清理错误仍由既有外层 errors.Join 保留；已获完成确认的业务事实不能由清理结果覆盖。

控制循环关闭以后重复命令返回既有 errResumeClosed，不提供“会话已经删除仍可重复确认”的新承诺。客户端已记录成功，无需等待确认的确认。活动表删除后的有界终态查询属于后续入口设计。

## 断线、恢复与期限

在 resetResultDelivery 中同时清零 completion.offeredGeneration。该 helper 已用于有效 detach 和成功 resume，确保上一代授权不能让新代在发送 completed 前接受完成确认。不要清零 acknowledged；它只在最后确认成功后置 true，之后不存在合法新代恢复。

未获 completed_ack 时，正常 Worker 完成后的既有 resultRetentionTimeout 持续计时；断线后恢复期限也照常生效，采用当前最早绝对期限规则。发送/重发 completed、普通 result_ack 以及恢复均不延长正常完成后的保留期限。

断线发生在 completed 之前、实际 Write 期间或写成功但未确认之后：旧连接仍由 attachment 清理，原结果/固定终态范围保留；新代依次 ready → 必要结果重放 → completed。恢复请求中的 appliedSeq 若已经覆盖 lastSeq，新代可直接在 ready 后发送 completed。

逻辑取消和到期仍由命令处理前的 stopCause 优先裁决。连接失败和确认并发到达时，以协调者已经提交的状态为准：先 detach 则该代确认失去资格，先接纳确认则进入最终清理。合法 completed_ack 已记录后，writer 因清理而退出的迟到取消/写失败不改变已提交的业务确认。

## 唯一 writer 接线

新增 writerCompletionSent 退出类型，lastSeq 为正常通知中的末尾序号，err=nil；表示本代 completed Write 成功，不表示客户端已确认。保留 writerResultsComplete 供已有仅结果部件入口使用。

可在 session_completion.go 或 result_writer.go 增加：

```go
// sendCompletion 取得本代完成授权，编码并同步限时写 completed。
// ctx 属于本代连接；不启动任务、不关闭连接、不等待客户端确认。
// 成功返回 writerCompletionSent；错误沿用停止/控制/写入分类。
func (w *resultWriter) sendCompletion(ctx context.Context) resultWriterExit
```

在 runLoop 的 `!offer.available && offer.workerCompleted` 分支：lastInput=nil 时保留 writerResultsComplete；非 nil 的完整连接输出入口调用 sendCompletion。此前 audio_ack 分支已经处理最终 inputEnded=true，因此输出顺序为 ready → 必要音频确认/result → completed。

sendCompletion 沿用 ready 的检查顺序：stopCause → requestCompletion → stopCause → 编码 → stopCause → writeMessage → 分类。失败时不假报成功，不自行撤销完成授权。成功返回 writerCompletionSent，无需另加 completedWritten 命令或等待 ACK 的新 goroutine。因为已登记授权，reader 可以在这个方法返回之前完成确认；此时该方法可能返回 writerStopped，外层仍按已提交确认完成清理。

handleConnectionEvent 为 writerCompletionSent 增加正常退出分支，保持 reader/连接可读取确认，仅记录本代输出完成事实。sessionConnections.outputComplete/lastSeq 的注释相应扩大为正常输出结束（根据事件类型区分是否含完成通知），不能拿 outputComplete 当作 completed_ack 的前置条件。保留旧 writerResultsComplete 分支用于原有部件/事件行为，真实 managed 输出会走新类型。

## 实现顺序与验收

建议一次实现这三个相连部分：协议/严格解码 → 协调者完成授权和确认 → writer/reader/连接事件接线。测试及既有夹具适配由助手完成。确认后的自动收尾会改变目前测试中显式 requestClose 的路径，新的网络成功场景应真正通过 completed_ack 结束，不能全部继续使用外部关闭代替。

必须验证：

- 正常 end、最终输入确认、尾部结果、completed 的顺序；空音频/零结果可完成；片段 final 或 end 单独出现都不能发送整场成功。
- Worker 提前 EOF、真实错误、超时及协议失败不输出 completed。
- 最终结果仅 Write 成功、普通 result_ack 覆盖全部结果，都不会代替 completed_ack 提前清理。
- 未授权、旧代、错误 finalOffset/lastSeq、缺失字段及溢出确认被拒绝，不部分清理；无独立 result_ack 的有效 completed_ack 可以确认全部结果。
- ACK 在 completed Write 返回之前到达仍可接受；controlDone 可以先关闭，但运行器必须等实际 Write/socket/任务退出后才释放缓冲。
- completed 写失败不假完成；通知丢失、确认丢失后窗口内恢复重新通知，音频终点/末序号不变、代次变化，未确认结果照常重放。
- 多次恢复/通知不延长固定 retention 期限；已计算成功但未确认保留到期仍明确记录交付未确认。
- 真实 WebSocket 客户端发送 completed_ack 后，运行器无需外部 requestClose 即完成所有清理；最后已清理状态下的重复确认不引发二次资源释放。

以上是待验证行为；本次仅提交设计文档，不记录测试通过数或恢复性能收益。
