# 第六阶段：持续音频接纳确认

日期：2026-10-07。状态：设计与实现指导，等待开发者实现；本步没有新增运行或测试结果。

## 能力目标与后续路线

ready 已能在每次连接接管后告诉客户端当前音频位置，但长连接运行期间还没有继续反馈。客户端若为断线恢复保留所有已发送音频，只靠下一次 ready 才能清理，缓存会随问诊持续增长。

本步在原有唯一写任务中输出累计 audio_ack：客户端可以据此删除已被当前网关逻辑会话接纳的音频前缀，并知道合法 end 是否已接纳。与识别结果组合验证后，下一步设计整场终态投递/确认；随后接公开握手、入口资源管理/空闲保护和客户端有限重放。客户端缓存实现仍在后续，因此本步不能记录“客户端内存下降多少”的已达成收益。

## 问题、选择与取舍

客户端已经发送 [0,9600) 字节，网关依次接纳到 3200、6400、9600。累计确认 9600 已包含前面两个位置，不要求排队发送三条确认。音频接纳位置与 Worker 处理进度分别服务于重放和积压控制；不能等到识别结果出现才确认，也不能用 TCP/WebSocket Write 成功推断网关已接纳。

| 方案 | 收益 | 代价/本步决定 |
| --- | --- | --- |
| reader 每收一块直接写确认 | 路径短 | 与结果 writer 并发写入，读路径受慢写阻塞；不采用 |
| 每块入确认队列，唯一 writer 消费 | 保留逐块响应关系 | 累计确认的中间位置可合并，队列反而需要容量及过载规则；不采用 |
| 固定间隔轮询位置 | 易控制确认频率 | 空闲时仍查询，多一组定时器和确认等待；需要测量后再考虑批量/限频 |
| **状态变化通知 + 每次查询最新累计位置** | 无逐块确认队列，空闲等待，可合并多个更新 | 必须避免漏唤醒，并安排与结果输出的顺序；本步采用 |

通知复用现有关闭并替换的 changed 通道，并将 helper 改名为 notifyOutputChange，说明它现在也覆盖输入接纳变化。查询复用 requestResult/offerResult：同一次协调命令返回输入快照、下一条结果授权和变化通知。若另起一次独立输入查询再读 changed，要额外处理两个查询之间发生更新的窗口；共用快照更容易推理。

写任务每轮最多发送一条 audio_ack，然后发送本次已经取得的一条 result。输入持续到来时，不在本轮追着最新位置反复发 ACK，否则可能一直推迟识别结果。多次状态变化合并到下一轮。慢 ACK 仍会挡住随后一条结果的写入，沿用每次 Write 的期限保护；轮流服务是防止饥饿的规则，不是实测延迟保证。

## 线格式与确认语义

在 wsprotocol/protocol.go 增加 MessageTypeAudioAck = "audio_ack"，新增 audio_ack_v2.go：

```go
// AudioAckMessage 确认网关已接纳的连续音频前缀以及合法输入终点。
// 位置是字节数；数值使用十进制字符串，零位置和 false 必须保留。
type AudioAckMessage struct {
    Type       MessageType `json:"type"`              // 固定 audio_ack。
    Generation uint64      `json:"generation,string"` // 当前连接的固定代次。
    NextOffset uint64      `json:"nextOffset,string"` // [0,NextOffset) 已被当前逻辑会话接纳。
    InputEnded bool        `json:"inputEnded"`        // 已接纳合法 end，NextOffset 即最终输入位置。
}
```

- 客户端收到 nextOffset=N 可以释放 [0,N) 的重放音频。这个承诺针对仍存活的当前网关逻辑会话/原 Worker；没有持久化，实例丢失后不能据此宣称原音频可恢复。
- inputEnded 从 false 变 true 本身需要一次确认，即使 offset 没变。它说明 end 已接纳，整场识别是否成功仍由后续终态协议说明。
- ready 已携带相同两项输入状态，视作本代第一次累计输入确认；不要随后无条件再发相同 audio_ack。
- 每次成功的新音频/end 推进状态。历史音频重发、重复 end 若未改变状态，不强制再发 ACK；此前尚未输出的变化仍会被累计快照覆盖。客户端不能依赖“一条请求对应一条 ACK”。
- 同一连接上的客户端收到的是有序消息；断线后用新代 ready 重新对齐位置。audio_ack 不需要另加 ACK-of-ACK，也不占识别结果 seq，不参与 result_ack 的确认范围。

## 数据结构与快照

新增 internal/gateway/session_output.go：

```go
// inputAcceptance 是协调者已提交的输入接纳状态值。
// 两字段构成一个快照；inputEnded=true 时 nextOffset 已固定。
// 可以按值比较，不能根据 Worker 处理位置构造。
type inputAcceptance struct {
    nextOffset uint64 // 连续已接纳字节位置。
    inputEnded bool   // 已接纳合法 end。
}
```

在 resultOffer 中新增 `input inputAcceptance` 字段。offerResult 成功时，无论 available 是否为 true，都从 worker.input.input 的 nextOffset/ended 复制 input。它与 workerCompleted、changed 和结果授权均来自这次协调处理；错误仍返回零值。

扩大 resultOffer/resultDeliveryState.changed 的注释：它们可用于结果部件，也提供统一连接输出需要的输入快照/唤醒；available 只表示本次是否借出结果，不能用来决定是否要发音频确认。无需另建音频确认缓冲、全局 lastAckOffset 或音频写成功命令。

## 状态变化通知

将 notifyResultChange 重命名为 notifyOutputChange，更新所有现有调用；保留当前关闭旧 channel、创建新 channel 的做法，最终退出仍只关闭当前 channel。

在协调者的 controlAudio 和 controlEnd 成功路径加入：

```go
// 只有首次接纳新音频或合法 end 才改变累计输入状态。
// 必须先提交状态，再通知，最后回复命令。
if accepted {
    worker.notifyOutputChange()
}
```

具体位置：controlAudio 在 offerAudio/offerHistoricalAudio 无错误后的统一成功回复前；controlEnd 在 acceptEnd 成功、必要的首次尾部期限建立之后、成功回复之前。现有 awaiting-status/historical/重复 end 特殊分支只允许不推进状态的重发，不需要人为通知。错误、旧代次、重复输入不新增唤醒，不重置任何期限。

通知不是排队消息：3 次输入推进可先后关闭通知通道，但 writer 被唤醒后只读最新 input 快照。如果快照读完后、真正等待前又发生更新，快照持有的旧 changed 已关闭，select 会立即唤醒并重新查询。因此不能在准备等待时直接读取 worker.delivery.changed 的当前值；只能使用本次 offer 返回的 changed。

这保证没有音频确认消息队列随输入块数增长。每次状态变化仍有 channel 创建/关闭开销，通知复用也可能唤醒只关心结果的部件；目前接受该成本，确认频率和分配成本留待负载测量后决定是否分离通知或批量触发。

## 唯一写任务的循环

复用现有 resultWriter，不启动新的写 goroutine。将当前 run 的循环主体抽到：

```go
// runLoop 在一个固定连接上同步执行输出循环。
// ctx 属于本代连接。lastInput 仅由本次写任务访问，不与协调者共享。
// 非 nil 时启用累计音频确认，其初值必须是已成功写出的 ready 输入快照；
// nil 保留已有独立结果部件的行为。不是可关闭生产确认的候选配置项。
// 返回前遵守现有结果成功汇报和实际 Write 返回规则。
func (w *resultWriter) runLoop(
    ctx context.Context, lastInput *inputAcceptance,
) resultWriterExit
```

原 run 保留为 `return w.runLoop(ctx, nil)` 的结果部件入口，nil ctx 的编程错误检查放在 runLoop。managed attachment 仍只调用 runWithReady；ready 成功写出后，用 ready 的 nextOffset/inputEnded 初始化一个局部 inputAcceptance，然后同步进入 runLoop(ctx, &lastInput)。不要在 ready 写成功后重新查询位置来初始化 lastInput，否则可能把尚未发给客户端的新位置误当作已经发送。

在原循环 requestResult 返回并通过 stopCause 检查之后、处理 offer.available 之前，加入音频确认分支：

```text
若 lastInput != nil 且 *lastInput != offer.input：
    使用固定 generation 和 offer.input 编码 audio_ack
    编码失败 → writerControlFailed
    写之前再次检查 stopCause
    复用 writeMessage 同步限时写入
    写失败 → 停止原因优先，否则 writerWriteFailed
    成功后 *lastInput = offer.input

再次检查 stopCause
若 offer.available：
    沿用原 result 编码、写入、成功汇报和 continue
否则若 offer.workerCompleted：
    返回 writerResultsComplete
否则：
    等待本次 offer.changed/生命周期信号，下一轮统一判断
```

不要在 audio_ack 写成功后立刻 continue：当前 offer 可能已经建立一条结果的 inFlight 授权，应继续完成该结果。若 ACK 写失败，则退出本代，由已有 detach/close 重置授权。不能自行清除 inFlight、假报 resultWritten 或丢弃已经取得的结果。

这是本步与 ready 的区别：ready 之前没有结果授权；audio_ack 之前的 requestResult 可能已授权一条结果。持有一条结果跨越一次 ACK 写入是明确的取舍，沿用现有“一条在途结果”上限，不引入额外结果队列。若将来要让协调者先选择消息类型再授权，可改为统一输出命令，但当前没有必要增加该调度状态。

ACK 写成功仅更新写任务本地的 lastInput，不代表客户端已消费；客户端是否收到通过连接状态和重连 ready 对齐。ready/ACK 正在 Write 期间新输入继续推进时，只记录本次实际发送的旧快照，下一轮再确认最新位置。

最后一次 inputEnded=true 的确认必须先于 writerResultsComplete，包含无结果/空音频的正常完成。Worker retaining 后输入位置不再推进；重复 end 已被此前 ready 或 ACK 覆盖，因此当前阶段仍可保留 writerResultsComplete 返回和 reader 继续读结果确认的安排，后续终态投递会继续演进该出口。

保持原结果 Write 成功后使用 controlCtx 汇报的语义；不能因增加 ACK 分支破坏迟到成功报告。writeMessage 的注释覆盖 ready/audio_ack/result；可以将错误包装文案统一为 write connection message，保留 ErrResultWriteTimeout 的既有身份和期限配置。

## 实现顺序与验收

开发者按一次小闭环完成：AudioAckMessage/inputAcceptance → 扩展 offerResult 快照 → 输入变化通知 → 抽取 runLoop 并串行发送确认。助手负责测试与既有 managed 网络/ready 测试的适配；旧结果部件入口保持原消息范围。测试不得盲目跳过 audio_ack，必须检查其代次、位置和 end 标志。

验收前固定要证明的行为：

1. ready 仍是首帧；没有新的输入变化不重复 ACK；新音频即使没有任何识别结果也能收到累计确认。
2. 从已知快照等待、在查询与等待之间更新、在 ACK Write 期间更新，均不漏最终位置；受控突发输入可合并到最新位置，不排逐块确认队列。
3. 输入持续推进且结果已经授权时，一次 ACK 后继续发送该结果，不无限追发 ACK、不重复借出结果；反过来结果连续到来时，每轮仍检查新的输入确认。
4. 相同 offset 的合法 end 必须输出 inputEnded=true；重复输入/重复 end 不额外推进状态、通知或 Worker 输入。空音频/end 后没有结果的完成也输出最终输入状态，先于 writerResultsComplete。
5. ACK 失败/超时/取消不假报结果成功；当前代清理后仍能凭新 ready 对齐最新接纳位置并重放原结果。旧写任务局部进度不影响新代。
6. 真实 WebSocket 组合：ready → 音频 → audio_ack；与 result 交错时结果序号完整有序；end 被确认；断开后两代 ready 和未确认结果重放正确。确认期间仍遵守写期限和共同清理。

本步可记录的是正确性行为、累计确认是否合并及应用层是否新增确认队列。客户端缓存尚未实现，也没有性能实验，不报告缓存下降比例、恢复耗时或吞吐提升。后续若引入按时间/字节阈值合并，必须设置最大确认等待并单独验证客户端重放预算。
