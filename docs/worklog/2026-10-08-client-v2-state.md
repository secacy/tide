# 第六阶段：v2 客户端协调状态

日期：2026-10-08。状态：设计与实现指导，等待开发者实现；本轮没有新增业务代码或测试结果。

## 能力目标与当前位置

服务端公开 v2 已接通，客户端有界音频缓存已通过独立验收。本步把缓存与客户端业务事实组合：建立会话身份、校准恢复位置、授权发送、接受音频确认、连续应用结果，以及记录整场成功。

本步新增 internal/wsclient/client_state_v2.go，由一个协调者串行调用。不启动 goroutine、网络、Timer 或结果回调，不调用既有 v1 Client.Run。它的测试会直接组合上一步真实 audioReplayBuffer，验证完整的本地状态演进。

随后两组工作：

1. 接入严格服务端消息解码、单连接读写任务和 v2 运行器，先验证正常端到端完成及资源退出。
2. 加入旧任务共同退出、绝对重试预算和退避，验证断线后自动恢复，再进行故障实验。接线时一起固定音频源与结果应用的取消/退出契约。

## 为什么需要这一层

例如客户端已缓存 12 字节，但只把前 8 字节交给写任务。缓存部件认为位置 12 是合法块边界；客户端却不能相信一个确认 12 的响应，因为后 4 字节根本没有交给网络。另一方面，ACK 可能先于 Write 的完成事件到达，因此也不能只允许确认“Write 已返回成功”的位置。

结果同样存在两个时刻：收到结果、业务成功应用结果。只有后者才允许发送 result_ack，或在 resume 中声明 appliedSeq。所有音频都已确认仍不代表识别完成；需要最终 completed 的音频终点和结果末尾都与本地事实一致。

## 方案与取舍

| 方案 | 收益 | 代价与决定 |
| --- | --- | --- |
| 在 reader/writer 各自更新位置并加锁 | 网络操作附近就能修改状态 | 身份、缓存释放、结果应用和恢复切换需要共同判断；跨多处锁容易出现局部已提交。本轮不采用 |
| 直接把全部业务分支放进网络运行循环 | 文件少，早期接线直观 | 验证边界需要反复制造网络时序，协议判断与退出逻辑纠缠 |
| 串行状态对象 + 后续运行器执行 I/O | 同一个方法检查相关事实后提交，可确定性验证；读写任务只报告事件 | 需要清晰的调用契约，状态对象不能单独保证任务退出。本轮采用 |

沿用已验收缓存的原块边界与复制所有权，不再引入另一个音频队列。发送前登记授权上界，替代“Write 成功后才能相信 ACK”的方案；代价是授权只说明可能发送过，失败后必须按服务端 ready 恢复，不能自行回滚并猜测对端接纳情况。

结果选择“先检查、业务应用、再提交位置”三步。先推进位置再调用业务处理会使失败结果被错误确认。首版由协调者保证三步串行，状态对象不执行回调；若以后让结果异步应用，须增加待应用任务、取消与跨代提交规则，不能直接并发调用这两个方法。

## 状态及字段

```go
// v2ClientState 保存一个存活客户端对本场会话已知的事实。
// 必须通过构造器创建，使用后不复制；仅由一个协调者串行访问。
// 不负责网络、超时、重试和资源关闭；含凭据，不得整体输出到日志。
type v2ClientState struct {
    audio *audioReplayBuffer // 独占缓存；外部不得绕过状态方法修改它。

    sessionID   string // 第一个合法 ready 确定，此后保持不变。
    resumeToken string // 同场恢复凭据，不得记录到普通日志。
    generation  uint64 // 最后接纳的服务端代次；尚无身份时为 0。
    attached    bool   // 当前连接已接纳 ready，可处理业务消息；不表示 socket 存活。

    sendOffset    uint64 // 本代下一次音频授权的起点；ready 时校准。
    offeredOffset uint64 // 本场曾交给唯一写任务的最高连续音频末端。
    endOffered    bool   // 本场曾授权发送合法 end；发送失败也不撤销。
    endOfferedOnConnection bool // 本代是否已授权 end，避免重复派发。
    inputEnded    bool   // 已从可信服务端消息得知 end 被接纳，不能回退。

    appliedSeq uint64 // 业务已连续成功应用的结果序号；尚无结果时为 0。
    completed  bool   // 已核对完整 completed 并记录成功；确认写失败不能撤销。
}
```

只维护有不同含义的事实。已采集末端、已确认位置和本地正常输入结束直接使用 audio.nextOffset、audio.ackedOffset、audio.sealed，不再复制计数。最终输入位置在 seal 后就是 audio.nextOffset。

主要不变量：

- `audio.ackedOffset <= sendOffset <= offeredOffset <= audio.nextOffset`。首次 ready 前允许采集，但 offeredOffset 必须为 0，不允许派发音频。
- offeredOffset 单调不减；sendOffset 在恢复时可退回合法的 ready.NextOffset，以便重放原来可能没被接纳的音频。
- 只有正常 EOF 才 seal；输入源报错由运行器终止，不能伪装为正常 end。
- endOffered 为真意味着本地已封口，且授权时全部原音频已按序交给写任务。
- appliedSeq 仅在业务成功应用后连续前进；completed 为真后不接纳新结果、不新建或恢复会话。

## 方法骨架

以下都是本步实现范围，放在一个文件中即可。返回错误的方法必须保证状态及缓存不发生部分修改。

```go
// newV2ClientState 创建尚无身份的状态，并按双预算创建独占音频缓存。
// maxBytes/maxChunks 含义和校验沿用 newAudioReplayBuffer。
func newV2ClientState(maxBytes uint64, maxChunks int) (*v2ClientState, error)

// appendAudio 复制新采集音频，返回逻辑起点；允许在握手或断线期间调用。
// 容量满、输入已封口等错误沿用缓存规则；不会推进发送授权位置。
func (s *v2ClientState) appendAudio(payload []byte) (uint64, error)

// sealInput 记录正常 EOF 并固定终点；允许重复调用，不代表 end 已发送。
func (s *v2ClientState) sealInput() uint64

// acceptReady 校验并接纳当前尝试的第一条服务端业务消息。
// 调用方已排除旧尝试事件，且上代实际读写任务已经退出。
// 全部检查通过后，才共同提交身份/代次、音频确认及本代发送游标。
func (s *v2ClientState) acceptReady(message wsprotocol.ReadyMessage) error

// detach 使指定代次不再接纳业务事件；旧代或重复通知返回 false。
// 保留身份、缓存、累计授权、结果应用及成功事实，不负责关闭或等待任务。
func (s *v2ClientState) detach(generation uint64) bool

// authorizeAudio 为当前代唯一写任务准备下一块独立副本，并先登记授权。
// 无待发送音频返回零值,false,nil；出错不推进位置。
// 调用方保证没有其他待写/在途任务，返回后不得另行排队无界副本。
func (s *v2ClientState) authorizeAudio(generation uint64) (replayAudioChunk, bool, error)

// authorizeEnd 在封口、音频派发至末端、此前写任务实际返回后授权 end。
// 条件未齐、已知输入结束或本代已授权时返回 0,false,nil。
// 成功返回固定 finalOffset，并在实际 Write 前记录授权事实。
func (s *v2ClientState) authorizeEnd(generation uint64) (uint64, bool, error)

// acceptAudioAck 校验绑定代次、授权上界和 end 事实，再释放缓存前缀。
// 返回音频确认位置或 inputEnded 是否前进；旧 ACK 不使任何事实回退。
func (s *v2ClientState) acceptAudioAck(generation uint64, message wsprotocol.AudioAckMessage) (bool, error)

// checkResult 检查本代结果是否需要业务应用，只读状态。
// 已应用序号返回 false,nil；下一个连续序号返回 true,nil；跳号报缺口。
func (s *v2ClientState) checkResult(generation uint64, message wsprotocol.SequencedResultMessage) (bool, error)

// commitApplied 仅在对应结果已被业务成功应用后调用，推进一个连续序号。
// 调用方把 checkResult/应用/commitApplied 串行执行，成功前不发 ACK。
func (s *v2ClientState) commitApplied(generation uint64, seq uint64) error

// acceptCompleted 校验本场输入终点、end 授权及连续结果末尾，然后记录成功。
// 首次成功返回 true,nil；当前代相同的重复通知返回 false,nil。
// 成功后才允许运行器构造并发送 completed_ack。
func (s *v2ClientState) acceptCompleted(generation uint64, message wsprotocol.CompletedMessage) (bool, error)
```

可以提取当前连接检查、输入结束事实检查等私有函数；不要为了预演而浅拷贝状态后调用 audio.acknowledge，那仍会修改原缓存。正确顺序是先只读检查全部条件，再提交缓存与普通字段。

## 各方法的具体规则

### ready：首次建立与恢复

共同检查 Type=ready、ID/token 非空、当前未 attached、尚未 completed。只有当前连接尝试的首条消息才能调用；这个条件由未来运行器的本地 attempt ID 与消息阶段保证，不能仅靠服务端 generation 猜测。

- 首次：generation 必须为 1，NextOffset=0、InputEnded=false、AckedResultSeq=0。客户端可以已缓存音频，但不能已有音频/end 授权或应用结果。检查后记录身份。
- 恢复：ID/token 必须与已存值完全一致；generation 严格大于上次已知值，允许跳号。比如第 2 代安装成功但 ready 丢失，下次合法 ready 可以是第 3 代，不能硬性要求只加 1。
- 恢复的 AckedResultSeq 必须等于本客户端 appliedSeq。未来运行器用该值提交 resume，并在握手期间冻结结果应用；现有服务端恢复提交会把结果累计确认推进到请求位置。不能用响应中的更高位置替本地跳过结果。
- 音频位置必须 `NextOffset <= offeredOffset`，且通过 audio.checkReplayOffset。小于已释放前缀报缺口；大于授权上界、超出本地范围或块中间位置拒绝。首次的零位置也符合此规则。
- InputEnded=true 仅在本地已封口、曾授权 end、且 NextOffset 等于最终输入位置时合法。已经知道 inputEnded=true 后，恢复 ready 不得返回 false。

全部检查通过，再 acknowledge、记录身份/代次、attached=true、sendOffset=NextOffset、endOfferedOnConnection=false、提交 inputEnded。offeredOffset、endOffered、appliedSeq 不因恢复清零。

### 派发音频与 end

authorizeAudio 先检查当前绑定代次及未 completed，调用 audio.copyChunkAt(sendOffset)。有块时，先把 sendOffset 推进到该块末端，再令 offeredOffset=max(offeredOffset,sendOffset)，最后返回副本。这次调用就是授权提交点，运行器紧接着把该任务交给唯一 writer。

只有之前写任务已实际返回，运行器才能再次授权音频或 end。状态部件没有 Write 的完成事件，无法自行证明“没有在途任务”；这是后续接线必须测试的调用契约。若写入失败或授权后取消，保持授权事实、关闭本代，再按 ready 恢复，不能原地继续派发或自行回滚。

authorizeEnd 需要 audio.sealed、sendOffset==audio.nextOffset，并且 inputEnded=false、endOfferedOnConnection=false。首次授权设置两个 end 标志。恢复 ready 表示尚未接纳 end 时，新代可重发同一终点；表示已接纳时不再发。空输入同样可以授权 finalOffset=0。

### 音频 ACK

先检查事件绑定的 generation 等于当前 attached 代次，再检查消息 Type 和消息自带 Generation 等于该绑定值。旧任务事件不会因伪造报文 Generation 获得新代资格。

ACK 的 NextOffset 不得超过 offeredOffset。InputEnded=true 必须满足与 ready 相同的本地封口、end 授权和终点一致条件。先检查这些条件，再调用 audio.acknowledge；没有先释放音频再检查 end 的路径。

重复/旧 ACK 位置允许无操作，包括落在已释放历史范围内的位置；这与恢复 ready 的缺口判断不同。旧快照的 false 不撤销已经知道的 inputEnded；合法 true 可以在位置不变时单独推进 end 确认。最后 sendOffset=max(sendOffset,audio.ackedOffset)，避免下一次重放访问已经释放的块。

### 结果检查、应用及确认

结果消息没有服务端 generation 字段，必须使用 reader 绑定的固定代次作为参数。先检查当前连接，再校验 Type=result、Seq>0。

- `Seq <= appliedSeq`：按序号去重，不再次执行业务回调；可由运行器再次发送当前累计 result_ack。
- `Seq > appliedSeq`：仅差值为 1 时可应用；差值比较避免 appliedSeq+1 在 uint64 上限回绕。已 completed 时拒绝新结果。
- 跳号返回结果缺口，不把 appliedSeq 直接跳到收到的序号。

checkResult 返回 true 后，未来运行器调用业务处理；只有成功才 commitApplied。commitApplied 再检查当前代次、未完成及严格下一个序号；提交后才可发送 result_ack，发送失败仍保留 appliedSeq，恢复请求声明该值。

去重依据服务端本场结果序号；相同 SegmentID 的多次 partial/final 更新可有不同 Seq，必须分别应用。首版不保留所有历史结果用于核对重复消息内容，依赖服务端同一序号内容不变的契约。业务回调若报错则终止本次客户端运行，不自动重试可能已有部分副作用的回调。这里不承诺任意外部系统或客户端崩溃后的 exactly-once。

### 整场完成

acceptCompleted 先检查绑定代次、Type=completed、消息 Generation，再检查：本地已封口、曾授权合法 end、FinalOffset 等于本地终点且等于 offeredOffset、LastSeq 等于 appliedSeq。所有条件都满足，才记录 completed=true。

合法 completed 本身包含对最终输入已处理完成的声明，不强制依赖此前单独观察到 audio_ack。提交时可把缓存累计确认推进到最终位置，置 inputEnded=true、sendOffset=finalOffset；全部校验必须先于释放。相同当前代通知幂等；与既有完成范围冲突的通知拒绝。

运行器在该提交之后才能发送 completed_ack。ACK 写失败或 detach 不撤销 completed，之后不再为了证明成功而重新 resume。completed=false 时，仅音频全部 ACK、socket 正常关闭或恢复返回 resume_unavailable 都不能替代完整完成判定。

## 错误、解析与运行器边界

建议使用包内哨兵，方便助手测试 errors.Is：

| 错误 | 适用范围 |
| --- | --- |
| errV2ClientState | 当前调用阶段非法，如已 attached 再次 acceptReady、未 attached 处理业务、非连续 commitApplied |
| errV2ClientGenerationMismatch | 事件绑定代次不是当前代；旧事件不能更改新代状态 |
| errV2ClientProtocol | 服务端 Type、身份、消息代次、授权上界、end/完成事实或结果零序号不合法 |
| errV2ClientResultGap | 新结果跳过了本地尚未应用的序号 |
| errV2ClientCompleted | 已记录完成，不再允许新音频派发、end 派发或新的 ready |

音频预算、封口、溢出、缺口及块边界错误沿用已有缓存错误。固定错误说明不得拼入凭据或完整消息。相同结果去重及 completed 幂等按上述规则处理，不能统一被 completed 错误挡掉；appendAudio/sealInput 沿用缓存封口规则。

网络解析后才能调用这些方法。当前协议类型不能独自区分“缺字段”和“合法零值”；后续解码必须验证必需字段、类型和单对象，不能只 json.Unmarshal 到结构体就视为合法。字段/消息读取上限、未知消息、入口错误分类及重试预算属于随后运行器的接线范围。

未来 reader/writer 事件同时绑定本地连接尝试 ID。ready 前还没有新服务端代次，仅凭旧 generation 无法识别迟到 ready；运行器要在调用 acceptReady 前筛选尝试。detach 也只是业务资格变化，旧 socket 关闭、读写共同退出及事件归属清理仍由运行器负责。

## 预先固定的验收场景

开发者先实现一个状态文件，助手随后评审并完成测试。测试串行调用真实缓存与状态，不使用虚假网络成功来替代状态证明：

1. 首次 ready 前已缓存 12 字节，但不能派发；合法 ready 建立身份后，授权到 8 时确认 12 被拒绝，缓存/状态完全不变。
2. 授权音频后，尚无 Write 完成事件即可接纳合法 ACK；授权失败或只读取副本均不被误认为接纳。
3. 已确认 4、曾授权到 12，断开后 ready=8，则下一块从 8 开始；ready=4 则仍从 4 重放，offeredOffset 不回退。恢复到已释放范围、块中间或超前位置均拒绝。
4. 第 1 代之后直接接受合法第 3 代 ready，模拟中间一次 ready 丢失；身份变化、同代/倒退代次及 AckedResultSeq 不一致拒绝。
5. 同一位置的 end 确认可以单独前进；未授权 end 却被报告输入结束时原子拒绝；旧 false ACK 不回退事实，恢复 ready 回退 inputEnded 则拒绝。
6. 空输入、正常封口、end 发送失败后新代重发，以及旧代 detach/ACK/result/completed 不影响新代。
7. 结果 1 应用提交后重复 1 不再次应用，3 在缺少 2 时拒绝；检查但不提交时 appliedSeq 不前进；提交 2 后恢复 ready 必须确认 2。包含 uint64 序号上限无回绕。
8. 完整输入/end 授权与结果位置一致才接纳 completed；无需先观察单独 audio_ack；不匹配的完成通知不得释放音频，重复一致通知幂等。
9. 记录 completed 后模拟确认写失败/断开，完成事实仍存在，后续 ready 被拒绝；仅音频 ACK 或 detach 不会产生成功。

上述是待验证判据，不是已通过结果。实现通过后记录覆盖和具体状态证据；真正的自动恢复成功率、缺口、重放范围及恢复耗时要等网络接线和故障实验。
