# 第六阶段：可恢复客户端与有界音频重放首步

日期：2026-10-08。状态：独立缓存已实现并通过部件验收；v2 客户端协调、网络接线及自动恢复待实现。

## 能力目标与实现顺序

服务端已有公开 v2 新建/恢复、音频累计确认、结果重放、完成确认、输入进展保护和稳定入口错误码。下一组能力是让同一存活客户端在断线后主动接回原逻辑会话，明确重放范围及失败原因。

当前 internal/wsclient 的 Client.Run 使用 v1：发送任务读取并复用一个缓冲区，Write 返回成功后继续读取；接收任务没有 ready、audio_ack 或结果序号处理，也没有跨连接缓存。直接在 Run 外层重试，无法从不可回退的音频源重建未确认内容。

按以下顺序推进：

1. 本步实现独立有界音频缓存，验证位置、确认释放、原块重放、封口与资源所有权。
2. 随后设计并接入独立 v2 客户端协调状态和网络运行器，统一管理 ready/ACK、结果连续应用与去重、完成事实、固定代次、源结束、旧连接实际退出和重试总预算；分可验收小步推进。
3. 使用真实 Gateway/可控 Worker 做自动恢复组合验收和故障实验，记录原始分母、重放范围、缺口、重复及恢复耗时。

先保持既有 v1 Client/负载实验入口可用；v2 配置与运行入口在接线时独立设计。本步的缓存是纯部件，不启动连接、goroutine 或 Timer。

## 为什么先做缓存

客户端发送一块音频后断线，有两种可能：Gateway 尚未接纳；Gateway 已接纳，但 audio_ack 未到客户端。Write 成功只说明本次本地写调用成功，Write 失败也不能证明对端没有接纳。

因此客户端需要保存未确认音频，在同一会话恢复 ready 中取得 Gateway 的 nextOffset，再从该处重放。只有来自正确身份/代次且通过范围校验的确认，才允许释放音频。不能用 Worker 处理进度或 socket 写完成替代 Gateway 接纳确认。

例：原块依次为 [0,4)、[4,8)、[8,12)，本地已缓存到 12，只收到确认 4。缓存仍保留 [4,12)。恢复 ready 返回 8，说明第二块也已接纳，可以释放 [4,8)，从原第三块 [8,12) 重发。若 ready 返回 12，则没有待重发音频；后续仍需处理 end 与结果完成。

## 方案与取舍

| 方案 | 收益 | 限制 / 决定 |
| --- | --- | --- |
| 依赖文件 Seek，断线后重新读取 | 文件模拟器容易实现 | 麦克风流通常不能倒回，也不能据此验证真实的有限缓存耗尽；不作为通用恢复契约 |
| 保存整场音频 | 任意历史位置都可重放 | 长问诊使内存随时间增长；本轮不采用 |
| 固定窗口覆盖最旧音频 | 内存有上限 | 可能覆盖仍未确认的数据，恢复时出现缺口；需要额外的丢弃范围管理 |
| 有界保存未确认的连续音频，确认后释放 | 容量、可恢复范围和释放依据清楚 | 长断网可能耗尽缓存，必须明确失败；本轮采用 |

存储采用固定槽位的环形队列，约束音频字节数和块数。整块确认后释放，保留原发送分块。任意字节环形缓存也可实现，但需要处理切片跨环、部分块释放和重新分块；当前协议按完整消息接纳音频，原块重放更容易验证。

容量耗尽时 append 返回明确错误，不覆盖未确认音频、不暗中等待。未来实时采集路径在该错误上停止本次恢复并报告缓存耗尽；若选择让采集等待，就会改变实时输入模型，必须另行说明。模拟实时输入时应让采集计划独立于网络写入，否则断网也暂停读取，会掩盖缓存耗尽场景。

## 位置与边界规则

缓存保存半开区间 `[ackedOffset,nextOffset)`：

- ackedOffset：已经验证并提交到缓存的 Gateway 连续接纳位置，也是仍可重放的最早位置。
- nextOffset：客户端已经复制进缓存的连续音频末端，是下一块本地新音频的起点；不表示这些字节已经发送。
- sealed：本地音频源正常结束后封口，此时 nextOffset 固定；不表示 Gateway 已接纳 end。

缓存确认范围只证明音频已交给存活的原 Gateway/Worker 链路，不提供持久化或跨实例恢复保证。

首版保持每块音频的原始起点和长度，初次发送及恢复重放均发送整块。Gateway 按完整消息接纳，因此合法 audio_ack/ready 的音频位置应落在本客户端原块边界或当前 nextOffset；块中间的位置应拒绝，不能随意切块来迁就错误响应。以后若支持动态分块，要一起调整这个契约。

普通累计 ACK 与恢复位置的判断不同：重复/旧 ACK（n <= ackedOffset）是无操作；恢复 ready 若 n < ackedOffset，则所需音频已经释放，必须报告缺口。恢复处理必须先调用 checkReplayOffset，通过全部身份、代次和范围验证后再 acknowledge，不能只调用“忽略旧 ACK”的方法。

缓存只能验证 `n <= nextOffset`。后续协调者还必须验证 `n <= offeredOffset`：offeredOffset 是已经授权交给唯一写任务的最高连续音频末端，在实际 Write 前登记，允许 ACK 先于写完成事件被观察到。未发送的离线采集数据不能被服务端提前确认；写调用成功位置也不能代替这个授权上界。身份、代次、原 end 授权与恢复 ready 的 inputEnded 一致性由后续协调者统一验证，本缓存不保存这些状态。

## 类型与方法指导

新建 `internal/wsclient/audio_replay_buffer.go`，仍属于 wsclient 包，暂不接入 Client.Run。

```go
// replayAudioChunk 描述原始分块和逻辑字节位置。
// 缓存内部保存独立副本，copyChunkAt 返回另一份归调用方所有的副本。
type replayAudioChunk struct {
    offset uint64 // 本块在本场音频中的起点。
    data   []byte // 非空负载；同一逻辑范围的音频内容必须保持一致。
}

// audioReplayBuffer 由未来客户端协调者串行访问。
// 通过构造器创建，使用后不复制；自身不保证并发安全。
type audioReplayBuffer struct {
    slots       []replayAudioChunk // 固定槽位，不随断网时间扩容。
    head        int               // 最早未确认块所在槽位。
    count       int               // 当前保留的块数。
    maxBytes    uint64            // 缓存拥有的音频负载字节上限。
    ackedOffset uint64            // 已提交的累计确认位置。
    nextOffset  uint64            // 已缓存末端；也是下一次 append 的起点。
    sealed      bool              // 本地输入已封口，禁止追加新音频。
}

// newAudioReplayBuffer 创建空缓存；两项上限必须为正。
// 仅预分配槽位，音频随 append 分配；非法配置返回 nil 和配置错误。
func newAudioReplayBuffer(maxBytes uint64, maxChunks int) (*audioReplayBuffer, error)

// append 复制一块新音频，成功返回其逻辑起点并推进 nextOffset。
// 调用期间 payload 不得被并发修改；返回后不持有其底层数组。
// 空块、封口后追加、位置溢出或预算不足均拒绝，失败时所有状态不变。
func (b *audioReplayBuffer) append(payload []byte) (uint64, error)

// checkReplayOffset 检查能否从 offset 继续原块发送，不修改任何状态。
// 小于 ackedOffset 是音频缺口；超过 nextOffset 或落在块中间是非法位置。
// offset == nextOffset 合法，表示当前没有待发送音频。
func (b *audioReplayBuffer) checkReplayOffset(offset uint64) error

// copyChunkAt 复制从 offset 开始的完整原块，不出队、不推进确认位置。
// offset 必须通过 checkReplayOffset；到达 nextOffset 返回零值,false,nil。
// 成功返回值的 data 归调用方，ACK 释放缓存不会影响这个副本。
func (b *audioReplayBuffer) copyChunkAt(offset uint64) (replayAudioChunk, bool, error)

// acknowledge 提交累计接纳位置，返回确认是否前进。
// nextOffset <= ackedOffset 返回 false,nil；新位置须是合法块边界。
// 完整校验后才释放前缀并清空槽位引用；错误不修改任何状态。
// 调用方已验证消息身份、代次、写入授权上界等协议条件。
func (b *audioReplayBuffer) acknowledge(nextOffset uint64) (bool, error)

// seal 标记本地正常输入结束，返回固定最终位置；允许重复调用。
// 不释放未确认音频，不证明 end 已发送或被 Gateway 接纳。
func (b *audioReplayBuffer) seal() uint64
```

错误使用包内哨兵，便于 errors.Is 分类：

| 错误 | 含义 |
| --- | --- |
| errInvalidReplayBufferLimits | 字节/块数上限非法 |
| errReplayBufferFull | 新块超过字节或槽位预算 |
| errReplayBufferSealed | 封口后追加 |
| errReplayAudioEmpty | 空音频块 |
| errReplayOffsetOverflow | 累计字节位置将溢出 |
| errAudioReplayGap | 所需重放位置早于已释放前缀 |
| errInvalidReplayOffset | 位置超前或落在原块中间 |

实现时保持以下不变量：

1. `ackedOffset <= nextOffset`；有效槽位从 ackedOffset 开始连续覆盖到 nextOffset，保留字节数可由 `nextOffset-ackedOffset` 得到，无需重复维护计数。
2. `0 <= count <= len(slots)`；字节和块数预算都包括已发送但未确认的音频。
3. append 先检查 sealed、空负载、`size > MaxUint64-nextOffset`、槽位和剩余字节预算，再复制、提交。预算比较使用减法避免溢出。
4. acknowledge 先完整验证目标边界，再逐块清空已确认前缀的槽位、更新 head/count/ackedOffset；一次 ACK 可以释放多块。
5. copyChunkAt 可以线性查找有效槽位。首版优先保证边界与所有权正确，未来依据负载证据考虑索引；不提前声称这是高吞吐最优实现。
6. seal 后仍可确认和重放。源读取异常应由未来协调者终止，不调用 seal 将异常当作正常 EOF。

## 为什么 copyChunkAt 返回副本

未来网络 Write 可能仍持有音频，而 ACK 已先到协调者。如果直接借出缓存底层切片，再因 ACK 清空槽位并重新使用预算，实际存活音频可能超过声称的缓存上限；复用数组还会造成内容被改写。

首版选择返回独立副本，并约束运行器同一时刻至多持有一个待写/在途副本，待实际写入任务返回后才准备下一个。另一方案是缓存维护借用标记，ACK 前进后仍保留在途块的物理预算，等写完成再释放；可以减少复制，但需要额外处理“已确认仍被借用”的状态。本轮先采用副本，后续通过实验决定是否优化。

maxBytes 限制缓存自身拥有的音频，不等于整个客户端内存上界。客户端接线时还要计入一块待写副本、组帧临时数据、音频源暂存、服务端消息读取上限与底层网络缓冲；禁止积攒无界的待写副本队列。本步只建立缓存局部预算，不能用它宣称全链路内存已有严格上限。

## 后续客户端的约束预告

音频捕获、发送授权、Gateway 接纳和结果应用分别记录。发送失败后保留未确认数据；旧连接/写任务实际退出后才启动新代，成功恢复使用原身份，并按 ready 校准确认和发送游标。重放不推进采集时间轴，也不重复计算“新采集字节”。

重试预算将使用明确的绝对截止时间；不能每次拨号、收到 busy 或收到 ready 就无条件重置。需要区分“已接回连接”和“积压已追平”，以免通过频繁短暂连接永远保持恢复。具体周期、退避和完成判据在协调者设计时固定。

现有 source io.Reader 不能保证响应 context 取消；网络运行器设计时必须同时明确源的关闭/取消契约，避免断网失败后源读取 goroutine 一直不退出。本步纯缓存不引入源任务。

## 验收安排

原计划开发者实现上述一个文件、助手负责评审与测试；随后开发者明确委托助手完成本步，助手补齐已有骨架及测试。验收使用指定字节序列，覆盖：

- 非法预算、空块、封口、uint64 溢出及失败无状态修改。
- append 独立复制；copyChunkAt 返回副本，外部修改不会污染缓存；确认后旧副本仍有效。
- ACK 不到时字节/块数任一预算触顶就拒绝；ACK 跨多块释放，环形槽位与字节预算可以复用，已释放槽位不再持有音频。
- 重复/旧 ACK 不回退；确认超前/块中间位置拒绝且不先释放部分数据。
- 恢复位置落在保留起点、原块边界、末端时可用；早于保留起点明确缺口，块中间/超前明确非法。
- ACK 丢失情景：原确认 4，恢复 ready 的已验证位置为 8，只释放相应前缀，得到完整第三块 [8,12)；既有全部块大小/内容保持一致。
- 空输入 seal、重复 seal、封口后继续 ACK/重放，以及释放至末端后的空缓存。

本步只验收部件正确性，不作为网络自动恢复或容量实验结果。后续网络故障实验才统计计划采集/缓存/发送授权/实际接纳范围、重放字节、重复送入 Worker 字节、结果重复应用、不可恢复缺口以及恢复和追平耗时。

## 实现与验收（2026-10-08）

按明确委托补齐 [audio_replay_buffer.go](../../internal/wsclient/audio_replay_buffer.go)，新增 [audio_replay_buffer_test.go](../../internal/wsclient/audio_replay_buffer_test.go)。部件没有接入既有 v1 Client.Run，不启动任何网络或后台任务。

实现沿用固定槽位环形队列：append 在全部校验后复制并提交；位置查询只接受原块边界；累计确认完整校验后才清空前缀槽位；seal 固定正常输入终点。copyChunkAt 返回独立副本，源数组、缓存和返回副本互不共享。查找复杂度为 O(保留块数)，一次累计确认的释放成本与被确认块数成正比；首版没有索引或借用状态。

两次测试首次运行均通过，未发生需要修正的测试失败：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/wsclient -run '^TestAudioReplay' -count=1 -timeout=30s
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./internal/wsclient -count=1 -timeout=60s
```

- 新增 10 个顶层测试、38 个叶级场景，38 通过、0 失败、0 跳过。定向运行只调用纯缓存；包回归另包含既有客户端真实回环 WebSocket 测试，共 25 个顶层、76 个叶级场景，76 通过、0 失败、0 跳过，无数据竞争报告。没有重复执行全项目回归。
- 字节预算为 6、槽位预算为 4 时，两块 3 字节音频已占满字节预算，追加 1 字节被拒绝；字节预算为 100、槽位预算为 2 时，同样两块占满槽位，追加被拒绝。两个场景均检查原块未被覆盖，读副本和旧 ACK 不释放容量，确认前 3 字节后可继续追加原位置 6 的新块。
- ACK 丢失情景保留原块 `[0,4)`、`[4,8)`、`[8,12)`：确认 4 后，已验证的恢复位置 8 释放第二块，仅返回完整第三块 `ijkl`；恢复位置 12 释放全部。位置 4 随后作为旧 ACK 是无操作，作为恢复起点则明确缺口。
- 3 槽位、9 字节预算下执行 256 轮回绕，共 1280 次追加及相应释放，并验证 256 次满槽拒绝。使用独立普通切片作为内容/位置参考；每次变更后检查连续覆盖、预算与空槽无引用，已返回副本在确认和槽位复用后保持原内容。此处是确定性正确性验证，不是长时负载实验。
- 块中间及超前的累计确认不释放任何前缀；封口不丢未确认数据，之后仍可确认及重放；接近 uint64 最大位置时可精确到达上限，再追加明确溢出且状态不变。

测试数表示覆盖规模，不表示恢复成功率或吞吐提升。maxBytes 仅约束缓存自身保留的负载；返回副本、槽位/对象开销、分配器及网络资源不属于该字节预算。race 回归也不改变单一所有者契约，缓存仍不支持并发调用。

下一步设计并实现 v2 客户端协调状态，组合身份/代次、发送授权上界、ready/ACK 校准、结果连续应用与完成判定；再接入受总预算约束的网络运行器与自动重连，最后进行真实断网组合验收及故障实验。目前没有自动重连、恢复耗时或全客户端严格内存上界的结果。
