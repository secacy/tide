# 第六阶段：结果序号、累计确认与有界保留

日期：2026-10-06。状态：实现指导，等待开发者编码及助手验收。依据 `cf27363`、[内部输入/上传协调验收](2026-10-06-session-upload-coordination.md#内部协调组合验收2026-10-06)和[恢复契约](2026-10-04-session-resume-contract.md)。核心由开发者实现，助手负责测试。

## 当前能力目标与后续路径

目标能力是客户端短暂断网后继续原逻辑会话，恢复输入及结果投递。输入接纳、代次隔离、上传派发与取消收尾已经完成内部组合验收；结果方向尚未接入新协调者。

本步把结果序号、累计确认、按位置读取和有限存储一起实现为 resultBuffer。随后接入唯一 Worker 接收任务、协调者事件处理、双向完成/错误裁决与尾部期限；再接连接投递与确认、恢复交接，安排真实断线验证。先确定结果如何保存和释放，才能给接收任务一个明确且有限的数据去向。

## 问题与方案比较

客户端断开时，原 Worker 仍可能产生识别结果。一次 WebSocket Write 返回并不能证明客户端已经应用文本；若立即丢弃结果，重连时可能补不回来。同一个 segmentId 可能依次产生多次 partial 和 final，它表示片段身份，无法单独标识一次投递。

| 方案 | 优点 | 代价及本次选择 |
| --- | --- | --- |
| 断开期间暂停读取 Worker | 少保存应用层结果 | Worker 的处理进度与尾部响应也会停在这条接收路径，底层仍有缓冲；适合明确采用暂停策略的系统，本轮不采用 |
| 按片段只保留最新文本 | 减少频繁 partial 的占用 | 必须定义快照替换、跨片段顺序及客户端补齐规则，不能再按完整事件序列重放；本轮先不采用 |
| 内存保存带序号的未确认结果，累计确认后释放 | 原有文本更新顺序清晰，重发/去重规则直接，可限制字节与条目数 | 客户端确认丢失时会暂时多占用；容量耗尽须明确失败；采用 |
| 持久化结果日志 | 可以进一步支持进程丢失后的查询和重放 | 需要存储、保留及恢复一致性设计，且不能单独恢复 Worker 模型上下文；超出首个同实例恢复范围 |

本次保存每一条识别结果，包括 partial 与 final，不进行按片段覆盖。未来若改为最新快照方案，需要连同恢复协议及验证标准一起调整。

## 序号与确认规则

每场逻辑会话独立从序号 1 开始，0 表示尚未应用任何结果。只有一条结果成功进入有界缓冲才分配序号；满额失败不消耗序号。断开、恢复、缓冲排空都不能重置序号。达到 uint64 最大值后拒绝继续追加，不回绕。

- lastSeq：最后成功保存的结果序号。
- ackedSeq：客户端已经连续应用、且 Gateway 已接纳确认的位置。
- 缓冲内容：恰好是 `(ackedSeq, lastSeq]` 的完整有序结果。
- 投递游标：连接写任务已推进到哪里，由后续连接协调维护，不能与 ackedSeq 混用。

客户端必须先按顺序应用结果，才报告累计确认 K，含义为 `[1,K]` 都已应用。累计确认可以一次跨过多个结果；相同或更旧确认是幂等操作，不倒退位置。确认超过 lastSeq 为错误，不能释放任何条目。

Worker 的 processed_audio_bytes 是另一条独立的处理进度，不进入识别结果序号。现有协议中 progress 与文本字段互斥；这项校验和处理进度保护将在接收任务接入时沿用。is_final 只表示片段定稿，不表示整场完成。完成通知及终态保留另行定义，不用空文本结果充当完成事件。

例如缓冲有 1～6，已确认到 2，则保留 3～6。读取游标 4 的下一条为 5；确认到 4 后释放 3、4，只保留 5、6。读取、准备发送或发送成功均不释放条目。

恢复声明低于 ackedSeq 表示客户端需要的某些结果已被清理，返回重放缺口。高于 lastSeq 表示声明了 Gateway 尚未生成的结果，返回位置越界。若客户端已应用到 6，但确认在网络中丢失，Gateway 仍保留 5、6，则恢复时可依据经过校验的 6 释放这两条，不必重复应用。

“旧 ACK 幂等”和“恢复游标过旧是缺口”是两件事：不能仅调用 acknowledge 就判定恢复可行。未来附着操作需要先校验恢复位置，再按协议接纳确认和安装候选连接。

## 数据结构与接口

新增 `internal/gateway/result_buffer.go`，使用内部值类型，不提前绑定 v2 JSON 线格式。

```go
// retainedResult 是一次识别结果更新，seq 在逻辑会话内唯一。
// 字符串只读；返回值可供后续单个写任务暂时持有。
type retainedResult struct {
    seq       uint64 // 投递序号，从 1 开始，跨连接代次保持连续。
    segmentID string // Worker 的片段标识；多个序号可以属于同一片段。
    text      string // 此次更新文本，保持 Worker 原值。
    isFinal   bool   // 此片段是否定稿，不表示整场识别完成。
}

// resultBuffer 保存尚未获累计确认的连续识别结果。
// 必须由构造器创建，通过指针使用；全部方法由唯一协调者串行调用。
// 不启动任务、不做网络 I/O；使用后不得复制或跨 goroutine 并发访问。
type resultBuffer struct {
    slots         []retainedResult // 固定长度环形槽位，构造后不扩容。
    head          int              // 最早未确认结果所在槽位。
    count         int              // 当前保留条目数。
    retainedBytes uint64           // 当前 segmentID 与 text 的 UTF-8 原始字节总数。
    maxBytes      uint64           // 字符串负载字节上限，构造后不变。
    lastSeq       uint64           // 最后成功保存的结果序号；清空后仍保留。
    ackedSeq      uint64           // 已接纳累计确认位置；只前进、不回退。
}

// newResultBuffer 创建空缓冲；maxBytes/maxResults 必须为正。
// maxResults 决定固定槽位数；只预分配槽位，不预分配文本负载。
// 非法预算返回 nil 和 errInvalidResultBufferLimits。
func newResultBuffer(maxBytes uint64, maxResults int) (*resultBuffer, error)

// append 接管一条识别更新的字符串副本，成功返回新 seq。
// 容量不足或序号耗尽返回 0 和错误，全部原状态保持不变。
// 不按 segmentID 去重，不因 isFinal 清理；空字符串也允许进入，仍占一个槽位。
// 业务字段是否合法由后续 Worker 响应解析层判断，本部件保留原值。
func (b *resultBuffer) append(segmentID, text string, isFinal bool) (uint64, error)

// acknowledge 接纳客户端连续应用到 seq 的累计确认。
// 新确认释放相应前缀并返回 true；seq<=ackedSeq 返回 false,nil。
// seq>lastSeq 返回 false,errResultSequenceAhead，不修改状态。
// 调用方须已完成会话凭据、当前代次及投递范围校验，不能直接接受外部任意 ACK。
func (b *resultBuffer) acknowledge(seq uint64) (bool, error)

// peekAfter 返回 after 之后紧邻的一条保留结果，不改变状态或释放预算。
// after==lastSeq 返回零值,false,nil；after<ackedSeq 返回 errResultReplayGap；
// after>lastSeq 返回 errResultSequenceAhead。错误时返回零值,false,error。
// 返回值的字符串保持只读，ACK 清理缓冲后仍可安全读取已经取得的值。
func (b *resultBuffer) peekAfter(after uint64) (retainedResult, bool, error)
```

内部哨兵错误及含义：

| 错误 | 含义 |
| --- | --- |
| errInvalidResultBufferLimits | 字节或条目预算不为正 |
| errResultBufferFull | 追加新结果会超过字节或条目上限 |
| errResultSequenceExhausted | lastSeq 已到 uint64 最大值，不能再分配 |
| errResultSequenceAhead | 确认或读取位置大于 lastSeq |
| errResultReplayGap | 读取位置低于已清理的累计确认位置 |

上层可用 errors.Is 分类。满额的返回本身不取消 RPC；未来协调者收到它后按明确的不可恢复结果积压失败结束会话，避免静默丢旧结果或用无限队列接替存储。

## 实现顺序与不变量

始终保持：

```text
0 <= ackedSeq <= lastSeq
uint64(count) == lastSeq - ackedSeq
0 <= count <= len(slots)
retainedBytes <= maxBytes
第 i 个有效槽位的 seq == ackedSeq + 1 + uint64(i)
```

append：

1. 先检查 lastSeq 是否已达 math.MaxUint64，再检查 count 是否已满。
2. 按 `maxBytes-retainedBytes` 得到剩余字节预算，分别比较 `uint64(len(segmentID))` 和 `uint64(len(text))`；第一项通过后再减去它比较第二项，避免先用 int 累加长度或做可能溢出的总量加法。
3. 预算检查通过后，使用 strings.Clone 分别取得两个字符串副本，避免短子串使较大的原字符串长期被保留。已核对本地 Go 1.26.5 strings.Clone 的副本契约。不要持有整个 protobuf 响应指针。
4. 计算新序号 lastSeq+1 及环形尾部，填入记录，再更新 count、retainedBytes、lastSeq。检查失败不能修改任何位置或消耗序号。

acknowledge：

1. seq>lastSeq 拒绝；seq<=ackedSeq 幂等返回。
2. 需要清理的条目数为 seq-ackedSeq，按照不变量必定不超过 count。
3. 从 head 逐条减去两个字符串的字节数，槽位置为 retainedResult{}，推进 head 并减少 count。
4. 最后将 ackedSeq 更新为 seq。即使全部清空，也保留 lastSeq/ackedSeq 的累计值，下次追加使用 lastSeq+1。

peekAfter：

1. 校验过旧和越界位置；after==lastSeq 时直接返回无结果，避免对最大序号加一。
2. 下一条在保留区的相对下标为 `after-ackedSeq`。有效范围已校验后再转换成 int，用 head 和槽位长度求环形下标。
3. 返回该槽位的值副本。这个方法不移动 head、不记为已发送、不确认，也不一次分配全部重放结果。

这些操作是串行的；累计确认的清理耗时与本次释放条目数成正比，上限由 maxResults 限制。它不提供无成本的大批量清理，也不需要为这项当前有界操作引入第二个所有者。

## 内存、投递与接入边界

字节预算计量 segmentID/text 的原始字符串字节；固定槽位控制元数据条目数。JSON 转义后的大小、协议头、gRPC 解码中的当前响应、已借给连接写任务的字符串及底层缓冲不属于 retainedBytes。后续接入必须限制单条响应/消息尺寸、接收事件及在途写任务数量，再验证完整链路资源界限。

读取后不清理，重复读取同一 after 返回同一条记录。结果字符串不可修改，ACK 只解除缓冲自身引用；已经交给写任务的值依然可读，其引用要由写任务结束时释放。后续协调者维护有限的在途写入，不能无限取结果后堆在另一队列。

外部 ACK 还要经过凭据、附着代次与投递范围检查。投递完成与客户端 ACK 可能并发到达，投递范围的记账点需要放在任务交付处，不能仅在 Write 返回后推进；本部件只检查已生成序号范围，不自行宣称完成这些连接层校验。恢复请求的位置也要在同一个协调处理段验证并交接，不能在注册表外并发调用缓冲方法。

本步暂不修改 v1 wsprotocol、Worker protobuf 或既有 download。新 resultBuffer 不包含 Worker 接收、结果写入、连接代次和完成状态；这些行为随后接入现有唯一协调者，避免出现两个并行状态拥有者。

## 验收计划与本轮交付

开发者实现 result_buffer.go，保留上述类型、字段及方法说明。助手在代码完成后补以下测试：

- 配置非法、独立实例及空缓冲；首次序号为 1，读取无结果不改变状态。
- 同一 segmentId 的 partial/partial/final 保留三个连续序号；多片段交错保持原顺序。
- 条目和字节恰好满额、分别超限、多字节文本按 UTF-8 字节计量；失败不消耗序号或修改缓存。
- 读取不释放，可反复读取相同结果；确认前条目一直占预算。
- ACK=0、重复/旧确认幂等、跨条目累计确认、确认越界不变；清空后追加序号不重置。
- 多轮环形复用，每次检查 FIFO、槽位清零、负载计数和连续序号不变量。
- 读取游标在 ackedSeq/lastSeq 边界、过旧缺口、超前错误；ACK 丢失时的模拟恢复位置处理。
- math.MaxUint64 边界不回绕；最后一条可读取和确认，后续追加明确耗尽。

关键验收例：在客户端已确认 2、Gateway 保存至 6 时，读取 4 的下一条必须为 5；确认 4 后只保留 5、6；若再声明从 3 恢复，必须报告缺口。它验证的是恢复所需的结果连续性规则，尚不是网络恢复率或耗时数据。

本次只记录设计与实现指导，未新增核心代码、测试结果或性能指标。真实 Worker 接收接入后，至少验证“断开期间继续积累结果 → 有效确认释放预算 → 恢复按原序号重放”的组合行为，再推进网络恢复。
