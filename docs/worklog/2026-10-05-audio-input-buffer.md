# 第六阶段：有界音频接管

日期：2026-10-05。状态：设计与实现指导，等待开发者实现。依据 `269604d` 与[音频输入状态验收](2026-10-05-audio-input-state.md#音频输入状态验收2026-10-05)。核心代码由开发者完成，助手负责测试。

## 为什么需要这一步

audioInputState 能判断连续范围并推进接纳位置，但它没有保存音频。后续发出接纳确认之前，必须有跨连接存活的逻辑会话接管这些字节；否则客户端释放重放缓存后，连接侧任务退出或复用切片就可能丢失音频。

本步新增 audioInputBuffer，把位置状态与有限音频存储组合起来：成功接纳后保存独立副本，按顺序交给唯一 Worker 上传任务，直到发送成功的完成事件被协调者处理才释放该块的预算。全部方法由协调者串行调用，不执行 I/O。

## 方案与取舍

| 方案 | 好处 | 代价与选择 |
| --- | --- | --- |
| 有缓冲 channel | 有现成的并发收发和阻塞语义 | 容量默认按块计，仍需单独维护字节预算、正在发送的块及位置提交；可行，但当前单一协调者无需内部并发队列 |
| 普通 slice FIFO | 入队简单 | 出队需避免持续保留旧引用和不受控的底层数组增长，可能涉及移动/压缩；小队列可行 |
| 固定槽位环形缓冲区 | 槽位数固定，入队和释放队首不搬移已有音频块 | 要维护 head/count 和回绕；本轮采用 |

只限制块数不能约束可变块长，只限制字节数则可能容纳大量极小块。采用 maxBytes 与 maxChunks 两个上限，都是有限正值，不使用 0 表示无限。

数据所有权有两种选择：直接接管调用方切片可省一次复制，但调用方必须永久停止修改并处理所有失败归还路径；复制则使接口更容易正确使用。本轮在检查范围和预算后，用 make(len(payload)) + copy 保存独立副本。成功返回后调用方可修改或释放原切片。后续出现实测瓶颈，再评估所有权转移或内存池。

队列满时采用非阻塞拒绝：offer 返回 errAudioBufferFull，不改变位置、不排队等待、不丢弃已经接纳的旧音频。未来链路接入时将容量耗尽作为明确失败收尾，客户端不能收到成功接纳确认；本步缓冲部件只报告错误，不自行终止会话。阻塞协调者会妨碍到期/停服处理，丢弃已确认数据会破坏恢复承诺。

## 范围、预算与所有权

缓冲区保存已接纳但尚未完成本地 Send 交付的音频。retainedBytes 和 count **都包含正在发送的队首块**；take 只借出队首，不出队、不归还预算。complete 在匹配的发送成功事件到达后才出队、清空槽位并归还预算。

例如 maxBytes=6400、maxChunks=2，两块各 3200 字节已占满。take 第一块后仍按 6400 字节/两块计费，不能因为“已经交给发送协程”就允许额外接纳。complete 第一块后才释放 3200 字节/一个槽位。完整重发无须新增存储，即使队列满也返回 false,nil。

这里的 complete 表示单次 Worker Send 成功后的本地交付完成，不表示 Worker 已处理或已产出结果。现有未确认处理预算仍有独立作用；Send 失败或超时不能调用 complete 当作成功，也不能在不确定是否已接收时重发该块。原 Worker 流失败时依既定范围终止，清理须先等待借用者退出，再释放整个缓冲对象。

payload 副本在借出后仍由缓冲区持有，发送任务只读，不得修改或复用底层数组。上传任务结束对该块的使用后汇报完成，协调者处理 complete。每场最多一个在途块，同一 stream 不并发 Send；take/complete 自身也只能在协调者内调用。

本步能限定的是缓冲区持有的音频字节总和与槽位数。WebSocket 当前输入消息、尚未交付的命令、发送任务/依赖库可能保留的引用、gRPC/操作系统缓冲，以及对象头和分配器开销不由该数字覆盖。它不是 Gateway 总内存或全链路积压的硬上限；接入时仍需入口消息限制、连接准入、发送期限和 Worker 处理预算。

## 文件和数据结构

新增 `internal/gateway/audio_input_buffer.go`，组合现有 audioInputState，不另存一份接纳位置。必须使用构造器创建，使用后不复制对象。

```go
// bufferedAudio 是缓冲区拥有的一块音频。
// data 借给发送任务后保持只读；offset 是该块在逻辑会话中的字节起点。
type bufferedAudio struct {
    offset uint64 // 与 data 一起确定已接纳范围。
    data   []byte // 独立副本，不与 offer 的调用方切片共享底层存储。
}

// audioInputBuffer 组合连续输入状态与有界音频存储。
// 仅由会话协调者串行操作，不提供内部锁，也不执行 Worker I/O。
// 必须通过构造器创建；成功接纳的数据跨连接保留，整场终止后对象不再复用。
type audioInputBuffer struct {
    input         audioInputState // 连续接纳位置和 end 状态，只经本对象改变。
    slots         []bufferedAudio // 固定长度环形槽位，长度即块数上限。
    head          int             // 非空时指向最早尚未 complete 的块。
    count         int             // 已占槽位数，包含在途队首。
    retainedBytes uint64          // 所有已占槽位的 data 长度之和，包含在途块。
    maxBytes      uint64          // 本对象持有的音频字节上限，创建后不变。
    inFlight      bool            // 队首是否已借给唯一发送任务。
}
```

保持以下不变量：0 <= count <= len(slots)，retainedBytes <= maxBytes，inFlight 为 true 时 count 必须大于零，未占用槽位不保留音频引用。各块按起点顺序存储，已完成发送的旧块被释放，但 input.nextOffset 不回退，因此历史重发仍可识别。

## 方法契约与实现顺序

```go
// newAudioInputBuffer 创建空缓冲区，maxBytes 是字节上限，maxChunks 是槽位上限。
// 两者必须为正；非法配置返回 nil 和 errInvalidAudioBufferLimits。
// 构造只分配槽位，不预分配 maxBytes 音频，不启动任务。
func newAudioInputBuffer(maxBytes uint64, maxChunks int) (*audioInputBuffer, error)

// offer 尝试接管 offset 起始的音频，以 len(payload) 为实际长度。
// 连续新数据成功复制并登记后返回 true,nil；完整重发返回 false,nil。
// 范围非法或容量不足时返回 false,error，全部状态保持不变。
// 非阻塞；调用期间调用方不得并发修改 payload，返回后不再持有其底层数组。
func (b *audioInputBuffer) offer(offset uint64, payload []byte) (bool, error)

// take 借出尚未发送的队首并标记在途，不出队、不归还字节或槽位预算。
// 队列为空或已有在途块时返回零值,false。
// 成功返回的 data 只读，不能修改；只允许一个发送任务使用该借用。
func (b *audioInputBuffer) take() (bufferedAudio, bool)

// complete 确认 offset 对应的在途队首已经成功完成 Send，释放该槽位与预算。
// 没有在途块或 offset 不匹配时返回 errAudioCompletionMismatch，不修改状态。
// offset 必须来自 take 结果；发送失败不调用本方法，交由会话失败清理。
func (b *audioInputBuffer) complete(offset uint64) error

// acceptEnd 接纳最终音频位置，沿用 audioInputState 的首次/重复与错误规则。
// 不消耗槽位，不直接 CloseSend；尚未发送的已接纳音频继续保留。
func (b *audioInputBuffer) acceptEnd(finalOffset uint64) (bool, error)

// inputDrained 报告已接纳 end，且全部已接纳音频都已 complete。
// 空队列但尚未 end 时为 false；只表示可准备半关闭 Worker 输入。
// 不表示已执行 CloseSend、Worker 已处理完成或整场会话已清理。
func (b *audioInputBuffer) inputDrained() bool
```

三个新增内部哨兵错误分别为 errInvalidAudioBufferLimits、errAudioBufferFull、errAudioCompletionMismatch，均用 errors.New 并注明含义。音频范围/end 错误沿用已有哨兵，不改错误身份。

### 1. 构造

先校验 maxBytes != 0、maxChunks > 0，再 make 固定长度槽位并保存 maxBytes。其他字段保持零值。配置上限由未来 Gateway 配置入口负责合理设定，本步不指定生产默认值或声称任意巨大配置都能成功分配。

### 2. offer：校验 → 预算 → 拷贝 → 记账与发布

1. size := uint64(len(payload))，调用 input.classifyAudio。错误直接返回；完整重发立即返回 false,nil，优先于容量判断，且不分配副本。
2. 新范围检查 count == len(slots)，或 size > maxBytes-retainedBytes；任一成立返回 errAudioBufferFull。使用减法比较，避免字节求和回绕。等于剩余预算时允许。
3. 创建与 payload 等长的独立切片并 copy。至此方法已拥有音频副本，但尚未对外报告接纳。
4. 调用 input.acceptAudio(offset,size) 登记。错误返回 false,error，局部副本可回收；由于同一所有者刚检查成功、期间不处理其他事件，正常路径必须返回 true。若意外得到 false,nil，用 panic 表达内部不变量破坏，不把它当普通重复请求。
5. 将副本放入尾部槽位 `(head+count)%len(slots)`，count++，retainedBytes += size，然后返回 true,nil。记账到槽位发布之间无 I/O、无外部回调、无并发观察且没有预期失败点。只有整个方法成功返回后，上层才可发送接纳确认或借出新块。

方法不会在拒绝新块后回退其他已接纳数据。成功拷贝不能替代输入消息本身的大小限制，也不应为任意拒绝请求先分配副本。

### 3. take：最多一个在途块

若 count==0 或 inFlight，返回 bufferedAudio{},false。否则置 inFlight=true，返回 slots[head],true。切片头按值返回，但底层副本仍归缓冲区，不能当可修改的独立拷贝。

### 4. complete：成功发送后释放队首

先检查 inFlight、count>0 和队首 offset 匹配；全部通过后扣除该块长度，将 slots[head] 置 bufferedAudio{}，head 前进并回绕，count--，inFlight=false。清空槽位很重要：仅减少 count 仍可能通过底层数组留住旧 payload。

同一 offset 重复 complete 必须拒绝。新音频 offset 严格递增，旧完成事件不能释放新队首。不要改变 input.nextOffset/ended，也不等待网络或写入响应。

### 5. acceptEnd / inputDrained

acceptEnd 直接委托 input.acceptEnd。inputDrained 返回 input.ended && count==0；count 已包含在途块，所以仅 take 最后一块时尚不能视为发送完毕。

inputDrained 是持续为真的状态条件，不是一次性事件。未来上传控制还须记录 CloseSend 是否已执行，确保只半关闭一次。重复 end 不新增队列项，也不重置尾部期限。

## 示例与验收计划

使用 6400 字节、两个槽位：接纳 [0,3200)、[3200,6400) 后满额；take 第一个块依然满额，offer(6400,新数据) 被拒绝，位置保持 6400；重发 [0,3200) 返回 false,nil；complete(0) 后才能接纳 [6400,9600)。环形数组复用槽位，历史位置继续保留在 input 中。例子里的数字只是测试夹具，不是容量建议。

助手在实现后验证：非法配置；首次/连续/重复接纳；原 payload 修改后副本保持不变；字节与槽位两个上限及恰好等额；预算失败后位置/队列均不变；满额重发；take 不提前释放预算且禁止重复借出；错误/重复 complete 不释放新块；FIFO 与回绕；槽位释放旧引用；已发送范围重发不重新入队；end 不占槽位、在途阻止 inputDrained、空输入 end、重复/错误 end；实例独立。测试不执行真实 Worker Send，不声称网络缓冲、恢复率或系统总内存已验收。

本轮只新增该文件，不改现有 v1 upload、Gateway 配置或运行中的控制循环。终止时由未来生命周期拥有者取消并等待发送任务，再释放缓冲对象；本步不添加可随意丢弃已确认音频的 clear 接口。完成独立验证后，再接入唯一上传任务的取块/完成事件和协议确认。
