# 第六阶段：音频位置与输入结束规则

日期：2026-10-05。状态：设计与实现指导，等待开发者实现。依据 `a0b021b` 和[资源所有权设计](2026-10-05-session-resource-ownership.md)。本步核心由开发者编写，助手在实现后补测试。

## 问题与目标

客户端发送成功不等于 Gateway 已接纳；接纳确认也可能在返回途中丢失。重连后若从最后一次发送位置继续，可能遗漏音频；若直接重发未确认音频，又可能把同一范围重复送入 Worker。

本步定义 Gateway 的连续接纳位置，以及新音频、完整重发、缺口、部分重叠和 end 的处理规则。先实现独立状态，不改动 v1 线格式、不启动网络或缓存，不将本部件通过测试解释为已经完成恢复。

## 标识方案与选择

| 方案 | 优点 | 代价与决定 |
| --- | --- | --- |
| 递增块序号 | 固定分块时简单，容易识别整块重试 | 需要稳定的分块约定；按字节释放缓存、表达音频终点还需额外位置关系；可行但本轮不选 |
| 音频字节偏移 + 实际负载长度 | 直接表达连续音频范围，与现有字节计量一致，不必保存所有历史块 ID | 必须处理溢出、缺口和重叠；本轮采用 |
| 音频时间戳或采样位置 | 与媒体时间直接对应 | 需要额外固定格式、单位和位置换算；当前统一字节流下没有必要 |

不采用乱序缓存或自动裁剪部分重叠：首版要求客户端按序重放，Gateway 只接纳连续的新范围。接受乱序会增加缺口等待、缓存预算和超时规则；裁剪重叠还需处理格式对齐和内容一致性。后续客户端可以从服务端返回的接纳位置准备合法范围。

位置只统计音频负载字节，不包含协议头。它绑定同一逻辑会话及原 Worker 流，不因 WebSocket 换代而归零；新 Worker 流不能直接套用旧位置。具体 v2 编码、格式校验及最大消息长度在协议接入时确定。

## 两种确认不能混用

- **连续接纳位置 nextOffset：** `[0, nextOffset)` 已由逻辑会话接管，客户端可据接纳确认释放对应重放缓存。
- **Worker 处理位置 processedBytes：** 已由 Worker 确认处理的范围，用于处理进度与积压保护。

当前 audioProgress.receivedBytes 在完整读取后增加，甚至包含随后因预算超限未转发的块，因此不能直接改名为 nextOffset。重发字节也不能再次当作新接纳音频累加。本步保持原计量器不变，未来在接入位置上分别处理。

接纳只承诺当前存活逻辑会话已接管数据，不是持久化，也不是 Worker 已经处理；进程/Worker 丢失仍按既定恢复范围明确失败。

## 范围规则

一条音频请求用 offset 与 size 表示半开区间 `[offset, offset+size)`，左端包含、右端不包含。size 必须取实际解码后的负载长度，不能盲目信任客户端另报的长度。记当前连续接纳位置为 N。

判断顺序固定如下，所有错误均不修改状态：

1. size 为 0：errAudioEmptyChunk。空音频不充当心跳或 end。
2. size 大于 math.MaxUint64-offset：errAudioPositionOverflow。先检查再相加，避免回绕。
3. 计算 endOffset；若 endOffset <= N：整个范围已经接纳，返回 audioChunkDuplicate。即使已接纳 end，这类重试仍可确认，但不能重新入队或发送给 Worker。
4. 若输入已结束：errAudioInputEnded。此时所有包含新字节的请求都拒绝，不再细分缺口和重叠。
5. offset == N：audioChunkNew。
6. offset > N：errAudioGap。
7. 其余为 offset < N < endOffset：errAudioOverlap。

例如 N=6400、尚未 end：

| 范围 | 判定 | 状态变化 |
| --- | --- | --- |
| [6400,9600) | 新数据 | 接管成功并记账后 N=9600 |
| [3200,6400) | 整个范围重复 | N 不变，不重复入队 |
| [9600,12800) | 中间缺失 [6400,9600) | 拒绝，N 不变 |
| [4800,8000) | 部分重复、部分新数据 | 拒绝，N 不变 |

“重复”是范围判定，不比较历史内容，也不要求重试范围恰好等于某个原始块。客户端必须保证同一位置代表相同音频；本步没有保存历史 payload 或校验摘要，不能声称能检测相同位置的内容篡改。

## 输入结束规则

end 携带 finalOffset，表示整场音频区间为 `[0, finalOffset)`。

- finalOffset 必须等于 nextOffset。小于或大于都返回 errAudioEndMismatch，不回退也不跳过缺口。
- 首次合法 end 将 ended 置 true，返回 true,nil。
- 后续相同 end 返回 false,nil，表示已确认过；不得重新启动尾部期限或重复半关闭 Worker。
- 初始状态接受 end(0)，表示空输入；是否允许具体业务创建空问诊可在上层另加规则。
- ended 后 nextOffset 不再增加，因而它同时保存已确认的终点，无需再存一份 finalOffset。

acceptEnd 只记录输入结束。未来 Worker 上传任务必须先发送完已经接纳的音频，再执行一次 CloseSend；不能因协调者接纳 end 就跳过待发送的队列。重复 end 不重置整场尾部等待预算。

## 本步 Go 接口

新增 `internal/gateway/audio_input_state.go`，使用以下类型与方法。字段和方法均保留注释；无需构造器，零值就是合法初始状态。

```go
// audioChunkKind 表示一段音频相对连续接纳位置的关系。
type audioChunkKind uint8

const (
    audioChunkInvalid   audioChunkKind = iota // 校验失败时的零值，调用方必须检查 error。
    audioChunkNew                             // 从当前接纳位置开始的连续新数据。
    audioChunkDuplicate                       // 整个范围均已接纳，不得重复送入 Worker。
)

// audioInputState 管理一场逻辑会话的连续音频接纳位置与输入结束状态。
// 零值可用；由会话协调者串行访问，自身不保证并发安全。
// 只记录元数据，不保存音频、不确认网络投递，也不操作 Worker。
type audioInputState struct {
    nextOffset uint64 // [0,nextOffset) 已接纳；也是下一段新音频的起点。
    ended      bool   // 已接纳合法 end；此后 nextOffset 固定为音频终点。
}

// classifyAudio 检查 [offset,offset+size) 是新数据还是完整重发，不修改状态。
// offset 是音频字节起点；size 是实际音频负载长度，必须为正。
// 溢出、缺口、部分重叠和 end 后新增音频均返回 audioChunkInvalid 及原因。
func (s *audioInputState) classifyAudio(offset, size uint64) (audioChunkKind, error)

// acceptAudio 记录该音频范围的接纳结果，内部再次调用 classifyAudio。
// 新数据推进 nextOffset，返回 true,nil；完整重发返回 false,nil；错误不改状态。
// 真实接入时，调用方必须先校验并取得新数据的有界存储/转发所有权，再执行记账。
// 本方法本身不证明数据已保存，也不发送接纳确认。
func (s *audioInputState) acceptAudio(offset, size uint64) (bool, error)

// acceptEnd 校验并记录输入终点；finalOffset 为最终音频字节位置。
// 仅等于 nextOffset 时合法；首次返回 true,nil，相同 end 重试返回 false,nil。
// 错误不改状态；不直接调用 Worker.CloseSend，也不操作尾部计时器。
func (s *audioInputState) acceptEnd(finalOffset uint64) (bool, error)
```

六个包内哨兵错误：errAudioEmptyChunk、errAudioPositionOverflow、errAudioGap、errAudioOverlap、errAudioInputEnded、errAudioEndMismatch。每项注明含义，使用 errors.New 定义，便于上层通过 errors.Is 判断；不在本步增加对外错误码。

classifyAudio 按前述顺序实现。acceptAudio 复用分类结果：错误返回 false,error，重复返回 false,nil，新数据执行 nextOffset += size 并返回 true,nil。acceptEnd 先比较终点，再区分首次/重复。相加只发生在溢出校验成功之后。

## 为什么分检查与记账

如果只提供“检查并立刻推进位置”，下一步有界缓存拒绝数据时就需要回退位置，还可能已经把错误确认发给客户端。本方案让状态机保持简单，将实际接管安排在检查与记账之间：

```text
先校验会话身份、连接代次和生命周期
    → classifyAudio
    → 重复：不入队，返回已有接纳位置
    → 新数据：非阻塞检查预算、取得有界会话缓冲的所有权
             → acceptAudio
             → 发布给唯一 Worker 上传任务，并安排接纳确认
```

取得所有权失败时不调用 acceptAudio、不发成功确认。实际所有者必须保住音频，即使原 WebSocket 随即断开也不能丢失。不可在位置校验前执行不可撤回的 Worker Send。

检查、接管和记账由同一个协调者顺序完成，中间不处理另一个音频/end/换代命令，也不插入阻塞网络 I/O；本步不引入跨协程的“两阶段事务”。重复检查保持 acceptAudio 独立调用时也能验证位置，但不能替代上述所有权契约。纯状态方法可用虚拟范围测试，不需要真的分配对应大小的音频。

## 实现范围与验收

本轮只新增上述核心文件，不给 resumableSession 增加占位字段，不改 v1 upload、audioProgress、控制命令或网络协议。下一步结合有界音频所有者和 v2 协议接入其调用，避免把元数据记账误当成完整恢复。

助手后续测试覆盖：零值与连续接纳；分类无副作用；完整重发不推进；缺口与部分重叠拒绝；空块与最大值溢出边界；首次/重复/错误终点 end；end 后重发允许、新数据拒绝；失败保持全部状态；只检查但尚未接管时位置不变；实例间独立。测试没有 WebSocket 或 Worker，不能据此量化真实重复转发率、恢复率或资源上限；这些指标由后续链路实验给出。
