# 第五阶段：Worker 处理占用与等待观测

日期：2026-10-01
状态：首步方案已明确，等待开发者实现；没有新增性能或容量结果。
相关：[共享处理名额](2026-09-27-worker-processing-capacity.md)、[Gateway 联合观测基线](../../experiments/gateway-observation-baseline.md)

## 为什么继续补这项

EXP-005-03 已用独立采样确认九批计划 1/2/4 场对应的 Gateway 活动峰值和前后零活动状态，但活动会话不等于正在处理的音频块。Worker 可能在接收音频、等待处理名额、模拟处理或发送结果；若只看 Gateway active，负载变慢后仍无法判断是否在争用共享处理能力。

本轮先让已有 processingSlots 提供一致的名额状态快照，随后再设计 Worker 访问方法、进程外查询和采样。仅计数不改变处理延迟、名额上限和音频流程，也不增加任务执行池或待处理音频队列。首步不增加等待时长分布、CPU/GPU 使用率、累计吞吐或容量结论。

## 方案比较与选择

| 方案 | 好处 | 代价与本次选择 |
| --- | --- | --- |
| 仅读取 len(tokens) | 不增加额外计数 | 只能看到物理预占，缺少等待者信息 |
| len(tokens) 加独立 atomic 等待计数 | 更新简单 | 获取/归还与计数交接跨多个操作，读取可能混合时刻；不适合声称一份一致状态 |
| 保留 channel，用短锁维护逻辑占用/等待并返回快照 | 复用既有阻塞唤醒和取消路径，快照字段统一读取 | 每块处理增加短临界区，需要明确交接中的等待口径；本次采用 |
| 改成互斥锁加通知 channel 或显式等待队列 | 状态与调度可以完全统一，能进一步做公平性 | 同时改变调度实现；广播可能造成集中唤醒，显式队列需要取消移除与交接协议；本轮不采用 |

采样一致性不等于无成本；不能预先宣称新增锁无性能影响。后续观测基线会包含这一实现版本，只有另做 A/B 才能量化它的开销。保留 channel 也不承诺严格 FIFO 或无饥饿。

## 指标定义

```go
// processingSlotsSnapshot 是一个处理名额池的值快照。
// 三个字段由同一临界区读取，不包含音频或会话对象。
type processingSlotsSnapshot struct {
    Limit int // 配置的处理名额上限，始终大于零，不是实测稳定容量。
    InUse int // 已登记成功获取、尚未执行有效归还的名额数。
    Waiting int // 因满额登记、尚未完成成功获取或取消收尾的请求数。
}
```

Waiting 只在非阻塞尝试发现满额后登记；容量内立即获取不增加它。等待者已经被唤醒但还未取得记账锁时，或已收到取消但还未完成收尾时，仍在 Waiting 中。因此它描述逻辑等待请求，不是运行时当前阻塞的 goroutine 数，也不是已接收音频字节数。获取成功时在同一锁内执行 Waiting--、InUse++，快照不会看到该登记请求同时属于两个逻辑状态。

InUse 表示名额所有权，包含从成功登记到实际模拟处理开始、以及处理结束到归还登记之间的很短交接区间，不是 CPU 正在执行的线程数。当前 Worker 在 processChunk 内获取并 defer 归还，每条流串行处理，所以这里对应处理音频块的获取请求；不能当成完整 Worker 活跃流计数。

例如 Gateway active=4、Limit=1 时，InUse=1、Waiting=3 是四个块同时争用名额的一种可能状态；InUse=1、Waiting=0 也合法，其他会话可能还在接收音频或发送结果。跨进程采样也不是同一时刻，不能要求 Gateway active 等于 InUse+Waiting。

必须满足：0 <= InUse <= Limit、Waiting >= 0；所有获取请求完成或取消、所有持有者归还后，两项都归零。Waiting 没有由 Limit 给出的上限，仍不构成完整排队或内存保护。

## 本小步代码范围

只修改 internal/mockasr/processing_slots.go，保留 newProcessingSlots(limit)、acquire(ctx) 的已有签名与成功/取消语义。

```go
// processingSlots 保留 channel 名额控制，并维护逻辑占用与等待状态。
// 必须通过 newProcessingSlots 创建，创建后不得复制。
type processingSlots struct {
    tokens chan struct{} // 物理名额，容量固定；只在本类型的方法中操作。
    mu sync.Mutex        // 保护 inUse/waiting 及逻辑状态转换；不跨容量等待持有。
    inUse int            // 已成功登记且尚未归还的名额数。
    waiting int          // 因满额登记、尚未完成获取或取消的请求数。
}

// snapshot 返回同一临界区内的值快照，不暴露内部 channel 或锁。
// 接收者必须是构造成功的非 nil 名额池；Limit 来自固定的 cap(tokens)。
func (s *processingSlots) snapshot() processingSlotsSnapshot

// newRelease 为一次已成功登记的名额创建幂等归还函数。
// 仅在本次 acquire 已增加 inUse 后调用一次；本方法自身不获取或归还名额。
// 返回函数可重复或并发调用，实际归还一次，不得归还后续持有者的名额。
func (s *processingSlots) newRelease() func()
```

newProcessingSlots 继续拒绝 limit<=0，只需创建固定容量 tokens，锁和计数使用零值。ctx 非 nil 仍为 acquire 调用前提。snapshot 不支持 nil 接收者；Worker 关闭限制时 slots=nil，后续 Worker 层必须明确表示“未启用限制/未采集”，不能伪造 Limit=0、InUse=0 表示空闲。本步不改 Worker 或对外接口。

## acquire 的实现顺序

### 立即获取路径

1. 检查 ctx.Err()，已经结束则原样返回 nil,error，不登记计数。
2. 加锁，再检查 ctx.Err()；若在等锁期间取消，解锁返回。
3. 在锁内做一次非阻塞发送：`select { case s.tokens <- struct{}{}: ...; default: ... }`。这里的 default 是关键，满额不能持锁阻塞，否则归还和 snapshot 都会受阻。
4. 发送成功后再次检查 ctx.Err()。若观察到取消，取回刚预占的一个 token、解锁并返回原错误，InUse/Waiting 不变。
5. 未取消则 InUse++，解锁，返回 s.newRelease(),nil。不要增加 Waiting。

### 满额等待路径

1. 上述 default 分支仍在锁内：Waiting++，保存该次调用已经登记等待的事实，然后解锁。
2. 使用原来的可取消 select，在向 tokens 发送与 ctx.Done() 之间等待，期间不持有 mu，不新增 goroutine、轮询或 timer。
3. 若取消分支胜出，加锁 Waiting--，解锁，返回 nil,ctx.Err()；本分支没有取得 token，不能接收 token，也不能减少 InUse。
4. 若发送分支胜出，加锁 Waiting--，再检查 ctx.Err()。
5. 已取消则取回一个刚预占的 token，解锁返回错误，InUse 不增加；未取消则 InUse++，解锁返回 s.newRelease(),nil。

每次因满额登记 Waiting 都必须且只能扣减一次。不要用循环重试时反复递增，也不要在已经扣减的分支另加 defer 再扣一次。取消与名额同时可用时，保留取得后复查取消的行为；成功返回后再发生取消仍由调用方完成处理退出并显式归还。

## 归还与快照

newRelease 每次创建独立的 sync.Once。返回闭包的 once.Do 内：加锁、InUse--、从 tokens 接收一个元素、解锁。这几个操作必须都在同一临界区内，防止新持有者先增加 InUse 而旧持有者还未扣减，出现超过 Limit 的逻辑计数。

此处接收 token 不承担容量等待：调用者已经成功持有一个名额，且所有有效归还/取消回滚都按所有权成对执行，因此池中必有可归还的 token。旧闭包重复调用被 sync.Once 拦截，不能拿走他人的 token。只有“获取名额”的发送可能因为满额而阻塞，必须在锁外执行。

snapshot 加锁读取 cap(tokens)、inUse、waiting 后返回值副本。不要用 len(tokens) 替代 InUse：慢路径中可能有已经物理取得 token、尚未完成逻辑登记的请求，此时仍记在 Waiting。Limit 取 cap(tokens) 无需再维护第二份可变上限。

## 验收计划与下一步

开发者实现，助手补测试：容量内快照、满额后的单个/多个等待者、归还后的等待转占用、等待取消/到期、成功后取消不自动归还、重复与并发归还、取消/归还竞争、并发快照边界和最后归零；继续运行已有名额池与 Worker 测试，验证网络发送/暂停/停读仍不占名额。

同步次序优先使用 testing/synctest 或明确的同步点，不用 sleep 猜测等待状态。计数测试不是吞吐实验；本步没有新增对外查询、采样文件或容量结论。验收后再设计 Worker 层的开关语义与访问入口，随后接外部采样和等待时长观测。
