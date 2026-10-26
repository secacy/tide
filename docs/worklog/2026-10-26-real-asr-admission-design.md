# 真实 ASR：准入、背压与有限并发设计评审

日期：2026-10-26。代码参照 `96100c2`。状态：代码边界已核对，选定本地占用准入方案；新增组件、Gateway 集成和真实并发对照尚未实现。本步没有新增容量或性能数据。

## 本次要解决什么

[上一轮](2026-10-26-real-asr-backend-baseline-results.md)证明固定短音频的正常结束、取消及后续新场符合预期。进入并发前，要明确“网关接纳了一场”“挑中了某个 Worker”“Worker 真正取得模型名额”是三个不同事实。

当前 `MaxSessions` 只限制网关总会话数，[RoundRobin](../../internal/workerpool/round_robin.go) 和加权轮询均只决定分配顺序。两个 Worker 各有一个真实模型名额时，设网关 `MaxSessions=2`，仍会发生：

1. 长会话 L 分到 A，短会话 S 分到 B，两场同时运行。
2. S 正常结束并清理，B 空闲，L 仍占 A；网关只剩一场。
3. 新会话 N 通过总名额检查，轮询下一次恰好又选 A。
4. A 忙而拒绝，B 的空闲名额没有被利用。

这是根据当前选择顺序推导出的场景，尚未作为真实模型实验结果统计。接下来的目标是让 N 可以使用 B，并在本地名额耗尽时立即失败；同时保留远端 Worker 对真实模型资源的最终限制。

## 已有保护到底约束什么

| 位置与配置 | 当前作用 | 不能据此得出的结论 |
| --- | --- | --- |
| Gateway `MaxSessions`，v2 `MaxHandshakes` | 限制逻辑会话及临时握手数；断开后保留的会话仍计数 | 不表示每个 Worker 都有剩余容量 |
| Gateway `MaxMessageBytes` | 限制单条完整 WebSocket 输入，v2 二进制含 8 字节 offset | 不是整场或整个进程内存上限 |
| v2 `MaxAudioBytes` / `MaxAudioChunks` | 约束网关保留的音频，包括在途块；默认 1MiB / 256 块 | 这是存储预算，不是整场累计输入或模型处理积压 |
| `WorkerSendTimeout`，默认 2s | 单次 gRPC Send 阻塞超时后取消原 RPC；等待本地调用实际退出 | Send 返回不代表模型已经收到、解码或处理完成 |
| `MaxPendingAudioBytes` | 有可信处理前缀时，限制接纳量减处理确认量 | 当前真实模型没有 AudioProgress，必须保持 0；不能用文本或 Send 成功伪造进度 |
| v2 `MaxResultBytes` / `MaxResults` | 限制未确认结果字符串/条目；默认 1MiB / 256 条 | 不包括所有协议/网络对象或原生模型内存 |
| `ResultWriteTimeout`，默认 2s | 限制单次客户端输出等待 | 不表示 ASR 推理最多两秒，也不必然终止可恢复的逻辑会话 |
| v2 `InputProgressTimeout`，默认 30s | attached 且未 end 时限制新音频接纳的空闲时间 | 客户端持续送新音频时，这不是模型无进展检测 |
| `TailTimeout`，默认 15s | 首次合法 end 后限制等待正常 Worker 收尾 | 不能保护 end 之前的全部计算过程 |
| `WorkerStatusTimeout`，默认 2s | Send/CloseSend 返回 EOF 后，等 Recv 的真实终态 | 不把上传 EOF 当成功或释放确认 |
| 原生 Worker 单名额 | 从模型创建到 Close 返回独占，忙时拒绝、不排队 | 本地 RPC 取消或网关归零不等于此名额已经归还 |
| 原生 Worker `MaxAudioBytes=960000` | 限制逐块校验并提交的整场原始 PCM，30 秒 | 与 v2 同名字段语义不同；不是 native RSS 或不可中断调用的时间上限 |

源码依据：[配置](../../internal/gateway/handler.go)、[v2 配置](../../internal/gateway/config_v2.go)、[音频缓冲](../../internal/gateway/audio_input_buffer.go)、[协调流程](../../internal/gateway/session_control.go)、[Worker](../../internal/realasr/worker.go)。

网关在 Send 成功后释放该块的应用缓冲预算，gRPC/HTTP2/内核和模型内部仍可能持有数据。实际流控产生阻塞时，发送超时与本地缓冲预算仍能保护相应环节；短流也可能在 Send 从未明显阻塞前就被传输层接纳。因此不能把这些机制等同于模型积压有界或严格实时性保证。模型调用若长期不返回，本地取消只能结束本地 I/O，Worker 仍须保持模型名额，直到原生调用和释放结束。

## 方案比较与决定

### 会话分配

| 方案 | 收益 | 代价与本次取舍 |
| --- | --- | --- |
| 保留轮询，由 Worker 拒绝忙请求 | 改动少，Worker 仍能守住实际名额 | 空闲 Worker 可能闲置；保留为对照组 |
| 本地选择与占位原子化，Worker 仍最终准入 | 能解决同一 Gateway 已知会话的错峰分配；持锁过程没有网络等待 | 本地视图不等于远端真实占用；本轮采用 |
| 先查 Worker 是否空闲再建流 | 看起来能使用实时状态 | 查询后状态仍会变化；查询不等于预留，不能单独解决并发竞争 |
| 增加 Worker 预留/释放协议 | 可以建立更强的远端名额契约 | 需处理预留过期、实例重启、响应丢失及身份；当前不扩协议 |
| 忙时排队或透明换 Worker 重试 | 可能提高部分请求完成率 | 引入等待、重放和重复处理问题；当前有限实验立即失败，不排队、不自动重试 |

**选择“本地占用准入 + Worker 实际准入”两层机制。** 本地名额表示该 Gateway 正在持有的分配；最终模型名额由 Worker 自己决定。每个实验 Worker 的本地上限设为 1，对应当前 Worker 的硬限制；这个数不是实测容量。

本轮限定一个 Gateway 对两个独立 Worker 进程，各配置条目必须对应独立实例。多个 Gateway 各自维护的本地计数不能合成全局容量；不同 ID 指向同一进程也不能凭空增加模型名额。

### 没有可信处理进度时的保护

| 方案 | 判断 |
| --- | --- |
| 从模型适配器提供真实连续处理前缀 | 长期可选；必须证明模型还会依赖哪些输入，不能将 Accept 或暂时无可解码步骤当作处理完毕 |
| 保留现有有界存储、发送/收尾期限及模型输入上限 | 当前采用；逐项陈述保护范围，并继续显式关闭未知进度预算 |
| 用文本更新间隔代替处理进度 | 不采用；正常静音也可能没有文本，文本字数与已处理音频量没有对应关系 |
| 立刻新增总会话超时或通用滞后检测 | 暂不增加；本轮是固定短输入，长问诊如何设置时限需随分段设计决定 |

本轮不会宣称模型处理积压已受控。有限并发实验验证已知条件下的准入和退出；发送被阻塞等失败路径先由可控后端确定性验证，再明确真实模型实验实际触发了哪些保护。

## 本地名额的所有权

选择器提供 `TryAcquire`，一次调用在同一把锁内检查上限、选中 Worker 并增加计数。全部已满时返回 `ErrNoWorkerCapacity`，不阻塞，不创建 goroutine，也不发 RPC。对有本地空额的条目做轮询，避免总是偏向列表首项。

名额由新会话拥有，以下退出规则不能分散在上传、接收等任务中：

| 路径 | 名额处理 |
| --- | --- |
| 新建入口选择失败 | 没有取得名额；按既有入口清理归还 Gateway 总名额 |
| 取得名额后建流或提交失败 | 先取消已创建 RPC、处理已接管资源，再归还本地名额；远端状态仍可能未知 |
| 正常结束 | 首版保留到逻辑会话本地清理结束后归还，包括结果保留/确认阶段 |
| 普通断线、恢复原会话 | 继续持有同一名额与原 RPC；恢复不再次选择或申请 |
| 取消、超时、停服 | 发出停止后等待已启动的本地 I/O 任务退出、释放会话缓冲，再归还 |

首版把名额持有到整场本地清理结束，比“模型已经结束就马上复用”保守，可能在结果保留阶段闲置计算资源；代价换来与现有资源所有权一致的释放位置。暂不拆出第二套释放协调流程。

**本地归还后，远端仍可能忙。** 既有 TCP 测试已覆盖客户端取消返回时原生 Decode/Close 尚未结束；[本轮运行基线](2026-10-26-real-asr-backend-baseline-results.md)中 Gateway 归零的观察也早于模型 Close 观察。新请求此时可能取得本地名额，随后被 Worker 拒绝。这种请求明确失败，不能计为识别完成，不能把整个后端再次标记为“已证实空闲”，也不固定 sleep 十毫秒来猜远端释放。

当前 `StreamingRecognize` 建流成功只表示本地流可用，不证明服务端已取得名额。Worker 忙通常从 Recv 终态体现，可能晚于 WebSocket ready；ready 不是模型准入确认。`ResourceExhausted` 还被用于音频上限等错误，通用业务层不能仅凭这个状态码无条件换 Worker 重放。

## 第一块实现：有本地占用上限的轮询池

先新增 `internal/workerpool/capacity_round_robin.go`，暂不修改 Gateway、现有 RoundRobin 或 WeightedRoundRobin。以下为建议的公开接口和内部状态；注释中的语义属于实现约束。

```go
// ErrNoWorkerCapacity 表示本地配置条目全部被占用；不表示远端实时容量。
var ErrNoWorkerCapacity = errors.New("no local worker capacity")

// WorkerCapacity 配置一个独立后端的本地占用上限。
type WorkerCapacity struct {
    Worker Worker // ID 唯一、Client 非 nil；连接仍由外部拥有。
    Limit  int    // 正数；真实单名额 Worker 本轮设为 1。
}

// capacityEntry 由池独占；使用后配置不可变。
type capacityEntry struct {
    worker Worker // 构造时复制的后端信息。
    limit  int    // 该条目的本地上限。
    inUse  int    // 已取得且未释放的本地名额，始终位于 [0, limit]。
}

// CapacityRoundRobin 支持并发调用；经构造创建，以指针使用，不得复制。
type CapacityRoundRobin struct {
    mu      sync.Mutex    // 覆盖检查、选择、占位和归还；锁内不调用外部代码。
    entries []capacityEntry // 构造时复制，不暴露可变状态。
    next    int           // 下次扫描起点；成功选择后移至选中项之后。
}

// WorkerLease 表示本地占用，不是服务端租约；必须以指针交接，不得复制。
// 由分配器创建；调用者最终负责 Release，释放后不得继续使用该次分配运行 RPC。
type WorkerLease struct {
    worker  Worker      // 该次分配的只读身份与客户端引用。
    release func()      // 私有归还动作，仅更新本地计数，不执行网络操作。
    once    sync.Once   // 防止多条清理路径或并发调用重复扣减。
}

// NewCapacityRoundRobin 拒绝空列表、空白/重复 ID、nil Client、非正 Limit。
// 复制配置，计数从零开始；不连接后端、不启动任务。
func NewCapacityRoundRobin(workers []WorkerCapacity) (*CapacityRoundRobin, error)

// TryAcquire 从 next 开始至多扫描一轮，选择首个 inUse < limit 的条目。
// 检查与加一原子完成；成功返回独占 Lease，失败返回 nil, ErrNoWorkerCapacity。
// 失败不移动 next；只描述本地容量，不等待或查询 Worker。
func (p *CapacityRoundRobin) TryAcquire() (*WorkerLease, error)

// Worker 返回配置值的副本；修改返回值不影响池或占用计数。
func (l *WorkerLease) Worker() Worker

// Release 恰好归还一次本地名额；允许重复/并发调用。
// 调用方保证已完成其负责的本地清理；不关闭 ClientConn，不确认远程释放。
func (l *WorkerLease) Release()
```

实现时，在 `TryAcquire` 的锁内完成扫描和计数加一；归还闭包捕获池及稳定条目下标，并在该池的同一把锁下减一。成功后 `next=(选中下标+1)%条目数`；不要在全满时返回零值 Worker，也不要先返回 Worker 再异步占位。两台 Worker 下 O(N) 扫描足够，暂不增加分片锁、优先队列或负载评分。

下一步由开发者实现这块核心逻辑；助手补并发与所有权测试。通过不代表 Gateway 已使用它，也不产生吞吐收益结论。

## 随后如何接入 Gateway

新增明确的分配接口，不通过可选 type assertion 猜测选择器是否有容量，也不改变旧 Pick 的语义：

```go
// WorkerAllocator 为新逻辑会话申请本地分配；实现必须支持并发调用。
type WorkerAllocator interface {
    TryAcquire() (*workerpool.WorkerLease, error)
}

// NewWithAllocator 使用明确的名额分配器；参数校验与 New 共用内部构造流程。
func NewWithAllocator(ctx context.Context, allocator WorkerAllocator, cfg Config) (*Gateway, error)
```

保留现有 `New(ctx, WorkerSelector, cfg)` 入口，通过显式的无容量适配器保持历史轮询/加权实验语义；适配器返回归还动作为空操作的 Lease，不能宣传其具有每 Worker 上限。两入口共用配置和运行逻辑。接口定义、适配器及 v1/v2 的获取/归还要在集成步成组完成，避免只改一个入口。

v2 在 `prepareV2Worker` 建流前申请 Lease，失败不调用 Worker；`sessionWorker` 在准备成功后接管 Lease。v1 在原 Pick 位置申请 Lease，由 `session.run` 的完整退出路径归还。本地无空额在 v2 入口沿用 `worker_unavailable` 公开类别，内部保留 `ErrNoWorkerCapacity` 供实验区分；Gateway 总会话满继续是 `session_limit`。远端后到的忙错误按实际 RPC/会话失败记录，不伪装成入口拒绝。

## 验证顺序与量化口径

1. 组件层：并发 Acquire 不超过每条目上限；全部占满立即失败；只归还 B 时可再次取得 B；重复释放不多造名额；配置/返回值修改不影响池。测试验证机制，不包装成容量成果。
2. Gateway + 可控 Worker：复现 L/S/N 错峰；验证总准入和池准入两个失败层次；恢复不重新申请；建流/提交失败及取消/停服正确归还；固定阻塞原生 Close 证明本地可复用与远端仍忙可以并存。用事件屏障而不是猜 sleep。
3. 两个真实 Worker：使用预登记的配对场景，对照旧轮询与本地占用选择；再验证两场同时运行时第三场被入口拒绝、不触发新模型创建。条件与分母见[有限并发对照计划](../experiments/real-asr-admission-comparison.md)。

当前已有测试复核通过：原生取消期间仍占名额、发送超时等待实际返回、在途块仍占缓冲预算、固定尾部期限、积压原子接纳及 v2 入口生命周期；使用根模块相关测试的 `-race` 定向运行，没有重新跑模型或产生新性能数字。

本轮设计的验收目标是“空闲条目能被利用、配置上限和失败语义清楚、名额所有权可验证”。真实稳定容量、可信模型处理前缀、长问诊分段及跨 Gateway 全局调度继续保留为独立能力边界。
