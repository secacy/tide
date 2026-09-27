# 第五阶段：多 Worker 新会话分配

日期：2026-09-27。状态：方案与首个实现任务已明确，尚未接入 Gateway。

## 背景与目标

多个 Mock Worker 已能独立启动，具备实例共享的处理名额；Gateway 目前仍只使用一个 ASRServiceClient。下一步在合法 start 校验后、StreamingRecognize 创建前为会话选择一个 Worker，后续音频、进度和识别结果始终使用同一条 stream。不能逐块换 Worker，否则有状态 ASR 的上下文和结果归属会被打散。

目标是建立可解释的新会话分配基线，并保留 Worker ID 供后续会话归属及指标使用；此时不预填容量提升或延迟改善数字。

## 策略选择与取舍

| 策略 | 优点 | 代价或限制 |
| --- | --- | --- |
| 轮询 | 状态少，固定列表下选择次数差至多 1，可建立可重复基线 | 不感知会话时长、Worker 速度及健康状态；不保证活跃数或计算负载均衡 |
| 随机 | 简单，无需轮询游标 | 小样本可能分配不均，实验需处理随机性 |
| 最少活跃会话 | 能随会话退出调整选择 | 必须准确登记、释放与处理建流失败；活跃数仍不是实际计算负载 |
| 加权轮询 | 可表达已知容量差异 | 权重需要依据，不能自动反映实时抖动 |

先实现轮询，后续在同等总处理名额和输入条件下比较更复杂策略。先不抽象通用调度接口：当前轮询不需要释放通知，未来最少活跃会话需要选择与登记的一致性，届时再设计完整获取/释放契约。

## 接入与资源责任（后续小步）

- cmd/gateway 组装固定 Worker 列表，每个 Worker 使用可复用的 grpc.ClientConn 和 ASRServiceClient。启动过程中失败关闭已创建连接；退出时先结束会话，再关闭连接。每个会话只新建 RPC stream，不新建 ClientConn。
- 网关准入与合法 start 校验成功后选一次 Worker。超额请求、非法 start 不消耗轮询选择。选择对象保留稳定 ID，便于后续日志和指标归因。
- 轮询锁只保护选择和游标更新；网络建流、Send、Recv 和会话清理均不在锁内。
- 选中后建流失败按现有 Worker 失败路径收尾，不回退轮询游标、不主动在列表中重试；后续新会话继续轮询。选中并不代表 Worker 健康，建流调用返回成功也不能代替整个流成功。
- 已建立会话固定使用选中 Worker 的 stream；不做会话迁移或音频重放。健康剔除/重试规则另行设计。
- 不增加会话 ID 注册表，已有 sessionTracker 仍负责 Gateway 总准入和清理等待；选择器不承担资源保护或会话生命周期职责。

## 当前唯一代码任务：独立轮询选择器

开发者新增 internal/workerpool/round_robin.go，暂不修改 Gateway、session 或启动配置。这里 workerpool 保存 Worker 客户端列表，不创建连接池、不创建 goroutine、不执行 RPC、不关闭外部连接。

类型：

```go
// Worker 描述可被选中的计算后端；Client 由外部创建并复用。
// ID 用于标识日志和指标中的归属，本身不参与网络寻址。
type Worker struct {
    ID string // 列表内唯一、非空白的稳定标识。
    Client asrv1.ASRServiceClient // 发起 RPC 的客户端，由外部管理连接生命周期。
}

// RoundRobin 在固定 Worker 列表上循环选择，支持并发调用。
// 必须由 NewRoundRobin 构造，使用后不得复制。
type RoundRobin struct {
    mu sync.Mutex // 只保护 next，持锁期间不执行网络操作。
    workers []Worker // 构造时复制的列表，之后不再修改。
    next int // 下一次选择下标，始终处于 [0, len(workers))。
}

// NewRoundRobin 校验并复制列表；保留调用方提供的顺序。
// 空列表、空白 ID、重复 ID 或 nil Client 返回错误；不连接或探测 Worker。
func NewRoundRobin(workers []Worker) (*RoundRobin, error)

// Pick 返回本次选中的 Worker，并推进下次选择位置。
// 每次调用算一次选择，即使后续建流失败也不回退；不判断健康或可用容量。
func (r *RoundRobin) Pick() Worker
```

构造使用 strings.TrimSpace 检查空白 ID，重复检测按原始 ID 精确匹配；浅复制切片及 Worker 值，Client 保持共享引用。Client 不得为 nil，调用者也不得传入包含 nil 指针的接口。成功构造保证列表非空且固定，Pick 不需要返回错误；未来动态摘除时需重新设计无可选 Worker 的错误契约。

Pick 在锁内取 workers[next]，推进并回绕下标，返回 Worker 值副本。优先选择 Mutex 而非原子自增取模：当前临界区仅几个内存操作，有界下标无累计计数溢出，便于后续理解状态一致性；不宣称此处已测得高并发性能。

## 验收与数字化依据

助手补：构造拒绝非法列表；单 Worker 恒定选择；A/B/C 连续选择顺序；修改原切片或返回的 Worker 值不改变内部列表；并发选择无数据竞争，固定 N 次调用的总数准确且各 Worker 被选择次数差至多 1。N 为测试条件，不是会话吞吐。

后续接线另验：非法 start 不分配、同会话多块音频只到同一后端、并发会话无串流、建流失败不回退或意外重试、停止时全部会话及连接清理。负载实验再记录各 Worker 选择数、活跃会话、失败数与延迟；不能仅凭轮询次数均匀断言稳定容量提高。
