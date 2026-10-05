# 第六阶段：会话命令入口与最小控制循环

日期：2026-10-05。状态：设计与实现指导，尚未实现。依据 `62646be` 和[注册表验收](2026-10-05-session-registry.md)。

## 问题与方案选择

注册表支持多个 handler 查到同一对象，但 resumeState 约定由一个协调者串行使用。两个恢复请求同时到达时，需要只有一个能把 detached 改为 attached，另一个得到明确拒绝；断开、关闭与恢复也要遵循同一套顺序。

| 方案 | 收益 | 代价与当前选择 |
| --- | --- | --- |
| 每会话 Mutex 包住恢复状态方法 | 状态操作很短时简单，不必增加循环 | 未来 Worker 事件、连接交接和计时器仍需统一所有权；可行，但本轮沿用已约定的协调者结构 |
| 命令 channel + 单个协调者循环 | 所有控制事件经过同一处，易于整合后续 Worker 退出与计时器 | 增加每会话控制循环和请求/回复语义；本轮采用 |
| 在注册表锁内直接恢复 | 表面上可串行 | 所有会话共享全局临界区，职责耦合；不采用 |

本步用无缓冲命令 channel。发送成功意味着协调者已经接收请求，不在会话内部另设待处理队列。它不能限制外部等待发送的 goroutine 数，真实握手准入及重试预算仍须后续接入。每个请求有容量为 1 的独立回复 channel，让协调者发出结果时不必等待调用方被调度。

## 请求取消与提交点

调用有两个阶段，必须明确区分：

1. **交付前：** 调用方可以因 ctx 取消或控制循环结束而返回，请求若未交付则没有状态变化。入口先检查已经取消的 ctx，避免把早已取消的请求交付出去。
2. **交付后：** 调用方等待该请求的唯一回复，不再因 ctx.Done 或 controlDone 抢先返回。协调者在处理前检查会话生命周期 ctx 和请求 ctx；已观察到会话停止则返回 errResumeClosed 并退出，已观察到请求取消则回复请求错误而不执行命令。

如果取消发生在检查之后，状态操作仍可能成功，调用方必须收到成功结果。取消是停止请求的通知，不能撤销已经发生的状态改变。后续连接交接必须遵循此规则：恢复成功后即取得该代次的责任，若连接同时断开，必须用该代次报告断开或终止，不能因为原请求 ctx 已取消就丢弃成功结果。

交付后的路径仅包含 ctx 检查、获取当前时间、状态操作和一次缓冲回复，不执行网络 I/O、等待其他 goroutine 或耗时清理。因此该阶段通常很短，但不承诺在任意调度或进程故障下满足调用方 deadline；若未来该路径需要阻塞工作，应重新设计提交/结果查询或回滚协议。

## 本步数据结构

新增 `internal/gateway/session_control.go`。扩展 resumableSession，新增字段在构造成功时初始化，构造器本身仍不启动 goroutine：

```go
commands    chan sessionControlCommand // 无缓冲，多个调用方提交，唯一控制循环接收。
controlDone chan struct{}              // 控制循环退出后关闭；不代表网络资源已清理。
```

命令结构与含义：

```go
// sessionControlKind 表示控制循环支持的内部操作。
type sessionControlKind uint8

const (
    controlResume sessionControlKind = iota // 尝试恢复连接，成功返回新代次。
    controlDetach                          // 报告某代次连接断开。
    controlClose                           // 结束恢复资格并退出控制循环。
)

// sessionControlCommand 表示一次不可复用的内部控制请求。
type sessionControlCommand struct {
    kind       sessionControlKind        // 本次操作。
    ctx        context.Context           // 请求取消信号，仅在处理前检查。
    generation uint64                    // detach 的连接代次，其他命令忽略。
    reply      chan<- sessionControlResult // 独立、容量为 1，循环只发送一次。
}

// sessionControlResult 是某次控制操作的确定结果。
// 零代次用于失败或非恢复操作；detached=false,nil 表示无需改变状态。
type sessionControlResult struct {
    generation uint64 // resume 成功时的新连接代次。
    detached   bool   // detach 是否实际将当前连接改为断开保留。
    err        error  // 状态错误、请求取消或控制循环终止原因。
}
```

命令和回复不包含恢复凭据。连接 handler 后续先验证身份再调用内部入口；命令入口不是公开的免鉴权接口。

## 方法与契约

```go
// runControl 由生命周期拥有者恰好启动一次，串行处理控制命令。
// ctx 属于逻辑会话生命周期，不能绑定任意一条客户端连接。
// now 在处理命令时取当前时间，生产传 time.Now；不得阻塞。
// ctx 和 now 必须非 nil。循环退出前关闭恢复资格，再关闭 controlDone。
// 本步不操作网络、注册表或准入，也没有自动到期计时器。
func (s *resumableSession) runControl(ctx context.Context, now func() time.Time)

// submitControl 提交命令并等待唯一回复，支持多个调用方并发调用。
// ctx 必须非 nil。交付前可取消；交付后必须取得明确处理结果。
// 循环已结束时返回 errResumeClosed。不得持有注册表锁调用。
func (s *resumableSession) submitControl(
    ctx context.Context, kind sessionControlKind, generation uint64,
) sessionControlResult

// requestResume 请求恢复；成功返回新代次，失败返回零和原因。
// 成功后即使 ctx 已取消，调用方也必须负责该代次的连接交接或断开报告。
func (s *resumableSession) requestResume(ctx context.Context) (uint64, error)

// reportDetach 报告指定代次断开，false,nil 表示旧代次或重复通知被忽略。
// 使用独立的控制请求 ctx，不能直接使用已因断连取消的连接 ctx。
// 交付失败时调用方仍负责重试或终止，不能静默丢失断开事件。
func (s *resumableSession) reportDetach(ctx context.Context, generation uint64) (bool, error)

// requestClose 请求结束恢复资格并等待控制循环退出。
// controlDone 已关闭时立即返回 nil，可重复调用。
// 尚未交付时允许 ctx 取消；成功交付后按确定结果收尾。
// 返回 nil 表示控制循环已退出，不表示 Worker 或网络资源已清理。
func (s *resumableSession) requestClose(ctx context.Context) error
```

生命周期拥有者负责保证 runControl 只启动一次，也不直接并发读写 s.resume。测试构造后可设置初始状态，但循环启动后只能经命令操作；需要断言状态时先等待 controlDone。身份与指针字段继续保持不变。

## 实现顺序与控制规则

1. 先定义类型，在 newResumableSession 成功返回前创建 commands 和 controlDone。commands 永远不由发送方关闭，也不需要关闭；控制循环是 controlDone 的唯一关闭者。
2. 编写 submitControl：先检查 ctx.Err，再创建容量 1 的回复 channel；select 等待 commands 发送、ctx.Done 或 controlDone。发送分支成功后只读回复 channel，禁止再次 select 请求取消/循环退出。失败分支返回零值字段及相应错误。
3. 编写 runControl：defer 中依次执行 s.resume.close() 和 close(s.controlDone)。select 等待会话 ctx.Done 或一条命令。收到命令后先检查会话 ctx.Err，再检查 cmd.ctx.Err；每个已接收命令都必须得到且只得到一次回复，即使会话正在停止。
4. 会话 ctx 已取消时，给已接收命令回复 errResumeClosed 后退出；仅请求 ctx 已取消时，回复该错误后继续。检查通过后不再以取消覆盖已执行命令的结果。
5. controlResume 调用 s.resume.resume(now())，原样返回结果；若返回后状态已是 resumeClosed，先回复再退出。包括过期恢复进入 closed 的情况。代次耗尽目前保留 detached，由后续到期或明确关闭收尾，沿用既有状态语义。
6. controlDetach 调用 s.resume.detach(cmd.generation, now())，返回布尔结果；旧代次或重复通知不是控制错误。controlClose 先关闭恢复资格，回复成功，再退出。未知 kind 返回新增哨兵 errInvalidSessionControlCommand，保留状态并继续循环。
7. requestResume/reportDetach 是薄包装。requestClose 先非阻塞检查 controlDone；已关闭则返回 nil，否则提交 close 命令。成功或 errResumeClosed 时继续等待 controlDone 并返回 nil；其他错误返回原错误。保证成功的关闭调用观察到循环退出，而不仅是命令已接收。

控制循环选择到请求与会话停止同时就绪时，仍有处理前的生命周期检查；但取消也可能发生在检查后，本轮以协调者实际处理顺序为准，不承诺分布式事件的绝对先后。结果回复是该次请求的结论，另一条命令可能随后立即改变状态，调用方不能将成功回复解释为状态永远不再变化。

## 首步限制与紧接着的工作

本步只实现并发控制通路，不接 WebSocket/Worker。恢复命令会检查当前期限；没有新请求时，detached 暂时依靠显式关闭或生命周期取消结束。**自动到期计时器是下一步必须补齐的内容，在补齐前不接入真实断连保留。**这也意味着本步验收不能称为完整的过期清理。

之后把恢复期限计时器加入同一个循环，再定义连接资源交接、停服和错误后的清理所有权。任何真实 I/O 都应通过独立工作协程及事件反馈进入协调者，不能直接塞入已承诺快速回复的控制命令处理段。

## 助手后续测试

由助手使用同步信号和可控 now 完成：无循环时请求超时；预取消不改变状态；同时恢复仅一个成功；旧代次断开忽略；交付后取消仍取得明确回复；协调者处理前观察取消时拒绝；关闭/恢复竞争不遗留等待者；生命周期取消结束并拒绝后续命令；重复关闭；恢复发现到期后退出；代次耗尽处理；非法命令不会破坏状态。并发测试不得读写运行中的 resumeState，也不靠 sleep 猜测请求处理阶段。

测试中的 now 可以在确定调用点用信号同步以构造交付后取消，但生产 now 必须立即返回。定向 race 验证控制正确性，后续真实恢复实验另测恢复率、音频缺口、重复结果及耗时。
