# Gateway Worker Lease：接入与所有权验收

日期：2026-10-27。状态：依据开发者委托，已完成核心接入、旧夹具迁移和新增所有权测试；后续[真实两 Worker 对照](2026-10-27-real-asr-admission-results.md)也已完成；下文保留本次接入验收范围。实现依据见[集成指导](2026-10-27-gateway-worker-lease-integration.md)。

## 当前行为

Gateway 现在统一通过 WorkerAllocator 为新逻辑会话取得分配。显式使用 NewWithAllocator 和 CapacityRoundRobin 时，每 Worker 本地占用参与准入；原 New 保留轮询/加权轮询行为，通过不占容量的适配器使用同一条运行路径。两个构造入口共用配置默认值及内部状态创建，构造不消耗选择或名额。

- [旧策略适配](../../internal/gateway/worker_allocator.go)每次只调用一次 Pick，使用 [NewUnreservedLease](../../internal/workerpool/worker_lease.go)；没有赋予原策略新的并发上限。
- [v1](../../internal/gateway/session.go)合法 start 后申请 Lease，失败不建 RPC；取得后持续持有，直到收尾及上传/接收本地任务全部退出。兜底 defer 明确先取消再归还。
- [v2 准备与交接](../../internal/gateway/handler_v2.go)在准备函数局部拥有 Lease；建流、取消或构造失败，集中回滚，先取消原 RPC 再归还；准备成功绑定 sessionWorker，由 startV2 接管。
- 提交失败时，任务尚未启动，只取消并释放已准备资源，不等待未启动任务的 done。提交成功后 runV2Session 等待完整本地清理，撤销注册、归还 Lease，再归还 Gateway 总名额。
- 普通断线、恢复和结果保留继续持有原 Lease；不绑定某代 WebSocket，不提前因握手 handler 退出而归还。sessionWorker 的 Lease 只在装配时绑定，纯部件夹具可没有名额。

本地占用只覆盖本 Gateway 已分配的会话。它不是远端预留，释放不确认原生资源已删除；Worker 继续独立执行实际模型准入。没有新增排队、透明重试或模型处理进度。

## 新增验证

[worker_allocator_test.go](../../internal/gateway/worker_allocator_test.go)通过真实 WebSocket/TCP 及可控流验证：

| 场景 | 依据 |
| --- | --- |
| 构造及兼容入口 | nil/动态 nil 拒绝；构造不申请；默认值与配置错误一致；旧适配只消费一次 Pick，凭证独立且释放幂等 |
| 两层准入 | Gateway 总额满返回 session_limit；总额尚有空位而 Worker 池满返回 worker_unavailable；拒绝不打开额外 Worker RPC |
| 准备失败 | 建流失败、入口取消、服务取消和 Worker 配置失败均取消已创建 RPC 并归还；错误包装保留内部身份 |
| 准备成功 | 入口 context 后续取消不再传播到已交接 RPC；名额继续持有，显式清理后可复用 |
| 提交失败 | 建流返回时使 gate 停止提交，未启动 I/O 的准备资源被回收；总名额与 Worker 名额均恢复 |
| v2 断开、恢复、保留 | 首条音频后断开并恢复，仅一次申请及一个 Worker RPC；完成通知已到但尚未 ACK 时仍占位，确认清理后可复用 |
| v1/v2 停服清理 | 持续阻塞本地 Send 的退出，取消后仍占位、Gateway 仍计数；Wait 不越过阻塞清理，任务真正退出后才归还 |
| v1/v2 错峰分配 | L 占 A，S 占 B；S 清理后 N 使用 B；原 L 不受影响并能完成，全部结束后两名额均可取得 |
| 本地/远端释放间隙 | 实际 realasr.Worker + 可控 Engine 通过 TCP 运行；阻塞原场 Close 后，网关可先清理并归还本地名额。下一请求远端 ResourceExhausted、未 completed、未创建第二个模型、无自动重试；原 Close 真正结束后新场可完成 |

最后一项是可控 Engine 的故障边界测试，不是原生模型硬取消或真实容量测量。错峰路由使用回显后端证明分配和数据隔离，不把测试中的 L/S/N 写成真实 ASR 实验完成率。

旧夹具迁移只改变 newSession 的分配器参数和直接依赖字段；f.pool 等外部观测选择器的计数/顺序断言保留。输入协议、结果处理、期限与预算的旧测试继续检验原行为。

## 验证命令与首次失败

定向新增测试、workerpool 组件 race、根模块 vet/build，以及 sherpa 独立模块普通 race 回归已通过；真实模型测试未显式启用。

```sh
go test -race ./internal/gateway ./internal/workerpool \
  -run 'TestGateway(Allocator|Lease)|TestCapacity|TestGatewaySelector' -count=1
go vet ./...
go build ./...
(cd workers/sherpa && go test -race ./... -count=1)
```

首次根模块完整 race 回归中，既有 `cmd/gateway/TestGatewayStartupCLI/help` 子进程超过五秒预算且未返回输出，完整命令因此退出失败；其余包通过，包括 Gateway 全量 race。保持源码和五秒期限不变，单独执行 `go test -race ./cmd/gateway -count=1 -timeout=90s`，该包完整通过。首次超时未复现，原因未确定，不凭单次复跑解释为已定位或修复了工具问题。没有放宽期限或改写首次结果。

验证覆盖根模块各包（首次完整运行与 cmd/gateway 单独复核分开记录）及 sherpa 嵌套模块，未把测试耗时当作业务延迟。

## 边界及下一步

当前接通的是可明确使用的库入口；cmd/gateway 仍通过原 New 组装已有策略。新本地占用策略尚未增加 CLI 选项，也未默认为现有部署切换。v2 入口公开错误类别保持既有约定，远端建流后才出现的忙错误仍属于会话失败，不伪装成 Gateway 总准入拒绝。

本轮有了使用实际 Gateway 生命周期的本地占用保护，仍没有真实模型调度收益或稳定容量数据。[两个真实 Worker 的对照计划](../experiments/real-asr-admission-comparison.md)是下一块：固定两进程及同输入，核对旧轮询/本地占用策略在错峰新场与超额入口拒绝下的实际表现，分别保留失败和原场是否受影响。
