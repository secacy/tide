# ADR-005: 会话绑定 Worker 与升级前名额预留

Status: Accepted
Date: 2026-09-12
Related: [ADR-004](ADR-004-admission-protection.md), [EXP-007](../experiments/EXP-007-admission-protection.md)

## Problem

Gateway 总会话上限不能表达多个 Worker 各自的容量。若选择 Worker 与增加占用分离，并发接入可能重复占用最后一个名额；若取消时就归还，尚未完成清理的会话会被漏计。

## Requirements

- 单 Gateway、固定 Worker 列表，每个 Worker 配置正整数会话配额。
- 升级前确定 Worker 并预留名额；无名额立即拒绝，不等待、不抢占。
- 一次会话固定一个 Worker；不自动换 Worker 或重放音频。
- 准备、运行和清理阶段都占名额，所有失败路径统一释放。

## Options

| 方案 | 收益 | 代价 |
| --- | --- | --- |
| 仅控制 Gateway 总会话数 | 结构简单 | 单个 Worker 可能超配，无法表达不同容量 |
| 选 Worker 后、建流时再增加占用 | 准备阶段不占 Worker 配额 | 存在选择竞争，升级后可能才发现容量不足 |
| 升级前原子选择与预留，清理后归还 | 拒绝边界明确，容量与生命周期一致 | 准备和清理中的会话保守占用名额 |

## Decision

采用 `WorkerPool` 保存固定 Worker、客户端、配额、预留数及是否允许新预留。`TryAcquire` 在同一临界区内选择并增加占用；不等待容量空位，不执行网络 I/O。返回的 `WorkerLease` 表示一次进程内预留，没有自动过期或续租。

接入先登记 Session，占 Gateway 名额，再预留 Worker；预留失败返回 HTTP 503 并撤销登记。接入流程持有 Lease，Session 只使用选定客户端。升级失败、非法 Start、建流失败、正常结束和异常清理最终都走统一收尾：连接及执行流清理 → 释放 Worker 名额 → 注销 Session。

Lease 的释放并发安全且只生效一次。显式停止某 Worker 接入，只禁止之后的新预留，之前已取得名额的会话继续；它不是自动健康检测。客户端连接由应用组装层创建并在 Gateway 清理后关闭，Pool 不负责关闭共享 gRPC 连接。

选择算法与资源生命周期分离。本决策批准提供轮询和最小预留比例两种显式候选，用实验比较；多 Worker 不设置隐式推荐算法。单 Worker 兼容入口不改变原调用语义。算法的最终推荐由实验和后续确认决定，不写入本记录。

## Invariants

- `0 <= reserved <= capacity`；检查资格、选择和增加占用具有同一个原子边界。
- 停止接入的 Worker 不获得新的 Lease，已取得的 Lease 不被撤销。
- Lease 不向另一 Worker 转移；重复释放不减去其他会话的占用。
- 注册表、Pool 和 Session 控制锁不嵌套；锁内不做网络操作或等待会话退出。
- Worker 预留归还先于 Session 注销，因此 Gateway 排空不遗漏其接入路径持有的预留。

## Trade-offs

配额是本 Gateway 的本地会话账本，不证明远端计算已经停止，也不能协调其他 Gateway 的预留。Start 尚无专用期限，准备连接会占名额；清理较慢也会延迟复用。本次不引入自动健康检测、动态配额、Worker 热增删或跨 Worker 重试。

## Validation

验证并发不超配、重复释放、停止接入与预留竞争、Gateway 总上限与 Worker 上限独立、失败路径释放、会话固定路由，以及 Wait 之后预留归零。EXP-008 比较同容量、不同容量、交错释放下的两种选择策略，并单独观察并发预留的开销；不以调度微基准代替真实模型容量。
