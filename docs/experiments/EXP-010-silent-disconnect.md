# EXP-010: 静默失联检测与心跳候选

Date: 2026-09-12
Commit: `fdfc7b4b4ea82ff7240372b1768e1e7c01977719`
Related: [M6](../MILESTONES.md)、[处理期限语义](../adr/ADR-003-processing-progress-deadline.md)、[预留生命周期](../adr/ADR-005-worker-reservation-lifecycle.md)、[TCP 清理实验](EXP-005-tcp-slow-client-cleanup.md)

## Question / Hypothesis

在音频已经全部处理、客户端暂时没有新输入且没有 End 时，现有期限能否在五秒目标内发现静默失联？独立 Ping/Pong 能否在双向或单向阻断时发现故障，并容忍 500 ms 暂停？如果 Send 同时停滞，实际由心跳还是处理期限先触发取消？

预期无待确认音频的基线不会因处理期限退出。每端独立、固定周期探测可覆盖这个盲区；正常空闲不是故障。检测、RPC cancellation 和注销分别记录，不把五秒检测目标当成无条件清理上界。

本实验只比较候选、记录事实，不确认生产心跳方案。用户已接受有限补齐的恢复方向；十五秒客户端音频缓存、十秒恢复预算仍是后续实验初值，本轮没有实现或验证重放。

## Setup

- 单客户端、单 Gateway、单 Worker，Gateway 与 Worker 会话配额均为一。WebSocket 使用两个 loopback TCP 连接组成的转发链路；Worker 使用真实 gRPC 服务和 bufconn。客户端持续 Read，Gateway 沿用生产 Reader / Sender / download / 协调者。
- 基线与候选都先发送合法 Start、确认 RPC 已建立。除 `idle` 外，再发送一块两字节音频，Worker 先发处理确认、后发 `ack` 回显；客户端收到回显后，Gateway 必然已消费前面的处理确认。这里不模拟音频吞吐或识别质量。
- 两组都在注入前各做一次成功 Ping，验证双方读取和控制帧响应可用；基线随后不再探测。候选双方各自每两秒 Ping，单次发送及等待 Pong 共用三秒预算，最多一个在途探测，不因普通数据到达而重置探测周期。
- 候选是测试侧 watchdog：Gateway 探测失败调用既有 `session.abort()`，客户端调用 `CloseNow()`。生产转发与配置未改动。Go overlay 仅在协调者选出结果后插入观测，记录选择时刻与原因，不修改选择结果。
- 保留生产默认三秒处理期限、五秒 End 期限、两秒结果写入期限和队列配置。`send_stall_isolated` 单独把处理期限改为十二秒，以隔离心跳检测路径；不能将该诊断组当成默认行为。
- 无 race 的正式场景顺序执行，各重复两次。环境为 Go 1.26.5、macOS 26.6.2 arm64、十二逻辑 CPU；源码及 overlay 哈希见原始元数据。没有采集真实模型、跨机器 RTT 或 CPU/GPU 容量。

### 故障模型与场景

转发器每个方向最多在用户态持有 32 KiB，另外还有 OS socket 缓冲。暂停时不再向目标转发已读字节，EOF 也等该方向重新开放后才传播；恢复时保留字节顺序。不制造 RST，不通过立即关闭连接代替静默故障。

这是 **TCP 字节转发暂停**，不是 IP 丢包、TCP 重传或真实半开 socket 的精确仿真。候选 Ping 会实际命中被暂停的方向；无探测的空闲基线可能没有任何字节尝试经过故障点，所以不能声称基线存在已观测的阻塞 Write。

| 场景 | 注入与目的 |
| --- | --- |
| `idle` | Start 后不发送音频；正常空闲对照 |
| `acked_idle` | 全部音频确认后保持健康连接；无文字不应导致误判 |
| `blackhole` | 探测周期开始约 200 ms 后，双向暂停转发 |
| `blackhole_late` | 约 1,200 ms 后双向暂停，验证检测延迟与故障相位有关 |
| `upstream_blackhole` | 只阻断客户端到 Gateway；另一方向仍可传输 |
| `downstream_blackhole` | 只阻断 Gateway 到客户端；另一方向仍可传输 |
| `pause` | 约 1,800 ms 后双向暂停 500 ms，覆盖下一次 Ping，再恢复 |
| `send_stall` | 测试适配器使第二次 Send 等待 RPC 取消，确认进入后双向阻断；保留三秒处理期限 |
| `send_stall_isolated` | 相同故障，处理期限十二秒；区分心跳独立推进与原处理期限的作用 |

每组观察故障后六秒；无故障组观察六秒，暂停组恢复后继续观察六秒。**先保存观察快照，再执行正常 End 或兜底恢复转发与 Abort。** 未在窗口内检测/清理记为未观察到，不能用兜底后的归零倒推自行恢复。

## Metrics / Quality Checks

- 原始事件均为本进程单调时间相对实验起点的偏移。分别保存注入、心跳失败、Gateway 协调者选择、RPC Context 取消观察、Worker 退出、客户端 Read 退出和 handler 返回。
- 汇总的 `gateway_detect_ms` 使用协调者选择时刻，表示会话首次失败判定，不自动证明已经识别网络故障；`kind=7 / processing_timeout` 必须单独解释。客户端使用首次 Ping 失败或 Read 退出。另列双方心跳失败时刻，避免把处理超时或收到对端关闭误称为独立心跳检测。
- RPC 取消通过 Context 回调观察，回调与协调者的调度可能交错；数十微秒量级的差值不能作为严格事件先后的依据。handler 返回代表本接入路径已完成执行流清理、归还 Lease 和注销；远端退出另行等待验证。
- 正常空闲、确认后空闲及短暂停顿统计提前退出情况，保留候选误判。五秒未达标和窗口内无检测都保留，不用零延迟替代缺失值；Go 测试通过表示驱动及清理校验通过，不直接表示检测目标达标。
- 汇总校验原始文件 SHA-256、源码与记录提交的一致性，以及 overlay 只有指定观测插入。沿用子测试墙钟跨度与 Go elapsed 差值绝对值超过一秒则拒绝性能汇总的规则。
- 观察结束后所有组都等待客户端 Reader、Worker 和转发执行流退出，检查 Session 登记与 Worker 预留归零。正常对照还需要收到 final；本轮没有接入恢复探针或重放音频。

## Procedure / Evidence

从仓库根目录运行，依赖已缓存。正式对照需要允许本机临时端口监听；输出路径不可已存在：

```bash
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off PYTHONDONTWRITEBYTECODE=1 \
  python3 scripts/run_disconnect_experiment.py --count 2 \
  --output docs/experiments/results/EXP-010-disconnect.jsonl

PYTHONDONTWRITEBYTECODE=1 python3 scripts/summarize_disconnect_experiment.py \
  docs/experiments/results/EXP-010-disconnect.jsonl \
  --output docs/experiments/results/EXP-010-summary.json
```

驱动与候选：[disconnect_experiment_test.go](../../internal/gateway/disconnect_experiment_test.go)；[运行工具](../../scripts/run_disconnect_experiment.py)；[汇总工具](../../scripts/summarize_disconnect_experiment.py)。保留[原始数据](results/EXP-010-disconnect.jsonl)、[元数据](results/EXP-010-disconnect.jsonl.meta.json)和[汇总](results/EXP-010-summary.json)。正式结果与 race / 开发失败日志分别保存。

## Development Failure: 停用心跳时误关健康连接

早期候选在结束观察后直接取消心跳 Context，并等待探测 goroutine，再发送 End。一次 race 模式检查中，`acked_idle_heartbeat` 在发送 End 时出现 `use of closed network connection`，该批检查随后被中断。

对当前依赖 `coder/websocket v1.8.15` 的实现检查表明：控制帧写入使用 Context 设置取消回调，取消正在进行的写入可以关闭底层 WebSocket。因此“停止探测”不能简单等同于“取消当前探测涉及的所有 I/O”。该失败发生在实验正常收尾阶段，不是 500 ms 暂停造成的误判，也不是 race detector 报告的数据竞争。

修正后的候选分开处理：正常停用只关闭后续探测入口，当前 Ping 在自己的有限预算内结束，join 之后再发 End；真正探测失败才执行现有 Abort / CloseNow。使用 net.Pipe 确定 Ping 已进入阻塞 Write，再分别执行两种停用方式的回归测试，可稳定复现前者关连接、后者完成 Pong 后仍可读取后续消息。

```bash
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -mod=readonly -tags=tide_disconnect ./internal/gateway \
  -run '^TestDisconnect(Gate|HeartbeatStopDuringPing)$' -count=10
```

保留[失败原始日志](results/EXP-010-development/heartbeat-stop-failure.jsonl)、[原始元数据](results/EXP-010-development/heartbeat-stop-failure.jsonl.meta.json)、[对应候选源码](results/EXP-010-development/disconnect_experiment_test.go.txt)及[校验说明](results/EXP-010-development/manifest.json)。这是中断的开发检查，元数据没有完成退出状态，不是正式性能样本。候选源码按原元数据哈希逐字节核对，其余源码可由正式驱动提交取得并核对；定点回归是当前版本的可重复复现入口。

## Results

共 36 个正式样本，九场景 × 两种模式 × 两轮，全部通过源码、overlay、原始数据、时间连续性和最终清理校验，无样本排除。下面的耗时均从故障注入算起，范围表示两轮结果，不是延迟百分位。

| 场景 | 基线观察 | 心跳候选观察 |
| --- | --- | --- |
| `idle`、`acked_idle` | 四次均正常保持，End 后完成 | 四次均正常保持，无提前退出；每端每次至少两次周期 Ping 成功，End 后完成 |
| `blackhole` | 两次在六秒窗内均无失败判定、无 RPC cancellation，登记与预留仍各为一 | 双方约 4,801 ms 发现失联，取消与注销在窗口内完成 |
| `blackhole_late` | 两次均未在窗口内检测 | 双方约 3,800–3,801 ms 发现失联并完成清理；注入更靠近下一次探测，等待更短 |
| `upstream_blackhole` | 两次均未在窗口内检测 | 双方约 4,801–4,802 ms 发现失联，清理完成；另一方向可传输不能掩盖往返失败 |
| `downstream_blackhole` | 两次均未在窗口内检测 | 双方约 4,801–4,802 ms 发现失联并完成清理 |
| `pause` | 两次恢复转发后保持连接，End 后完成 | 两次均未误判，每端各有四次周期 Ping 成功，End 后完成；探测确实命中暂停窗口 |
| `send_stall` | 两次约 3,001 ms 由处理期限判定失败并取消 RPC；客户端尚未发现断开，六秒时登记与预留仍各为一 | 同样先约 3,001 ms 处理超时；约 4,801 ms 心跳失败后客户端退出、handler 注销 |
| `send_stall_isolated` | 十二秒处理期限下，两次均未在六秒窗内判定失败或取消 | 双方约 4,799–4,801 ms 通过心跳发现失联；Send 响应取消退出，完成清理 |

六类故障的十二个心跳候选样本中，双方均在五秒目标内观察到心跳失败，且在兜底前注销；其中两个默认 Send 停滞样本的 RPC cancellation 来自更早的处理期限。另六个心跳健康/暂停对照全部保持连接并正常结束，没有观察到误判，但样本数量不足以估计生产误判率。

十二个基线故障样本中，十个在观察窗内没有会话失败判定，两个由处理期限失败；全部在六秒观察结束时仍占名额。它们随后通过明确标记的兜底清理归零，不能将这种归零计为自行检测或恢复，也不能把六秒观察窗解释为永远无法清理。

默认 Send 停滞的候选组，RPC cancellation 到 handler 返回相隔约 1.800 秒。心跳在此解除的是失联连接的收尾等待，不是使三秒处理取消提前。其余心跳故障组中，RPC cancellation 观察到 handler 返回约 0.09–0.35 ms；远端 Worker 退出独立验证，有些样本稍晚于本地 handler 返回。这些本机小样本不构成资源释放或远端停止的硬期限。

全部三十六个会话最终归还登记与 Worker 预留，客户端 Reader、Worker 及转发器执行流退出。正式样本关闭 race；改动后十八场景 race 检查、定点停用与 gate 测试各十次 race、常规全量 race 回归及带实验标签的 vet 均通过。中断的开发失败样本另存，不混入上述三十六次统计。

## Conclusion / Boundaries

现有处理与 End 期限没有覆盖无待确认音频、未 End 时的静默失联。证据支持进一步采用双方独立的 Ping/Pong 候选：本轮两秒周期、三秒单次预算在两个注入相位、单向阻断和 Send 停滞诊断组满足五秒检测目标，并容忍已测试的 500 ms 暂停。

心跳与处理期限需要并存，且必须保留不同失败来源；处理超时既不能当作网络失联的确诊，也不能直接升级为整个 Worker 的健康结论。正常停止心跳与异常终止连接还需要独立生命周期语义，开发失败及定点回归提供了具体约束。

目前候选由测试 watchdog 在 RPC 已建立后启动，失败时复用现有 Abort/CloseNow。它没有完成生产协调者的心跳事件、关闭原因和 goroutine 所有权接入，也没有覆盖升级后等待 Start、慢建流、持续大量结果写入时的控制帧争用或服务停接竞争。正式方案必须说明探测从何时开始、何时停止，再补充相应回归；不能直接把实验 helper 当作已上线能力。

本轮没有比较应用层 heartbeat、TCP keepalive 或其他探测周期的成本，也没有多连接心跳负载、长尾 RTT、TLS、真正丢包重传及 Gateway 进程退出验证。少量故障相位与零误判样本不能证明所有弱网下五秒必达；五秒仍是检测目标，不是整个会话清理硬上界。

恢复协议、音频缓存、十秒追赶预算、Worker 自动摘除与重新接入均未实现。下一步在对话中确认检测方案及上述生命周期范围，再记录最终 ADR 并实现；有限恢复语义继续后续讨论。
