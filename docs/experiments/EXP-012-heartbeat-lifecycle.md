# EXP-012: 生产心跳的启停与业务收尾

Date: 2026-09-13
Commit: `7575ff216a3303504e9354d6fee600ba9c6c4dad`
Related: [ADR-008](../adr/ADR-008-websocket-heartbeat-lifecycle.md), [EXP-010](EXP-010-silent-disconnect.md), [EXP-011](EXP-011-heartbeat-interval.md)

## Question / Hypothesis

独立心跳接入生产生命周期后，等待 Start、慢建流和 Send 停滞能否取消退出？正常 End、在途 Ping、业务期限和 Abort 并发时，能否保持原有完成及失败语义？

预期 Reader 与建流解耦，心跳覆盖准备阶段；正常停用不取消在途 Ping；业务期限先取消 RPC。若在途 Ping 也失败，应中断连接并保留已经选定的业务失败，避免继续等待关闭握手。

## Setup / Metrics

这是故障与生命周期回归，不是 benchmark。公共 Monitor 使用 `testing/synctest` 验证两秒周期、三秒预算及单探测语义；真实网络测试使用缩短的 1–20 ms 周期和 100–1,000 ms 预算，使失败可快速重复。不能把这些预算或测试总耗时换算成生产检测 P95。

- Gateway：单会话、单 Worker 配额；bufconn 或 net.Pipe 的 WebSocket，受控建流/Send 和真实 gRPC Mock；检查取消原因、操作退出、handler 返回、登记和预留归零。
- Client：真实 `Client.Run` 与 loopback WebSocket 服务。服务端持续 Read 但故意不回 Pong，分别覆盖仍在发送和已发送 End；另在 Ping 等待期间发送 final/1000，验证正常关闭不被重新归类为失败。
- 完整链路：实际 Client → loopback Gateway → gRPC/bufconn Mock，16,000 B PCM，按实时节奏发送，双方心跳同时启用，检查正常完成和名额归还。
- 收尾竞争：net.Pipe 确认 Ping 已进入阻塞 Write，再停用或 Abort；不是用 sleep 猜测是否存在在途 I/O。
- 环境：Go 1.26.5、darwin/arm64、12 逻辑 CPU；正式回归开启 race。精确环境、命令及源码哈希保存在元数据。

## Development Failure: 停用后的探测失败仍追加关闭等待

早期接入已能停止后续心跳、先取消业务 RPC，但等待在途探测返回错误后仍进入原有关闭握手。定点场景设置处理预算 100 ms、心跳周期 20 ms、探测预算 200 ms；对端不读取控制帧。测试先确认 Send 因取消退出，再等待 handler；在 700 ms 观察窗内 handler 仍未返回。该失败没有证明最终一定耗时五秒，只证明探测已经失效时仍可能追加关闭等待。

修正为：join 得到探测错误时先 `CloseNow`，正常完成候选转为失败；若已有处理等业务失败，保留该原因。健康在途 Ping 成功时仍走原有关闭握手。这样既不靠停用时取消 Ping 破坏健康连接，也不向已失败连接继续等待通知。

保留[失败日志](results/EXP-012-development/cleanup-before.jsonl)、[元数据](results/EXP-012-development/cleanup-before.jsonl.meta.json)和[修正前 session.go](results/EXP-012-development/session-before.go.txt)。该样本退出码为 1，不混入修正后统计。元数据中的全部源码哈希已核对：以本页 Commit 重建时，只有 `internal/gateway/session.go` 需要使用保存的修正前文本覆盖；其他源码逐字节一致。可用 Go `-overlay` 将该路径映射到保存文本，再执行元数据中的定点测试，预期失败；不要覆盖正式样本。

## Procedure / Evidence

正式回归命令（原始结果文件不覆盖）：

```bash
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -mod=readonly \
  ./internal/wsheartbeat ./internal/wsclient ./internal/gateway \
  -run '^Test(Normalize|FromEnvironment|Monitor.*|GatewayHeartbeat.*|GatewayOpeningWorkerProcessingDeadline|Client.*)$' \
  -count=10 -timeout=2m -json > /tmp/heartbeat-lifecycle-repeat.jsonl
```

保留[正式原始日志](results/EXP-012-heartbeat-lifecycle.jsonl)、[命令、源码与结果哈希](results/EXP-012-heartbeat-lifecycle.jsonl.meta.json)和[汇总](results/EXP-012-summary.json)。源码哈希与记录 Commit 一致，原始文件 SHA-256 匹配；三个包均通过，十五个顶层测试各通过十次，无失败和排除。父测试与子测试不重复计数。

| 验证 | 结果 |
| --- | --- |
| 等待 Start / 慢建流 / 空闲 / Send 停滞，无 Pong | 每场景十次，心跳取消与清理通过 |
| 慢建流时继续 Read，End 后仍探测，停接只拒绝新请求 | 十次，保活后 final/1000 正常完成 |
| 慢建流时音频处理期限仍有效 | 十次，建流被取消，返回 1013 并清理 |
| 在途 Ping 阻塞时处理期限先取消 RPC | 十次，取消不等待 Ping join，保留处理超时原因 |
| 处理失败后 Ping 也失败 | 十次，不依赖对端关闭响应，在 700 ms 回归观察窗内释放 |
| 真实阻塞 Ping Write 上正常 Stop / Abort | 每分支十次，前者保持健康连接，后者中断 I/O |
| 客户端在发送中 / End 后主动检测失败 | 每分支十次，返回可识别的心跳错误 |
| 客户端 Ping 等待中收到 final/1000 | 共 200 次正常完成，服务端 handler 全部退出 |
| 实际 Client / Gateway / Mock | 十次正常完成，Gateway Wait 后预留归零 |

公共组件另验证首个周期、单探测共享预算、重复 Stop、父取消不撤销在途 Ping、禁用及配置默认/覆盖/拒绝。全量带 `tide_disconnect tide_load tide_tcp` 标签的 race 和 vet 检查另行执行，不混入上述正式定点结果。

## Conclusion / Boundaries

回归支持将候选落入生产生命周期：检测不依赖 Worker Send/建流推进，正常停用与强制中断分开，业务取消早于等待收尾，探测失败后不继续等待关闭握手。

EXP-010/011 的原始数据仍属于对应候选版本；当前候选驱动显式关闭新生产心跳，避免叠加两套探测。复现历史数字须使用其记录版本。本轮没有重复完整黑洞/单向阻断的生产默认周期负载，也没有测生产容量、WAN/TLS 或真实恢复性能。

健康但不发 Start 的连接仍可持续占用名额，心跳不是 Start 期限。输入 `io.Reader` 必须可返回，不能由 Context 中断任意阻塞 Read。当前失败只结束本次会话，不重连、缓存、重放或去重；M6 的有限恢复语义仍待下一项设计。
