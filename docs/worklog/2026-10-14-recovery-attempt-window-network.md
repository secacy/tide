# 第六阶段：滑动恢复预算真实网络组合验收

日期：2026-10-14。承接[客户端实现](2026-10-14-recovery-attempt-window.md)，本步只改测试与文档。真实 WebSocket Gateway 和 TCP gRPC Worker 均走生产接口，Worker 为记录实际输入与结果的可控替身，客户端/网关/Worker 在同一进程。

## 为什么做这一步

客户端可控时钟测试已经验证计数和时间边界；还需要确认切断真实连接后，恢复仍接回原场，音频和结果位置不受预算策略改变影响，耗尽后连接及原 RPC 能释放。当前不从长时负载开始：先用小输入及屏障准确核对每个字节和结果，避免负载抖动遮蔽协议问题；后续量化对照才采用固定采集节奏。

复用已有多轮代理，增加不可变的“丢弃每次恢复 ready”故障选项。旧构造入口默认关闭该选项，累计预算的原测试及预期保持不变。丢掉完整 ready 后关闭 socket，不制造残缺消息，不合成服务端入口错误。每次实际转发握手前等待旧 Gateway handler 返回及 coordinator detached，排除 session_busy 竞争；此屏障让失败原因可控，也意味着本步不能推断自然网络下的恢复延迟。

## 固定场景与核对

统一 4 字节/块、可区分的 PCM 输入，共 32 字节可供本场发送；只有分散场景发送全部输入并结束。通过唯一 PipeReader 记录实际采集，通过原 RPC 记录完整字节顺序及处理量，通过客户端回调记录连续且只应用一次的结果。代理在指定 offset 丢一块并关闭连接；恢复握手尚未进入 Gateway 时继续采集一块。

- 分散：MaxAttempts=1、AttemptWindow=300ms，三次断线，成功后核实两个新结果，再保持连接 600ms 才触发下一次。单轮恢复期限 2s，服务端断线保留 1s。
- 密集：MaxAttempts=2、AttemptWindow=30s，三次断线，前两轮各接回并处理新音频，第三轮直接次数耗尽。整个测试保护期限 5s，近期记录不会在测试中到期。
- 握手失败：MaxAttempts=2、AttemptWindow=30s，首轮断线后，Gateway 两次接纳恢复并生成 ready，代理都丢掉 ready 后关闭连接。客户端未接纳恢复成功，但两次启动均占额度，最终耗尽。

三类各重复三次 race 验收：

| 场景 | 核实次数 | 启动尝试 / 客户端接纳（每场） | 采集 / Worker 处理（B） | 已应用结果 | 结束结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| 分散断线 | 3/3 | 3 / 3 | 32 / 32 | 9（含唯一尾部） | 完成位置 32，completed_ack 实际写出 |
| 密集成功后再次断线 | 3/3 | 2 / 2 | 24 / 20 | 5（无尾部） | ErrV2RecoveryExhausted，非 DeadlineExceeded，无第三次尝试 |
| 两次恢复 ready 丢失 | 3/3 | 2 / 0 | 12 / 4 | 1（无尾部） | ErrV2RecoveryExhausted，非 DeadlineExceeded，无第三次尝试 |

两种失败分别区分已采集未处理和未采集：密集场为 [20,24) 4B、[24,32) 8B；握手失败场为 [4,12) 8B、[12,32) 20B。只把前者称为已采集音频缺口，不把尚未采集的后缀混入。失败场无假 completed 或 end；分散场 Worker 输入逐字节相同、位置连续，客户端结果 seq/text 和尾部逐条匹配，没有把重复传输算成重复识别。

九场都核对一次 Worker pick、一次原 RPC，源 Close 一次；代理连接数分别为 4、3、3，与初始连接加实际尝试一致。Worker 自然退出、活动会话/握手/注册项/代理 handler 回到零，均发生在兜底取消前；父测试在非并行子测试 Cleanup 后另外核实 TCP ClientConn Close 与 Serve 实际退出。失败后原场等待服务端自己的保留期限，并不因客户端返回立刻释放；本步不测量释放延迟。

代码：[新增网络验收](../../internal/gateway/client_v2_attempt_window_test.go)、[复用代理与故障选项](../../internal/gateway/client_v2_repeated_recovery_test.go)。日志只写业务计数及清理事实，不输出恢复 ID/token。

## 验证与修正记录

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -json ./internal/gateway \
  -run '^TestV2ClientAttemptWindowGateway$' -count=3 -timeout=60s
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -json ./internal/gateway ./internal/wsclient -count=1 -timeout=5m
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go vet ./internal/gateway
```

首轮三次重复共九场的业务及清理断言全部通过，但计划输入字段包含未使用的 4B 备用内容，写成 36B，与分散场的完整输入 32B 不一致。检查日志后把输入字面量缩为 32B，实际发送步骤及每场字节/结果断言不变，再执行三轮九场，全部通过。初轮日志保留，最终验收使用修正后的日志；没有业务失败后替换运行。最终定向三轮共 12 个父/子测试节点通过，九场结构化业务记录核实；两包完整 race 回归 1745 个节点通过、19 默认实验/辅助入口跳过，另含三场新网络场景，无数据竞争报告，Gateway vet 通过。[原日志与验证摘要](../experiments/results/recovery-attempt-window-network-2026-10-14/README.md)保留初轮、最终三轮及两包回归输出，三个压缩日志均读回逐字节一致，源码与日志摘要已核对。

本步证明三个可控短场景的组合正确性，不能包装成生产完成率、网络性能或长问诊稳定性。小块、人工放行、无真实 ASR；精确窗口边界仍由前一步可控时钟测试证明。九场 9/9 是预期行为核实，业务完整完成只有分散组三场，另外六场应当明确失败，不能合并宣称恢复成功率 100%。

## 下一步

先预登记相同十分钟音频与六次低频断线下的整场累计/滑动窗口对照：固定默认五次、单轮期限、回看窗口、完成与音频缺口判据，复用现有采集/独立分析工具，必要时增加配置及策略标记。测试证明规则可用，正式对照再回答该固定负载下的实际完成行为与资源表现；当前不提高默认次数，不重复堆剖析，也不据此进入生产容量结论。
