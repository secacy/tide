# 同一会话多次断线：测试实现与验收

日期：2026-10-12。承接[第六阶段检查点](2026-10-12-stage-six-review.md)。**当前状态：新增真实网络三个场景、五轮 race 及相关两包回归均通过。**

## 为什么补这一组测试

已有正式恢复实验每场只有一次断线。客户端 `MaxAttempts` 限制整场累计恢复 Dial，成功接回不清零；已有模拟时间测试验证这一规则，但还需要核对真实 Gateway、WebSocket 和 TCP gRPC 组合：接回后实际处理新音频，再断线时是否继续沿用原场及其位置。

本步不修改预算规则。先验证已有契约，再根据延长观察决定是否适合长时问诊；“每次断线很短”不表示整场可以无限次恢复。

## 方案选择

| 方案 | 特点 | 选择 |
| --- | --- | --- |
| 只补客户端状态替身测试 | 模拟时间与事件容易控制，已覆盖累计预算 | 无法补足真实网络组合证据 |
| 按固定 sleep 时间反复切连接 | 实现简单 | 可能上一轮尚未接回，混淆一轮握手重试与多轮断网 |
| 在新音频结果应用后丢下一块并切断，暂扣下一次握手 | 每轮有明确的业务进展及输入范围，便于检查重放、位置与次数 | 本步采用；屏障控制的正确性测试，不作恢复性能测量 |

新增 [client_v2_repeated_recovery_test.go](../../internal/gateway/client_v2_repeated_recovery_test.go)。测试复用公开 v2 Gateway、TCP gRPC 监听与客户端实现；专用 Worker 记录实际收到/处理的字节、合法输入结束和 RPC 实际退出。代理只转发协议消息、暂扣恢复握手、丢指定音频块并关闭真实 socket，不改内部会话状态。

## 三个预定场景与判据

| 场景 | 累计预算 / 断线轮次 | 已验证行为 |
| --- | --- | --- |
| within_budget | 3 / 2 | 接回两次，沿用原 RPC，24 字节完整处理，6 条音频结果及 1 条尾部结果，正常完成 |
| last_attempt_then_complete | 2 / 2 | 第二次、也是最后一次允许的恢复成功后，继续传输并正常完成，不因预算已用完立即终止 |
| next_outage_exhausted | 2 / 3 | 前两次接回；第三次断线后不再 Dial，明确 recovery_exhausted，已应用位置为 5，原场随后自然到期清理 |

每块四字节且内容不同。第一块结果应用后，代理丢弃 offset=4 的下一块并切断；恢复握手尚未进入 Gateway 时再读取 offset=8 的新块。放行后要求补齐两块并观察两条新结果，才触发第二轮，丢 offset=12，再在握手期间采集 offset=16。成功场最后发送 offset=20 并正常结束；失败场第三轮丢 offset=20。

失败场的预定输入为 28 字节，实际采集 24、Worker 处理 20，已采集未处理 `[20,24)` 四字节，尚未采集 `[24,28)` 四字节。这个范围由可控输入及 Worker 事实核对，不声称客户端产品已能精确诊断任意崩溃的缺口。

共同检查：同一源只关闭一次；所有恢复使用同一 ID/token，握手 appliedSeq 随业务应用推进；ready 代次 1→2→3，接纳位置按各轮预期推进；初始 start、Worker 选择、RPC 均恰好一次；两次恢复后没有回退新建；音频逐字一致、结果序号/文本连续且无额外应用。Worker、代理 handler、Gateway 会话/握手/注册表必须在夹具兜底清理前归零。

源使用 io.Pipe 和事件屏障。它证明断线期间继续读取及缓存，**不验证固定采集节奏、长时间运行、恢复耗时分布或群体竞争**。独立测试期限为五秒，服务端保留一秒，客户端每轮恢复窗口两秒；这些是短行为测试配置，不是生产推荐。

## 首次检查及环境阻塞（历史记录）

- 默认非 race/race 编译检查均通过（`-run '^$'`，未执行测试）；`go vet ./internal/gateway` 通过。
- 已有 `TestV2ResumeWindowAndAttemptLimits` 三个模拟时间子场景在本轮一次 race 运行中通过：固定窗口、成功后累计次数不清零、退避期间取消。它们是已有预算契约的检查，不能计为新增端到端测试通过。
- 默认沙箱首次运行新增三个场景时，均在创建 TCP Worker 监听器处失败：`bind: operation not permitted`。业务链路尚未启动，不能当作恢复逻辑失败或成功。
- 请求真实网络 race 运行时，自动审批因登录凭据失效而未能完成，命令未执行。Git 暂存也受到同一审批问题影响。此问题不是操作不安全的判定；没有绕过审批或将编译检查替代实际验收。

原始检查记录见[验证目录](../experiments/results/repeated-recovery-validation-pending-2026-10-12/README.md)。测试只修改专用夹具与场景，生产代码及恢复默认参数未改。本步不提供新增完成率、恢复延迟或资源趋势成果。

## 恢复环境后的实际验收（2026-10-12）

本次自动审批恢复可用。新增三个场景首次真实网络运行全部通过，没有业务运行失败或夹具修正；随后 race 各重复五次，共 15 个叶级场景通过，零失败/跳过、无竞态报告。源、位置、原 RPC 和自然清理判据均满足。原始输出见[本次验证目录](../experiments/results/repeated-recovery-validation-2026-10-12/README.md)，此前沙箱受阻记录仍完整保留。

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -json ./internal/gateway -run '^TestV2ClientRepeatedGatewayRecovery$' \
  -count=1 -timeout=30s

GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -json ./internal/gateway -run '^TestV2ClientRepeatedGatewayRecovery$' \
  -count=5 -timeout=90s

GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -p 1 ./internal/gateway ./internal/wsclient -count=1 -timeout=5m
```

相关两包串行 race 回归 Gateway 1073、wsclient 327 个叶级检查通过，共 1400 通过、零失败；17 个默认跳过项为 16 个显式实验及一个私有进程入口，没有竞态报告。新增场景未修改生产逻辑，核心默认值沿用。完整日志压缩后读回一致，归档文件及新测试源码摘要保存在 manifest。

已完成本步验收，下一步进入延长实验预登记，先固定无故障对照、多轮断线时间表、总预算、采集节奏与资源趋势判据，再实施工具和测量。本步是短场景行为验收，没有固定采集节奏、长时稳定性或恢复性能结论。
