# EXP-004-06：尾部停滞改造前基线

Date: 2026-09-22
Status: 历史改造前对照完成；该次测量时尾部等待辅助函数尚未接入。后续改造结果见[尾部期限验收](tail-timeout.md)。
Related: [尾部期限设计](../worklog/designs/2026-09-22-tail-timeout.md)

## 问题与条件

验证输入已结束、Worker 不结束响应流时，会话是否继续占用名额。辅助函数的行为测试通过并不意味着真实会话已经受到尾部期限保护。

- MaxSessions=1；InputIdleTimeout 和 WorkerSendTimeout 均为 100ms。
- 使用真实 WebSocket / gRPC 本机 TCP，客户端、网关、可控 Worker 在同一进程。
- Worker 接收两条测试音频请求并返回片段结果，随后读取请求 EOF，等待测试放行尾部或 RPC 取消。本实验始终不放行尾部。
- 客户端在 end 后维持连接并继续读取，排除客户端不读结果引入的阻塞。
- 从测试观察到 Worker EOF 通知起等待 500ms，然后尝试接入第二个客户端，检查是否满载拒绝。
- 检查后才取消服务 context 兜底清理；取消是实验操作，不是系统自主超时。

这是协议和生命周期实验，使用测试字符串作为 Worker 输入，不是真实 PCM 识别负载。环境 Go 1.26.5、macOS 26.6.2；硬件沿用同日准入实验机器，见 manifest。

## 结果

正式运行三次（未启用 race）：

| 指标 | 实测 |
| --- | --- |
| Worker EOF 通知后的实际观察窗口 | 500.23–501.10ms |
| 窗口结束仍占用唯一名额 | 3/3 |
| 第二个客户端被容量限制拒绝（HTTP 503） | 3/3 |
| 使用外部服务取消兜底 | 3/3 |
| 兜底后 Worker 取消、客户端收到 1001、handler 退出 | 3/3 |
| 最终 active | 三次均为 0 |
| 外部取消发起至 handler 返回 | 0.285–0.451ms |

最后一个耗时仅说明此次客户端持续读取条件下，服务取消能清理会话。它不是尾部超时的耗时，更不是任意异常下的清理保证。

这些数据证明在受测 500ms 窗口内没有自主退出；“当前代码没有尾部期限”还需结合源码判断，不能仅凭有限窗口断言无限等待。

定向等待函数测试连续三轮通过 race 检查；本实验另以 race 运行一次通过。辅助函数已补退出时停止 timer，尚未被 waitSessionResult 调用。

## 下一步对照

开发者接入 TailTimeout 配置、session 参数、waitSessionResult 和 finish 后，使用同样的停滞 Worker、读取中的客户端以及单名额约束，配置较短的测试预算（如 200ms）进行三次对照。

分别记录正常/尾部超时/其他错误、是否需要外部取消、Worker 与 handler 退出时间、名额归还及再次接入。预算内的正常尾部应完整返回；默认 15 秒配置需要独立校验，不能将 200ms 测试预算当作生产推荐值。

## 复现与证据

```sh
TIDE_RUN_TAIL_STALL_BASELINE=1 go test ./internal/gateway \
  -run '^TestTailStallBaselineExperiment$' -count=3 -v
```

此命令的历史结论对应 manifest 中的源码版本。接入默认 15 秒期限后，500ms 观察窗口仍可能看到占位，不能拿它判断整个期限保护是否生效。

- [实验测试](../../internal/gateway/tail_stall_experiment_test.go)
- [原始日志](results/tail-stall-baseline-2026-09-22.log)、[结果汇总](results/tail-stall-baseline-2026-09-22-summary.json)
- [环境、配置、源码摘要与命令](results/tail-stall-baseline-2026-09-22-manifest.json)
- [相对基准提交的源码修改](results/tail-stall-baseline-2026-09-22-source.patch)
