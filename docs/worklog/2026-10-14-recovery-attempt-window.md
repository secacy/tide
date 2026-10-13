# 第六阶段：客户端恢复次数滑动窗口

日期：2026-10-14。开发者明确委托实现；承接[预算评审](2026-10-13-recovery-budget-review.md)。客户端功能已接通，验证结果见下文；新策略的真实 Gateway 短场组合验收为下一步。

## 变化与实现选择

长问诊中相隔很久的故障会耗尽旧整场次数预算。现可通过 `AttemptWindow > 0` 显式限制最近该时长内的启动尝试数；零值继续限制整个 Run。默认 MaxAttempts=5、单轮 Window=5s，其他超时/退避/jitter 规则沿用原配置。已存在的字段草稿继续完善，没有改动无关工作区文件。

滑动记录保存在 `v2RecoveryState.attemptTimes`，由 Run 的协调者独占。`resumeAttemptAllowed(now)` 统一处理故障处与下次启动前的额度判断，用带单调时钟的时间差清除满窗口的记录，再按近期有效数判断；`recordResumeAttempt(now)` 在启动恢复握手任务时唯一记账。每次检查 O(N)，仅恢复时执行，正常传输不扫描；slice 按需分配、原地压缩、清零尾部引用并复用，有效记录至多 MaxAttempts，没有锁、额外任务或清理定时器。

整场 attempts / ResumeAttempts 两种模式都持续累计，首次 start 不计。计数点是本地启动握手任务，不保证网络拨号或请求已到达服务器。成功 ready 不清空近期历史；只结束当前恢复期限并重置连续失败退避。记录满额时立即返回 ErrV2RecoveryExhausted，保留最近故障，不等待额度过期。

第 N 次成功后仍可正常工作，只有再次需要恢复时判断是否已满。额度自然过期不延长单轮恢复期限或首次 end 后的完成期限。客户端仍使用同一身份、采集任务、音频缓存与结果应用位置，连接清理完成后才启动下一次握手。

代码：[配置与校验](../../internal/wsclient/client_v2.go)、[协调者与额度记录](../../internal/wsclient/client_v2_run.go)、[新增测试](../../internal/wsclient/client_v2_attempt_budget_test.go)。

## 验证范围

新增测试走可控时间与连接替身下的生产协调者、握手/采集任务及 cleanup：

- 负值拒绝、配置复制、正值 JSON 标记和旧默认 JSON 完全相同。
- MaxAttempts=2、AttemptWindow=1s，相隔 1s 的六次恢复成功并完成空输入会话，ResumeAttempts=6；近期记录不超过 2。
- 两次密集成功及“只过期最旧一次”的序列，下一次断线均明确耗尽，无额外 Dial，最近故障可 errors.Is 查询；耗尽额度不会自行结束健康传输。
- Dial 失败及 session_busy 入口拒绝各消耗一次；随后成功不会把这些历史清空。
- 过期前 1ns 仍占额度，恰好窗口边界过期；较新的记录保留，整场统计不重置，存储尾部清零。
- 失败耗时使近期额度过期后可以继续尝试，但单轮 1s 截止点保持固定；迟到 ready 不接纳，原完成期限不续期，退避期间取消不再 Dial；跟踪源核实恰好 Close 一次。

这些是正确性场景，不是网络延迟、临床问诊完成率或真实模型吞吐数据。空输入完成验证协调和收尾，音频重放/结果去重等既有场景继续由回归覆盖；新策略真实 Gateway 组合仍需下一步专门验证。

验证命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race ./internal/wsclient -run TestV2AttemptWindow -count=1 -timeout=60s
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -json ./... -count=1 -timeout=5m
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go vet ./internal/wsclient ./internal/gateway
```

新增定向 race 通过，完整回归 12 个有测试包通过（另两个包无测试），3330 个测试节点通过、19 个实验/辅助入口按默认配置跳过；包括新增 14 个父/子节点，没有报告数据竞争，两包 vet 通过。首轮定向及完整验证均成功，无失败后替换场次。原 JSON 日志压缩读回逐字节一致，源码及日志摘要、命令、逐包结果和跳过列表见[验证归档](../experiments/results/recovery-attempt-window-2026-10-14/README.md)。测试节点数只表示检查覆盖，不代表业务性能收益。没有修改既有累计次数测试的预期，也没有调整冻结实验、默认生产建议或堆诊断结论。

## 下一步

短场真实 Gateway/TCP Mock 组合：分散故障可以超过整场 N 次，密集 ready 后立即断线耗尽，持续恢复握手失败按额度/期限终止。核对原 RPC/Worker pick、采集/结果完整性、实际尝试与自然资源回落。通过后再决定是否预登记长时累计/滑动策略对照，当前不先扩展 CLI 或重复堆诊断。
