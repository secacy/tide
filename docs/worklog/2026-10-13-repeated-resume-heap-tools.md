# 多轮恢复堆诊断工具与短场景验收

日期：2026-10-13。关联[设计与取舍](2026-10-13-repeated-resume-heap-design.md)、[EXP-006-06](../experiments/repeated-resume-heap-diagnostic.md)。本步完成工具实现和 A/B 各一场十二秒验收；十分钟正式四场尚未运行，原堆增长尚未归因。

## 实现与原理

1. [快照工具](../../internal/gateway/heap_diagnostic_test.go)独占创建二进制 profile 与逐点 JSON，保存两个 GC 前后、写盘后的 MemStats、采样率、开始结束时点、字节数、摘要和写入/关闭错误。增量取摘要，不在 Go 内存中保留 profile 内容。速率为一只由专用进程启动参数设置，普通测试不修改全局 MemProfileRate。
2. [夹具观察点](../../internal/gateway/extended_resume_harness_test.go)可选启用，沿用原输入、Mock、故障位置、预算和 JSONL 结构。独立任务等首块单调原点发布，串行在 2/10s（正式 90/570s）采集；源 Read/结果回调不等待采集。取消后必须 join；迟到超过一秒、原会话不再活动或跨越 Run 返回的活动快照无效，后续不能补造。
3. P3 置于无兜底自然空闲与环境拆除之间。P4 在非并行子测试及全部 Cleanup 返回后采集。[TCP 夹具](../../internal/gateway/network_fixture_test.go)提供仅含布尔值的 ClientConn Close / Serve 退出结果；代理、HTTP handler、采样和 profile 任务另行 join。外层移除捕获业务实例的 state 闭包，只持数值配置、终态和路径。这里仍不保证第三方库内部所有后台任务均退出，相关栈需解释。
4. [专用采集器](../experiments/scripts/run_heap_resume.py)一次非 race 构建、逐场独立进程、原始事件/快照/日志、源码冻结及摘要，固定 probe A→B、formal A→B→B→A。正式进程 720s、外部 725s，probe 90/95s；只有专用子进程设置 `GODEBUG=memprofilerate=1`。复用原睡眠审计，不增加业务 pprof 端点。
5. [独立核对与导出](../experiments/scripts/summarize_heap_resume.py)从原业务事件核对输入、结果、原 RPC、六次恢复和自然释放，另核对五点阶段、原时间轴、rate/GC、单点/总表一致、文件摘要与清理事实。调用同版本 Go pprof 实际解析文件，五点绝对值及三种差分的四项指标各保留完整 top，另存 raw、命令、stderr/退出码。保留负值，不归一化；相对 argv 使搬移后报告可复算。

业务事件中的 `configuration.experiment=EXP-006-05` 表示复用的原输入/服务条件及事件契约；新诊断身份明确写在独立 manifest 与 heap-report 的 EXP-006-06 中，不修改旧分析器或旧实验。原分析器计算的节奏、延迟和增长字段只是剖析干预下的描述，不用它们替代原正式性能结果。

## 有意义的验证

[Go 反例](../../internal/gateway/heap_diagnostic_validation_test.go)检查短写/写入失败/关闭失败、独占文件、停止必须等待实际 action 退出、首块尚未发布时取消、活动点过早/迟到/不再活动/跨越返回，以及子测试 Cleanup 的关闭结果。已知对象在专用 rate=1 子进程同一分配栈分配至少 4MiB，存活时 profile 可见 4,194,608 字节，解除引用并 GC 后为 112 字节；只核对该栈，不硬比整个进程堆。

Python 新增 15 个测试方法，含多项数据反例：错误配置/顺序/分母/采样率/GC/资源阶段、文件缺失/截断/摘要变化、独立元数据矛盾、数字被布尔值替换、清理缺失、场次丢失和构建失败归档。刻意将一个 profile 替换为无效内容并修复摘要，元数据检查可以通过，但真实 pprof 导出仍拒绝；证明摘要一致不能替代格式解析。

首次普通 Go 验证的网络夹具测试受沙箱 TCP bind 限制，允许 loopback 后通过，无业务修复。首次 Python 构建失败反例把 subprocess.run 模拟范围设得过大，连带 git check_output 返回空值；将模拟限定为构建分支后通过。两次日志保留。第一次真实 A/B 采集通过；之后补阶段反例、独立核对及报告可搬移性，以最终版本重新采两场，没有拼接或替换首次场次。

完整 Gateway / wsclient race 验证 1,342 / 385 通过，合计 1,727，19 个默认跳过，零失败、无竞态报告。后续仅补注释/测试 Helper 定位，以最终源码另核对诊断工具 race：15 通过、1 个显式入口默认跳过；Gateway vet 通过。全部实验 Python 192 个方法通过，含本步新增 15 个。数量表示验证覆盖，不作为业务收益。

## 最终 probe

```sh
PYTHONDONTWRITEBYTECODE=1 GOCACHE=/private/tmp/tide-review-gocache \
  GOPROXY=off GOSUMDB=off \
  python3 docs/experiments/scripts/run_heap_resume.py \
  --mode probe --output /private/tmp/tide-heap-tools-2026-10-13-run-2
```

测量时 HEAD 为 129a5fb，包含本步未提交代码；实际受测版本以 332 份冻结源码/计划为准，前后摘要一致。同一非 race 二进制、两独立进程，无未知或替代场。A/B 各完整处理 384,000 字节、应用 121 条结果，源各关闭一次、原 RPC / Worker pick 各一次；B 六次恢复、A 零次。原会话自然空闲，TCP ClientConn Close 与 Serve 退出、夹具拥有任务 join、state 闭包清空均核实。

| 项目 | 最终 probe 结果 |
| --- | --- |
| 计划 / 业务完整 / 阶段核实 | 2 / 2 / 2 场 |
| 五点快照 / 有效 | 10 / 10 |
| 全部 pprof 导出 | 80 / 80 命令成功 |
| 单点 GC + profile + Close + 最终 MemStats 耗时 | 21.201–100.196ms |
| 四个活动点开始迟到量 | 1.238–2.127ms，低于固定 1s 上限 |
| 原场自然退出 / 完整夹具清理 | 2 / 2，均无兜底 |

这说明短场景下工具能够采集、核对和解释采集边界；上述采集耗时并非用户转录延迟，也不证明十分钟采集开销相同。正式四场仍需检查每场实际采集干预、业务、质量及清理，不提前宣称泄漏或优化成果。

原始 profile、事件、二进制、受测源码、计划、80 份报告与命令见[归档](../experiments/results/heap-resume-tools-2026-10-13/README.md)。首轮也单独保留，原 EXP-006-05 归档及降级事实未改。最终 530 文件与首次 529 文件全部解压读回摘要一致；最终原目录冻结复算与归档搬移后冻结复算的 163 个分析文件分别逐字节一致。

## 下一步与边界

工具验收及归档完成后，冻结提交，按预登记执行 A→B→B→A 四场十分钟诊断（业务约四十分钟）。比较晚期−早期、自然空闲−晚期、拆除后−环境前的分配栈，先定位对象来源，再检查具体引用与释放路径。GC 和全量分配剖析改变缓存与调度；即使诊断不复现增长，也不能据此宣称原自然 GC 环境无增长。累计恢复预算仍待后续讨论，当前不调整默认值。
