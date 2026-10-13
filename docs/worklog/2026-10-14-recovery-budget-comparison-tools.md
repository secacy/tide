# 第六阶段：恢复预算对照工具与 probe 验收

日期：2026-10-14。承接 [EXP-006-07](../experiments/recovery-attempt-window-comparison.md) 与[工具设计](2026-10-14-recovery-budget-comparison-design.md)。本步工具实现及四场 probe 已验收，正式十分钟四场尚未运行，没有新增正式完成率或恢复性能数据。

## 实现与兼容

复用 extended 夹具、采集器和逐场分析器，通过显式 profile 参数选择旧 EXP-006-05 或新 EXP-006-07。旧调用默认使用旧配置/规则；新入口固定 L/S 四场，不替换旧 A/B/C、不修改全局常量。Go 配置增加可选 Experiment 和 AttemptWindow，零值 omitempty 保持旧配置形状；原位置式初始化改为具名初始化。配置事件与实际客户端均接入相同窗口，新正式入口须显式启用，默认测试跳过长时采集。

独立 profile 固定 formal S=60s、probe S=1s、两组 MaxAttempts=5、L→S→S→L；采集器单次构建、独立进程、固定槽位与源摘要，错误/超时保留。分析器按配置核对身份与窗口；累计组仍检查整场次数 <=5，滑动组保留整场统计并核对接纳/请求数，不把第六次合法尝试误报超额。正式误带 memprofilerate=1 时拒绝采集，仍保留失败 manifest。

核心共享事件校验、原音频 SHA 算法、计时/资源窗口和质量判据不变。代理请求时刻不冒充客户端启动时刻；精确窗口限额仍依据此前可控时钟测试，不为本轮添加业务观察回调。

代码入口：[Go 专用采集](../../internal/gateway/attempt_window_resume_collection_test.go)、[固定 profile](../experiments/scripts/attempt_window_resume_contract.py)、[采集 CLI](../experiments/scripts/run_attempt_window_resume.py)、[分析 CLI](../experiments/scripts/summarize_attempt_window_resume.py)、[新反例](../experiments/scripts/test_attempt_window_resume.py)。

## 四场 probe

命令：

```sh
PYTHONDONTWRITEBYTECODE=1 GOCACHE=/private/tmp/tide-review-gocache \
  GOPROXY=off GOSUMDB=off \
  python3 docs/experiments/scripts/run_attempt_window_resume.py \
  --output /private/tmp/tide-policy-probe-2026-10-14
```

基线 0a59312 加本步冻结实现，339 份源码/计划前后摘要一致，单次非 race 构建四个串行独立进程。12s/120 块、3200B/100ms、六轮 500ms 阻断、Mock 10ms/块；S 回看 1s，L 整场累计，两组五次。probe 同时运行两包回归作为工具验证，本步不从其耗时/资源数值推断性能；正式运行按预登记独占负载。

| 组 | 核实 / 计划 | 完整 / 明确失败 / 未知 | 尝试 / 接纳（每场） | 采集 / 处理（B） | 应用结果（每场） |
| --- | ---: | ---: | ---: | ---: | ---: |
| L 累计 | 2/2 | 0 / 2 / 0 | 5 / 5 | 275200 / 272000 | 85，无尾部 |
| S 滑动 | 2/2 | 2 / 0 / 0 | 6 / 6 | 384000 / 384000 | 121，含唯一尾部 |

四场完整性证据及预期行为均核实，节奏/采样质量四场合格。S 输入摘要、位置、结果及完成确认完整；L 第六轮无恢复请求，明确 recovery_exhausted，已采集未处理 [272000,275200) 3200B、尚未采集 [275200,384000) 108800B 分开，最后恢复等待 null/右删失。均原 RPC/Worker pick 一次、源 Close 一次，自然空闲，无兜底参与。上述数量仅证明十二秒工具链路，未替代十分钟正式结论。

## 检查、首次错误与复算

- 新 Go 固定模式/窗口、正式开关、负值/身份及配置副本检查通过；两包完整 race 1746 父/子节点通过、20 个实验/辅助入口默认跳过，Gateway vet 通过。
- Python 原 192 方法加新 14 方法，共 206 方法通过。新反例覆盖窗口/mode/身份漂移、累计超额、滑动尝试与接纳矛盾、伪完成、强制清理、位置/结果跳跃、缺事件/缺场及新依赖快照遗漏。
- 首次全量 Python 的一个新反例对 reversed() 迭代器直接取切片，触发 TypeError，尚未进入分析器；改为同长度的明确错误顺序后 206 方法全部通过。首次日志保留，没有隐藏失败或替换 probe。
- probe 完成后唯一源码变化是上述反例测试修正；采集器、分析器及 Go 夹具未变。归档保留测量时原始 339 文件（含测试修正前版本），验证清单另记最终测试 SHA，避免把两者说成完全同一源码快照。默认从归档载入 fixture 的新 14 方法另行通过。
- 364 文件全部 tar.xz 归档读回摘要一致。原目录冻结分析四输出、搬移后冻结分析四输出分别逐字节一致；旧 EXP-006-05 probe 用当前默认分析器复算四输出也与旧归档逐字节一致。CSV 保留在二进制归档内，原行尾不受 Git 文本规范化影响。

完整 manifest、事件、终态、冻结源码/二进制、首次/最终验证日志及摘要见[归档](../experiments/results/recovery-budget-tools-2026-10-14/README.md)。测试节点数和包耗时仅为验证规模/耗时，无网络成功率或业务收益含义。

## 下一步

冻结本步验收后的提交，按预登记运行四场十分钟对照：

```sh
PYTHONDONTWRITEBYTECODE=1 GOCACHE=/private/tmp/tide-review-gocache \
  GOPROXY=off GOSUMDB=off \
  python3 docs/experiments/scripts/run_attempt_window_resume.py \
  --mode formal --output 一个尚不存在的新目录
```

正式 S 使用 60s，不能改成 probe 的 1s；累计/滑动四场预计约 37 分钟。保持唤醒，采集器持有自己的保护并审计睡眠，逐场原值及质量保留；完成后独立复算与归档，再整理第六阶段结论。真实 ASR 尚未接入。
