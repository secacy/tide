# 固定分段 120/600 秒验证工具

条件与判据见[预登记计划](../../docs/experiments/real-asr-segmented-120s.md)。仅显式环境变量启用，不改变默认模型、准入或恢复规则。

首轮正式运行和独立复核已完成，见[结果](../../docs/worklog/2026-10-27-segmented-120s-results.md)及[冻结证据](../../docs/experiments/results/segmented-asr-120s-2026-10-27/README.md)。

准备非 race Worker 和测试驱动，再冻结源码、模型、动态库、输入和实际二进制摘要。在仓库根目录执行（证据目录须不存在）：

```sh
go -C workers/sherpa build -o /tmp/tide-segment120-worker ./cmd/asr-worker
go -C workers/sherpa test -c -o /tmp/tide-segment120-driver ./cmd/asr-worker
python3 experiments/asrsegment/prepare.py /tmp/tide-segment120-new \
  --worker-bin /tmp/tide-segment120-worker --driver-bin /tmp/tide-segment120-driver \
  --model-dir /path/to/fixed-model
```

驱动需在其包目录运行，先检查实际程序和源码与冻结摘要一致，随后运行三个已登记槽位：

```sh
cd workers/sherpa/cmd/asr-worker
TIDE_SHERPA_SEGMENT120_DIR=/tmp/tide-segment120-new \
TIDE_SHERPA_WORKER_BIN=/tmp/tide-segment120-worker \
TIDE_SHERPA_MODEL_DIR=/path/to/fixed-model \
  /tmp/tide-segment120-driver -test.run '^TestRealWorkerSegmented120s$' -test.v -test.timeout=5m
```

返回仓库根目录，独立复核及损坏副本检查：

```sh
python3 experiments/asrsegment/verify.py /tmp/tide-segment120-new --output /tmp/tide-segment120-new/reconciled.json
python3 experiments/asrsegment/counterchecks.py /tmp/tide-segment120-new
python3 -m unittest discover -s experiments/asrsegment -v
```

data/ 保留源读取、实际 Gateway gRPC Send/Recv、客户端结果、各场状态、Worker 原始日志、模型事件和 500ms 采样。观察器 SHA 是成功提交的 float32 样本摘要，不能作为处理完成确认；首流时间含模型工厂，后续 StartSegment 才是独立换流创建时间。

客户端回调仅保留当前段，但 RPC 观察 trace 会保留本场有限结果直到归档；驱动堆包含工具和冻结输入。公共 Gateway Snapshot 没有内部缓冲占用，因此本工具不报告精确队列峰值。采样缺口或 RSS 缺失会使资源质量标记为 false，不能用完整性通过替代资源判据。

## 十分钟档

[600 秒计划](../../docs/experiments/real-asr-segmented-600s.md)保持同一模型、Worker 二进制、30 秒段长和采集方式。使用 `--seconds=600` 准备，显式设置 `TIDE_SHERPA_SEGMENT600_DIR` 并运行 `TestRealWorkerSegmented600s`，总测试期限改为 `-test.timeout=12m`。准备器核对 Worker 是否为两分钟基线保留的原程序；本轮对照不能用其他构建替代。其他构建或环境需要另行登记条件。

首轮[十分钟结果](../../docs/worklog/2026-10-27-segmented-600s-results.md)已完成，原始记录与冻结工具见[归档](../../docs/experiments/results/segmented-asr-600s-2026-10-27/README.md)。

两个档共用驱动、复核器和损坏副本检查，默认仍为 120 秒。600 秒汇总增加五个两分钟窗口的边界等待和 RSS 范围，保留所有原始点，不设置运行后挑选的达标阈值。驱动堆还包含完整输入和各场 RPC 汇总快照；19.2MB 输入与 3.84MB 输入之间的工具堆差值不能解释为业务泄漏。
