# EXP-006-03 正式次数预算对照归档

日期：2026-10-11，实测提交 `a7358df`。同一次非 race 构建、四条件各三批、96 场；12 批证据合格，68 完成、28 明确失败，无未知、强制清理或删改业务失败。

`collection.tar.xz` 保存完整采集目录：309 份源码/工具/预登记快照、本机 darwin/arm64 测试程序、实际配置/环境、构建与原始 Go 事件、初次分析及新目录独立复核。`environment-audit.json` 和 `review-metrics.json` 是测量后的补充核对，原采集清单和原始报告没有改写。

旁边的 manifest、summary、CSV 和补充指标是归档内文件的原样副本。`archive-index.json` 记录全部 328 个解压文件及归档/副本摘要；已在新目录读回，使用快照分析器复算，四类数据输出逐字节一致。CSV 用目录内 `.gitattributes` 保留原始 CRLF，避免换行转换破坏摘要。

新目录复核：

```sh
mkdir /private/tmp/tide-group-budget-audit-new
tar -xf docs/experiments/results/group-resume-budget-comparison-2026-10-11-run-1/collection.tar.xz \
  -C /private/tmp/tide-group-budget-audit-new
PYTHONDONTWRITEBYTECODE=1 python3 \
  /private/tmp/tide-group-budget-audit-new/collection/source/docs/experiments/scripts/summarize_group_resume_budget.py \
  /private/tmp/tide-group-budget-audit-new/collection \
  --output /private/tmp/tide-group-budget-audit-new/analysis-new
```

2s 占位时，五次 0/24、八次 24/24，实际请求 120→154；八次全部实际用到第六至第八次机会。500ms 组观察到 20/24→24/24，但八次组全部在前五次内接回，不能将该差异或请求减少归因于额外次数。具体时延、成本及限制见[实验报告](../../group-resume-budget-comparison.md#正式结果2026-10-11)和[执行记录](../../../worklog/2026-10-11-group-resume-budget-results.md)。

RPC·秒是故障后原 Worker 流的存活时间之和，不是 CPU 时间。heap/goroutine 是整个测试进程的采样，不是严格上界或单会话资源。成功时延与失败分母分别展示，本轮不证明 jitter、真实 ASR、长时多次故障或生产恢复 SLO。
