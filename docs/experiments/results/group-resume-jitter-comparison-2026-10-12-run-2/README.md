# EXP-006-04 正式随机退避对照：合格完整复测

日期：2026-10-12，实测 `b503a3c`。同一次非 race 构建，四条件各四批、128 场，全部完整完成且证据合格，无删批、替代 RPC 或强制清理。

首次 `4b547c8` 的时间证据不合格并[完整保留](../group-resume-jitter-timing-invalid-2026-10-12-run-1/README.md)。本次为双时钟释放夹具修正后的独立完整复测，没有混用首次合格批次。

`collection.tar.xz` 保存完整目录：318 份源码/工具/两份条件快照、darwin/arm64 测试二进制、实际配置/环境、构建与完整事件、第一次分析及新目录复算、补充环境/review 指标。旁边 JSON/CSV 是原样副本，补充指标包含实际额外请求轨迹，不修改原始事件。

`archive-index.json` 记录全部 341 个解压文件及外层摘要；已读回核对，快照分析器复算 reports/summary/sessions/attempt-bins/release-bins 五类数据逐字节一致。CSV 保留原 CRLF。

```sh
mkdir /private/tmp/tide-jitter-formal-audit-new
tar -xf docs/experiments/results/group-resume-jitter-comparison-2026-10-12-run-2/collection.tar.xz \
  -C /private/tmp/tide-jitter-formal-audit-new
PYTHONDONTWRITEBYTECODE=1 python3 \
  /private/tmp/tide-jitter-formal-audit-new/collection/source/docs/experiments/scripts/summarize_group_resume_jitter.py \
  /private/tmp/tide-jitter-formal-audit-new/collection \
  --output /private/tmp/tide-jitter-formal-audit-new/analysis-new
```

500ms 请求 144→129，2s 为 203→195，开放后拒绝分别 16→0、11→0。最大转录间隔下降，但 2s 间隔中位数 2632.343→2778.848ms，第四轮最大值也变差。详见[正式结果与权衡](../../../worklog/2026-10-12-group-resume-jitter-results.md)。四批/六秒/一次故障的 Mock 条件，不推断真实 ASR、生产默认值或长时恢复 SLO。
