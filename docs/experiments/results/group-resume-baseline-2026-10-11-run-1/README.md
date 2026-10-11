# EXP-006-02 正式首轮归档

2026-10-11，实测提交 `6d23d40`，非 race，五组各三批、120 场，全部保留。94 场完整完成、26 场 recovery_exhausted；15 批证据合格并自然清理。业务结果及范围见[实验报告](../../group-resume-baseline.md)。

`collection.tar.xz` 保存完整原始目录，包括源码快照、实际配置/环境、预登记、构建日志、本机 darwin/arm64 测试程序、Go 原始事件、初次分析和独立复核。采集清单及原始记录未改写；`environment-audit.json` 和 `review-metrics.json` 是采集后的补充检查。压缩避免重复保存长日志中的音频和事件，不改变字节。

旁边的 manifest、summary、CSV 和补充指标是归档内文件的原样副本，便于阅读。`archive-index.json` 记录归档/副本摘要和全部 323 个解压文件的摘要。已在新目录解压，并使用快照中的分析器校验源码、产物和全部会话；四种分析输出逐字节一致。`collection.log`、`reanalysis.log`、`archive-validation.log` 保存三次复核状态。

使用新目录复核：

```sh
mkdir /private/tmp/tide-group-formal-audit-new
tar -xf docs/experiments/results/group-resume-baseline-2026-10-11-run-1/collection.tar.xz \
  -C /private/tmp/tide-group-formal-audit-new
PYTHONDONTWRITEBYTECODE=1 python3 \
  /private/tmp/tide-group-formal-audit-new/collection/source/docs/experiments/scripts/summarize_group_resume.py \
  /private/tmp/tide-group-formal-audit-new/collection \
  --output /private/tmp/tide-group-formal-audit-new/analysis-new
```

时间指标采用原始纳秒时间重新计算。summary 中每场 `remote_idle_after_return_ms` 是该场 Run 返回到**整批**自然空闲的时间，不能解释成该场的精确释放时刻；原 RPC 的退出时刻另见完整 reports 中的 worker.exited_at。运行时 heap/goroutine 是整个测试进程的采样，不能换算成单场严格上限。

这是八场共享入口、固定 Mock 与受控断线/握手占位的首轮基线。50ms 请求桶用于观察聚集；没有随机退避对照、生产容量、真实弱网或真实 ASR 的结论。
