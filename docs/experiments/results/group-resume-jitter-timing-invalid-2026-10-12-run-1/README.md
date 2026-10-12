# EXP-006-04 首次正式采集：时间证据不合格

2026-10-12，实测 `4b547c8`。16 批执行，但三批墙钟 T0→释放小于配置 2s；整次 evidence_valid=false，不用于确定策略收益。详见[问题、方案与复测规则](../../../worklog/2026-10-12-group-resume-release-clock.md)。

`collection.tar.xz` 保存全部采集目录：317 份源码/工具/两份条件快照、测试二进制、原始事件/清单、第一次分析和独立复算，以及补充环境记录与采集日志。旁边 JSON/CSV 为原样副本。`archive-index.json` 记录全部 338 个文件摘要；已读回核对，快照分析器复现原不合格结果，五类输出一致。CSV 保留原 CRLF。

复核时在新目录解压，运行 `collection/source/docs/experiments/scripts/summarize_group_resume_jitter.py collection --output 新目录`。分析预期退出 1 并包含三条 occupancy timing differs；这表示首次证据被严格拒绝，不应通过删批或容差改为合格。

本次完整数据保留，后续复测另建 run-2，不和本次合并分母。

旁边 release-clock-race/related 压缩日志是后续夹具修正的验证：定向三轮 39 通过、相关两包 1397 通过。它们不属于首次采集目录；压缩包内保留首次未修正版源码，仍能复现时间证据不合格。
