# EXP-006-05：正式六场原始证据

日期：2026-10-13。对应[正式结果](../../../worklog/2026-10-13-repeated-resume-extended-results.md)和[预登记](../../repeated-resume-extended.md)。测量提交 ecf8a92，600 秒/6000 块，固定 ABC、CBA 顺序，六场独立非 race 进程；这是本轮唯一正式运行，没有补场或替代场。

- `collection.tar.xz`：根目录 `collection`，359 文件；含 326 份选择的源码/工具/冻结计划、非 race 二进制、构建/逐场 stdout/stderr、原始 JSONL/终态、manifest 和四分析输出。
- `archive-index.json`：压缩包及所有内部文件 SHA256、解压读回一致性、日志压缩/原文摘要与四输出复算摘要；359 文件全部读回一致。
- `manifest.json`、`preregistration.md`、`summary.json.xz`、`sessions.csv`、`outages.csv`、`resource-windows.csv`：完整档案中的便读副本。包含大量位置序列的完整 summary 另压缩保存，解压与原文件一致，未压缩原文件仍在完整归档中。计划保留测量前版本，工作区后续更新进度不会改写归档。
- `validation-manifest.json`：固定分母、证据有效性、行为与质量分级、归档/复算检查及睡眠审计。
- `quality-audit.json`：第二轮 B 源块 1592 开始读取落后 140.746542ms 的原始时点及采样间隔，公式与冻结分析器一致；原因未知，保留业务证据并降级整场时间/资源解释资格。
- `collection.log.xz`、`frozen-analysis.log.xz`、`reanalysis.log.xz`：原采集与两次复算的输出。分析器成功时无 stdout，空日志是实际文件，不是缺少原始事件。

六场完整性均核实：A/B 各 2/2 完整，C 0/2 完整、2/2 明确耗尽；未知零、行为 6/6 合格，时间/资源质量 5/6。各原 RPC/Worker 选择一次、源关闭一次、自然清理。共处理 107,520,000 字节、应用 33,604 条结果；不把正确失败当作业务完成。

冻结分析器在解压搬移后的完整目录复算四输出，当前同版本分析器在另一新目录复算，均逐字节一致。复算命令为 `python3 collection/source/docs/experiments/scripts/summarize_extended_resume.py collection --output 新目录`。manifest 保留原执行路径，实际读取搬移后的文件。快照是选择的实验源码与二进制，不包含仓库全部历史夹具，不声称可独立重建整个仓库全部测试。

自有防休眠进程结束前仍在，pmset 审计成功且无转换；不能据此认定第二轮 B 的停顿原因。31 个合格窗口 goroutine 均 30，但四个合格场触发 HeapAlloc 小幅增长线索，不能宣称无泄漏或严格内存有界。所有不利事实和降级原值保留，没有删除慢块或另测替换。详见正式报告；不推断真实 ASR、生产容量或数十分钟临床表现。
