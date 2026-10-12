# 多轮延长观察：独立工具验收证据

日期：2026-10-13。对应[实现与验收](../../../worklog/2026-10-13-extended-resume-tools.md)。本目录是十二秒 probe 和工具回归，十分钟正式六场尚未运行。

- `collection.tar.xz`：最终两轮六场完整采集目录，根目录 `collection`，359 文件；含 326 份选择的 Go/proto/模块/Python/计划源码快照、当场非 race 二进制、构建与进程日志、原始事件/终态、manifest 和四份分析输出。
- `initial-collection.tar.xz`：初次一轮三场，347 文件；后续分析器加强前的版本单独保存。它不是最终六场的补充场，不混合统计。
- `archive-index.json`：两个压缩包与各内部文件 SHA256、日志压缩/原文摘要、解压读回和冻结复算核对。最终 359 与初次 347 文件全部一致；各自冻结分析器在搬移后的完整目录重算，四输出分别与各自原始分析逐字节一致。
- `manifest.json`、`summary.json`、`sessions.csv`、`outages.csv`、`resource-windows.csv`：最终档案及分析便读副本；原文件也在完整归档中。
- `validation-manifest.json`：命令、检查计数和证据范围；`collection.log.xz`、`initial-collection.log.xz`、两个冻结分析日志、两个 Python 日志、`related-race.jsonl.xz` 保留执行记录。Python 默认夹具从本目录最终归档解压，18 个新方法通过；全工具 177 方法、相关 Go race 1430 通过/18 跳过。

最终六场证据/观测质量均合格：A/B 各两场完整处理 384000 字节与 121 条结果；B 每场六次接回；C 两场第五次接回后，第六次断线不再拨号，明确 `recovery_exhausted`，采集 275200、处理 272000，末轮等待 null/右删失。原 RPC、源关闭和自然空闲均核对。未知数零，明确失败两场是预登记行为，不能写成六场业务全部完成。

测量 HEAD 为 bf85832，包含本步未提交变更；实际受测版本以冻结源码摘要为准。采集期间源码/计划前后不变，六份本步 Go/Python 源码与最终快照一致。测量后计划文档更新进度，归档保留原计划，不覆盖旧证据。完整快照是选择的实验源码和二进制，不包含仓库全部历史结果夹具；用于冻结分析复算，不声称能独立重建整个仓库全部测试。

复算时先把归档解压到新目录，再运行其中 `collection/source/docs/experiments/scripts/summarize_extended_resume.py`，输入 `collection` 并指定新的 `--output`。manifest 保留原始执行路径，读取搬移后的实际文件。这些结果不证明十分钟稳定性、真实 ASR 容量或生产资源边界。
