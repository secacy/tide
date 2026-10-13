# EXP-006-06 堆诊断工具验收归档

只含 A/B 各一次十二秒 probe，没有十分钟正式场或泄漏定位结论。[实现与验收](../../../worklog/2026-10-13-repeated-resume-heap-tools.md)、[预登记](../../repeated-resume-heap-diagnostic.md)。

- `collection.tar.xz`：最终版 530 文件、46,601,767 字节原内容。含 332 份受测源码/计划、同一非 race 二进制、两场完整原事件/终态、10 份二进制 heap profile 与各点元数据、80 份 pprof top/raw 及命令/stderr、独立分析。
- `first-collection.tar.xz`：首次成功 probe 的完整 529 文件、46,467,500 字节原内容。后续增强导出报告的可搬移性及反例，最终两场重新执行，首次没有被补改或拼接。
- 顶层 `manifest.json` / `summary.json` / `preregistration.md` 为最终档副本，`first-` 前缀为首次档副本；原文件全部也在各自压缩包中。
- `archive-index.json` 保存两个压缩包及其所有原文件的 SHA-256；全部文件解压读回一致。
- `validation-manifest.json` 及对应日志保存完整 Go race、最终诊断 race、192 个 Python 方法、首次 Python 反例实现错误及修正后结果、两次真实 probe 和冻结复算。完整 race JSONL 压缩保存并读回核对。

最终两场业务/阶段/清理均核实，10/10 快照有效、80/80 pprof 命令成功。剖析期间的业务和时间值只用于诊断事实，不能替代 EXP-006-05 性能结果。原堆增长未归因。

测量时 HEAD=129a5fb，实际代码包括未提交变更，以包内受测源码为准；正式诊断必须另冻结本步完成后的版本，不把这个 commit 字段误当成最终工具提交。

复算示例（替换输出目录，要求 Go 1.26.5；目录必须尚不存在）：

```sh
mkdir /private/tmp/tide-heap-archive-check
tar -xJf collection.tar.xz -C /private/tmp/tide-heap-archive-check
PYTHONDONTWRITEBYTECODE=1 GOCACHE=/private/tmp/tide-review-gocache \
  GOPROXY=off GOSUMDB=off \
  python3 /private/tmp/tide-heap-archive-check/collection/source/docs/experiments/scripts/summarize_heap_resume.py \
  --input /private/tmp/tide-heap-archive-check/collection \
  --output /private/tmp/tide-heap-archive-reanalysis
```

最终版原目录冻结工具复算及归档搬移后冻结工具复算，163 个分析文件（含 summary、全部 top/raw、stderr 和命令）分别全部逐字节一致。首次版本的命令记录使用原绝对路径；其压缩包为原始版本事实，未追写最终版的相对命令。
