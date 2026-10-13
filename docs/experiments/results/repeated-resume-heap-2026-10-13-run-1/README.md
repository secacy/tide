# EXP-006-06 正式四场堆诊断归档

[结果与边界](../../../worklog/2026-10-13-repeated-resume-heap-results.md)、[固定预登记](../../repeated-resume-heap-diagnostic.md)。冻结 bda6e26、332 源码/计划、一次非 race 构建，A→B→B→A 四独立进程，各十分钟；业务/阶段/清理核实 4/4，20/20 快照有效，160/160 pprof 导出成功。两场 B 各六次原 RPC 恢复。

- `collection.tar.xz`：722 文件、136,233,007 字节原内容。完整受测源码/计划、二进制、原事件/终态、20 个 profile、逐点与总元数据、160 份 top/raw、命令/stderr、独立分析都保留。压缩包内根目录为 `collection`。
- `manifest.json` / `preregistration.md` 为包内原副本；`summary.json.xz` 是包内原分析 JSON 的无损压缩副本。`archive-index.json` 保存全文件及压缩包 SHA-256。
- `profile-totals.csv`：每个绝对/差分 raw 样本四种指标的精确和；`checkpoints.csv`：MemStats、采集时点及 profile 存活量并列；`selected-paths.csv`：按经过指定分配路径汇总。标签可能重叠，不能跨标签求总和，也不把分配路径当作持有者。
- `stack-summary.json.xz`：完整符号栈与正负值，非 top 截图。`stack_report.py` 可由保存的 raw 重新生成；`test_stack_report.py` / `stack-tests.log` 记录五个格式器反例。
- `source-references.tar.xz` / `source-references.json`：本机 SDK 与模块实现的七份实际源码、三份原许可证及 SHA，供核对 JSON/HPACK 缓存、runtime 等待节点/记账机制。它们是源码复核依据，不是对象引用图，也不保证网络文档与本机 SDK 逐字节一致。
- `collection.log`、冻结/搬移复算日志及验证清单保存实际执行和结果。旧 EXP-006-05 及本次工具 probe 各自归档未改。

复算（Go 1.26.5；目录尚不存在，路径按需替换）：

```sh
mkdir /private/tmp/tide-heap-formal-check
tar -xJf collection.tar.xz -C /private/tmp/tide-heap-formal-check
PYTHONDONTWRITEBYTECODE=1 GOCACHE=/private/tmp/tide-review-gocache \
  GOPROXY=off GOSUMDB=off \
  python3 /private/tmp/tide-heap-formal-check/collection/source/docs/experiments/scripts/summarize_heap_resume.py \
  --input /private/tmp/tide-heap-formal-check/collection \
  --output /private/tmp/tide-heap-formal-reanalysis
PYTHONDONTWRITEBYTECODE=1 python3 stack_report.py \
  /private/tmp/tide-heap-formal-check/collection /private/tmp/tide-heap-formal-stack-report
```

722 个归档文件解压读回全部一致；原目录及搬移后冻结工具的 325 个分析文件分别逐字节一致，搬移后的格式器 JSON/三份 CSV 也逐字节一致；采用 LF 行尾，避免 Git 规范化改变文件摘要。

四场已发布 profile 的晚期−早期净存活差分别 +584、+24880、+15280、−984B。三个缓冲及逻辑会话构造路径的存活量在活动点固定、自然结束后为零；B1 结果克隆路径 P4 仍有 16B，持有者不明。拆除后约 223–238KB 增量包含共享类型/HTTP2 缓存和运行时分配，没有生产无泄漏或严格资源上界结论。MemStats 与 profile 时点/记账不同，尤其 A1 的约 49KB MemStats 差没有被当作同量 profile 存活对象。
