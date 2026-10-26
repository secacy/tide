# 同 PCM 直接探针对照证据

2026-10-26，q04/q05 各一次。两场完整完成；最后全文、23/28 次变化快照均与 Gateway 基线相同，CER 分别仍为 60.61%、32.35%。范围与限制见[定位记录](../../../worklog/2026-10-26-real-asr-direct-diagnostic.md)。这是绕过业务链路后的错误复现，不是模型质量修复。

- `collection.tar.xz`：37080B，38 份文件；SHA 和逐文件摘要见 `archive-index.json`。
- `summary.json`：简明对照结果。逐字对齐见归档内 `comparison.json`。
- 归档含执行前计划和14份源码/配置、八份与旧归档摘要一致的基线文件、两个探针的逐调用事件/摘要、stdout/stderr、进程执行记录、冻结比较脚本及五个反例。
- 不含音频/WAV、模型和可执行程序。哈希用于核对，推理重放仍需要同一份本地素材及依赖。`run.py` 保留实际执行路径，不是可直接在任意目录重新推理的命令。

38 文件全部读回及搬移后摘要一致；搬移后的冻结比较和五个反例复算通过。解压后可不依赖音频/模型重算比较：

```sh
tar -xf collection.tar.xz
PYTHONDONTWRITEBYTECODE=1 python3 collection/source/experiments/asrquality/compare_probe.py collection
PYTHONDONTWRITEBYTECODE=1 python3 collection/counterchecks.py
```

两条路径的计时起点和音频送达节奏不同，本轮没有作延迟收益比较。两个难例在查看基线后选定，不与原六段合并统计。
