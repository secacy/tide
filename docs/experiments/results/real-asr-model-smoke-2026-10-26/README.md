# 中文流式候选模型：技术冒烟证据

2026-10-26，新模型两段公开语音、静音、主动取消及默认旧模型共五场均符合预期。新模型两段语音均输入期间出字；没有对六段自录问诊运行质量比较。条件、资产来源和限制见[工作记录](../../../worklog/2026-10-26-real-asr-model-smoke.md)。

- `collection.tar.xz`：90732B、70 文件，压缩包及逐文件摘要见 `archive-index.json`。
- `summary.json`：五场指标与最后原始快照。
- 归档包括执行前计划/源码/资源摘要、五场事件与报告、进程命令和退出状态、官方发布元数据、两组 ONNX metadata、源码特征选择依据、测试和复核工具。
- `relocation-check.json`：全部 70 文件搬移读回验证，以及五场重算、七类损坏证据反例和 Python 18 项测试的搬移执行结果。复核无需模型与原始音频，也没有重新运行推理。
- `post-run-tool-supplement.json` 明确标记运行后复制的原有质量工具，供搬移执行测试；不把它们描述成执行前冻结的源码。

不含模型、音频、原生库或二进制；权重许可仍未确认，归档中的来源说明不能作为部署授权。模型元数据包含 Whisper 特征标记，后续需要按模型与配套配置组合对照解释。

```sh
tar -xf collection.tar.xz
PYTHONDONTWRITEBYTECODE=1 python3 collection/recheck.py
PYTHONDONTWRITEBYTECODE=1 python3 collection/counterchecks.py
```

`run.py` 保留当时的本机路径和测量过程；异机推理需要单独准备锁定依赖与新输出目录，不应覆盖本轮证据。第一次归档检查拒绝了元数据检查产生的 Python 缓存文件，删除该生成缓存后再归档，概述保留在 `preparation/first-attempts.txt`。
