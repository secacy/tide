# 首个本地真实 ASR 探针证据

详见[验收与限制](../../../worklog/2026-10-25-real-asr-probe.md)、[运行模块](../../../../experiments/asrprobe/README.md)。

- `collection.tar.xz`：首次/最终各四场的原始事件、摘要、stdout/stderr、命令与源摘要，以及对应源码快照、公开绑定/模型元数据和相关原生实现。共 55 文件，65432B 压缩；未包含模型、音频或可执行/动态库二进制。
- `archive-index.json`：归档摘要及全部 55 文件 SHA256，已读回核对。
- `manifest.json`：最终四场原运行清单的原样副本。
- `reconciled.json`：在新目录解包后，冻结校验器对最终四场事件/摘要的核对输出。
- `validation.json`：源快照、搬移核对、测试及口径修正说明。首次源码为事后还原、与运行前哈希逐字节匹配；最终快照与运行清单匹配。

在一个新目录解包后，可离线核对最终四场，不需要模型：

```sh
tar -xf collection.tar.xz
PYTHONDONTWRITEBYTECODE=1 python3 collection/verify.py collection/final
```

`first` 保留修正前字段与取消场无可比意义的 RTF，不能由最终校验器按新口径解释。该轮数据没有并入最终分母。事件是实际运行记录，校验器只核对内部一致性，不证明模型处理了每个原始样本、文本准确、硬取消或生产容量。
