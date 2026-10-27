# 分段 Worker 独立进程短验证

2026-10-27 的实现验收证据；不是预登记容量或长时实验。

- cases.json：两场输入分别为 64000B、67200B 的 16kHz 单声道 S16LE 零样本，返回的原始结果及 final ID；输入无真实节奏等待。
- server.log：本次独立进程的原始 JSON 日志，单段 32000B、整场 128000B、CPU 两线程，两个 model_created 和两个 engine_closed。
- identity.json：运行后收集的相关生产代码、模型、Worker 程序及证据摘要；不声称在运行前冻结条件。

两场正常 EOF，分别两个和三个 final；每个满段 final 都在继续送入下一段之前收到，模型释放后进程正常 SIGTERM 退出。详细范围见[实现验收](../../../worklog/2026-10-27-segmented-worker-validation.md)。这里没有逐段原生流日志、资源采样、识别质量或实际长输入数据。

在 sherpa 模块显式复现，模型目录沿用仓库下载器校验的固定原始资源：

```sh
go build -o /tmp/tide-segmented-worker ./cmd/asr-worker
TIDE_SHERPA_MODEL_DIR=/path/to/fixed-model \
TIDE_SHERPA_WORKER_BIN=/tmp/tide-segmented-worker \
TIDE_SHERPA_SEGMENTED_EVIDENCE_DIR=/tmp/tide-segmented-evidence-new \
  go test -v ./cmd/asr-worker -run '^TestRealWorkerSegmentedProcess$' -count=1 -timeout=60s
```

证据目录须尚不存在；普通测试默认跳过，不自动下载或加载模型。源码在测试中核对输入长度对应的结果数量、片段 ID、静音文本、正常 EOF、模型 Close 和停服结果。
