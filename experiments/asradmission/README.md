# 两个真实 Worker 的准入对照

固定一个活动 Gateway、两个持续运行的原生 Worker，对照普通轮询与本地占用轮询。实验判据见[预登记计划](../../docs/experiments/real-asr-admission-comparison.md)。本工具不测稳定容量或模型识别准确率。

## 运行

固定旧 bilingual/int8 模型及 SDK 的准备见 [Worker](../../workers/sherpa/README.md)。在仓库根目录：

```sh
(cd workers/sherpa && CGO_ENABLED=1 go build -o /tmp/tide-admission-worker ./cmd/asr-worker)
python3 experiments/asradmission/prepare.py /tmp/tide-admission-new \
  --worker-bin /tmp/tide-admission-worker --model-dir /tmp/tide-sherpa-model
```

源码冻结记录当前版本、相关源码/依赖、实验计划、两份 PCM、模型与二进制/原生库摘要。目录必须不存在，失败不覆盖。进入 `workers/sherpa` 执行非 race 实验，保持唤醒：

```sh
caffeinate -i env \
  TIDE_SHERPA_WORKER_BIN=/tmp/tide-admission-worker \
  TIDE_SHERPA_MODEL_DIR=/tmp/tide-sherpa-model \
  TIDE_SHERPA_ADMISSION_EVIDENCE_DIR=/tmp/tide-admission-new/data \
  go test -v -count=1 -timeout=10m ./cmd/asr-worker \
    -run '^TestRealWorkerAdmissionComparison$' \
  > /tmp/tide-admission-new/driver.log 2>&1
```

包括两场暖机、三个配对批次（旧→新、新→旧、旧→新）及三个超额批次，共预登记 29 次客户端尝试。每臂有 45 秒总观察预算，批末自然清理预算五秒；臂间固定空闲两秒。失败保留原始回调、客户端返回与槽位，不补场、不重启拼接。普通 race 回归不启用这个真实模型实验。

## 复核

回仓库根目录，用冻结脚本复核原始证据：

```sh
python3 /tmp/tide-admission-new/source/experiments/asradmission/verify.py \
  /tmp/tide-admission-new --output /tmp/tide-admission-new/reconciled.json
python3 -m unittest discover -s experiments/asradmission -p 'test_*.py' -v
```

复核器交叉检查 29 次尝试、选择与 RPC 事件、原生创建/释放、音频字节/SHA、快照序列、完成确认、触发重叠及资源采样。完整未通过运行会明确失败，不能填为成功分母；采样缺口和 RSS 不可用分别列出。

本地容量不在运行中通过 TryAcquire 轮询探测，避免改变游标；只在批末 Gateway 已清理后检查全部名额可用。Gateway 触发快照与两 Worker 的模型计数另存原始证据；它们是连续观察，不是跨进程原子快照。模型日志使用驱动观测时刻，不跨进程相减墙钟。

每场的 `report` 复用旧单场记录类型；其中旧 cleanup 字段不代表本轮逐场清理。并发臂的实际自然清理在 `arms.json` 判定。旧策略的本地容量复用字段为 null，不伪装为有容量限制；忙拒绝不创建模型，入口拒绝不建立 Worker RPC。结果不保存恢复凭据。

两 Worker 的资源由 500ms 采样记录；RSS 为独立进程驻留量，Go 堆含客户端和工具开销。本轮主要比较固定重叠场景的完成与拒绝；不从小样本推 p99 或“并发能力提升倍数”。
