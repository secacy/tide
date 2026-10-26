# 真实 ASR 后端连续会话基线

本工具复用 sherpa 模块的真实网络夹具，测量同一进程的短会话成本及重复清理。固定旧 bilingual/int8 模型、CPU 两线程、greedy；一场预热，五轮“首条非空 partial 后取消 → 正常完成”。模型仍每场创建，保留 Gateway 默认十秒恢复窗口。

采集代码位于 `workers/sherpa/cmd/asr-worker/backend_*_test.go`，仅显式环境变量启用。这里负责运行前源码/资产冻结和离线复核，不引入新的业务服务。完整设计与取舍见[设计记录](../../docs/worklog/2026-10-26-real-asr-backend-baseline-design.md)。

## 准备与运行

准备固定模型的方法见 [Worker 说明](../../workers/sherpa/README.md)。先在 `workers/sherpa` 构建非 race Worker，然后回仓库根目录：

```sh
(cd workers/sherpa && CGO_ENABLED=1 go build -o /tmp/tide-backend-worker ./cmd/asr-worker)
python3 experiments/asrbackend/prepare.py /tmp/tide-backend-collection-new \
  --worker-bin /tmp/tide-backend-worker --model-dir /tmp/tide-sherpa-model
```

冻结目录必须不存在；冻结模型摘要、固定示例 0 的 PCM 摘要、Worker 构建信息，以及相关源码/依赖快照。正式运行前后保持同一套源码与程序。进入 `workers/sherpa`：

```sh
caffeinate -i env \
  TIDE_SHERPA_WORKER_BIN=/tmp/tide-backend-worker \
  TIDE_SHERPA_MODEL_DIR=/tmp/tide-sherpa-model \
  TIDE_SHERPA_BACKEND_EVIDENCE_DIR=/tmp/tide-backend-collection-new/data \
  go test -v -count=1 -timeout=10m ./cmd/asr-worker \
    -run '^TestRealWorkerBackendBaseline$' \
  > /tmp/tide-backend-collection-new/driver.log 2>&1
```

macOS arm64 运行约三分钟；保持电脑唤醒。测量使用非 race 驱动，普通 race 回归会跳过此实验。失败不覆盖原目录，不补场、不重启后拼接结果；`slots.json` 保留预登记的十一场及 passed/failed/running/not_run 状态，`driver.log` 保留测试失败。兜底终止不能作为自然清理成功证据。

## 离线复核与口径

回到仓库根目录运行；使用冻结复核脚本可避免后来工具变化影响旧结果：

```sh
python3 /tmp/tide-backend-collection-new/source/experiments/asrbackend/verify.py \
  /tmp/tide-backend-collection-new \
  --output /tmp/tide-backend-collection-new/reconciled.json
python3 -m unittest discover -s experiments/asrbackend -p 'test_*.py' -v
```

复核完整通过运行，交叉检查十一场应用回调、gRPC 快照、客户端返回、原生事件及源码/条件身份。未通过的运行仍保存原始证据，不能报告为 5/5；不完整运行复核会明确失败。RSS 不可用和内部采样缺口单独列为数据质量问题，已核实的完成结果仍保留。

- 正常完成与预期取消分别为 5 场，预热单列；不合成一个“11/11 转录完成率”。
- 首字从客户端 Run 起计，含加载/握手；尾部从源 EOF 到 Run 返回；取消分别记录到客户端返回、观察到模型 Close、观察到网关归零。
- 500ms 采样驱动 Go 堆/goroutine、Gateway 活跃数与独立 Worker RSS；采样是多次观察，字段非原子快照。RSS 子命令超时为一秒，失败为 null 与原因，不填零。
- 原生日志按完整行增量读取；驱动单调时刻记录“观察到事件”，不跨进程相减墙钟。完整日志保留原生非 JSON 诊断。
- 各场详细事件落盘、RPC 清理后移除观察器 trace；空闲窗口不保留历次转录全文。Go 堆仍包含网关、客户端及测试工具，RSS 包含原生分配器缓存和映射。
- 五轮只报告逐场值、范围和空闲资源变化；不能据此推导长问诊、稳定并发容量、硬取消或生产无泄漏。未知模型处理进度仍不启用积压字节预算。
