# sherpa 流式 ASR Worker

独立 Go 模块，复用根模块的 `internal/realasr` 和 protobuf 服务。当前目标为 **macOS + cgo + CPU**，使用固定 `sherpa-onnx-go-macos v1.13.8` 和中英流式 Zipformer int8；没有 Linux/GPU 构建或医疗质量结论。

本模块提供真实模型运行入口。[探针](../../experiments/asrprobe/README.md)继续独立保留实验行为与原始证据。根目录的 `go test ./...` 不包含本嵌套模块，需单独测试。

## 准备与运行

在仓库根目录下载已固定 revision/摘要的资源到新目录：

```sh
python3 experiments/asrprobe/fetch_assets.py /tmp/tide-sherpa-model
```

若已有该下载器核对的相同资源，可以复用原目录。随后进入本模块：

```sh
cd workers/sherpa
go mod download
go test -race ./...
go vet ./...
CGO_ENABLED=1 go build -o /tmp/tide-sherpa-worker ./cmd/asr-worker
/tmp/tide-sherpa-worker -model-dir /tmp/tide-sherpa-model
```

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `-model-dir` | 必填 | 四份 int8 模型/词表文件目录 |
| `-threads` | 2 | 原生 CPU 线程数，允许 1～16 |
| `-listen` | 127.0.0.1:50051 | gRPC 监听地址；`:0` 可分配临时端口 |

`-h` 帮助退出 0；参数、文件、监听和服务错误退出 1。启动先检查文件，模型在每场取得唯一名额后创建；`listening` 日志的 `model_loaded=false` 不代表模型已成功加载。`model_created` 记录每场加载耗时，`engine_closed` 只在原生释放调用返回后记录，均不记录转录文本。

首版固定单场、每场原始 PCM 最多 960000B（30 秒）、每块最多 3200B、每个文本快照最多 4096B；gRPC 接收消息限制为 4096B。超限明确失败。输入为 16kHz、单声道、PCM S16LE，不发送 WAV 容器；不足 100ms 的末块只需保持两字节样本对齐。

输出使用同一 segment ID 的完整可修订快照；消费者应替换同片段文字。正常半关闭输入后完成尾部解码，发送一次 final 再结束 RPC。单个片段 final 与 Gateway 的整场 completed 不同。

## 真实运行验证

普通测试无需模型文件，但编译/链接需要上述原生依赖。真实模型测试显式启用：

```sh
TIDE_SHERPA_MODEL_DIR=/tmp/tide-sherpa-model \
  go test -v -count=1 ./internal/native -run '^TestRealModelLifecycle$'

TIDE_SHERPA_MODEL_DIR=/tmp/tide-sherpa-model \
TIDE_SHERPA_WORKER_BIN=/tmp/tide-sherpa-worker \
TIDE_SHERPA_EVIDENCE_DIR=/tmp/tide-sherpa-evidence-new \
  go test -v -count=1 ./cmd/asr-worker -run '^TestRealWorkerProcess$'
```

证据目录必须尚不存在。进程测试使用固定示例 0/1、静音、出字后取消和取消后新场，保存逐条结果、摘要、启动/释放日志、帮助与错误退出输出。它依赖本机 TCP 监听权限，不是并发负载实验。正式验证使用非 race Worker 二进制；摘要中的首字等待从 RPC 发起计时，包含本场模型加载，尾部从 CloseSend 调用开始至 EOF 计时。

可用 `python3 verify.py /tmp/tide-sherpa-evidence-new` 独立核对摘要、逐条消息与创建/释放日志。[本机证据归档](../../docs/experiments/results/sherpa-worker-smoke-2026-10-26/README.md)保留一次完整接通运行。

生产 Gateway 处理器和 v2 客户端的真实网络闭环另行启用：

```sh
TIDE_SHERPA_MODEL_DIR=/tmp/tide-sherpa-model \
TIDE_SHERPA_WORKER_BIN=/tmp/tide-sherpa-worker \
TIDE_SHERPA_GATEWAY_EVIDENCE_DIR=/tmp/tide-sherpa-gateway-evidence-new \
  go test -v -count=1 ./cmd/asr-worker -run '^TestRealWorkerGatewayV2$'
```

该验证在临时 HTTP 端口组装生产 Gateway/V2Client，Worker 是独立进程；它检查音频转发 SHA、快照替换、completed 和自然清理，不覆盖 cmd/gateway 启动与演示客户端。保留默认十秒恢复窗口，因此取消场景会等待窗口到期，全部场景约三十秒。消息中不保存 ready 凭据。最终结果、首次计时修正及复核命令见[闭环证据](../../docs/experiments/results/real-asr-gateway-v2-2026-10-26/README.md)。


## 连续会话与资源采样

同一 Worker 进程的五轮取消/正常会话基线见 [asrbackend 工具说明](../../experiments/asrbackend/README.md)。显式设置 `TIDE_SHERPA_BACKEND_EVIDENCE_DIR` 才启用 `TestRealWorkerBackendBaseline`；它保留默认十秒恢复窗口，约三分钟，使用非 race 驱动和 Worker。运行前冻结源码与输入/模型/程序摘要，运行后独立核对逐场证据、原生创建/释放、网关归零及 500ms 资源采样。

该实验区分客户端返回、原生释放返回与 Gateway 会话归零。RSS 包含原生堆、映射与分配器缓存，Go 堆包含客户端及工具；五轮结果只描述重复短会话行为。

## 当前边界

- 不发送 AudioProgress。专用 Gateway 接通配置必须显式 `MaxPendingAudioBytes=0`；这项处理积压保护当前不可用，不能用模型接收量或文本位置代替。
- 每场重新加载模型并独占 recognizer/stream。300ms/600ms 前后 padding 不计入原始音频；不启用 endpoint/reset。长问诊分段与共享预加载尚未实现。
- 取消在原生调用之间观察；不会并发删除正在使用的对象。SIGINT/SIGTERM 先等待五秒自然结束，超时后取消 RPC 并等待 handler 清理，退出 1。原生永久卡住时不能保证进程在五秒退出。
- 部分绑定方法不返回错误，Go stream 包装对象也不暴露底层 C 指针状态；不承诺捕获全部原生故障。没有新增处理位置、说话人或准确率保证。
- 动态库按 SDK 自带 rpath 加载，移动二进制仍需保留相应原生库路径；当前没有自包含分发包。

原生接入边界见[组装记录](../../docs/worklog/2026-10-26-sherpa-engine-assembly.md)，短音频完整链路见[Gateway/v2 验收](../../docs/worklog/2026-10-26-real-asr-gateway-v2.md)。
