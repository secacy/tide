## wav转pcm
```bash
ffmpeg \
  -i "data/wav/sample-3s.wav" \
  -ar 16000 \
  -ac 1 \
  -acodec pcm_s16le \
  -f s16le \
  "data/pcm/test3s.pcm"
```


## 运行
```bash
go run ./cmd/asr-worker
```

Worker 已拆分为多个 Go 文件，使用包路径启动。查看帮助：

```bash
go run ./cmd/asr-worker -h
```

Worker 启动参数：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `-listen` | `:50051` | TCP 监听地址 |
| `-processing-concurrency` | `0` | 同一 Worker 共享的处理名额；0 关闭限制，负数启动失败 |
| `-processing-delay` | `0s` | 每个有效音频块的模拟处理耗时；允许 0，拒绝负数 |
| `-response-delay` | `50ms` | 每条 partial/final 文本响应的等待；进度消息不增加此等待；允许 0，拒绝负数 |

在两个终端分别启动配置不同的 Worker，例如：

```bash
go run ./cmd/asr-worker -listen=127.0.0.1:50051 -processing-concurrency=1 -processing-delay=10ms -response-delay=0s
```

```bash
go run ./cmd/asr-worker -listen=127.0.0.1:50052 -processing-concurrency=2 -processing-delay=20ms -response-delay=5ms
```

这些是启动示例，不代表稳定容量结论。启动日志记录实际监听地址和三个处理/响应参数；用 `-listen=127.0.0.1:0` 可让系统分配空闲端口，实际端口见日志。参数解析及 Worker 配置校验在监听之前完成，帮助正常退出，配置或监听错误非零退出。

Gateway 默认使用 `localhost:50051`；配置上述两个 Worker 时：

```bash
go run ./cmd/gateway -workers=127.0.0.1:50051,127.0.0.1:50052
```

`-workers` 为逗号分隔的有序 `host:port` 列表，IPv6 使用 `[::1]:50051` 格式。每项首尾空白会去除；空项、重复地址、非法格式或非 1～65535 的纯数字端口会导致启动失败。地址按去空白后的字符串查重，不合并域名/IP 别名；本步不支持 `dns:///...` 等解析器 URI。重复指定 `-workers` 时使用最后一个值。

新会话在合法 `start` 后按列表顺序轮询，整场固定使用选中的 Worker。每个后端复用一个 gRPC 客户端；Gateway 退出时统一关闭。启动日志记录地址列表与 `round_robin` 策略，配置成功不代表后端在线；当前没有健康剔除或跨 Worker 自动重试。

多 Worker 与积压预算可同时配置：

```bash
go run ./cmd/gateway -workers=127.0.0.1:50051,127.0.0.1:50052 -max-pending-audio-bytes=32000
```


```bash
go run ./cmd/gateway
```

默认关闭未确认音频预算。需要显式启用时：

```bash
go run ./cmd/gateway -max-pending-audio-bytes=32000
```

参数单位是原始音频字节；`0` 关闭，正值启用，负值启动失败。`32000` 在当前 16kHz、单声道、16-bit PCM 下相当于 1 秒音频，是实验候选值，不是实际等待时长。启用后 Worker 需要汇报处理进度，否则未确认量会随输入累积并触发超限。

```bash
go run ./cmd/gateway -h
```

Gateway 已拆分为多个 Go 文件，使用包路径启动，避免单独运行 `main.go` 时遗漏配置解析代码。

Gateway 状态查询（使用现有 HTTP 监听器）：

```bash
curl -i http://127.0.0.1:8080/debug/gateway
```

正常响应为 200，Content-Type: application/json，Cache-Control: no-store，例如：

```json
{"schema_version":1,"active_sessions":2,"max_sessions":64,"stopping":false}
```

这是查询接口自身的 v1 格式，与负载报告的 JSON v2 独立。active_sessions 表示已接纳、尚未完成清理的会话数，含连接升级、等待 start、识别和收尾；max_sessions 是配置接纳上限，不是实测稳定容量；stopping 表示已停止接纳新会话，不代表已有会话全部退出。

查询不占用会话名额，只要 HTTP 服务仍能响应，满额或停止接入都可返回当前快照。支持 HEAD（无响应体），其他方法返回 405。服务关闭或网络异常导致查询失败时，状态应记为未知，不能记为活动数零。此接口复用当前 :8080 监听范围，/debug 路径不意味着仅本机可达。定时采样可使用下文的独立 gateway-sampler 命令；此 HTTP 接口自身不启动后台采样，也不保存历史曲线。


```bash
go run cmd/ws-client/main.go
```


## 有限并发负载

先启动上文的 Mock Worker 与 Gateway，再从项目根目录构建负载程序。使用直接运行的二进制测试 Ctrl+C/SIGTERM，避免混入 `go run` 工具进程的信号行为。

```bash
go build -o /tmp/tide-loadgen ./cmd/loadgen
mkdir -p /tmp/tide-loadgen-reports
/tmp/tide-loadgen -h
```

运行两场、每场 2 秒静音 PCM 的短批次（当前默认 Mock 的尾部文本为“今天天气不错”）：

```bash
/tmp/tide-loadgen \
  -url=ws://localhost:8080/v1/asr \
  -sessions=2 \
  -audio-bytes=64000 \
  -chunk-bytes=3200 \
  -realtime=true \
  -session-timeout=10s \
  -expected-final-text='今天天气不错' \
  -output=/tmp/tide-loadgen-reports/smoke-001.json
```

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `-url` | `ws://localhost:8080/v1/asr` | Gateway WebSocket 地址 |
| `-sessions` | `1` | 有限批次计划会话数；结束后不补充新会话 |
| `-audio-bytes` | `1920000` | 每场静音 PCM 字节数，默认对应 60 秒音频 |
| `-chunk-bytes` | `3200` | 音频块上限；当前格式对应 100ms |
| `-realtime` | `true` | 按现有 Pacer 节奏发送；关闭用 `-realtime=false` |
| `-session-timeout` | `90s` | 包含连接、发送及尾部等待的单场期限 |
| `-expected-final-text` | 无，必填 | 与本次 Mock 配置一致的非空尾部文本 |
| `-output` | 无，必填 | JSON 文件路径；本轮不支持 `-` 标准输出 |

输出文件在负载开始前独占创建，已有路径会导致失败且不启动负载；再次运行请换新文件名。程序不自动创建父目录。报告保存共同配置、每场观测、错误及摘要；时间为 UTC，时长字段后缀 `_ns` 表示整数纳秒，`null` 表示缺失样本。

当前输出 `schema_version=2`。每场 `observation.audio_schedule_samples` 记录有计划且有实际开始时间的音频写入尝试数（包含失败尝试）；`max_audio_schedule_lag_ns` 为最大开始发送落后。无样本输出 `null`，有样本但未晚发输出 `0`。实验读取脚本兼容历史 v1，旧报告的缺失指标按未知处理，不补为零；这些每场最大值不能还原逐块落后的 p95。

帮助或全部会话完整完成且报告写入、关闭成功时退出 0；参数、运行、非完整会话或保存错误退出 1。部分/全部会话失败仍会保存报告，`batch_error=null` 不代表全部会话完成，应结合逐场 outcome 和摘要。运行中 Ctrl+C/SIGTERM 会取消负载，等待收尾后保存报告并退出 1；运行开始前取消没有报告。

运行期间文件可能为空；写入失败可能留下不完整文件。仅在命令返回后读取报告，且检查命令错误。当前不承诺原子发布、掉电持久化或强杀后保留结果；关闭错误合并已代码审查，未通过实际磁盘故障注入验收。

此例是小批次操作示例，尚未作为容量实验记录。`-sessions` 是计划尝试数，不能解释为持续活跃连接数；第一块立即发送，音频时长也不等同于发送墙钟耗时。



## 网关状态采样

先启动 Gateway，再从项目根目录构建独立采样命令。使用编译后的二进制验证信号与退出码：

```bash
go build -o /tmp/tide-gateway-sampler ./cmd/gateway-sampler
mkdir -p /tmp/tide-gateway-sampling
/tmp/tide-gateway-sampler -h
```

运行一个 30 秒的观察窗口：

```bash
/tmp/tide-gateway-sampler \
  -url=http://localhost:8080/debug/gateway \
  -interval=100ms \
  -request-timeout=1s \
  -duration=30s \
  -output-dir=/tmp/tide-gateway-sampling/run-001
```

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `-url` | `http://localhost:8080/debug/gateway` | 快照 HTTP/HTTPS 地址 |
| `-interval` | `100ms` | 上次查询及样本交付完成后的等待，不保证固定每秒十次 |
| `-request-timeout` | `1s` | 单次查询期限，含响应体读取；查询失败仍记录样本并继续 |
| `-duration` | `30s` | 整体采样预算；到期后仍需完成文件收尾，不是进程退出硬期限 |
| `-output-dir` | 无，必填 | 独占创建的新目录，父目录须已存在 |

三个时长必须为正，彼此不强制大小关系；实际请求也受剩余整体预算限制。目录不能使用 `-`，已有目录即使为空也拒绝，再次执行请换新的目录名。

每次运行保存 `samples.jsonl`（逐次查询，包括失败）和 `manifest.json`（最终记录报告）。清单的 `output_path` 相对于清单目录，整个目录可以一起移动。查询失败表示状态未知，不能解释为活动会话数零；成功查询的 active_sessions=0 或 stopping=true 仍是有效观测。

请求帮助，或按计划到期、至少取得一个有效快照且两份文件保存成功时退出 0。混合成功/失败样本允许退出 0，必须继续核对成功/失败数量、采样缺口和观察窗口。全程无有效快照、提前停止、启动或保存失败退出 1；运行中 Ctrl+C/SIGTERM 会停止采样、尽力保存已有证据后主动退出 1。

清单记录核心层的真实停止原因：正常按期完成也会保留 `stop_reason=deadline_exceeded` 和 sampling_error，这与命令退出 0 不矛盾。启动日志记录参数，结束摘要记录样本总数、成功/失败数、两份产物路径与 manifest_saved；日志输出到 stderr。Duration 和信号来源没有新增到清单格式中，后续实验脚本还需归档启动参数、日志及实际退出状态。

应在命令返回后读取产物；manifest_saved=true 只表示清单写入和关闭成功，不证明网关健康、观察覆盖充分或掉电持久化。失败产物保留，不重试覆盖或删除；进程强杀可能留下空文件或部分文件，当前没有跨文件原子发布保证。真实磁盘故障尚未注入验收。

本命令独立于 loadgen，不会自动等待负载开始或识别负载结束。当前采用有限时长窗口；正式实验的进程协调、时间覆盖和数据分析仍需接入，以上用法不代表容量实验已完成。


## protoc代码生成
```
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    routeguide/route_guide.proto
```
