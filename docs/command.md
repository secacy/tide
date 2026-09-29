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

帮助或全部会话完整完成且报告写入、关闭成功时退出 0；参数、运行、非完整会话或保存错误退出 1。部分/全部会话失败仍会保存报告，`batch_error=null` 不代表全部会话完成，应结合逐场 outcome 和摘要。运行中 Ctrl+C/SIGTERM 会取消负载，等待收尾后保存报告并退出 1；运行开始前取消没有报告。

运行期间文件可能为空；写入失败可能留下不完整文件。仅在命令返回后读取报告，且检查命令错误。当前不承诺原子发布、掉电持久化或强杀后保留结果；关闭错误合并已代码审查，未通过实际磁盘故障注入验收。

此例是小批次操作示例，尚未作为容量实验记录。`-sessions` 是计划尝试数，不能解释为持续活跃连接数；第一块立即发送，音频时长也不等同于发送墙钟耗时。


## protoc代码生成
```
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    routeguide/route_guide.proto
```
