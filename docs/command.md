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


## protoc代码生成
```
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    routeguide/route_guide.proto
```
