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
go run cmd/asr-worker/main.go
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
