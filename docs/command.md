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

```bash
go run cmd/ws-client/main.go
```


## protoc代码生成
```
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    routeguide/route_guide.proto
```