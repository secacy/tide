# Tide

Tide 是面向医疗问诊场景的实时语音通信网关。它接入浏览器麦克风产生的 PCM16 音频，通过双向流式 gRPC 转发给 ASR，并将 partial/final 转录实时送回浏览器。网关不保存音频或转录正文，真实 ASR 与自动病案生成不属于本仓库。

## 架构

```text
Browser ── WebSocket ──> Edge gateway ── peer gRPC ──> Owner gateway
   ▲                                                        │
   └──────────────── transcript events ─────────────────────┤
                                                            ▼
                                                     bidirectional gRPC
                                                            │
                                                           ASR

                         Redis
               session / generation / owner lease
                 （不含音频和转录正文）
```

逻辑 `stream_id` 与 WebSocket 连接分离。连接断开后，逻辑流保留 30 秒；新连接消费轮换后的 resume token 并增加 attachment generation，旧连接随即失效。每条流只有 owner 节点持有 ASR 流，连接落到其他节点时由内部 `GatewayPeer.Relay` 转发。

Owner 宕机或优雅下线后，新节点取得过期租约并增加 `epoch`。客户端收到 `discontinuity`，继续使用同一 `stream_id`，但 ASR 上下文会重新建立，不承诺无损恢复。

## 本地运行

要求 Go 1.26。原生运行不依赖 Redis，此时使用内存会话存储，只支持单节点：

```bash
# 终端 1
go run ./cmd/mock-asr

# 终端 2
go run ./cmd/gateway
```

打开 <http://localhost:8080>，允许麦克风权限即可体验。开发模式页面会从 `/dev/token` 获取 15 分钟临时 JWT；该端点在 `TIDE_ENV=production` 时不存在。

也可以使用容器启动 Redis、mock ASR 与网关：

```bash
docker compose up --build
```

## HTTP 与 WebSocket 协议

创建逻辑流：

```http
POST /v1/streams
Authorization: Bearer <JWT>
Content-Type: application/json

{"language_code":"zh-CN"}
```

JWT 必须包含 `iss`、`aud`、`exp`、`sub` 与 `tenant_id`。生产环境通过 `TIDE_JWT_JWKS_URL` 校验 RS256；开发环境也支持配置的 HS256 secret。

响应：

```json
{
  "stream_id": "9b4a…",
  "websocket_url": "ws://localhost:8080/v1/streams/9b4a…/ws",
  "attach_token": "<short-lived, one-time token>",
  "expires_at": "2026-07-26T12:00:00Z",
  "audio": {
    "encoding": "pcm_s16le",
    "sample_rate_hz": 16000,
    "channels": 1,
    "frame_ms": 40
  }
}
```

WebSocket 升级后必须在 3 秒内发送：

```json
{"type":"hello","token":"<attach-or-resume-token>"}
```

服务端返回新的、单次使用的 resume token：

```json
{
  "type": "ready",
  "stream_id": "9b4a…",
  "epoch": 1,
  "next_sample_offset": 0,
  "resume_token": "<rotated-token>",
  "expires_at": "2026-07-26T12:02:00Z"
}
```

音频采用二进制消息：

- 前 8 字节：大端 `uint64 sample_offset`，表示从逻辑流开始累计的采样点位置。
- 后续字节：PCM16LE；常规帧为 1,280 字节，即 640 个采样点/40 ms。
- PCM 最少 2 字节、最多 1,280 字节且必须为偶数长度。
- 重复 offset 会被去重并返回当前 ACK；出现缺口会返回 `invalid_offset`，不会静默丢帧。

服务端 JSON 事件：

- `ack`: `next_sample_offset`
- `transcript`: `epoch`、`segment_id`、`revision`、`text`、`is_final`、`start_ms`、`end_ms`
- `discontinuity`: `previous_epoch`、`epoch`、`reason`
- `error`: 稳定的 `code`、安全的 `message`、`retryable`
- `ended`: `reason`

客户端以 `{"type":"end"}` 显式结束，也可调用：

```http
DELETE /v1/streams/{stream_id}
Authorization: Bearer <JWT>
```

DELETE 幂等，只允许同一租户操作。

## ASR 适配

公共契约在 [`proto/tide/asr/v1/asr.proto`](proto/tide/asr/v1/asr.proto)。`Transcribe` 是双向流：

- 网关发送 `Start`、`Audio`、`End`；
- ASR 返回 `Ready`、`Ack`、`Transcript`、`Error`；
- `Start` 包含固定音频配置、逻辑 stream、epoch 与恢复时的初始 offset。

接入真实 ASR 时，实现该服务或在服务前增加厂商 adapter。生成代码：

```bash
make generate
```

Buf 使用远程插件，不要求本机安装 `protoc`。

## 配置与安全

主要配置：

| 环境变量 | 默认值 | 说明 |
|---|---:|---|
| `TIDE_ENV` | `development` | `production` 会启用强制安全校验 |
| `TIDE_HTTP_ADDR` | `:8080` | HTTP/WebSocket 地址 |
| `TIDE_METRICS_ADDR` | `:8081` | 仅供管理网络访问的 Prometheus 地址 |
| `TIDE_PEER_ADDR` | `:9090` | 内部 peer gRPC 监听地址 |
| `TIDE_PEER_ADVERTISE_ADDR` | `127.0.0.1:9090` | 写入 owner 目录的可达地址 |
| `TIDE_ASR_ADDR` | `127.0.0.1:9091` | ASR gRPC 地址 |
| `TIDE_REDIS_ADDR` | 空 | 空值使用内存存储；生产必填 |
| `TIDE_TOKEN_SECRET` | 仅开发值 | attach/resume token 签名密钥，至少 32 字节 |
| `TIDE_JWT_JWKS_URL` | 空 | 生产必填 |
| `TIDE_JWT_ISSUER` | `tide-dev` | access JWT issuer |
| `TIDE_JWT_AUDIENCE` | `tide-gateway` | access JWT audience |
| `TIDE_ALLOWED_ORIGINS` | 本机地址 | 逗号分隔的 WebSocket Origin host pattern |
| `TIDE_MAX_CONNECTIONS` | `10000` | 节点连接过载阈值 |
| `TIDE_TENANT_MAX_STREAMS` | `1000` | Redis 中执行的租户活跃流配额 |
| `TIDE_CREATE_RATE_PER_MIN` | `120` | 每节点、每租户的逻辑流创建速率 |
| `TIDE_CONNECT_RATE_PER_MIN` | `6000` | 每节点、每租户的 WebSocket 挂载速率 |
| `TIDE_OWNER_LEASE` | `10s` | owner 租约 |
| `TIDE_OWNER_RENEW` | `3s` | owner 续租间隔 |
| `TIDE_DETACH_WINDOW` | `30s` | 断线续接窗口 |
| `TIDE_MAX_SESSION_AGE` | `4h` | 会话最大时长 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 空 | 配置后启用 OTLP Trace |

生产模式还要求：

- `TIDE_PUBLIC_WS_BASE_URL`
- `TIDE_ASR_TLS_CA`、`TIDE_ASR_TLS_CERT`、`TIDE_ASR_TLS_KEY`
- `TIDE_PEER_TLS_CA`、`TIDE_PEER_TLS_CERT`、`TIDE_PEER_TLS_KEY`

日志、Trace 和指标不记录 token、PCM、转录正文、租户 ID、用户 ID 或患者信息。`/metrics` 仅由独立的 8081 管理端口暴露。ASR 和 peer 使用 TLS/mTLS；入口 TLS 由 Ingress 或负载均衡器终止。

Kubernetes 示例在 [`deploy/k8s/gateway.yaml`](deploy/k8s/gateway.yaml)。部署前必须创建：

- `tide-runtime` Secret：`redis-address`、`redis-password`、`token-secret`
- `tide-mtls` Secret：`ca.crt`、`tls.crt`、`tls.key`

HPA 的 `tide_active_connections` 需要 Prometheus Adapter 暴露为 Pods custom metric；没有 adapter 时应暂时删除该指标，仅使用 CPU 扩缩容。

## 测试与压测

```bash
make lint
make test
make race
make build
```

测试包含 token 轮换与防重放、会话状态/配额、Redis Lua、owner 租约接管、跨节点 peer relay、协议长度校验、协议 fuzz seed，以及真实 WebSocket 到内存 gRPC ASR 的端到端链路。设置 `TIDE_TEST_REDIS_ADDR` 可额外运行真实 Redis 集成测试。

本地压测：

```bash
# 网关默认创建速率较低，压测前提高：
TIDE_CREATE_RATE_PER_MIN=20000 \
TIDE_TENANT_MAX_STREAMS=12000 \
go run ./cmd/gateway

go run ./cmd/loadgen \
  -gateway http://127.0.0.1:8080 \
  -connections 10000 \
  -ramp 2m \
  -duration 30m
```

`loadgen` 发送 40 ms 静音 PCM 帧，统计连接、帧、ACK、转录和 ACK 往返 P50/P95/P99。正式 1 万流验收应在 16 vCPU、32 GiB、10 GbE 节点进行；单向 PCM 输入约 320 MB/s，连同 ASR 转发需为网络和代理预留超过 5 Gbit/s 的持续吞吐。

Prometheus 指标：

- `tide_active_connections`
- `tide_active_owned_streams`
- `tide_streams_created_total`
- `tide_audio_bytes_total`
- `tide_audio_relay_seconds`
- `tide_result_relay_seconds`
- `tide_errors_total{code}`

目标是两个网关转发方向均达到 P95 ≤ 50 ms；该指标不包含 ASR 推理时间。
