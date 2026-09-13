## 运行
```bash
go run ./cmd/mock-asr
```

```bash
go run ./cmd/gateway
```

```bash
go run ./cmd/ws-client
```


## 多 Worker 本地运行

分别在两个终端启动 Mock：

```bash
TIDE_MOCK_ASR_ADDR=:50051 go run ./cmd/mock-asr
TIDE_MOCK_ASR_ADDR=:50052 go run ./cmd/mock-asr
```

创建本地 `gateway.json`：

```json
{
  "max_sessions": 8,
  "worker_policy": "least_reserved_ratio",
  "workers": [
    {"id": "asr-a", "address": "localhost:50051", "capacity": 2},
    {"id": "asr-b", "address": "localhost:50052", "capacity": 6}
  ]
}
```

```bash
TIDE_GATEWAY_CONFIG=./gateway.json go run ./cmd/gateway
```

`worker_policy` 可显式配置为 `round_robin` 或 `least_reserved_ratio`。[ADR-006](adr/ADR-006-least-reserved-ratio-selection.md) 确认当前固定 Worker 场景推荐 `least_reserved_ratio`，上面示例使用该策略；2/6 仍是演示配额，不能作为生产容量。总会话上限与各 Worker 配额独立检查，全部满额时在 WebSocket 升级前返回 HTTP 503。一次会话始终使用同一个 Worker。

配置仅在启动时读取。多 Worker 缺少策略、重复 ID/同字面地址、非正配额或未知字段均使启动失败。不要将同一后端用地址别名重复配置；当前没有后端身份探测。未设置 `TIDE_GATEWAY_CONFIG` 时保留单 Worker `localhost:50051`，总上限与 Worker 配额均为 100。

显式停止某 Worker 新预留使用应用内 `pool.StopAccepting(workerID)`；没有新增管理 HTTP 接口，也不自动检测健康。配置成功及 `/healthz` 不代表后端可达。应用创建的 gRPC 连接在关闭编排结束后关闭。

[ADR-007](adr/ADR-007-experimental-capacity-margin.md) 确认的 1/3 配额仅适用于 EXP-009 指定的共享槽位、12 ms 模拟处理和输入负载。复现该容量验证应使用 [EXP-009 的实验命令](experiments/EXP-009-multi-worker-capacity.md)，不能把上述启动示例当作同一工作负载。

命令行 Mock 仍按各 RPC 独立模拟处理；共享处理槽位由实验 Worker 提供，不能用这里的两个 Mock 进程直接证明模型容量。复现实验参见 [EXP-008](experiments/EXP-008-worker-selection.md)。

## WebSocket 心跳

Gateway 和 Go 客户端默认各自每两秒主动 Ping，写入和等待 Pong 共用三秒预算。两端可独立配置，启动后不热更新；空值或 `0` 使用默认值，负值和无效 duration 使启动失败。

```bash
TIDE_HEARTBEAT_INTERVAL=2s TIDE_HEARTBEAT_TIMEOUT=3s go run ./cmd/gateway
TIDE_HEARTBEAT_INTERVAL=2s TIDE_HEARTBEAT_TIMEOUT=3s go run ./cmd/ws-client
```

将间隔改为 `5s` 可复验较低探测频率，但检测目标相应约为八秒。参数作用于当前命令所在进程，不会自动同步给另一端。库调用者可设置 `gateway.Config.Heartbeat` / `wsclient.Config.Heartbeat`；`wsheartbeat.Config.Disabled` 是显式实验对照或外部接管入口，不提供隐式禁用的负数约定。

客户端仍须持续读取结果和控制帧；心跳不能只靠发送音频推进。v1 失败返回不代表已经重连或补齐；显式开启 v2 后的恢复与终止行为见下文。依据见 [ADR-008](adr/ADR-008-websocket-heartbeat-lifecycle.md)。

## protoc代码生成
```
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    routeguide/route_guide.proto
```

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

## 有限恢复客户端（v2）

先启动支持新 RPC 的 Mock 和 Gateway，再显式开启恢复：

```bash
TIDE_RECOVERY=true go run ./cmd/ws-client
```

默认不设置该变量时仍运行 v1；`false` 也使用 v1，无效布尔值使启动失败。v2 逐条打印已提交的稳定区间和文字，结束时输出完整 JSON 报告；有缺口、预算耗尽或其他失败时仍先输出报告，再以错误退出。恢复预算不会自动重新开始，调用方需显式决定下一次问诊识别。

十五秒缓存、十秒总预算及三秒 Ready 初值由 `wsclient.RecoveryConfig` 配置，本次没有新增这些参数的命令行开关。详细格式、缺口含义及 Source 约束见 [有限恢复协议](recovery-protocol.md)。Mock 的 checkpoint 不是实际模型能力证明。
