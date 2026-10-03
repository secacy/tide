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
| `-debug-listen` | 空字符串 | 可选 HTTP 状态查询监听地址；空值关闭，纯空白拒绝；与处理限制开关独立 |
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

开启 Worker 状态查询时，为每个实例指定独立地址。以下使用编译后的程序，便于直接管理进程信号：

```bash
go build -o /tmp/tide-asr-worker ./cmd/asr-worker
/tmp/tide-asr-worker -listen=127.0.0.1:50051 -debug-listen=127.0.0.1:50081 -processing-concurrency=1 -processing-delay=10ms -response-delay=0s
```

在另一个终端查询：

```bash
curl -i http://127.0.0.1:50081/debug/worker
```

启用限制且空闲时返回：

```json
{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":1,"in_use":0,"waiting":0}}
```

若 `-processing-concurrency=0`，即使启用 HTTP，也返回 `processing_limit_enabled=false` 和 `processing=null`，表示没有这项名额计数，不能解释为没有处理任务。`in_use` 是已登记持有的处理名额，`waiting` 是已登记、尚未完成获取或取消收尾的请求数，均不是活跃会话数或 CPU 使用率。查询支持 GET/HEAD，响应禁止缓存；网络查询失败应保留未知状态，不能记为零。

`-debug-listen=127.0.0.1:0` 可由系统分配 HTTP 端口，启动日志的 `debug_address` 记录实际地址；`address` 仍是 gRPC 地址，`debug_enabled` 表示是否启用查询。默认不创建 HTTP 监听器。`:50081` 是通配监听地址，`/debug` 路径本身不限制访问范围，本机实验使用 `127.0.0.1`。

HTTP 显式启用时，两个端口全部取得成功后才运行服务，HTTP 绑定失败会释放此前取得的 gRPC 端口；两个服务查询/处理同一个 Worker。运行中任何一方异常会触发另一方清理。收到 SIGINT（Ctrl+C）或 SIGTERM 后，两个服务共享五秒自然收尾窗口；正常收尾退出 0，到期强制关闭并保留错误，退出 1。强制停止会中断未完成的转录，不能记为成功完成。五秒限制的是优雅等待阶段，不是任意 handler 退出的硬期限；HTTP 服务另有 5 秒请求头/写回期限和 30 秒空闲连接期限，这些均为开发保护值。

Gateway 默认使用 `localhost:50051`；配置上述两个 Worker 时：

```bash
go run ./cmd/gateway -workers=127.0.0.1:50051,127.0.0.1:50052
```

`-workers` 为逗号分隔的有序 `host:port` 列表，IPv6 使用 `[::1]:50051` 格式。每项首尾空白会去除；空项、重复地址、非法格式或非 1～65535 的纯数字端口会导致启动失败。地址按去空白后的字符串查重，不合并域名/IP 别名；本步不支持 `dns:///...` 等解析器 URI。重复指定 `-workers` 时使用最后一个值。

`-worker-strategy` 默认为 `round_robin`。新会话在合法 `start` 后选择一次 Worker，整场固定使用该后端。普通轮询按地址列表循环；加权轮询按配置比例选择新会话，每个后端复用一个 gRPC 客户端，Gateway 退出时统一关闭。

配置静态平滑加权轮询：

```bash
go run ./cmd/gateway -workers=127.0.0.1:50051,127.0.0.1:50052 -worker-strategy=weighted_round_robin -worker-weights=2,1 -max-pending-audio-bytes=32000
```

权重按地址位置对应，上例第一个后端权重 2、第二个权重 1。每项必须为十进制正整数，允许首尾空白、正号和前导零；数量须等于地址数，总和不超过 1000000。加权策略必须显式提供权重，普通轮询不得提供权重参数（包括空值）；未知策略导致启动失败。权重表示新会话选择比例，不表示并发上限，也不保证任意时刻活跃会话比例。

启动日志在选择器创建成功后记录 `strategy`、有序 `workers` 和对应 `weights`，含空格的列表会由 slog 加引号。配置日志不表示后端在线；监听成功另有日志。当前没有健康剔除、自动调权或跨 Worker 自动重试。可运行接入已通过验收，性能收益待同条件实验验证。

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


## Worker 处理状态采样

先启动启用了调试监听和共享处理限制的 Worker（独立终端）：

```bash
go run ./cmd/asr-worker -listen=127.0.0.1:50051 -debug-listen=127.0.0.1:50081 -processing-concurrency=1 -processing-delay=10ms
```

从项目根目录构建并运行独立采样器：

```bash
go build -o /tmp/tide-worker-sampler ./cmd/worker-sampler
/tmp/tide-worker-sampler \
  -url=http://127.0.0.1:50081/debug/worker \
  -interval=100ms \
  -request-timeout=1s \
  -duration=10s \
  -output-dir=/tmp/tide-worker-observation-001
```

`-url` 默认 `http://localhost:50081/debug/worker`；其余参数默认值与上面的 Gateway 采样命令相同。输出目录必须不存在，父目录必须存在；再次运行请换新目录名。观察负载时，需要另行启动 Gateway 和客户端，采样命令自身不产生音频请求。

输出 `samples.jsonl` 与 `manifest.json`，均带 `schema_version: 1` 和 `source_kind: "worker"`。清单中的 `output_path` 是相对路径。样本保留查询时间、耗时、状态或错误：

- 限制已启用：`state.processing_limit_enabled=true`，`state.processing` 包含 `limit`、`in_use`、`waiting`。
- 限制未启用：查询成功，但 `state.processing_limit_enabled=false`、`state.processing=null`；不能算作零占用。
- 查询失败：`state=null`、`error` 为错误文本；保留失败样本并继续查询，不能用前一条状态填补。

按期结束、至少一次查询成功且两份文件保存成功时退出 0；未启用限制也属于查询成功，因此退出 0 不代表取得了容量分析所需的计数。混合成功和失败允许退出 0，分析仍须检查失败数、采样间隔和负载窗口覆盖。全失败、提前停止或保存失败退出 1；Ctrl+C/SIGTERM 会尽力保存已采集数据再退出 1。

正常预算到期时，清单仍保留 `stop_reason=deadline_exceeded` 和原始 `sampling_error`。仅在命令返回后读取完整产物；失败文件保留，文件写入和关闭成功不代表掉电持久化或跨文件原子发布。本轮仅完成采样工具，联合负载实验和稳定容量结论另行记录。

## 单 Worker 逐级加压实验

从项目根目录运行（脚本会构建并管理自己的进程；8080 被占用时中止）：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/run_pressure_sweep.py \
  --output /tmp/tide-pressure-sweep-new
```

目录必须不存在。固定 4/8/12 场、每场 20 秒、各三批，另有预热和每批 40 秒采样窗口，整轮约六分钟。输出包含客户端成功和失败报告、Gateway/Worker 样本、配置与退出记录以及分组分析。实验脚本成功退出表示证据完整通过校验；查看 `groups.json` 的 `all_repetitions_meet_criteria` 判断各档位是否达标，不能把根清单的 `status=passed` 当作全部会话成功。具体预算、口径和已测结果见 [EXP-005-05](experiments/pressure-sweep.md)。

## 单 Worker 边界细化与两分钟观察

先执行短时细化，再由原始结果按预定义规则选点。两个输出目录均须不存在：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/run_boundary_study.py \
  --phase short --output /tmp/tide-boundary-short-new

# macOS：只在本次命令运行期间防止自动空闲休眠。
/usr/bin/caffeinate -i env PYTHONDONTWRITEBYTECODE=1 \
  python3 docs/experiments/scripts/run_boundary_study.py \
  --phase extended --source /tmp/tide-boundary-short-new \
  --output /tmp/tide-boundary-extended-new
```

短时 9/10/11 场各三批、每场 20 秒；延长观察为 8 场及规则选出的边界候选，各两批、每场 120 秒。完整两阶段约 16 分钟，需保持机器唤醒；空闲休眠保护不保证合盖、关机或进程中断时实验继续有效。出现采样覆盖不合格或文件未收尾时保留目录，不能把它当成容量结论，也不能拼接多个尝试中的好批次。

见 [EXP-005-06 报告](experiments/boundary-study.md)及[运行点清单](experiments/results/boundary-study-operating-points-2026-10-02.json)。

## 单 / 双 Worker 对照

```bash
/usr/bin/caffeinate -i env PYTHONDONTWRITEBYTECODE=1 \
  python3 docs/experiments/scripts/run_worker_comparison.py \
  --output /tmp/tide-worker-comparison-new

# 已完成目录可单独复算，不重新产生负载：
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/run_worker_comparison.py \
  --output /tmp/tide-worker-comparison-new --analyze-only
```

输出目录必须新建。两组均为 8/9/10 场各三批、每场 20 秒，总计约 13 分钟；先单 Worker，再双 Worker。Linux 可省略 `caffeinate -i`，但当前环境采集脚本使用 macOS 命令，需先适配环境记录。`comparison.json` 保留双方完整分母和逐 Worker 观测；增加资源后的改善不等于调度策略收益。见 [EXP-005-07](experiments/worker-count-comparison.md)。

## 双 Worker 逐级加压

```bash
/usr/bin/caffeinate -i env PYTHONDONTWRITEBYTECODE=1 \
  python3 docs/experiments/scripts/run_dual_worker_sweep.py \
  --output /tmp/tide-dual-worker-sweep-new

# 对已完成证据复算：
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/run_dual_worker_sweep.py \
  --output /tmp/tide-dual-worker-sweep-new --analyze-only
```

固定两个同配置 Worker，16/18/20 场各三批、每场 20 秒，整轮约六分钟。输出目录必须不存在；采集失败和业务失败均保留。`groups.json` 包含完成率、成功尾部、发送落后和逐 Worker 观测，根清单通过不代表所有档位达标。方案和后续选择规则见 [EXP-005-08](experiments/dual-worker-sweep.md)。

## 双 Worker 19 并发细测

```bash
/usr/bin/caffeinate -i env PYTHONDONTWRITEBYTECODE=1 \
  python3 docs/experiments/scripts/run_dual_worker_boundary.py \
  --source docs/experiments/results/dual-worker-sweep-2026-10-02 \
  --output /tmp/tide-dual-worker-boundary-new

# 对已完成证据复算：
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/run_dual_worker_boundary.py \
  --output /tmp/tide-dual-worker-boundary-new --analyze-only
```

源目录须为完整的双 Worker 16/18/20 采集，且达标次数为 3/3/0；读取时复核原始证据，记录来源哈希。新输出目录必须不存在。固定 19 场、20 秒、三批，预计约两分钟。条件和后续规则见 [EXP-005-09](experiments/dual-worker-boundary.md)。

## 双 Worker 两分钟观察

```bash
/usr/bin/caffeinate -i env PYTHONDONTWRITEBYTECODE=1 \
  python3 docs/experiments/scripts/run_dual_worker_extended.py \
  --points docs/experiments/results/dual-worker-short-operating-points-2026-10-02.json \
  --output /tmp/tide-dual-worker-extended-new

# 对已完成证据复算：
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/run_dual_worker_extended.py \
  --output /tmp/tide-dual-worker-extended-new --analyze-only
```

参考点及其原始来源须完整；读取时复核来源哈希和数字，新输出目录必须不存在。固定 16/18 场各两批、每场 120 秒，会话期限 140 秒、采样窗口 150 秒，总计约十分钟。两分钟通过属于筛查证据，方案和判据见 [EXP-005-10](experiments/dual-worker-extended.md)。

## 异速 Worker 轮询观察

固定两个单名额实例：10ms / 20ms，8/10/12 并发各三批、每场 20 秒。完整条件与解释边界见 [EXP-005-11](experiments/heterogeneous-workers.md)。

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off python3 docs/experiments/scripts/run_heterogeneous_workers.py --output docs/experiments/results/heterogeneous-workers-2026-10-02
```

输出目录须为新目录；加 `--analyze-only` 只复核已完成证据。

## protoc代码生成
```
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    routeguide/route_guide.proto
```
