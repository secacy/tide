# 本地中文流式模型探针

独立 Go 模块，当前仅支持 macOS + cgo；主项目的 `go.mod` 与 Mock 测试不引入原生依赖。它验证单个模型的调用行为，尚未实现生产 Worker 适配器。

固定 Go 绑定 `sherpa-onnx-go-macos v1.13.8`，CPU/2 线程、greedy search、endpoint reset 关闭。模型为中英流式 Zipformer 的 int8 版本，revision 及七份资源的 SHA256 写在 `fetch_assets.py`。模型卡声明 Apache-2.0，来源和上游链接随下载保留。模型约 198MB，连同两个 WAV 等文件约 199MB；原生 Go 依赖另行下载，均不纳入 Git。

在本目录执行，路径换成尚不存在的位置：

```sh
python3 fetch_assets.py /tmp/tide-probe-model
go mod download
go test -race ./...
go build -o /tmp/tide-asrprobe .
/tmp/tide-asrprobe -model-dir /tmp/tide-probe-model \
  -wav /tmp/tide-probe-model/test_wavs/0.wav -output /tmp/tide-probe-speech
/tmp/tide-asrprobe -model-dir /tmp/tide-probe-model \
  -silence 2035ms -output /tmp/tide-probe-silence
/tmp/tide-asrprobe -model-dir /tmp/tide-probe-model \
  -wav /tmp/tide-probe-model/test_wavs/0.wav \
  -cancel-after 350ms -output /tmp/tide-probe-canceled
```

正常探针退出 0，取消/失败退出 1。输出目录独占创建，含逐调用 `events.jsonl` 与最终 `summary.json`；取消和可返回的错误也保存结果。原生进程崩溃或不可返回的调用不能保证保存 summary，调用者需另设进程保护。动态库按 Go 模块自带 rpath 加载，移动二进制后仍需保留对应模块库路径。

## 输入与结果含义

- WAV 上限 16MiB，严格限制为 PCM S16LE/16kHz/单声道；跳过 RIFF 容器、元数据及 padding，无静默重采样。`-silence` 生成确定的零样本，最长 30s。
- 默认按原始计划每 100ms 送 1600 样本，末块不足也保留；`-realtime=false` 可取消人为等待。计划以样本位置计算，计算变慢不移动未来排期，保存最大落后。
- 参照官方示例先送 300ms 零 padding，结束送 600ms 零 padding，再 InputFinished 和 drain。两类 padding 分别计数，绝不推进原始音频字节位置。此配置是首个探针约定，尚未做 padding/endpoint 策略优化。
- 同一场始终使用一个模型 stream。`text_snapshot` 和 `last_text_snapshot` 是整场当前文本快照，可能修订，不能逐条拼接；不是每块一个片段，也没有把观察到的最新文字标成片段 final。
- `completed_probe` 表示本地完整输入、结束刷新及可用解码循环返回，随后 stream/recognizer 的释放调用返回；不证明识别准确、完整覆盖所有样本、生产 gRPC completed 已接通或无内存泄漏。
- `processed_audio_bytes=null`：公开 API 没有经本步证明的连续已处理字节映射。提交量、文本末词时间及 IsReady=false 不用作替代值。
- 取消是合作式：在每个同步调用前后检查 context。截止时刻起不再启动新调用；已开始的 C 调用返回后才关闭 stream，再关闭 recognizer。没有让另一个 goroutine 释放正在使用的指针，也不承诺原生计算可被硬中断。

## 指标与限制

`load_ns` 是本进程创建模型和 stream 的耗时，不包含磁盘资源摘要、文件读取；后续独立进程可能使用操作系统文件缓存，不能叫四次冷启动。其余时刻使用加载后单调时间起点。`first_nonempty_text_ns` 为首次可观察非空快照时刻；`first_text_before_input_end` 同时核对还有原始块未提交。它不是每条文字相对对应语音的延迟。

`last_input_call_end_to_flush_end_ns` 从最后一块实际 Accept 返回计时，含剩余 drain、尾部 padding、InputFinished 和 drain。`engine_call_ns` 累计 Accept/Decode/GetResult/InputFinished 包围段（含 padding 分配和少量调用内记账），不含人为排期等待、模型加载、Ready 检查、日志输出和资源删除；`call_rtf` 为该累计量/原始音频时长，仅完整探针计算。含 padding、单场、无竞争，不能据此推算并发容量。`elapsed_ns` 额外包含排期、输出与模型释放。

可用 `-reference human.txt` 计算 CER：Unicode 小写、去空白及标点，保留数字/否定词/语气词，不做繁简转换。参考由调用方负责；工具不验证其来源。没有独立核对的人工作文时 `character_score=null`，不能用模型自身输出当参考。当前公开示例只用于接入行为，无临床或术语质量结论。

测试使用可控 engine 检查取消、关闭顺序、输出故障、原排期、短尾与 padding、字符计分及 WAV 格式。实际原生行为由工作日志中的探针运行另行核对。首轮与最终版证据分别保留；结果入口见[验收记录](../../docs/worklog/2026-10-25-real-asr-probe.md)。
