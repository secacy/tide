# 中文流式候选：资产冻结、探针配置与技术冒烟

日期：2026-10-26。状态：技术冒烟完成；未开始六对质量测量，权重使用条件仍未确认。依据[模型对照设计](2026-10-26-real-asr-model-comparison-design.md)完成第一步，生产 Worker 仍使用原模型。


> 后续范围调整（2026-10-26）：本文保留当时的实验事实与决策。质量实验现已收口，下文关于继续核查错词、补充素材或选择模型的建议不再作为当前任务；后端开发不等待准确率改善。下一步见[真实 Worker 连续会话运行基线](2026-10-26-real-asr-backend-baseline-design.md)，重点验证生命周期、等待时间与资源释放。

## 结论与范围

`sherpa-onnx-streaming-zipformer-zh-int8-2025-06-30` 可由固定的 macOS Go SDK `v1.13.8` 加载。候选两段公开语音均在输入结束前出字并完成刷新；静音正常结束且无文字；350ms 主动取消场景未发送完整输入、未执行 Finish，释放调用返回。加上旧模型默认路径，共五场全部符合本步预期，没有模型加载、进程超时或推理失败。

这证明当前本机、短公开音频下的调用与收尾行为，不证明医疗质量、长时能力、硬取消或无泄漏。未对 q01～q06 运行新模型，未计算新模型 CER，也未改变 Gateway/Worker/V2Client 或主项目依赖。

## 资产身份与使用条件

- 来源为 sherpa 官方 release 资产 `268558776`，发布/更新时间为 2025-06-30。压缩包 **132634597B**，SHA256 为 `5a2832047ea1f97dd0dc595b816c230c4bafad65cfc0341fa57517cadc50afd0`，本地下载值与 GitHub API 的 digest 一致。
- 压缩包内 encoder、decoder、joiner、词表及四份辅助文件全部单独固定摘要，见[资产清单](../../experiments/asrprobe/model-assets.json)。decoder 为 `decoder.onnx`，不将整个包描述成所有组件都是 int8。
- 原始 checkpoint 元数据修订为 `87163dfd71478fe2397af74b841d7b666c90c665`，`gated=auto`、无公开 cardData；首次读取 README 返回 HTTP 401。维护者公开 clone 修订 `d17b865e7988f9e2f67d2b8bb4af24aa3be09f48` 的 README 只说明来源。导出包 README 也未声明权重许可证。
- 因此使用条件仍是 unresolved，不从 sherpa 或训练代码的源码许可推导权重许可。依照原设计，正式质量比较仍需处理这一候选资格缺口：补齐上游条件，或明确改选使用条件可确认的候选；本轮没有访问受限内容、接受上游条件或分发模型文件。

公开资料：[官方模型入口](https://k2-fsa.github.io/sherpa/onnx/pretrained_models/online-transducer/zipformer-transducer-models.html#sherpa-onnx-streaming-zipformer-zh-int8-2025-06-30-chinese)、[原始 checkpoint 页面](https://huggingface.co/yuekai/icefall-asr-multi-zh-hans-zipformer-large)、[维护者固定修订说明](https://huggingface.co/csukuangfj/icefall-asr-multi-zh-hans-zipformer-large/blob/d17b865e7988f9e2f67d2b8bb4af24aa3be09f48/README.md)。本步只记录可观察声明及缺口，不给出法律许可结论。

## 探针变化与取舍

在独立 `experiments/asrprobe` 模块内增加两个固定 profile，而非任意模型插件接口。默认 `zipformer-bilingual-2023` 保留旧文件映射与原生参数；候选 `zipformer-zh-2025` 独立声明新文件名及 `cjkchar`。模型目录负责位置，profile 负责加载方式，内嵌锁文件负责确切资产身份。未知 profile 或摘要不符在原生构造、输出目录创建之前拒绝。

新报告为 v3，保存与原生构造相同配置值中的模型字段、CPU provider、线程与解码配置，以及实际送样参数、文件和内嵌锁文件摘要。送样和报告引用同一组常量，避免文档写 100ms 而执行另一种块大小。文件解码、原排期、padding、逐调用记录和资源所有权保持既有语义。

复核器对 v1/v2 仍强制原模型摘要；v3 候选要求调用方提供运行前的独立期望，不能让报告自己决定预期身份。历史默认入口不接受候选冒充旧模型。未来锁文件有变化时，应使用运行前保存的清单或冻结复核源码，不能改写旧报告使之匹配新清单。

下载工具先核对发布压缩包的长度/摘要，再复制并核对指定成员；目标目录独占创建。实际发布包已通过该解包函数。小型本地归档测试覆盖损坏压缩包、成员摘要不符、拒绝覆盖和仅提取锁定成员。

## 新发现：特征配置也发生变化

本步读取 ONNX 顶层 metadata_props，未解析或验证完整计算图；实际兼容性由后续真实加载验证。候选 encoder 声明 `model_type=zipformer2`、`feature=whisper`、`decode_chunk_len=32`、`T=45`；旧 encoder 没有 `feature=whisper` 标记。

固定 SDK 对应的[原生实现](https://github.com/k2-fsa/sherpa-onnx/blob/f1c2e997470d4d435b4b4f5b0cd0e979697200f9/sherpa-onnx/csrc/online-recognizer-transducer-impl.h)根据 `UseWhisperFeature()` 设置内部 `is_whisper`。Go 绑定的 FeatureConfig 只暴露采样率/维数，两者相同不代表内部特征处理相同。相应源码快照、摘要与两组模型元数据已归档。

因此后续实验按**模型及配套特征配置组合对照**解释，不声称只改变权重。本轮没有调整送样、padding 或 SDK，也没有根据六段录音得分选择特征。100ms 仍只是探针提交节奏，不等于模型内部解码步长。

## 实际观察

五个独立进程顺序运行，窗口 **2026-10-26 13:13:13–13:13:44（Asia/Taipei）**；CPU 两线程、greedy/0、原排期 100ms 送样、300ms/600ms padding、关闭 endpoint。每场六十秒外部保护均未触发，持有 `caffeinate -i`。公开 WAV 没有重采样或改写；包中 8kHz WAV 不用于本轮。

| 场景 | 原始输入 / 实际提交 | 出字与结束 | 末块样本数 |
| --- | --- | --- | ---: |
| 候选公开语音 0 | 179568B / 179568B，5.6115 秒 | 输入期间出字；19 次解码、14 次快照变化，Finish/Close 返回 | 184 |
| 候选公开语音 1 | 164898B / 164898B，5.1530625 秒 | 输入期间出字；18 次解码、14 次快照变化，Finish/Close 返回 | 849 |
| 候选静音 | 65120B / 65120B，2.035 秒 | 无文字；Finish/Close 返回 | 560 |
| 候选主动取消 | 179568B / 12800B | 退出 1、status=canceled；无 Finish/尾部补零，Close 返回 | 1600 |
| 默认旧模型公开语音 | 321700B / 321700B | 输入期间出字；Finish/Close 返回 | 850 |

候选两段首次非空快照分别在加载后约 **824.58ms / 423.14ms**；末块 Accept 返回到刷新完成约 **67.33ms / 40.77ms**，最大排期落后约 **1.69ms / 1.42ms**。每场一次，仅描述本轮观测；这些数值不含模型加载，也不是对应词语的声学延迟或生产 SLA。

候选语音 0 最后全文为“对我做了介绍哈那么我想说的是呢大家如果对我的研究感兴趣呢”；语音 1 为“重点呢想谈三个问题首先呢就是这一轮全球金融动荡的表现”。没有经独立确认的人类参考，不计算公开样例的 CER。

取消期限为 350ms，加载后探针总耗时含释放约 **375.12ms**。这是合作取消场景的一次观察，不是原生计算可在 25ms 内被中断的保证，也没有覆盖慢原生调用期间的真实取消。后者所有权约束由可控 engine 测试覆盖，真实最坏等待仍未知。

## 验证与证据

Go 模块 race 回归和 vet 通过；Python 质量工具 17 项及候选归档校验测试 1 项通过。测试核对两组真实 SDK 字段映射、非法 profile/错误资产提前拒绝、v3 模型和送样配置校验、旧版不能被解释成候选。实际五场原始事件另行核对输入块、padding、字节量、全文快照、释放和退出状态。

v1 的两场直接诊断仍为 2/2 可复核；v2 的十二场解码对照仍完整，原先不利的 -7.5 个百分点收益保持原值。七类损坏 v3 证据均被拒绝，覆盖错误 profile、encoder 摘要、清单摘要、缺失 profile、padding、修改全文及未完成释放。

初次格式化命令误用工作目录，出现路径不存在，随后修正；该次 Go 测试仍通过。上游 README 的 HTTP 401 及访问条件缺口保留概述与公开元数据。第一次归档检查拒绝了元数据检查产生的 Python 缓存文件，删除生成缓存后重新归档，未重跑推理。模型下载摘要校验、真实解包与五场原生运行均首轮通过，没有重试后替换失败场的情况。

执行前冻结探针源码/配置、计划、输入/模型/程序/随包 dylib 摘要，执行后、文档更新前核对均未变。随包库摘要包含不同架构文件，不代表全部被本进程加载。最终说明文档随后更新，归档保留运行前版本。

[证据归档](../experiments/results/real-asr-model-smoke-2026-10-26/README.md)为 90732B、70 文件，含五场原始事件/报告、命令及退出状态、资产来源与元数据、冻结源码/计划、测试、重算脚本与反例；不包含音频、模型或二进制。全部 70 文件搬移读回摘要一致，五场复核、七类反例及 Python 18 项测试搬移执行通过。临时模型位于 `/private/tmp/tide-model-preparation-2026-10-26/locked-model`，原始运行位于 `/private/tmp/tide-model-smoke-2026-10-26-1`，二进制为 `/private/tmp/tide-model-profile-probe-2026-10-26`；复核归档不依赖这些本地路径。

后续同日[正式计划](../experiments/real-asr-model-comparison.md)调整前置范围：权重使用条件继续作为采用与分发前待办，本机离线六对技术测量继续，不据此确认许可。新素材、人工关键事实审核与真实链路确认继续作为后续关卡，不将本步冒烟通过记为质量改善。
