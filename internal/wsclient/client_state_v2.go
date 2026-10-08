package wsclient

import (
	"errors"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

var (
	errV2ClientState              = errors.New("invalid v2 client state")       // 当前操作阶段或本地提交顺序非法。
	errV2ClientGenerationMismatch = errors.New("v2 client generation mismatch") // 事件所属连接不是当前代，不能修改本代状态。
	errV2ClientProtocol           = errors.New("invalid v2 server message")     // 服务端消息与已知身份、授权或完成范围不一致。
	errV2ClientResultGap          = errors.New("v2 client result gap")          // 结果序号跳过尚未应用的更新。
	errV2ClientCompleted          = errors.New("v2 client already completed")   // 已记录成功，不再开始新的业务操作。
)

// v2ClientState 保存一个存活客户端对本场会话已知的事实。
// 必须通过构造器创建，使用后不复制；仅由一个协调者串行访问。
// 不负责网络、超时、重试和资源关闭；含凭据，不得整体输出到日志。
type v2ClientState struct {
	audio *audioReplayBuffer // 独占缓存；外部不得绕过状态方法修改它。

	sessionID   string // 第一个合法 ready 确定，此后保持不变。
	resumeToken string // 同场恢复凭据，不得记录到普通日志。
	generation  uint64 // 最后接纳的服务端代次；尚无身份时为 0。
	attached    bool   // 当前连接已接纳 ready，可处理业务消息；不表示 socket 存活。

	sendOffset             uint64 // 本代下一次音频授权的起点；ready 时校准。
	offeredOffset          uint64 // 本场曾交给唯一写任务的最高连续音频末端。
	endOffered             bool   // 本场曾授权发送合法 end；发送失败也不撤销。
	endOfferedOnConnection bool   // 本代是否已授权 end，避免重复派发。
	inputEnded             bool   // 已从可信服务端消息得知 end 被接纳，不能回退。

	appliedSeq uint64 // 业务已连续成功应用的结果序号；尚无结果时为 0。
	completed  bool   // 已核对完整 completed 并记录成功；确认写失败不能撤销。
}

// newV2ClientState 创建尚无身份的状态，并按双预算创建独占音频缓存。
// maxBytes/maxChunks 含义和校验沿用 newAudioReplayBuffer。
func newV2ClientState(maxBytes uint64, maxChunks int) (*v2ClientState, error) {
	audio, err := newAudioReplayBuffer(maxBytes, maxChunks)
	if err != nil {
		return nil, err
	}
	return &v2ClientState{audio: audio}, nil
}

// appendAudio 复制新采集音频，返回逻辑起点；允许在握手或断线期间调用。
// 容量满、输入已封口等错误沿用缓存规则；不会推进发送授权位置。
func (s *v2ClientState) appendAudio(payload []byte) (uint64, error) {
	return s.audio.append(payload)
}

// sealInput 记录正常 EOF 并固定终点；允许重复调用，不代表 end 已发送。
func (s *v2ClientState) sealInput() uint64 {
	return s.audio.seal()
}

// acceptReady 校验并接纳当前尝试的第一条服务端业务消息。
// 调用方已排除旧尝试事件，且上代实际读写任务已经退出。
// 全部检查通过后，才共同提交身份/代次、音频确认及本代发送游标。
func (s *v2ClientState) acceptReady(message wsprotocol.ReadyMessage) error {
	if s.completed {
		return errV2ClientCompleted
	}
	if s.attached {
		return errV2ClientState
	}
	if message.Type != wsprotocol.MessageTypeReady || message.SessionID == "" || message.ResumeToken == "" {
		return errV2ClientProtocol
	}
	if s.generation == 0 {
		if s.offeredOffset != 0 || s.endOffered || s.appliedSeq != 0 {
			return errV2ClientState
		}
		if message.Generation != 1 || message.NextOffset != 0 || message.InputEnded || message.AckedResultSeq != 0 {
			return errV2ClientProtocol
		}
	} else if message.SessionID != s.sessionID || message.ResumeToken != s.resumeToken ||
		message.Generation <= s.generation || message.AckedResultSeq != s.appliedSeq {
		return errV2ClientProtocol
	}
	if message.NextOffset > s.offeredOffset || (s.inputEnded && !message.InputEnded) {
		return errV2ClientProtocol
	}
	if message.InputEnded && !s.validInputEnd(message.NextOffset) {
		return errV2ClientProtocol
	}
	if err := s.audio.checkReplayOffset(message.NextOffset); err != nil {
		return err
	}

	// 前面只读检查，不能用指针共享的浅副本预演缓存释放。
	if _, err := s.audio.acknowledge(message.NextOffset); err != nil {
		return err
	}
	s.sessionID, s.resumeToken = message.SessionID, message.ResumeToken
	s.generation, s.attached = message.Generation, true
	s.sendOffset = message.NextOffset
	s.endOfferedOnConnection = false
	s.inputEnded = message.InputEnded
	return nil
}

// detach 使指定代次不再接纳业务事件；旧代或重复通知返回 false。
// 保留身份、缓存、累计授权、结果应用及成功事实，不负责关闭或等待任务。
func (s *v2ClientState) detach(generation uint64) bool {
	if !s.attached || generation != s.generation {
		return false
	}
	s.attached = false
	return true
}

// authorizeAudio 为当前代唯一写任务准备下一块独立副本，并先登记授权。
// 无待发送音频返回零值,false,nil；出错不推进位置。
// 调用方保证没有其他待写/在途任务，返回后不得另行排队无界副本。
func (s *v2ClientState) authorizeAudio(generation uint64) (replayAudioChunk, bool, error) {
	if err := s.requireAttached(generation); err != nil {
		return replayAudioChunk{}, false, err
	}
	if s.completed {
		return replayAudioChunk{}, false, errV2ClientCompleted
	}
	chunk, ok, err := s.audio.copyChunkAt(s.sendOffset)
	if err != nil || !ok {
		return replayAudioChunk{}, false, err
	}
	// 这是派发授权，ACK 可以先于未来 Write 的完成事件到达。
	s.sendOffset = chunk.offset + uint64(len(chunk.data))
	s.offeredOffset = max(s.offeredOffset, s.sendOffset)
	return chunk, true, nil
}

// authorizeEnd 在封口、音频派发至末端、此前写任务实际返回后授权 end。
// 条件未齐、已知输入结束或本代已授权时返回 0,false,nil。
// 成功返回固定 finalOffset，并在实际 Write 前记录授权事实。
func (s *v2ClientState) authorizeEnd(generation uint64) (uint64, bool, error) {
	if err := s.requireAttached(generation); err != nil {
		return 0, false, err
	}
	if s.completed {
		return 0, false, errV2ClientCompleted
	}
	if !s.audio.sealed || s.sendOffset != s.audio.nextOffset || s.inputEnded || s.endOfferedOnConnection {
		return 0, false, nil
	}
	s.endOffered, s.endOfferedOnConnection = true, true
	return s.audio.nextOffset, true, nil
}

// acceptAudioAck 校验绑定代次、授权上界和 end 事实，再释放缓存前缀。
// 返回音频确认位置或 inputEnded 是否前进；旧 ACK 不使任何事实回退。
func (s *v2ClientState) acceptAudioAck(generation uint64, message wsprotocol.AudioAckMessage) (bool, error) {
	if err := s.requireAttached(generation); err != nil {
		return false, err
	}
	if message.Type != wsprotocol.MessageTypeAudioAck || message.Generation != generation ||
		message.NextOffset > s.offeredOffset || (message.InputEnded && !s.validInputEnd(message.NextOffset)) {
		return false, errV2ClientProtocol
	}
	advanced, err := s.audio.acknowledge(message.NextOffset)
	if err != nil {
		return false, err
	}
	changed := advanced || (message.InputEnded && !s.inputEnded)
	s.inputEnded = s.inputEnded || message.InputEnded
	// 恢复后 ACK 可能确认过去已授权、而本代尚未再次派发的前缀。
	s.sendOffset = max(s.sendOffset, s.audio.ackedOffset)
	return changed, nil
}

// checkResult 检查本代结果是否需要业务应用，只读状态。
// 已应用序号返回 false,nil；下一个连续序号返回 true,nil；跳号报缺口。
func (s *v2ClientState) checkResult(generation uint64, message wsprotocol.SequencedResultMessage) (bool, error) {
	if err := s.requireAttached(generation); err != nil {
		return false, err
	}
	if message.Type != wsprotocol.MessageTypeResult || message.Seq == 0 {
		return false, errV2ClientProtocol
	}
	if message.Seq <= s.appliedSeq {
		return false, nil
	}
	if s.completed {
		return false, errV2ClientCompleted
	}
	if message.Seq-s.appliedSeq != 1 {
		return false, errV2ClientResultGap
	}
	return true, nil
}

// commitApplied 仅在对应结果已被业务成功应用后调用，推进一个连续序号。
// 调用方把 checkResult/应用/commitApplied 串行执行，成功前不发 ACK。
func (s *v2ClientState) commitApplied(generation uint64, seq uint64) error {
	if err := s.requireAttached(generation); err != nil {
		return err
	}
	if s.completed {
		return errV2ClientCompleted
	}
	if seq <= s.appliedSeq || seq-s.appliedSeq != 1 {
		return errV2ClientState
	}
	s.appliedSeq = seq
	return nil
}

// acceptCompleted 校验本场输入终点、end 授权及连续结果末尾，然后记录成功。
// 首次成功返回 true,nil；当前代相同的重复通知返回 false,nil。
// 成功后才允许运行器构造并发送 completed_ack。
func (s *v2ClientState) acceptCompleted(generation uint64, message wsprotocol.CompletedMessage) (bool, error) {
	if err := s.requireAttached(generation); err != nil {
		return false, err
	}
	if message.Type != wsprotocol.MessageTypeCompleted || message.Generation != generation ||
		!s.validInputEnd(message.FinalOffset) || message.FinalOffset != s.offeredOffset || message.LastSeq != s.appliedSeq {
		return false, errV2ClientProtocol
	}
	if s.completed {
		return false, nil
	}
	// completed 也是最终输入已被接纳并处理的证据，不依赖先观察到 audio_ack。
	if _, err := s.audio.acknowledge(message.FinalOffset); err != nil {
		return false, err
	}
	s.sendOffset = message.FinalOffset
	s.inputEnded, s.completed = true, true
	return true, nil
}

// requireAttached 检查由任务绑定的代次；报文自带代次不得替代 generation 参数。
// 匹配本代但已经 detach 时也拒绝，不推断 socket 或任务已经退出。
func (s *v2ClientState) requireAttached(generation uint64) error {
	if generation == 0 || generation != s.generation {
		return errV2ClientGenerationMismatch
	}
	if !s.attached {
		return errV2ClientState
	}
	return nil
}

// validInputEnd 只读检查服务端声明的最终位置是否有合法本地输入/end 授权。
func (s *v2ClientState) validInputEnd(offset uint64) bool {
	return s.audio.sealed && s.endOffered && offset == s.audio.nextOffset
}
