package wsclient

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// snapshotV2Client 深拷贝真实缓存；拒绝操作必须保持全部事实及音频不变。
func snapshotV2Client(s *v2ClientState) v2ClientState {
	before := *s
	audio := snapshotAudioReplay(s.audio)
	before.audio = &audio
	return before
}

func assertV2ClientUnchanged(t *testing.T, s *v2ClientState, before v2ClientState) {
	t.Helper()
	if after := snapshotV2Client(s); !reflect.DeepEqual(after, before) {
		t.Fatal("client facts or retained audio changed during rejected/read-only operation")
	}
	assertV2ClientInvariant(t, s)
}

func assertV2ClientInvariant(t *testing.T, s *v2ClientState) {
	t.Helper()
	assertAudioReplayInvariant(t, s.audio)
	if s.audio.ackedOffset > s.sendOffset || s.sendOffset > s.offeredOffset || s.offeredOffset > s.audio.nextOffset {
		t.Fatalf("invalid positions: acked=%d send=%d offered=%d captured=%d", s.audio.ackedOffset, s.sendOffset, s.offeredOffset, s.audio.nextOffset)
	}
	if s.generation == 0 {
		if s.attached || s.sessionID != "" || s.resumeToken != "" || s.offeredOffset != 0 || s.endOffered || s.appliedSeq != 0 {
			t.Fatal("unestablished client already has identity or business authorization")
		}
	} else if s.sessionID == "" || s.resumeToken == "" {
		t.Fatal("established client lost identity")
	}
	if (s.endOffered && (!s.audio.sealed || s.offeredOffset != s.audio.nextOffset)) ||
		(s.endOfferedOnConnection && !s.endOffered) ||
		(s.inputEnded && (!s.endOffered || s.audio.ackedOffset != s.audio.nextOffset)) ||
		(s.completed && !s.inputEnded) {
		t.Fatal("end/completion facts inconsistent with local input and acknowledgement")
	}
}

func newTestV2Client(t *testing.T, maxBytes uint64, maxChunks int) *v2ClientState {
	t.Helper()
	s, err := newV2ClientState(maxBytes, maxChunks)
	if err != nil {
		t.Fatal(err)
	}
	assertV2ClientInvariant(t, s)
	return s
}

func testV2Ready(generation, offset, applied uint64, ended bool) wsprotocol.ReadyMessage {
	return wsprotocol.ReadyMessage{
		Type: wsprotocol.MessageTypeReady, SessionID: "test-session", ResumeToken: "test-token",
		Generation: generation, NextOffset: offset, InputEnded: ended, AckedResultSeq: applied,
	}
}

func attachTestV2Client(t *testing.T, s *v2ClientState, message wsprotocol.ReadyMessage) {
	t.Helper()
	if err := s.acceptReady(message); err != nil {
		t.Fatal(err)
	}
	assertV2ClientInvariant(t, s)
}

func appendTestV2Audio(t *testing.T, s *v2ClientState, offset uint64, data string) {
	t.Helper()
	if got, err := s.appendAudio([]byte(data)); got != offset || err != nil {
		t.Fatalf("append = (%d, %v), want offset=%d", got, err, offset)
	}
	assertV2ClientInvariant(t, s)
}

// authorizeTestV2Audio 串行观察授权部件，不伪造实际网络写入或接纳。
// 调用者遵守状态方法契约，没有实际在途任务。
func authorizeTestV2Audio(t *testing.T, s *v2ClientState, offset uint64, data string) replayAudioChunk {
	t.Helper()
	chunk, ok, err := s.authorizeAudio(s.generation)
	if err != nil || !ok || chunk.offset != offset || string(chunk.data) != data {
		t.Fatalf("authorize = (%+v, %v, %v), want offset=%d data=%q", chunk, ok, err, offset, data)
	}
	assertV2ClientInvariant(t, s)
	return chunk
}

func ackTestV2Audio(t *testing.T, s *v2ClientState, offset uint64, ended, wantChanged bool) {
	t.Helper()
	changed, err := s.acceptAudioAck(s.generation, wsprotocol.AudioAckMessage{
		Type: wsprotocol.MessageTypeAudioAck, Generation: s.generation, NextOffset: offset, InputEnded: ended,
	})
	if err != nil || changed != wantChanged {
		t.Fatalf("ack = (%v, %v), want changed=%v", changed, err, wantChanged)
	}
	assertV2ClientInvariant(t, s)
}

func applyTestV2Result(t *testing.T, s *v2ClientState, seq uint64) {
	t.Helper()
	message := wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: seq, Text: "test"}
	if apply, err := s.checkResult(s.generation, message); !apply || err != nil {
		t.Fatalf("check result = (%v, %v)", apply, err)
	}
	if err := s.commitApplied(s.generation, seq); err != nil {
		t.Fatal(err)
	}
	assertV2ClientInvariant(t, s)
}

func newTestV2AudioClient(t *testing.T) *v2ClientState {
	t.Helper()
	s := newTestV2Client(t, 64, 8)
	for i, data := range []string{"abcd", "efgh", "ijkl"} {
		appendTestV2Audio(t, s, uint64(i*4), data)
	}
	attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
	return s
}

func offerAllTestV2Audio(t *testing.T, s *v2ClientState) {
	t.Helper()
	for i, data := range []string{"abcd", "efgh", "ijkl"} {
		authorizeTestV2Audio(t, s, uint64(i*4), data)
	}
}

func authorizeTestV2End(t *testing.T, s *v2ClientState, final uint64) {
	t.Helper()
	if got := s.sealInput(); got != final {
		t.Fatalf("seal = %d, want %d", got, final)
	}
	if offset, ok, err := s.authorizeEnd(s.generation); offset != final || !ok || err != nil {
		t.Fatalf("end = (%d, %v, %v)", offset, ok, err)
	}
	assertV2ClientInvariant(t, s)
}

func TestV2ClientStateConstruction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bytes  uint64
		chunks int
		valid  bool
	}{
		{"zero_bytes", 0, 1, false},
		{"zero_chunks", 1, 0, false},
		{"negative_chunks", 1, -1, false},
		{"smallest", 1, 1, true},
		{"maximum_bytes_without_payload", math.MaxUint64, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := newV2ClientState(tc.bytes, tc.chunks)
			if !tc.valid {
				if s != nil || !errors.Is(err, errInvalidReplayBufferLimits) {
					t.Fatalf("constructor = (%v, %v)", s != nil, err)
				}
				return
			}
			if err != nil || s == nil || s.audio == nil {
				t.Fatalf("valid constructor: present=%v err=%v", s != nil, err)
			}
			want := &v2ClientState{audio: newTestAudioReplayBuffer(t, tc.bytes, tc.chunks)}
			if !reflect.DeepEqual(s, want) {
				t.Fatal("constructor populated facts before establishment")
			}
			assertV2ClientInvariant(t, s)
		})
	}
}

func TestV2ClientStateFirstReadyRejection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*wsprotocol.ReadyMessage)
	}{
		{"wrong_type", func(m *wsprotocol.ReadyMessage) { m.Type = wsprotocol.MessageTypeAudioAck }},
		{"missing_id", func(m *wsprotocol.ReadyMessage) { m.SessionID = "" }},
		{"missing_token", func(m *wsprotocol.ReadyMessage) { m.ResumeToken = "" }},
		{"zero_generation", func(m *wsprotocol.ReadyMessage) { m.Generation = 0 }},
		{"not_first_generation", func(m *wsprotocol.ReadyMessage) { m.Generation = 2 }},
		{"max_generation", func(m *wsprotocol.ReadyMessage) { m.Generation = math.MaxUint64 }},
		{"nonzero_audio_before_authorization", func(m *wsprotocol.ReadyMessage) { m.NextOffset = 4 }},
		{"input_ended_before_authorization", func(m *wsprotocol.ReadyMessage) { m.InputEnded = true }},
		{"result_confirmed_before_application", func(m *wsprotocol.ReadyMessage) { m.AckedResultSeq = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2Client(t, 12, 3)
			appendTestV2Audio(t, s, 0, "abcd")
			message := testV2Ready(1, 0, 0, false)
			tc.mutate(&message)
			before := snapshotV2Client(s)
			if err := s.acceptReady(message); !errors.Is(err, errV2ClientProtocol) {
				t.Fatalf("ready = %v", err)
			}
			assertV2ClientUnchanged(t, s, before)
		})
	}
}

func TestV2ClientStateAuthorizationAndSealing(t *testing.T) {
	s := newTestV2Client(t, 12, 3)
	for i, data := range []string{"abcd", "efgh", "ijkl"} {
		appendTestV2Audio(t, s, uint64(i*4), data)
	}
	before := snapshotV2Client(s)
	if chunk, ok, err := s.authorizeAudio(1); ok || chunk.data != nil || !errors.Is(err, errV2ClientGenerationMismatch) {
		t.Fatalf("audio before ready = (%+v, %v, %v)", chunk, ok, err)
	}
	if _, ok, err := s.authorizeEnd(1); ok || !errors.Is(err, errV2ClientGenerationMismatch) {
		t.Fatalf("end before ready = (%v, %v)", ok, err)
	}
	assertV2ClientUnchanged(t, s, before)
	attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
	before = snapshotV2Client(s)
	if err := s.acceptReady(testV2Ready(1, 0, 0, false)); !errors.Is(err, errV2ClientState) {
		t.Fatalf("duplicate ready on attached connection = %v", err)
	}
	if offset, ok, err := s.authorizeEnd(1); offset != 0 || ok || err != nil {
		t.Fatalf("end before EOF = (%d, %v, %v)", offset, ok, err)
	}
	assertV2ClientUnchanged(t, s, before)
	chunk := authorizeTestV2Audio(t, s, 0, "abcd")
	clear(chunk.data)
	assertReplayChunk(t, s.audio, 0, "abcd")
	if s.sendOffset != 4 || s.offeredOffset != 4 || s.audio.ackedOffset != 0 || s.audio.count != 3 {
		t.Fatal("authorization must advance offered position but retain all unacknowledged audio")
	}
	if s.sealInput() != 12 || s.sealInput() != 12 {
		t.Fatal("sealing did not fix final offset")
	}
	before = snapshotV2Client(s)
	if _, err := s.appendAudio([]byte("more")); !errors.Is(err, errReplayBufferSealed) {
		t.Fatalf("append after EOF = %v", err)
	}
	if offset, ok, err := s.authorizeEnd(1); offset != 0 || ok || err != nil {
		t.Fatalf("end before remaining audio = (%d, %v, %v)", offset, ok, err)
	}
	assertV2ClientUnchanged(t, s, before)
	authorizeTestV2Audio(t, s, 4, "efgh")
	authorizeTestV2Audio(t, s, 8, "ijkl")
	before = snapshotV2Client(s)
	if chunk, ok, err := s.authorizeAudio(1); chunk.offset != 0 || chunk.data != nil || ok || err != nil {
		t.Fatalf("audio at end = (%+v, %v, %v)", chunk, ok, err)
	}
	assertV2ClientUnchanged(t, s, before)
	if offset, ok, err := s.authorizeEnd(1); offset != 12 || !ok || err != nil {
		t.Fatalf("authorized end = (%d, %v, %v)", offset, ok, err)
	}
	if !s.endOffered || !s.endOfferedOnConnection || s.inputEnded || s.completed {
		t.Fatal("end authorization must not imply server acceptance/completion")
	}
	before = snapshotV2Client(s)
	if offset, ok, err := s.authorizeEnd(1); offset != 0 || ok || err != nil {
		t.Fatalf("repeated end = (%d, %v, %v)", offset, ok, err)
	}
	assertV2ClientUnchanged(t, s, before)
}

func TestV2ClientStateAudioAckRejection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*wsprotocol.AudioAckMessage)
		want   error
	}{
		{"wrong_type", func(m *wsprotocol.AudioAckMessage) { m.Type = wsprotocol.MessageTypeReady }, errV2ClientProtocol},
		{"missing_message_generation", func(m *wsprotocol.AudioAckMessage) { m.Generation = 0 }, errV2ClientProtocol},
		{"wrong_message_generation", func(m *wsprotocol.AudioAckMessage) { m.Generation = 3 }, errV2ClientProtocol},
		{"cached_but_not_offered", func(m *wsprotocol.AudioAckMessage) { m.NextOffset = 12 }, errV2ClientProtocol},
		{"ahead_of_cache", func(m *wsprotocol.AudioAckMessage) { m.NextOffset = 16 }, errV2ClientProtocol},
		{"max_offset", func(m *wsprotocol.AudioAckMessage) { m.NextOffset = math.MaxUint64 }, errV2ClientProtocol},
		{"interior_without_partial_release", func(m *wsprotocol.AudioAckMessage) { m.NextOffset = 6 }, errInvalidReplayOffset},
		{"end_not_authorized_without_partial_release", func(m *wsprotocol.AudioAckMessage) { m.InputEnded = true }, errV2ClientProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2AudioClient(t)
			authorizeTestV2Audio(t, s, 0, "abcd")
			authorizeTestV2Audio(t, s, 4, "efgh")
			message := wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 4}
			tc.mutate(&message)
			before := snapshotV2Client(s)
			if changed, err := s.acceptAudioAck(1, message); changed || !errors.Is(err, tc.want) {
				t.Fatalf("rejected ack = (%v, %v), want %v", changed, err, tc.want)
			}
			assertV2ClientUnchanged(t, s, before)
		})
	}
}

func TestV2ClientStateAckBeforeWriteCompletionAndOldAcks(t *testing.T) {
	s := newTestV2AudioClient(t)
	authorizeTestV2Audio(t, s, 0, "abcd")
	authorizeTestV2Audio(t, s, 4, "efgh")
	// 状态方法没有 Write 完成回调；授权上界足以验证这个 ACK。
	ackTestV2Audio(t, s, 8, false, true)
	if s.audio.count != 1 || s.completed || s.inputEnded {
		t.Fatal("audio acceptance must release only the prefix and not complete the encounter")
	}
	for _, offset := range []uint64{8, 4, 0, 1, 7} {
		before := snapshotV2Client(s)
		ackTestV2Audio(t, s, offset, false, false)
		assertV2ClientUnchanged(t, s, before)
	}
	assertReplayChunk(t, s.audio, 8, "ijkl")
}

func TestV2ClientStateResumeRejection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*wsprotocol.ReadyMessage)
		want   error
	}{
		{"wrong_type", func(m *wsprotocol.ReadyMessage) { m.Type = wsprotocol.MessageTypeResult }, errV2ClientProtocol},
		{"changed_id", func(m *wsprotocol.ReadyMessage) { m.SessionID = "another-session" }, errV2ClientProtocol},
		{"changed_token", func(m *wsprotocol.ReadyMessage) { m.ResumeToken = "another-token" }, errV2ClientProtocol},
		{"same_generation", func(m *wsprotocol.ReadyMessage) { m.Generation = 1 }, errV2ClientProtocol},
		{"generation_zero", func(m *wsprotocol.ReadyMessage) { m.Generation = 0 }, errV2ClientProtocol},
		{"applied_result_behind", func(m *wsprotocol.ReadyMessage) { m.AckedResultSeq = 1 }, errV2ClientProtocol},
		{"applied_result_ahead", func(m *wsprotocol.ReadyMessage) { m.AckedResultSeq = 3 }, errV2ClientProtocol},
		{"cached_offline_but_unoffered", func(m *wsprotocol.ReadyMessage) { m.NextOffset = 16 }, errV2ClientProtocol},
		{"released_audio_gap", func(m *wsprotocol.ReadyMessage) { m.NextOffset = 0 }, errAudioReplayGap},
		{"block_interior", func(m *wsprotocol.ReadyMessage) { m.NextOffset = 6 }, errInvalidReplayOffset},
		{"end_claim_before_local_end", func(m *wsprotocol.ReadyMessage) { m.NextOffset = 12; m.InputEnded = true }, errV2ClientProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2AudioClient(t)
			offerAllTestV2Audio(t, s)
			ackTestV2Audio(t, s, 4, false, true)
			applyTestV2Result(t, s, 1)
			applyTestV2Result(t, s, 2)
			if !s.detach(1) {
				t.Fatal("detach failed")
			}
			appendTestV2Audio(t, s, 12, "mnop")
			message := testV2Ready(3, 8, 2, false)
			tc.mutate(&message)
			before := snapshotV2Client(s)
			if err := s.acceptReady(message); !errors.Is(err, tc.want) {
				t.Fatalf("rejected resume = %v, want %v", err, tc.want)
			}
			assertV2ClientUnchanged(t, s, before)
			// 候选拒绝不占新代、不清空位置，正确 ready 随后仍可接纳。
			attachTestV2Client(t, s, testV2Ready(3, 8, 2, false))
			if s.generation != 3 || s.audio.ackedOffset != 8 || s.offeredOffset != 12 || s.appliedSeq != 2 {
				t.Fatal("valid resume after rejection lost prior facts")
			}
		})
	}
}

func TestV2ClientStateResumeCalibration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset uint64
		data   string
	}{
		{"replay_from_known_ack", 4, "efgh"},
		{"lost_ack_calibrates_forward", 8, "ijkl"},
		{"all_audio_already_accepted", 12, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2AudioClient(t)
			offerAllTestV2Audio(t, s)
			ackTestV2Audio(t, s, 4, false, true)
			applyTestV2Result(t, s, 1)
			authorizeTestV2End(t, s, 12)
			if !s.detach(1) {
				t.Fatal("detach failed")
			}
			before := snapshotV2Client(s)
			if s.detach(1) || s.detach(0) {
				t.Fatal("repeat/old detach succeeded")
			}
			assertV2ClientUnchanged(t, s, before)
			attachTestV2Client(t, s, testV2Ready(3, tc.offset, 1, false))
			if s.offeredOffset != 12 || s.sendOffset != tc.offset || s.audio.ackedOffset != tc.offset || !s.endOffered || s.endOfferedOnConnection || s.appliedSeq != 1 || s.inputEnded || s.completed {
				t.Fatal("resume reset cumulative facts or failed to calibrate the cursor")
			}
			if tc.data != "" {
				authorizeTestV2Audio(t, s, tc.offset, tc.data)
			} else if chunk, ok, err := s.authorizeAudio(3); chunk.data != nil || ok || err != nil {
				t.Fatalf("empty replay = (%+v, %v, %v)", chunk, ok, err)
			}
			assertV2ClientInvariant(t, s)
		})
	}
}

func TestV2ClientStateAckAdvancesReplayCursor(t *testing.T) {
	s := newTestV2AudioClient(t)
	offerAllTestV2Audio(t, s)
	ackTestV2Audio(t, s, 4, false, true)
	s.detach(1)
	attachTestV2Client(t, s, testV2Ready(2, 4, 0, false))
	// 过去授权过的音频被当前代确认；下一次派发必须跳过已释放的块。
	ackTestV2Audio(t, s, 8, false, true)
	if s.sendOffset != 8 || s.offeredOffset != 12 {
		t.Fatal("ACK did not advance replay cursor while preserving authorization")
	}
	authorizeTestV2Audio(t, s, 8, "ijkl")
}

func TestV2ClientStateInputEndConfirmation(t *testing.T) {
	s := newTestV2AudioClient(t)
	offerAllTestV2Audio(t, s)
	ackTestV2Audio(t, s, 12, false, true)
	before := snapshotV2Client(s)
	if changed, err := s.acceptAudioAck(1, wsprotocol.AudioAckMessage{
		Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 12, InputEnded: true,
	}); changed || !errors.Is(err, errV2ClientProtocol) {
		t.Fatalf("input end before EOF = (%v, %v)", changed, err)
	}
	assertV2ClientUnchanged(t, s, before)
	s.sealInput()
	before = snapshotV2Client(s)
	if changed, err := s.acceptAudioAck(1, wsprotocol.AudioAckMessage{
		Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 12, InputEnded: true,
	}); changed || !errors.Is(err, errV2ClientProtocol) {
		t.Fatalf("input end before end authorization = (%v, %v)", changed, err)
	}
	assertV2ClientUnchanged(t, s, before)
	authorizeTestV2End(t, s, 12)
	before = snapshotV2Client(s)
	if changed, err := s.acceptAudioAck(1, wsprotocol.AudioAckMessage{
		Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 8, InputEnded: true,
	}); changed || !errors.Is(err, errV2ClientProtocol) {
		t.Fatalf("wrong end position = (%v, %v)", changed, err)
	}
	assertV2ClientUnchanged(t, s, before)
	ackTestV2Audio(t, s, 12, true, true) // 音频位置未变，仅推进 end 事实。
	for _, message := range []wsprotocol.AudioAckMessage{
		{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 12, InputEnded: true},
		{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 12},
		{Type: wsprotocol.MessageTypeAudioAck, Generation: 1, NextOffset: 4},
	} {
		before := snapshotV2Client(s)
		if changed, err := s.acceptAudioAck(1, message); changed || err != nil {
			t.Fatalf("old end snapshot = (%v, %v)", changed, err)
		}
		assertV2ClientUnchanged(t, s, before)
	}
	if s.completed {
		t.Fatal("input end confirmation implied encounter completion")
	}
	s.detach(1)
	before = snapshotV2Client(s)
	if err := s.acceptReady(testV2Ready(2, 12, 0, false)); !errors.Is(err, errV2ClientProtocol) {
		t.Fatalf("ready regresses input end = %v", err)
	}
	assertV2ClientUnchanged(t, s, before)
	attachTestV2Client(t, s, testV2Ready(2, 12, 0, true))
	before = snapshotV2Client(s)
	if offset, ok, err := s.authorizeEnd(2); offset != 0 || ok || err != nil {
		t.Fatalf("end already accepted = (%d, %v, %v)", offset, ok, err)
	}
	assertV2ClientUnchanged(t, s, before)
}

func TestV2ClientStateEndRecovery(t *testing.T) {
	for _, ended := range []bool{false, true} {
		name := "resend_end_when_unconfirmed"
		if ended {
			name = "ready_confirms_end_without_prior_ack"
		}
		t.Run(name, func(t *testing.T) {
			s := newTestV2AudioClient(t)
			offerAllTestV2Audio(t, s)
			authorizeTestV2End(t, s, 12)
			s.detach(1) // end 已授权但写入/接纳结果未知，不回滚 endOffered。
			attachTestV2Client(t, s, testV2Ready(3, 12, 0, ended))
			if offset, ok, err := s.authorizeEnd(3); ok != !ended || err != nil || (ok && offset != 12) || (!ok && offset != 0) {
				t.Fatalf("end after ready = (%d, %v, %v), inputEnded=%v", offset, ok, err, ended)
			}
			if !s.endOffered || s.inputEnded != ended || s.completed {
				t.Fatal("recovered end facts mismatch")
			}
			assertV2ClientInvariant(t, s)
		})
	}
}

func TestV2ClientStateResumeInputEndRejection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		offerEnd bool
		offset   uint64
	}{
		{"sealed_without_end_authorization", false, 12},
		{"end_authorized_but_wrong_ready_endpoint", true, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2AudioClient(t)
			offerAllTestV2Audio(t, s)
			s.sealInput()
			if tc.offerEnd {
				authorizeTestV2End(t, s, 12)
			}
			s.detach(1)
			before := snapshotV2Client(s)
			if err := s.acceptReady(testV2Ready(3, tc.offset, 0, true)); !errors.Is(err, errV2ClientProtocol) {
				t.Fatalf("invalid ready end = %v", err)
			}
			assertV2ClientUnchanged(t, s, before)
		})
	}
}

func TestV2ClientStateMaximumAudioOffset(t *testing.T) {
	s := newTestV2Client(t, math.MaxUint64, 1)
	attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
	// 模拟很长的已确认前缀；真实保留音频只有最后三个字节。
	s.audio.ackedOffset, s.audio.nextOffset = math.MaxUint64-3, math.MaxUint64-3
	s.sendOffset, s.offeredOffset = math.MaxUint64-3, math.MaxUint64-3
	before := snapshotV2Client(s)
	if _, err := s.appendAudio([]byte("four")); !errors.Is(err, errReplayOffsetOverflow) {
		t.Fatalf("overflowed input = %v", err)
	}
	assertV2ClientUnchanged(t, s, before)
	appendTestV2Audio(t, s, math.MaxUint64-3, "end")
	authorizeTestV2Audio(t, s, math.MaxUint64-3, "end")
	ackTestV2Audio(t, s, math.MaxUint64, false, true)
	authorizeTestV2End(t, s, math.MaxUint64)
	if changed, err := s.acceptCompleted(1, wsprotocol.CompletedMessage{
		Type: wsprotocol.MessageTypeCompleted, Generation: 1, FinalOffset: math.MaxUint64,
	}); !changed || err != nil {
		t.Fatalf("maximum offset completed = (%v, %v)", changed, err)
	}
	assertV2ClientInvariant(t, s)
}

func TestV2ClientStateEmptyInput(t *testing.T) {
	for _, sealedBeforeReady := range []bool{false, true} {
		name := "EOF_after_ready"
		if sealedBeforeReady {
			name = "EOF_before_ready"
		}
		t.Run(name, func(t *testing.T) {
			s := newTestV2Client(t, 1, 1)
			if sealedBeforeReady {
				s.sealInput()
			}
			attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
			authorizeTestV2End(t, s, 0)
			s.detach(1)
			attachTestV2Client(t, s, testV2Ready(2, 0, 0, true))
			if changed, err := s.acceptCompleted(2, wsprotocol.CompletedMessage{
				Type: wsprotocol.MessageTypeCompleted, Generation: 2,
			}); !changed || err != nil || !s.completed {
				t.Fatalf("empty completed = (%v, %v)", changed, err)
			}
			assertV2ClientInvariant(t, s)
		})
	}
}

func TestV2ClientStateResults(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  wsprotocol.MessageType
		seq  uint64
		want error
	}{
		{"wrong_type", wsprotocol.MessageTypeReady, 1, errV2ClientProtocol},
		{"zero_seq", wsprotocol.MessageTypeResult, 0, errV2ClientProtocol},
		{"skipped_first", wsprotocol.MessageTypeResult, 2, errV2ClientResultGap},
		{"far_ahead", wsprotocol.MessageTypeResult, math.MaxUint64, errV2ClientResultGap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2Client(t, 4, 1)
			attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
			before := snapshotV2Client(s)
			if apply, err := s.checkResult(1, wsprotocol.SequencedResultMessage{Type: tc.typ, Seq: tc.seq}); apply || !errors.Is(err, tc.want) {
				t.Fatalf("result = (%v, %v), want %v", apply, err, tc.want)
			}
			assertV2ClientUnchanged(t, s, before)
		})
	}
	t.Run("application_commit_and_deduplication", func(t *testing.T) {
		s := newTestV2Client(t, 4, 1)
		attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
		first := wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1, SegmentID: "segment", Text: "partial"}
		before := snapshotV2Client(s)
		for range 2 {
			if apply, err := s.checkResult(1, first); !apply || err != nil {
				t.Fatalf("check without application = (%v, %v)", apply, err)
			}
			assertV2ClientUnchanged(t, s, before)
		}
		// 未成功应用就不调用 commitApplied，检查本身不产生任何累计确认。
		var applications []wsprotocol.SequencedResultMessage
		for _, message := range []wsprotocol.SequencedResultMessage{
			first, first,
			{Type: wsprotocol.MessageTypeResult, Seq: 2, SegmentID: "segment", Text: "final", IsFinal: true},
		} {
			apply, err := s.checkResult(1, message)
			if err != nil {
				t.Fatal(err)
			}
			if apply {
				applications = append(applications, message)
				if err := s.commitApplied(1, message.Seq); err != nil {
					t.Fatal(err)
				}
			}
		}
		if len(applications) != 2 || applications[0].Seq != 1 || applications[1].Seq != 2 || !applications[1].IsFinal || s.appliedSeq != 2 {
			t.Fatal("duplicate sequence reapplied or same-segment update lost")
		}
		s.detach(1)
		attachTestV2Client(t, s, testV2Ready(3, 0, 2, false))
		before = snapshotV2Client(s)
		if apply, err := s.checkResult(3, first); apply || err != nil {
			t.Fatalf("duplicate across connection = (%v, %v)", apply, err)
		}
		assertV2ClientUnchanged(t, s, before)
	})
}

func TestV2ClientStateAppliedCommitRejection(t *testing.T) {
	for _, tc := range []struct {
		name string
		seq  uint64
	}{
		{"zero", 0}, {"duplicate", 1}, {"skip_next", 3}, {"far_ahead", math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2Client(t, 4, 1)
			attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
			applyTestV2Result(t, s, 1)
			before := snapshotV2Client(s)
			if err := s.commitApplied(1, tc.seq); !errors.Is(err, errV2ClientState) {
				t.Fatalf("commit = %v", err)
			}
			assertV2ClientUnchanged(t, s, before)
		})
	}
}

func TestV2ClientStateMaximumGenerationAndResult(t *testing.T) {
	s := newTestV2Client(t, 1, 1)
	attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
	s.detach(1)
	attachTestV2Client(t, s, testV2Ready(math.MaxUint64, 0, 0, false))
	// 合法模拟已经应用的前缀，避免制造天文数量的更新。
	s.appliedSeq = math.MaxUint64 - 1
	applyTestV2Result(t, s, math.MaxUint64)
	before := snapshotV2Client(s)
	if apply, err := s.checkResult(math.MaxUint64, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 0}); apply || !errors.Is(err, errV2ClientProtocol) {
		t.Fatalf("wrapped result = (%v, %v)", apply, err)
	}
	if err := s.commitApplied(math.MaxUint64, 0); !errors.Is(err, errV2ClientState) {
		t.Fatalf("wrapped commit = %v", err)
	}
	if apply, err := s.checkResult(math.MaxUint64, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: math.MaxUint64}); apply || err != nil {
		t.Fatalf("max duplicate = (%v, %v)", apply, err)
	}
	assertV2ClientUnchanged(t, s, before)
	s.detach(math.MaxUint64)
	for _, generation := range []uint64{0, 1, math.MaxUint64} {
		before := snapshotV2Client(s)
		if err := s.acceptReady(testV2Ready(generation, 0, math.MaxUint64, false)); !errors.Is(err, errV2ClientProtocol) {
			t.Fatalf("generation cannot advance from max: %v", err)
		}
		assertV2ClientUnchanged(t, s, before)
	}
}

func TestV2ClientStateCompletedRejection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		omitSeal bool
		omitEnd  bool
		mutate   func(*wsprotocol.CompletedMessage)
	}{
		{name: "not_sealed", omitSeal: true, omitEnd: true},
		{name: "sealed_but_end_not_authorized", omitEnd: true},
		{name: "wrong_type", mutate: func(m *wsprotocol.CompletedMessage) { m.Type = wsprotocol.MessageTypeResult }},
		{name: "zero_generation", mutate: func(m *wsprotocol.CompletedMessage) { m.Generation = 0 }},
		{name: "wrong_message_generation", mutate: func(m *wsprotocol.CompletedMessage) { m.Generation = 3 }},
		{name: "wrong_final_prefix_without_release", mutate: func(m *wsprotocol.CompletedMessage) { m.FinalOffset = 8 }},
		{name: "wrong_final_ahead", mutate: func(m *wsprotocol.CompletedMessage) { m.FinalOffset = 16 }},
		{name: "last_seq_behind", mutate: func(m *wsprotocol.CompletedMessage) { m.LastSeq = 1 }},
		{name: "last_seq_not_applied_without_release", mutate: func(m *wsprotocol.CompletedMessage) { m.LastSeq = 3 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestV2AudioClient(t)
			offerAllTestV2Audio(t, s)
			applyTestV2Result(t, s, 1)
			applyTestV2Result(t, s, 2)
			if !tc.omitSeal {
				s.sealInput()
			}
			if !tc.omitEnd {
				authorizeTestV2End(t, s, 12)
			}
			message := wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1, FinalOffset: 12, LastSeq: 2}
			if tc.mutate != nil {
				tc.mutate(&message)
			}
			before := snapshotV2Client(s)
			if changed, err := s.acceptCompleted(1, message); changed || !errors.Is(err, errV2ClientProtocol) {
				t.Fatalf("rejected completed = (%v, %v)", changed, err)
			}
			assertV2ClientUnchanged(t, s, before)
		})
	}
}

func TestV2ClientStateCompletedIsStable(t *testing.T) {
	s := newTestV2AudioClient(t)
	offerAllTestV2Audio(t, s)
	authorizeTestV2End(t, s, 12)
	applyTestV2Result(t, s, 1)
	applyTestV2Result(t, s, 2)
	if s.audio.ackedOffset != 0 || s.inputEnded {
		t.Fatal("fixture should not have observed separate audio ACK")
	}
	message := wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: 1, FinalOffset: 12, LastSeq: 2}
	if changed, err := s.acceptCompleted(1, message); !changed || err != nil {
		t.Fatalf("completed = (%v, %v)", changed, err)
	}
	if !s.completed || !s.inputEnded || s.audio.count != 0 || s.audio.ackedOffset != 12 || s.sendOffset != 12 {
		t.Fatal("validated completion did not record success and release audio")
	}
	before := snapshotV2Client(s)
	if changed, err := s.acceptCompleted(1, message); changed || err != nil {
		t.Fatalf("duplicate completed = (%v, %v)", changed, err)
	}
	conflict := message
	conflict.LastSeq++
	if changed, err := s.acceptCompleted(1, conflict); changed || !errors.Is(err, errV2ClientProtocol) {
		t.Fatalf("conflicting completed = (%v, %v)", changed, err)
	}
	if apply, err := s.checkResult(1, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 2}); apply || err != nil {
		t.Fatalf("known duplicate after completed = (%v, %v)", apply, err)
	}
	if apply, err := s.checkResult(1, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 3}); apply || !errors.Is(err, errV2ClientCompleted) {
		t.Fatalf("new result after completed = (%v, %v)", apply, err)
	}
	if err := s.commitApplied(1, 3); !errors.Is(err, errV2ClientCompleted) {
		t.Fatalf("new commit after completed = %v", err)
	}
	if _, ok, err := s.authorizeAudio(1); ok || !errors.Is(err, errV2ClientCompleted) {
		t.Fatalf("new audio after completed = (%v, %v)", ok, err)
	}
	if _, ok, err := s.authorizeEnd(1); ok || !errors.Is(err, errV2ClientCompleted) {
		t.Fatalf("new end after completed = (%v, %v)", ok, err)
	}
	if err := s.acceptReady(testV2Ready(2, 12, 2, true)); !errors.Is(err, errV2ClientCompleted) {
		t.Fatalf("resume completed = %v", err)
	}
	assertV2ClientUnchanged(t, s, before)
	// 没有 completed_ack 写成功的反馈；detach 仍须保留已经核实的业务成功。
	if !s.detach(1) || !s.completed || s.appliedSeq != 2 || s.audio.nextOffset != 12 {
		t.Fatal("disconnect revoked the locally validated completed fact")
	}
	before = snapshotV2Client(s)
	if err := s.acceptReady(testV2Ready(2, 12, 2, true)); !errors.Is(err, errV2ClientCompleted) {
		t.Fatalf("resume after lost completion ack = %v", err)
	}
	if _, err := s.appendAudio([]byte("new")); !errors.Is(err, errReplayBufferSealed) {
		t.Fatalf("append after completed = %v", err)
	}
	assertV2ClientUnchanged(t, s, before)
}

func TestV2ClientStateConnectionEventIsolation(t *testing.T) {
	operations := []struct {
		name string
		call func(*v2ClientState, uint64) error
	}{
		{"authorize_audio", func(s *v2ClientState, g uint64) error { _, _, err := s.authorizeAudio(g); return err }},
		{"authorize_end", func(s *v2ClientState, g uint64) error { _, _, err := s.authorizeEnd(g); return err }},
		{"audio_ack_spoofing_current_message_generation", func(s *v2ClientState, g uint64) error {
			_, err := s.acceptAudioAck(g, wsprotocol.AudioAckMessage{Type: wsprotocol.MessageTypeAudioAck, Generation: s.generation, NextOffset: 8})
			return err
		}},
		{"result", func(s *v2ClientState, g uint64) error {
			_, err := s.checkResult(g, wsprotocol.SequencedResultMessage{Type: wsprotocol.MessageTypeResult, Seq: 1})
			return err
		}},
		{"commit", func(s *v2ClientState, g uint64) error { return s.commitApplied(g, 1) }},
		{"completed_spoofing_current_message_generation", func(s *v2ClientState, g uint64) error {
			_, err := s.acceptCompleted(g, wsprotocol.CompletedMessage{Type: wsprotocol.MessageTypeCompleted, Generation: s.generation, FinalOffset: 12})
			return err
		}},
	}
	for _, phase := range []struct {
		name       string
		generation uint64
		detached   bool
		want       error
	}{
		{"old_generation", 1, false, errV2ClientGenerationMismatch},
		{"zero_generation", 0, false, errV2ClientGenerationMismatch},
		{"future_generation", 4, false, errV2ClientGenerationMismatch},
		{"detached_current_generation", 3, true, errV2ClientState},
	} {
		t.Run(phase.name, func(t *testing.T) {
			for _, op := range operations {
				t.Run(op.name, func(t *testing.T) {
					s := newTestV2AudioClient(t)
					offerAllTestV2Audio(t, s)
					authorizeTestV2End(t, s, 12)
					ackTestV2Audio(t, s, 4, false, true)
					s.detach(1)
					attachTestV2Client(t, s, testV2Ready(3, 4, 0, false))
					if phase.detached {
						s.detach(3)
					}
					before := snapshotV2Client(s)
					if err := op.call(s, phase.generation); !errors.Is(err, phase.want) {
						t.Fatalf("event = %v, want %v", err, phase.want)
					}
					if s.detach(phase.generation) {
						t.Fatal("ineligible detach changed current connection")
					}
					assertV2ClientUnchanged(t, s, before)
				})
			}
		})
	}
}

func TestV2ClientStateCombinedRecoveryToCompletion(t *testing.T) {
	s := newTestV2Client(t, 8, 2)
	appendTestV2Audio(t, s, 0, "abcd")
	appendTestV2Audio(t, s, 4, "efgh")
	before := snapshotV2Client(s)
	if _, err := s.appendAudio([]byte("ijkl")); !errors.Is(err, errReplayBufferFull) {
		t.Fatalf("full capture = %v", err)
	}
	assertV2ClientUnchanged(t, s, before)
	attachTestV2Client(t, s, testV2Ready(1, 0, 0, false))
	authorizeTestV2Audio(t, s, 0, "abcd")
	authorizeTestV2Audio(t, s, 4, "efgh")
	ackTestV2Audio(t, s, 4, false, true)
	applyTestV2Result(t, s, 1)
	s.detach(1)
	// 旧音频 ACK 与结果 ACK 丢失，都由已验证 ready 校准，不重新应用结果 1。
	attachTestV2Client(t, s, testV2Ready(3, 8, 1, false))
	appendTestV2Audio(t, s, 8, "ijkl")
	authorizeTestV2Audio(t, s, 8, "ijkl")
	authorizeTestV2End(t, s, 12)
	applyTestV2Result(t, s, 2)
	if changed, err := s.acceptCompleted(3, wsprotocol.CompletedMessage{
		Type: wsprotocol.MessageTypeCompleted, Generation: 3, FinalOffset: 12, LastSeq: 2,
	}); !changed || err != nil {
		t.Fatalf("combined completed = (%v, %v)", changed, err)
	}
	s.detach(3)
	if !s.completed || s.appliedSeq != 2 || s.audio.ackedOffset != 12 || s.audio.count != 0 || s.offeredOffset != 12 {
		t.Fatal("combined recovery lost continuity or completed fact")
	}
	assertV2ClientInvariant(t, s)
}
