package realasr

import (
	"errors"
	"strings"
	"testing"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/protobuf/proto"
)

func newTestSnapshotMapper(t *testing.T, limit int) *snapshotMapper {
	t.Helper()
	m, err := newSnapshotMapper("1", limit)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func assertSnapshotResponse(t *testing.T, got *asrv1.StreamingRecognizeResponse, text string, final bool) {
	t.Helper()
	want := &asrv1.StreamingRecognizeResponse{SegmentId: "1", Text: text, IsFinal: final}
	if !proto.Equal(got, want) {
		t.Fatalf("response = %v, want %v", got, want)
	}
}

func TestSnapshotMapperConstruction(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		limit    int
	}{
		{"empty_id", "", 4096},
		{"invalid_utf8_id", string([]byte{0xff}), 4096},
		{"zero_limit", "1", 0},
		{"negative_limit", "1", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if m, err := newSnapshotMapper(tc.id, tc.limit); m != nil || err == nil {
				t.Fatalf("constructor = (%v, %v), want nil and error", m, err)
			}
		})
	}
	// 片段 ID 不限于数字，映射时必须原样保留合法 ID。
	m, err := newSnapshotMapper("片段-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.mapSnapshot("a", true)
	if err != nil || r == nil || r.SegmentId != "片段-a" || r.Text != "a" || !r.IsFinal || r.Progress != nil {
		t.Fatalf("custom segment mapping = (%v, %v)", r, err)
	}
}

func TestSnapshotMapperRevisionsAndFinal(t *testing.T) {
	m := newTestSnapshotMapper(t, 4096)
	steps := []struct {
		name, text  string
		final, emit bool
	}{
		{"initial_silence", "", false, false},
		{"first_text", "患者发烧", false, true},
		{"unchanged", "患者发烧", false, false},
		{"revise_previous_words", "患者没有发烧", false, true},
		{"withdraw_all_text", "", false, true},
		{"unchanged_empty", "", false, false},
		{"resume_text", "患者没有发烧", false, true},
		{"final_same_text", "患者没有发烧", true, true},
	}
	var history []*asrv1.StreamingRecognizeResponse
	var copies []*asrv1.StreamingRecognizeResponse
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			r, err := m.mapSnapshot(step.text, step.final)
			if err != nil {
				t.Fatal(err)
			}
			if !step.emit {
				if r != nil {
					t.Fatalf("unchanged partial emitted %v", r)
				}
				return
			}
			assertSnapshotResponse(t, r, step.text, step.final)
			for i, previous := range history {
				if r == previous || !proto.Equal(previous, copies[i]) {
					t.Fatalf("new update reused or modified response %d", i)
				}
			}
			history = append(history, r)
			copies = append(copies, proto.Clone(r).(*asrv1.StreamingRecognizeResponse))
		})
	}
}

func TestSnapshotMapperFinalWithoutMatchingPartial(t *testing.T) {
	for _, text := range []string{"", "尾部刷新后才出现的文字"} {
		t.Run(text, func(t *testing.T) {
			m := newTestSnapshotMapper(t, 4096)
			r, err := m.mapSnapshot(text, true)
			if err != nil {
				t.Fatal(err)
			}
			assertSnapshotResponse(t, r, text, true)
		})
	}
}

func TestSnapshotMapperUTF8ByteLimit(t *testing.T) {
	for _, final := range []bool{false, true} {
		m := newTestSnapshotMapper(t, 6)
		// 两个汉字恰好六字节；七字节输入必须拒绝，不能按字符数判断。
		if r, err := m.mapSnapshot("患者a", final); r != nil || err == nil {
			t.Fatalf("seven-byte input = (%v, %v), want rejection", r, err)
		}
		r, err := m.mapSnapshot("患者", final)
		if err != nil {
			t.Fatal(err)
		}
		assertSnapshotResponse(t, r, "患者", final)
	}
}

func TestSnapshotMapperRejectedInputPreservesState(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		wantErr    error
	}{
		{"invalid_utf8", "private\xff", errInvalidUTF8},
		{"oversized", "private transcript exceeding the limit", nil},
	} {
		for _, final := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/partial", true: "/final"}[final], func(t *testing.T) {
				m := newTestSnapshotMapper(t, 8)
				if _, err := m.mapSnapshot("原文", false); err != nil {
					t.Fatal(err)
				}
				before := *m
				r, err := m.mapSnapshot(tc.text, final)
				if r != nil || err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
					t.Fatalf("invalid input = (%v, %v), want rejection %v", r, err, tc.wantErr)
				}
				if strings.Contains(err.Error(), "private") {
					t.Fatalf("error exposed recognition text: %v", err)
				}
				if *m != before {
					t.Fatal("rejected input changed mapper state")
				}
				if r, err := m.mapSnapshot("原文", false); r != nil || err != nil {
					t.Fatalf("previous partial no longer deduplicated: (%v, %v)", r, err)
				}
				r, err = m.mapSnapshot("定稿", true)
				if err != nil {
					t.Fatal(err)
				}
				assertSnapshotResponse(t, r, "定稿", true)
			})
		}
	}
}

func TestSnapshotMapperRejectsAllUpdatesAfterFinal(t *testing.T) {
	m := newTestSnapshotMapper(t, 8)
	if _, err := m.mapSnapshot("完成", true); err != nil {
		t.Fatal(err)
	}
	before := *m
	for _, text := range []string{"完成", "", "修订", "\xff", "longer than limit"} {
		for _, final := range []bool{false, true} {
			if r, err := m.mapSnapshot(text, final); r != nil || !errors.Is(err, errFinalized) {
				t.Fatalf("post-final update = (%v, %v), want errFinalized", r, err)
			}
			if *m != before {
				t.Fatal("post-final rejection changed state")
			}
		}
	}
}

func TestSnapshotMapperResponsesAreIndependent(t *testing.T) {
	m := newTestSnapshotMapper(t, 4096)
	r, err := m.mapSnapshot("原文", false)
	if err != nil {
		t.Fatal(err)
	}
	// 调用方修改响应不会改动映射器，也不会污染后续生成的消息。
	r.SegmentId, r.Text, r.IsFinal = "other", "caller edit", true
	r.Progress = &asrv1.AudioProgress{ProcessedAudioBytes: 100}
	if next, err := m.mapSnapshot("原文", false); next != nil || err != nil {
		t.Fatalf("caller edit affected deduplication: (%v, %v)", next, err)
	}
	next, err := m.mapSnapshot("原文", true)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotResponse(t, next, "原文", true)
}
