package gateway

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// newTestResumableSession 用固定材料构造对象，便于确定性制造 ID 冲突。
func newTestResumableSession(t *testing.T, material []byte) *resumableSession {
	t.Helper()
	s, err := newResumableSession(bytes.NewReader(material), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// resumableCountingReader 记录读取次数，检查无效配置是否提前返回。
type resumableCountingReader struct {
	calls int
	err   error
}

func (r *resumableCountingReader) Read([]byte) (int, error) {
	r.calls++
	return 0, r.err
}

func TestResumableSessionConstructionAndIsolation(t *testing.T) {
	material := identityTestMaterial()
	a := newTestResumableSession(t, material)
	b := newTestResumableSession(t, material)
	if a.identity != newTestSessionIdentity(t, material) {
		t.Fatal("container must retain the generated identity")
	}
	if a == b || a.resume == nil || a.resume == b.resume {
		t.Fatal("constructors must create independent containers and states")
	}
	assertResumeState(t, a.resume, resumeAttached, 1, time.Time{})
	original := a.identity
	a.resume.close()
	assertResumeState(t, b.resume, resumeAttached, 1, time.Time{})
	if a.identity != original {
		t.Fatal("closing recovery state must not change identity")
	}
}

func TestResumableSessionInvalidWindowDoesNotReadRandom(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
	}{
		{"zero", 0}, {"negative", -time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &resumableCountingReader{err: errors.New("must not read")}
			s, err := newResumableSession(r, tc.window)
			if s != nil || err == nil || r.calls != 0 {
				t.Fatalf("invalid window: nonnil=%v, error=%v, reads=%d", s != nil, err, r.calls)
			}
		})
	}
}

func TestResumableSessionIdentityFailure(t *testing.T) {
	sourceErr := errors.New("test random source failed")
	for _, tc := range []struct {
		name   string
		random io.Reader
		cause  error
	}{
		{"nil", nil, nil},
		{"empty", bytes.NewReader(nil), io.EOF},
		{"partial", bytes.NewReader(make([]byte, 47)), io.ErrUnexpectedEOF},
		{"source_error", &resumableCountingReader{err: sourceErr}, sourceErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := newResumableSession(tc.random, time.Second)
			if s != nil || err == nil {
				t.Fatal("identity failure must return nil session and error")
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("cause not preserved: %v", err)
			}
		})
	}
}
