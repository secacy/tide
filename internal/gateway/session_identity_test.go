package gateway

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// identityTestMaterial 返回固定测试材料，不使用真实随机源。
func identityTestMaterial() []byte {
	material := make([]byte, 48)
	for i := range material {
		material[i] = byte(i)
	}
	return material
}

func newTestSessionIdentity(t *testing.T, material []byte) sessionIdentity {
	t.Helper()
	identity, err := newSessionIdentity(bytes.NewReader(material))
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

// identityShortReader 每次只返回少量数据，检查构造器是否正确继续读取。
type identityShortReader struct {
	reader io.Reader
	limit  int
}

func (r *identityShortReader) Read(p []byte) (int, error) {
	if len(p) > r.limit {
		p = p[:r.limit]
	}
	return r.reader.Read(p)
}

// identityErrorReader 在一次读取中同时返回数据和错误，覆盖部分成功后失败。
type identityErrorReader struct {
	data []byte
	err  error
}

func (r *identityErrorReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, r.err
}

func TestSessionIdentityExactEncoding(t *testing.T) {
	for _, tc := range []struct {
		name     string
		material []byte
		id       string
		token    string
	}{
		{
			name: "sequential_bytes", material: identityTestMaterial(),
			id:    "000102030405060708090a0b0c0d0e0f",
			token: "EBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8",
		},
		{
			name: "zero_test_material", material: make([]byte, 48),
			id: strings.Repeat("0", 32), token: strings.Repeat("A", 43),
		},
		{
			name: "url_safe_without_padding", material: bytes.Repeat([]byte{255}, 48),
			id: strings.Repeat("f", 32), token: strings.Repeat("_", 42) + "8",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 留出后续字节，验证一次身份构造只消费本次所需材料。
			reader := bytes.NewReader(append(append([]byte(nil), tc.material...), 9, 8, 7))
			identity, err := newSessionIdentity(reader)
			if err != nil {
				t.Fatal(err)
			}
			if identity.id != tc.id || identity.resumeToken != tc.token {
				t.Fatal("identity does not match the fixed encoding vector")
			}
			if len(identity.id) != 32 || len(identity.resumeToken) != 43 || reader.Len() != 3 {
				t.Fatal("incorrect encoding lengths or random material consumption")
			}
		})
	}
}

func TestSessionIdentityRandomMaterialIsSeparated(t *testing.T) {
	material := identityTestMaterial()
	identity := newTestSessionIdentity(t, material)
	changedID := append([]byte(nil), material...)
	changedID[0] ^= 255
	idVariant := newTestSessionIdentity(t, changedID)
	if identity.id == idVariant.id || identity.resumeToken != idVariant.resumeToken {
		t.Fatal("changing ID material must only change the ID")
	}
	changedToken := append([]byte(nil), material...)
	changedToken[16] ^= 255
	tokenVariant := newTestSessionIdentity(t, changedToken)
	if identity.id != tokenVariant.id || identity.resumeToken == tokenVariant.resumeToken {
		t.Fatal("changing token material must only change the token")
	}
	if identity.matchesResumeToken(tokenVariant.resumeToken) || tokenVariant.matchesResumeToken(identity.resumeToken) {
		t.Fatal("different token material must not authorize the other identity")
	}
}

func TestSessionIdentityShortReads(t *testing.T) {
	want := newTestSessionIdentity(t, identityTestMaterial())
	for _, tc := range []struct {
		name   string
		reader io.Reader
	}{
		{"one_byte_at_a_time", &identityShortReader{bytes.NewReader(identityTestMaterial()), 1}},
		{"seven_bytes_at_a_time", &identityShortReader{bytes.NewReader(identityTestMaterial()), 7}},
		{"complete_material_with_eof", &identityErrorReader{identityTestMaterial(), io.EOF}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newSessionIdentity(tc.reader)
			if err != nil || got != want {
				t.Fatalf("full material must succeed, error = %v", err)
			}
		})
	}
}

func TestSessionIdentityReadFailuresReturnZeroValue(t *testing.T) {
	sourceErr := errors.New("test entropy source failed")
	for _, tc := range []struct {
		name   string
		reader io.Reader
		cause  error
	}{
		{"nil_interface", nil, nil},
		{"empty_eof", bytes.NewReader(nil), io.EOF},
		{"before_id_complete", bytes.NewReader(make([]byte, 15)), io.ErrUnexpectedEOF},
		{"only_id_material", bytes.NewReader(make([]byte, 16)), io.ErrUnexpectedEOF},
		{"last_byte_missing", bytes.NewReader(make([]byte, 47)), io.ErrUnexpectedEOF},
		{"source_error_without_data", &identityErrorReader{nil, sourceErr}, sourceErr},
		{"source_error_with_partial_data", &identityErrorReader{make([]byte, 31), sourceErr}, sourceErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := newSessionIdentity(tc.reader)
			if err == nil || identity != (sessionIdentity{}) {
				t.Fatal("read failure must return an error and zero identity")
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("cause not preserved: got %v, want %v", err, tc.cause)
			}
		})
	}
}

func TestSessionIdentityTokenMatchingIsExact(t *testing.T) {
	identity := newTestSessionIdentity(t, identityTestMaterial())
	original := identity
	token := identity.resumeToken
	for _, tc := range []struct {
		name      string
		candidate string
		match     bool
	}{
		{"exact", token, true},
		{"empty", "", false},
		{"short", token[:42], false},
		{"long", token + "A", false},
		{"first_byte_changed", "A" + token[1:], false},
		{"middle_byte_changed", token[:20] + "A" + token[21:], false},
		{"last_byte_changed", token[:42] + "A", false},
		{"leading_space", " " + token, false},
		{"trailing_space", token + " ", false},
		{"same_length_whitespace", " " + token[1:], false},
		{"case_changed", strings.ToLower(token), false},
		{"padded_base64", token + "=", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := identity.matchesResumeToken(tc.candidate); got != tc.match {
				t.Fatalf("matches = %v, want %v", got, tc.match)
			}
			if identity != original {
				t.Fatal("token comparison must not mutate identity")
			}
		})
	}
}

func TestSessionIdentityZeroOrInvalidStoredTokenCannotMatch(t *testing.T) {
	valid := newTestSessionIdentity(t, identityTestMaterial())
	for _, tc := range []struct {
		name     string
		identity sessionIdentity
		input    string
	}{
		{"zero_with_empty_input", sessionIdentity{}, ""},
		{"zero_with_valid_input", sessionIdentity{}, valid.resumeToken},
		{"short_stored_token", sessionIdentity{resumeToken: "A"}, "A"},
		{"long_stored_token", sessionIdentity{resumeToken: strings.Repeat("A", 44)}, strings.Repeat("A", 44)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.identity.matchesResumeToken(tc.input) {
				t.Fatal("zero or invalid-length stored token must not match")
			}
		})
	}
}

func TestSessionIdentityConcurrentReadOnlyMatching(t *testing.T) {
	identity := newTestSessionIdentity(t, identityTestMaterial())
	original := identity
	var wg sync.WaitGroup
	failed := make(chan struct{}, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if !identity.matchesResumeToken(original.resumeToken) || identity.matchesResumeToken("invalid") {
					failed <- struct{}{}
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(failed) != 0 || identity != original {
		t.Fatal("read-only matching failed or identity changed")
	}
}
