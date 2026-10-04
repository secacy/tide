package gateway

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
)

const (
	sessionIDRandomBytes     = 16
	resumeTokenRandomBytes   = 32
	sessionIdentityRandBytes = sessionIDRandomBytes + resumeTokenRandomBytes

	sessionIDLength   = 32
	resumeTokenLength = 43
)

// sessionIdentity 保存一场逻辑会话的稳定标识和恢复凭据。
// 由 newSessionIdentity 创建，发布后保持不变。
// 不得将整个对象或 resumeToken 写入普通日志。
type sessionIdentity struct {
	id          string // 用于定位会话，可用于日志关联。
	resumeToken string // 用于验证恢复请求，应保密。
}

// newSessionIdentity 从 random 读取随机材料并创建身份。
// 生产调用必须传入 crypto/rand.Reader，测试可传可控 Reader。
// nil 接口或读取失败时，返回零值身份和错误。
// 不访问注册表，也不检查全局唯一性。
func newSessionIdentity(random io.Reader) (sessionIdentity, error) {
	if random == nil {
		return sessionIdentity{}, fmt.Errorf("session identity: random reader is nil")
	}

	var material [sessionIdentityRandBytes]byte
	if _, err := io.ReadFull(random, material[:]); err != nil {
		return sessionIdentity{}, fmt.Errorf("session identity: read random material: %w", err)
	}

	id := hex.EncodeToString(material[:sessionIDRandomBytes])
	resumeToken := base64.RawURLEncoding.EncodeToString(material[sessionIDRandomBytes:])

	return sessionIdentity{
		id:          id,
		resumeToken: resumeToken,
	}, nil
}

// matchesResumeToken 检查 candidate 是否与恢复凭据完全一致。
// 零值身份、空值、长度错误或内容不同均返回 false。
// 不修改身份，不延长恢复窗口，也不代表会话仍可恢复。
func (i sessionIdentity) matchesResumeToken(candidate string) bool {
	if len(i.resumeToken) != resumeTokenLength {
		return false
	}

	if len(candidate) != resumeTokenLength {
		return false
	}

	return subtle.ConstantTimeCompare(
		[]byte(i.resumeToken),
		[]byte(candidate),
	) == 1
}
