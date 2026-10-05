package gateway

import (
	"fmt"
	"io"
	"time"
)

// resumableSession 保存一场逻辑会话的身份与连接保留状态。
// identity 和 resume 指针在构造后保持不变。
// resume 指向的可变状态由会话协调者串行操作。
type resumableSession struct {
	identity    sessionIdentity            // 稳定身份及恢复凭据。
	resume      *resumeState               // 连接附着状态和恢复期限。
	commands    chan sessionControlCommand // 无缓冲，向唯一协调者提交命令。
	controlDone chan struct{}              // 控制循环退出时关闭。
}

// newResumableSession 组合身份与恢复状态。
// window 必须为正；生产 random 使用 crypto/rand.Reader。
// 失败返回 nil 和可追溯的错误。
// 创建对象不占准入名额，也不启动连接、Worker 或后台任务。
func newResumableSession(
	random io.Reader,
	window time.Duration,
) (*resumableSession, error) {
	resume, err := newResumeState(window)
	if err != nil {
		return nil, fmt.Errorf("create resumable session resume state: %w", err)
	}

	identity, err := newSessionIdentity(random)
	if err != nil {
		return nil, fmt.Errorf("create resumable session identity: %w", err)
	}

	return &resumableSession{
		identity:    identity,
		resume:      resume,
		commands:    make(chan sessionControlCommand),
		controlDone: make(chan struct{}),
	}, nil
}
