//go:build !race

package gateway

// groupResumeRaceEnabled 由构建决定，不能被环境变量修改。
const groupResumeRaceEnabled = false
