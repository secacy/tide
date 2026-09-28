package loadgen

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// BatchConfig 定义一次有限批次，会话结束后不补充新会话。
type BatchConfig struct {
	Sessions int           // 计划调用 RunSession 的次数，必须为正。
	Session  SessionConfig // 所有会话使用的相同负载条件。
}

// SessionResult 保存一个计划位置对应的完整运行结果。
type SessionResult struct {
	Index  int           // 从 0 开始的批次内序号，不是服务端会话 ID。
	Report SessionReport // 单场报告；发生错误时也要保留。
	Err    error         // RunSession 返回的原始错误。
}

// BatchReport 保存整批结果，本步不计算成功率或分位数。
type BatchReport struct {
	Config     BatchConfig // 本批输入参数。
	StartedAt  time.Time   // 放行所有会话前立即记录。
	FinishedAt time.Time   // 等待所有会话返回后立即记录。

	// 长度固定为 Sessions，按 Index 排列，不按完成顺序排列。
	Results []SessionResult
}

// RunBatch 并发运行有限批次，并等待每场会话返回。
// 前置失败返回零报告；单场失败保存在对应结果中。
// 父 context 取消时仍等待全部会话退出，返回报告和父 context 错误。
func RunBatch(ctx context.Context, cfg BatchConfig) (BatchReport, error) {
	// 前置检查
	if cfg.Sessions <= 0 {
		return BatchReport{}, errors.New("sessions must be greater than zero")
	}
	if err := cfg.Session.validate(); err != nil {
		return BatchReport{}, err
	}
	if ctx.Err() != nil {
		return BatchReport{}, ctx.Err()
	}
	// 准备固定结果位置和开始信号
	results := make([]SessionResult, cfg.Sessions)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < cfg.Sessions; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			report, err := RunSession(ctx, cfg.Session)
			results[index] = SessionResult{
				Index:  index,
				Report: report,
				Err:    err,
			}
		}(i)
	}

	// 全部任务已经创建，再记录批次开始并统一放行
	report := BatchReport{
		Config:    cfg,
		StartedAt: time.Now(),
		Results:   results,
	}
	close(start)

	wg.Wait()

	report.FinishedAt = time.Now()

	if err := ctx.Err(); err != nil {
		return report, fmt.Errorf("batch context ended: %w", err)
	}

	return report, nil
}
