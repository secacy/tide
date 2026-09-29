package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// main 解析参数、监听停止信号，并将运行结果转换为进程退出状态。
func main() {
	cfg, err := parseLoadConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}

		log.Printf("invalid loadgen arguments: %v", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	err = run(ctx, cfg)

	// os.Exit 不执行 defer，因此在决定退出前显式释放 signal 资源。
	stop()

	if err != nil {
		log.Printf("loadgen failed; report path: %s: %v", cfg.OutputPath, err)
		os.Exit(1)
	}

	log.Printf("loadgen completed; report path: %s", cfg.OutputPath)
}

// run 使用已经通过 parseLoadConfig 校验的配置执行一轮有限负载。
// 独占创建并关闭输出文件；已开始的批次即使失败或取消也尝试保存报告。
// 返回错误合并运行、非完整会话、输出及关闭失败；不退出进程、不自动重试。
func run(ctx context.Context, cfg loadConfig) (err error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("loadgen context already done: %w", ctxErr)
	}

	file, err := os.OpenFile(
		cfg.OutputPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0600,
	)
	if err != nil {
		return fmt.Errorf("create report %q: %w", cfg.OutputPath, err)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close report %q: %w", cfg.OutputPath, closeErr))
		}
	}()

	report, batchErr := loadgen.RunBatch(ctx, cfg.Batch)

	// RunBatch 的零 StartedAt 表示批次没有真正开始。
	// 此时没有合法报告可供 WriteBatchJSON 输出。
	if report.StartedAt.IsZero() {
		if batchErr != nil {
			return fmt.Errorf("run batch before start: %w", batchErr)
		}

		return errors.New("run batch returned an unstarted report without error")
	}

	incomplete := 0
	for _, result := range report.Results {
		if result.Report.Outcome != loadgen.SessionCompleted {
			incomplete++
		}
	}

	var outcomeErr error
	if incomplete != 0 {
		outcomeErr = fmt.Errorf("%d of %d sessions did not complete", incomplete, len(report.Results))
	}

	var saveErr error
	if writeErr := loadgen.WriteBatchJSON(file, report, batchErr); writeErr != nil {
		saveErr = fmt.Errorf("write report %q: %w", cfg.OutputPath, writeErr)
	}

	return errors.Join(batchErr, outcomeErr, saveErr)
}
