package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/secacy/tide-artisan/internal/workerpool"
)

// parseWorkerWeights 解析与有序后端列表对应的固定权重。
// 每个元素允许两侧空白，必须为十进制正整数。
// 元素数量必须等于 workerCount，总和不得超过
// workerpool.MaxTotalWeight；失败返回 nil 和错误。
func parseWorkerWeights(raw string, workerCount int) ([]int64, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != workerCount {
		return nil, fmt.Errorf("worker weights count %d does not match worker count %d", len(parts), workerCount)
	}

	weights := make([]int64, len(parts))
	var totalWeight int64

	for i, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			return nil, fmt.Errorf("worker weight at index %d is empty", i)
		}

		weight, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("worker weight at index %d is invalid: %w", i, err)
		}
		if weight <= 0 {
			return nil, fmt.Errorf("worker weight at index %d must be positive", i)
		}
		if weight > workerpool.MaxTotalWeight-totalWeight {
			return nil, fmt.Errorf("total worker weight exceeds maximum %d", workerpool.MaxTotalWeight)
		}

		weights[i] = weight
		totalWeight += weight
	}

	return weights, nil
}
