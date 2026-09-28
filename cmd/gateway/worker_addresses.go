package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// parseWorkerAddresses 解析逗号分隔的 host:port 列表。
// 去除每项首尾空白，保留顺序；拒绝空项、重复地址和非法地址格式。
// 本函数不解析 DNS、不建立连接，也不判断后端是否在线。
func parseWorkerAddresses(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			return nil, fmt.Errorf("worker address must not be empty")
		}

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid worker address %q: %w", addr, err)
		}

		if host == "" {
			return nil, fmt.Errorf("invalid worker address %q: host must not be empty", addr)
		}
		if strings.ContainsAny(host, " \t\r\n/\\") {
			return nil, fmt.Errorf("invalid worker address %q: invalid host %q", addr, host)
		}

		portNum, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid worker address %q: port must be a decimal number between 1 and 65535", addr)
		}
		if portNum == 0 {
			return nil, fmt.Errorf("invalid worker address %q: port must be between 1 and 65535", addr)
		}

		if _, ok := seen[addr]; ok {
			return nil, fmt.Errorf("duplicate worker address %q", addr)
		}
		seen[addr] = struct{}{}

		addrs = append(addrs, addr)
	}

	return addrs, nil
}
