//go:build linux

package cache

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"strings"
)

func detectOSMemory() int64 {
	if mem := readCgroupMemoryLimit(); mem > 0 {
		return mem
	}
	if mem := readProcMeminfo(); mem > 0 {
		return mem
	}
	return 0
}

func readCgroupMemoryLimit() int64 {
	// Cgroups v2: /sys/fs/cgroup/memory.max
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		str := strings.TrimSpace(string(data))
		if str != "max" && str != "" {
			if limit, err := strconv.ParseInt(str, 10, 64); err == nil && limit > 0 && limit < math.MaxInt64 {
				return limit
			}
		}
	}

	// Cgroups v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		str := strings.TrimSpace(string(data))
		if limit, err := strconv.ParseInt(str, 10, 64); err == nil && limit > 0 && limit < (1<<62) {
			return limit
		}
	}

	return 0
}

func readProcMeminfo() int64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil && kb > 0 {
					return kb * 1024
				}
			}
		}
	}
	return 0
}
