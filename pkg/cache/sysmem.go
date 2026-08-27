package cache

import (
	"bufio"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	availMemOnce   sync.Once
	cachedAvailMem int64
)

// AvailableMemory returns the detected total or available physical/cgroup memory in bytes.
// If detection fails, a conservative fallback of 512 MiB is returned.
func AvailableMemory() int64 {
	availMemOnce.Do(func() {
		cachedAvailMem = detectAvailableMemory()
	})
	return cachedAvailMem
}

// DefaultBudgetLimit returns 50% of the available system or container memory.
func DefaultBudgetLimit() int64 {
	mem := AvailableMemory()
	if mem <= 0 {
		return 256 << 20 // 256 MiB fallback (50% of 512 MiB)
	}
	return mem / 2
}

func detectAvailableMemory() int64 {
	// 1. Check GOMEMLIMIT if explicitly configured by the runtime or user
	if gomem := debug.SetMemoryLimit(-1); gomem > 0 && gomem < math.MaxInt64 {
		return gomem
	}

	// 2. On Linux, check cgroup v2 & v1 memory limits
	if runtime.GOOS == "linux" {
		if mem := readCgroupMemoryLimit(); mem > 0 {
			return mem
		}
		if mem := readProcMeminfo(); mem > 0 {
			return mem
		}
	}

	// 3. On Darwin (macOS), check sysctl hw.memsize
	if runtime.GOOS == "darwin" {
		if mem, err := unix.SysctlUint64("hw.memsize"); err == nil && mem > 0 && mem < math.MaxInt64 {
			return int64(mem)
		}
	}

	// 4. Default fallback: 512 MiB
	return 512 << 20
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
