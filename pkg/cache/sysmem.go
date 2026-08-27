package cache

import (
	"math"
	"runtime/debug"
	"sync"
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

	// 2. OS-specific detection (cgroups/procfs on Linux, sysctl on Darwin)
	if mem := detectOSMemory(); mem > 0 {
		return mem
	}

	// 3. Default fallback: 512 MiB
	return 512 << 20
}
