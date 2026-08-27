//go:build darwin

package cache

import (
	"math"

	"golang.org/x/sys/unix"
)

func detectOSMemory() int64 {
	if mem, err := unix.SysctlUint64("hw.memsize"); err == nil && mem > 0 && mem < math.MaxInt64 {
		return int64(mem)
	}
	return 0
}
