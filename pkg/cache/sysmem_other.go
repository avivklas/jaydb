//go:build !darwin && !linux

package cache

func detectOSMemory() int64 {
	return 0
}
