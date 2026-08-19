package monitor

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
)

var errUsageFactHistoryDiskPressure = errors.New("全历史冷任务因事实卷空间水位暂停")

type usageFactDiskPressureLevel int64

const (
	usageFactDiskNormal usageFactDiskPressureLevel = iota
	usageFactDiskWarning
	usageFactDiskThrottled
	usageFactDiskColdBlocked
	usageFactDiskCritical
)

func (level usageFactDiskPressureLevel) String() string {
	switch level {
	case usageFactDiskWarning:
		return "warning"
	case usageFactDiskThrottled:
		return "throttled"
	case usageFactDiskColdBlocked:
		return "cold_blocked"
	case usageFactDiskCritical:
		return "critical"
	default:
		return "normal"
	}
}

func usageFactDiskPressure(total, free int64, coldStopPercent int, minFree int64) usageFactDiskPressureLevel {
	if total <= 0 || free < 0 || free > total {
		return usageFactDiskCritical
	}
	if coldStopPercent <= 0 || coldStopPercent > 90 {
		coldStopPercent = 80
	}
	if minFree <= 0 {
		minFree = 2 * 1024 * 1024 * 1024
	}
	usedBPS := (total - free) * 10_000 / total
	criticalPercent := coldStopPercent + 5
	if criticalPercent > 95 {
		criticalPercent = 95
	}
	criticalFree := minFree / 2
	if usedBPS >= int64(criticalPercent)*100 || free < criticalFree {
		return usageFactDiskCritical
	}
	if usedBPS >= int64(coldStopPercent)*100 || free < minFree {
		return usageFactDiskColdBlocked
	}
	if usedBPS >= int64(max(coldStopPercent-10, 0))*100 || free < minFree+minFree/2 {
		return usageFactDiskThrottled
	}
	if usedBPS >= int64(max(coldStopPercent-20, 0))*100 || free < minFree*2 {
		return usageFactDiskWarning
	}
	return usageFactDiskNormal
}

func (m *Monitor) usageFactHistoryCapacityOK() (bool, error) {
	path := m.cfg.UsageFactsStorePath
	if path == "" {
		path = m.cfg.StorePath
	}
	if path == "" {
		m.markUsageFactDiskCapacityUnknown()
		return false, fmt.Errorf("%w：事实库路径为空", errUsageFactHistoryDiskPressure)
	}
	dir := filepath.Dir(path)
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		m.markUsageFactDiskCapacityUnknown()
		return false, fmt.Errorf("%w：读取事实卷空间失败: %w", errUsageFactHistoryDiskPressure, err)
	}
	blockSize := uint64(fs.Bsize)
	total := int64(fs.Blocks * blockSize)
	free := int64(fs.Bavail * blockSize)
	usedBPS := int64(0)
	if total > 0 {
		usedBPS = (total - free) * 10_000 / total
	}
	m.usageFactsHistoryDiskFreeBytes.Store(free)
	m.usageFactsHistoryDiskUsedBPS.Store(usedBPS)
	maxUsed := m.cfg.UsageFactsHistoryMaxDiskUsedPercent
	if maxUsed <= 0 || maxUsed > 95 {
		maxUsed = 80
	}
	minFree := m.cfg.UsageFactsHistoryMinFreeBytes
	if minFree <= 0 {
		minFree = 2 * 1024 * 1024 * 1024
	}
	level := usageFactDiskPressure(total, free, maxUsed, minFree)
	m.usageFactsHistoryDiskLevel.Store(int64(level))
	blocked := level >= usageFactDiskColdBlocked
	m.usageFactsHistoryDiskBlocked.Store(blocked)
	if blocked {
		return false, fmt.Errorf("%w：level=%s free=%d used=%.2f%%", errUsageFactHistoryDiskPressure, level, free, float64(usedBPS)/100)
	}
	return true, nil
}

func (m *Monitor) markUsageFactDiskCapacityUnknown() {
	// An unreadable filesystem watermark is not evidence of spare capacity.
	// Fail toward the critical tier so a mount disappearance or permission error
	// cannot make Tail continue writing until SQLite reports ENOSPC.
	m.usageFactsHistoryDiskFreeBytes.Store(0)
	m.usageFactsHistoryDiskUsedBPS.Store(10_000)
	m.usageFactsHistoryDiskLevel.Store(int64(usageFactDiskCritical))
	m.usageFactsHistoryDiskBlocked.Store(true)
}

// ensureUsageFactDerivedWritesCapacity keeps the bounded recent Tail and
// profile snapshot alive through warning/throttled/cold-blocked tiers, but
// stops every derived write at critical. The cold worker has its own stricter
// 80% gate; this method closes the independent high-Tail/profile loophole.
func (m *Monitor) ensureUsageFactDerivedWritesCapacity() error {
	if !m.usageFactsFullHistoryEnabled() {
		return nil
	}
	_, err := m.usageFactHistoryCapacityOK()
	if usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()) < usageFactDiskCritical {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w：事实卷达到 critical 水位", errUsageFactHistoryDiskPressure)
}
