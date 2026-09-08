package monitor

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
)

var errAICodeWithRecordCapacity = errors.New("春秋记录回补因 Monitor 主库磁盘空间不足或不可读取而暂停；保留断点及已发布账单，释放空间后重试")

// Record checkpoints live in the MAIN store, not necessarily on the facts
// volume. Reuse the configured cold-import watermarks (default 80% / 2 GiB),
// but do not overwrite the facts-volume status with this independent reading.
// Check before fetching and again before committing each bounded page.
func (m *Monitor) ensureAICodeWithRecordCapacity() error {
	if m.cfg.StorePath == "" {
		return errAICodeWithRecordCapacity
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(m.cfg.StorePath), &fs); err != nil {
		return fmt.Errorf("%w: %w", errAICodeWithRecordCapacity, err)
	}
	total, free := int64(fs.Blocks*uint64(fs.Bsize)), int64(fs.Bavail*uint64(fs.Bsize))
	level := usageFactDiskPressure(total, free, m.cfg.UsageFactsHistoryMaxDiskUsedPercent, m.cfg.UsageFactsHistoryMinFreeBytes)
	if level >= usageFactDiskColdBlocked {
		return fmt.Errorf("%w: level=%s free_bytes=%d", errAICodeWithRecordCapacity, level, free)
	}
	return nil
}
