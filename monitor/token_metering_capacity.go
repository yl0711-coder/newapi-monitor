package monitor

import (
	"path/filepath"
	"syscall"
)

// The legacy store keeps its existing 80% / 2 GiB guard. An explicitly
// isolated store can use an approved absolute reserve for the chosen disk.
// No production slot is held during this check.
func (m *Monitor) tokenMeteringDiskOK() bool {
	if !storeUsesFile(m.tokenStorePath()) {
		return true
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(m.tokenStorePath()), &fs); err != nil {
		return false
	}
	total, free := int64(fs.Blocks*uint64(fs.Bsize)), int64(fs.Bavail*uint64(fs.Bsize))
	percent, reserve := 80, int64(2<<30)
	if m.tokenDB != nil {
		if m.cfg.TokenStoreMaxUsedPercent != 0 {
			percent = m.cfg.TokenStoreMaxUsedPercent
		}
		if m.cfg.TokenStoreMinFreeBytes != 0 {
			reserve = m.cfg.TokenStoreMinFreeBytes
		}
		return tokenStoreDiskWithinBudget(total, free, percent, reserve)
	}
	return usageFactDiskPressure(total, free, percent, reserve) < usageFactDiskColdBlocked
}

func tokenStoreDiskWithinBudget(total, free int64, percent int, reserve int64) bool {
	if total <= 0 || free < 0 || free > total || percent < 50 || percent > 99 || reserve < 2<<30 {
		return false
	}
	return free >= reserve && float64(total-free)/float64(total)*100 < float64(percent)
}
