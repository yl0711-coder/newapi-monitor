package monitor

import (
	"fmt"
	"strings"
	"time"
)

// Finance currently consumes these compact hourly aggregates directly. Until
// a separately verified financial archive replaces them, never prune its
// source interval. Raw logs/problem details still follow normal retention.
func (m *Monitor) financeProtectedAggregateCutoff(cutoff int64) (int64, error) {
	protect := m.cfg.FinanceEnabled || m.cfg.FinanceFactsSyncEnabled
	if !protect {
		// Disabling the UI/sync must not destroy a previously populated ledger.
		db := m.usageFactsStore()
		if db != nil && db.Migrator().HasTable(&FinanceUserHourState{}) {
			var count int64
			if err := db.Raw("SELECT COUNT(*) FROM (SELECT 1 FROM finance_user_hour_states LIMIT 1)").Scan(&count).Error; err != nil {
				return cutoff, fmt.Errorf("核验财务留存保护: %w", err)
			}
			protect = count > 0
		}
	}
	if !protect {
		return cutoff, nil
	}
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return cutoff, err
	}
	start, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(m.cfg.FinanceStartDate), loc)
	if err != nil {
		return cutoff, fmt.Errorf("核算起点无效，暂停清理核算依据: %w", err)
	}
	return min(cutoff, start.Unix()), nil
}
