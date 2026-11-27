package monitor

import (
	"fmt"
	"strings"
)

// SQL dialect detection is local. SQLite tests never receive MySQL hints.
// Bounded aggregates live in usage_logs_quota.go and keep bounds/SUM/COUNT
// inside one SELECT. The old two-autocommit-query span adapter is retired;
// concurrent inserts could otherwise fall outside its previously read MAX(id).
func (m *Monitor) usageLogsMySQL() bool {
	return m.prodDB != nil && strings.Contains(strings.ToLower(fmt.Sprintf("%T", m.prodDB.Driver())), "mysql")
}

// Explicit internal user IDs can use the existing user/time index. This page
// never infers an ID from mutable username metadata; raw names stay raw.
func (m *Monitor) usageLogsPageSource(f usageLogFilter) string {
	if f.UserID > 0 && m.usageLogsMySQL() {
		return "logs FORCE INDEX (idx_user_created_type)"
	}
	return "logs"
}
