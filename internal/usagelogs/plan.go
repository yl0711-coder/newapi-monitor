package usagelogs

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Query plans for the usage-log page on the new-api replica.
//
// logs has (user_id, created_at, type) but no (channel_id, created_at) index.
// Production 2026-11-18, 10-hour window:
//   - page rows filtered by username + channel: MySQL intersected the full
//     username and channel_id indexes (index_merge) and hit the 18s cap;
//     the same page through user_id + idx_user_created_type took 0.2-1.2s.
//   - COUNT/SUM filtered by channel only: every candidate row needed a random
//     lookup to test created_at (>18s); scanning the primary-key span of the
//     window sequentially took 0.3-1.2s.
// Both rewrites return exactly the same rows as the original predicates.

// usageLogsMySQL reports whether the replica speaks MySQL, so index hints may
// be used. The unit-test replica is SQLite, which rejects FORCE INDEX.
func (s *Service) usageLogsMySQL() bool {
	return s.replica != nil && strings.Contains(strings.ToLower(fmt.Sprintf("%T", s.replica.Driver())), "mysql")
}

// usageLogsPageSource is the FROM source for the page's id lookup. A filter
// already answered through user_id walks only that user's rows in the window.
func (s *Service) usageLogsPageSource(f Filter) string {
	if f.userID > 0 && s.usageLogsMySQL() {
		return "logs FORCE INDEX (idx_user_created_type)"
	}
	return "logs"
}

// usageLogsWantsIDSpan reports whether a COUNT/SUM should scan the primary-key
// span of its time window: a bounded window, no user/token/request filter (those
// have selective indexes of their own), and a filter on a column that no time
// index covers (channel, model, group), which otherwise costs one random row
// read per candidate.
func usageLogsWantsIDSpan(f Filter) bool {
	return f.userID == 0 && f.Username == "" && f.TokenName == "" && f.RequestID == "" && f.UpstreamRequestID == "" &&
		f.Start > 0 && f.End >= f.Start && (f.Channel != 0 || f.ModelName != "" || f.Group != "")
}

// usageLogsWithIDSpan narrows a COUNT/SUM to the primary-key span of the
// filter's time window and returns the FROM source to use with it.
//
// Exactness: lo/hi are MIN(id)/MAX(id) over the rows whose created_at is in the
// window, so every such row has lo <= id <= hi; the created_at predicates stay
// in where, so rows of the span outside the window are still excluded. The
// bound lookup reads only a created_at index. empty=true means the window has
// no rows at all. ok=false keeps the original plan (not MySQL, filter not
// eligible, or the bound lookup failed).
func (s *Service) usageLogsWithIDSpan(ctx context.Context, db usageLogsQuerier, f Filter, where *usageLogWhere) (narrowed *usageLogWhere, source string, empty, ok bool) {
	return usageLogsIDSpan(ctx, db, s.usageLogsMySQL(), f, where)
}

func usageLogsIDSpan(ctx context.Context, db usageLogsQuerier, mysql bool, f Filter, where *usageLogWhere) (narrowed *usageLogWhere, source string, empty, ok bool) {
	if !mysql || !usageLogsWantsIDSpan(f) {
		return where, "logs", false, false
	}
	var lo, hi sql.NullInt64
	err := db.QueryRowContext(ctx, usageLogsSelect+"MIN(id), MAX(id) FROM logs WHERE created_at >= ? AND created_at <= ?", f.Start, f.End).Scan(&lo, &hi)
	if err != nil {
		return where, "logs", false, false
	}
	if !lo.Valid || !hi.Valid {
		return where, "logs", true, true
	}
	narrowed = &usageLogWhere{clauses: append([]string{}, where.clauses...), args: append([]any{}, where.args...)}
	narrowed.add("id BETWEEN ? AND ?", lo.Int64, hi.Int64)
	return narrowed, "logs FORCE INDEX (PRIMARY)", false, true
}
