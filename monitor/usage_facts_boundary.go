package monitor

// usage_facts_boundary.go discovers the real, per-member source boundary used
// by the all-history worker.  It deliberately does not create jobs or publish
// snapshots: discovery is a small, independently testable source operation and
// its result is committed by the durable job state machine.

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

const (
	usageFactHistoryBoundaryBatch  = 50
	usageFactQuerySemanticsVersion = 1
)

// usageFactSourceBoundary distinguishes an account with no matching user
// traffic from one whose discovery query has not run.  RegisteredAt may be
// zero for a source-deleted legacy user; in that case the first retained log
// remains an authoritative lower bound.
type usageFactSourceBoundary struct {
	UserID       int64
	RegisteredAt int64
	FirstLogAt   int64
	LastLogAt    int64
}

func (b usageFactSourceBoundary) sourceFloorHour() (int64, bool) {
	// The logical coverage floor is the earlier of registration and the first
	// retained log. MIN(created_at) proves the registration-to-first-use gap is
	// empty, so the worker can advance across that interval without issuing a
	// query for every empty day. A migrated log may legitimately predate the
	// current users.created_at value and must then win.
	floor := b.RegisteredAt
	if b.FirstLogAt > 0 && (floor <= 0 || b.FirstLogAt < floor) {
		floor = b.FirstLogAt
	}
	if floor <= 0 {
		return 0, false
	}
	return usageFactDayStart(floor), true
}

// historyStartHour is the first day that needs a dimensional source query.
// The successful MIN query is itself the proof for the known-empty prefix.
// No-history users therefore have a coverage floor but no history query start.
func (b usageFactSourceBoundary) historyStartHour() (int64, bool) {
	if b.FirstLogAt <= 0 {
		return 0, false
	}
	return usageFactDayStart(b.FirstLogAt), true
}

func (b usageFactSourceBoundary) sourceCeilingHour() (int64, bool) {
	if b.LastLogAt <= 0 {
		return 0, false
	}
	return time.Unix(b.LastLogAt, 0).Add(time.Hour).Truncate(time.Hour).Unix(), true
}

func normalizedUsageFactHistoryIDs(ids []int64, maxBatch int) ([]int64, error) {
	if maxBatch <= 0 {
		return nil, errors.New("历史成员批次上限无效")
	}
	ordered := append([]int64(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	unique := ordered[:0]
	for _, id := range ordered {
		if id <= 0 {
			return nil, errors.New("历史成员 ID 必须为正整数")
		}
		if len(unique) == 0 || unique[len(unique)-1] != id {
			unique = append(unique, id)
		}
	}
	if len(unique) == 0 {
		return nil, errors.New("历史成员不能为空")
	}
	if len(unique) > maxBatch {
		return nil, fmt.Errorf("历史成员单批最多 %d 人", maxBatch)
	}
	return unique, nil
}

// discoverUsageFactSourceBoundaries performs two bounded index-friendly
// queries: a users primary-key lookup and a logs range-boundary lookup using
// (user_id, created_at, type).  MIN/MAX proves that no earlier/later matching
// user traffic exists in the currently selected source; it does not infer a
// boundary by scanning empty hours.
func (m *Monitor) discoverUsageFactSourceBoundaries(ctx context.Context, ids []int64) ([]usageFactSourceBoundary, error) {
	ordered, err := normalizedUsageFactHistoryIDs(ids, usageFactHistoryBoundaryBatch)
	if err != nil {
		return nil, err
	}
	if m.prodDB == nil {
		return nil, errSourceNotReady
	}
	inSQL, inArgs := usageIn("id", ordered)
	registered := make(map[int64]int64, len(ordered))
	err = m.withUsageFactHistorySourceQuery(ctx, func(qctx context.Context) error {
		rows, queryErr := m.prodDB.QueryContext(qctx,
			"SELECT id, COALESCE(created_at,0) FROM users WHERE "+inSQL, inArgs...)
		if queryErr != nil {
			return fmt.Errorf("读取用户注册边界失败: %w", queryErr)
		}
		defer rows.Close()
		for rows.Next() {
			var id, createdAt int64
			if scanErr := rows.Scan(&id, &createdAt); scanErr != nil {
				return scanErr
			}
			registered[id] = createdAt
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}

	type logBoundary struct{ first, last int64 }
	logs := make(map[int64]logBoundary, len(ordered))
	// COUNT(*) is deliberately absent: combining it with MIN/MAX would force a
	// full per-user history scan. One UNION statement performs two bounded index
	// seeks per member (oldest/newest qualifying row) while still consuming only
	// one source-query permit for the whole <=50 member batch.
	indexHint := " FORCE INDEX (idx_user_created_type)"
	if m.usageDayExpr != "" { // local fake SQLite used by tests
		indexHint = ""
	}
	predicate := m.channelTestSourcePredicateSQL()
	parts := make([]string, 0, len(ordered))
	args := make([]any, 0, len(ordered)*3)
	for range ordered {
		parts = append(parts, `SELECT ? AS user_id,
 COALESCE((SELECT created_at FROM logs`+indexHint+` WHERE user_id=? AND type IN (2,6) AND NOT (`+predicate+`) ORDER BY created_at ASC LIMIT 1),0) AS first_log_at,
 COALESCE((SELECT created_at FROM logs`+indexHint+` WHERE user_id=? AND type IN (2,6) AND NOT (`+predicate+`) ORDER BY created_at DESC LIMIT 1),0) AS last_log_at`)
	}
	for _, id := range ordered {
		args = append(args, id, id, id)
	}
	query := `SELECT /*+ MAX_EXECUTION_TIME(5000) */ user_id, first_log_at, last_log_at FROM (` +
		strings.Join(parts, " UNION ALL ") + `) usage_boundaries ORDER BY user_id`
	err = m.withUsageFactHistorySourceQuery(ctx, func(qctx context.Context) error {
		rows, queryErr := m.prodDB.QueryContext(qctx, query, args...)
		if queryErr != nil {
			return fmt.Errorf("读取用户日志边界失败: %w", queryErr)
		}
		defer rows.Close()
		seen := 0
		for rows.Next() {
			var id int64
			var item logBoundary
			if scanErr := rows.Scan(&id, &item.first, &item.last); scanErr != nil {
				return scanErr
			}
			seen++
			if seen > len(ordered) {
				return errors.New("用户日志边界结果超过成员批次")
			}
			logs[id] = item
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}

	out := make([]usageFactSourceBoundary, 0, len(ordered))
	for _, id := range ordered {
		item := logs[id]
		out = append(out, usageFactSourceBoundary{
			UserID:       id,
			RegisteredAt: registered[id],
			FirstLogAt:   item.first,
			LastLogAt:    item.last,
		})
	}
	return out, nil
}

// withUsageFactHistorySourceQuery gives cold history the low-priority source
// lane.  It never consumes the Tail execution budget while waiting, yields to
// interactive reads, and caps each SQL execution at five seconds.  A durable
// job treats busy/cancelled as retryable without fabricating coverage.
func (m *Monitor) withUsageFactHistorySourceQuery(ctx context.Context, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.usageInteractiveWaiters.Load() > 0 {
		return errUsageFactSourceBusy
	}
	releaseSource, err := m.acquireBackgroundSourceLow(ctx)
	if err != nil {
		return err
	}
	defer releaseSource()

	waitCtx, cancelWait := context.WithTimeout(ctx, usageFactSourceGateWait)
	err = m.acquireUsageGate(waitCtx)
	cancelWait()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return errUsageFactSourceBusy
		}
		return err
	}
	defer m.releaseUsageGate()
	if m.usageInteractiveWaiters.Load() > 0 {
		return errUsageFactSourceBusy
	}

	queryCtx, cancelQuery := context.WithTimeout(ctx, 5*time.Second)
	defer cancelQuery()
	queryStarted := time.Now()
	err = fn(queryCtx)
	queryDuration := time.Since(queryStarted)
	// Full-history is a low-priority bulk reader just like Stability migration:
	// every actual SQL contributes a 20%-duty cooldown. The cooldown is low-only,
	// so Tail/sampler still acquire the next global start window.
	m.deferBackgroundSourceStart(m.usageFactHistorySourceCooldown(queryDuration))
	if usageFactHistoryShouldReportSourceError(err) {
		m.reportSourceQueryError(err)
	}
	if queryCtx.Err() != nil && ctx.Err() == nil {
		return queryCtx.Err()
	}
	return err
}

func usageFactHistoryShouldReportSourceError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errUsageFactHistoryRangeTooLarge) || errors.Is(err, errUsageFactHistoryControl) ||
		errors.Is(err, errUsageMemberControlIntegrity) || errors.Is(err, errUsageFactSourceBusy) ||
		errors.Is(err, errSourceNotReady) {
		return false
	}
	// reportSourceQueryError itself distinguishes permanent MySQL permission /
	// schema failures from transient connection failures. Application-level
	// validation errors must never be classified as an unknown permanent source
	// failure and tear down the whole source epoch.
	var my *mysqlDriver.MySQLError
	if errors.As(err, &my) {
		if my.Number == 3024 { // workload-local MAX_EXECUTION_TIME; shrink the chunk
			return false
		}
		return true
	}
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	return isSourceConnectionFailure(err) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostname) || errors.As(err, &invalidCert)
}

func usageFactHistoryRangeShouldFallback(err error) bool {
	if errors.Is(err, errUsageFactHistoryRangeTooLarge) || errors.Is(err, errUsageFactHistoryControl) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var my *mysqlDriver.MySQLError
	return errors.As(err, &my) && my.Number == 3024
}
