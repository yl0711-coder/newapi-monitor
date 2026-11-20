package usagelogs

import (
	"context"
	"fmt"
	"time"
)

// Count holds the exact total for a log list filter.
type Count struct {
	Total int64 `json:"total"`
}

// Stats holds consumption and rate statistics for the log view.
type Stats struct {
	Quota        any   `json:"quota"`
	QuotaSkipped bool  `json:"quota_skipped"`
	RPM          int64 `json:"rpm"`
	TPM          int64 `json:"tpm"`
}

// Count returns the exact list total, not the consume-only statistics count. Never apply a
// pagination cursor here or substitute the rate/count from /stat.
func (r *Request) Count(f Filter) (Count, error) {
	where, err := buildUsageLogWhere(f, usageLogWhereList)
	if err != nil {
		return Count{}, err
	}
	db, session, err := r.open(true)
	if err != nil {
		return Count{}, err
	}
	ctx := session
	var total int64
	started := time.Now()
	countFilter, err := usageLogsWithUserIndex(ctx, db, f)
	if err == nil && countFilter.userID > 0 {
		where, err = buildUsageLogWhere(countFilter, usageLogWhereList)
	}
	source := "logs"
	empty := false
	if err == nil {
		where, source, empty, _ = r.service.usageLogsWithIDSpan(ctx, db, countFilter, where)
	}
	if err == nil && !empty {
		err = db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM "+source+where.sql(), where.args...).Scan(&total)
	}
	usageLogsObserve("list_count", started, countFilter)
	if err != nil {
		err = usageLogsQueryErr(ctx, err)
		usageLogsWarnFailure("count", started, countFilter, err)
		return Count{}, fmt.Errorf("查询日志总数失败: %w", err)
	}
	return Count{Total: total}, nil
}

// Stat returns consumption and rate statistics, optionally skipping the quota sum.
func (r *Request) Stat(f Filter, skipQuota bool) (Stats, error) {
	quotaWhere, err := buildUsageLogWhere(f, usageLogWhereStat)
	if err != nil {
		return Stats{}, err
	}
	rateWhere, _ := buildUsageLogWhere(f, usageLogWhereRate)
	rateWhere.add("created_at >= ?", time.Now().Unix()-usageLogsRPMWindowSeconds)
	// skip_quota=1: the page defers the range SUM (the costly part over a wide
	// range) until the admin asks for it. The remaining last-minute rates are
	// cheap, so they take a list slot instead of queueing behind heavy work.
	db, session, err := r.open(!skipQuota)
	if err != nil {
		return Stats{}, err
	}
	ctx, cancel := context.WithTimeout(session, usageLogsQueryTimeout)
	defer cancel()
	statStarted := time.Now()
	statFilter, err := usageLogsWithUserIndex(ctx, db, f)
	if err == nil && statFilter.userID > 0 {
		if quotaWhere, err = buildUsageLogWhere(statFilter, usageLogWhereStat); err == nil {
			rateWhere, err = buildUsageLogWhere(statFilter, usageLogWhereRate)
			if err == nil {
				rateWhere.add("created_at >= ?", time.Now().Unix()-usageLogsRPMWindowSeconds)
			}
		}
	}
	if err != nil {
		err = usageLogsQueryErr(ctx, err)
		usageLogsWarnFailure("stat_user_index", statStarted, statFilter, err)
		return Stats{}, fmt.Errorf("查询统计数据失败: %w", err)
	}
	var quota, rpm, prompt, completion int64
	if !skipQuota {
		quotaStarted := time.Now()
		var quotaSource string
		var emptySpan bool
		quotaWhere, quotaSource, emptySpan, _ = r.service.usageLogsWithIDSpan(ctx, db, statFilter, quotaWhere)
		if !emptySpan {
			err = db.QueryRowContext(ctx, usageLogsSelect+"COALESCE(SUM(quota),0) FROM "+quotaSource+quotaWhere.sql(), quotaWhere.args...).Scan(&quota)
		}
		usageLogsObserve("stat_quota", quotaStarted, statFilter)
		if err != nil {
			err = usageLogsQueryErr(ctx, err)
			usageLogsWarnFailure("stat_quota", quotaStarted, statFilter, err)
			return Stats{}, fmt.Errorf("查询统计数据失败: %w", err)
		}
	}
	rateStarted := time.Now()
	err = db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*), COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0) FROM logs"+rateWhere.sql(), rateWhere.args...).Scan(&rpm, &prompt, &completion)
	if err == nil {
		// A driver can return a row while the client or query deadline is
		// cancelling. Do not report success for that request.
		err = ctx.Err()
	}
	if err != nil {
		err = usageLogsQueryErr(ctx, err)
		usageLogsWarnFailure("stat_rate", rateStarted, statFilter, err)
		return Stats{}, fmt.Errorf("查询统计数据失败: %w", err)
	}
	var quotaOut any = quota
	if skipQuota {
		quotaOut = nil
	}
	return Stats{Quota: quotaOut, QuotaSkipped: skipQuota, RPM: rpm, TPM: prompt + completion}, nil
}
