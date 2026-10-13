package monitor

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"
)

// Uses an existing bucket provenance field, not a new table or migration.
// Sparse hours are final only after the closed-day totals have been checked.
const sub2VerifiedDaySource = "sub2api_day_verified"

func markSub2UsageDayVerified(result *upstreamUsageResult) {
	for i := range result.Hours {
		result.Hours[i].Provisional = false
		result.Hours[i].SourceKind = sub2VerifiedDaySource
	}
}

// The normal collector needs the same independent control as manual prefix
// repair. Do not compare two moving snapshots for today's open window. Keep
// capability probes/manual previews separate so they retain their budgets.
func fetchSub2APIUsageForSync(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential, from, to int64, pacer *upstreamUsageRequestPacer, adapter string) (upstreamUsageResult, error) {
	result, err := fetchSub2APIUsageWindow(ctx, client, row, cred, from, to, pacer, adapter)
	if err != nil || to != from+86400 || to > cstDayStart(time.Now().Unix()) {
		return result, err
	}
	if result.Adapter == upstreamUsageAdapterSub2Trend {
		daily, err := fetchSub2APIUsageStats(ctx, client, row, cred, from, to, pacer)
		if err != nil {
			// Preserve typed auth/rate-limit errors, Retry-After and the shared
			// refresh-token recovery path. Never publish a partially checked day.
			return upstreamUsageResult{}, fmt.Errorf("核对 Sub2API 闭合日汇总失败: %w", err)
		}
		var requests, tokens int64
		var cost float64
		for _, hour := range result.Hours {
			if hour.Requests > math.MaxInt64-requests || hour.Tokens > math.MaxInt64-tokens {
				return upstreamUsageResult{}, fmt.Errorf("Sub2API 小时账单计数溢出，保留原账单")
			}
			requests += hour.Requests
			tokens += hour.Tokens
			cost += hour.CostUSD
		}
		if len(daily.Hours) != 1 || math.IsNaN(cost) || math.IsInf(cost, 0) ||
			requests != daily.Hours[0].Requests || tokens != daily.Hours[0].Tokens || math.Abs(cost-daily.Hours[0].CostUSD) > 0.000001 {
			return upstreamUsageResult{}, fmt.Errorf("Sub2API 小时账单与自然日汇总不一致，保留原账单并等待重试")
		}
	}
	// Older providers with only /usage/stats already supply one natural-day
	// total; keep that granularity rather than inventing an hourly breakdown.
	markSub2UsageDayVerified(&result)
	return result, nil
}

// A missing open-day hour must not erase money previously returned by the
// same provider. Preserve its timestamp/coverage, but revoke completeness.
// Other providers and verified zero corrections retain their original path.
func retainSub2OmittedUsageHours(existing, incoming []ChannelUpstreamUsageHour) (map[int64]int64, error) {
	oldByHour := make(map[int64]ChannelUpstreamUsageHour)
	for _, old := range existing {
		if old.Provider == upstreamProviderSub2API {
			oldByHour[old.HourTs] = old
		}
	}
	retained := make(map[int64]int64)
	for i, hour := range incoming {
		if hour.Provider != upstreamProviderSub2API || !hour.Provisional || hour.Requests != 0 || hour.Tokens != 0 || hour.Quota != 0 || hour.CostUSD != 0 {
			continue
		}
		if old, ok := oldByHour[hour.HourTs]; ok && old.Domain == hour.Domain {
			if old.BucketSeconds > hour.BucketSeconds {
				// In particular, never place a retained daily total into one
				// missing hour beside other hourly rows (overlap/double count).
				return nil, fmt.Errorf("Sub2API 缺失小时与已存账单粒度不兼容，保留原账单")
			}
			old.Provisional = true
			incoming[i] = old
			retained[hour.HourTs] = old.FetchedAt
		}
	}
	return retained, nil
}

// Correlated lookups use the existing (domain,hour_ts) key and only yesterday.
// No rows means no authority to extend an account's collection history. A
// verified day is either 24 disjoint hourly buckets or one natural-day bucket.
const sub2UnverifiedYesterdaySQL = `
 EXISTS (SELECT 1 FROM channel_upstream_usage_hours u
   WHERE u.domain=channel_upstream_accounts.domain AND u.provider='sub2api'
   AND u.hour_ts>=? AND u.hour_ts<?)
 AND NOT EXISTS (SELECT 1 FROM channel_upstream_usage_hours u
   WHERE u.domain=channel_upstream_accounts.domain AND u.provider='sub2api'
   AND u.hour_ts>=? AND u.hour_ts<?
   GROUP BY u.domain
   HAVING COUNT(*) IN (1,24) AND SUM(u.bucket_seconds)=86400
   AND MIN(CASE WHEN u.source_kind='sub2api_day_verified' AND COALESCE(u.provisional,0)=0
     AND u.fetched_at>=? AND
     ((u.bucket_seconds=3600 AND u.hour_ts%3600=0) OR
      (u.bucket_seconds=86400 AND u.hour_ts=?)) THEN 1 ELSE 0 END)=1)
`

// Sub2API tail reads today, not yesterday. Independently of history completion,
// finish yesterday first, once, on the SAME due history lane and request
// budget. A failed verification uses the existing history backoff. No cursor
// rewind, extra worker, schema change or request on page reads is needed.
func (m *Monitor) prioritizeSub2RecentUsage(ctx context.Context, row ChannelUpstreamAccount, now int64, plan upstreamUsageSyncPlan) (upstreamUsageSyncPlan, error) {
	today := cstDayStart(now)
	yesterday := today - 86400
	if row.Provider != upstreamProviderSub2API || plan.backfillTo <= plan.backfillFrom ||
		(!plan.recentOnly && plan.backfillFrom >= yesterday) {
		return plan, nil
	}
	pending := hasSub2PendingCloseout(row)
	if pending {
		day, err := sub2PendingCloseoutDay(row, now)
		if err != nil {
			return plan, err
		}
		yesterday, today = day, day+86400
		plan.backfillFrom, plan.backfillTo, plan.recentOnly = yesterday, today, true
	}
	var hours []ChannelUpstreamUsageHour
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", row.Domain, yesterday, today).
		Order("hour_ts ASC").Limit(25).Find(&hours).Error; err != nil {
		return plan, err
	}
	// Do not silently extend an account's collection start or fill a wholly
	// absent day. This path only closes a day already collected by the tail.
	if len(hours) == 0 {
		if pending {
			return plan, fmt.Errorf("Sub2API 待核验日期的原账单缺失，保留任务，不自动扩大采集范围")
		}
		if plan.recentOnly {
			plan.backfillFrom, plan.backfillTo, plan.recentOnly = 0, 0, false
		}
		return plan, nil
	}
	cursor := yesterday
	valid := len(hours) == 1 || len(hours) == 24
	for _, hour := range hours {
		if hour.Provider != upstreamProviderSub2API || hour.HourTs != cursor ||
			(hour.BucketSeconds != 3600 && (hour.BucketSeconds != 86400 || hour.HourTs != yesterday)) ||
			hour.Provisional || hour.SourceKind != sub2VerifiedDaySource || hour.FetchedAt < today {
			valid = false
			break
		}
		cursor += hour.BucketSeconds
	}
	if !valid || cursor != today {
		plan.backfillFrom, plan.backfillTo, plan.recentOnly = yesterday, today, true
	} else if plan.recentOnly {
		plan.backfillFrom, plan.backfillTo, plan.recentOnly = 0, 0, false
		plan.closeoutResolved = pending
	}
	return plan, nil
}
