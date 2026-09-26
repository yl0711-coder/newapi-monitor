package monitor

import (
	"context"
	"fmt"
	"strings"
)

// Seek the first eligible hour for each known channel using the existing
// (channel_id, hour_ts) indexes. Joining every historical model/group row to
// its channel before grouping needlessly makes coverage checks grow with all
// traffic. Keep both customer and test activity, including disabled/deleted
// channels; only the original eligibility predicates decide whether it counts.
// MIN ignores the NULL returned by a channel without eligible activity.
const financeUpstreamActivityQuery = `SELECT domain, MIN(hour_ts) first_ts FROM (
	SELECT LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain,
		(SELECT s.hour_ts FROM stability_hour_samples s
		 WHERE s.channel_id=c.id AND s.hour_ts<? AND s.traffic_class_version=?
		 AND (s.success+s.anomaly+s.failed<>0 OR s.quota<>0 OR s.refund_quota<>0)
		 ORDER BY s.hour_ts LIMIT 1) hour_ts
	FROM channel_snaps c
	UNION ALL
	SELECT LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain,
		(SELECT t.hour_ts FROM channel_test_hour_samples t
		 WHERE t.channel_id=c.id AND t.hour_ts<? AND t.traffic_class_version=?
		 AND (t.requests<>0 OR t.quota<>0)
		 ORDER BY t.hour_ts LIMIT 1) hour_ts
	FROM channel_snaps c
	UNION ALL
	SELECT LOWER(TRIM(domain)) domain, MIN(hour_ts) hour_ts
	FROM channel_upstream_usage_hours
	WHERE hour_ts<? AND (requests<>0 OR tokens<>0 OR quota<>0 OR cost_usd<>0)
	GROUP BY LOWER(TRIM(domain))
) activity GROUP BY domain HAVING MIN(hour_ts) IS NOT NULL`

func financeUpstreamActivityArgs(to int64) []any {
	return []any{to, stabilityTrafficClassificationVersion, to, stabilityTrafficClassificationVersion, to}
}

func (m *Monitor) loadFinanceUpstreamActivityStarts(ctx context.Context, to int64) (map[string]int64, error) {
	var starts []struct {
		Domain  string
		FirstTs int64
	}
	if err := m.storeDB.WithContext(ctx).Raw(financeUpstreamActivityQuery, financeUpstreamActivityArgs(to)...).Scan(&starts).Error; err != nil {
		return nil, fmt.Errorf("读取上游经营生效边界: %w", err)
	}
	firstByDomain := make(map[string]int64, len(starts))
	for _, row := range starts {
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if domain != "" && row.FirstTs >= 0 {
			firstByDomain[domain] = row.FirstTs
		}
	}
	return firstByDomain, nil
}
