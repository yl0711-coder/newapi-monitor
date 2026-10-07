package monitor

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// Operational diagnostics only. These counters never authorize a cost,
// attribution, internal deduction or profit. In particular, verified empty
// hours must not be presented as successfully paired business activity.
type financePairingHourView struct {
	Domain                 string `json:"domain"`
	PublishedHours         int64  `json:"published_hours"`
	PairedActivityHours    int64  `json:"paired_activity_hours"`
	VerifiedEmptyHours     int64  `json:"verified_empty_hours"`
	UnallocatedCostHours   int64  `json:"unallocated_cost_hours"`
	UpstreamZeroCheckHours int64  `json:"upstream_zero_check_hours"`
	OtherIncompleteHours   int64  `json:"other_incomplete_hours"`
}

// Aggregate only the current manifest's account epoch and current child
// revisions. Old epochs, superseded children and unpublished evidence cannot
// inflate progress. The existing hour index bounds every child lookup.
const financePairingHoursSQL = `WITH hour_facts AS (
	SELECT mp.domain,mp.hour_ts,mp.coverage_status,mp.profit_known,mp.local_fact_status,
		mp.row_count,COUNT(c.publication_id) actual_rows,
		COALESCE(SUM(CASE WHEN c.publication_id IS NOT NULL AND p.coverage_status!='superseded_empty'
			AND (p.local_requests!=0 OR p.upstream_requests!=0 OR p.local_consume_quota!=0
			OR p.local_refund_records!=0 OR p.local_refund_quota!=0 OR p.upstream_charge_units!=0
			OR p.revenue_micro_usd!=0 OR p.upstream_cost_micro_usd!=0 OR p.corrected_cost_micro_usd!=0)
			THEN 1 ELSE 0 END),0) activity_rows,
		COALESCE(SUM(CASE WHEN c.publication_id IS NOT NULL AND p.coverage_status!='superseded_empty'
			AND (p.coverage_status!='verified_complete' OR p.profit_known!=1) THEN 1 ELSE 0 END),0) blocked_rows,
		COALESCE(SUM(CASE WHEN c.publication_id IS NOT NULL AND p.coverage_status='unallocated_cost' THEN 1 ELSE 0 END),0) unallocated_rows,
		COALESCE(SUM(CASE WHEN c.publication_id IS NOT NULL AND p.coverage_status='upstream_cost_missing'
			AND (p.local_requests>0 OR p.local_consume_quota>0 OR p.local_refund_records>0 OR p.local_refund_quota>0)
			THEN 1 ELSE 0 END),0) missing_cost_rows,
		CASE WHEN s.status='verified' AND s.reconcile_status='matched' AND s.content_hash=mp.cost_source_hash
			AND s.control_charge_units=0 AND s.evidence_charge_units=0 AND s.requests=0
			AND s.evidence_rows=0 AND s.reconcile_delta=0 THEN 1 ELSE 0 END verified_zero_source
	FROM channel_economics_hour_manifest_current mc
	CROSS JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id
	LEFT JOIN channel_economics_hour_publications p INDEXED BY idx_channel_economics_hour_publications_hour_ts
		ON p.domain=mp.domain AND p.hour_ts=mp.hour_ts AND p.account_epoch=mp.authoritative_epoch
		AND p.semantics_version=mp.semantics_version
	LEFT JOIN channel_economics_hour_current c ON c.publication_id=p.publication_id
	LEFT JOIN channel_upstream_cost_hour_states s ON s.domain=mp.domain AND s.hour_ts=mp.hour_ts
		AND s.account_epoch=mp.authoritative_epoch AND s.semantics_version=mp.semantics_version
	WHERE mc.semantics_version=? AND mc.hour_ts>=? AND mc.hour_ts<?
		AND mp.domain=mc.domain AND mp.hour_ts=mc.hour_ts AND mp.semantics_version=mc.semantics_version
	GROUP BY mp.domain,mp.hour_ts
), classified AS (
	SELECT domain,CASE
		WHEN actual_rows!=row_count THEN 'incomplete'
		WHEN coverage_status='verified_complete' AND profit_known=1 AND local_fact_status='verified'
			AND blocked_rows=0 AND activity_rows>0 THEN 'paired_activity'
		WHEN coverage_status='verified_complete' AND profit_known=1 AND local_fact_status='verified'
			AND blocked_rows=0 AND activity_rows=0 AND verified_zero_source=1 THEN 'verified_empty'
		WHEN unallocated_rows>0 THEN 'unallocated'
		WHEN missing_cost_rows>0 AND verified_zero_source=1 THEN 'upstream_zero_check'
		ELSE 'incomplete' END kind FROM hour_facts
)
SELECT domain,COUNT(*) published_hours,
	SUM(CASE WHEN kind='paired_activity' THEN 1 ELSE 0 END) paired_activity_hours,
	SUM(CASE WHEN kind='verified_empty' THEN 1 ELSE 0 END) verified_empty_hours,
	SUM(CASE WHEN kind='unallocated' THEN 1 ELSE 0 END) unallocated_cost_hours,
	SUM(CASE WHEN kind='upstream_zero_check' THEN 1 ELSE 0 END) upstream_zero_check_hours,
	SUM(CASE WHEN kind='incomplete' THEN 1 ELSE 0 END) other_incomplete_hours
FROM classified GROUP BY domain ORDER BY domain`

func loadFinancePairingHours(ctx context.Context, db *gorm.DB, scope stabilityScope) ([]financePairingHourView, error) {
	rows := []financePairingHourView{}
	if err := db.WithContext(ctx).Raw(financePairingHoursSQL, channelEconomicsSemanticsVersion,
		scope.FromTs, scope.ToTs).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("读取成本配对小时诊断: %w", err)
	}
	return rows, nil
}
