package monitor

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// syncChannelCostStatus is a bounded operational projection. It contains no
// raw source identity, credential, HMAC or upstream response body.
type syncChannelCostStatus struct {
	Enabled                bool             `json:"enabled"`
	ReportEnabled          bool             `json:"report_enabled"`
	Domains                []string         `json:"domains"`
	VerifiedCostHours      int64            `json:"verified_cost_hours"`
	PendingCostHours       int64            `json:"pending_cost_hours"`
	MismatchCostHours      int64            `json:"mismatch_cost_hours"`
	LatestVerifiedHour     int64            `json:"latest_verified_hour"`
	OldestIncompleteHour   int64            `json:"oldest_incomplete_hour"`
	CostDirty              int64            `json:"cost_dirty"`
	EconomicsDirty         int64            `json:"economics_dirty"`
	ManifestHours          int64            `json:"manifest_hours"`
	ProfitKnownHours       int64            `json:"profit_known_hours"`
	LatestManifestHour     int64            `json:"latest_manifest_hour"`
	UnallocatedSources     int64            `json:"unallocated_sources"`
	ProposalStatusCounts   map[string]int64 `json:"proposal_status_counts"`
	ActivationStatusCounts map[string]int64 `json:"activation_status_counts"`
	PendingActivationSlots int64            `json:"pending_activation_slots"`
	LastError              string           `json:"last_error,omitempty"`
}

func (m *Monitor) channelCostSyncStatus(ctx context.Context) (syncChannelCostStatus, error) {
	status := syncChannelCostStatus{
		Enabled: m.cfg.ChannelCostClosureEnabled, ReportEnabled: m.cfg.ChannelEconomicsReportEnabled,
		Domains: sortedUnique(m.cfg.ChannelCostClosureDomains), ProposalStatusCounts: map[string]int64{},
		ActivationStatusCounts: map[string]int64{},
	}
	if !status.Enabled || len(status.Domains) == 0 {
		return status, nil
	}
	type costTotals struct {
		Verified       int64
		Pending        int64
		Mismatch       int64
		LatestVerified int64
		OldestBad      int64
	}
	var costs costTotals
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT
		COALESCE(SUM(CASE WHEN status='verified' AND reconcile_status='matched' THEN 1 ELSE 0 END),0) verified,
		COALESCE(SUM(CASE WHEN status!='verified' OR reconcile_status NOT IN ('matched','mismatch') THEN 1 ELSE 0 END),0) pending,
		COALESCE(SUM(CASE WHEN reconcile_status='mismatch' THEN 1 ELSE 0 END),0) mismatch,
		COALESCE(MAX(CASE WHEN status='verified' AND reconcile_status='matched' THEN hour_ts ELSE 0 END),0) latest_verified,
		COALESCE(MIN(CASE WHEN status!='verified' OR reconcile_status!='matched' THEN hour_ts ELSE NULL END),0) oldest_bad
		FROM channel_upstream_cost_hour_states
		WHERE semantics_version=? AND domain IN ?`, channelCostEvidenceSemanticsVersion, status.Domains).Scan(&costs).Error; err != nil {
		return status, fmt.Errorf("读取计价证据状态: %w", err)
	}
	status.VerifiedCostHours, status.PendingCostHours, status.MismatchCostHours = costs.Verified, costs.Pending, costs.Mismatch
	status.LatestVerifiedHour, status.OldestIncompleteHour = costs.LatestVerified, costs.OldestBad
	if err := m.storeDB.WithContext(ctx).Model(&ChannelCostDirtyHour{}).
		Where("domain IN ? AND status IN ('pending','retry','running')", status.Domains).Count(&status.CostDirty).Error; err != nil {
		return status, fmt.Errorf("读取计价重算队列: %w", err)
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelEconomicsDirtyHour{}).
		Where("domain IN ? AND status IN ('pending','retry','running')", status.Domains).Count(&status.EconomicsDirty).Error; err != nil {
		return status, fmt.Errorf("读取经济账重算队列: %w", err)
	}
	type manifestTotals struct {
		Hours       int64
		ProfitKnown int64
		Latest      int64
	}
	var manifests manifestTotals
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT COUNT(*) hours,
		COALESCE(SUM(CASE WHEN mp.profit_known=1 AND mp.coverage_status='verified_complete' THEN 1 ELSE 0 END),0) profit_known,
		COALESCE(MAX(mp.hour_ts),0) latest
		FROM channel_economics_hour_manifest_current mc
		JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id
		WHERE mp.semantics_version=? AND mp.domain IN ?`, channelEconomicsSemanticsVersion, status.Domains).Scan(&manifests).Error; err != nil {
		return status, fmt.Errorf("读取经济账发布状态: %w", err)
	}
	status.ManifestHours, status.ProfitKnownHours, status.LatestManifestHour = manifests.Hours, manifests.ProfitKnown, manifests.Latest
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT COUNT(*) FROM (
		SELECT DISTINCT e.domain,e.account_epoch,e.source_ref
		FROM channel_upstream_cost_hour_evidence e
		LEFT JOIN channel_cost_source_bindings b
		  ON b.domain=e.domain AND b.account_epoch=e.account_epoch AND b.source_ref=e.source_ref
		 AND b.status='confirmed' AND b.valid_to=0
		WHERE e.semantics_version=? AND e.domain IN ? AND b.source_ref IS NULL
	)`, channelCostEvidenceSemanticsVersion, status.Domains).Scan(&status.UnallocatedSources).Error; err != nil {
		return status, fmt.Errorf("读取成本来源归属状态: %w", err)
	}
	var proposalCounts []struct {
		Status string
		Count  int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelPricingChangeProposal{}).Select("status,COUNT(*) count").Where("domain IN ?", status.Domains).Group("status").Scan(&proposalCounts).Error; err != nil {
		return status, fmt.Errorf("读取倍率候选状态: %w", err)
	}
	for _, row := range proposalCounts {
		status.ProposalStatusCounts[strings.TrimSpace(row.Status)] = row.Count
	}
	var activationCounts []struct {
		Status string
		Count  int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelFinanceActivation{}).Select("status,COUNT(*) count").Where("domain IN ?", status.Domains).Group("status").Scan(&activationCounts).Error; err != nil {
		return status, fmt.Errorf("读取倍率生效任务: %w", err)
	}
	for _, row := range activationCounts {
		status.ActivationStatusCounts[strings.TrimSpace(row.Status)] = row.Count
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelFinanceActivationSlot{}).Where("domain IN ?", status.Domains).Count(&status.PendingActivationSlots).Error; err != nil {
		return status, fmt.Errorf("读取待生效槽位: %w", err)
	}
	var lastErrors []string
	var stateError string
	_ = m.storeDB.WithContext(ctx).Model(&ChannelUpstreamCostHourState{}).
		Select("last_error").Where("domain IN ? AND last_error<>''", status.Domains).Order("updated_at DESC").Limit(1).Scan(&stateError).Error
	if strings.TrimSpace(stateError) != "" {
		lastErrors = append(lastErrors, strings.TrimSpace(stateError))
	}
	var dirtyError string
	_ = m.storeDB.WithContext(ctx).Model(&ChannelEconomicsDirtyHour{}).
		Select("last_error").Where("domain IN ? AND last_error<>''", status.Domains).Order("updated_at DESC").Limit(1).Scan(&dirtyError).Error
	if strings.TrimSpace(dirtyError) != "" {
		lastErrors = append(lastErrors, strings.TrimSpace(dirtyError))
	}
	sort.Strings(lastErrors)
	status.LastError = strings.Join(lastErrors, "；")
	if len(status.LastError) > 512 {
		status.LastError = status.LastError[:512]
	}
	return status, nil
}
