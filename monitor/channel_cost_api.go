package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const channelCostSourceListLimit = 500
const channelCostHistoricalBindingMaxHours = 24 * 90
const channelCostHistoricalWeakOverlap = 0.1
const channelCostHistoricalStrongOverlap = 0.8

func (m *Monitor) channelCostAPIAllowed(domain string) bool {
	return m.cfg.ChannelCostClosureEnabled && channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, domain)
}

// channelCostRecoveryDomains returns domains that still own a durable pending
// activation. They remain visible to root operators after the feature flag is
// disabled or a domain is removed from the rollout allowlist, so the safety
// switch cannot strand an invisible task that nobody can cancel.
func (m *Monitor) channelCostRecoveryDomains(ctx context.Context) ([]string, error) {
	var domains []string
	err := m.storeDB.WithContext(ctx).Model(&ChannelFinanceActivationSlot{}).
		Distinct().Order("domain ASC").Pluck("domain", &domains).Error
	return domains, err
}

type channelCostSourceView struct {
	SourceRef               string                    `json:"source_ref"`
	SourceRefKind           string                    `json:"source_ref_kind"`
	HMACKeyID               string                    `json:"hmac_key_id"`
	SourceGroup             string                    `json:"source_group"`
	UpstreamModel           string                    `json:"upstream_model"`
	SourceGroups            []string                  `json:"source_groups" gorm:"-"`
	UpstreamModels          []string                  `json:"upstream_models" gorm:"-"`
	DimensionCount          int                       `json:"dimension_count" gorm:"-"`
	FirstHour               int64                     `json:"first_hour"`
	LastHour                int64                     `json:"last_hour"`
	Requests                int64                     `json:"requests"`
	ChargeUnits             int64                     `json:"charge_units"`
	ChargeUnit              string                    `json:"charge_unit"`
	CurrentBinding          *ChannelCostSourceBinding `json:"current_binding,omitempty" gorm:"-"`
	CurrentBindingSignature string                    `json:"current_binding_signature,omitempty" gorm:"-"`
	AttributionState        string                    `json:"attribution_state"`
	HistoricalBindingCount  int                       `json:"historical_binding_count" gorm:"-"`
	HistoricalBoundFrom     int64                     `json:"historical_bound_from,omitempty" gorm:"-"`
	HistoricalBoundTo       int64                     `json:"historical_bound_to,omitempty" gorm:"-"`
}

type channelCostSourceDimensionRow struct {
	SourceRef     string `gorm:"column:source_ref"`
	SourceGroup   string `gorm:"column:source_group"`
	UpstreamModel string `gorm:"column:upstream_model"`
}

type channelCostBindingInput struct {
	Domain         string `json:"domain" binding:"required"`
	AccountEpoch   string `json:"account_epoch" binding:"required"`
	SourceRef      string `json:"source_ref" binding:"required"`
	SourceRefKind  string `json:"source_ref_kind" binding:"required"`
	HMACKeyID      string `json:"hmac_key_id" binding:"required"`
	LocalChannelID int    `json:"local_channel_id"`
	ValidFrom      int64  `json:"valid_from"`
	ValidTo        int64  `json:"valid_to"`
	AllocationMode string `json:"allocation_mode" binding:"required"`
	Reason         string `json:"reason"`
	// nil/0 means the caller expects no current open binding. Replacing an
	// existing binding must echo its valid_from to prevent stale-page writes.
	ExpectedCurrentValidFrom *int64 `json:"expected_current_valid_from"`
	ExpectedCurrentSignature string `json:"expected_current_signature"`
}

type channelCostHistoricalBindingInput struct {
	Domain         string `json:"domain" binding:"required"`
	AccountEpoch   string `json:"account_epoch" binding:"required"`
	SourceRef      string `json:"source_ref" binding:"required"`
	LocalChannelID int    `json:"local_channel_id" binding:"required"`
	Reason         string `json:"reason" binding:"required"`
}

type channelCostHistoricalBindingPlan struct {
	Binding                  ChannelCostSourceBinding  `json:"binding"`
	ChannelName              string                    `json:"channel_name"`
	EvidenceHours            int64                     `json:"evidence_hours"`
	EvidenceRequests         int64                     `json:"evidence_requests"`
	EvidenceBilledCost       channelEconomicsMoneyView `json:"evidence_billed_cost"`
	LocalActiveHours         int64                     `json:"local_active_hours"`
	LocalRequests            int64                     `json:"local_requests"`
	LocalTestActiveHours     int64                     `json:"local_test_active_hours"`
	LocalTestRequests        int64                     `json:"local_test_requests"`
	ActiveEvidenceRequests   int64                     `json:"active_evidence_requests"`
	InactiveEvidenceRequests int64                     `json:"inactive_evidence_requests"`
	ActiveBilledCost         channelEconomicsMoneyView `json:"active_billed_cost"`
	InactiveBilledCost       channelEconomicsMoneyView `json:"inactive_billed_cost"`
	WillQueueHours           int64                     `json:"will_queue_hours"`
	HasLocalActivity         bool                      `json:"has_local_activity"`
	ActivityCoverage         float64                   `json:"activity_coverage"`
	TestActivityCoverage     float64                   `json:"test_activity_coverage"`
	CostActivityCoverage     float64                   `json:"cost_activity_coverage"`
	TemporalOverlapQuality   string                    `json:"temporal_overlap_quality"`
	RiskWarnings             []string                  `json:"risk_warnings"`
}

func historicalBindingEvidenceQuality(evidenceHours, localActiveHours int64) (float64, string, []string) {
	if evidenceHours <= 0 {
		return 0, "invalid", []string{"上游证据小时数无效"}
	}
	coverage := float64(localActiveHours) / float64(evidenceHours)
	switch {
	case localActiveHours == 0:
		return coverage, "no_activity", []string{"所选本地渠道在上游证据时段内没有同小时请求，不能仅凭当前配置确认历史归属"}
	case coverage < channelCostHistoricalWeakOverlap:
		return coverage, "weak", []string{"本地渠道同小时活动覆盖低于 10%，历史归属证据弱，需额外核对上游令牌或账户记录"}
	case coverage < channelCostHistoricalStrongOverlap:
		return coverage, "review", []string{"本地渠道同小时活动覆盖不足 80%，请核对缺失时段的采集覆盖或渠道变更记录"}
	default:
		return coverage, "strong", nil
	}
}

type channelFinanceActivationView struct {
	ActivationID      string `json:"activation_id"`
	Action            string `json:"action"`
	Status            string `json:"status"`
	EffectiveAt       int64  `json:"effective_at"`
	RequestedBy       string `json:"requested_by"`
	RequestedAt       int64  `json:"requested_at"`
	Reason            string `json:"reason"`
	AppliedVersion    int64  `json:"applied_version"`
	AppliedAt         int64  `json:"applied_at"`
	Attempts          int    `json:"attempts"`
	NextAttemptAt     int64  `json:"next_attempt_at"`
	LastError         string `json:"last_error"`
	RollbackOfVersion int64  `json:"rollback_of_version"`
	UpdatedAt         int64  `json:"updated_at"`
}

func channelFinanceActivationViewOf(row ChannelFinanceActivation) channelFinanceActivationView {
	return channelFinanceActivationView{
		ActivationID: row.ActivationID, Action: row.Action, Status: row.Status,
		EffectiveAt: row.EffectiveAt, RequestedBy: row.RequestedBy, RequestedAt: row.RequestedAt,
		Reason: row.Reason, AppliedVersion: row.AppliedVersion, AppliedAt: row.AppliedAt,
		Attempts: row.Attempts, NextAttemptAt: row.NextAttemptAt, LastError: row.LastError,
		RollbackOfVersion: row.RollbackOfVersion, UpdatedAt: row.UpdatedAt,
	}
}

type channelPricingProposalView struct {
	ChannelPricingChangeProposal
	Activation      *channelFinanceActivationView  `json:"activation,omitempty"`
	Impact          *channelFinanceActivationPatch `json:"impact,omitempty"`
	ImpactError     string                         `json:"impact_error,omitempty"`
	ImpactTotal     int                            `json:"impact_total"`
	ImpactTruncated bool                           `json:"impact_truncated"`
	RollbackAllowed bool                           `json:"rollback_allowed"`
}

const channelPricingImpactPreviewRows = 20

func compactChannelPricingImpact(view *channelPricingProposalView) {
	if view == nil || view.Impact == nil {
		return
	}
	view.ImpactTotal = len(view.Impact.Rows)
	if len(view.Impact.Rows) <= channelPricingImpactPreviewRows {
		return
	}
	copyPatch := channelFinanceActivationPatch{Rows: append([]channelFinanceActivationPatchRow(nil), view.Impact.Rows[:channelPricingImpactPreviewRows]...)}
	view.Impact = &copyPatch
	view.ImpactTruncated = true
}

func (m *Monitor) fullChannelPricingProposalImpact(ctx context.Context, proposal ChannelPricingChangeProposal) (channelFinanceActivationPatch, error) {
	switch proposal.Status {
	case "pending":
		return buildFinanceActivationPatch(m.storeDB.WithContext(ctx), proposal)
	case "scheduled", "rollback_scheduled":
		var slot ChannelFinanceActivationSlot
		if err := m.storeDB.WithContext(ctx).Where("domain = ?", proposal.Domain).First(&slot).Error; err != nil {
			return channelFinanceActivationPatch{}, errors.New("当前待生效任务不存在")
		}
		var activation ChannelFinanceActivation
		if err := m.storeDB.WithContext(ctx).Where("activation_id = ?", slot.ActivationID).First(&activation).Error; err != nil || activation.ProposalKey != proposal.ProposalKey || activation.Status != "scheduled" {
			return channelFinanceActivationPatch{}, errors.New("当前待生效任务与候选状态不一致")
		}
		return decodeFinanceActivationPatch(activation.PatchJSON)
	case "applied":
		var latest ChannelFinanceVersion
		if err := m.storeDB.WithContext(ctx).Where("domain = ?", proposal.Domain).Order("version DESC").First(&latest).Error; err != nil || latest.Version != proposal.AppliedVersion {
			return channelFinanceActivationPatch{}, errors.New("该倍率候选已不是当前生效版本，不能回滚")
		}
		var activation ChannelFinanceActivation
		if err := m.storeDB.WithContext(ctx).Where("proposal_key = ? AND action = 'approve' AND status = 'applied' AND applied_version = ?", proposal.ProposalKey, proposal.AppliedVersion).Order("requested_at DESC").First(&activation).Error; err != nil {
			return channelFinanceActivationPatch{}, errors.New("找不到与当前生效版本匹配的不可变倍率补丁")
		}
		patch, err := decodeFinanceActivationPatch(activation.PatchJSON)
		if err != nil {
			return channelFinanceActivationPatch{}, err
		}
		return reverseFinanceActivationPatch(patch), nil
	default:
		return channelFinanceActivationPatch{}, errors.New("当前候选状态没有可执行的倍率影响范围")
	}
}

type channelFinanceVersionChangeView struct {
	Scope    string `json:"scope"`
	Key      string `json:"key"`
	Field    string `json:"field"`
	OldValue string `json:"old_value"`
	NewValue string `json:"new_value"`
}

type channelFinanceVersionAuditView struct {
	Version      int64                             `json:"version"`
	EffectiveAt  int64                             `json:"effective_at"`
	CreatedAt    int64                             `json:"created_at"`
	UpdatedBy    string                            `json:"updated_by"`
	SnapshotHash string                            `json:"snapshot_hash"`
	Changes      []channelFinanceVersionChangeView `json:"changes"`
	Truncated    bool                              `json:"truncated"`
}

func financeAuditNumber(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func appendFinanceAuditChange(changes *[]channelFinanceVersionChangeView, scope, key, field, oldValue, newValue string) {
	if oldValue == newValue {
		return
	}
	*changes = append(*changes, channelFinanceVersionChangeView{Scope: scope, Key: key, Field: field, OldValue: oldValue, NewValue: newValue})
}

func diffChannelFinanceSnapshots(before, after channelFinanceVersionSnapshot) []channelFinanceVersionChangeView {
	changes := make([]channelFinanceVersionChangeView, 0)
	appendFinanceAuditChange(&changes, "domain", after.Domain, "fx_benchmark", financeAuditNumber(before.FXBenchmark), financeAuditNumber(after.FXBenchmark))
	appendFinanceAuditChange(&changes, "domain", after.Domain, "site_recharge_paid", financeAuditNumber(before.SiteRechargePaid), financeAuditNumber(after.SiteRechargePaid))
	appendFinanceAuditChange(&changes, "domain", after.Domain, "site_recharge_credit", financeAuditNumber(before.SiteRechargeCredit), financeAuditNumber(after.SiteRechargeCredit))
	appendFinanceAuditChange(&changes, "domain", after.Domain, "upstream_recharge_paid", financeAuditNumber(before.UpstreamRechargePaid), financeAuditNumber(after.UpstreamRechargePaid))
	appendFinanceAuditChange(&changes, "domain", after.Domain, "upstream_recharge_credit", financeAuditNumber(before.UpstreamRechargeCredit), financeAuditNumber(after.UpstreamRechargeCredit))
	beforeGroups := make(map[string]channelFinanceVersionGroup, len(before.Groups))
	afterGroups := make(map[string]channelFinanceVersionGroup, len(after.Groups))
	keys := make(map[string]bool, len(before.Groups)+len(after.Groups))
	for _, row := range before.Groups {
		beforeGroups[row.Group], keys[row.Group] = row, true
	}
	for _, row := range after.Groups {
		afterGroups[row.Group], keys[row.Group] = row, true
	}
	groupKeys := make([]string, 0, len(keys))
	for key := range keys {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)
	for _, key := range groupKeys {
		old, oldOK := beforeGroups[key]
		current, currentOK := afterGroups[key]
		if !oldOK || !currentOK {
			appendFinanceAuditChange(&changes, "group", key, "record", map[bool]string{true: "present", false: "missing"}[oldOK], map[bool]string{true: "present", false: "missing"}[currentOK])
			continue
		}
		appendFinanceAuditChange(&changes, "group", key, "site_multiplier", financeAuditNumber(old.SiteMultiplier), financeAuditNumber(current.SiteMultiplier))
		appendFinanceAuditChange(&changes, "group", key, "upstream_multiplier", financeAuditNumber(old.UpstreamMultiplier), financeAuditNumber(current.UpstreamMultiplier))
		appendFinanceAuditChange(&changes, "group", key, "upstream_discount_factor", financeAuditNumber(old.UpstreamDiscountFactor), financeAuditNumber(current.UpstreamDiscountFactor))
	}
	channelKey := func(row channelFinanceVersionChannel) string { return strconv.Itoa(row.ChannelID) + "/" + row.Group }
	beforeChannels := make(map[string]channelFinanceVersionChannel, len(before.ChannelRates))
	afterChannels := make(map[string]channelFinanceVersionChannel, len(after.ChannelRates))
	keys = make(map[string]bool, len(before.ChannelRates)+len(after.ChannelRates))
	for _, row := range before.ChannelRates {
		key := channelKey(row)
		beforeChannels[key], keys[key] = row, true
	}
	for _, row := range after.ChannelRates {
		key := channelKey(row)
		afterChannels[key], keys[key] = row, true
	}
	channelKeys := make([]string, 0, len(keys))
	for key := range keys {
		channelKeys = append(channelKeys, key)
	}
	sort.Strings(channelKeys)
	for _, key := range channelKeys {
		old, oldOK := beforeChannels[key]
		current, currentOK := afterChannels[key]
		if !oldOK || !currentOK {
			appendFinanceAuditChange(&changes, "channel_group", key, "record", map[bool]string{true: "present", false: "missing"}[oldOK], map[bool]string{true: "present", false: "missing"}[currentOK])
			continue
		}
		appendFinanceAuditChange(&changes, "channel_group", key, "upstream_group_name", old.UpstreamGroupName, current.UpstreamGroupName)
		appendFinanceAuditChange(&changes, "channel_group", key, "upstream_multiplier", financeAuditNumber(old.UpstreamMultiplier), financeAuditNumber(current.UpstreamMultiplier))
		appendFinanceAuditChange(&changes, "channel_group", key, "upstream_discount_factor", financeAuditNumber(old.UpstreamDiscountFactor), financeAuditNumber(current.UpstreamDiscountFactor))
	}
	return changes
}

func channelFinanceVersionAuditViews(rows []ChannelFinanceVersion) ([]channelFinanceVersionAuditView, error) {
	// The query is newest-first; reverse it so every row can be compared to its
	// immediate predecessor. When 101 rows are present, the oldest is baseline
	// only and is not returned, keeping the public payload bounded to 100.
	for left, right := 0, len(rows)-1; left < right; left, right = left+1, right-1 {
		rows[left], rows[right] = rows[right], rows[left]
	}
	start := 0
	if len(rows) > 100 {
		start = 1
	}
	views := make([]channelFinanceVersionAuditView, 0, len(rows)-start)
	var previous channelFinanceVersionSnapshot
	for i, row := range rows {
		var snapshot channelFinanceVersionSnapshot
		if err := json.Unmarshal([]byte(row.SnapshotJSON), &snapshot); err != nil {
			return nil, err
		}
		_, hash, err := channelFinanceSnapshotHash(row.SnapshotJSON)
		if err != nil {
			return nil, err
		}
		if i >= start {
			changes := []channelFinanceVersionChangeView{}
			if i > 0 {
				changes = diffChannelFinanceSnapshots(previous, snapshot)
			}
			truncated := len(changes) > 200
			if truncated {
				changes = changes[:200]
			}
			views = append(views, channelFinanceVersionAuditView{Version: row.Version, EffectiveAt: row.EffectiveAt, CreatedAt: row.CreatedAt, UpdatedBy: row.UpdatedBy, SnapshotHash: hash, Changes: changes, Truncated: truncated})
		}
		previous = snapshot
	}
	for left, right := 0, len(views)-1; left < right; left, right = left+1, right-1 {
		views[left], views[right] = views[right], views[left]
	}
	return views, nil
}

func (m *Monitor) listChannelCostSourcesHandler(c *gin.Context) {
	domain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	if domain == "" || len(domain) > 253 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "domain 无效"})
		return
	}
	// A local snapshot may inspect already-copied evidence and run the read-only
	// historical preview without enabling the production rollout. All binding
	// save handlers remain fail-closed in LocalSnapshotOnly mode.
	if !m.cfg.LocalSnapshotOnly && !m.channelCostAPIAllowed(domain) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该域名的渠道成本闭环未进入灰度"})
		return
	}
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(c.Request.Context()).Where("domain = ?", domain).First(&account).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "上游账户不存在"})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取上游账户失败"})
		}
		return
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	var views []channelCostSourceView
	if err := m.storeDB.WithContext(c.Request.Context()).Model(&ChannelUpstreamCostHourEvidence{}).
		Select(`source_ref, source_ref_kind, hmac_key_id,
			MIN(hour_ts) first_hour, MAX(hour_ts) last_hour, SUM(requests) requests,
			SUM(charge_units) charge_units, MAX(charge_unit) charge_unit`).
		Where("domain = ? AND account_epoch = ? AND semantics_version = ?", domain, epoch, channelCostEvidenceSemanticsVersion).
		Group("source_ref, source_ref_kind, hmac_key_id").
		Order("last_hour DESC, source_ref").Limit(channelCostSourceListLimit + 1).
		Scan(&views).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取渠道成本来源失败"})
		return
	}
	sourcesTruncated := len(views) > channelCostSourceListLimit
	if sourcesTruncated {
		views = views[:channelCostSourceListLimit]
	}
	refs := make([]string, 0, len(views))
	byRef := make(map[string]*channelCostSourceView, len(views))
	for i := range views {
		refs = append(refs, views[i].SourceRef)
		byRef[views[i].SourceRef] = &views[i]
	}
	if len(refs) > 0 {
		var dimensions []channelCostSourceDimensionRow
		if err := m.storeDB.WithContext(c.Request.Context()).Model(&ChannelUpstreamCostHourEvidence{}).
			Select("source_ref, source_group, upstream_model").
			Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND source_ref IN ?", domain, epoch, channelCostEvidenceSemanticsVersion, refs).
			Group("source_ref, source_group, upstream_model").Order("source_ref, source_group, upstream_model").
			Limit(channelCostSourceListLimit * 40).Scan(&dimensions).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取渠道成本来源维度失败"})
			return
		}
		groups := make(map[string]map[string]bool, len(views))
		models := make(map[string]map[string]bool, len(views))
		for _, dimension := range dimensions {
			view := byRef[dimension.SourceRef]
			if view == nil {
				continue
			}
			view.DimensionCount++
			if groups[dimension.SourceRef] == nil {
				groups[dimension.SourceRef], models[dimension.SourceRef] = map[string]bool{}, map[string]bool{}
			}
			if value := strings.TrimSpace(dimension.SourceGroup); value != "" {
				groups[dimension.SourceRef][value] = true
			}
			if value := strings.TrimSpace(dimension.UpstreamModel); value != "" {
				models[dimension.SourceRef][value] = true
			}
		}
		for i := range views {
			for value := range groups[views[i].SourceRef] {
				views[i].SourceGroups = append(views[i].SourceGroups, value)
			}
			for value := range models[views[i].SourceRef] {
				views[i].UpstreamModels = append(views[i].UpstreamModels, value)
			}
			sort.Strings(views[i].SourceGroups)
			sort.Strings(views[i].UpstreamModels)
			if len(views[i].SourceGroups) > 0 {
				views[i].SourceGroup = views[i].SourceGroups[0]
			}
			if len(views[i].UpstreamModels) > 0 {
				views[i].UpstreamModel = views[i].UpstreamModels[0]
			}
		}
		var bindings []ChannelCostSourceBinding
		if err := m.storeDB.WithContext(c.Request.Context()).Where(
			"domain = ? AND account_epoch = ? AND source_ref IN ? AND status = 'confirmed'",
			domain, epoch, refs,
		).Order("source_ref, valid_from").Find(&bindings).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取历史来源映射失败"})
			return
		}
		for _, binding := range bindings {
			view := byRef[binding.SourceRef]
			if view == nil {
				continue
			}
			evidenceTo := view.LastHour + 3600
			bindingTo := costIntervalEnd(binding.ValidTo)
			if binding.ValidFrom >= evidenceTo || bindingTo <= view.FirstHour {
				continue
			}
			view.HistoricalBindingCount++
			left := max(binding.ValidFrom, view.FirstHour)
			right := min(bindingTo, evidenceTo)
			if view.HistoricalBoundFrom == 0 || left < view.HistoricalBoundFrom {
				view.HistoricalBoundFrom = left
			}
			if right > view.HistoricalBoundTo {
				view.HistoricalBoundTo = right
			}
		}
	}
	// Manual mappings are scheduled for the next whole hour. Resolve the
	// upcoming effective binding so a successful save is immediately visible
	// instead of appearing to have been lost until the clock crosses the hour.
	lookupHour := nextWholeHour(time.Now().Unix())
	for i := range views {
		binding, err := m.costSourceBindingAt(c.Request.Context(), domain, epoch, views[i].SourceRef, lookupHour)
		if err != nil {
			views[i].AttributionState = "unattributable"
			continue
		}
		views[i].CurrentBinding = &binding
		views[i].CurrentBindingSignature = channelCostBindingSignature(binding)
		if binding.AllocationMode == "allocated" {
			views[i].AttributionState = "allocated"
		} else {
			views[i].AttributionState = binding.AllocationMode
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"domain": domain, "account_epoch": epoch, "enabled": m.channelCostEnabledFor(account),
		"sources": views, "truncated": sourcesTruncated,
	})
}

func normalizeChannelCostHistoricalBindingInput(in *channelCostHistoricalBindingInput) error {
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	in.AccountEpoch = strings.TrimSpace(in.AccountEpoch)
	in.SourceRef = strings.TrimSpace(in.SourceRef)
	in.Reason = strings.TrimSpace(in.Reason)
	if in.Domain == "" || len(in.Domain) > 253 {
		return errors.New("domain 无效")
	}
	if !validSHA256Hex(in.AccountEpoch) || !validSHA256Hex(in.SourceRef) {
		return errors.New("历史来源身份无效")
	}
	if in.Reason == "" || len(in.Reason) > 512 {
		return errors.New("必须填写 1-512 字符的历史归属审计原因")
	}
	return nil
}

// planChannelCostHistoricalBinding validates the exact finite interval and
// gathers same-hour local activity for operator review. It never writes.
func (m *Monitor) planChannelCostHistoricalBinding(ctx context.Context, in channelCostHistoricalBindingInput, createdBy string) (channelCostHistoricalBindingPlan, int, error) {
	var plan channelCostHistoricalBindingPlan
	if err := normalizeChannelCostHistoricalBindingInput(&in); err != nil {
		return plan, http.StatusBadRequest, err
	}
	var channel ChannelSnap
	if err := m.storeDB.WithContext(ctx).Where("id = ?", in.LocalChannelID).First(&channel).Error; err != nil {
		return plan, http.StatusBadRequest, errors.New("本地渠道不存在")
	}
	if strings.ToLower(strings.TrimSpace(channel.BaseDomain)) != in.Domain {
		return plan, http.StatusBadRequest, errors.New("本地渠道不属于该上游主域名")
	}
	var evidence struct {
		Provider           string
		SourceRefKind      string
		HMACKeyID          string
		ProviderCount      int64
		SourceRefKindCount int64
		HMACKeyIDCount     int64
		FirstHour          int64
		LastHour           int64
		Hours              int64
		Requests           int64
	}
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamCostHourEvidence{}).
		Select(`MAX(provider) provider, MAX(source_ref_kind) source_ref_kind, MAX(hmac_key_id) hmac_key_id,
			COUNT(DISTINCT provider) provider_count, COUNT(DISTINCT source_ref_kind) source_ref_kind_count,
			COUNT(DISTINCT hmac_key_id) hmac_key_id_count, MIN(hour_ts) first_hour, MAX(hour_ts) last_hour,
			COUNT(DISTINCT hour_ts) hours, COALESCE(SUM(requests), 0) requests`).
		Where("domain = ? AND account_epoch = ? AND source_ref = ? AND semantics_version = ?", in.Domain, in.AccountEpoch, in.SourceRef, channelCostEvidenceSemanticsVersion).
		Scan(&evidence).Error; err != nil {
		return plan, http.StatusServiceUnavailable, errors.New("读取历史计价证据失败")
	}
	if evidence.Hours == 0 {
		return plan, http.StatusBadRequest, errors.New("来源未在该账户代际的核验证据中出现")
	}
	if evidence.ProviderCount != 1 || evidence.SourceRefKindCount != 1 || evidence.HMACKeyIDCount != 1 ||
		strings.TrimSpace(evidence.Provider) == "" || strings.TrimSpace(evidence.SourceRefKind) == "" || strings.TrimSpace(evidence.HMACKeyID) == "" {
		return plan, http.StatusConflict, errors.New("历史来源元数据不一致，禁止自动归属")
	}
	if evidence.Hours > channelCostHistoricalBindingMaxHours {
		return plan, http.StatusRequestEntityTooLarge, errors.New("历史来源跨度超过单次安全回填上限，需分段审计")
	}
	if evidence.FirstHour < 0 || evidence.FirstHour%3600 != 0 || evidence.LastHour < evidence.FirstHour || evidence.LastHour%3600 != 0 || evidence.LastHour > math.MaxInt64-3600 {
		return plan, http.StatusConflict, errors.New("历史证据时间范围无效")
	}
	validTo := evidence.LastHour + 3600
	if validTo-evidence.FirstHour > int64(channelCostHistoricalBindingMaxHours)*3600 {
		return plan, http.StatusRequestEntityTooLarge, errors.New("历史来源跨度超过单次安全回填上限，需分段审计")
	}
	closedHour := time.Now().Unix() / 3600 * 3600
	if validTo <= evidence.FirstHour || validTo > closedHour {
		return plan, http.StatusConflict, errors.New("历史证据时间范围尚未闭合，请等待当前小时完成后重试")
	}
	var queueable struct {
		Hours int64
	}
	if err := m.storeDB.WithContext(ctx).Table("channel_upstream_cost_hour_evidence e").
		Select("COUNT(DISTINCT e.hour_ts) hours").
		Joins(`JOIN channel_upstream_cost_hour_states s
			ON s.domain=e.domain AND s.account_epoch=e.account_epoch AND s.hour_ts=e.hour_ts
			AND s.semantics_version=e.semantics_version`).
		Where("e.domain = ? AND e.account_epoch = ? AND e.source_ref = ? AND e.semantics_version = ?", in.Domain, in.AccountEpoch, in.SourceRef, channelCostEvidenceSemanticsVersion).
		Where("s.status = 'verified' AND s.reconcile_status = 'matched'").
		Scan(&queueable).Error; err != nil {
		return plan, http.StatusServiceUnavailable, errors.New("读取可发布历史计价证据失败")
	}
	var overlaps int64
	if err := m.storeDB.WithContext(ctx).Model(&ChannelCostSourceBinding{}).
		Where("domain = ? AND account_epoch = ? AND source_ref = ? AND status = 'confirmed'", in.Domain, in.AccountEpoch, in.SourceRef).
		Where("valid_from < ? AND (valid_to = 0 OR valid_to > ?)", validTo, evidence.FirstHour).Count(&overlaps).Error; err != nil {
		return plan, http.StatusServiceUnavailable, errors.New("核验已有历史映射失败")
	}
	if overlaps > 0 {
		return plan, http.StatusConflict, errors.New("同一来源存在重叠的已确认映射")
	}
	type localActivityHour struct {
		HourTs   int64
		Requests int64
	}
	var activityHours []localActivityHour
	if err := m.storeDB.WithContext(ctx).Model(&StabilityHourSample{}).
		Select("hour_ts, COALESCE(SUM(success + anomaly + failed), 0) requests").
		Where("channel_id = ?", in.LocalChannelID).
		Where(`EXISTS (SELECT 1 FROM channel_upstream_cost_hour_evidence e
			WHERE e.domain = ? AND e.account_epoch = ? AND e.source_ref = ?
			AND e.semantics_version = ? AND e.hour_ts = stability_hour_samples.hour_ts)`,
			in.Domain, in.AccountEpoch, in.SourceRef, channelCostEvidenceSemanticsVersion).
		Group("hour_ts").Order("hour_ts").Scan(&activityHours).Error; err != nil {
		return plan, http.StatusServiceUnavailable, errors.New("读取本地渠道同时段活动失败")
	}
	activeHours := make(map[int64]bool, len(activityHours))
	var localRequests int64
	for _, activity := range activityHours {
		activeHours[activity.HourTs] = true
		if err := addEconomicsInt64(&localRequests, activity.Requests); err != nil {
			return plan, http.StatusConflict, errors.New("本地渠道同时段请求数超出安全范围")
		}
	}
	type localTestActivityHour struct {
		HourTs   int64
		Requests int64
	}
	var testActivityHours []localTestActivityHour
	if err := m.storeDB.WithContext(ctx).Model(&ChannelTestHourSample{}).
		Select("hour_ts, COALESCE(SUM(requests), 0) requests").
		Where("channel_id = ? AND traffic_class_version = ?", in.LocalChannelID, stabilityTrafficClassificationVersion).
		Where(`EXISTS (SELECT 1 FROM channel_upstream_cost_hour_evidence e
			WHERE e.domain = ? AND e.account_epoch = ? AND e.source_ref = ?
			AND e.semantics_version = ? AND e.hour_ts = channel_test_hour_samples.hour_ts)`,
			in.Domain, in.AccountEpoch, in.SourceRef, channelCostEvidenceSemanticsVersion).
		Group("hour_ts").Order("hour_ts").Scan(&testActivityHours).Error; err != nil {
		return plan, http.StatusServiceUnavailable, errors.New("读取本地渠道内部测试活动失败")
	}
	var localTestRequests int64
	for _, activity := range testActivityHours {
		if err := addEconomicsInt64(&localTestRequests, activity.Requests); err != nil {
			return plan, http.StatusConflict, errors.New("本地渠道内部测试请求数超出安全范围")
		}
	}
	type evidenceCostHour struct {
		HourTs            int64
		ChargeUnits       int64
		ChargeUnitsPerUSD string
		Requests          int64
	}
	var costHours []evidenceCostHour
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamCostHourEvidence{}).
		Select("hour_ts, charge_units_per_usd, COALESCE(SUM(charge_units), 0) charge_units, COALESCE(SUM(requests), 0) requests").
		Where("domain = ? AND account_epoch = ? AND source_ref = ? AND semantics_version = ?", in.Domain, in.AccountEpoch, in.SourceRef, channelCostEvidenceSemanticsVersion).
		Group("hour_ts, charge_units_per_usd").Order("hour_ts").Scan(&costHours).Error; err != nil {
		return plan, http.StatusServiceUnavailable, errors.New("读取历史来源金额证据失败")
	}
	var evidenceCost, activeCost, activeEvidenceRequests int64
	for _, costHour := range costHours {
		cost, err := unitsToMicroUSDCanonical(costHour.ChargeUnits, costHour.ChargeUnitsPerUSD)
		if err != nil {
			return plan, http.StatusConflict, errors.New("历史来源包含无效计费单位，禁止回填")
		}
		if err := addEconomicsInt64(&evidenceCost, cost); err != nil {
			return plan, http.StatusConflict, errors.New("历史来源金额超出安全范围")
		}
		if activeHours[costHour.HourTs] {
			if err := addEconomicsInt64(&activeCost, cost); err != nil {
				return plan, http.StatusConflict, errors.New("同时段金额超出安全范围")
			}
			if err := addEconomicsInt64(&activeEvidenceRequests, costHour.Requests); err != nil {
				return plan, http.StatusConflict, errors.New("同时段上游请求数超出安全范围")
			}
		}
	}
	inactiveCost := evidenceCost - activeCost
	inactiveEvidenceRequests := evidence.Requests - activeEvidenceRequests
	if inactiveCost < 0 || inactiveEvidenceRequests < 0 {
		return plan, http.StatusConflict, errors.New("历史来源金额或请求数校验失败")
	}
	costCoverage := float64(0)
	if evidenceCost > 0 {
		costCoverage = float64(activeCost) / float64(evidenceCost)
	}
	row := ChannelCostSourceBinding{
		Domain: in.Domain, AccountEpoch: in.AccountEpoch, SourceRef: in.SourceRef,
		Provider: evidence.Provider, SourceRefKind: evidence.SourceRefKind, HMACKeyID: evidence.HMACKeyID,
		LocalChannelID: in.LocalChannelID, ValidFrom: evidence.FirstHour, ValidTo: validTo,
		Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual_history",
		Reason: in.Reason, CreatedBy: createdBy, CreatedAt: time.Now().Unix(),
	}
	coverage, quality, warnings := historicalBindingEvidenceQuality(evidence.Hours, int64(len(activityHours)))
	testCoverage := float64(0)
	if evidence.Hours > 0 {
		testCoverage = float64(len(testActivityHours)) / float64(evidence.Hours)
	}
	if localTestRequests > 0 {
		if localRequests == 0 {
			warnings = append(warnings, "该来源时段仅发现内部测试活动，不得将该成本当作客户流量成本发布")
		} else {
			warnings = append(warnings, "该来源时段同时存在客户流量与内部测试，后续需单独拆分内部测试成本")
		}
	}
	if queueable.Hours < evidence.Hours {
		warnings = append(warnings, fmt.Sprintf("%d 个小时尚未核验对平，本次仅将 %d 个小时加入重算队列", evidence.Hours-queueable.Hours, queueable.Hours))
	}
	plan = channelCostHistoricalBindingPlan{
		Binding: row, ChannelName: channel.Name, EvidenceHours: evidence.Hours,
		EvidenceRequests: evidence.Requests, EvidenceBilledCost: economicsMoney(evidenceCost),
		LocalActiveHours: int64(len(activityHours)), LocalRequests: localRequests,
		LocalTestActiveHours: int64(len(testActivityHours)), LocalTestRequests: localTestRequests,
		ActiveEvidenceRequests: activeEvidenceRequests, InactiveEvidenceRequests: inactiveEvidenceRequests,
		ActiveBilledCost: economicsMoney(activeCost), InactiveBilledCost: economicsMoney(inactiveCost),
		WillQueueHours: queueable.Hours, HasLocalActivity: len(activityHours) > 0,
		ActivityCoverage: coverage, TestActivityCoverage: testCoverage, CostActivityCoverage: costCoverage,
		TemporalOverlapQuality: quality, RiskWarnings: warnings,
	}
	return plan, http.StatusOK, nil
}

func (m *Monitor) previewChannelCostHistoricalBindingHandler(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var in channelCostHistoricalBindingInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "历史来源映射参数无效"})
		return
	}
	domain := strings.ToLower(strings.TrimSpace(in.Domain))
	if !m.cfg.LocalSnapshotOnly && !m.channelCostAPIAllowed(domain) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该域名的渠道成本闭环未进入灰度"})
		return
	}
	plan, status, err := m.planChannelCostHistoricalBinding(c.Request.Context(), in, c.GetString("uname"))
	if err != nil {
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, plan)
}

// saveChannelCostHistoricalBindingHandler attributes one observed source for
// exactly its currently recorded evidence lifetime. It is intentionally a
// separate endpoint from next-hour switching: historical writes are finite,
// auditable and cannot silently change future routing or pricing.
func (m *Monitor) saveChannelCostHistoricalBindingHandler(c *gin.Context) {
	if m.cfg.LocalSnapshotOnly {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地快照模式禁止写入历史来源映射"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var in channelCostHistoricalBindingInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "历史来源映射参数无效"})
		return
	}
	domain := strings.ToLower(strings.TrimSpace(in.Domain))
	if !m.channelCostAPIAllowed(domain) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该域名的渠道成本闭环未进入灰度"})
		return
	}
	plan, status, err := m.planChannelCostHistoricalBinding(c.Request.Context(), in, c.GetString("uname"))
	if err != nil {
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	row := plan.Binding
	if err := m.saveCostSourceBinding(c.Request.Context(), row); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "binding": row, "evidence_hours": plan.EvidenceHours})
}

func (m *Monitor) saveChannelCostBindingHandler(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var in channelCostBindingInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "来源映射参数无效"})
		return
	}
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	if !m.channelCostAPIAllowed(in.Domain) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该域名的渠道成本闭环未进入灰度"})
		return
	}
	in.Reason = strings.TrimSpace(in.Reason)
	if in.Reason == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "必须填写来源映射的审计原因"})
		return
	}
	if len(in.Reason) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "映射说明过长"})
		return
	}
	in.ExpectedCurrentSignature = strings.TrimSpace(in.ExpectedCurrentSignature)
	if in.ExpectedCurrentSignature != "" && !validSHA256Hex(in.ExpectedCurrentSignature) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "当前来源映射签名无效"})
		return
	}
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(c.Request.Context()).Where("domain = ?", in.Domain).First(&account).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "上游账户不存在"})
		return
	}
	currentEpoch := newAPIUpstreamAccountEpoch(account)
	if in.AccountEpoch != currentEpoch {
		c.JSON(http.StatusConflict, gin.H{"error": "上游账户身份已变化，请刷新后重新映射", "current_account_epoch": currentEpoch})
		return
	}
	var observed int64
	if err := m.storeDB.WithContext(c.Request.Context()).Model(&ChannelUpstreamCostHourEvidence{}).
		Where("domain = ? AND account_epoch = ? AND source_ref = ? AND source_ref_kind = ? AND hmac_key_id = ?", in.Domain, in.AccountEpoch, in.SourceRef, in.SourceRefKind, in.HMACKeyID).
		Count(&observed).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "核验来源证据失败"})
		return
	}
	if observed == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "来源未在当前账户代际的核验证据中出现"})
		return
	}
	if in.AllocationMode == "allocated" {
		var channel ChannelSnap
		if err := m.storeDB.WithContext(c.Request.Context()).Where("id = ? AND deleted_at = 0", in.LocalChannelID).First(&channel).Error; err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "本地渠道不存在或已删除"})
			return
		}
		if strings.ToLower(strings.TrimSpace(channel.BaseDomain)) != in.Domain {
			c.JSON(http.StatusBadRequest, gin.H{"error": "本地渠道不属于该上游主域名"})
			return
		}
	}
	nextHour := nextWholeHour(time.Now().Unix())
	if in.ValidFrom == 0 {
		in.ValidFrom = nextHour
	} else if in.ValidFrom != nextHour {
		c.JSON(http.StatusBadRequest, gin.H{"error": "来源映射只能从下一整点开始生效"})
		return
	}
	row := ChannelCostSourceBinding{
		Domain: in.Domain, AccountEpoch: in.AccountEpoch, SourceRef: in.SourceRef,
		Provider: account.Provider, SourceRefKind: in.SourceRefKind, HMACKeyID: in.HMACKeyID,
		LocalChannelID: in.LocalChannelID, ValidFrom: in.ValidFrom, ValidTo: in.ValidTo,
		Status: "confirmed", AllocationMode: in.AllocationMode, MappingSource: "manual",
		Reason: in.Reason, CreatedBy: c.GetString("uname"), CreatedAt: time.Now().Unix(),
	}
	if err := m.replaceCostSourceBinding(c.Request.Context(), row, in.ExpectedCurrentValidFrom, in.ExpectedCurrentSignature); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "binding": row})
}

func (m *Monitor) listChannelPricingProposalsHandler(c *gin.Context) {
	domain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	recoveryDomains, err := m.channelCostRecoveryDomains(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取待生效计价任务失败"})
		return
	}
	recoverySet := make(map[string]bool, len(recoveryDomains))
	for _, recoveryDomain := range recoveryDomains {
		recoverySet[recoveryDomain] = true
	}
	if domain != "" && !m.channelCostAPIAllowed(domain) && !recoverySet[domain] {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该域名的渠道成本闭环未进入灰度"})
		return
	}
	candidates := append([]string(nil), recoveryDomains...)
	if m.cfg.ChannelCostClosureEnabled {
		candidates = append(candidates, m.cfg.ChannelCostClosureDomains...)
	}
	visibleSet := make(map[string]bool, len(candidates))
	visibleDomains := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.ToLower(strings.TrimSpace(candidate))
		if candidate != "" && !visibleSet[candidate] {
			visibleSet[candidate] = true
			visibleDomains = append(visibleDomains, candidate)
		}
	}
	sort.Strings(visibleDomains)
	proposalScope := func() *gorm.DB {
		query := m.storeDB.WithContext(c.Request.Context()).Model(&ChannelPricingChangeProposal{})
		if domain != "" {
			return query.Where("domain = ?", domain)
		}
		return query.Where("domain IN ?", visibleDomains)
	}
	actionableStatuses := []string{"pending", "scheduled", "rollback_scheduled"}
	var actionable []ChannelPricingChangeProposal
	if err := proposalScope().Where("status IN ?", actionableStatuses).Order("created_at ASC, proposal_key ASC").Limit(501).Find(&actionable).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取计价变更候选失败"})
		return
	}
	actionableTruncated := len(actionable) > 500
	if actionableTruncated {
		actionable = actionable[:500]
	}
	proposals := append([]ChannelPricingChangeProposal(nil), actionable...)
	included := make(map[string]bool, 601)
	for _, row := range proposals {
		included[row.ProposalKey] = true
	}
	// Always retain the currently applied proposal for each allowlisted domain;
	// otherwise a large history tail could make the only valid rollback target
	// disappear from the page.
	domains := append([]string(nil), visibleDomains...)
	if domain != "" {
		domains = []string{domain}
	}
	latestVersionByDomain := make(map[string]int64, len(domains))
	for _, allowedDomain := range domains {
		var latest ChannelFinanceVersion
		if err := m.storeDB.WithContext(c.Request.Context()).Where("domain = ?", allowedDomain).Order("version DESC").First(&latest).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取当前计价版本失败"})
			return
		}
		latestVersionByDomain[allowedDomain] = latest.Version
		var applied ChannelPricingChangeProposal
		err := m.storeDB.WithContext(c.Request.Context()).Where("domain = ? AND status = 'applied' AND applied_version = ?", allowedDomain, latest.Version).Order("created_at DESC").First(&applied).Error
		if err == nil && !included[applied.ProposalKey] {
			proposals = append(proposals, applied)
			included[applied.ProposalKey] = true
		} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取当前生效倍率候选失败"})
			return
		}
	}
	var history []ChannelPricingChangeProposal
	historyQuery := proposalScope().Where("status NOT IN ?", actionableStatuses).Order("created_at DESC, proposal_key DESC").Limit(101)
	if len(included) > 0 {
		keys := make([]string, 0, len(included))
		for key := range included {
			keys = append(keys, key)
		}
		historyQuery = historyQuery.Where("proposal_key NOT IN ?", keys)
	}
	if err := historyQuery.Find(&history).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取计价变更历史失败"})
		return
	}
	historyTruncated := len(history) > 100
	if historyTruncated {
		history = history[:100]
	}
	proposals = append(proposals, history...)
	proposalsTruncated := actionableTruncated || historyTruncated
	views := make([]channelPricingProposalView, len(proposals))
	proposalKeys := make([]string, 0, len(proposals))
	for i := range proposals {
		views[i].ChannelPricingChangeProposal = proposals[i]
		views[i].RollbackAllowed = proposals[i].Status == "applied" && proposals[i].AppliedVersion > 0 && latestVersionByDomain[proposals[i].Domain] == proposals[i].AppliedVersion
		proposalKeys = append(proposalKeys, proposals[i].ProposalKey)
	}
	if len(proposalKeys) > 0 {
		var activations []ChannelFinanceActivation
		if err := m.storeDB.WithContext(c.Request.Context()).
			Where("proposal_key IN ?", proposalKeys).
			Order("requested_at DESC, activation_id DESC").Find(&activations).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取计价变更生效状态失败"})
			return
		}
		var slots []ChannelFinanceActivationSlot
		if err := m.storeDB.WithContext(c.Request.Context()).Find(&slots).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取待生效计价任务失败"})
			return
		}
		activationByID := make(map[string]ChannelFinanceActivation, len(activations))
		for _, row := range activations {
			activationByID[row.ActivationID] = row
		}
		activeByProposal := make(map[string]ChannelFinanceActivation, len(slots))
		for _, slot := range slots {
			if row, ok := activationByID[slot.ActivationID]; ok && row.Status == "scheduled" {
				activeByProposal[row.ProposalKey] = row
			}
		}
		appliedApproveByProposalVersion := make(map[string]ChannelFinanceActivation)
		for _, row := range activations { // newest first
			key := row.ProposalKey + "\x00" + strconv.FormatInt(row.AppliedVersion, 10)
			if _, exists := appliedApproveByProposalVersion[key]; !exists && row.Action == "approve" && row.Status == "applied" {
				appliedApproveByProposalVersion[key] = row
			}
		}
		for i := range views {
			var row ChannelFinanceActivation
			var selected bool
			switch views[i].Status {
			case "scheduled", "rollback_scheduled":
				row, selected = activeByProposal[views[i].ProposalKey]
			case "applied":
				if !views[i].RollbackAllowed {
					break
				}
				key := views[i].ProposalKey + "\x00" + strconv.FormatInt(views[i].AppliedVersion, 10)
				row, selected = appliedApproveByProposalVersion[key]
			}
			if !selected {
				if views[i].Status == "scheduled" || views[i].Status == "rollback_scheduled" || (views[i].Status == "applied" && views[i].RollbackAllowed) {
					views[i].ImpactError = "未找到与当前状态匹配的不可变倍率补丁"
				}
				continue
			}
			activation := channelFinanceActivationViewOf(row)
			views[i].Activation = &activation
			patch, err := decodeFinanceActivationPatch(row.PatchJSON)
			if err != nil {
				views[i].ImpactError = "不可变倍率补丁损坏，已停止展示影响范围"
			} else {
				if views[i].Status == "applied" {
					patch = reverseFinanceActivationPatch(patch)
				}
				views[i].Impact = &patch
			}
		}
	}
	// Pending proposals have no immutable activation patch yet. Preview the
	// exact rows approval would affect from the current local finance snapshot;
	// the decision transaction rebuilds and version-CAS checks this patch.
	pendingChannelIDs := make([]int, 0)
	pendingChannelSet := make(map[int]bool)
	for i := range views {
		if views[i].Status == "pending" && !pendingChannelSet[views[i].LocalChannelID] {
			pendingChannelSet[views[i].LocalChannelID] = true
			pendingChannelIDs = append(pendingChannelIDs, views[i].LocalChannelID)
		}
	}
	financeRowsByChannel := make(map[int][]ChannelFinanceChannelCost, len(pendingChannelIDs))
	if len(pendingChannelIDs) > 0 {
		var financeRows []ChannelFinanceChannelCost
		if err := m.storeDB.WithContext(c.Request.Context()).Where("channel_id IN ?", pendingChannelIDs).Order("channel_id, grp").Find(&financeRows).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取倍率影响范围失败"})
			return
		}
		for _, row := range financeRows {
			financeRowsByChannel[row.ChannelID] = append(financeRowsByChannel[row.ChannelID], row)
		}
	}
	for i := range views {
		if views[i].Status != "pending" {
			continue
		}
		patch, err := buildFinanceActivationPatchFromRows(proposals[i], financeRowsByChannel[proposals[i].LocalChannelID])
		if err != nil {
			views[i].ImpactError = err.Error()
			continue
		}
		views[i].Impact = &patch
	}
	for i := range views {
		compactChannelPricingImpact(&views[i])
	}
	versions := []channelFinanceVersionAuditView{}
	versionsTruncated := false
	if domain != "" {
		var rows []ChannelFinanceVersion
		if err := m.storeDB.WithContext(c.Request.Context()).Where("domain = ?", domain).Order("version DESC").Limit(101).Find(&rows).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取计价版本历史失败"})
			return
		}
		versionsTruncated = len(rows) > 100
		var err error
		versions, err = channelFinanceVersionAuditViews(rows)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "计价版本历史损坏，已停止展示"})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"proposals": views, "proposals_truncated": proposalsTruncated, "actionable_truncated": actionableTruncated, "versions": versions, "versions_truncated": versionsTruncated})
}

func (m *Monitor) getChannelPricingProposalImpactHandler(c *gin.Context) {
	if !m.cfg.ChannelCostClosureEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "渠道成本闭环未进入灰度"})
		return
	}
	proposalKey := strings.TrimSpace(c.Param("proposal_key"))
	if !validSHA256Hex(proposalKey) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "倍率候选标识无效"})
		return
	}
	var proposal ChannelPricingChangeProposal
	if err := m.storeDB.WithContext(c.Request.Context()).Where("proposal_key = ?", proposalKey).First(&proposal).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "倍率候选不存在"})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取倍率候选失败"})
		}
		return
	}
	if !m.channelCostAPIAllowed(proposal.Domain) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该域名的渠道成本闭环未进入灰度"})
		return
	}
	patch, err := m.fullChannelPricingProposalImpact(c.Request.Context(), proposal)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"proposal_key": proposal.ProposalKey, "status": proposal.Status, "impact": patch, "impact_total": len(patch.Rows)})
}
