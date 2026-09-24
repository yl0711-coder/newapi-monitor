package monitor

// finance_report.go 是经营核算的第一阶段：只读 Monitor 已经入库的
// 用户用量、上游账单与不可变经济事实。它不是会计总账，不会把尚未接入的
// 赠送扣减或 AWS 成本当成 0，也不会在页面请求中访问 NewAPI、上游或 AWS。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
)

const (
	financeReportMaxDays            = 730
	financePairingSourceDetailLimit = 100
)

type financeSourceView struct {
	Key         string `json:"key"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Description string `json:"description"`
}

type financeStatementView struct {
	GrossUserConsumption             channelEconomicsMoneyView  `json:"gross_user_consumption"`
	UserRefunds                      channelEconomicsMoneyView  `json:"user_refunds"`
	UserConsumption                  *channelEconomicsMoneyView `json:"user_consumption"`
	KnownUserConsumption             channelEconomicsMoneyView  `json:"known_user_consumption"`
	RegistrationGiftConsumption      *channelEconomicsMoneyView `json:"registration_gift_consumption"`
	KnownRegistrationGiftConsumption channelEconomicsMoneyView  `json:"known_registration_gift_consumption"`
	OperatingRevenue                 *channelEconomicsMoneyView `json:"operating_revenue"`
	KnownOperatingRevenue            channelEconomicsMoneyView  `json:"known_operating_revenue"`
	InternalTestConsumption          channelEconomicsMoneyView  `json:"internal_test_consumption"`
	InternalTestRequests             int64                      `json:"internal_test_requests"`
	InternalTestUpstreamCost         *channelEconomicsMoneyView `json:"internal_test_upstream_cost"`
	KnownInternalTestUpstreamCost    channelEconomicsMoneyView  `json:"known_internal_test_upstream_cost"`
	InternalTestCostRows             int64                      `json:"internal_test_cost_rows"`
	InternalTestMixedRows            int64                      `json:"internal_test_mixed_rows"`
	InternalTestUnverifiedPairs      int64                      `json:"internal_test_unverified_pairs"`
	InternalCostDeductionStatus      string                     `json:"internal_cost_deduction_status,omitempty"`
	UpstreamBilledCost               *channelEconomicsMoneyView `json:"upstream_billed_cost"`
	KnownUpstreamBilledCost          channelEconomicsMoneyView  `json:"known_upstream_billed_cost"`
	RawCorrectedUpstreamCost         *channelEconomicsMoneyView `json:"raw_corrected_upstream_cost"`
	KnownRawCorrectedUpstreamCost    channelEconomicsMoneyView  `json:"known_raw_corrected_upstream_cost"`
	CorrectedUpstreamCost            *channelEconomicsMoneyView `json:"corrected_upstream_cost"`
	KnownCorrectedUpstreamCost       channelEconomicsMoneyView  `json:"known_corrected_upstream_cost"`
	PairedUserConsumption            channelEconomicsMoneyView  `json:"paired_user_consumption"`
	PairedCorrectedCost              channelEconomicsMoneyView  `json:"paired_corrected_upstream_cost"`
	ContributionProfit               *channelEconomicsMoneyView `json:"contribution_profit"`
	KnownContributionProfit          channelEconomicsMoneyView  `json:"known_contribution_profit"`
	ContributionMargin               *string                    `json:"contribution_margin_percent"`
	PairedContributionMargin         *string                    `json:"paired_contribution_margin_percent"`
	AWSInfrastructureCost            *channelEconomicsMoneyView `json:"aws_infrastructure_cost"`
	KnownAWSInfrastructureCost       channelEconomicsMoneyView  `json:"known_aws_infrastructure_cost"`
	OperatingProfit                  *channelEconomicsMoneyView `json:"operating_profit"`
	KnownOperatingProfit             channelEconomicsMoneyView  `json:"known_operating_profit"`
}

type financePeriodView struct {
	Period           string                      `json:"period"`
	From             int64                       `json:"from"`
	To               int64                       `json:"to"`
	Statement        financeStatementView        `json:"statement"`
	UserCoverage     StabilityDataCoverage       `json:"user_coverage"`
	UpstreamCoverage financeUpstreamCoverageView `json:"upstream_coverage"`
	Status           string                      `json:"status"`
}

type financeCostDetailView struct {
	Domain                         string                     `json:"domain"`
	Provider                       string                     `json:"provider,omitempty"`
	ProviderName                   string                     `json:"provider_name,omitempty"`
	UserRequests                   int64                      `json:"user_requests"`
	UserConsumption                channelEconomicsMoneyView  `json:"user_consumption"`
	UpstreamRequests               int64                      `json:"upstream_requests"`
	BilledCost                     *channelEconomicsMoneyView `json:"billed_cost"`
	KnownBilledCost                channelEconomicsMoneyView  `json:"known_billed_cost"`
	CorrectedCost                  *channelEconomicsMoneyView `json:"corrected_cost"`
	KnownCorrectedCost             channelEconomicsMoneyView  `json:"known_corrected_cost"`
	PairedRevenue                  channelEconomicsMoneyView  `json:"paired_revenue"`
	PairedCost                     channelEconomicsMoneyView  `json:"paired_cost"`
	PairedRows                     int64                      `json:"paired_rows"`
	Contribution                   *channelEconomicsMoneyView `json:"contribution"`
	KnownContribution              channelEconomicsMoneyView  `json:"known_contribution"`
	ExpectedHours                  int64                      `json:"expected_hours"`
	CompletedHours                 int64                      `json:"completed_hours"`
	DataUntil                      int64                      `json:"data_until"`
	CorrectionSource               string                     `json:"correction_source"`
	BillBasis                      string                     `json:"bill_basis"`
	Status                         string                     `json:"status"`
	PairingStatus                  string                     `json:"pairing_status"`
	ClosureReadiness               string                     `json:"closure_readiness"`
	ClosureNextAction              string                     `json:"closure_next_action"`
	ClosureBlockers                []string                   `json:"closure_blockers"`
	UnallocatedSources             int64                      `json:"unallocated_sources"`
	CostEvidenceHours              int64                      `json:"cost_evidence_hours"`
	CostVerifiedHours              int64                      `json:"cost_verified_hours"`
	CostEvidenceRequests           int64                      `json:"cost_evidence_requests"`
	CostVerifiedRequests           int64                      `json:"cost_verified_requests"`
	CostEvidenceFrom               int64                      `json:"cost_evidence_from"`
	FinanceVersions                int64                      `json:"finance_versions"`
	FinanceVersionFrom             int64                      `json:"finance_version_from"`
	FinanceHistoryCovered          bool                       `json:"finance_history_covered"`
	EvidenceBackfillStatus         string                     `json:"evidence_backfill_status"`
	EvidenceBackfillFrom           int64                      `json:"evidence_backfill_from"`
	EvidenceBackfillTo             int64                      `json:"evidence_backfill_to"`
	EvidenceBackfillCalendarHours  int64                      `json:"evidence_backfill_calendar_hours"`
	EvidenceBackfillActiveHours    int64                      `json:"evidence_backfill_active_hours"`
	EvidenceBackfillGranularity    string                     `json:"evidence_backfill_granularity"`
	EvidenceBackfillActiveDays     int64                      `json:"evidence_backfill_active_days"`
	EvidenceBackfillEstimatedCalls int64                      `json:"evidence_backfill_estimated_calls"`
	EvidenceBackfillEstimatedRuns  int64                      `json:"evidence_backfill_estimated_runs"`
	EvidenceBackfillClosesCost     bool                       `json:"evidence_backfill_closes_cost"`
	EvidenceBackfillNote           string                     `json:"evidence_backfill_note"`
}

type financeDailyView struct {
	Date                          string                         `json:"date"`
	From                          int64                          `json:"from"`
	To                            int64                          `json:"to"`
	Statement                     financeStatementView           `json:"statement"`
	UserCoverage                  financeDailyCoverageView       `json:"user_coverage"`
	EconomicsCoverage             channelEconomicsCoverageView   `json:"economics_coverage"`
	BillCoverage                  financeDailyBillCoverage       `json:"bill_coverage"`
	RechargeCorrection            financeDailyRechargeCorrection `json:"recharge_correction"`
	LedgerCorrection              financeDailyLedgerCorrection   `json:"ledger_correction"`
	CostReconciliation            financeDailyCostReconciliation `json:"cost_reconciliation"`
	InternalCostComplete          bool                           `json:"internal_cost_complete"`
	InternalCostUnverifiedReasons map[string]int64               `json:"internal_cost_unverified_reasons"`
	Status                        string                         `json:"status"`
	ledgerSourceCosts             map[string]int64
}

type financeDailyCoverageView struct {
	FromTs             int64   `json:"from_ts"`
	ToTs               int64   `json:"to_ts"`
	ExpectedHours      int64   `json:"expected_hours"`
	CompletedHours     int64   `json:"completed_hours"`
	MissingHours       int64   `json:"missing_hours"`
	Percent            float64 `json:"percent"`
	Complete           bool    `json:"complete"`
	LatestHourPending  bool    `json:"latest_hour_pending"`
	PendingHourTs      int64   `json:"pending_hour_ts,omitempty"`
	RequestedToTs      int64   `json:"requested_to_ts,omitempty"`
	ProvisionalSeconds int64   `json:"provisional_seconds,omitempty"`
}

type financeUpstreamCoverageView struct {
	RelevantDomains             int                       `json:"relevant_domains"`
	UsageEnabledDomains         int                       `json:"usage_enabled_domains"`
	AvailableDomains            int                       `json:"available_domains"`
	CompleteDomains             int                       `json:"complete_domains"`
	CorrectedDomains            int                       `json:"corrected_domains"`
	ExpectedDomainHours         int64                     `json:"expected_domain_hours"`
	CompletedDomainHours        int64                     `json:"completed_domain_hours"`
	UnconfiguredDomains         int                       `json:"unconfigured_domains"`
	UnconfiguredUserRequests    int64                     `json:"unconfigured_user_requests"`
	UnconfiguredUserConsumption channelEconomicsMoneyView `json:"unconfigured_user_consumption"`
	Complete                    bool                      `json:"complete"`
}

func (view financeUpstreamCoverageView) wholeCostScopeComplete() bool {
	return view.Complete && view.UnconfiguredDomains == 0
}

// financeInternalTestCostFact is intentionally limited to immutable economics
// rows whose channel-hour has test traffic and no customer traffic. Mixed
// customer/test hours are counted as unresolved and are never ratio-allocated.
type financeInternalTestCostFact struct {
	RevenueMicroUSD       int64
	UpstreamCostMicroUSD  int64
	CorrectedCostMicroUSD int64
	ProfitMicroUSD        int64
	Rows                  int64
}

type financeInternalTestCostEvidence struct {
	Total             financeInternalTestCostFact
	ByDomain          map[string]financeInternalTestCostFact
	ByDay             map[int64]financeInternalTestCostFact
	ExcludeByDomain   map[string]financeInternalTestCostFact
	ExcludeByDay      map[int64]financeInternalTestCostFact
	TestPairs         int64
	StrictPairs       int64
	MixedPairs        int64
	UnverifiedPairs   int64
	UnverifiedReasons map[string]int64
	Complete          bool
	SourceComplete    bool
	SourceScope       stabilityScope
	Events            []financeInternalTestCostEvent
}

type financeInternalTestCostEvent struct {
	Reason         string
	FinanceVersion int64
	HourTs         int64
	Domain         string
	State          string
	Fact           financeInternalTestCostFact
}

// financePairingAuditView explains why independently available revenue and
// upstream cost have not yet become a publishable contribution margin. It is
// deliberately derived from the immutable current-pointer ledger; it never
// reconstructs a margin by subtracting unrelated aggregates.
type financePairingAuditView struct {
	PublicationRows     int64                       `json:"publication_rows"`
	PairedRows          int64                       `json:"paired_rows"`
	BlockedRows         int64                       `json:"blocked_rows"`
	RelevantDomains     int                         `json:"relevant_domains"`
	LedgerDomains       []string                    `json:"ledger_domains"`
	UnenrolledDomains   []string                    `json:"unenrolled_domains"`
	UnallocatedSources  int64                       `json:"unallocated_sources"`
	UnallocatedByDomain map[string]int64            `json:"unallocated_sources_by_domain"`
	SourcesTruncated    bool                        `json:"sources_truncated"`
	StatusCounts        map[string]int64            `json:"status_counts"`
	Blockers            []financePairingBlockerView `json:"blockers"`
	Sources             []financePairingSourceView  `json:"sources"`
}

type financePairingBlockerView struct {
	Key           string                    `json:"key"`
	Name          string                    `json:"name"`
	Rows          int64                     `json:"rows"`
	Domains       int64                     `json:"domains"`
	DomainNames   []string                  `json:"domain_names"`
	ChannelIDs    []int                     `json:"channel_ids"`
	FirstHour     int64                     `json:"first_hour"`
	LastHour      int64                     `json:"last_hour"`
	Revenue       channelEconomicsMoneyView `json:"revenue"`
	CorrectedCost channelEconomicsMoneyView `json:"corrected_cost"`
	Description   string                    `json:"description"`
}

type financeOperatingReport struct {
	Enabled          bool                        `json:"enabled"`
	Stage            string                      `json:"stage"`
	GeneratedAt      int64                       `json:"generated_at"`
	DataAsOf         int64                       `json:"data_as_of,omitempty"`
	From             int64                       `json:"from"`
	To               int64                       `json:"to"`
	TimeZone         string                      `json:"time_zone"`
	Statement        financeStatementView        `json:"statement"`
	UserCoverage     StabilityDataCoverage       `json:"user_coverage"`
	GiftCoverage     financeGiftCoverageView     `json:"gift_coverage"`
	UpstreamCoverage financeUpstreamCoverageView `json:"upstream_coverage"`
	Periods          []financePeriodView         `json:"periods"`
	Days             []financeDailyView          `json:"days"`
	CostDetails      []financeCostDetailView     `json:"cost_details"`
	PairingAudit     financePairingAuditView     `json:"pairing_audit"`
	CURCost          financeCURCostView          `json:"cur_cost"`
	CURProducts      []financeCURProductCostView `json:"cur_products"`
	InternalAccounts financeInternalFactStatus   `json:"internal_accounts"`
	Sources          []financeSourceView         `json:"sources"`
	Notices          []string                    `json:"notices"`
	SemanticsNote    string                      `json:"semantics_note"`
}

type financePairingAuditRow struct {
	Status                string
	Domain                string
	LocalChannelID        int
	FirstHour             int64
	LastHour              int64
	Rows                  int64
	RevenueMicroUSD       int64
	CorrectedCostMicroUSD int64
}

type financePairingBlockerAccumulator struct {
	view                  financePairingBlockerView
	domains               map[string]bool
	channels              map[int]bool
	revenueMicroUSD       int64
	correctedCostMicroUSD int64
}

type financePairingSourceView struct {
	Domain                  string                    `json:"domain"`
	SourceRef               string                    `json:"source_ref"`
	SourceGroups            []string                  `json:"source_groups"`
	UpstreamModels          []string                  `json:"upstream_models"`
	FirstHour               int64                     `json:"first_hour"`
	LastHour                int64                     `json:"last_hour"`
	EvidenceHours           int64                     `json:"evidence_hours"`
	Requests                int64                     `json:"requests"`
	BilledCost              channelEconomicsMoneyView `json:"billed_cost"`
	CandidateState          string                    `json:"candidate_state"`
	CurrentConfigCandidates []financePairingCandidate `json:"current_config_candidates"`
}

// financePairingCandidate 只是当前配置的人工核对提示。它既不是历史归属证据，
// 也不会写入绑定表或参与任何收入、成本和毛利计算。
type financePairingCandidate struct {
	ChannelID     int      `json:"channel_id"`
	ChannelName   string   `json:"channel_name"`
	MatchedGroups []string `json:"matched_groups"`
}

type financePairingSourceRow struct {
	Domain        string
	AccountEpoch  string
	SourceRef     string
	FirstHour     int64
	LastHour      int64
	EvidenceHours int64
	Requests      int64
}

type financePairingDomainSourceCountRow struct {
	Domain  string
	Sources int64
}

type financeClosureCostEvidenceRow struct {
	Domain           string
	FirstHour        int64
	EvidenceHours    int64
	VerifiedHours    int64
	EvidenceRequests int64
	VerifiedRequests int64
}

type financeClosureVersionRow struct {
	Domain         string
	Versions       int64
	FirstEffective int64
}

type financeEvidenceBackfillRow struct {
	Domain        string
	FirstHour     int64
	LastTo        int64
	ActiveHours   int64
	HourlyBuckets int64
	DailyBuckets  int64
}

type financeEvidenceBackfillBucketRow struct {
	Domain        string
	HourTs        int64
	BucketSeconds int64
	Requests      int64
}

type financePairingSourceDimensionRow struct {
	Domain        string
	AccountEpoch  string
	SourceRef     string
	SourceGroup   string
	UpstreamModel string
}

type financePairingSourceCostRow struct {
	Domain            string
	AccountEpoch      string
	SourceRef         string
	ChargeUnitsPerUSD string
	ChargeUnits       int64
}

type financePairingSourceAccumulator struct {
	view       financePairingSourceView
	groups     map[string]bool
	models     map[string]bool
	billedCost int64
}

func financePairingBlockerDescription(status string) string {
	switch status {
	case "unallocated_cost":
		return "上游成本已经入账，但来源尚未在对应有效时段归属到本地渠道；禁止与任意收入直接相减。"
	case "upstream_cost_missing":
		return "本地渠道收入已经入账，但同渠道同小时没有已归属的上游成本。"
	case "finance_version_missing":
		return "该小时缺少当时有效的充值/折扣版本，无法计算修正成本。"
	case "local_revenue_missing":
		return "上游成本已经归属，但同渠道同小时没有可核验的本地收入事实。"
	case "local_fact_unverified":
		return "本地收入存在，但该小时采集尚未通过完整性校验。"
	case "refund_unallocated":
		return "退款记录无法归属到确定渠道，为避免高估收入而暂停毛利发布。"
	case "manifest_children_mismatch", "manifest_status_unknown":
		return "不可变发布清单与明细不一致，需要重新校验发布链路。"
	default:
		return "该状态未满足收入、成本、渠道归属和财务版本同时核验的发布条件。"
	}
}

func financePairingBlockerName(status string) string {
	switch status {
	case "unallocated_cost":
		return "成本来源未归属"
	case "upstream_cost_missing":
		return "收入缺少对应成本"
	case "finance_version_missing":
		return "历史财务版本缺失"
	case "local_revenue_missing":
		return "成本缺少对应收入"
	case "local_fact_unverified":
		return "用户用量小时未核验"
	case "refund_unallocated":
		return "退款归属不明确"
	case "manifest_children_mismatch":
		return "发布清单明细不一致"
	case "manifest_status_unknown":
		return "发布清单状态未知"
	default:
		return status
	}
}

const financeUnallocatedSourceScopeSQL = `e.semantics_version=? AND e.hour_ts>=? AND e.hour_ts<?
	AND NOT EXISTS (SELECT 1 FROM channel_cost_source_bindings b
		WHERE b.domain=e.domain AND b.account_epoch=e.account_epoch AND b.source_ref=e.source_ref
		AND b.status='confirmed' AND b.allocation_mode='allocated' AND b.local_channel_id>0
		AND b.valid_from<=e.hour_ts AND (b.valid_to=0 OR b.valid_to>e.hour_ts))`

func financePairingSourceKey(domain, epoch, sourceRef string) string {
	return domain + "\x00" + epoch + "\x00" + sourceRef
}

func (m *Monitor) loadFinancePairingSources(ctx context.Context, scope stabilityScope, total int64) ([]financePairingSourceView, bool, error) {
	var rows []financePairingSourceRow
	query := `SELECT e.domain domain,e.account_epoch account_epoch,e.source_ref source_ref,
		MIN(e.hour_ts) first_hour,MAX(e.hour_ts) last_hour,
		COUNT(DISTINCT e.hour_ts) evidence_hours,COALESCE(SUM(e.requests),0) requests
		FROM channel_upstream_cost_hour_evidence e WHERE ` + financeUnallocatedSourceScopeSQL + `
		GROUP BY e.domain,e.account_epoch,e.source_ref
		ORDER BY requests DESC,e.domain,e.source_ref LIMIT ?`
	if err := m.storeDB.WithContext(ctx).Raw(query,
		channelCostEvidenceSemanticsVersion, scope.FromTs, scope.ToTs, financePairingSourceDetailLimit,
	).Scan(&rows).Error; err != nil {
		return nil, false, fmt.Errorf("读取未归属上游来源明细: %w", err)
	}
	if len(rows) == 0 {
		return []financePairingSourceView{}, false, nil
	}
	refs := make([]string, 0, len(rows))
	accumulators := make(map[string]*financePairingSourceAccumulator, len(rows))
	for _, row := range rows {
		refs = append(refs, row.SourceRef)
		key := financePairingSourceKey(row.Domain, row.AccountEpoch, row.SourceRef)
		accumulators[key] = &financePairingSourceAccumulator{
			view: financePairingSourceView{
				Domain: row.Domain, SourceRef: row.SourceRef, SourceGroups: []string{}, UpstreamModels: []string{},
				FirstHour: row.FirstHour, LastHour: row.LastHour, EvidenceHours: row.EvidenceHours, Requests: row.Requests,
			},
			groups: map[string]bool{}, models: map[string]bool{},
		}
	}
	var dimensions []financePairingSourceDimensionRow
	dimensionQuery := `SELECT e.domain domain,e.account_epoch account_epoch,e.source_ref source_ref,
		e.source_group source_group,e.upstream_model upstream_model
		FROM channel_upstream_cost_hour_evidence e WHERE ` + financeUnallocatedSourceScopeSQL + `
		AND e.source_ref IN ?
		GROUP BY e.domain,e.account_epoch,e.source_ref,e.source_group,e.upstream_model
		ORDER BY e.domain,e.source_ref,e.source_group,e.upstream_model`
	if err := m.storeDB.WithContext(ctx).Raw(dimensionQuery,
		channelCostEvidenceSemanticsVersion, scope.FromTs, scope.ToTs, refs,
	).Scan(&dimensions).Error; err != nil {
		return nil, false, fmt.Errorf("读取未归属上游来源维度: %w", err)
	}
	for _, row := range dimensions {
		accumulator := accumulators[financePairingSourceKey(row.Domain, row.AccountEpoch, row.SourceRef)]
		if accumulator == nil {
			continue
		}
		if value := strings.TrimSpace(row.SourceGroup); value != "" {
			accumulator.groups[value] = true
		}
		if value := strings.TrimSpace(row.UpstreamModel); value != "" {
			accumulator.models[value] = true
		}
	}
	var costs []financePairingSourceCostRow
	costQuery := `SELECT e.domain domain,e.account_epoch account_epoch,e.source_ref source_ref,
		e.charge_units_per_usd charge_units_per_usd,COALESCE(SUM(e.charge_units),0) charge_units
		FROM channel_upstream_cost_hour_evidence e WHERE ` + financeUnallocatedSourceScopeSQL + `
		AND e.source_ref IN ?
		GROUP BY e.domain,e.account_epoch,e.source_ref,e.charge_units_per_usd`
	if err := m.storeDB.WithContext(ctx).Raw(costQuery,
		channelCostEvidenceSemanticsVersion, scope.FromTs, scope.ToTs, refs,
	).Scan(&costs).Error; err != nil {
		return nil, false, fmt.Errorf("读取未归属上游来源金额: %w", err)
	}
	for _, row := range costs {
		accumulator := accumulators[financePairingSourceKey(row.Domain, row.AccountEpoch, row.SourceRef)]
		if accumulator == nil {
			continue
		}
		microUSD, err := unitsToMicroUSDCanonical(row.ChargeUnits, row.ChargeUnitsPerUSD)
		if err != nil {
			return nil, false, fmt.Errorf("计算未归属上游来源金额: %w", err)
		}
		if err := addEconomicsInt64(&accumulator.billedCost, microUSD); err != nil {
			return nil, false, err
		}
	}
	result := make([]financePairingSourceView, 0, len(rows))
	for _, row := range rows {
		accumulator := accumulators[financePairingSourceKey(row.Domain, row.AccountEpoch, row.SourceRef)]
		if accumulator == nil {
			continue
		}
		for value := range accumulator.groups {
			accumulator.view.SourceGroups = append(accumulator.view.SourceGroups, value)
		}
		for value := range accumulator.models {
			accumulator.view.UpstreamModels = append(accumulator.view.UpstreamModels, value)
		}
		sort.Strings(accumulator.view.SourceGroups)
		sort.Strings(accumulator.view.UpstreamModels)
		accumulator.view.BilledCost = economicsMoney(accumulator.billedCost)
		result = append(result, accumulator.view)
	}
	return result, total > int64(len(result)), nil
}

func (m *Monitor) loadFinancePairingAudit(ctx context.Context, scope stabilityScope) (financePairingAuditView, error) {
	audit := financePairingAuditView{
		StatusCounts: map[string]int64{}, Blockers: []financePairingBlockerView{}, Sources: []financePairingSourceView{},
		LedgerDomains: []string{}, UnenrolledDomains: []string{}, UnallocatedByDomain: map[string]int64{},
	}
	var rows []financePairingAuditRow
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT p.coverage_status status,
		p.domain domain,p.local_channel_id local_channel_id,
		MIN(p.hour_ts) first_hour,MAX(p.hour_ts) last_hour,COUNT(*) rows,
		COALESCE(SUM(p.revenue_micro_usd),0) revenue_micro_usd,
		COALESCE(SUM(CASE WHEN p.corrected_cost_known=1 THEN p.corrected_cost_micro_usd ELSE 0 END),0) corrected_cost_micro_usd
		FROM channel_economics_hour_current c
		JOIN channel_economics_hour_publications p ON p.publication_id=c.publication_id
		WHERE p.semantics_version=? AND p.hour_ts>=? AND p.hour_ts<?
		GROUP BY p.coverage_status,p.domain,p.local_channel_id ORDER BY rows DESC,status,p.domain,p.local_channel_id`, channelEconomicsSemanticsVersion, scope.FromTs, scope.ToTs).Scan(&rows).Error; err != nil {
		return audit, fmt.Errorf("读取收入成本配对缺口: %w", err)
	}
	blockers := make(map[string]*financePairingBlockerAccumulator)
	ledgerDomains := map[string]bool{}
	for _, row := range rows {
		status := strings.TrimSpace(row.Status)
		if domain := strings.ToLower(strings.TrimSpace(row.Domain)); domain != "" {
			ledgerDomains[domain] = true
		}
		audit.StatusCounts[status] += row.Rows
		audit.PublicationRows += row.Rows
		if status == "verified_complete" {
			audit.PairedRows += row.Rows
			continue
		}
		if status == "superseded_empty" {
			continue
		}
		audit.BlockedRows += row.Rows
		accumulator := blockers[status]
		if accumulator == nil {
			accumulator = &financePairingBlockerAccumulator{
				view: financePairingBlockerView{
					Key: status, Name: financePairingBlockerName(status), DomainNames: []string{}, ChannelIDs: []int{},
					Description: financePairingBlockerDescription(status),
				},
				domains: map[string]bool{}, channels: map[int]bool{},
			}
			blockers[status] = accumulator
		}
		accumulator.view.Rows += row.Rows
		if err := addEconomicsInt64(&accumulator.revenueMicroUSD, row.RevenueMicroUSD); err != nil {
			return audit, err
		}
		if err := addEconomicsInt64(&accumulator.correctedCostMicroUSD, row.CorrectedCostMicroUSD); err != nil {
			return audit, err
		}
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if domain == "" {
			domain = "未配置/历史"
		}
		accumulator.domains[domain] = true
		accumulator.channels[row.LocalChannelID] = true
		if accumulator.view.FirstHour == 0 || row.FirstHour < accumulator.view.FirstHour {
			accumulator.view.FirstHour = row.FirstHour
		}
		if row.LastHour > accumulator.view.LastHour {
			accumulator.view.LastHour = row.LastHour
		}
	}
	for domain := range ledgerDomains {
		audit.LedgerDomains = append(audit.LedgerDomains, domain)
	}
	sort.Strings(audit.LedgerDomains)
	for _, accumulator := range blockers {
		for domain := range accumulator.domains {
			accumulator.view.DomainNames = append(accumulator.view.DomainNames, domain)
		}
		for channelID := range accumulator.channels {
			accumulator.view.ChannelIDs = append(accumulator.view.ChannelIDs, channelID)
		}
		sort.Strings(accumulator.view.DomainNames)
		sort.Ints(accumulator.view.ChannelIDs)
		accumulator.view.Domains = int64(len(accumulator.view.DomainNames))
		accumulator.view.Revenue = economicsMoney(accumulator.revenueMicroUSD)
		accumulator.view.CorrectedCost = economicsMoney(accumulator.correctedCostMicroUSD)
		audit.Blockers = append(audit.Blockers, accumulator.view)
	}
	sort.Slice(audit.Blockers, func(i, j int) bool {
		if audit.Blockers[i].Rows != audit.Blockers[j].Rows {
			return audit.Blockers[i].Rows > audit.Blockers[j].Rows
		}
		return audit.Blockers[i].Key < audit.Blockers[j].Key
	})
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT COUNT(*) FROM (
		SELECT e.domain,e.account_epoch,e.source_ref
		FROM channel_upstream_cost_hour_evidence e
		WHERE `+financeUnallocatedSourceScopeSQL+`
		GROUP BY e.domain,e.account_epoch,e.source_ref
	)`, channelCostEvidenceSemanticsVersion, scope.FromTs, scope.ToTs).Scan(&audit.UnallocatedSources).Error; err != nil {
		return audit, fmt.Errorf("读取未归属上游来源: %w", err)
	}
	var domainSourceCounts []financePairingDomainSourceCountRow
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,COUNT(*) sources FROM (
		SELECT e.domain,e.account_epoch,e.source_ref
		FROM channel_upstream_cost_hour_evidence e
		WHERE `+financeUnallocatedSourceScopeSQL+`
		GROUP BY e.domain,e.account_epoch,e.source_ref
	) GROUP BY domain ORDER BY domain`, channelCostEvidenceSemanticsVersion, scope.FromTs, scope.ToTs).Scan(&domainSourceCounts).Error; err != nil {
		return audit, fmt.Errorf("按域名读取未归属上游来源: %w", err)
	}
	for _, row := range domainSourceCounts {
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if domain != "" && row.Sources > 0 {
			audit.UnallocatedByDomain[domain] = row.Sources
		}
	}
	var err error
	audit.Sources, audit.SourcesTruncated, err = m.loadFinancePairingSources(ctx, scope, audit.UnallocatedSources)
	if err != nil {
		return audit, err
	}
	return audit, nil
}

// applyFinancePairingSourceCandidates projects current channel configuration onto
// anonymous upstream sources as an operator hint only. A matching upstream group
// name cannot prove historical ownership, so this function deliberately cannot
// create bindings and its result is never consumed by financial calculations.
func applyFinancePairingSourceCandidates(audit *financePairingAuditView, finance channelFinanceSnapshot, channels []ChannelSnap) {
	if audit == nil {
		return
	}
	type configuredCandidate struct {
		id   int
		name string
	}
	configured := make(map[string]map[string][]configuredCandidate)
	for _, channel := range channels {
		domain := strings.ToLower(strings.TrimSpace(channel.BaseDomain))
		if channel.ID <= 0 || channel.DeletedAt != 0 || domain == "" || finance.channelCostConflict[channel.ID] {
			continue
		}
		cost, ok := finance.channelCanonicalCost[channel.ID]
		if !ok {
			continue
		}
		group := strings.ToLower(strings.TrimSpace(cost.UpstreamGroupName))
		if group == "" {
			continue
		}
		if configured[domain] == nil {
			configured[domain] = make(map[string][]configuredCandidate)
		}
		configured[domain][group] = append(configured[domain][group], configuredCandidate{id: channel.ID, name: strings.TrimSpace(channel.Name)})
	}

	for index := range audit.Sources {
		source := &audit.Sources[index]
		source.CandidateState = "no_configured_match"
		source.CurrentConfigCandidates = []financePairingCandidate{}
		byGroup := configured[strings.ToLower(strings.TrimSpace(source.Domain))]
		if len(byGroup) == 0 || len(source.SourceGroups) == 0 {
			continue
		}
		candidateGroups := make(map[int]map[string]bool)
		candidateNames := make(map[int]string)
		sourceGroups := make(map[string]bool)
		matchedSourceGroups := make(map[string]bool)
		for _, sourceGroup := range source.SourceGroups {
			normalized := strings.ToLower(strings.TrimSpace(sourceGroup))
			if normalized == "" {
				continue
			}
			sourceGroups[normalized] = true
			for _, candidate := range byGroup[normalized] {
				if candidateGroups[candidate.id] == nil {
					candidateGroups[candidate.id] = make(map[string]bool)
				}
				candidateGroups[candidate.id][sourceGroup] = true
				candidateNames[candidate.id] = candidate.name
				matchedSourceGroups[normalized] = true
			}
		}
		ids := make([]int, 0, len(candidateGroups))
		for id := range candidateGroups {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			groups := make([]string, 0, len(candidateGroups[id]))
			for group := range candidateGroups[id] {
				groups = append(groups, group)
			}
			sort.Strings(groups)
			source.CurrentConfigCandidates = append(source.CurrentConfigCandidates, financePairingCandidate{
				ChannelID: id, ChannelName: candidateNames[id], MatchedGroups: groups,
			})
		}
		switch {
		case len(ids) > 1:
			source.CandidateState = "configured_ambiguous"
		case len(ids) == 1 && len(sourceGroups) == len(matchedSourceGroups):
			source.CandidateState = "configured_unique"
		case len(ids) == 1:
			source.CandidateState = "configured_partial"
		}
	}
}

func applyFinancePairingDomainCoverage(audit *financePairingAuditView, details []financeCostDetailView) {
	if audit == nil {
		return
	}
	ledger := make(map[string]bool, len(audit.LedgerDomains))
	for _, domain := range audit.LedgerDomains {
		if domain = strings.ToLower(strings.TrimSpace(domain)); domain != "" {
			ledger[domain] = true
		}
	}
	relevant := map[string]bool{}
	for _, detail := range details {
		if detail.Status == "not_configured" {
			continue
		}
		domain := strings.ToLower(strings.TrimSpace(detail.Domain))
		if domain == "" {
			continue
		}
		relevant[domain] = true
	}
	audit.RelevantDomains = len(relevant)
	audit.LedgerDomains = audit.LedgerDomains[:0]
	audit.UnenrolledDomains = audit.UnenrolledDomains[:0]
	for domain := range relevant {
		if ledger[domain] {
			audit.LedgerDomains = append(audit.LedgerDomains, domain)
		} else {
			audit.UnenrolledDomains = append(audit.UnenrolledDomains, domain)
		}
	}
	sort.Strings(audit.LedgerDomains)
	sort.Strings(audit.UnenrolledDomains)
}

// loadFinanceClosureEvidence reads only local immutable control rows. The
// legacy aggregate bill can show an amount before the newer cost ledger has
// reconstructed the same historical requests, so those two kinds of evidence
// must remain visibly separate during gray rollout.
func (m *Monitor) loadFinanceClosureEvidence(ctx context.Context, scope stabilityScope, details []financeCostDetailView) (map[string]financeClosureCostEvidenceRow, map[string]financeClosureVersionRow, error) {
	domainSet := make(map[string]bool, len(details))
	for _, detail := range details {
		if detail.Status == "not_configured" {
			continue
		}
		if domain := strings.ToLower(strings.TrimSpace(detail.Domain)); domain != "" {
			domainSet[domain] = true
		}
	}
	domains := make([]string, 0, len(domainSet))
	for domain := range domainSet {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	if len(domains) == 0 {
		return map[string]financeClosureCostEvidenceRow{}, map[string]financeClosureVersionRow{}, nil
	}
	costRows := []financeClosureCostEvidenceRow{}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,
		MIN(hour_ts) first_hour,COUNT(*) evidence_hours,
		SUM(CASE WHEN status='verified' AND reconcile_status='matched' THEN 1 ELSE 0 END) verified_hours,
		COALESCE(SUM(requests),0) evidence_requests,
		COALESCE(SUM(CASE WHEN status='verified' AND reconcile_status='matched' THEN requests ELSE 0 END),0) verified_requests
		FROM channel_upstream_cost_hour_states
		WHERE domain IN ? AND semantics_version=? AND hour_ts>=? AND hour_ts<?
		AND (requests>0 OR evidence_rows>0 OR control_charge_units<>0 OR evidence_charge_units<>0)
		GROUP BY domain ORDER BY domain`, domains, channelCostEvidenceSemanticsVersion, scope.FromTs, scope.ToTs).Scan(&costRows).Error; err != nil {
		return nil, nil, fmt.Errorf("读取成本闭环小时证据: %w", err)
	}
	versionRows := []financeClosureVersionRow{}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,COUNT(*) versions,MIN(effective_at) first_effective
		FROM channel_finance_versions WHERE domain IN ? GROUP BY domain ORDER BY domain`, domains).Scan(&versionRows).Error; err != nil {
		return nil, nil, fmt.Errorf("读取成本闭环财务版本: %w", err)
	}
	costByDomain := make(map[string]financeClosureCostEvidenceRow, len(costRows))
	for _, row := range costRows {
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if domain != "" {
			costByDomain[domain] = row
		}
	}
	versionByDomain := make(map[string]financeClosureVersionRow, len(versionRows))
	for _, row := range versionRows {
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if domain != "" {
			versionByDomain[domain] = row
		}
	}
	return costByDomain, versionByDomain, nil
}

func applyFinanceClosureEvidence(details []financeCostDetailView, costByDomain map[string]financeClosureCostEvidenceRow, versionByDomain map[string]financeClosureVersionRow) {
	for index := range details {
		domain := strings.ToLower(strings.TrimSpace(details[index].Domain))
		cost := costByDomain[domain]
		version := versionByDomain[domain]
		details[index].CostEvidenceHours = cost.EvidenceHours
		details[index].CostVerifiedHours = cost.VerifiedHours
		details[index].CostEvidenceRequests = cost.EvidenceRequests
		details[index].CostVerifiedRequests = cost.VerifiedRequests
		details[index].CostEvidenceFrom = cost.FirstHour
		details[index].FinanceVersions = version.Versions
		details[index].FinanceVersionFrom = version.FirstEffective
		details[index].FinanceHistoryCovered = cost.FirstHour > 0 && version.Versions > 0 && version.FirstEffective <= cost.FirstHour
	}
}

// applyFinanceEvidenceBackfillEstimate produces a local, lower-bound plan. It
// never creates a sync state and never contacts an upstream. Provider formulas
// mirror their actual adapters: NewAPI works per hour, Sub2API pages a natural
// day, and AICodeWith reads every configured key for a natural day. Retries,
// rate limits and remote retention are deliberately excluded and remain
// explicit probe risks.
func (m *Monitor) applyFinanceEvidenceBackfillEstimate(ctx context.Context, scope stabilityScope, now int64, details []financeCostDetailView, accounts map[string]ChannelUpstreamAccountView) error {
	domainSet := make(map[string]bool, len(details))
	for _, detail := range details {
		if detail.ClosureReadiness != "cost_evidence_missing" && detail.ClosureReadiness != "cost_evidence_incomplete" {
			continue
		}
		if domain := strings.ToLower(strings.TrimSpace(detail.Domain)); domain != "" {
			domainSet[domain] = true
		}
	}
	if len(domainSet) == 0 {
		return nil
	}
	domains := make([]string, 0, len(domainSet))
	for domain := range domainSet {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	bucketRows := []financeEvidenceBackfillBucketRow{}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,hour_ts,bucket_seconds,requests
		FROM channel_upstream_usage_hours
		WHERE domain IN ? AND hour_ts>=? AND hour_ts<?
		AND (requests>0 OR quota<>0 OR cost_usd<>0)
		ORDER BY domain,hour_ts`, domains, scope.FromTs, scope.ToTs).Scan(&bucketRows).Error; err != nil {
		return fmt.Errorf("读取历史成本证据补采分桶: %w", err)
	}
	bucketsByDomain := make(map[string][]financeEvidenceBackfillBucketRow, len(domains))
	byDomain := make(map[string]financeEvidenceBackfillRow, len(domains))
	for _, bucket := range bucketRows {
		domain := strings.ToLower(strings.TrimSpace(bucket.Domain))
		if domain == "" {
			continue
		}
		bucketsByDomain[domain] = append(bucketsByDomain[domain], bucket)
		summary := byDomain[domain]
		summary.Domain = domain
		if summary.FirstHour == 0 || bucket.HourTs < summary.FirstHour {
			summary.FirstHour = bucket.HourTs
		}
		if bucketTo := bucket.HourTs + bucket.BucketSeconds; bucketTo > summary.LastTo {
			summary.LastTo = bucketTo
		}
		switch bucket.BucketSeconds {
		case 3600:
			summary.ActiveHours++
			summary.HourlyBuckets++
		case 86400:
			summary.DailyBuckets++
		}
		byDomain[domain] = summary
	}
	maxHistoryFrom := now - int64(180*24*3600)
	maxHistoryFrom -= maxHistoryFrom % 3600
	for index := range details {
		detail := &details[index]
		if !domainSet[strings.ToLower(strings.TrimSpace(detail.Domain))] {
			continue
		}
		domain := strings.ToLower(strings.TrimSpace(detail.Domain))
		account, configured := accounts[domain]
		row, hasActivity := byDomain[domain]
		detail.EvidenceBackfillStatus = "estimate_unavailable"
		if configured {
			detail.Provider = account.Provider
			detail.ProviderName = account.ProviderName
		}
		switch {
		case !configured || !account.Configured:
			detail.EvidenceBackfillStatus = "account_missing"
			detail.EvidenceBackfillNote = "本地没有可用的上游账户配置。"
		case !account.Enabled || !account.UsageSyncEnabled:
			detail.EvidenceBackfillStatus = "account_disabled"
			detail.EvidenceBackfillNote = "上游账户或用量同步未启用。"
		case !hasActivity:
			detail.EvidenceBackfillStatus = "no_local_activity"
			detail.EvidenceBackfillNote = "当前报表区间没有可用于估算的上游活动小时。"
		case row.FirstHour < maxHistoryFrom:
			detail.EvidenceBackfillStatus = "range_exceeds_limit"
			detail.EvidenceBackfillFrom, detail.EvidenceBackfillTo = row.FirstHour, row.LastTo
			detail.EvidenceBackfillActiveHours = row.ActiveHours
			detail.EvidenceBackfillNote = "历史起点超过当前安全上限 180 天，需先确认分段方案。"
		case account.Provider == upstreamProviderNewAPI:
			if row.DailyBuckets > 0 || row.HourlyBuckets != row.ActiveHours {
				detail.EvidenceBackfillStatus = "granularity_unsupported"
				detail.EvidenceBackfillNote = "本地旧账单不是纯小时粒度，无法安全推导 NewAPI 逐小时补采成本。"
				continue
			}
			calendarHours, estimatedCalls, estimatedRuns := estimateNewAPIEvidenceBackfill(row, bucketsByDomain[domain])
			detail.EvidenceBackfillStatus = "local_estimate_ready"
			detail.EvidenceBackfillGranularity = "hour"
			detail.EvidenceBackfillFrom, detail.EvidenceBackfillTo = row.FirstHour, row.LastTo
			detail.EvidenceBackfillCalendarHours = calendarHours
			detail.EvidenceBackfillActiveHours = row.ActiveHours
			detail.EvidenceBackfillEstimatedCalls = estimatedCalls
			detail.EvidenceBackfillEstimatedRuns = estimatedRuns
			detail.EvidenceBackfillClosesCost = true
			detail.EvidenceBackfillNote = "为本地下限估算；未包含上游历史保留限制、限流和重试，正式启用前仍需只读探测。"
			detail.ClosureNextAction += fmt.Sprintf(" 本地估算需要扫描 %d 个日历小时（%d 个活动小时），至少 %d 次上游读取、%d 个单工作器调度轮次。", calendarHours, row.ActiveHours, estimatedCalls, estimatedRuns)
		case account.Provider == upstreamProviderSub2API:
			if account.UsageAdapter != upstreamUsageAdapterSub2Trend || row.DailyBuckets > 0 || row.HourlyBuckets != row.ActiveHours {
				detail.EvidenceBackfillStatus = "granularity_unsupported"
				detail.EvidenceBackfillNote = "该 Sub2API 账户没有逐请求小时日志，不能构建历史成本证据。"
				continue
			}
			from, to, calendarHours, activeDays, calls, runs := estimateSub2EvidenceBackfill(row, bucketsByDomain[domain])
			detail.EvidenceBackfillStatus = "pricing_evidence_only"
			detail.EvidenceBackfillGranularity = "natural_day"
			detail.EvidenceBackfillFrom, detail.EvidenceBackfillTo = from, to
			detail.EvidenceBackfillCalendarHours = calendarHours
			detail.EvidenceBackfillActiveHours = row.ActiveHours
			detail.EvidenceBackfillActiveDays = activeDays
			detail.EvidenceBackfillEstimatedCalls = calls
			detail.EvidenceBackfillEstimatedRuns = runs
			detail.EvidenceBackfillNote = "Sub2API 可补自然日上游倍率/费用证据，但当前不会生成带本地渠道归属的不可变成本账本，因此不能单独解除渠道毛利阻断。"
			detail.ClosureNextAction += fmt.Sprintf(" Sub2API 倍率证据本地估算需扫描 %d 个自然日，至少 %d 次上游读取、%d 个单工作器调度轮次；这不等于渠道成本闭环。", calendarHours/24, calls, runs)
		case account.Provider == upstreamProviderAICodeWith:
			if account.APIKeyCount < 1 {
				detail.EvidenceBackfillStatus = "adapter_probe_required"
				detail.EvidenceBackfillFrom, detail.EvidenceBackfillTo = row.FirstHour, row.LastTo
				detail.EvidenceBackfillActiveHours = row.ActiveHours
				detail.EvidenceBackfillNote = "本地账户视图没有可核对的 AICodeWith Key 数，需先检查账户配置。"
				continue
			}
			from := cstDayStart(row.FirstHour)
			to := cstDayStart(row.LastTo-1) + 86400
			days := (to - from) / 86400
			activeDays := financeActiveNaturalDays(bucketsByDomain[domain], from, to)
			calls := days * int64(account.APIKeyCount) * 2
			runs := days * int64(2*((account.APIKeyCount+aiCodeWithKeysPerTurn-1)/aiCodeWithKeysPerTurn))
			detail.EvidenceBackfillStatus = "pricing_evidence_only"
			detail.EvidenceBackfillGranularity = "natural_day"
			detail.EvidenceBackfillFrom, detail.EvidenceBackfillTo = from, to
			detail.EvidenceBackfillCalendarHours = days * 24
			detail.EvidenceBackfillActiveHours = row.ActiveHours
			detail.EvidenceBackfillActiveDays = activeDays
			detail.EvidenceBackfillEstimatedCalls = calls
			detail.EvidenceBackfillEstimatedRuns = runs
			detail.EvidenceBackfillNote = fmt.Sprintf("AICodeWith 可按自然日补 %d 把 Key 的上游倍率/费用证据，但当前不会生成带本地渠道归属的不可变成本账本，因此不能单独解除渠道毛利阻断；单 Key 单日 1000 条上限和上游历史保留期仍需灰度探测。", account.APIKeyCount)
			detail.ClosureNextAction += fmt.Sprintf(" AICodeWith 倍率证据本地估算需要扫描 %d 个自然日×%d 把 Key，至少 %d 次上游读取、%d 个单工作器调度轮次；这不等于渠道成本闭环。", days, account.APIKeyCount, calls, runs)
		default:
			detail.EvidenceBackfillStatus = "adapter_probe_required"
			detail.EvidenceBackfillFrom, detail.EvidenceBackfillTo = row.FirstHour, row.LastTo
			detail.EvidenceBackfillActiveHours = row.ActiveHours
			detail.EvidenceBackfillNote = fmt.Sprintf("%s 尚无可验证的成本证据补采公式。", account.ProviderName)
		}
	}
	return nil
}

// estimateSub2EvidenceBackfill mirrors fetchSub2PricingDay. One scan reads all
// pages; multi-page days add a first-page consistency probe. Every 20-request
// budget rollover re-reads page one before resuming the durable checkpoint.
// Publishing deletes that checkpoint, so verification performs the same full
// scan a second time.
func estimateSub2EvidenceBackfill(summary financeEvidenceBackfillRow, buckets []financeEvidenceBackfillBucketRow) (from, to, calendarHours, activeDays, calls, runs int64) {
	from = cstDayStart(summary.FirstHour)
	to = cstDayStart(summary.LastTo-1) + 86400
	if to <= from {
		return from, to, 0, 0, 0, 0
	}
	requestsByDay := make(map[int64]int64)
	activeDaySet := make(map[int64]bool)
	for _, bucket := range buckets {
		if bucket.BucketSeconds != 3600 || bucket.HourTs < from || bucket.HourTs >= to {
			continue
		}
		day := cstDayStart(bucket.HourTs)
		requestsByDay[day] += bucket.Requests
		activeDaySet[day] = true
	}
	for day := from; day < to; day += 86400 {
		scanCalls := estimatePagedPricingScanCalls(requestsByDay[day])
		calls += 2 * scanCalls
		runs += 2 * ((scanCalls + int64(upstreamPricingMaxRequestsPerRun) - 1) / int64(upstreamPricingMaxRequestsPerRun))
	}
	calendarHours = (to - from) / 3600
	activeDays = int64(len(activeDaySet))
	return from, to, calendarHours, activeDays, calls, runs
}

func estimateNewAPIEvidenceBackfill(summary financeEvidenceBackfillRow, buckets []financeEvidenceBackfillBucketRow) (calendarHours, calls, runs int64) {
	calendarHours = (summary.LastTo - summary.FirstHour) / 3600
	if calendarHours < summary.ActiveHours {
		calendarHours = summary.ActiveHours
	}
	active := make(map[int64]bool)
	for _, bucket := range buckets {
		if bucket.BucketSeconds != 3600 || bucket.HourTs < summary.FirstHour || bucket.HourTs >= summary.LastTo {
			continue
		}
		active[bucket.HourTs] = true
		calls += 2 * estimatePagedPricingScanCalls(bucket.Requests)
	}
	emptyHours := calendarHours - int64(len(active))
	if emptyHours > 0 {
		calls += emptyHours * 2
	}
	// This is a lower bound: the global 20-request budget and the requirement
	// to observe an hour before verifying it are independent constraints.
	runs = (calls + int64(upstreamPricingMaxRequestsPerRun) - 1) / int64(upstreamPricingMaxRequestsPerRun)
	if dependencyRuns := calendarHours + 1; dependencyRuns > runs {
		runs = dependencyRuns
	}
	return calendarHours, calls, runs
}

func estimatePagedPricingScanCalls(requests int64) int64 {
	pages := (requests + int64(upstreamUsagePageSize) - 1) / int64(upstreamUsagePageSize)
	if pages <= 1 {
		return 1
	}
	if pages <= int64(upstreamPricingMaxRequestsPerRun) {
		return pages + 1
	}
	// The first turn consumes 20 real pages. Every resume spends one request
	// revalidating page one and can then read 19 new pages; the last page-one
	// read also acts as the scan's final consistency probe.
	remaining := pages - int64(upstreamPricingMaxRequestsPerRun)
	resumes := (remaining + int64(upstreamPricingMaxRequestsPerRun) - 2) / int64(upstreamPricingMaxRequestsPerRun-1)
	return pages + resumes
}

func financeActiveNaturalDays(buckets []financeEvidenceBackfillBucketRow, from, to int64) int64 {
	days := make(map[int64]bool)
	for _, bucket := range buckets {
		if bucket.HourTs >= from && bucket.HourTs < to {
			days[cstDayStart(bucket.HourTs)] = true
		}
	}
	return int64(len(days))
}

// applyFinanceCostPairingStatuses keeps upstream-bill readiness separate from
// revenue/cost pairing readiness. A complete upstream bill does not mean the
// same domain has entered the immutable economics ledger, and a partial pairing
// must never be presented as a fully verified domain contribution.
func applyFinanceCostPairingStatuses(details []financeCostDetailView, audit financePairingAuditView) {
	ledger := make(map[string]bool, len(audit.LedgerDomains))
	for _, domain := range audit.LedgerDomains {
		if domain = strings.ToLower(strings.TrimSpace(domain)); domain != "" {
			ledger[domain] = true
		}
	}
	blockersByDomain := make(map[string][]string)
	for _, blocker := range audit.Blockers {
		for _, value := range blocker.DomainNames {
			domain := strings.ToLower(strings.TrimSpace(value))
			if domain == "" || domain == "未配置/历史" {
				continue
			}
			blockersByDomain[domain] = append(blockersByDomain[domain], blocker.Key)
		}
	}
	for index := range details {
		domain := strings.ToLower(strings.TrimSpace(details[index].Domain))
		details[index].ClosureBlockers = append([]string(nil), blockersByDomain[domain]...)
		sort.Strings(details[index].ClosureBlockers)
		details[index].UnallocatedSources = audit.UnallocatedByDomain[domain]
		switch {
		case details[index].Status == "not_configured":
			details[index].PairingStatus = "not_required"
		case !ledger[domain]:
			details[index].PairingStatus = "not_enrolled"
		case details[index].Contribution != nil:
			details[index].PairingStatus = "paired_verified"
		case details[index].KnownContribution.MicroUSD != "":
			details[index].PairingStatus = "partially_paired"
		default:
			details[index].PairingStatus = "binding_required"
		}
		applyFinanceClosureReadiness(&details[index])
	}
}

// applyFinanceClosureReadiness explains the next safe action without treating
// an aggregate bill as proof that the domain can already publish contribution
// margin. Source binding, finance-version history and same-hour local revenue
// still have to pass through the immutable economics ledger.
func applyFinanceClosureReadiness(detail *financeCostDetailView) {
	if detail == nil {
		return
	}
	switch {
	case detail.PairingStatus == "paired_verified":
		detail.ClosureReadiness = "verified"
		detail.ClosureNextAction = "当前区间收入、修正成本与渠道归属已经同窗核验。"
	case detail.Status == "not_configured":
		detail.ClosureReadiness = "not_required"
		detail.ClosureNextAction = "未配置上游账户，暂不进入账单覆盖率和正式毛利；配置后再补采并纳入核算。"
	case detail.Status == "not_connected":
		detail.ClosureReadiness = "bill_not_connected"
		detail.ClosureNextAction = "先配置并验证上游账单同步，不能按零成本处理。"
	case detail.KnownBilledCost.MicroUSD == "":
		detail.ClosureReadiness = "bill_missing"
		detail.ClosureNextAction = "先补齐上游账单证据，再评估成本闭环。"
	case detail.KnownCorrectedCost.MicroUSD == "":
		detail.ClosureReadiness = "correction_missing"
		detail.ClosureNextAction = "先补充值到账/支付比例或其它可审计修正依据。"
	case detail.PairingStatus == "partially_paired" || detail.PairingStatus == "binding_required":
		detail.ClosureReadiness = "in_progress"
		if detail.UnallocatedSources > 0 {
			detail.ClosureNextAction = fmt.Sprintf("已进入成本闭环，但当前区间仍有 %d 个成本来源未归属；需继续核对来源绑定和历史证据。", detail.UnallocatedSources)
		} else {
			detail.ClosureNextAction = "已进入成本闭环；继续补历史财务版本或缺失的同小时收入成本。"
		}
	case detail.CostEvidenceHours == 0:
		detail.ClosureReadiness = "cost_evidence_missing"
		detail.ClosureNextAction = "已有汇总账单，但当前区间尚无可配对的上游成本小时证据；需先完成历史证据采集。"
	case detail.CostVerifiedHours < detail.CostEvidenceHours || detail.CostVerifiedRequests < detail.CostEvidenceRequests || (detail.UpstreamRequests > 0 && detail.CostVerifiedRequests < detail.UpstreamRequests):
		targetRequests := max(detail.UpstreamRequests, detail.CostEvidenceRequests)
		detail.ClosureReadiness = "cost_evidence_incomplete"
		detail.ClosureNextAction = fmt.Sprintf("成本证据已验证 %d/%d 个活动小时、%d/%d 个请求；需先补齐历史范围。", detail.CostVerifiedHours, detail.CostEvidenceHours, detail.CostVerifiedRequests, targetRequests)
	case !detail.FinanceHistoryCovered:
		detail.ClosureReadiness = "finance_history_missing"
		if detail.FinanceVersions == 0 {
			detail.ClosureNextAction = "没有可追溯的充值比例/折扣财务版本；不能用当前配置覆盖历史。"
		} else {
			detail.ClosureNextAction = "最早财务版本晚于成本证据起点；需补充或人工确认早期口径。"
		}
	case detail.UnallocatedSources > 0:
		detail.ClosureReadiness = "source_binding_required"
		detail.ClosureNextAction = fmt.Sprintf("已发现 %d 个未归属成本来源；先核对并建立有效时段的渠道绑定，不会自动归属。", detail.UnallocatedSources)
	default:
		detail.ClosureReadiness = "ledger_backfill_required"
		detail.ClosureNextAction = "成本证据、历史财务版本和来源归属已通过前置核对；下一步只读试算不可变配对账本。"
	}
}

func validateFinanceSettings(s Settings) error {
	if s.FinanceFastSnapshotEnabled && (!s.FinanceEnabled || !s.FinanceReportSnapshotReadEnabled || !s.FinanceReportSnapshotShadowEnabled) {
		return errors.New("经营核算快速快照需要同时开启经营核算、快照影子写入和快照读取")
	}
	if s.FinanceFactsReadIsolationEnabled && !s.FinanceEnabled {
		return errors.New("经营核算事实只读隔离需要 MONITOR_FINANCE_ENABLED=true")
	}
	if !s.FinanceEnabled && !s.FinanceFactsSyncEnabled && !s.FinanceCURArtifactEnabled {
		return nil
	}
	if s.FinanceReportSnapshotReadEnabled && !s.FinanceReportSnapshotShadowEnabled {
		return errors.New("经营核算持久缓存读取需要同时开启影子写入")
	}
	if s.FinanceCURArtifactEnabled {
		path := strings.TrimSpace(s.FinanceCURArtifactPath)
		if path == "" || !filepath.IsAbs(path) {
			return errors.New("MONITOR_FINANCE_CUR_ARTIFACT_PATH 必须是非空绝对路径")
		}
		if !s.FinanceEnabled {
			return errors.New("CUR 核算产物展示需要 MONITOR_FINANCE_ENABLED=true")
		}
	}
	if s.FinanceFactsSyncEnabled {
		if s.LocalSnapshotOnly {
			return errors.New("经营核算事实同步不能在本地快照只读模式开启")
		}
		if !s.UsageFactsEnabled {
			return errors.New("经营核算事实同步需要 MONITOR_USAGE_FACTS_ENABLED=true 以复用受控的来源查询闸门")
		}
		if strings.ToLower(strings.TrimSpace(s.UsageFactsHistorySourceMode)) != "complete" {
			return errors.New("经营核算历史事实必须显式确认 MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE=complete")
		}
		epoch := strings.TrimSpace(s.UsageFactsHistorySourceEpoch)
		if epoch == "" || len(epoch) > 64 {
			return errors.New("经营核算事实同步必须配置 1～64 字节的 MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH")
		}
	}
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return err
	}
	start, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(s.FinanceStartDate), loc)
	if err != nil {
		return errors.New("MONITOR_FINANCE_START_DATE 必须是 YYYY-MM-DD")
	}
	if start.Before(time.Date(2020, 1, 1, 0, 0, 0, 0, loc)) || start.After(time.Now().In(loc).AddDate(0, 0, 1)) {
		return errors.New("MONITOR_FINANCE_START_DATE 超出可接受范围")
	}
	return nil
}

type financeDomainUserFact struct {
	Domain       string
	Requests     int64
	ConsumeQuota int64
	RefundQuota  int64
}

type financeDailyUserFact struct {
	DayTs        int64
	ConsumeQuota int64
	RefundQuota  int64
	TestRequests int64
	TestQuota    int64
}

func financeMoneyFromQuota(quota int64) (channelEconomicsMoneyView, error) {
	micro, err := signedUnitsToMicroUSDCanonical(quota, strconv.FormatInt(int64(quotaPerUSD), 10))
	if err != nil {
		return channelEconomicsMoneyView{}, err
	}
	return economicsMoney(micro), nil
}

func financeMoneyFromUSD(value float64) (channelEconomicsMoneyView, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value >= float64(math.MaxInt64)/1_000_000 {
		return channelEconomicsMoneyView{}, errors.New("上游金额超出可接受范围")
	}
	return economicsMoney(int64(math.Round(value * 1_000_000))), nil
}

func financeMoneyInt64(value channelEconomicsMoneyView) (int64, bool) {
	if strings.TrimSpace(value.MicroUSD) == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value.MicroUSD, 10, 64)
	return parsed, err == nil
}

func financeMoneyPointer(value channelEconomicsMoneyView) *channelEconomicsMoneyView {
	copy := value
	return &copy
}

func financeSubtract(left, right channelEconomicsMoneyView) (channelEconomicsMoneyView, error) {
	l, lok := financeMoneyInt64(left)
	r, rok := financeMoneyInt64(right)
	if !lok || !rok {
		return channelEconomicsMoneyView{}, errors.New("缺少可相减金额")
	}
	if (r > 0 && l < math.MinInt64+r) || (r < 0 && l > math.MaxInt64+r) {
		return channelEconomicsMoneyView{}, errors.New("经营差额超出 int64 安全范围")
	}
	return economicsMoney(l - r), nil
}

func applyFinanceGiftAllocation(statement *financeStatementView, result financeGiftAllocationResult) error {
	if statement == nil || !result.Coverage.Complete {
		return nil
	}
	gift := economicsMoney(result.Allocation.PeriodGiftConsumptionMicroUSD)
	statement.KnownRegistrationGiftConsumption = gift
	statement.RegistrationGiftConsumption = financeMoneyPointer(gift)
	if statement.UserConsumption == nil {
		return nil
	}
	revenue, err := financeSubtract(*statement.UserConsumption, gift)
	if err != nil {
		return err
	}
	statement.KnownOperatingRevenue = revenue
	statement.OperatingRevenue = financeMoneyPointer(revenue)
	return nil
}

// applyFinanceKnownGiftRevenue publishes only the verified prefix as a known
// partial value. It deliberately leaves OperatingRevenue nil: callers must
// never turn a missing tail into an exact full-range amount.
func applyFinanceKnownGiftRevenue(statement *financeStatementView, verifiedUserConsumption channelEconomicsMoneyView, result financeGiftAllocationResult) error {
	if statement == nil || !result.Coverage.Complete {
		return nil
	}
	revenue, err := financeSubtract(verifiedUserConsumption, economicsMoney(result.Allocation.PeriodGiftConsumptionMicroUSD))
	if err != nil {
		return err
	}
	statement.KnownOperatingRevenue = revenue
	return nil
}

func (m *Monitor) applyFinanceVerifiedPrefixRevenue(ctx context.Context, statement *financeStatementView, result financeGiftAllocationResult, verifiedTo, requestedTo int64) error {
	if statement == nil || statement.OperatingRevenue != nil || !result.Coverage.Complete ||
		statement.KnownUserConsumption.MicroUSD == "" || verifiedTo >= requestedTo {
		return nil
	}
	var laterFactRows int64
	if err := m.storeDB.WithContext(ctx).Model(&StabilityHourSample{}).
		Where("hour_ts >= ? AND hour_ts < ? AND traffic_class_version = ?", verifiedTo, requestedTo, stabilityTrafficClassificationVersion).
		Count(&laterFactRows).Error; err != nil {
		return err
	}
	if laterFactRows != 0 {
		return nil
	}
	return applyFinanceKnownGiftRevenue(statement, statement.KnownUserConsumption, result)
}

// applyFinanceGiftBreakdowns projects one already verified allocation ledger
// onto month and day rows. Keeping this separate from source collection makes
// it impossible for individual table rows to observe different source states.
func applyFinanceGiftBreakdowns(report *financeOperatingReport, result financeGiftAllocationResult) error {
	if report == nil {
		return nil
	}
	strictPrefix := !result.Coverage.Complete
	if strictPrefix {
		if result.verifiedPrefix == nil || !result.verifiedPrefix.Coverage.Complete {
			return nil
		}
		result = *result.verifiedPrefix
	}
	type target struct {
		label     string
		from      int64
		to        int64
		statement *financeStatementView
	}
	targets := make([]target, 0, len(report.Periods)+len(report.Days))
	boundaries := make(map[int64]struct{}, len(report.Periods)*2+len(report.Days)*2)
	for index := range report.Periods {
		if strictPrefix && report.Periods[index].To > result.Coverage.ToTs {
			continue
		}
		from := report.Periods[index].From
		to := min(report.Periods[index].To, result.Coverage.ToTs)
		if from < result.Coverage.FromTs || to <= from {
			continue
		}
		targets = append(targets, target{label: report.Periods[index].Period, from: from, to: to, statement: &report.Periods[index].Statement})
		boundaries[from], boundaries[to] = struct{}{}, struct{}{}
	}
	for index := range report.Days {
		if strictPrefix && report.Days[index].To > result.Coverage.ToTs {
			continue
		}
		from := report.Days[index].From
		to := min(report.Days[index].To, result.Coverage.ToTs)
		if from < result.Coverage.FromTs || to <= from {
			continue
		}
		targets = append(targets, target{label: report.Days[index].Date, from: from, to: to, statement: &report.Days[index].Statement})
		boundaries[from], boundaries[to] = struct{}{}, struct{}{}
	}
	if len(targets) == 0 {
		return nil
	}
	orderedBoundaries := make([]int64, 0, len(boundaries))
	for boundary := range boundaries {
		orderedBoundaries = append(orderedBoundaries, boundary)
	}
	sort.Slice(orderedBoundaries, func(i, j int) bool { return orderedBoundaries[i] < orderedBoundaries[j] })
	ranges := make([]financecredit.AllocationRange, 0, len(orderedBoundaries)-1)
	for index := 1; index < len(orderedBoundaries); index++ {
		ranges = append(ranges, financecredit.AllocationRange{From: orderedBoundaries[index-1], To: orderedBoundaries[index]})
	}
	allocations, err := financecredit.AllocateTrialGiftConsumptionSeries(result.ledger, ranges)
	if err != nil {
		return fmt.Errorf("批量计算注册赠送扣减: %w", err)
	}
	for _, item := range targets {
		var giftMicroUSD int64
		for index, period := range ranges {
			if period.From < item.from || period.To > item.to {
				continue
			}
			if err := addEconomicsInt64(&giftMicroUSD, allocations[index].PeriodGiftConsumptionMicroUSD); err != nil {
				return fmt.Errorf("汇总 %s 注册赠送扣减: %w", item.label, err)
			}
		}
		allocation := financeGiftAllocationResult{
			Allocation: financecredit.GiftAllocation{From: item.from, To: item.to, PeriodGiftConsumptionMicroUSD: giftMicroUSD},
			Coverage:   financeGiftCoverageView{FromTs: item.from, ToTs: item.to, Complete: true},
		}
		if err := applyFinanceGiftAllocation(item.statement, allocation); err != nil {
			return fmt.Errorf("应用 %s 注册赠送扣减: %w", item.label, err)
		}
	}
	return nil
}

func financePeriodStatus(user StabilityDataCoverage, upstream financeUpstreamCoverageView, hasUser bool) string {
	if !hasUser && upstream.AvailableDomains == 0 {
		return "no_data"
	}
	if user.Complete && upstream.Complete {
		return "verified"
	}
	return "incomplete"
}

func financeDailyStatus(user financeDailyCoverageView, economics channelEconomicsDayView, hasUser bool) string {
	if !hasUser && economics.Totals.PairedPublicationRows == 0 {
		return "no_data"
	}
	if user.Complete && economics.Coverage.Complete && economics.Totals.PairedPublicationRows > 0 {
		return "paired_verified"
	}
	return "incomplete"
}

// loadFinanceDailyUserFacts performs three bounded scans for the whole report
// range. It deliberately does not call stabilityDataCoverage once per day:
// doing so would multiply the expensive zero-hour contradiction query by the
// number of displayed days.
func (m *Monitor) loadFinanceDailyUserFacts(ctx context.Context, scope stabilityScope, now int64, internalAccounts financeConfiguredInternalEvidence, businessGroups map[string]bool) (map[int64]financeDailyUserFact, map[int64]financeDailyCoverageView, error) {
	const cstDaySQL = "((hour_ts + 28800) / 86400) * 86400 - 28800"
	var usageRows []struct {
		DayTs        int64
		Grp          string
		ConsumeQuota int64
		RefundQuota  int64
	}
	usageSQL := `SELECT ` + cstDaySQL + ` day_ts,grp,
		COALESCE(SUM(quota),0) consume_quota, COALESCE(SUM(refund_quota),0) refund_quota
		FROM stability_hour_samples
		WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version=? GROUP BY day_ts,grp ORDER BY day_ts`
	if err := m.storeDB.WithContext(ctx).Raw(usageSQL, scope.FromTs, scope.ToTs, stabilityTrafficClassificationVersion).Scan(&usageRows).Error; err != nil {
		return nil, nil, fmt.Errorf("读取每日用户用量事实: %w", err)
	}
	var testRows []struct {
		DayTs    int64
		Requests int64
		Quota    int64
	}
	testSQL := `SELECT ` + cstDaySQL + ` day_ts,
		COALESCE(SUM(requests),0) requests, COALESCE(SUM(quota),0) quota
		FROM channel_test_hour_samples WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version=? GROUP BY day_ts ORDER BY day_ts`
	if err := m.storeDB.WithContext(ctx).Raw(testSQL, scope.FromTs, scope.ToTs, stabilityTrafficClassificationVersion).Scan(&testRows).Error; err != nil {
		return nil, nil, fmt.Errorf("读取每日内部测试用量事实: %w", err)
	}

	facts := make(map[int64]financeDailyUserFact, len(usageRows)+len(testRows))
	for _, row := range usageRows {
		if !channelBusinessGroupIncluded(businessGroups, row.Grp) {
			continue
		}
		fact := facts[row.DayTs]
		fact.DayTs = row.DayTs
		if err := addEconomicsInt64(&fact.ConsumeQuota, row.ConsumeQuota); err != nil {
			return nil, nil, err
		}
		if err := addEconomicsInt64(&fact.RefundQuota, row.RefundQuota); err != nil {
			return nil, nil, err
		}
		facts[row.DayTs] = fact
	}
	for _, row := range testRows {
		fact := facts[row.DayTs]
		fact.DayTs, fact.TestRequests, fact.TestQuota = row.DayTs, row.Requests, row.Quota
		facts[row.DayTs] = fact
	}
	for _, row := range internalAccounts.Rows {
		day := cstDayStart(row.HourTs)
		fact := facts[day]
		fact.DayTs = day
		if err := addEconomicsInt64(&fact.ConsumeQuota, -row.ConsumeQuota); err != nil {
			return nil, nil, err
		}
		if err := addEconomicsInt64(&fact.RefundQuota, -row.RefundQuota); err != nil {
			return nil, nil, err
		}
		if err := addEconomicsInt64(&fact.TestQuota, row.ConsumeQuota-row.RefundQuota); err != nil {
			return nil, nil, err
		}
		if err := addEconomicsInt64(&fact.TestRequests, row.Requests); err != nil {
			return nil, nil, err
		}
		if fact.ConsumeQuota < 0 || fact.RefundQuota < 0 {
			return nil, nil, errors.New("每日内部账号用量超过同口径用量")
		}
		facts[day] = fact
	}

	finalizedTo := min(scope.ToTs, finalizedStabilityHourTo(now))
	completedByDay := map[int64]int64{}
	if finalizedTo > scope.FromTs {
		var completeHours []int64
		strictSQL := `SELECT hs.hour_ts FROM stability_hour_ingest_states hs WHERE hs.hour_ts>=? AND hs.hour_ts<? AND ` +
			stabilityCompleteHourPredicateSQL("hs")
		if err := m.storeDB.WithContext(ctx).Raw(strictSQL, scope.FromTs, finalizedTo,
			stabilityTrafficClassificationVersion, stabilityTrafficClassificationVersion).Scan(&completeHours).Error; err != nil {
			return nil, nil, fmt.Errorf("读取每日用量覆盖台账: %w", err)
		}
		for _, hour := range completeHours {
			completedByDay[cstDayStart(hour)]++
		}
	}

	coverages := map[int64]financeDailyCoverageView{}
	for day := cstDayStart(scope.FromTs); day < scope.ToTs; day += 86400 {
		left, right := max(scope.FromTs, day), min(scope.ToTs, day+86400)
		closedRight := min(right, finalizedTo)
		coverage := financeDailyCoverageView{FromTs: left, ToTs: max(left, closedRight), RequestedToTs: right}
		if closedRight > left {
			coverage.ExpectedHours = (closedRight - left) / 3600
		}
		coverage.CompletedHours = completedByDay[day]
		coverage.MissingHours = max(int64(0), coverage.ExpectedHours-coverage.CompletedHours)
		coverage.Complete = coverage.MissingHours == 0
		if coverage.ExpectedHours > 0 {
			coverage.Percent = float64(coverage.CompletedHours) * 100 / float64(coverage.ExpectedHours)
		} else {
			coverage.Percent = 100
		}
		if right > closedRight {
			coverage.ProvisionalSeconds = right - max(left, closedRight)
			coverage.LatestHourPending = true
			coverage.PendingHourTs = max(left, closedRight)
		}
		if !financeEvidenceScopeComplete(internalAccounts.Complete, internalAccounts.VerifiedScope, stabilityScope{FromTs: left, ToTs: right}) {
			coverage.Complete = false
		}
		coverages[day] = coverage
	}
	return facts, coverages, nil
}

func (m *Monitor) buildFinanceDailyViews(ctx context.Context, scope stabilityScope, now int64, internalTestCost financeInternalTestCostEvidence, internalAccounts financeConfiguredInternalEvidence, businessGroups map[string]bool) ([]financeDailyView, error) {
	facts, coverages, err := m.loadFinanceDailyUserFacts(ctx, scope, now, internalAccounts, businessGroups)
	if err != nil {
		return nil, err
	}
	ledger, err := m.buildChannelEconomicsReportMode(ctx, scope, "", true)
	if err != nil {
		return nil, fmt.Errorf("读取每日已发布经济事实: %w", err)
	}
	economicsByDay := make(map[int64]channelEconomicsDayView, len(ledger.Daily))
	for _, day := range ledger.Daily {
		economicsByDay[day.DayTs] = day
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	views := make([]financeDailyView, 0, (scope.ToTs-scope.FromTs)/86400+1)
	for day := cstDayStart(scope.FromTs); day < scope.ToTs; day += 86400 {
		left, right := max(scope.FromTs, day), min(scope.ToTs, day+86400)
		fact, userCoverage, economics := facts[day], coverages[day], economicsByDay[day]
		internalComplete := financeEvidenceScopeComplete(internalAccounts.Complete, internalAccounts.VerifiedScope, stabilityScope{FromTs: left, ToTs: right})
		gross, err := financeMoneyFromQuota(fact.ConsumeQuota)
		if err != nil {
			return nil, err
		}
		refunds, err := financeMoneyFromQuota(fact.RefundQuota)
		if err != nil {
			return nil, err
		}
		net, err := financeMoneyFromQuota(fact.ConsumeQuota - fact.RefundQuota)
		if err != nil {
			return nil, err
		}
		internal, err := financeMoneyFromQuota(fact.TestQuota)
		if err != nil {
			return nil, err
		}
		statement := financeStatementView{
			GrossUserConsumption: gross, UserRefunds: refunds, KnownUserConsumption: net,
			InternalTestConsumption: internal, InternalTestRequests: fact.TestRequests,
		}
		if internalAccounts.Accounts > 0 && !internalComplete {
			statement.KnownUserConsumption = channelEconomicsMoneyView{}
		}
		if userCoverage.Complete && internalComplete {
			statement.UserConsumption = financeMoneyPointer(net)
		}
		if economics.Totals.IncludedPublicationRows > 0 {
			statement.KnownCorrectedUpstreamCost = economics.Totals.KnownCorrectedCost
			if economics.Totals.CorrectedCostKnown {
				statement.CorrectedUpstreamCost = financeMoneyPointer(economics.Totals.KnownCorrectedCost)
			}
		}
		if economics.Totals.PairedPublicationRows > 0 {
			statement.PairedUserConsumption = economics.Totals.PairedRevenue
			statement.PairedCorrectedCost = economics.Totals.PairedCorrectedCost
			statement.KnownContributionProfit = economics.Totals.KnownProfit
			if revenue, ok := financeMoneyInt64(economics.Totals.PairedRevenue); ok && revenue > 0 {
				profit, _ := financeMoneyInt64(economics.Totals.KnownProfit)
				margin := strconv.FormatFloat(float64(profit)*100/float64(revenue), 'f', 2, 64)
				statement.PairedContributionMargin = &margin
			}
		}
		dayCostEvidence, err := financeInternalTestCostSubrange(internalTestCost, stabilityScope{FromTs: left, ToTs: right})
		if err != nil {
			return nil, err
		}
		dayInternalCost := dayCostEvidence.Total
		internalCostComplete := internalComplete && dayCostEvidence.Complete
		applyFinanceInternalTestCost(&statement, dayInternalCost, dayCostEvidence.MixedPairs, dayCostEvidence.UnverifiedPairs, internalCostComplete)
		statement.KnownRawCorrectedUpstreamCost = statement.KnownCorrectedUpstreamCost
		statement.RawCorrectedUpstreamCost = statement.CorrectedUpstreamCost
		statement.CorrectedUpstreamCost = nil
		daySources := map[string]financeDeductionSource{}
		for domain, cost := range ledger.dailyCorrectedCosts[day] {
			daySources[domain] = financeDeductionSource{Cost: economicsMoney(cost), Included: true}
		}
		statement.KnownCorrectedUpstreamCost, statement.InternalCostDeductionStatus = financeInternalCostDeduction(statement.KnownRawCorrectedUpstreamCost, dayCostEvidence, daySources)
		if statement.InternalCostDeductionStatus != "" {
			statement.RawCorrectedUpstreamCost = nil
		}
		if internalComplete && dayCostEvidence.Complete && statement.RawCorrectedUpstreamCost != nil && statement.InternalCostDeductionStatus == "" {
			statement.CorrectedUpstreamCost = financeMoneyPointer(statement.KnownCorrectedUpstreamCost)
		}
		if err := subtractFinanceInternalTestFromContribution(&statement, internalTestCost.ExcludeByDay[day], economics.Totals.PairedPublicationRows); err != nil {
			return nil, fmt.Errorf("从 %s 客户贡献中分离内部测试成本: %w", time.Unix(day, 0).In(loc).Format("2006-01-02"), err)
		}
		// Compare customer-only revenue on both sides. The immutable ledger's
		// raw revenue still includes strictly identified internal-test traffic;
		// its paired revenue above has already had that traffic and cost removed.
		// Equality still fails closed for non-business groups or missing domains.
		ledgerRevenue, ledgerRevenueOK := financeMoneyInt64(statement.PairedUserConsumption)
		netRevenue, _ := financeMoneyInt64(net)
		if userCoverage.Complete && dayCostEvidence.Complete && economics.Totals.ProfitKnown && economics.Totals.RevenueKnown && ledgerRevenueOK && ledgerRevenue == netRevenue && statement.KnownContributionProfit.MicroUSD != "" {
			statement.ContributionProfit = financeMoneyPointer(statement.KnownContributionProfit)
			statement.ContributionMargin = statement.PairedContributionMargin
		}
		views = append(views, financeDailyView{
			Date: time.Unix(day, 0).In(loc).Format("2006-01-02"), From: left, To: right,
			Statement: statement, UserCoverage: userCoverage, EconomicsCoverage: economics.Coverage,
			InternalCostComplete:          internalCostComplete,
			InternalCostUnverifiedReasons: dayCostEvidence.UnverifiedReasons,
			ledgerSourceCosts:             ledger.dailyPublishedCosts[day],
			Status:                        financeDailyStatus(userCoverage, economics, fact.ConsumeQuota != 0 || fact.RefundQuota != 0),
		})
	}
	return views, nil
}

func (m *Monitor) loadFinanceUserFacts(ctx context.Context, scope stabilityScope, now int64, internalAccounts financeConfiguredInternalEvidence, businessGroups map[string]bool) (financeStatementView, StabilityDataCoverage, map[string]financeDomainUserFact, error) {
	// The stability fact table is large. Aggregate it once by channel and resolve
	// the domain from the small snapshot table in memory. A LEFT JOIN followed by
	// a domain GROUP BY made the full-history finance view repeatedly scan and
	// sort the large table, which is both slower and more likely to hit SQLite's
	// read deadline while background collection is active.
	var rows []struct {
		ChannelID    int
		Grp          string
		Requests     int64
		ConsumeQuota int64
		RefundQuota  int64
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT channel_id,grp,
		COALESCE(SUM(success+anomaly+failed),0) requests,
		COALESCE(SUM(quota),0) consume_quota,
		COALESCE(SUM(refund_quota),0) refund_quota
		FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version=?
		GROUP BY channel_id,grp`, scope.FromTs, scope.ToTs, stabilityTrafficClassificationVersion).Scan(&rows).Error; err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, fmt.Errorf("读取全站用户用量事实: %w", err)
	}
	var snaps []ChannelSnap
	if err := m.storeDB.WithContext(ctx).Select("id", "base_domain").Find(&snaps).Error; err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, fmt.Errorf("读取渠道域名归属: %w", err)
	}
	domainByChannel := make(map[int]string, len(snaps))
	for _, snap := range snaps {
		domain := strings.ToLower(strings.TrimSpace(snap.BaseDomain))
		if domain == "" {
			domain = "未配置/历史"
		}
		domainByChannel[snap.ID] = domain
	}
	domains := make(map[string]financeDomainUserFact, len(rows))
	var totalConsumeQuota, totalRefundQuota int64
	for _, row := range rows {
		if !channelBusinessGroupIncluded(businessGroups, row.Grp) {
			continue
		}
		domain := domainByChannel[row.ChannelID]
		if domain == "" {
			domain = "未配置/历史"
		}
		fact := domains[domain]
		fact.Domain = domain
		fact.Requests += row.Requests
		if err := addEconomicsInt64(&fact.ConsumeQuota, row.ConsumeQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
		if err := addEconomicsInt64(&fact.RefundQuota, row.RefundQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
		domains[domain] = fact
		if err := addEconomicsInt64(&totalConsumeQuota, row.ConsumeQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
		if err := addEconomicsInt64(&totalRefundQuota, row.RefundQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
	}
	// 配置的内部账号已包含在 stability_hour_samples 的用户流量中，
	// 因此在这里按同一个渠道和业务分组口径精确扣除。内部账号事实
	// 未补齐时加载器不会返回部分行，后面也会把 UserConsumption 保持为未发布。
	for _, row := range internalAccounts.Rows {
		domain := domainByChannel[row.ChannelID]
		if domain == "" {
			domain = "未配置/历史"
		}
		fact := domains[domain]
		fact.Domain = domain
		fact.Requests -= row.Requests
		if err := addEconomicsInt64(&fact.ConsumeQuota, -row.ConsumeQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
		if err := addEconomicsInt64(&fact.RefundQuota, -row.RefundQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
		if fact.Requests < 0 || fact.ConsumeQuota < 0 || fact.RefundQuota < 0 {
			return financeStatementView{}, StabilityDataCoverage{}, nil, errors.New("内部账号用量超过同口径渠道用量")
		}
		domains[domain] = fact
		if err := addEconomicsInt64(&totalConsumeQuota, -row.ConsumeQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
		if err := addEconomicsInt64(&totalRefundQuota, -row.RefundQuota); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, nil, err
		}
	}
	if totalConsumeQuota < 0 || totalRefundQuota < 0 {
		return financeStatementView{}, StabilityDataCoverage{}, nil, errors.New("内部账号用量超过全站同口径用量")
	}
	var internal struct {
		Requests int64
		Quota    int64
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(SUM(requests),0) requests,COALESCE(SUM(quota),0) quota
		FROM channel_test_hour_samples WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version=?`,
		scope.FromTs, scope.ToTs, stabilityTrafficClassificationVersion).Scan(&internal).Error; err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, fmt.Errorf("读取内部测试用量事实: %w", err)
	}
	grossMoney, err := financeMoneyFromQuota(totalConsumeQuota)
	if err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, err
	}
	refundMoney, err := financeMoneyFromQuota(totalRefundQuota)
	if err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, err
	}
	userMoney, err := financeMoneyFromQuota(totalConsumeQuota - totalRefundQuota)
	if err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, err
	}
	configuredInternalNet := internalAccounts.NetQuota
	if err := addEconomicsInt64(&configuredInternalNet, internal.Quota); err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, err
	}
	internalMoney, err := financeMoneyFromQuota(configuredInternalNet)
	if err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, nil, err
	}
	coverage := m.stabilityDataCoverage(ctx, scope.FromTs, scope.ToTs, now)
	coverage.Complete = coverage.Complete && internalAccounts.Complete
	statement := financeStatementView{
		GrossUserConsumption: grossMoney, UserRefunds: refundMoney,
		KnownUserConsumption: userMoney, InternalTestConsumption: internalMoney, InternalTestRequests: internal.Requests + internalAccounts.Requests,
	}
	if internalAccounts.Accounts > 0 && !internalAccounts.Complete {
		statement.KnownUserConsumption = channelEconomicsMoneyView{}
		domains = map[string]financeDomainUserFact{}
	}
	if coverage.Complete && internalAccounts.Complete {
		statement.UserConsumption = financeMoneyPointer(userMoney)
	}
	return statement, coverage, domains, nil
}

func financeMonthRanges(from, to time.Time) [][2]time.Time {
	// 财务月按产品口径的北京时间切分，不能依赖容器或 CI runner 的本地时区。
	// HTTP 参数及 SQLite 事实均是 Unix 时间；先转换时区不会改变边界瞬间，
	// 但能保证 UTC 环境和 Asia/Shanghai 环境得到同一组自然月。
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		// Go 运行时通常包含该时区；极端精简运行时仍要保持 UTC+8 财务边界。
		loc = time.FixedZone("CST", 8*60*60)
	}
	from = from.In(loc)
	to = to.In(loc)
	if !from.Before(to) {
		return nil
	}
	cursor := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, from.Location())
	ranges := make([][2]time.Time, 0, 24)
	for cursor.Before(to) {
		next := cursor.AddDate(0, 1, 0)
		left, right := cursor, next
		if left.Before(from) {
			left = from
		}
		if right.After(to) {
			right = to
		}
		ranges = append(ranges, [2]time.Time{left, right})
		cursor = next
	}
	return ranges
}

// financeClosedNaturalDayScope returns only complete Beijing calendar days
// contained by the requested range. Daily-only providers cannot truthfully
// allocate their bill to a partial first or current day.
func financeClosedNaturalDayScope(scope stabilityScope, now int64) stabilityScope {
	from := scope.FromTs
	if from != cstDayStart(from) {
		from = cstDayStart(from) + 86400
	}
	to := scope.ToTs
	if to != cstDayStart(to) {
		to = cstDayStart(to)
	}
	if today := cstDayStart(now); to > today {
		to = today
	}
	if to < from {
		to = from
	}
	return stabilityScope{FromTs: from, ToTs: to}
}

type financeTestPairKey struct {
	HourTs    int64
	ChannelID int
}

type financeManifestHourKey struct {
	Domain string
	HourTs int64
}

// loadFinanceInternalTestCostEvidence identifies only strict test-only
// channel-hours from already-published immutable economics facts. A mixed
// customer/test hour is deliberately left unresolved: request counts are not a
// defensible allocation key for model cost.
func (m *Monitor) loadFinanceInternalTestCostEvidence(ctx context.Context, scope stabilityScope, configured financeConfiguredInternalEvidence) (financeInternalTestCostEvidence, error) {
	var err error
	configured, err = m.financeCostExclusions(ctx, scope, configured)
	if err != nil {
		return financeInternalTestCostEvidence{}, err
	}
	return m.loadFinanceInternalCostEvidence(ctx, scope, configured, true)
}

// loadFinanceConfiguredAccountCostEvidence is the narrower channel-management
// view: it filters only the administrator-configured user IDs. Automatic probe
// traffic remains part of the finance report's separate internal-test policy,
// but must not make an otherwise valid configured-account filter unavailable.
func (m *Monitor) loadFinanceConfiguredAccountCostEvidence(ctx context.Context, scope stabilityScope, configured financeConfiguredInternalEvidence) (financeInternalTestCostEvidence, error) {
	return m.loadFinanceInternalCostEvidence(ctx, scope, configured, false)
}

func (m *Monitor) loadFinanceInternalCostEvidence(ctx context.Context, scope stabilityScope, configured financeConfiguredInternalEvidence, includeAutomaticTests bool) (financeInternalTestCostEvidence, error) {
	result := financeInternalTestCostEvidence{
		ByDomain:        map[string]financeInternalTestCostFact{},
		ByDay:           map[int64]financeInternalTestCostFact{},
		ExcludeByDomain: map[string]financeInternalTestCostFact{},
		ExcludeByDay:    map[int64]financeInternalTestCostFact{},
		SourceComplete:  configured.Complete,
		SourceScope:     configured.VerifiedScope,
	}
	if scope.ToTs <= scope.FromTs {
		return result, nil
	}
	type testPairRow struct {
		HourTs                       int64
		ChannelID                    int
		Requests                     int64
		LocalInternalRequests        int64
		LocalInternalRevenueMicroUSD int64
	}
	var automaticPairs []testPairRow
	if includeAutomaticTests {
		if err := m.storeDB.WithContext(ctx).Raw(`SELECT hour_ts,channel_id,COALESCE(SUM(requests),0) requests
		FROM channel_test_hour_samples
		WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version=?
		GROUP BY hour_ts,channel_id HAVING COALESCE(SUM(requests),0)>0`,
			scope.FromTs, scope.ToTs, stabilityTrafficClassificationVersion).Scan(&automaticPairs).Error; err != nil {
			return result, fmt.Errorf("读取内部测试小时事实: %w", err)
		}
	}
	pairMap := make(map[financeTestPairKey]testPairRow, len(automaticPairs)+len(configured.Rows))
	for _, row := range automaticPairs {
		key := financeTestPairKey{HourTs: row.HourTs, ChannelID: row.ChannelID}
		pairMap[key] = row
	}
	for _, row := range configured.Rows {
		if (row.Requests <= 0 && row.RefundRecords <= 0) || row.ChannelID <= 0 {
			continue
		}
		key := financeTestPairKey{HourTs: row.HourTs, ChannelID: row.ChannelID}
		pair := pairMap[key]
		pair.HourTs, pair.ChannelID = row.HourTs, row.ChannelID
		if err := addEconomicsInt64(&pair.Requests, row.Requests); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&pair.LocalInternalRequests, row.Requests); err != nil {
			return result, err
		}
		netQuota := row.ConsumeQuota - row.RefundQuota
		micro, conversionErr := signedUnitsToMicroUSDCanonical(netQuota, strconv.FormatInt(int64(quotaPerUSD), 10))
		if conversionErr != nil {
			return result, conversionErr
		}
		if err := addEconomicsInt64(&pair.LocalInternalRevenueMicroUSD, micro); err != nil {
			return result, err
		}
		pairMap[key] = pair
	}
	testPairs := make([]testPairRow, 0, len(pairMap))
	for _, row := range pairMap {
		testPairs = append(testPairs, row)
	}
	sort.Slice(testPairs, func(i, j int) bool {
		if testPairs[i].HourTs != testPairs[j].HourTs {
			return testPairs[i].HourTs < testPairs[j].HourTs
		}
		return testPairs[i].ChannelID < testPairs[j].ChannelID
	})
	result.TestPairs = int64(len(testPairs))
	if len(testPairs) == 0 {
		result.Complete = configured.Complete
		return result, nil
	}

	var manifests []ChannelEconomicsHourManifestPublication
	if err := m.storeDB.WithContext(ctx).Table("channel_economics_hour_manifest_current mc").
		Select("mp.*").
		Joins("JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").
		Where("mp.semantics_version=? AND mp.hour_ts>=? AND mp.hour_ts<?", channelEconomicsSemanticsVersion, scope.FromTs, scope.ToTs).
		Find(&manifests).Error; err != nil {
		return result, fmt.Errorf("读取内部测试成本发布头: %w", err)
	}
	var publications []channelEconomicsReportRow
	if err := m.storeDB.WithContext(ctx).Table("channel_economics_hour_manifest_current mc").
		Select(`p.publication_id,p.finance_version,p.domain,p.account_epoch,p.hour_ts,p.local_channel_id,p.local_requests,p.upstream_requests,
			p.local_refund_records,p.revenue_micro_usd,p.upstream_charge_units,p.upstream_cost_micro_usd,
			p.corrected_cost_micro_usd,p.profit_micro_usd,p.corrected_cost_known,p.profit_known,p.coverage_status`).
		Joins("JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").
		Joins("JOIN channel_economics_hour_publications p ON p.domain=mp.domain AND p.hour_ts=mp.hour_ts AND p.account_epoch=mp.authoritative_epoch AND p.semantics_version=mp.semantics_version").
		Joins("JOIN channel_economics_hour_current c ON c.publication_id=p.publication_id").
		Where("p.semantics_version=? AND p.hour_ts>=? AND p.hour_ts<?", channelEconomicsSemanticsVersion, scope.FromTs, scope.ToTs).
		Limit(maxChannelEconomicsReportRows + 1).Scan(&publications).Error; err != nil {
		return result, fmt.Errorf("读取内部测试成本发布事实: %w", err)
	}
	if len(publications) > maxChannelEconomicsReportRows {
		return result, fmt.Errorf("内部测试成本明细超过安全上限 %d，请缩小查询范围", maxChannelEconomicsReportRows)
	}

	rowsByHour := make(map[financeManifestHourKey][]channelEconomicsReportRow)
	for _, row := range publications {
		key := financeManifestHourKey{Domain: row.Domain, HourTs: row.HourTs}
		rowsByHour[key] = append(rowsByHour[key], row)
	}
	validHour := make(map[financeManifestHourKey]bool, len(manifests))
	for _, manifest := range manifests {
		key := financeManifestHourKey{Domain: manifest.Domain, HourTs: manifest.HourTs}
		rows := rowsByHour[key]
		ids := make([]string, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.PublicationID)
		}
		sort.Strings(ids)
		digest := sha256.Sum256([]byte(strings.Join(ids, "\n")))
		validHour[key] = manifest.CoverageStatus == "verified_complete" && manifest.ProfitKnown &&
			manifest.RowCount == int64(len(rows)) && manifest.PublicationSetHash == hex.EncodeToString(digest[:])
	}
	validByPair := make(map[financeTestPairKey][]channelEconomicsReportRow)
	diagnosticsByPair := make(map[financeTestPairKey]financeInternalPairDiagnostic)
	domainByPair := make(map[financeTestPairKey]string)
	for _, row := range publications {
		pair := financeTestPairKey{HourTs: row.HourTs, ChannelID: row.LocalChannelID}
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if previous, found := domainByPair[pair]; !found {
			domainByPair[pair] = domain
		} else if previous != domain {
			domainByPair[pair] = ""
		}
		hourKey := financeManifestHourKey{Domain: row.Domain, HourTs: row.HourTs}
		diagnostic := diagnosticsByPair[pair]
		diagnostic.HasPublication = true
		diagnostic.MissingCost = diagnostic.MissingCost || row.CoverageStatus == "upstream_cost_missing"
		diagnostic.ManifestUnverified = diagnostic.ManifestUnverified || !validHour[hourKey]
		diagnosticsByPair[pair] = diagnostic
		if !validHour[hourKey] || row.LocalChannelID <= 0 || row.CoverageStatus != "verified_complete" ||
			!row.CorrectedCostKnown || !row.ProfitKnown {
			continue
		}
		validByPair[pair] = append(validByPair[pair], row)
	}

	for _, test := range testPairs {
		pair := financeTestPairKey{HourTs: test.HourTs, ChannelID: test.ChannelID}
		rows := validByPair[pair]
		if len(rows) != 1 {
			result.UnverifiedPairs++
			reason := diagnosticsByPair[pair].reason(len(rows))
			result.UnverifiedReasons = incrementFinanceInternalReason(result.UnverifiedReasons, reason)
			result.Events = append(result.Events, financeInternalTestCostEvent{HourTs: test.HourTs, Domain: domainByPair[pair], State: "unverified", Reason: reason})
			continue
		}
		row := rows[0]
		exclusionFact := financeInternalTestCostFact{
			RevenueMicroUSD:       row.RevenueMicroUSD,
			UpstreamCostMicroUSD:  row.UpstreamCostMicroUSD,
			CorrectedCostMicroUSD: row.CorrectedCostMicroUSD,
			ProfitMicroUSD:        row.ProfitMicroUSD,
			Rows:                  1,
		}
		excludedDomain := result.ExcludeByDomain[row.Domain]
		if err := addEconomicsInt64(&excludedDomain.RevenueMicroUSD, exclusionFact.RevenueMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&excludedDomain.UpstreamCostMicroUSD, exclusionFact.UpstreamCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&excludedDomain.CorrectedCostMicroUSD, exclusionFact.CorrectedCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&excludedDomain.ProfitMicroUSD, exclusionFact.ProfitMicroUSD); err != nil {
			return result, err
		}
		excludedDomain.Rows++
		result.ExcludeByDomain[row.Domain] = excludedDomain
		day := cstDayStart(row.HourTs)
		excludedDay := result.ExcludeByDay[day]
		if err := addEconomicsInt64(&excludedDay.RevenueMicroUSD, exclusionFact.RevenueMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&excludedDay.UpstreamCostMicroUSD, exclusionFact.UpstreamCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&excludedDay.CorrectedCostMicroUSD, exclusionFact.CorrectedCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&excludedDay.ProfitMicroUSD, exclusionFact.ProfitMicroUSD); err != nil {
			return result, err
		}
		excludedDay.Rows++
		result.ExcludeByDay[day] = excludedDay
		if row.LocalRequests != test.LocalInternalRequests || row.RevenueMicroUSD != test.LocalInternalRevenueMicroUSD {
			result.MixedPairs++
			result.Events = append(result.Events, financeInternalTestCostEvent{HourTs: row.HourTs, Domain: row.Domain, State: "mixed", Fact: exclusionFact})
			continue
		}
		fact := financeInternalTestCostFact{
			RevenueMicroUSD:       row.RevenueMicroUSD,
			UpstreamCostMicroUSD:  row.UpstreamCostMicroUSD,
			CorrectedCostMicroUSD: row.CorrectedCostMicroUSD,
			ProfitMicroUSD:        row.ProfitMicroUSD,
			Rows:                  1,
		}
		for _, target := range []*financeInternalTestCostFact{&result.Total} {
			if err := addEconomicsInt64(&target.UpstreamCostMicroUSD, fact.UpstreamCostMicroUSD); err != nil {
				return result, err
			}
			if err := addEconomicsInt64(&target.CorrectedCostMicroUSD, fact.CorrectedCostMicroUSD); err != nil {
				return result, err
			}
			if err := addEconomicsInt64(&target.ProfitMicroUSD, fact.ProfitMicroUSD); err != nil {
				return result, err
			}
			target.Rows++
		}
		domainFact := result.ByDomain[row.Domain]
		if err := addEconomicsInt64(&domainFact.UpstreamCostMicroUSD, fact.UpstreamCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&domainFact.CorrectedCostMicroUSD, fact.CorrectedCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&domainFact.ProfitMicroUSD, fact.ProfitMicroUSD); err != nil {
			return result, err
		}
		domainFact.Rows++
		result.ByDomain[row.Domain] = domainFact
		dayFact := result.ByDay[day]
		if err := addEconomicsInt64(&dayFact.UpstreamCostMicroUSD, fact.UpstreamCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&dayFact.CorrectedCostMicroUSD, fact.CorrectedCostMicroUSD); err != nil {
			return result, err
		}
		if err := addEconomicsInt64(&dayFact.ProfitMicroUSD, fact.ProfitMicroUSD); err != nil {
			return result, err
		}
		dayFact.Rows++
		result.ByDay[day] = dayFact
		result.StrictPairs++
		result.Events = append(result.Events, financeInternalTestCostEvent{FinanceVersion: row.FinanceVersion, HourTs: row.HourTs, Domain: row.Domain, State: "strict", Fact: fact})
	}
	result.Complete = configured.Complete && result.StrictPairs == result.TestPairs && result.MixedPairs == 0 && result.UnverifiedPairs == 0
	return result, nil
}

func addFinanceInternalTestFact(dst *financeInternalTestCostFact, fact financeInternalTestCostFact) error {
	if err := addEconomicsInt64(&dst.RevenueMicroUSD, fact.RevenueMicroUSD); err != nil {
		return err
	}
	if err := addEconomicsInt64(&dst.CorrectedCostMicroUSD, fact.CorrectedCostMicroUSD); err != nil {
		return err
	}
	if err := addEconomicsInt64(&dst.UpstreamCostMicroUSD, fact.UpstreamCostMicroUSD); err != nil {
		return err
	}
	if err := addEconomicsInt64(&dst.ProfitMicroUSD, fact.ProfitMicroUSD); err != nil {
		return err
	}
	dst.Rows += fact.Rows
	return nil
}

// financeInternalTestCostSubrange derives month/day views from the one bounded
// database scan used for the full report. It prevents the finance page from
// rescanning the immutable ledger once per displayed month.
func financeInternalTestCostSubrange(source financeInternalTestCostEvidence, scope stabilityScope) (financeInternalTestCostEvidence, error) {
	result := financeInternalTestCostEvidence{
		ByDomain:        map[string]financeInternalTestCostFact{},
		ByDay:           map[int64]financeInternalTestCostFact{},
		ExcludeByDomain: map[string]financeInternalTestCostFact{},
		ExcludeByDay:    map[int64]financeInternalTestCostFact{},
		SourceComplete:  financeEvidenceScopeComplete(source.SourceComplete, source.SourceScope, scope),
		SourceScope:     financeEvidenceScopeIntersection(source.SourceScope, scope),
	}
	for _, event := range source.Events {
		if event.HourTs < scope.FromTs || event.HourTs >= scope.ToTs {
			continue
		}
		result.Events = append(result.Events, event)
		result.TestPairs++
		switch event.State {
		case "unverified":
			result.UnverifiedPairs++
			result.UnverifiedReasons = incrementFinanceInternalReason(result.UnverifiedReasons, event.Reason)
			continue
		case "mixed":
			result.MixedPairs++
		case "strict":
			result.StrictPairs++
			if err := addFinanceInternalTestFact(&result.Total, event.Fact); err != nil {
				return result, err
			}
			domainFact := result.ByDomain[event.Domain]
			if err := addFinanceInternalTestFact(&domainFact, event.Fact); err != nil {
				return result, err
			}
			result.ByDomain[event.Domain] = domainFact
			day := cstDayStart(event.HourTs)
			dayFact := result.ByDay[day]
			if err := addFinanceInternalTestFact(&dayFact, event.Fact); err != nil {
				return result, err
			}
			result.ByDay[day] = dayFact
		default:
			return result, fmt.Errorf("未知内部测试成本事件状态 %q", event.State)
		}
		excludedDomain := result.ExcludeByDomain[event.Domain]
		if err := addFinanceInternalTestFact(&excludedDomain, event.Fact); err != nil {
			return result, err
		}
		result.ExcludeByDomain[event.Domain] = excludedDomain
		day := cstDayStart(event.HourTs)
		excludedDay := result.ExcludeByDay[day]
		if err := addFinanceInternalTestFact(&excludedDay, event.Fact); err != nil {
			return result, err
		}
		result.ExcludeByDay[day] = excludedDay
	}
	result.Complete = result.SourceComplete && result.StrictPairs == result.TestPairs && result.MixedPairs == 0 && result.UnverifiedPairs == 0
	return result, nil
}

func applyFinanceInternalTestCost(statement *financeStatementView, fact financeInternalTestCostFact, mixedRows, unverifiedPairs int64, complete bool) {
	if statement == nil {
		return
	}
	statement.InternalTestCostRows = fact.Rows
	statement.InternalTestMixedRows = mixedRows
	statement.InternalTestUnverifiedPairs = unverifiedPairs
	if fact.Rows == 0 {
		return
	}
	statement.KnownInternalTestUpstreamCost = economicsMoney(fact.CorrectedCostMicroUSD)
	if complete {
		statement.InternalTestUpstreamCost = financeMoneyPointer(statement.KnownInternalTestUpstreamCost)
	}
}

func subtractFinanceInternalTestFromContribution(statement *financeStatementView, fact financeInternalTestCostFact, pairedRows int64) error {
	if statement == nil || fact.Rows == 0 {
		return nil
	}
	if pairedRows <= fact.Rows {
		statement.PairedUserConsumption = channelEconomicsMoneyView{}
		statement.PairedCorrectedCost = channelEconomicsMoneyView{}
		statement.KnownContributionProfit = channelEconomicsMoneyView{}
		statement.ContributionProfit = nil
		statement.ContributionMargin = nil
		statement.PairedContributionMargin = nil
		return nil
	}
	pairedCost, ok := financeMoneyInt64(statement.PairedCorrectedCost)
	if !ok {
		return errors.New("内部测试成本已识别，但配对成本缺失")
	}
	profit, ok := financeMoneyInt64(statement.KnownContributionProfit)
	if !ok {
		return errors.New("内部测试成本已识别，但配对贡献缺失")
	}
	pairedRevenue, ok := financeMoneyInt64(statement.PairedUserConsumption)
	if !ok {
		return errors.New("内部测试流量已识别，但配对收入缺失")
	}
	if fact.RevenueMicroUSD == math.MinInt64 || fact.CorrectedCostMicroUSD == math.MinInt64 || fact.ProfitMicroUSD == math.MinInt64 {
		return errors.New("内部测试成本超出可安全扣减范围")
	}
	if err := addEconomicsInt64(&pairedRevenue, -fact.RevenueMicroUSD); err != nil {
		return err
	}
	if err := addEconomicsInt64(&pairedCost, -fact.CorrectedCostMicroUSD); err != nil {
		return err
	}
	if err := addEconomicsInt64(&profit, -fact.ProfitMicroUSD); err != nil {
		return err
	}
	statement.PairedUserConsumption = economicsMoney(pairedRevenue)
	statement.PairedCorrectedCost = economicsMoney(pairedCost)
	statement.KnownContributionProfit = economicsMoney(profit)
	if statement.ContributionProfit != nil {
		statement.ContributionProfit = financeMoneyPointer(statement.KnownContributionProfit)
	}
	if pairedRevenue > 0 {
		margin := strconv.FormatFloat(float64(profit)*100/float64(pairedRevenue), 'f', 2, 64)
		statement.PairedContributionMargin = &margin
		if statement.ContributionProfit != nil {
			statement.ContributionMargin = &margin
		}
	}
	return nil
}

func subtractFinanceInternalTestFromDetail(detail *financeCostDetailView, fact financeInternalTestCostFact) error {
	if detail == nil || fact.Rows == 0 {
		return nil
	}
	statement := financeStatementView{
		PairedUserConsumption:   detail.PairedRevenue,
		PairedCorrectedCost:     detail.PairedCost,
		KnownContributionProfit: detail.KnownContribution,
		ContributionProfit:      detail.Contribution,
	}
	if err := subtractFinanceInternalTestFromContribution(&statement, fact, detail.PairedRows); err != nil {
		return err
	}
	detail.PairedRows -= fact.Rows
	if detail.PairedRows < 0 {
		detail.PairedRows = 0
	}
	detail.PairedRevenue = statement.PairedUserConsumption
	detail.PairedCost = statement.PairedCorrectedCost
	detail.KnownContribution = statement.KnownContributionProfit
	detail.Contribution = statement.ContributionProfit
	return nil
}

// financeRelevantUpstreamAccounts derives a conservative business-lifecycle
// boundary for each configured upstream. The first relevant hour is the
// earliest of customer traffic, classified internal-test traffic, or a
// non-zero upstream bill. Zero-filled backfill rows before that boundary prove
// nothing about a not-yet-used account and must not make every historical
// report permanently incomplete. Conversely, local activity before the first
// upstream bucket remains an explicit, blocking coverage gap.
func (m *Monitor) financeRelevantUpstreamAccounts(ctx context.Context, scope stabilityScope, accounts map[string]ChannelUpstreamAccountView) (map[string]ChannelUpstreamAccountView, error) {
	type activityStart struct {
		Domain  string
		FirstTs int64
	}
	var starts []activityStart
	query := `SELECT domain, MIN(hour_ts) first_ts FROM (
		SELECT LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain, s.hour_ts
		FROM stability_hour_samples s JOIN channel_snaps c ON c.id=s.channel_id
		WHERE s.traffic_class_version=? AND (s.success+s.anomaly+s.failed<>0 OR s.quota<>0 OR s.refund_quota<>0)
		UNION ALL
		SELECT LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain, t.hour_ts
		FROM channel_test_hour_samples t JOIN channel_snaps c ON c.id=t.channel_id
		WHERE t.traffic_class_version=? AND (t.requests<>0 OR t.quota<>0)
		UNION ALL
		SELECT LOWER(TRIM(domain)) domain, hour_ts FROM channel_upstream_usage_hours
		WHERE requests<>0 OR tokens<>0 OR quota<>0 OR cost_usd<>0
	) activity WHERE hour_ts<? GROUP BY domain`
	if err := m.storeDB.WithContext(ctx).Raw(query, stabilityTrafficClassificationVersion, stabilityTrafficClassificationVersion, scope.ToTs).Scan(&starts).Error; err != nil {
		return nil, fmt.Errorf("读取上游经营生效边界: %w", err)
	}
	firstByDomain := make(map[string]int64, len(starts))
	for _, row := range starts {
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if domain != "" && row.FirstTs >= 0 {
			firstByDomain[domain] = row.FirstTs
		}
	}
	relevant := make(map[string]ChannelUpstreamAccountView, len(accounts))
	for domain, account := range accounts {
		first, ok := firstByDomain[domain]
		if !account.UsageSyncEnabled || !ok || first >= scope.ToTs {
			continue
		}
		if first < scope.FromTs {
			first = scope.FromTs
		}
		granularity := account.UsageGranularity
		if granularity == "" {
			granularity = upstreamUsageGranularity(account.Provider, account.UsageAdapter)
		}
		if granularity == "day" || account.Provider == upstreamProviderAICodeWith {
			first = cstDayStart(first)
		} else {
			first -= first % 3600
		}
		account.FinanceRequiredFrom = first
		relevant[domain] = account
	}
	return relevant, nil
}

func (m *Monitor) loadFinanceUpstreamFacts(ctx context.Context, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot, userByDomain map[string]financeDomainUserFact) (channelEconomicsMoneyView, *channelEconomicsMoneyView, channelEconomicsMoneyView, *channelEconomicsMoneyView, financeUpstreamCoverageView, []financeCostDetailView, error) {
	financeAccounts, err := m.financeRelevantUpstreamAccounts(ctx, scope, accounts)
	if err != nil {
		return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, financeUpstreamCoverageView{}, nil, err
	}
	usage, rawBills, err := m.loadFinanceBillWindow(ctx, scope, now, financeAccounts, finance)
	if err != nil {
		return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, financeUpstreamCoverageView{}, nil, fmt.Errorf("读取上游账单小时事实: %w", err)
	}
	restrictUpstreamUsageToWholeDays(usage, scope)
	// For a range ending inside the current day, preserve complete closed-day
	// bills from daily-only providers as known evidence instead of discarding the
	// entire provider. They remain partial for the requested range and therefore
	// can never make the exact total complete.
	dailyAccounts := make(map[string]ChannelUpstreamAccountView)
	for domain, account := range financeAccounts {
		granularity := account.UsageGranularity
		if granularity == "" {
			granularity = upstreamUsageGranularity(account.Provider, account.UsageAdapter)
		}
		if account.UsageSyncEnabled && (granularity == "day" || account.Provider == upstreamProviderAICodeWith) {
			dailyAccounts[domain] = account
		}
	}
	dailyPartial := make(map[string]bool, len(dailyAccounts))
	closedDayScope := financeClosedNaturalDayScope(scope, now)
	if len(dailyAccounts) > 0 && closedDayScope.FromTs < closedDayScope.ToTs &&
		(closedDayScope.FromTs != scope.FromTs || closedDayScope.ToTs != scope.ToTs) {
		dailyUsage, dailyBills, dailyErr := m.loadFinanceBillWindow(ctx, closedDayScope, now, dailyAccounts, finance)
		if dailyErr != nil {
			return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, financeUpstreamCoverageView{}, nil, fmt.Errorf("读取上游自然日账单事实: %w", dailyErr)
		}
		for domain, metrics := range dailyUsage {
			if current, ok := usage[domain]; !ok || current.IntegrityStatus == upstreamUsageIntegrityWindowMismatch {
				usage[domain] = metrics
				delete(rawBills, domain)
				if billed, found := dailyBills[domain]; found {
					rawBills[domain] = billed
				}
				dailyPartial[domain] = true
			}
		}
	}
	ledger, err := m.buildChannelEconomicsReportMode(ctx, scope, "", true)
	if err != nil {
		return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, financeUpstreamCoverageView{}, nil, fmt.Errorf("读取已发布经济事实: %w", err)
	}
	ledgerByDomain := make(map[string]channelEconomicsDomainView, len(ledger.Domains))
	for _, domain := range ledger.Domains {
		ledgerByDomain[domain.Domain] = domain
	}
	domainSet := make(map[string]bool, len(userByDomain)+len(accounts))
	for domain := range userByDomain {
		domainSet[domain] = true
	}
	for domain, account := range financeAccounts {
		if account.UsageSyncEnabled {
			domainSet[domain] = true
		}
	}
	domains := make([]string, 0, len(domainSet))
	for domain := range domainSet {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	coverage := financeUpstreamCoverageView{}
	var knownBilled, knownCorrected int64
	var unconfiguredUserConsumption int64
	correctedCompleteDomains := 0
	details := make([]financeCostDetailView, 0, len(domains))
	for _, domain := range domains {
		local := userByDomain[domain]
		localMoney, moneyErr := financeMoneyFromQuota(local.ConsumeQuota - local.RefundQuota)
		if moneyErr != nil {
			return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, coverage, nil, moneyErr
		}
		account, accountFound := accounts[domain]
		configured := accountFound && account.Configured
		metrics, usageFound := usage[domain]
		detail := financeCostDetailView{Domain: domain, UserRequests: local.Requests, UserConsumption: localMoney}
		if configured {
			coverage.RelevantDomains++
		} else {
			coverage.UnconfiguredDomains++
			coverage.UnconfiguredUserRequests += local.Requests
			if value, ok := financeMoneyInt64(localMoney); ok {
				if err := addEconomicsInt64(&unconfiguredUserConsumption, value); err != nil {
					return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, coverage, nil, err
				}
			}
		}
		if configured && account.UsageSyncEnabled {
			coverage.UsageEnabledDomains++
		}
		if usageFound {
			detail.ExpectedHours, detail.CompletedHours, detail.DataUntil = metrics.ExpectedHours, metrics.CompletedHours, metrics.DataUntil
			detail.BillBasis = metrics.Granularity
			if dailyPartial[domain] {
				detail.BillBasis = "closed_natural_days"
			}
		}
		validUsage := configured && account.UsageSyncEnabled && usageFound && metrics.Available && (metrics.IntegrityStatus == "" || metrics.IntegrityStatus == upstreamUsageIntegrityComplete)
		if validUsage {
			coverage.AvailableDomains++
			coverage.ExpectedDomainHours += metrics.ExpectedHours
			coverage.CompletedDomainHours += metrics.CompletedHours
			detail.UpstreamRequests = metrics.Requests
			amounts, found := rawBills[domain]
			if !found {
				return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, coverage, nil, fmt.Errorf("上游 %s 缺少已校验账单金额", domain)
			}
			billed := amounts.Raw
			detail.KnownBilledCost = billed
			if value, ok := financeMoneyInt64(billed); ok {
				if err := addEconomicsInt64(&knownBilled, value); err != nil {
					return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, coverage, nil, err
				}
			}
			if metrics.Complete && !dailyPartial[domain] {
				coverage.CompleteDomains++
				detail.BilledCost = financeMoneyPointer(billed)
			}
		}
		var corrected channelEconomicsMoneyView
		correctedKnown, correctedExact := false, false
		if validUsage && metrics.AdjustedCostAvailable {
			corrected = rawBills[domain].RechargeCorrected
			if _, ok := financeMoneyInt64(corrected); !ok {
				return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, coverage, nil, fmt.Errorf("上游 %s 缺少已校验修正金额", domain)
			}
			correctedKnown, correctedExact, detail.CorrectionSource = true, metrics.Complete && !dailyPartial[domain], "充值版本"
		} else if economic, found := ledgerByDomain[domain]; configured && found {
			if value, ok := financeMoneyInt64(economic.Totals.KnownCorrectedCost); ok {
				corrected = economicsMoney(value)
				correctedKnown = true
				correctedExact = economic.Totals.CorrectedCostKnown && economic.Coverage.Complete
				detail.CorrectionSource = "经济事实账"
			}
		}
		if correctedKnown {
			coverage.CorrectedDomains++
			detail.KnownCorrectedCost = corrected
			if value, ok := financeMoneyInt64(corrected); ok {
				if err := addEconomicsInt64(&knownCorrected, value); err != nil {
					return channelEconomicsMoneyView{}, nil, channelEconomicsMoneyView{}, nil, coverage, nil, err
				}
			}
			if correctedExact {
				correctedCompleteDomains++
				detail.CorrectedCost = financeMoneyPointer(corrected)
			}
		}
		// Contribution is intentionally read from the immutable economics ledger.
		// Its paired revenue/cost include only publication rows whose local fact,
		// upstream cost, mapping and finance version were verified together. Never
		// subtract a partial cost from the domain's all-range user consumption.
		if economic, found := ledgerByDomain[domain]; configured && found && economic.Totals.PairedPublicationRows > 0 {
			detail.PairedRevenue = economic.Totals.PairedRevenue
			detail.PairedCost = economic.Totals.PairedCorrectedCost
			detail.PairedRows = economic.Totals.PairedPublicationRows
			detail.KnownContribution = economic.Totals.KnownProfit
			if economic.Totals.ProfitKnown {
				detail.Contribution = financeMoneyPointer(economic.Totals.KnownProfit)
			}
		}
		switch {
		case !configured:
			detail.Status = "not_configured"
		case !account.UsageSyncEnabled:
			detail.Status = "not_connected"
		case !validUsage:
			detail.Status = "no_data"
		case metrics.Complete && correctedExact:
			detail.Status = "verified"
		default:
			detail.Status = "incomplete"
		}
		details = append(details, detail)
	}
	if coverage.UnconfiguredDomains > 0 {
		coverage.UnconfiguredUserConsumption = economicsMoney(unconfiguredUserConsumption)
	}
	knownBilledMoney, knownCorrectedMoney := economicsMoney(knownBilled), economicsMoney(knownCorrected)
	allRaw := coverage.RelevantDomains > 0 && coverage.UsageEnabledDomains == coverage.RelevantDomains && coverage.CompleteDomains == coverage.RelevantDomains
	coverage.Complete = allRaw && correctedCompleteDomains == coverage.RelevantDomains
	var billedExact, correctedExact *channelEconomicsMoneyView
	if allRaw && coverage.UnconfiguredDomains == 0 {
		billedExact = financeMoneyPointer(knownBilledMoney)
	}
	if coverage.wholeCostScopeComplete() {
		correctedExact = financeMoneyPointer(knownCorrectedMoney)
	}
	sort.SliceStable(details, func(i, j int) bool {
		left, _ := financeMoneyInt64(details[i].UserConsumption)
		right, _ := financeMoneyInt64(details[j].UserConsumption)
		if left != right {
			return left > right
		}
		return details[i].Domain < details[j].Domain
	})
	return knownBilledMoney, billedExact, knownCorrectedMoney, correctedExact, coverage, details, nil
}

func (m *Monitor) buildFinancePeriod(ctx context.Context, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot, internalTestCost financeInternalTestCostEvidence, internalAccounts financeConfiguredInternalEvidence, businessGroups map[string]bool) (financeStatementView, StabilityDataCoverage, financeUpstreamCoverageView, []financeCostDetailView, error) {
	statement, userCoverage, userByDomain, err := m.loadFinanceUserFacts(ctx, scope, now, internalAccounts, businessGroups)
	if err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, financeUpstreamCoverageView{}, nil, err
	}
	knownBilled, billed, knownCorrected, corrected, upstreamCoverage, details, err := m.loadFinanceUpstreamFacts(ctx, scope, now, accounts, finance, userByDomain)
	if err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, financeUpstreamCoverageView{}, nil, err
	}
	for index := range details {
		if err := subtractFinanceInternalTestFromDetail(&details[index], internalTestCost.ExcludeByDomain[details[index].Domain]); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, financeUpstreamCoverageView{}, nil,
				fmt.Errorf("从 %s 客户贡献中分离内部测试成本: %w", details[index].Domain, err)
		}
	}
	applyFinanceInternalTestCost(&statement, internalTestCost.Total, internalTestCost.MixedPairs, internalTestCost.UnverifiedPairs, internalTestCost.Complete)
	statement.KnownUpstreamBilledCost, statement.UpstreamBilledCost = knownBilled, billed
	statement.KnownRawCorrectedUpstreamCost, statement.RawCorrectedUpstreamCost = knownCorrected, corrected
	deductionSources, err := m.loadFinancePeriodDeductionSources(ctx, details, internalTestCost)
	if err != nil {
		return financeStatementView{}, StabilityDataCoverage{}, financeUpstreamCoverageView{}, nil, err
	}
	statement.KnownCorrectedUpstreamCost, statement.InternalCostDeductionStatus = financeInternalCostDeduction(knownCorrected, internalTestCost, deductionSources)
	if statement.InternalCostDeductionStatus != "" {
		// Keep the independently known gross cost, but inconsistent inclusion
		// evidence cannot certify all-site expense/profit as complete.
		statement.RawCorrectedUpstreamCost = nil
	}
	if corrected != nil && internalTestCost.Complete && statement.InternalCostDeductionStatus == "" {
		statement.CorrectedUpstreamCost = financeMoneyPointer(statement.KnownCorrectedUpstreamCost)
	}
	// Zero is a valid amount only when there is evidence for at least one domain.
	// With no bill/correction evidence, keep the value absent so the UI cannot
	// accidentally present "unknown" as "$0.00".
	if upstreamCoverage.AvailableDomains == 0 {
		statement.KnownUpstreamBilledCost = channelEconomicsMoneyView{}
		statement.UpstreamBilledCost = nil
		statement.KnownRawCorrectedUpstreamCost = channelEconomicsMoneyView{}
		statement.RawCorrectedUpstreamCost = nil
	}
	if upstreamCoverage.CorrectedDomains == 0 {
		statement.KnownRawCorrectedUpstreamCost = channelEconomicsMoneyView{}
		statement.RawCorrectedUpstreamCost = nil
		statement.KnownCorrectedUpstreamCost = channelEconomicsMoneyView{}
		statement.CorrectedUpstreamCost = nil
	}
	// A partial upstream cost must never be subtracted from whole-site revenue:
	// that would present the uncovered domains as if their cost were zero. The
	// known contribution is therefore only the sum of domains where both local
	// usage and a correction-backed upstream cost are present.
	var pairedRevenue, pairedCost, knownContribution int64
	knownContributionDomains := 0
	for i := range details {
		detail := &details[i]
		if detail.Status == "not_configured" {
			continue
		}
		if detail.KnownContribution.MicroUSD == "" {
			continue
		}
		if _, err := financeAddMoney(&pairedRevenue, detail.PairedRevenue); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, financeUpstreamCoverageView{}, nil, err
		}
		if _, err := financeAddMoney(&pairedCost, detail.PairedCost); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, financeUpstreamCoverageView{}, nil, err
		}
		value, ok := financeMoneyInt64(detail.KnownContribution)
		if !ok {
			continue
		}
		if err := addEconomicsInt64(&knownContribution, value); err != nil {
			return financeStatementView{}, StabilityDataCoverage{}, financeUpstreamCoverageView{}, nil, err
		}
		knownContributionDomains++
	}
	if knownContributionDomains > 0 {
		statement.PairedUserConsumption = economicsMoney(pairedRevenue)
		statement.PairedCorrectedCost = economicsMoney(pairedCost)
		statement.KnownContributionProfit = economicsMoney(knownContribution)
		if pairedRevenue > 0 {
			margin := strconv.FormatFloat(float64(knownContribution)*100/float64(pairedRevenue), 'f', 2, 64)
			statement.PairedContributionMargin = &margin
		}
	}
	if userCoverage.Complete && upstreamCoverage.Complete && knownContributionDomains == upstreamCoverage.RelevantDomains {
		statement.ContributionProfit = financeMoneyPointer(statement.KnownContributionProfit)
		statement.ContributionMargin = statement.PairedContributionMargin
	}
	return statement, userCoverage, upstreamCoverage, details, nil
}

type financeCostDetailAccumulator struct {
	view               financeCostDetailView
	userMicro          int64
	billedMicro        int64
	correctedMicro     int64
	pairedRevenueMicro int64
	pairedCostMicro    int64
	contributionMicro  int64
	billedSeen         bool
	correctedSeen      bool
	contributionSeen   bool
	pairedSeen         bool
	billedExact        bool
	correctedExact     bool
	contributionExact  bool
	correctionSources  map[string]bool
	billBases          map[string]bool
}

func financeAddMoney(total *int64, value channelEconomicsMoneyView) (bool, error) {
	micro, ok := financeMoneyInt64(value)
	if !ok {
		return false, nil
	}
	return true, addEconomicsInt64(total, micro)
}

func mergeFinanceCostDetails(periodDetails [][]financeCostDetailView, accounts map[string]ChannelUpstreamAccountView) (financeUpstreamCoverageView, []financeCostDetailView, error) {
	byDomain := make(map[string]*financeCostDetailAccumulator)
	for _, details := range periodDetails {
		for _, detail := range details {
			accumulator := byDomain[detail.Domain]
			if accumulator == nil {
				accumulator = &financeCostDetailAccumulator{
					view: financeCostDetailView{Domain: detail.Domain}, billedExact: true, correctedExact: true,
					contributionExact: true, correctionSources: map[string]bool{}, billBases: map[string]bool{},
				}
				byDomain[detail.Domain] = accumulator
			}
			accumulator.view.UserRequests += detail.UserRequests
			accumulator.view.UpstreamRequests += detail.UpstreamRequests
			accumulator.view.PairedRows += detail.PairedRows
			if _, err := financeAddMoney(&accumulator.userMicro, detail.UserConsumption); err != nil {
				return financeUpstreamCoverageView{}, nil, err
			}
			seen, err := financeAddMoney(&accumulator.billedMicro, detail.KnownBilledCost)
			if err != nil {
				return financeUpstreamCoverageView{}, nil, err
			}
			accumulator.billedSeen = accumulator.billedSeen || seen
			accumulator.billedExact = accumulator.billedExact && detail.BilledCost != nil
			seen, err = financeAddMoney(&accumulator.correctedMicro, detail.KnownCorrectedCost)
			if err != nil {
				return financeUpstreamCoverageView{}, nil, err
			}
			accumulator.correctedSeen = accumulator.correctedSeen || seen
			accumulator.correctedExact = accumulator.correctedExact && detail.CorrectedCost != nil
			pairedRevenueSeen, err := financeAddMoney(&accumulator.pairedRevenueMicro, detail.PairedRevenue)
			if err != nil {
				return financeUpstreamCoverageView{}, nil, err
			}
			pairedCostSeen, err := financeAddMoney(&accumulator.pairedCostMicro, detail.PairedCost)
			if err != nil {
				return financeUpstreamCoverageView{}, nil, err
			}
			accumulator.pairedSeen = accumulator.pairedSeen || (pairedRevenueSeen && pairedCostSeen)
			seen, err = financeAddMoney(&accumulator.contributionMicro, detail.KnownContribution)
			if err != nil {
				return financeUpstreamCoverageView{}, nil, err
			}
			accumulator.contributionSeen = accumulator.contributionSeen || seen
			accumulator.contributionExact = accumulator.contributionExact && detail.Contribution != nil
			accumulator.view.ExpectedHours += detail.ExpectedHours
			accumulator.view.CompletedHours += detail.CompletedHours
			if detail.DataUntil > accumulator.view.DataUntil {
				accumulator.view.DataUntil = detail.DataUntil
			}
			if source := strings.TrimSpace(detail.CorrectionSource); source != "" {
				accumulator.correctionSources[source] = true
			}
			if basis := strings.TrimSpace(detail.BillBasis); basis != "" {
				accumulator.billBases[basis] = true
			}
		}
	}

	domains := make([]string, 0, len(byDomain))
	for domain := range byDomain {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	coverage := financeUpstreamCoverageView{}
	details := make([]financeCostDetailView, 0, len(domains))
	correctedCompleteDomains := 0
	var unconfiguredUserConsumption int64
	for _, domain := range domains {
		accumulator := byDomain[domain]
		view := accumulator.view
		view.UserConsumption = economicsMoney(accumulator.userMicro)
		account, accountFound := accounts[domain]
		configured := accountFound && account.Configured
		if configured {
			coverage.RelevantDomains++
		} else {
			coverage.UnconfiguredDomains++
			coverage.UnconfiguredUserRequests += view.UserRequests
			if err := addEconomicsInt64(&unconfiguredUserConsumption, accumulator.userMicro); err != nil {
				return financeUpstreamCoverageView{}, nil, err
			}
		}
		if configured && account.UsageSyncEnabled {
			coverage.UsageEnabledDomains++
		}
		if configured && accumulator.billedSeen {
			coverage.AvailableDomains++
			view.KnownBilledCost = economicsMoney(accumulator.billedMicro)
			if accumulator.billedExact {
				coverage.CompleteDomains++
				view.BilledCost = financeMoneyPointer(view.KnownBilledCost)
			}
		}
		if configured && accumulator.correctedSeen {
			coverage.CorrectedDomains++
			view.KnownCorrectedCost = economicsMoney(accumulator.correctedMicro)
			if accumulator.correctedExact {
				correctedCompleteDomains++
				view.CorrectedCost = financeMoneyPointer(view.KnownCorrectedCost)
			}
		}
		if configured && accumulator.contributionSeen {
			if accumulator.pairedSeen {
				view.PairedRevenue = economicsMoney(accumulator.pairedRevenueMicro)
				view.PairedCost = economicsMoney(accumulator.pairedCostMicro)
			}
			view.KnownContribution = economicsMoney(accumulator.contributionMicro)
			if accumulator.contributionExact {
				view.Contribution = financeMoneyPointer(view.KnownContribution)
			}
		}
		sources := make([]string, 0, len(accumulator.correctionSources))
		for source := range accumulator.correctionSources {
			sources = append(sources, source)
		}
		sort.Strings(sources)
		view.CorrectionSource = strings.Join(sources, " + ")
		bases := make([]string, 0, len(accumulator.billBases))
		for basis := range accumulator.billBases {
			bases = append(bases, basis)
		}
		sort.Strings(bases)
		view.BillBasis = strings.Join(bases, "+")
		if configured {
			coverage.ExpectedDomainHours += view.ExpectedHours
			coverage.CompletedDomainHours += view.CompletedHours
		}
		switch {
		case !configured:
			view.Status = "not_configured"
		case !account.UsageSyncEnabled:
			view.Status = "not_connected"
		case !accumulator.billedSeen:
			view.Status = "no_data"
		case accumulator.billedExact && accumulator.correctedExact:
			view.Status = "verified"
		default:
			view.Status = "incomplete"
		}
		details = append(details, view)
	}
	if coverage.UnconfiguredDomains > 0 {
		coverage.UnconfiguredUserConsumption = economicsMoney(unconfiguredUserConsumption)
	}
	coverage.Complete = coverage.RelevantDomains > 0 && coverage.UsageEnabledDomains == coverage.RelevantDomains &&
		coverage.CompleteDomains == coverage.RelevantDomains && correctedCompleteDomains == coverage.RelevantDomains
	sort.SliceStable(details, func(i, j int) bool {
		left, _ := financeMoneyInt64(details[i].UserConsumption)
		right, _ := financeMoneyInt64(details[j].UserConsumption)
		if left != right {
			return left > right
		}
		return details[i].Domain < details[j].Domain
	})
	return coverage, details, nil
}

func aggregateFinancePeriods(periods []financePeriodView, userCoverage StabilityDataCoverage, upstreamCoverage financeUpstreamCoverageView) (financeStatementView, error) {
	statement := financeStatementView{}
	var grossMicro, refundMicro, userMicro, internalMicro, internalCostMicro, billedMicro, rawCorrectedMicro, correctedMicro int64
	var pairedRevenueMicro, pairedCostMicro, contributionMicro int64
	var billedSeen, rawCorrectedSeen, correctedSeen, contributionSeen, internalCostSeen bool
	allUserKnown := true
	allInternalCostExact := true
	allBilledExact, allRawCorrectedExact, allCorrectedExact, allContributionExact := len(periods) > 0, len(periods) > 0, len(periods) > 0, len(periods) > 0
	for _, period := range periods {
		if period.Statement.InternalCostDeductionStatus != "" {
			statement.InternalCostDeductionStatus = "cost_exclusion_incomplete"
		}
		allBilledExact = allBilledExact && period.Statement.UpstreamBilledCost != nil
		allRawCorrectedExact = allRawCorrectedExact && period.Statement.RawCorrectedUpstreamCost != nil
		allCorrectedExact = allCorrectedExact && period.Statement.CorrectedUpstreamCost != nil
		allContributionExact = allContributionExact && period.Statement.ContributionProfit != nil
		if _, err := financeAddMoney(&grossMicro, period.Statement.GrossUserConsumption); err != nil {
			return statement, err
		}
		if _, err := financeAddMoney(&refundMicro, period.Statement.UserRefunds); err != nil {
			return statement, err
		}
		seenUser, err := financeAddMoney(&userMicro, period.Statement.KnownUserConsumption)
		if err != nil {
			return statement, err
		}
		allUserKnown = allUserKnown && seenUser
		if _, err := financeAddMoney(&internalMicro, period.Statement.InternalTestConsumption); err != nil {
			return statement, err
		}
		statement.InternalTestRequests += period.Statement.InternalTestRequests
		statement.InternalTestCostRows += period.Statement.InternalTestCostRows
		statement.InternalTestMixedRows += period.Statement.InternalTestMixedRows
		statement.InternalTestUnverifiedPairs += period.Statement.InternalTestUnverifiedPairs
		seen, err := financeAddMoney(&internalCostMicro, period.Statement.KnownInternalTestUpstreamCost)
		if err != nil {
			return statement, err
		}
		internalCostSeen = internalCostSeen || seen
		if period.Statement.InternalTestRequests > 0 && period.Statement.InternalTestUpstreamCost == nil {
			allInternalCostExact = false
		}
		seen, err = financeAddMoney(&billedMicro, period.Statement.KnownUpstreamBilledCost)
		if err != nil {
			return statement, err
		}
		billedSeen = billedSeen || seen
		seen, err = financeAddMoney(&rawCorrectedMicro, period.Statement.KnownRawCorrectedUpstreamCost)
		if err != nil {
			return statement, err
		}
		rawCorrectedSeen = rawCorrectedSeen || (seen && period.UpstreamCoverage.CorrectedDomains > 0)
		seen, err = financeAddMoney(&correctedMicro, period.Statement.KnownCorrectedUpstreamCost)
		if err != nil {
			return statement, err
		}
		correctedSeen = correctedSeen || (seen && period.UpstreamCoverage.CorrectedDomains > 0)
		if _, err := financeAddMoney(&pairedRevenueMicro, period.Statement.PairedUserConsumption); err != nil {
			return statement, err
		}
		if _, err := financeAddMoney(&pairedCostMicro, period.Statement.PairedCorrectedCost); err != nil {
			return statement, err
		}
		seen, err = financeAddMoney(&contributionMicro, period.Statement.KnownContributionProfit)
		if err != nil {
			return statement, err
		}
		contributionSeen = contributionSeen || seen
	}
	statement.GrossUserConsumption = economicsMoney(grossMicro)
	statement.UserRefunds = economicsMoney(refundMicro)
	if allUserKnown {
		statement.KnownUserConsumption = economicsMoney(userMicro)
	}
	statement.InternalTestConsumption = economicsMoney(internalMicro)
	if internalCostSeen {
		statement.KnownInternalTestUpstreamCost = economicsMoney(internalCostMicro)
		if allInternalCostExact {
			statement.InternalTestUpstreamCost = financeMoneyPointer(statement.KnownInternalTestUpstreamCost)
		}
	}
	if userCoverage.Complete {
		statement.UserConsumption = financeMoneyPointer(statement.KnownUserConsumption)
	}
	if billedSeen {
		statement.KnownUpstreamBilledCost = economicsMoney(billedMicro)
		// Raw bills can be complete while recharge correction remains unknown.
		// Do not make their total stricter than the exact monthly bill rows.
		if upstreamCoverage.RelevantDomains > 0 && upstreamCoverage.UnconfiguredDomains == 0 &&
			upstreamCoverage.CompleteDomains == upstreamCoverage.RelevantDomains && allBilledExact {
			statement.UpstreamBilledCost = financeMoneyPointer(statement.KnownUpstreamBilledCost)
		}
	}
	if correctedSeen && statement.InternalCostDeductionStatus == "" {
		statement.KnownCorrectedUpstreamCost = economicsMoney(correctedMicro)
		if upstreamCoverage.wholeCostScopeComplete() && allCorrectedExact {
			statement.CorrectedUpstreamCost = financeMoneyPointer(statement.KnownCorrectedUpstreamCost)
		}
	}
	if rawCorrectedSeen {
		statement.KnownRawCorrectedUpstreamCost = economicsMoney(rawCorrectedMicro)
		if upstreamCoverage.wholeCostScopeComplete() && allRawCorrectedExact {
			statement.RawCorrectedUpstreamCost = financeMoneyPointer(statement.KnownRawCorrectedUpstreamCost)
		}
	}
	if contributionSeen {
		statement.PairedUserConsumption = economicsMoney(pairedRevenueMicro)
		statement.PairedCorrectedCost = economicsMoney(pairedCostMicro)
		statement.KnownContributionProfit = economicsMoney(contributionMicro)
		if pairedRevenueMicro > 0 {
			margin := strconv.FormatFloat(float64(contributionMicro)*100/float64(pairedRevenueMicro), 'f', 2, 64)
			statement.PairedContributionMargin = &margin
		}
	}
	if userCoverage.Complete && upstreamCoverage.wholeCostScopeComplete() && contributionSeen && allContributionExact {
		statement.ContributionProfit = financeMoneyPointer(statement.KnownContributionProfit)
		statement.ContributionMargin = statement.PairedContributionMargin
	}
	return statement, nil
}

func (m *Monitor) buildFinanceOperatingReport(ctx context.Context, from, to time.Time) (*financeOperatingReport, error) {
	report := &financeOperatingReport{
		Enabled: m.cfg.FinanceEnabled, Stage: "read_only_preview", GeneratedAt: time.Now().Unix(),
		From: from.Unix(), To: to.Unix(), TimeZone: "Asia/Shanghai",
		Periods: []financePeriodView{}, Days: []financeDailyView{}, CostDetails: []financeCostDetailView{},
		SemanticsNote: "用户净计费消耗 = 消费扣额 - 退还额度，自动测试和人工配置的内部账号另行列示，不算客户收入。注册赠送只在用户用量、额度调整和赠送用户逐笔金额顺序全部闭合后扣减，内部账号不参与赠送消耗扣减。修正上游总成本包含内部测试实际支出，平台经营结果全额扣除；可核验业务上游成本只在同源证据完整时分离严格识别的内部测试成本，“其中：内部测试上游成本”不重复扣除。上游账单原值仍完整保留用于对账。未配置上游账户的来源会保留用户消费但暂不进入账单覆盖率、补采任务和正式经营毛利，不会按零成本处理。已配对计费贡献只来自同一小时内计费扣额、上游成本、渠道映射和倍率版本同时核验的不可变配对事实。混合流量小时不做比例估算，未闭合部分不会被当作 0。",
		Notices: []string{
			"本页只读 Monitor 本地用户用量事实、上游账单和不可变经济事实，刷新不会访问生产库或上游。",
			"注册赠送事实覆盖不完整时经营收入保持空值；内部测试上游成本仅发布严格识别部分，混合流量和 AWS CUR 尚未闭合，因此最终经营毛利保持空值。",
		},
	}
	if !report.Enabled {
		report.Stage = "disabled"
		report.Sources = financeSourceViews(StabilityDataCoverage{}, financeUpstreamCoverageView{}, financeStatementView{}, financeGiftCoverageView{}, financeCURCostView{})
		return report, nil
	}
	finance, err := m.loadChannelFinanceSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取渠道倍率版本: %w", err)
	}
	accounts, err := m.loadChannelUpstreamViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取上游账户状态: %w", err)
	}
	fullScope := stabilityScope{FromTs: from.Unix(), ToTs: to.Unix()}
	businessGroups, err := loadChannelBusinessGroupPolicies(ctx, m.storeDB)
	if err != nil {
		return nil, fmt.Errorf("读取经营核算分组范围: %w", err)
	}
	configuredInternal, err := m.loadFinanceInternalEvidence(ctx, fullScope, businessGroups, true)
	if err != nil {
		return nil, fmt.Errorf("读取内部账号核算事实: %w", err)
	}
	report.InternalAccounts = configuredInternal.SyncState
	internalTestCost, err := m.loadFinanceInternalTestCostEvidence(ctx, fullScope, configuredInternal)
	if err != nil {
		return nil, err
	}
	periodConfigurationHash, err := m.financeReportConfigurationHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取月度经营核算配置版本: %w", err)
	}
	periodDetails := make([][]financeCostDetailView, 0, 24)
	for _, monthRange := range financeMonthRanges(from, to) {
		monthScope := stabilityScope{FromTs: monthRange[0].Unix(), ToTs: monthRange[1].Unix()}
		monthInternalAccounts, sliceErr := financeConfiguredInternalSubrange(configuredInternal, monthScope)
		if sliceErr != nil {
			return nil, fmt.Errorf("汇总 %s 内部账号用量: %w", monthRange[0].Format("2006-01"), sliceErr)
		}
		monthInternalTestCost, sliceErr := financeInternalTestCostSubrange(internalTestCost, monthScope)
		if sliceErr != nil {
			return nil, fmt.Errorf("汇总 %s 内部测试成本: %w", monthRange[0].Format("2006-01"), sliceErr)
		}
		component, _, err := m.buildFinancePeriodComponent(
			ctx, monthScope, report.GeneratedAt, periodConfigurationHash, accounts, finance,
			monthInternalTestCost, monthInternalAccounts, businessGroups,
		)
		if err != nil {
			return nil, fmt.Errorf("读取 %s 月度经济事实: %w", monthRange[0].Format("2006-01"), err)
		}
		report.Periods = append(report.Periods, financePeriodView{
			Period: monthRange[0].Format("2006-01"), From: monthRange[0].Unix(), To: monthRange[1].Unix(),
			Statement: component.Statement, UserCoverage: component.UserCoverage, UpstreamCoverage: component.UpstreamCoverage,
			Status: financePeriodStatus(component.UserCoverage, component.UpstreamCoverage, component.Statement.KnownUserConsumption.MicroUSD != ""),
		})
		periodDetails = append(periodDetails, component.CostDetails)
		report.Days = append(report.Days, component.Days...)
	}
	report.UserCoverage = m.stabilityDataCoverage(ctx, from.Unix(), to.Unix(), report.GeneratedAt)
	report.UserCoverage.Complete = report.UserCoverage.Complete && configuredInternal.Complete
	report.UpstreamCoverage, report.CostDetails, err = mergeFinanceCostDetails(periodDetails, accounts)
	if err != nil {
		return nil, fmt.Errorf("汇总上游成本明细: %w", err)
	}
	report.Statement, err = aggregateFinancePeriods(report.Periods, report.UserCoverage, report.UpstreamCoverage)
	if err != nil {
		return nil, fmt.Errorf("汇总经营核算期间: %w", err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	financeSeed, seedErr := time.ParseInLocation("2006-01-02", strings.TrimSpace(m.cfg.FinanceStartDate), loc)
	if seedErr != nil {
		return nil, fmt.Errorf("读取经营核算起始日: %w", seedErr)
	}
	var giftResult financeGiftAllocationResult
	giftTo := min(to.Unix(), m.usageFactFinalizedHour(time.Unix(report.GeneratedAt, 0)))
	publishedThrough, publicationErr := m.financeFactPublishedThrough(ctx, financeSeed.Unix())
	if publicationErr != nil {
		return nil, fmt.Errorf("读取经营核算事实发布水位: %w", publicationErr)
	}
	giftTo = min(giftTo, publishedThrough)
	if from.Unix() >= financeSeed.Unix() && giftTo > from.Unix() {
		var giftErr error
		excludedGiftUsers := make(map[int64]bool)
		for _, userID := range configuredInternal.AccountIDs {
			excludedGiftUsers[userID] = true
		}
		giftResult, giftErr = m.loadFinanceGiftAllocationForScope(ctx, financeSeed.Unix(), from.Unix(), giftTo, businessGroups, excludedGiftUsers)
		if giftErr != nil {
			return nil, fmt.Errorf("核验注册赠送实际消耗: %w", giftErr)
		}
		if giftResult.Coverage.ScopeUnknownEvents > 0 {
			report.Notices = append(report.Notices, "部分历史赠送消耗缺少分组依据；缺口前已完整核验的期间正常显示，跨越缺口的期间及总收入待补齐，其他已知金额仍保留。")
		}
		giftResult, giftErr = excludeFinanceGiftUsers(giftResult, excludedGiftUsers)
		if giftErr != nil {
			return nil, fmt.Errorf("从赠送消耗中分离内部账号: %w", giftErr)
		}
		giftResult.Coverage.RequestedToTs = to.Unix()
		if giftTo < to.Unix() {
			giftResult.Coverage.LatestHourPending = true
			giftResult.Coverage.ProvisionalSeconds = to.Unix() - giftTo
		}
		report.GiftCoverage = giftResult.Coverage
		if err := applyFinanceGiftAllocation(&report.Statement, giftResult); err != nil {
			return nil, fmt.Errorf("计算赠送扣减后经营收入: %w", err)
		}
		// A local snapshot or temporarily lagging collector can have a verified
		// prefix followed only by absent tail hours. Preserve that prefix as an
		// explicitly partial revenue value; if any later user fact exists, fail
		// closed because the whole-range known amount no longer equals the prefix.
		if err := m.applyFinanceVerifiedPrefixRevenue(ctx, &report.Statement, giftResult, giftTo, to.Unix()); err != nil {
			return nil, fmt.Errorf("核验经营收入已知前缀: %w", err)
		}
	} else {
		report.GiftCoverage = financeGiftCoverageView{
			SeedFromTs: financeSeed.Unix(), FromTs: from.Unix(), ToTs: giftTo, RequestedToTs: to.Unix(),
		}
		if giftTo < to.Unix() {
			report.GiftCoverage.LatestHourPending = true
			report.GiftCoverage.ProvisionalSeconds = to.Unix() - giftTo
		}
	}
	if err := applyFinanceGiftBreakdowns(report, giftResult); err != nil {
		return nil, err
	}
	report.PairingAudit, err = m.loadFinancePairingAudit(ctx, stabilityScope{FromTs: from.Unix(), ToTs: to.Unix()})
	if err != nil {
		return nil, err
	}
	var currentChannels []ChannelSnap
	if err := m.storeDB.WithContext(ctx).Select("id", "name", "base_domain", "deleted_at").Find(&currentChannels).Error; err != nil {
		return nil, fmt.Errorf("读取待归属成本的当前渠道候选: %w", err)
	}
	applyFinancePairingSourceCandidates(&report.PairingAudit, finance, currentChannels)
	applyFinancePairingDomainCoverage(&report.PairingAudit, report.CostDetails)
	costEvidence, financeVersions, closureErr := m.loadFinanceClosureEvidence(ctx, fullScope, report.CostDetails)
	if closureErr != nil {
		return nil, closureErr
	}
	applyFinanceClosureEvidence(report.CostDetails, costEvidence, financeVersions)
	applyFinanceCostPairingStatuses(report.CostDetails, report.PairingAudit)
	if err := m.applyFinanceEvidenceBackfillEstimate(ctx, fullScope, report.GeneratedAt, report.CostDetails, accounts); err != nil {
		return nil, err
	}
	m.applyFinanceCURArtifactToReport(report)
	if report.CURCost.Status == "error" {
		report.Notices = append(report.Notices, "AWS CUR 本地核算产物读取或校验失败；AWS 成本保持空值，不按 0 计算。")
	} else if report.CURCost.Status == "draft" {
		report.Notices = append(report.Notices, "AWS CUR 已载入草稿产物；当前仅展示已知成本，不参与正式经营毛利发布。")
	}
	report.Sources = financeSourceViews(report.UserCoverage, report.UpstreamCoverage, report.Statement, report.GiftCoverage, report.CURCost)
	return report, nil
}

func financeSourceViews(user StabilityDataCoverage, upstream financeUpstreamCoverageView, statement financeStatementView, gift financeGiftCoverageView, cur financeCURCostView) []financeSourceView {
	economicsStatus := "disabled"
	economicsDescription := "经营核算功能当前关闭"
	if user.ExpectedHours > 0 {
		economicsStatus = "incomplete"
		if user.Complete {
			economicsStatus = "verified"
		}
		economicsDescription = fmt.Sprintf("已完成 %d / %d 个用户流量小时，内部测试已分离", user.CompletedHours, user.ExpectedHours)
	}
	upstreamStatus := "incomplete"
	if upstream.RelevantDomains == 0 {
		upstreamStatus = "no_data"
	} else if upstream.Complete {
		upstreamStatus = "verified"
	}
	upstreamDescription := fmt.Sprintf("%d / %d 个已配置域名取得账单，%d 个具备可用修正证据", upstream.AvailableDomains, upstream.RelevantDomains, upstream.CorrectedDomains)
	if upstream.UnconfiguredDomains > 0 {
		upstreamDescription += fmt.Sprintf("；另有 %d 个未配置来源、%s 用户消费暂不纳入正式毛利", upstream.UnconfiguredDomains, upstream.UnconfiguredUserConsumption.Display)
	}
	pairedStatus := "no_data"
	pairedDescription := "当前未找到收入、成本、渠道映射和倍率版本同时核验的发布事实"
	if !user.Complete && user.ExpectedHours == 0 {
		pairedStatus = "disabled"
		pairedDescription = "经营核算功能当前关闭"
	} else if statement.KnownContributionProfit.MicroUSD != "" {
		pairedStatus = "incomplete"
		if statement.ContributionProfit != nil {
			pairedStatus = "verified"
		}
		pairedDescription = fmt.Sprintf("已配对收入 %s、已配对修正成本 %s；只在证据完整的同一小时内做差额", statement.PairedUserConsumption.Display, statement.PairedCorrectedCost.Display)
	}
	giftStatus := "incomplete"
	giftDescription := fmt.Sprintf("用户小时 %d/%d、额度小时 %d/%d、赠送用户明细 %d/%d",
		gift.UserCompletedHours, gift.ExpectedHours, gift.CreditCompletedHours, gift.ExpectedHours,
		gift.CompletedBoundaryUserHours, gift.ExpectedBoundaryUserHours)
	if gift.ExpectedHours == 0 {
		giftStatus = "no_data"
		giftDescription = "当前查询区间不在经营核算事实范围内"
	} else if gift.Complete {
		giftStatus = "verified"
		giftDescription = fmt.Sprintf("%d 笔候选注册赠送、%d 个用户已完成逐笔消耗分摊", gift.EligibleGrants, gift.GiftUsers)
	}
	internalTestStatus := "no_data"
	internalTestDescription := "当前区间没有内部测试请求"
	if statement.InternalTestRequests > 0 {
		internalTestStatus = "incomplete"
		knownCost := "尚无可严格识别金额"
		if statement.KnownInternalTestUpstreamCost.MicroUSD != "" {
			knownCost = statement.KnownInternalTestUpstreamCost.Display
		}
		internalTestDescription = fmt.Sprintf("严格识别 %d 个纯测试渠道小时，已知修正成本 %s；%d 个混合流量小时、%d 个未核验对不做估算",
			statement.InternalTestCostRows, knownCost,
			statement.InternalTestMixedRows, statement.InternalTestUnverifiedPairs)
		if statement.InternalTestUpstreamCost != nil {
			internalTestStatus = "verified"
			internalTestDescription = fmt.Sprintf("已完整核验 %d 个纯内部测试渠道小时，修正成本 %s",
				statement.InternalTestCostRows, statement.InternalTestUpstreamCost.Display)
		}
	}
	curStatus, curDescription := financeCURSourceStatus(cur)
	return []financeSourceView{
		{Key: "user_usage", Name: "用户净计费消耗", Status: economicsStatus, Description: economicsDescription + "，退还额度已从消费扣额中减除"},
		{Key: "upstream_bill", Name: "上游账单与成本修正", Status: upstreamStatus, Description: upstreamDescription},
		{Key: "paired_economics", Name: "不可变收入成本配对", Status: pairedStatus, Description: pairedDescription},
		{Key: "internal_test_cost", Name: "内部测试上游成本", Status: internalTestStatus, Description: internalTestDescription},
		{Key: "signup_gift", Name: "注册赠送实际消耗", Status: giftStatus, Description: giftDescription},
		{Key: "aws_cur", Name: "AWS 资源成本", Status: curStatus, Description: curDescription},
	}
}

func parseFinanceRange(c *gin.Context, now time.Time, startDate string) (time.Time, time.Time, error) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	now = now.In(loc)
	defaultFrom, err := time.ParseInLocation("2006-01-02", startDate, loc)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	from := defaultFrom
	if raw := strings.TrimSpace(c.Query("from")); raw != "" {
		from, err = time.ParseInLocation("2006-01-02", raw, loc)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("from 必须是 YYYY-MM-DD")
		}
	}
	to := now.Truncate(time.Hour)
	if raw := strings.TrimSpace(c.Query("to")); raw != "" {
		day, parseErr := time.ParseInLocation("2006-01-02", raw, loc)
		if parseErr != nil {
			return time.Time{}, time.Time{}, errors.New("to 必须是 YYYY-MM-DD")
		}
		to = day.AddDate(0, 0, 1)
		closedHour := now.Truncate(time.Hour)
		if to.After(closedHour) {
			to = closedHour
		}
	}
	if !from.Before(to) {
		return time.Time{}, time.Time{}, errors.New("查询开始时间必须早于已闭合的结束时间")
	}
	if to.Sub(from) > financeReportMaxDays*24*time.Hour {
		return time.Time{}, time.Time{}, fmt.Errorf("查询范围不能超过 %d 天", financeReportMaxDays)
	}
	return from, to, nil
}

// clampFinanceRangeToLocalSnapshot prevents a static acceptance snapshot from
// accumulating artificial tail gaps as wall-clock time advances. Historical
// gaps inside the snapshot remain visible; production ranges are never
// changed. The returned asOf is the exclusive end of the latest strictly
// complete stability hour.
func (m *Monitor) clampFinanceRangeToLocalSnapshot(ctx context.Context, to time.Time) (clamped time.Time, asOf int64, changed bool, err error) {
	if !m.cfg.LocalSnapshotOnly {
		return to, 0, false, nil
	}
	var latest struct {
		HourTs int64
	}
	predicate := stabilityCompleteHourPredicateSQL("hs")
	if tx := m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(MAX(hs.hour_ts),0) hour_ts
		FROM stability_hour_ingest_states hs WHERE `+predicate,
		stabilityTrafficClassificationVersion, stabilityTrafficClassificationVersion).Scan(&latest); tx.Error != nil {
		return to, 0, false, fmt.Errorf("读取本地快照截止时间: %w", tx.Error)
	}
	if latest.HourTs <= 0 || latest.HourTs > math.MaxInt64-3600 {
		return to, 0, false, nil
	}
	asOf = latest.HourTs + 3600
	if to.Unix() <= asOf {
		return to, asOf, false, nil
	}
	return time.Unix(asOf, 0).In(to.Location()), asOf, true, nil
}

func (m *Monitor) serveFinanceOperatingReport(c *gin.Context) {
	c.Header("Cache-Control", "private, no-store")
	from, to, err := parseFinanceRange(c, time.Now(), m.cfg.FinanceStartDate)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), financeReportBuildTimeout)
	defer cancel()
	if !m.cfg.FinanceEnabled {
		report, _ := m.buildFinanceOperatingReport(ctx, from, to)
		c.JSON(http.StatusOK, report)
		return
	}
	if c.Query("fresh") == "1" && !m.cfg.FinanceFastSnapshotEnabled {
		ctx = context.WithValue(ctx, financeForceRebuildKey{}, true)
	}
	to, snapshotAsOf, snapshotClamped, err := m.clampFinanceRangeToLocalSnapshot(ctx, to)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "经营核算报表暂不可用", "detail": err.Error()})
		return
	}
	if !from.Before(to) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "查询范围晚于本地快照截止时间"})
		return
	}
	configStarted := time.Now()
	configurationHash, configErr := m.financeReportConfigurationHash(ctx)
	logFinanceReadStageTiming("configuration", configStarted, configErr)
	if configErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "经营核算报表暂不可用", "detail": configErr.Error()})
		return
	}
	if m.cfg.FinanceFastSnapshotEnabled {
		fastRequest := financeReportRequest{from: from, to: to, snapshotAsOf: snapshotAsOf, snapshotClamped: snapshotClamped, configurationHash: configurationHash}
		m.serveFinanceQueuedReport(c, fastRequest)
		return
	}
	fingerprintStarted := time.Now()
	sourceFingerprint, fingerprintErr := m.financeReportSourceFingerprint(ctx, from.Unix(), to.Unix())
	logFinanceReadStageTiming("source_fingerprint", fingerprintStarted, fingerprintErr)
	if fingerprintErr != nil {
		// A validated prior snapshot can still be displayed as stale. A new
		// build must establish a source version again; never skip consistency
		// validation merely because this first probe was unavailable.
		sourceFingerprint = ""
	}
	request := financeReportRequest{from: from, to: to, snapshotAsOf: snapshotAsOf, snapshotClamped: snapshotClamped, configurationHash: configurationHash, sourceFingerprint: sourceFingerprint}
	payload, cacheStatus, err := m.financeReportPayload(ctx, request, c.Query("fresh") == "1")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "经营核算报表暂不可用", "detail": err.Error()})
		return
	}
	c.Header("X-Monitor-Finance-Cache", cacheStatus)
	c.Data(http.StatusOK, "application/json; charset=utf-8", payload)
}
