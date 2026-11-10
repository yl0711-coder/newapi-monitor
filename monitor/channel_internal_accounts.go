package monitor

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	channelInternalSourceIncomplete   = "source_incomplete"
	channelInternalMixedUsage         = "mixed_usage"
	channelInternalPairingUnverified  = "pairing_unverified"
	channelInternalOwnershipUnknown   = "ownership_unknown"
	channelInternalAmountInconsistent = "amount_inconsistent"
)

// Preserve all applicable reasons; diagnostics must not imply that waiting for
// the collector can resolve a mixed hour or missing historical ownership.
func channelInternalCostReasons(evidence financeInternalTestCostEvidence, domain string) []string {
	var reasons []string
	if !evidence.SourceComplete {
		reasons = append(reasons, channelInternalSourceIncomplete)
	}
	domain = strings.ToLower(strings.TrimSpace(domain))
	found := map[string]bool{}
	for _, event := range evidence.Events {
		owner := strings.ToLower(strings.TrimSpace(event.Domain))
		if event.State == "strict" || (owner != "" && owner != domain) {
			continue
		}
		if owner == "" {
			found[channelInternalOwnershipUnknown] = true
		}
		if event.State == "mixed" {
			found[channelInternalMixedUsage] = true
		} else {
			found[channelInternalPairingUnverified] = true
		}
	}
	for _, reason := range []string{channelInternalMixedUsage, channelInternalPairingUnverified, channelInternalOwnershipUnknown} {
		if found[reason] {
			reasons = append(reasons, reason)
		}
	}
	return reasons
}

func applyChannelInternalCostEvidence(metrics ChannelUpstreamUsageMetrics, evidence financeInternalTestCostEvidence, domain string, accounts int64) ChannelUpstreamUsageMetrics {
	result := applyChannelInternalCostFilter(metrics, evidence.ByDomain[domain], accounts, financeInternalCostDomainComplete(evidence, domain))
	if accounts > 0 && result.InternalFilterStatus != "complete" {
		result.InternalFilterReasons = channelInternalCostReasons(evidence, domain)
		if result.InternalFilterStatus == "inconsistent" {
			result.InternalFilterReasons = append(result.InternalFilterReasons, channelInternalAmountInconsistent)
		}
	}
	return result
}

// Both channel cards and the sync page project the same local cost evidence.
// No source reads, synchronization jobs or monetary facts are changed here.
func (m *Monitor) applyChannelInternalCostViews(ctx context.Context, scope stabilityScope, now int64, policies map[string]bool, internal financeConfiguredInternalEvidence, usage map[string]ChannelUpstreamUsageMetrics, bills map[string]*ChannelUpstreamNaturalDayBill) error {
	costs, err := m.loadFinanceConfiguredAccountCostEvidence(ctx, scope, internal)
	if err != nil {
		return fmt.Errorf("核验内部账号上游成本: %w", err)
	}
	for domain, metrics := range usage {
		usage[domain] = applyChannelInternalCostEvidence(metrics, costs, domain, internal.Accounts)
	}
	if len(bills) == 0 {
		return nil
	}
	dailyInternal := internal
	dailyCosts := costs
	if internal.Accounts > 0 {
		billScope := naturalDayBillingScope(scope, now)
		dailyInternal, err = m.loadFinanceConfiguredInternalEvidence(ctx, billScope, policies)
		if err != nil {
			return fmt.Errorf("读取内部账号自然日事实: %w", err)
		}
		dailyCosts, err = m.loadFinanceConfiguredAccountCostEvidence(ctx, billScope, dailyInternal)
		if err != nil {
			return fmt.Errorf("核验内部账号自然日成本: %w", err)
		}
	}
	for domain, bill := range bills {
		if bill != nil {
			bill.Usage = applyChannelInternalCostEvidence(bill.Usage, dailyCosts, domain, dailyInternal.Accounts)
		}
	}
	return nil
}

const channelInternalCostToleranceUSD = 0.00001

type channelInternalUsageKey struct {
	ChannelID int
	Group     string
}

type channelInternalUsageFact struct {
	Requests int64
	Tokens   int64
	Quota    int64
}

func channelInternalUsageByChannelGroup(evidence financeConfiguredInternalEvidence) (map[channelInternalUsageKey]channelInternalUsageFact, error) {
	result := make(map[channelInternalUsageKey]channelInternalUsageFact)
	if evidence.Accounts == 0 || !evidence.Complete {
		return result, nil
	}
	for _, row := range evidence.Rows {
		key := channelInternalUsageKey{ChannelID: row.ChannelID, Group: row.Grp}
		fact := result[key]
		if err := addEconomicsInt64(&fact.Requests, row.Requests); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&fact.Tokens, row.Tokens); err != nil {
			return nil, err
		}
		if err := addEconomicsInt64(&fact.Quota, row.ConsumeQuota); err != nil {
			return nil, err
		}
		result[key] = fact
	}
	return result, nil
}

// ChannelManagementDataWarning reports a channel×group whose configured
// internal-account usage (log-derived facts) exceeds the sampled channel usage
// for the same scope. That means sampled hours are missing; the row is shown
// clamped at zero and flagged instead of failing the whole report.
type ChannelManagementDataWarning struct {
	Kind            string  `json:"kind"`
	ChannelID       int     `json:"channel_id"`
	Group           string  `json:"group"`
	MissingRequests int64   `json:"missing_requests"`
	MissingTokens   int64   `json:"missing_tokens"`
	MissingQuota    int64   `json:"missing_quota"`
	Hours           []int64 `json:"hours,omitempty"`
	HoursTruncated  bool    `json:"hours_truncated,omitempty"`
}

const channelDataWarningInternalExceedsSampled = "internal_exceeds_sampled"

// subtractChannelInternalUsage removes configured internal-account usage. When
// the sampled row is smaller (missing sampler hours), it clamps at zero and
// returns the shortfall so the caller can report it; it never fails the report.
func subtractChannelInternalUsage(row *channelManagementUsageRow, fact channelInternalUsageFact) (channelInternalUsageFact, bool) {
	var shortfall channelInternalUsageFact
	if row == nil {
		return shortfall, false
	}
	clamp := func(value *int64, sub int64, missing *int64) {
		*value -= sub
		if *value < 0 {
			*missing = -*value
			*value = 0
		}
	}
	clamp(&row.Requests, fact.Requests, &shortfall.Requests)
	clamp(&row.Tokens, fact.Tokens, &shortfall.Tokens)
	clamp(&row.Quota, fact.Quota, &shortfall.Quota)
	return shortfall, shortfall.Requests > 0 || shortfall.Tokens > 0 || shortfall.Quota > 0
}

const channelDataWarningMaxHours = 48

// channelInternalShortfallHours finds the hours where internal facts exceed the
// sampled usage for one channel×group, so operators know which hours to repair.
func channelInternalShortfallHours(internal financeConfiguredInternalEvidence, sampled map[int64]channelInternalUsageFact, channelID int, group string) ([]int64, bool) {
	byHour := map[int64]channelInternalUsageFact{}
	for _, row := range internal.Rows {
		if row.ChannelID != channelID || strings.TrimSpace(row.Grp) != group {
			continue
		}
		fact := byHour[row.HourTs]
		fact.Requests += row.Requests
		fact.Tokens += row.Tokens
		fact.Quota += row.ConsumeQuota
		byHour[row.HourTs] = fact
	}
	hours := make([]int64, 0, len(byHour))
	for hour, fact := range byHour {
		got := sampled[hour]
		if fact.Requests > got.Requests || fact.Tokens > got.Tokens || fact.Quota > got.Quota {
			hours = append(hours, hour)
		}
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })
	if len(hours) > channelDataWarningMaxHours {
		return hours[:channelDataWarningMaxHours], true
	}
	return hours, false
}

// financeInternalCostDomainComplete isolates evidence failures by upstream
// account. A mixed hour on one domain must not blank every other account. An
// unverified pair whose domain cannot be recovered remains a global blocker.
func financeInternalCostDomainComplete(evidence financeInternalTestCostEvidence, domain string) bool {
	if !evidence.SourceComplete {
		return false
	}
	domain = strings.ToLower(strings.TrimSpace(domain))
	for _, event := range evidence.Events {
		if event.State == "strict" {
			continue
		}
		eventDomain := strings.ToLower(strings.TrimSpace(event.Domain))
		if eventDomain == "" || eventDomain == domain {
			return false
		}
	}
	return true
}

// applyChannelInternalCostFilter 保留上游原账单字段，同时只在
// 内部账号历史与不可变渠道经济事实全部闭合时发布业务成本。
// 混合客户/内部小时不做比例分摊，而是明确保持业务成本不可用。
func applyChannelInternalCostFilter(metrics ChannelUpstreamUsageMetrics, fact financeInternalTestCostFact, configuredAccounts int64, complete bool) ChannelUpstreamUsageMetrics {
	metrics.BusinessCostAvailable = metrics.Available
	metrics.BusinessCostUSD = metrics.CostUSD
	metrics.BusinessAdjustedCostAvailable = metrics.AdjustedCostAvailable
	metrics.BusinessAdjustedCostUSD = metrics.AdjustedCostUSD
	metrics.InternalFilterStatus = "not_configured"
	if configuredAccounts == 0 {
		return metrics
	}
	metrics.BusinessCostAvailable = false
	metrics.BusinessAdjustedCostAvailable = false
	metrics.BusinessCostUSD = 0
	metrics.BusinessAdjustedCostUSD = 0
	metrics.InternalFilterStatus = "backfilling"
	if !complete {
		return metrics
	}
	metrics.InternalFilterStatus = "complete"
	metrics.InternalExcludedCostUSD = float64(fact.UpstreamCostMicroUSD) / 1_000_000
	metrics.InternalExcludedAdjustedUSD = float64(fact.CorrectedCostMicroUSD) / 1_000_000
	if metrics.Available {
		remaining := metrics.CostUSD - metrics.InternalExcludedCostUSD
		if remaining >= -channelInternalCostToleranceUSD && !math.IsNaN(remaining) && !math.IsInf(remaining, 0) {
			metrics.BusinessCostUSD = math.Max(0, remaining)
			metrics.BusinessCostAvailable = true
		} else {
			metrics.InternalFilterStatus = "inconsistent"
		}
	}
	if metrics.AdjustedCostAvailable {
		remaining := metrics.AdjustedCostUSD - metrics.InternalExcludedAdjustedUSD
		if remaining >= -channelInternalCostToleranceUSD && !math.IsNaN(remaining) && !math.IsInf(remaining, 0) {
			metrics.BusinessAdjustedCostUSD = math.Max(0, remaining)
			metrics.BusinessAdjustedCostAvailable = true
		} else {
			metrics.InternalFilterStatus = "inconsistent"
		}
	}
	return metrics
}
