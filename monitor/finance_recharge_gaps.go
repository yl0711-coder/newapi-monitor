package monitor

import (
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
)

const (
	financeRechargeGapRangeLimit      = 10_000
	financeRechargeInspectBucketLimit = 200_000
	// This inspector also reads snapshots of newer adapters without importing
	// their network clients into the finance-only offline acceptance bundle.
	financeRechargeSubscriptionProvider = "openox"
)

// FinanceRechargeGapRange describes original bill buckets, not permission to
// backdate a rate. Face value uses the platform's unified USD display unit.
// Source currency labels do not add an FX step to paid/credit correction;
// this display unit is not a statement about bank settlement currency.
type FinanceRechargeGapRange struct {
	FromTs              int64                             `json:"from_ts"`
	ToTs                int64                             `json:"to_ts"`
	BucketSeconds       int64                             `json:"bucket_seconds"`
	Reason              string                            `json:"reason"`
	Buckets             int64                             `json:"buckets"`
	NonzeroCostBuckets  int64                             `json:"nonzero_cost_buckets"`
	FaceValueMicroUnits string                            `json:"face_value_micro_units"`
	ReviewAction        string                            `json:"review_action"`
	StartVersion        *FinanceRechargeVersionReference  `json:"start_version,omitempty"`
	RecordedChanges     []FinanceRechargeVersionReference `json:"recorded_changes,omitempty"`
}

type FinanceRechargeGapDomain struct {
	Domain              string                           `json:"domain"`
	FirstVersionAt      int64                            `json:"first_version_at,omitempty"`
	FirstVersionNumber  int64                            `json:"first_version_number,omitempty"`
	BillComplete        bool                             `json:"bill_complete"`
	Gaps                []FinanceRechargeGapRange        `json:"gaps"`
	ReferenceAtRangeEnd *FinanceRechargeVersionReference `json:"reference_at_range_end,omitempty"`
}

type FinanceRechargeGapSkipped struct {
	Domain string `json:"domain"`
	Reason string `json:"reason"`
}

type FinanceRechargeGapInspection struct {
	Mode                 string                      `json:"mode"`
	FromTs               int64                       `json:"from_ts"`
	ToTs                 int64                       `json:"to_ts"`
	SourceSnapshotSHA256 string                      `json:"source_snapshot_sha256,omitempty"`
	MonetaryUnit         string                      `json:"monetary_unit"`
	ExcludedDomains      []string                    `json:"excluded_domains"`
	Domains              []FinanceRechargeGapDomain  `json:"domains"`
	Skipped              []FinanceRechargeGapSkipped `json:"skipped"`
}

// Use the report's existing bill verifier and recharge selection rules. A
// diagnostic must not accept an overlapping/invalid bill that accounting
// rejects, nor make a provisional bucket into a safe historical repair target.
func inspectFinanceRechargeGaps(ctx context.Context, in *financeBillInputs, now int64) (FinanceRechargeGapInspection, error) {
	var empty FinanceRechargeGapInspection
	if in == nil || in.scope.FromTs < 0 || in.scope.ToTs <= in.scope.FromTs || in.scope.ToTs > now-now%3600 ||
		in.scope.ToTs-in.scope.FromTs > financeReportMaxDays*86400 || len(in.rows) > financeRechargeInspectBucketLimit {
		return empty, errors.New("recharge inspection requires a bounded closed accounting range")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	rows := append([]ChannelUpstreamUsageHour(nil), in.rows...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Domain != rows[j].Domain {
			return rows[i].Domain < rows[j].Domain
		}
		return rows[i].HourTs < rows[j].HourTs
	})
	// Do not pass unconfigured accounts even if a stale local bill exists.
	accounts := make(map[string]ChannelUpstreamAccountView)
	for domain, account := range in.accounts {
		if account.Configured && account.UsageSyncEnabled {
			accounts[domain] = account
		}
	}
	accepted := make([]ChannelUpstreamUsageHour, 0, len(rows))
	for _, row := range rows {
		if account, ok := accounts[row.Domain]; !ok || account.Provider != row.Provider {
			continue
		}
		seconds := row.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		if row.HourTs < 0 || row.HourTs > math.MaxInt64-seconds || (seconds != 3600 && seconds != 86400) {
			return empty, errors.New("recharge inspection found an invalid original bill interval")
		}
	}
	metrics, err := projectUpstreamUsageBuckets(rows, in.scope, now, accounts, in.versions, func(row ChannelUpstreamUsageHour) {
		accepted = append(accepted, row)
	})
	if err != nil {
		return empty, err
	}
	result := FinanceRechargeGapInspection{Mode: "offline_recharge_gap_plan_no_writes", FromTs: in.scope.FromTs, ToTs: in.scope.ToTs,
		MonetaryUnit: "platform_accounting_usd", Domains: []FinanceRechargeGapDomain{}, Skipped: []FinanceRechargeGapSkipped{}}
	byDomain, skipped, err := financeRechargeInspectionDomains(ctx, metrics, accounts, in.versions, in.scope.ToTs)
	if err != nil {
		return empty, err
	}
	result.Skipped = skipped
	ranges := 0
	for _, row := range accepted {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		detail := byDomain[row.Domain]
		if detail == nil {
			continue // One bad bucket invalidates the entire source's aggregate.
		}
		added, err := addFinanceRechargeGap(detail, row, in.versions[row.Domain])
		if err != nil {
			return empty, err
		}
		if added {
			ranges++
			if ranges > financeRechargeGapRangeLimit {
				return empty, errors.New("recharge gap plan exceeds its range budget; use a smaller period")
			}
		}
	}
	for _, detail := range byDomain {
		if len(detail.Gaps) > 0 {
			result.Domains = append(result.Domains, *detail)
		}
	}
	sort.Slice(result.Domains, func(i, j int) bool { return result.Domains[i].Domain < result.Domains[j].Domain })
	return result, ctx.Err()
}

func financeRechargeInspectionDomains(ctx context.Context, metrics map[string]ChannelUpstreamUsageMetrics, accounts map[string]ChannelUpstreamAccountView, versions map[string][]channelRechargeVersion, to int64) (map[string]*FinanceRechargeGapDomain, []FinanceRechargeGapSkipped, error) {
	byDomain := make(map[string]*FinanceRechargeGapDomain)
	skipped := []FinanceRechargeGapSkipped{}
	for domain, metric := range metrics {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		reason := metric.IntegrityStatus
		switch {
		case !metric.Available:
			reason = "bill_missing"
		case metric.IntegrityStatus != upstreamUsageIntegrityComplete:
		case metric.Provisional:
			reason = "bill_provisional"
		case accounts[domain].Provider == financeRechargeSubscriptionProvider:
			reason = "subscription_cash_cost_unverified"
		default:
			detail := &FinanceRechargeGapDomain{Domain: domain, BillComplete: metric.Complete, Gaps: []FinanceRechargeGapRange{}}
			if history := versions[domain]; len(history) > 0 {
				detail.FirstVersionAt, detail.FirstVersionNumber = history[0].EffectiveAt, history[0].Version
				detail.ReferenceAtRangeEnd, _ = financeRechargeReviewVersions(history, to-1, to)
			}
			byDomain[domain] = detail
			continue
		}
		skipped = append(skipped, FinanceRechargeGapSkipped{Domain: domain, Reason: reason})
	}
	sort.Slice(skipped, func(i, j int) bool { return skipped[i].Domain < skipped[j].Domain })
	return byDomain, skipped, nil
}

func addFinanceRechargeGap(detail *FinanceRechargeGapDomain, row ChannelUpstreamUsageHour, versions []channelRechargeVersion) (bool, error) {
	if upstreamBillHasZeroFaceValue(row) {
		return false, nil // Validated zero needs no invented historical rate.
	}
	seconds := row.BucketSeconds
	if seconds <= 0 {
		seconds = 3600
	}
	_, _, status := rechargeTermsForBucket(versions, row.HourTs, row.HourTs+seconds)
	if status == upstreamAdjustedCostComplete {
		return false, nil
	}
	reason := status
	if status == upstreamAdjustedCostMissingHistory {
		if len(versions) == 0 {
			reason = "no_history"
		} else if row.HourTs < detail.FirstVersionAt {
			reason = "before_first_version"
		} else {
			reason = "invalid_effective_version"
		}
	}
	money, err := financeMoneyFromUSD(row.CostUSD) // Precision only, not a currency assertion.
	if err != nil {
		return false, err
	}
	face, ok := financeMoneyInt64(money)
	if !ok {
		return false, errors.New("original face value exceeds accounting precision")
	}
	index := len(detail.Gaps) - 1
	startVersion, changes := financeRechargeReviewVersions(versions, row.HourTs, row.HourTs+seconds)
	added := index < 0 || detail.Gaps[index].ToTs != row.HourTs || detail.Gaps[index].Reason != reason || detail.Gaps[index].BucketSeconds != seconds
	if !added {
		previous := detail.Gaps[index]
		// A merged range must not hide a recorded boundary or an invalid
		// version replacement, even when both buckets share the same reason.
		added = len(previous.RecordedChanges) > 0 || len(changes) > 0 || !sameFinanceRechargeReviewVersion(previous.StartVersion, startVersion)
	}
	if added {
		detail.Gaps = append(detail.Gaps, FinanceRechargeGapRange{FromTs: row.HourTs, BucketSeconds: seconds, Reason: reason, FaceValueMicroUnits: "0",
			ReviewAction: financeRechargeReviewAction(reason, len(changes) > 0), StartVersion: startVersion, RecordedChanges: changes})
		index++
	}
	gap := &detail.Gaps[index]
	value, err := strconv.ParseInt(gap.FaceValueMicroUnits, 10, 64)
	if err != nil || addEconomicsInt64(&value, face) != nil {
		return false, errors.New("gap face-value total exceeds accounting precision")
	}
	gap.FaceValueMicroUnits = strconv.FormatInt(value, 10)
	gap.ToTs = row.HourTs + seconds
	gap.Buckets++
	if row.CostUSD > 0 {
		gap.NonzeroCostBuckets++
	}
	return added, nil
}
