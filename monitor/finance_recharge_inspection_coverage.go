package monitor

import (
	"context"
	"errors"
	"math"
	"sort"
)

const financeRechargeUnsupportedInterval = "bill_interval_unsupported"

// Bill coverage is independent of recharge-term gaps. A source with valid
// terms but missing bills must not disappear from a diagnostic. RecordedHours
// is the projection's observed whole hours, not proof of finality on its own.
type FinanceRechargeBillCoverage struct {
	Domain        string `json:"domain"`
	Status        string `json:"status"`
	ExpectedHours int64  `json:"expected_hours"`
	RecordedHours int64  `json:"recorded_hours"`
	Complete      bool   `json:"complete"`
	Provisional   bool   `json:"provisional"`
	DataUntil     int64  `json:"data_until"`
}

// Nonstandard intervals may be legitimate partial upstream observations, but
// this repair tool cannot reprice them. Isolate the WHOLE source, not just the
// unusual row, so accepted peers remain inspectable without a partial repair.
// Impossible/overflowing timestamps still reject the entire malformed input.
func financeRechargeInspectionRows(ctx context.Context, rows []ChannelUpstreamUsageHour, accounts map[string]ChannelUpstreamAccountView) ([]ChannelUpstreamUsageHour, map[string]bool, error) {
	unsupported := make(map[string]bool)
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if account, ok := accounts[row.Domain]; !ok || account.Provider != row.Provider {
			continue
		}
		seconds := row.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		if row.HourTs < 0 || row.HourTs > math.MaxInt64-seconds {
			return nil, nil, errors.New("recharge inspection found an invalid original bill interval")
		}
		if seconds != 3600 && seconds != 86400 {
			unsupported[row.Domain] = true
		}
	}
	if len(unsupported) == 0 {
		return rows, unsupported, nil
	}
	filtered := make([]ChannelUpstreamUsageHour, 0, len(rows))
	for _, row := range rows {
		if !unsupported[row.Domain] {
			filtered = append(filtered, row)
		}
	}
	return filtered, unsupported, nil
}

func financeRechargeInspectionCoverage(metrics map[string]ChannelUpstreamUsageMetrics) []FinanceRechargeBillCoverage {
	coverage := make([]FinanceRechargeBillCoverage, 0, len(metrics))
	for domain, metric := range metrics {
		status := "complete"
		switch {
		case !metric.Available:
			status = "bill_missing"
		case metric.IntegrityStatus != upstreamUsageIntegrityComplete:
			status = metric.IntegrityStatus
		case metric.Provisional:
			status = "bill_provisional"
		case !metric.Complete:
			status = "bill_incomplete"
		}
		coverage = append(coverage, FinanceRechargeBillCoverage{Domain: domain, Status: status,
			ExpectedHours: metric.ExpectedHours, RecordedHours: metric.CompletedHours,
			Complete: metric.Complete, Provisional: metric.Provisional, DataUntil: metric.DataUntil})
	}
	sort.Slice(coverage, func(i, j int) bool { return coverage[i].Domain < coverage[j].Domain })
	return coverage
}
