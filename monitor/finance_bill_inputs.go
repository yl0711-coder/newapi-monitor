package monitor

import (
	"context"
	"errors"
)

// These read-only inputs live for one component build, never across requests,
// periods or retries. Persistent caches retain their source-version fences.
type financePeriodSources struct {
	ledger *channelEconomicsReport
	bills  *financeBillInputs
}

type financeBillInputs struct {
	scope    stabilityScope
	accounts map[string]ChannelUpstreamAccountView
	rows     []ChannelUpstreamUsageHour
	versions map[string][]channelRechargeVersion
}

func (m *Monitor) loadFinanceBillInputs(ctx context.Context, scope stabilityScope, relevant map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot) (*financeBillInputs, error) {
	rows, err := m.loadChannelUpstreamUsageRows(ctx, scope)
	if err != nil {
		return nil, err
	}
	versions, err := m.loadChannelRechargeVersions(ctx, relevant, finance)
	if err != nil {
		return nil, err
	}
	return &financeBillInputs{scope: scope, accounts: relevant, rows: rows, versions: versions}, nil
}

func (m *Monitor) financePeriodBills(ctx context.Context, scope stabilityScope, accounts map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot, shared *financeBillInputs) (*financeBillInputs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if shared != nil {
		if shared.scope.FromTs != scope.FromTs || shared.scope.ToTs != scope.ToTs {
			return nil, errors.New("经营核算账单输入区间不匹配")
		}
		return shared, nil
	}
	relevant, err := m.financeRelevantUpstreamAccounts(ctx, scope, accounts)
	if err != nil {
		return nil, err
	}
	return m.loadFinanceBillInputs(ctx, scope, relevant, finance)
}

func (in *financeBillInputs) window(scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView) (map[string]ChannelUpstreamUsageMetrics, map[string]financeAccountBillAmounts, error) {
	if scope.FromTs < in.scope.FromTs || scope.ToTs > in.scope.ToTs || scope.ToTs <= scope.FromTs {
		return nil, nil, errors.New("经营核算账单子区间超出已读取范围")
	}
	rows := in.rows
	if scope.FromTs != in.scope.FromTs || scope.ToTs != in.scope.ToTs {
		// Match loadChannelUpstreamUsageRows exactly, including the first day's
		// overlapping bucket. Dropping or retaining different rows can change
		// a window-mismatch check; never prorate a daily bill into hours.
		rows = make([]ChannelUpstreamUsageHour, 0)
		from := cstDayStart(scope.FromTs)
		for _, row := range in.rows {
			if row.HourTs >= from && row.HourTs < scope.ToTs {
				rows = append(rows, row)
			}
		}
	}
	return projectFinanceBillWindow(rows, scope, now, accounts, in.versions)
}

func financeBillAccountBoundaries(accounts map[string]ChannelUpstreamAccountView) map[string]int64 {
	boundaries := make(map[string]int64, len(accounts))
	for domain, account := range accounts {
		boundaries[domain] = account.FinanceRequiredFrom
	}
	return boundaries
}
