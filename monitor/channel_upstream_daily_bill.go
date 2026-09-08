package monitor

import "context"

// ChannelUpstreamNaturalDayBill is a separately labelled source bill, never a
// replacement for the exact report interval. Daily-only providers cannot split
// a bill at an arbitrary hour. Keep their money visible without silently adding
// it to hourly totals or changing the user-side revenue window.
type ChannelUpstreamNaturalDayBill struct {
	FromTs int64                       `json:"from_ts"`
	ToTs   int64                       `json:"to_ts"`
	Usage  ChannelUpstreamUsageMetrics `json:"usage"`
}

func naturalDayBillingScope(scope stabilityScope, now int64) stabilityScope {
	if scope.FromTs >= scope.ToTs || scope.FromTs >= now {
		return stabilityScope{}
	}
	// Query windows are half-open: midnight at the end must not include the
	// following day. Only today's bill may end before a complete natural day.
	return stabilityScope{
		FromTs: cstDayStart(scope.FromTs),
		ToTs:   min(cstDayStart(scope.ToTs-1)+86400, now),
	}
}

func (m *Monitor) loadChannelUpstreamNaturalDayBills(ctx context.Context, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot) (map[string]*ChannelUpstreamNaturalDayBill, error) {
	daily := make(map[string]ChannelUpstreamAccountView)
	for domain, account := range accounts {
		granularity := account.UsageGranularity
		if granularity == "" {
			granularity = upstreamUsageGranularity(account.Provider, account.UsageAdapter)
		}
		if account.UsageSyncEnabled && (granularity == "day" || account.Provider == upstreamProviderAICodeWith) {
			daily[domain] = account
		}
	}
	result := make(map[string]*ChannelUpstreamNaturalDayBill, len(daily))
	billScope := naturalDayBillingScope(scope, now)
	if len(daily) == 0 || billScope.FromTs >= billScope.ToTs {
		return result, nil
	}
	// Reuse all monetary, overlap and historical recharge-version checks.
	// This reads Monitor's local summaries only; it never contacts an upstream.
	usage, err := m.loadChannelUpstreamUsageWindow(ctx, billScope, now, daily, finance)
	if err != nil {
		return nil, err
	}
	for domain, metrics := range usage {
		// The calendar window itself is representable. An absent current-day
		// bucket is a real missing bill, not another unsupported hour selection.
		if !metrics.Available && metrics.IntegrityStatus == upstreamUsageIntegrityWindowMismatch {
			metrics.IntegrityStatus = ""
		}
		result[domain] = &ChannelUpstreamNaturalDayBill{FromTs: billScope.FromTs, ToTs: billScope.ToTs, Usage: metrics}
	}
	return result, nil
}
