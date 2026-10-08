package monitor

// Keep balance presentation and runway assessment on the same freshness
// window. These are derived read-only fields, not new persisted worker state.
func upstreamBalanceFreshnessLimit(syncMinutes int) int64 {
	return int64(max(30, syncMinutes*3) * 60)
}

func upstreamBalanceFresh(lastSuccess, now int64, syncMinutes int) bool {
	return lastSuccess > 0 && lastSuccess <= now && now-lastSuccess <= upstreamBalanceFreshnessLimit(syncMinutes)
}

func decorateUpstreamBalanceHealth(view *ChannelUpstreamAccountView, row ChannelUpstreamAccount, s Settings, now int64) {
	minutes := upstreamSyncMinutes(s)
	view.BalanceWorkerEnabled = s.UpstreamSyncEnabled
	view.BalanceFreshnessLimitSeconds = upstreamBalanceFreshnessLimit(minutes)
	view.BalanceFresh = view.BalanceUSD != nil && upstreamBalanceFresh(row.LastSuccessAt, now, minutes)
	switch {
	case !s.UpstreamSyncEnabled:
		view.BalanceEffectiveStatus = "global_off"
	case !row.Enabled:
		view.BalanceEffectiveStatus = upstreamStatusDisabled
	case row.Status == upstreamStatusReconnect || row.Status == upstreamStatusUnsupported:
		view.BalanceEffectiveStatus = row.Status
	case row.NextSyncAt == upstreamAccountIsolatedUntil:
		view.BalanceEffectiveStatus = "paused"
	case row.Status == upstreamStatusError:
		view.BalanceEffectiveStatus = row.Status
	case view.BalanceUSD == nil:
		view.BalanceEffectiveStatus = "queued"
	case !view.BalanceFresh:
		view.BalanceEffectiveStatus = "stale"
	default:
		view.BalanceEffectiveStatus = row.Status
	}
}
