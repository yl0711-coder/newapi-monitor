package monthlysource

// UsageObservationFromToken projects only the independently queried user totals
// and wallet from the SAME actual source transaction. It performs no query and
// creates no new observation time. The caller must retain the token original;
// this projection is not an additional independent capture or bill approval.
func UsageObservationFromToken(o TokenUsageWindowObservation) (UsageWindowObservation, error) {
	if err := ValidateTokenUsageWindowObservation(o); err != nil {
		return UsageWindowObservation{}, err
	}
	wallet := o.Wallet
	if wallet.BalanceQuota != nil {
		value := *wallet.BalanceQuota
		wallet.BalanceQuota = &value
	}
	if wallet.UsedQuota != nil {
		value := *wallet.UsedQuota
		wallet.UsedQuota = &value
	}
	rows := make([]UsageWindowRow, len(o.Totals))
	copy(rows, o.Totals)
	u := UsageWindowObservation{Version: o.Version, Basis: usageWindowBasis, Request: o.Request,
		StartedAtMS: o.StartedAtMS, CompletedAtMS: o.CompletedAtMS, Wallet: wallet, Rows: rows}
	if err := ValidateUsageWindowObservation(u); err != nil {
		return UsageWindowObservation{}, err
	}
	return u, nil
}
