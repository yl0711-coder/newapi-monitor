package monitor

import (
	"math/big"
	"time"
)

// Explicit per-account, per-month ACTUAL refund evidence, including an explicit
// zero. Wallet boundary deltas establish net debit, not gross debit/refunds.
// A complete log range is a comparison source and cannot by itself attest that
// a logged refund actually reached the wallet. Sources are checked by the CLI
// and reviewed by the existing publication workflow, not a customer assertion.
type MonthlyPreparationWalletMonth struct {
	UserID      int64  `json:"user_id"`
	Month       string `json:"month"`
	RefundQuota int64  `json:"refund_quota"`
	Source      string `json:"source"`
}

const monthlyExactWalletBasis = "exact_wallet_month_boundaries"

// Return true only when the input explicitly selects the exact-wallet path for
// this month. Invalid selected evidence must not silently fall back to a later
// log-matching anchor. Nothing is prorated, imported or written by this helper.
func monthlyPreparationWalletRow(a MonthlyPreparationAccount, v monthlyPreparationTotals, start, end time.Time, row *MonthlyPreparationRow) bool {
	var wallet *MonthlyPreparationWalletMonth
	for i := range a.WalletMonths {
		if a.WalletMonths[i].Month == row.Month {
			wallet = &a.WalletMonths[i]
			break
		}
	}
	if wallet == nil {
		return false
	}
	row.Status, row.Reason = "incomplete_wallet_sources", "实际扣费制单需要精确月初、月末余额及累计用量依据；不按邻近日快照估算"
	opening, closing := a.Baseline, MonthlyPreparationBalance{}
	foundOpening, foundClosing := a.Baseline.At == start.Unix(), false
	for _, snapshot := range a.Snapshots {
		if snapshot.At == start.Unix() {
			opening, foundOpening = snapshot, true
		}
		if snapshot.At == end.Unix() {
			closing, foundClosing = snapshot, true
		}
	}
	if !foundOpening || !foundClosing || !opening.hasReviewedCutoff() || !closing.hasReviewedCutoff() {
		return true
	}
	refs := map[string]bool{opening.Source: true, closing.Source: true, opening.CutoffSource: true, closing.CutoffSource: true, wallet.Source: true, a.BasisSource: true}
	for ref := range v.sources {
		refs[ref] = true
	}
	row.Sources = monthlyPreparationRefs(refs)
	// Use exact arithmetic, including signed balances and transfers. A wallet
	// net debit is accepted only if the independent cumulative counter delta
	// agrees; an arbitrary residual is NOT automatically called consumption.
	net := new(big.Int).SetInt64(opening.BalanceQuota)
	net.Add(net, big.NewInt(v.increase))
	net.Add(net, big.NewInt(v.decrease))
	net.Sub(net, big.NewInt(closing.BalanceQuota))
	counter := new(big.Int).Sub(big.NewInt(closing.UsedQuota), big.NewInt(opening.UsedQuota))
	if !net.IsInt64() || net.Cmp(counter) != 0 {
		row.Status, row.Reason = "wallet_counter_mismatch", "本月余额变动与累计实际用量不一致，需内部核对；未将差额转为客户消费"
		return true
	}
	gross := new(big.Int).Add(new(big.Int).Set(net), big.NewInt(wallet.RefundQuota))
	if !gross.IsInt64() || gross.Sign() < 0 {
		row.Status, row.Reason = "invalid_wallet_debit", "实际扣费与退款依据不一致或金额超限"
		return true
	}
	// Do not newly bill an apparent overcharge through this recovery path.
	// A lower logged net may mean incomplete logs, incorrect funding, or an
	// actual overcharge; those cases need separate investigation.
	logNet := new(big.Int).Sub(big.NewInt(v.consume), big.NewInt(v.refund))
	if net.Cmp(logNet) > 0 {
		row.Status, row.Reason = "wallet_debit_above_logs", "实际净扣费高于日志净消费，需内部核对；不自动增加客户账单费用"
		return true
	}
	row.Status, row.Reason = "reconciled_draft", ""
	row.Basis, row.OpeningSource, row.RefundSource = monthlyExactWalletBasis, opening.Source, wallet.Source
	row.AnchorAt, row.AnchorSource = closing.At, closing.Source
	row.Amounts = &MonthlyPreparationAmounts{
		Opening: monthlyQuotaMoney(opening.BalanceQuota), Increase: monthlyQuotaMoney(v.increase), Decrease: monthlyQuotaMoney(v.decrease),
		Consume: monthlyQuotaMoney(gross.Int64()), Refund: monthlyQuotaMoney(wallet.RefundQuota), Closing: monthlyQuotaMoney(closing.BalanceQuota),
	}
	return true
}
