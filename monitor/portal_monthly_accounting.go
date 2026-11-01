package monitor

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// Private, operator-reviewed evidence. References must point to retained,
// independently reconciled wallet boundaries and complete money movements.
// A reference is an audit requirement, not automatic proof of its contents.
// Never manufacture these attestations from the same consumption-log totals.
// Customer JSON/XLSX contains only actual ConsumeQuota/RefundQuota, not these
// internal references or the log-versus-wallet discrepancy.
type MonthlyBillAccountingEvidence struct {
	Basis                string `json:"basis"`
	OpeningSource        string `json:"opening_source"`
	ClosingSource        string `json:"closing_source"`
	MovementSource       string `json:"movement_source"`
	RefundSource         string `json:"refund_source"`
	ReconciliationSource string `json:"reconciliation_source"`
	LogConsumeQuota      int64  `json:"log_consume_quota"`
	LogRefundQuota       int64  `json:"log_refund_quota"`
}

func validateMonthlyAccountingEvidence(a *MonthlyBillAccountingEvidence) error {
	if a == nil || a.Basis != "independent_wallet_reconciliation" || a.LogConsumeQuota < 0 || a.LogRefundQuota < 0 {
		return errors.New("缺少有效的独立余额核验及日志对照证据，不能按日志金额代替实际扣费")
	}
	for _, ref := range []string{a.OpeningSource, a.ClosingSource, a.MovementSource, a.RefundSource, a.ReconciliationSource} {
		if strings.TrimSpace(ref) == "" || len(ref) > 1024 {
			return errors.New("实际扣费证据须包含期初、期末、资金变动、退款与月度核验来源")
		}
	}
	return nil
}

// Shared by the list, detail reader and download gate. Validate even a correctly
// hashed stored payload: a checksum alone does not establish completeness.
// This checks exact arithmetic, not the truth of the independent source proofs;
// the latter remain a prerequisite of publication. Never infer a missing balance.
func monthlyStatementAmountsComplete(p monthlyStatement) bool {
	return monthlyStatementMoneyComplete(p) && monthlyMembersComplete(p) && monthlyDisplayBalancesValid(p)
}

func monthlyStatementMoneyComplete(p monthlyStatement) bool {
	c, s := p.Credits, p.Consumption
	if p.MonthOpeningBalance == nil || p.MonthEndBalance == nil || !c.Available || c.Unknown != 0 || c.RechargeCount == nil || *c.RechargeCount < 0 || c.RechargeTotal == nil || c.GiftTotal == nil || len(c.Rows) > monthlyCreditLimit || s == nil {
		return false
	}
	if !monthlyStatisticsValid(*s) {
		return false
	}
	var opening, closing, recharge, gift, total, adjustment, consume, refund, net int64
	for _, field := range []struct {
		text  string
		quota *int64
	}{
		{*p.MonthOpeningBalance, &opening}, {*p.MonthEndBalance, &closing},
		{*c.RechargeTotal, &recharge}, {*c.GiftTotal, &gift}, {c.KnownTotal, &total},
		{c.Adjustment, &adjustment}, {s.Amount, &consume}, {s.Refund, &refund}, {s.Net, &net},
	} {
		if len(field.text) > 30 {
			return false
		}
		q, ok := newMonthlyMoney(field.text)
		if !ok {
			return false
		}
		*field.quota = q
	}
	// Negative wallet balances and a negative net (refunds of an earlier month)
	// are valid; unknown amounts, negative gross charges and positive debits aren't.
	if recharge < 0 || gift < 0 || adjustment > 0 || consume < 0 || refund < 0 {
		return false
	}
	correction, ok := monthlyCorrectionMoney(c.BalanceCorrection)
	if !ok {
		return false
	}
	known, charged := recharge, consume
	if addEconomicsInt64(&known, gift) != nil || known != total || addEconomicsInt64(&charged, -refund) != nil || charged != net {
		return false
	}
	balance := opening
	for _, q := range []int64{total, adjustment, -consume, refund, correction} {
		if addEconomicsInt64(&balance, q) != nil {
			return false
		}
	}
	if balance != closing {
		return false
	}
	var rowRecharge, rowGift, rowAdjustment int64
	count := 0
	for _, row := range c.Rows {
		if len(row.Amount) > 30 || row.UserID <= 0 {
			return false
		}
		q, ok := newMonthlyMoney(row.Amount)
		if !ok || q == 0 || (row.Kind == "adjustment") != (q < 0) {
			return false
		}
		var target *int64
		switch row.Kind {
		case "recharge":
			target = &rowRecharge
			count++
		case "gift":
			target = &rowGift
		case "adjustment":
			target = &rowAdjustment
		default:
			return false
		}
		if addEconomicsInt64(target, q) != nil {
			return false
		}
	}
	return rowRecharge == recharge && rowGift == gift && rowAdjustment == adjustment && count == *c.RechargeCount
}

// Re-hash the saved body rather than trusting its separately saved hash. This
// is used by local publication/continuity checks, not every customer request.
func readStoredMonthlyEvidence(r PortalMonthlyBill) (MonthlyBillEvidence, error) {
	var e MonthlyBillEvidence
	if len(r.Evidence) == 0 || len(r.Evidence) > 2<<20 || monthlyDigest([]byte(r.Evidence)) != r.EvidenceHash {
		return e, errors.New("已存账单原始证据完整性校验失败")
	}
	e, err := decodeMonthlyBillEvidence([]byte(r.Evidence))
	if err != nil {
		return e, errors.New("已存账单原始证据格式无效")
	}
	ids := make([]int64, 0, len(e.Users))
	seen := make(map[int64]bool, len(e.Users))
	for _, u := range e.Users {
		if u.UserID <= 0 || seen[u.UserID] {
			return e, errors.New("已存账单原始证据成员无效")
		}
		seen[u.UserID] = true
		ids = append(ids, u.UserID)
	}
	if (e.Version < 1 || e.Version > 5) || len(ids) == 0 || e.GroupID != r.GroupID || e.Month != r.Month || portalMemberFingerprintFromIDs(ids) != r.MemberFingerprint {
		return e, errors.New("已存账单与原始证据范围不一致")
	}
	return e, nil
}

// Validate both directions: historical bills may be imported newest-first.
// Match the same user, never merely organization totals that can cancel errors.
// A missing month is not silently filled with zero. Existing publications and
// evidence are never rewritten to force a match.
func validateMonthlyBillContinuity(tx *gorm.DB, e MonthlyBillEvidence, now time.Time) error {
	if err := validateMonthlyHistoricalDecisionReuse(tx, e); err != nil {
		return err
	}
	start, _, err := monthlyWindow(e.Month, now)
	if err != nil {
		return err
	}
	previous, next := start.AddDate(0, -1, 0).Format("2006-01"), start.AddDate(0, 1, 0).Format("2006-01")
	var neighbors []PortalMonthlyBill
	if err := tx.Where("group_id = ? AND month IN ?", e.GroupID, []string{previous, next}).Find(&neighbors).Error; err != nil {
		return err
	}
	current := make(map[int64]MonthlyBillUserEvidence, len(e.Users))
	for _, u := range e.Users {
		current[u.UserID] = u
	}
	for _, r := range neighbors {
		other, err := readStoredMonthlyEvidence(r)
		if err != nil {
			return fmt.Errorf("相邻月份 %s: %w", r.Month, err)
		}
		if _, err := readPublishedMonthlyBill(r); err != nil {
			return fmt.Errorf("相邻月份 %s 账单完整性校验失败", r.Month)
		}
		for _, u := range other.Users {
			v, ok := current[u.UserID]
			if !ok {
				continue // different membership; do not invent another user's balance
			}
			if (r.Month == previous && u.ClosingQuota != v.OpeningQuota) || (r.Month == next && v.ClosingQuota != u.OpeningQuota) {
				return fmt.Errorf("用户 %d 的 %s 与 %s 月界余额不一致，拒绝发布", u.UserID, r.Month, e.Month)
			}
		}
	}
	return nil
}
