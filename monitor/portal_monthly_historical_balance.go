package monitor

// Historical recognition is an explicitly accepted business treatment, not
// proof that a missing transaction occurred in the recognition month. It is
// private preparation metadata: no customer request can supply this decision.
import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"

	"gorm.io/gorm"
)

const monthlyHistoricalBalancePolicy = "historical-wallet-recognition-v1"

// Amount gate only, not a decision or publication approval. Keep source
// diagnostics and final validation on exactly the same integer-money limits.
func MonthlyHistoricalBalanceWithinLimits(quota, logNet int64) bool {
	return quota > 0 && quota <= monthlyCorrectionQuotaLimit && logNet > 0 &&
		new(big.Int).Mul(big.NewInt(quota), big.NewInt(10000)).Cmp(big.NewInt(logNet)) <= 0
}

type MonthlyHistoricalBalanceRecognition struct {
	Version          int    `json:"version"`
	UserID           int64  `json:"user_id"`
	GroupID          int64  `json:"group_id"`
	RecognitionMonth string `json:"recognition_month"`
	Quota            int64  `json:"quota"`
	DecisionAt       int64  `json:"decision_at"`
	DecisionSource   string `json:"decision_source"`
	DecisionSHA256   string `json:"decision_sha256"`
}

// The policy decision and its ledger scope survive in private bill evidence.
// Neither these fields nor a guessed incident/cause are customer DTO fields.
type MonthlyBillHistoricalBalance struct {
	Recognition MonthlyHistoricalBalanceRecognition `json:"recognition"`
	LogNetQuota int64                               `json:"log_net_quota"`
	ScopeSHA256 string                              `json:"scope_sha256"`
}

func monthlyHistoricalBalanceJSONShape(raw []byte, required func(map[string]json.RawMessage, ...string) error) error {
	var history map[string]json.RawMessage
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &history) != nil || len(history) != 3 {
		return errors.New("历史余额校正结构无效")
	}
	if err := required(history, "recognition", "log_net_quota", "scope_sha256"); err != nil {
		return err
	}
	var decision map[string]json.RawMessage
	if json.Unmarshal(history["recognition"], &decision) != nil || len(decision) != 8 {
		return errors.New("历史余额处理决定结构无效")
	}
	return required(decision, "version", "user_id", "group_id", "recognition_month", "quota", "decision_at", "decision_source", "decision_sha256")
}

func validateMonthlyHistoricalDecision(h MonthlyHistoricalBalanceRecognition, user, group int64, month string, logNet int64, reviewedAt int64, now time.Time) error {
	_, end, err := monthlyWindow(h.RecognitionMonth, now)
	id, selector, found := strings.Cut(h.DecisionSource, "#")
	if err != nil || len(h.RecognitionMonth) != 7 || end.AddDate(0, -1, 0).Format("2006-01") != h.RecognitionMonth || h.Version != 1 || h.UserID != user || h.GroupID != group ||
		h.UserID <= 0 || h.GroupID <= 0 || month < h.RecognitionMonth || h.Quota <= 0 || h.Quota > monthlyCorrectionQuotaLimit ||
		h.DecisionAt < end.Unix() || h.DecisionAt > reviewedAt || reviewedAt > now.Unix() || !found || strings.TrimSpace(id) == "" || strings.TrimSpace(selector) == "" || len(h.DecisionSource) > 1024 || !validMonthlyPublication(h.DecisionSHA256) {
		return errors.New("历史余额校正缺少有效的逐账号处理决定或超出处理范围")
	}
	if !MonthlyHistoricalBalanceWithinLimits(h.Quota, logNet) {
		return errors.New("历史余额差异超过小额处理上限，不能自动平账")
	}
	return nil
}

func validateMonthlyHistoricalBalance(u MonthlyBillUserEvidence, group int64, month string, reviewedAt int64, now time.Time) error {
	h := u.HistoricalBalance
	if h == nil {
		return nil
	}
	if err := validateMonthlyHistoricalDecision(h.Recognition, u.UserID, group, month, h.LogNetQuota, reviewedAt, now); err != nil {
		return err
	}
	if h.ScopeSHA256 != MonthlyCorrectionScopeSHA256(u, month) || u.Accounting == nil || u.ConsumeQuota != u.Accounting.LogConsumeQuota || u.RefundQuota != u.Accounting.LogRefundQuota {
		return errors.New("历史余额校正与原始消费、退款或账单范围不一致")
	}
	c := u.BalanceCorrection
	if month == h.Recognition.RecognitionMonth {
		if c == nil || c.Version != 1 || c.Quota != h.Recognition.Quota || c.Observations == nil || len(c.Observations) != 0 || new(big.Int).Sub(big.NewInt(u.ConsumeQuota), big.NewInt(u.RefundQuota)).Cmp(big.NewInt(h.LogNetQuota)) != 0 {
			return errors.New("历史余额校正须在确认账期单列一次，不伪造当月独立观察")
		}
	} else if c != nil {
		return errors.New("已确认的历史余额差异只能承接到期初，不能再次校正")
	}
	return nil
}

// Called only after ordinary coverage, event identity, month totals and the
// independent later-snapshot checks have been performed. A decision cannot
// excuse missing sources, an opposite counter gap, or a varying residual.
func monthlyRecognizeHistoricalBalance(a MonthlyPreparationAccount, report MonthlyPreparationAccountReport, r MonthlySnapshotReconstruction, u MonthlyBillUserEvidence, reverse, used *big.Int, now time.Time) (MonthlyBillUserEvidence, error) {
	h := r.HistoricalBalance
	if h == nil {
		return u, errors.New("未选择历史余额处理规则")
	}
	from, to, err := monthlyWindow(h.RecognitionMonth, now)
	if err != nil || from.Unix() < a.Baseline.At {
		return u, errors.New("历史余额确认账期早于完整可重建范围")
	}
	net, err := monthlyPreparationRange(a, from.Unix(), to.Unix())
	if err != nil {
		return u, err
	}
	logNet := new(big.Int).Sub(big.NewInt(net.consume), big.NewInt(net.refund))
	if !logNet.IsInt64() {
		return u, errors.New("历史确认账期金额超出范围")
	}
	validationMonth := r.Month
	if r.SameView != nil && r.Month < h.RecognitionMonth {
		validationMonth = h.RecognitionMonth
	}
	if err := validateMonthlyHistoricalDecision(*h, a.UserID, a.GroupID, validationMonth, logNet.Int64(), now.Unix(), now); err != nil {
		return u, err
	}
	_, monthEnd, _ := monthlyWindow(r.Month, now)
	if r.BridgeDirection != "forward" {
		through := r.ObservedAt
		if r.SameView != nil {
			through = r.SameView.Boundary
		}
		bridge, err := monthlyPreparationRange(a, monthEnd.Unix(), through)
		consume, refund := big.NewInt(bridge.consume), big.NewInt(bridge.refund)
		if r.SameView != nil && r.SameView.TailConsumeQuota != nil && r.SameView.TailRefundQuota != nil {
			consume.Add(consume, big.NewInt(*r.SameView.TailConsumeQuota))
			refund.Add(refund, big.NewInt(*r.SameView.TailRefundQuota))
		}
		if err != nil || consume.Cmp(big.NewInt(*r.BridgeConsumeQuota)) != 0 || refund.Cmp(big.NewInt(*r.BridgeRefundQuota)) != 0 {
			return u, errors.New("历史余额校正缺少完整且一致的余额衔接区间")
		}
	}
	gap := big.NewInt(h.Quota)
	if !reverse.IsInt64() || new(big.Int).Sub(new(big.Int).Set(reverse), big.NewInt(u.ClosingQuota)).Cmp(gap) != 0 || new(big.Int).Sub(new(big.Int).Set(used), big.NewInt(*r.UsedQuota)).Cmp(gap) != 0 {
		return u, errors.New("历史余额与累计用量差额不等于已确认金额，不能以规则掩盖新差异")
	}
	// A selected source must be a retained original balance, not an amount
	// entered only in the reconstruction context to force the equation.
	matched := false
	// Only the native-verified same-view API admits this metadata. Its raw
	// balance, counter, tail and original hashes are re-audited by the caller;
	// do not manufacture an ordinary historical snapshot in preparation input.
	if r.SameView != nil {
		matched = true
	}
	for _, snapshot := range a.Snapshots {
		if snapshot.At == r.ObservedAt && snapshot.Source == r.SnapshotSource && snapshot.BalanceQuota == *r.BalanceQuota && snapshot.UsedQuota == *r.UsedQuota {
			matched = true
		}
	}
	if !matched {
		return u, errors.New("历史余额校正所选余额并非留存的原始快照")
	}
	for _, anchor := range report.Anchors {
		if anchor.At > r.ObservedAt || anchor.TimeBasis != monthlySourceLogCutoff || anchor.Status == "incomplete_sources" {
			continue
		}
		b, bok := new(big.Int).SetString(anchor.BalanceGapQuota, 10)
		c, cok := new(big.Int).SetString(anchor.CounterGapQuota, 10)
		if !bok || !cok || new(big.Int).Add(new(big.Int).Set(b), c).Sign() != 0 || b.Sign() < 0 || b.Cmp(gap) > 0 ||
			(anchor.At <= from.Unix() && b.Sign() != 0) || (anchor.At >= to.Unix() && b.Cmp(gap) != 0) {
			return u, errors.New("历史处理决定与其他已核验截点冲突，不能覆盖新差异")
		}
	}
	if r.Month < h.RecognitionMonth {
		// This business credit is not booked yet. Remove the future recognition
		// from the reverse projection, not from original wallet/log observations.
		// No correction or historical decision is published in this earlier bill.
		reverse.Sub(reverse, gap)
		return u, nil
	}
	if r.Month > h.RecognitionMonth {
		if err := addEconomicsInt64(&u.OpeningQuota, h.Quota); err != nil {
			return u, err
		}
	} else {
		u.BalanceCorrection = &MonthlyBillBalanceCorrection{Version: 1, Quota: h.Quota, Observations: []MonthlyBillCorrectionObservation{}}
	}
	if err := addEconomicsInt64(&u.ClosingQuota, h.Quota); err != nil {
		return u, err
	}
	copyDecision := *h
	u.HistoricalBalance = &MonthlyBillHistoricalBalance{Recognition: copyDecision, LogNetQuota: logNet.Int64()}
	return u, nil
}

// Bound and stream private evidence only at publication, never on a customer
// read. One accepted legacy difference must not be rebooked under a different
// month/decision even when an intermediate month's bill has not been generated.
func validateMonthlyHistoricalDecisionReuse(tx *gorm.DB, e MonthlyBillEvidence) error {
	decisions := map[int64]MonthlyHistoricalBalanceRecognition{}
	for _, u := range e.Users {
		if u.HistoricalBalance != nil {
			decisions[u.UserID] = u.HistoricalBalance.Recognition
		}
	}
	if len(decisions) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(decisions))
	for id := range decisions {
		ids = append(ids, id)
	}
	// Account identity, not merely organization identity, owns a correction.
	// JSON filtering avoids reading every unrelated organization's evidence.
	rows, err := tx.Model(&PortalMonthlyBill{}).Select("group_id, month, evidence, evidence_hash, member_fingerprint").
		Where("NOT (group_id = ? AND month = ?)", e.GroupID, e.Month).
		Where("EXISTS (SELECT 1 FROM json_each(portal_monthly_bills.evidence, '$.users') AS member WHERE json_extract(member.value, '$.user_id') IN ? AND json_type(member.value, '$.historical_balance') = 'object')", ids).
		Order("month").Limit(121).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		if count > 120 {
			return errors.New("历史余额处理范围超过核验预算，请分批复核")
		}
		var record PortalMonthlyBill
		if err := tx.ScanRows(rows, &record); err != nil {
			return err
		}
		old, err := readStoredMonthlyEvidence(record)
		if err != nil {
			return err
		}
		for _, user := range old.Users {
			decision, exists := decisions[user.UserID]
			if exists && user.HistoricalBalance != nil && decision != user.HistoricalBalance.Recognition {
				return errors.New("同一账号已有不同的历史余额处理决定，拒绝重复归期或重复校正")
			}
		}
	}
	return rows.Err()
}
