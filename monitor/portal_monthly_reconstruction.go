package monitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"strings"
	"time"
)

func monthlyReconstructionContextShape(raw []byte) error {
	var root struct {
		Accounts []map[string]json.RawMessage `json:"accounts"`
	}
	if err := json.Unmarshal(raw, &root); err != nil {
		return err
	}
	for _, account := range root.Accounts {
		for key, value := range account {
			if !strings.EqualFold(key, "reconstructions") {
				continue
			}
			if key != "reconstructions" || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return errors.New("历史重建选择不得为空或使用大小写别名")
			}
			var rows []map[string]json.RawMessage
			if err := json.Unmarshal(value, &rows); err != nil {
				return err
			}
			for _, row := range rows {
				if err := monthlySameViewShape(row); err != nil {
					return err
				}
				for key, value := range row {
					if strings.EqualFold(key, "bridge_direction") {
						var direction string
						if key != "bridge_direction" || json.Unmarshal(value, &direction) != nil || direction != "forward" {
							return errors.New("余额衔接方向须明确为 forward；原反推方式应省略该字段")
						}
					}
					if strings.EqualFold(key, "historical_balance") {
						if key != "historical_balance" || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
							return errors.New("历史余额处理决定不得为空或使用大小写别名")
						}
						var decision map[string]json.RawMessage
						if json.Unmarshal(value, &decision) != nil || len(decision) != 8 {
							return errors.New("历史余额处理决定结构无效")
						}
						for _, field := range strings.Fields("version user_id group_id recognition_month quota decision_at decision_source decision_sha256") {
							if value, ok := decision[field]; !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
								return errors.New("历史余额处理决定字段缺失或为空")
							}
						}
					}
				}
				for _, field := range strings.Fields("user_id month observed_at snapshot_source scope_source month_source bridge_source funding_source opening_basis_source balance_quota used_quota month_consume_quota month_refund_quota bridge_consume_quota bridge_refund_quota funding_event_ids") {
					if value, ok := row[field]; !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
						return errors.New("历史重建字段缺失、为空或使用大小写别名")
					}
				}
			}
		}
	}
	return nil
}

// Private, explicitly selected evidence from one retained database view. The
// bridge ends at that view's visibility boundary, NOT at ObservedAt seconds.
// ScopeSource must document capture/query semantics and complete visibility;
// file hashes establish identity, not the truth of that operator attestation.
// No adjustments or editable opening/closing balances are accepted here.
type MonthlySnapshotReconstruction struct {
	UserID             int64  `json:"user_id"`
	Month              string `json:"month"`
	ObservedAt         int64  `json:"observed_at"`
	SnapshotSource     string `json:"snapshot_source"`
	ScopeSource        string `json:"scope_source"`
	MonthSource        string `json:"month_source"`
	BridgeSource       string `json:"bridge_source"`
	FundingSource      string `json:"funding_source"`
	OpeningBasisSource string `json:"opening_basis_source"`
	BalanceQuota       *int64 `json:"balance_quota"`
	UsedQuota          *int64 `json:"used_quota"`
	MonthConsumeQuota  *int64 `json:"month_consume_quota"`
	MonthRefundQuota   *int64 `json:"month_refund_quota"`
	BridgeConsumeQuota *int64 `json:"bridge_consume_quota"`
	BridgeRefundQuota  *int64 `json:"bridge_refund_quota"`
	// Explicit complete funding-event set visible in the independent capture,
	// from the preparation baseline through the observation. No inferred credits.
	FundingEventIDs []int64 `json:"funding_event_ids"`
	// Explicitly accepted historical recognition; omitted retains the strict,
	// original no-adjustment reconstruction path unchanged.
	HistoricalBalance *MonthlyHistoricalBalanceRecognition `json:"historical_balance,omitempty"`
	// Opt-in historical compatibility only. A retained observation inside the
	// closed month is carried forward by independently retained tail usage.
	// Without an accepted historical difference, require the exact tail in the
	// preparation spans and a matching original snapshot; never infer a tail
	// from the desired ending balance or a counter residual.
	// Omitted keeps the existing later-snapshot reverse reconstruction unchanged.
	BridgeDirection string `json:"bridge_direction,omitempty"`
	// Explicitly reviewed same-transaction view. Its visible tail is NOT a
	// complete wall-clock second and never becomes a preparation coverage span.
	SameView *MonthlySameViewSnapshot `json:"same_view,omitempty"`
}

func monthlyReconstructionValid(r MonthlySnapshotReconstruction, a MonthlyPreparationAccount, now time.Time, sources map[string]bool) error {
	start, end, err := monthlyWindow(r.Month, now)
	if err != nil || r.UserID != a.UserID || r.ObservedAt > now.Unix() || r.FundingEventIDs == nil || len(r.FundingEventIDs) > 25000 {
		return errors.New("历史重建范围或资金事件集合无效")
	}
	if (r.BridgeDirection == "" && r.ObservedAt < end.Unix()) ||
		(r.BridgeDirection == "forward" && (r.ObservedAt < start.Unix() || r.ObservedAt >= end.Unix())) ||
		(r.BridgeDirection != "" && r.BridgeDirection != "forward") {
		return errors.New("历史重建余额衔接方向、处理决定或观察范围无效")
	}
	if err := monthlySameViewValid(r, end.Unix(), sources, now); err != nil {
		return err
	}
	for _, ref := range []string{r.SnapshotSource, r.ScopeSource, r.MonthSource, r.BridgeSource, r.FundingSource, r.OpeningBasisSource} {
		_, selector, found := strings.Cut(ref, "#")
		if !monthlyBundleRef(ref, sources) || !found || strings.TrimSpace(selector) == "" {
			return errors.New("历史重建缺少留存来源")
		}
	}
	if r.BalanceQuota == nil {
		return errors.New("历史重建缺少独立余额")
	}
	for _, q := range []*int64{r.UsedQuota, r.MonthConsumeQuota, r.MonthRefundQuota, r.BridgeConsumeQuota, r.BridgeRefundQuota} {
		if q == nil || *q < 0 {
			return errors.New("历史重建金额缺失或无效；未知不按零处理")
		}
	}
	for _, wallet := range a.WalletMonths {
		if wallet.Month == r.Month {
			return errors.New("同月不能同时选择精确钱包与历史重建")
		}
	}
	return nil
}

// Deliberately no fallback: once selected, mismatching reconstruction evidence
// cannot be ignored in favor of another matching legacy observation.
func monthlyBundleReconstructedUser(a MonthlyPreparationAccount, report MonthlyPreparationAccountReport, r MonthlySnapshotReconstruction, u MonthlyBillUserEvidence, sources map[string]bool, now time.Time) (MonthlyBillUserEvidence, error) {
	if err := monthlyReconstructionValid(r, a, now, sources); err != nil {
		return u, err
	}
	start, end, _ := monthlyWindow(r.Month, now)
	sort.Slice(a.Spans, func(i, j int) bool { return a.Spans[i].From < a.Spans[j].From })
	sort.Slice(a.MovementCoverage, func(i, j int) bool { return a.MovementCoverage[i].From < a.MovementCoverage[j].From })
	before, err := monthlyPreparationRange(a, a.Baseline.At, start.Unix())
	if err != nil {
		return u, err
	}
	month, err := monthlyPreparationRange(a, start.Unix(), end.Unix())
	if err != nil {
		return u, err
	}
	month, err = monthlyResolveMonthStatistics(a, r.Month, month)
	if err != nil {
		return u, err
	}
	if month.consume != *r.MonthConsumeQuota || month.refund != *r.MonthRefundQuota {
		return u, errors.New("独立截面中的本月消费或退款与原日志不一致")
	}
	// An explicitly reviewed earlier cutoff is contrary evidence, unlike an
	// unrelated active collection observation. Never erase it by reconstruction.
	for _, snapshot := range a.Snapshots {
		if snapshot.At > r.ObservedAt || !snapshot.hasReviewedCutoff() || r.HistoricalBalance != nil {
			continue
		}
		for _, anchor := range report.Anchors {
			if anchor.At == snapshot.At && anchor.Status != "matched" {
				return u, errors.New("历史重建与已核验截点冲突或缺少衔接证据")
			}
		}
	}
	ids := map[int64]bool{}
	for _, id := range r.FundingEventIDs {
		if id <= 0 || ids[id] {
			return u, errors.New("历史重建资金事件重复或无效")
		}
		ids[id] = true
	}
	bridgeFunding := new(big.Int)
	viewIDs := map[int64]bool{}
	fundingThrough := r.ObservedAt
	if r.SameView != nil {
		fundingThrough = r.SameView.Boundary
		for _, m := range r.SameView.Funding {
			viewIDs[m.ID] = true
		}
	}
	if r.BridgeDirection == "forward" {
		if r.HistoricalBalance == nil {
			// Existing accepted-difference inputs retain their separately audited
			// partial-tail contract. The new no-difference path is stricter: the
			// exact observation boundary must be present in retained spans.
			if err := monthlyForwardReconstructionTail(a, r, end.Unix()); err != nil {
				return u, err
			}
		}
		fundingThrough = end.Unix()
	}
	for _, m := range a.Movements {
		if viewIDs[m.ID] {
			return u, errors.New("同视图资金与完整月份重复")
		}
		if r.SameView != nil && m.At >= fundingThrough {
			return u, errors.New("同视图尾部资金不能冒充完整区间流水")
		}
		if m.At >= fundingThrough {
			if ids[m.ID] {
				return u, errors.New("重建资金事件与采集边界冲突，需单独核验")
			}
			continue
		}
		if !ids[m.ID] {
			return u, errors.New("独立资金集合遗漏准备输入中的事件")
		}
		delete(ids, m.ID)
		if (r.BridgeDirection == "forward" && m.At >= r.ObservedAt) || (r.BridgeDirection == "" && m.At >= end.Unix()) {
			bridgeFunding.Add(bridgeFunding, big.NewInt(m.DeltaQuota))
		}
	}
	if len(ids) != 0 {
		return u, errors.New("独立资金集合含未取得的事件")
	}
	if r.SameView != nil {
		for _, m := range r.SameView.Funding {
			bridgeFunding.Add(bridgeFunding, big.NewInt(m.DeltaQuota))
		}
	}
	u.OpeningQuota, err = before.balance(a.Baseline.BalanceQuota)
	if err != nil {
		return u, err
	}
	u.ClosingQuota, err = month.balance(u.OpeningQuota)
	if err != nil {
		return u, err
	}
	reverse := new(big.Int).Sub(big.NewInt(*r.BalanceQuota), bridgeFunding)
	reverse.Add(reverse, big.NewInt(*r.BridgeConsumeQuota))
	reverse.Sub(reverse, big.NewInt(*r.BridgeRefundQuota))
	used := big.NewInt(a.Baseline.UsedQuota)
	for _, q := range []int64{before.consume, -before.refund, month.consume, -month.refund, *r.BridgeConsumeQuota, -*r.BridgeRefundQuota} {
		used.Add(used, big.NewInt(q))
	}
	if r.BridgeDirection == "forward" {
		// Tail amounts are an explicitly reviewed independent source selection,
		// not a proportional split of a day's money/tokens or a balancing residual.
		if *r.BridgeConsumeQuota > month.consume || *r.BridgeRefundQuota > month.refund {
			return u, errors.New("前推衔接消费或退款超过完整账期原始日志")
		}
		reverse.SetInt64(*r.BalanceQuota)
		reverse.Add(reverse, bridgeFunding)
		reverse.Sub(reverse, big.NewInt(*r.BridgeConsumeQuota))
		reverse.Add(reverse, big.NewInt(*r.BridgeRefundQuota))
		used.SetInt64(a.Baseline.UsedQuota)
		for _, q := range []int64{before.consume, -before.refund, month.consume, -month.refund, -*r.BridgeConsumeQuota, *r.BridgeRefundQuota} {
			used.Add(used, big.NewInt(q))
		}
	}
	if r.HistoricalBalance != nil {
		u, err = monthlyRecognizeHistoricalBalance(a, report, r, u, reverse, used, now)
		if err != nil {
			return u, err
		}
	} else if !reverse.IsInt64() || reverse.Int64() != u.ClosingQuota || used.Cmp(big.NewInt(*r.UsedQuota)) != 0 {
		return u, errors.New("历史重建正反向余额或累计用量不一致；不分摊差额")
	}
	u.ReverseClosingQuota, u.ConsumeQuota, u.RefundQuota = reverse.Int64(), month.consume, month.refund
	if month.requestsKnown {
		u.Requests = month.requests
	}
	if month.tokensKnown {
		u.Tokens = month.tokens
	}
	if !month.requestsKnown || !month.tokensKnown {
		u.Statistics = &MonthlyBillStatisticsCoverage{RequestsKnown: month.requestsKnown, TokensKnown: month.tokensKnown}
	}
	u.Accounting.LogConsumeQuota, u.Accounting.LogRefundQuota = month.consume, month.refund
	u.Accounting.OpeningSource = "historical-reconstruction:" + r.OpeningBasisSource
	u.Accounting.ClosingSource = "historical-reconstruction:" + r.SnapshotSource
	if r.HistoricalBalance != nil && r.Month < r.HistoricalBalance.RecognitionMonth {
		u.Accounting.ClosingSource += ":future-policy-credit-excluded"
	}
	if u.HistoricalBalance != nil {
		u.HistoricalBalance.ScopeSHA256 = MonthlyCorrectionScopeSHA256(u, r.Month)
	}
	// ReconciliationSource already binds the exact context bytes, including all
	// independent amounts, event IDs and source references. No proof is discarded.
	return u, nil
}

func monthlyForwardReconstructionTail(a MonthlyPreparationAccount, r MonthlySnapshotReconstruction, end int64) error {
	tail, err := monthlyPreparationRange(a, r.ObservedAt, end)
	if err != nil || tail.consume != *r.BridgeConsumeQuota || tail.refund != *r.BridgeRefundQuota {
		return errors.New("前推重建缺少精确观察边界后的消费、退款或资金覆盖；不能按余额残差推定")
	}
	for _, snapshot := range a.Snapshots {
		if snapshot.At == r.ObservedAt && snapshot.Source == r.SnapshotSource && snapshot.UserID == a.UserID &&
			snapshot.BalanceQuota == *r.BalanceQuota && snapshot.UsedQuota == *r.UsedQuota {
			return nil
		}
	}
	return errors.New("前推重建所选余额与累计用量并非留存的原始快照")
}
