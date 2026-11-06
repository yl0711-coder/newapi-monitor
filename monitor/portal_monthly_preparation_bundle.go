package monitor

// Local preparation only. This file has no database, HTTP, collector or init
// hooks. A candidate deliberately lacks publication/review fields: producing
// files is not an operator's attestation and cannot publish a customer bill.
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"time"
)

type MonthlyBundleContext struct {
	Version     int                           `json:"version"`
	InputSHA256 string                        `json:"input_sha256"`
	Groups      []MonthlyBundleGroup          `json:"groups"`
	Accounts    []MonthlyBundleAccountContext `json:"accounts"`
}

type MonthlyBundleMember struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

type MonthlyBundleGroup struct {
	GroupID int64                 `json:"group_id"`
	Name    string                `json:"name"`
	Source  string                `json:"source"` // retained authorization/member directory
	Months  []string              `json:"months"`
	Members []MonthlyBundleMember `json:"members"`
}

type MonthlyBundleAccountContext struct {
	UserID          int64                           `json:"user_id"`
	WalletOnly      bool                            `json:"wallet_only"`
	WalletSource    string                          `json:"wallet_source"`
	Events          []MonthlyBundleEvent            `json:"events"`
	Registration    *MonthlyBundleRegistration      `json:"registration,omitempty"`
	Reconstructions []MonthlySnapshotReconstruction `json:"reconstructions,omitempty"`
}

type MonthlyBundleEvent struct {
	ID         int64  `json:"id"`
	At         int64  `json:"at"`
	DeltaQuota int64  `json:"delta_quota"`
	Kind       string `json:"kind"`
	Method     string `json:"method"`
	Source     string `json:"source"` // must equal the normalized movement's reference
}

// An absent pre-registration member is not automatically a zero member. These
// references require explicit source review; a matching SHA is not proof that
// a retained query was complete or that an asserted registration date is true.
type MonthlyBundleRegistration struct {
	At                      int64  `json:"at"`
	EventID                 int64  `json:"event_id"`
	Source                  string `json:"source"`
	NoEarlierActivitySource string `json:"no_earlier_activity_source"`
}

type MonthlyBillCandidate struct {
	GroupID            int64                     `json:"group_id"`
	Month              string                    `json:"month"`
	SourceReference    string                    `json:"source_reference"`
	Users              []MonthlyBillUserEvidence `json:"users"`
	ReconstructedUsers []int64                   `json:"reconstructed_users,omitempty"`
}

type MonthlyBundleIssue struct {
	UserID int64  `json:"user_id"`
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

type MonthlyBundleEntry struct {
	GroupID         int64                 `json:"group_id"`
	Account         string                `json:"account"`
	Month           string                `json:"month"`
	Members         []MonthlyBundleMember `json:"members"`
	Status          string                `json:"status"`
	Issues          []MonthlyBundleIssue  `json:"issues"`
	Candidate       *MonthlyBillCandidate `json:"candidate,omitempty"`
	CandidateSHA256 string                `json:"candidate_sha256,omitempty"`
}

type MonthlyPreparationBundle struct {
	Version         int                      `json:"version"`
	Mode            string                   `json:"mode"`
	InputSHA256     string                   `json:"input_sha256"`
	ContextSHA256   string                   `json:"context_sha256"`
	SourceIntegrity string                   `json:"source_integrity"`
	ReadyForReview  int                      `json:"ready_for_review"`
	Blocked         int                      `json:"blocked"`
	Reconciliation  MonthlyPreparationReport `json:"reconciliation"`
	Entries         []MonthlyBundleEntry     `json:"entries"`
}

func decodeMonthlyBundleContext(raw []byte) (MonthlyBundleContext, error) {
	var c MonthlyBundleContext
	if len(raw) > MonthlyPreparationInputLimit {
		return c, errors.New("制单上下文超过 8MiB")
	}
	if err := monthlyUniqueJSONKeys(raw); err != nil {
		return c, err
	}
	if err := monthlyReconstructionContextShape(raw); err != nil {
		return c, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return c, errors.New("制单上下文包含多余内容")
	}
	if c.Version != 1 || !validMonthlyPublication(c.InputSHA256) || len(c.Groups) == 0 || len(c.Groups) > 500 || len(c.Accounts) > 500 {
		return c, errors.New("制单上下文版本、指纹或范围无效")
	}
	return c, nil
}

func monthlyBundleRef(s string, sources map[string]bool) bool {
	id, _, _ := strings.Cut(s, "#")
	return len(s) <= 1024 && sources[id]
}

// Structural scope errors reject the whole envelope, rather than quietly
// selecting a different customer or omitting one of its authorized members.
func validateMonthlyBundleContext(c MonthlyBundleContext, input MonthlyPreparationInput, now time.Time) error {
	sources, accounts := map[string]bool{}, map[int64]MonthlyPreparationAccount{}
	for _, s := range input.Sources {
		sources[s.ID] = true
	}
	for _, a := range input.Accounts {
		accounts[a.UserID] = a
	}
	groups, members, metadata := map[int64]bool{}, map[int64]bool{}, map[int64]bool{}
	monthCount, memberMonths, events := 0, 0, 0
	for _, g := range c.Groups {
		if g.GroupID <= 0 || groups[g.GroupID] || strings.TrimSpace(g.Name) == "" || len(g.Name) > 256 || !monthlyBundleRef(g.Source, sources) || len(g.Months) == 0 || len(g.Months) > 120 || len(g.Members) == 0 || len(g.Members) > maxTrackedUsers {
			return errors.New("制单组织范围、目录来源或名称无效")
		}
		groups[g.GroupID] = true
		months := map[string]bool{}
		for _, month := range g.Months {
			start, _, err := monthlyWindow(month, now)
			if err != nil || len(month) != 7 || month != start.Format("2006-01") || months[month] {
				return errors.New("制单月份无效、重复或尚未结束")
			}
			months[month] = true
		}
		monthCount += len(g.Months)
		memberMonths += len(g.Months) * len(g.Members)
		if monthCount > 2000 || memberMonths > 10000 {
			return errors.New("制单批次超过上限，请按组织或月份分批")
		}
		for _, member := range g.Members {
			if member.UserID <= 0 || members[member.UserID] || strings.TrimSpace(member.Username) == "" || len(member.Username) > 256 {
				return errors.New("制单成员缺失、重复或跨组织重复")
			}
			members[member.UserID] = true
			if a, ok := accounts[member.UserID]; ok && (a.GroupID != g.GroupID || a.Username != member.Username) {
				return errors.New("制单成员与核对输入中的账号归属或名称不一致")
			}
		}
	}
	for _, a := range input.Accounts {
		if groups[a.GroupID] && !members[a.UserID] {
			return errors.New("制单范围遗漏核对输入中同组织的成员；不能生成部分客户账单")
		}
	}
	for _, a := range c.Accounts {
		if a.UserID <= 0 || metadata[a.UserID] || !members[a.UserID] {
			return errors.New("制单账号上下文重复或不在选定成员范围内")
		}
		metadata[a.UserID] = true
		seenMonths := map[string]bool{}
		if len(a.Reconstructions) > 120 {
			return errors.New("历史重建月份数量超限")
		}
		for _, reconstruction := range a.Reconstructions {
			if reconstruction.UserID != a.UserID || seenMonths[reconstruction.Month] {
				return errors.New("历史重建账号不符或月份重复")
			}
			seenMonths[reconstruction.Month] = true
			if h := reconstruction.HistoricalBalance; h != nil {
				id, selector, found := strings.Cut(h.DecisionSource, "#")
				bound := false
				for _, source := range input.Sources {
					bound = bound || (source.ID == id && source.SHA256 == h.DecisionSHA256)
				}
				if !found || strings.TrimSpace(selector) == "" || !bound {
					return errors.New("历史余额处理决定未绑定独立留存来源及其指纹")
				}
			}
			accountMonth := false
			for _, month := range accounts[a.UserID].Months {
				if month == reconstruction.Month {
					accountMonth = true
				}
			}
			if !accountMonth {
				return errors.New("历史重建月份不在账号准备范围")
			}
			found := false
			for _, group := range c.Groups {
				if group.GroupID != accounts[a.UserID].GroupID {
					continue
				}
				for _, month := range group.Months {
					if month == reconstruction.Month {
						found = true
					}
				}
			}
			if !found {
				return errors.New("历史重建月份不在选定范围")
			}
		}
		events += len(a.Events)
		if events > 25000 {
			return errors.New("制单事件上下文超过上限，请分批")
		}
	}
	return nil
}

func monthlyBundleEvents(a MonthlyPreparationAccount, meta MonthlyBundleAccountContext, from, to int64) ([]MonthlyBillCreditEvidence, error) {
	byID := map[int64]MonthlyBundleEvent{}
	for _, event := range meta.Events {
		if _, exists := byID[event.ID]; exists || event.ID <= 0 {
			return nil, errors.New("额度事件上下文 ID 无效或重复")
		}
		byID[event.ID] = event
	}
	matched := map[int64]bool{}
	result := []MonthlyBillCreditEvidence{}
	for _, movement := range a.Movements {
		if movement.At < from || movement.At >= to {
			continue
		}
		event, ok := byID[movement.ID]
		if !ok || event.At != movement.At || event.DeltaQuota != movement.DeltaQuota || event.Source != movement.Source {
			return nil, fmt.Errorf("额度事件 %d 缺少方式依据或与原始额度变动不一致", movement.ID)
		}
		matched[event.ID] = true
		result = append(result, MonthlyBillCreditEvidence{ID: event.ID, At: event.At, DeltaQuota: event.DeltaQuota, Kind: event.Kind, Method: event.Method})
	}
	for _, event := range meta.Events {
		if event.At >= from && event.At < to && !matched[event.ID] {
			return nil, fmt.Errorf("额度事件 %d 不存在于该账号的已核对记录中", event.ID)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].At == result[j].At {
			return result[i].ID < result[j].ID
		}
		return result[i].At < result[j].At
	})
	return result, nil
}

func monthlyBundleBeforeRegistration(a MonthlyPreparationAccount, meta MonthlyBundleAccountContext, end int64, sources map[string]bool) bool {
	r := meta.Registration
	if r == nil || r.At < end || end > a.Baseline.At || a.Baseline.BalanceQuota != 0 || a.Baseline.UsedQuota != 0 || !monthlyBundleRef(r.Source, sources) || !monthlyBundleRef(r.NoEarlierActivitySource, sources) {
		return false
	}
	matched := false
	for _, movement := range a.Movements {
		if movement.At < r.At {
			return false
		}
		if movement.ID == r.EventID && movement.At == r.At && movement.DeltaQuota > 0 {
			for _, e := range meta.Events {
				if e.ID == movement.ID && e.At == movement.At && e.DeltaQuota == movement.DeltaQuota && e.Source == movement.Source && e.Kind == "gift" && e.Method == "注册赠送" {
					matched = true
				}
			}
		}
	}
	return matched
}

// Convert only our freshly recomputed report, not a supplied report or manually
// entered balances. Reverse closing is reconstructed again from the chosen
// independent snapshot and its exact bridge, using the existing range logic.
func monthlyBundleUser(a MonthlyPreparationAccount, report MonthlyPreparationAccountReport, meta MonthlyBundleAccountContext, month, binding string, sources map[string]bool, now time.Time) (MonthlyBillUserEvidence, error) {
	u := MonthlyBillUserEvidence{UserID: a.UserID, Complete: true, WalletOnly: true, Reversals: []MonthlyBillReversal{}}
	start, end, err := monthlyWindow(month, now)
	if err != nil {
		return u, err
	}
	u.Through = end.Unix()
	if report.Error != "" {
		return u, errors.New(report.Error)
	}
	if meta.UserID != a.UserID || !meta.WalletOnly || !monthlyBundleRef(meta.WalletSource, sources) || meta.Events == nil {
		return u, errors.New("缺少逐账号钱包范围或额度事件审阅依据")
	}
	u.Events, err = monthlyBundleEvents(a, meta, start.Unix(), end.Unix())
	if err != nil {
		return u, err
	}
	u.ExpectedCreditEvents = len(u.Events)
	u.Accounting = &MonthlyBillAccountingEvidence{Basis: "independent_wallet_reconciliation", OpeningSource: a.Baseline.Source, MovementSource: binding + "/movement_coverage", RefundSource: a.BasisSource, ReconciliationSource: binding}
	for _, reconstruction := range meta.Reconstructions {
		if reconstruction.Month == month {
			return monthlyBundleReconstructedUser(a, report, reconstruction, u, sources, now)
		}
	}
	if monthlyBundleBeforeRegistration(a, meta, end.Unix(), sources) {
		if len(u.Events) != 0 {
			return u, errors.New("注册前月份意外含额度事件")
		}
		u.Accounting.OpeningSource = meta.Registration.NoEarlierActivitySource
		u.Accounting.ClosingSource = meta.Registration.Source
		return u, nil
	}
	var row *MonthlyPreparationRow
	for i := range report.Months {
		if report.Months[i].Month == month {
			row = &report.Months[i]
			break
		}
	}
	if row == nil {
		return u, errors.New("核对输入未包含该账号月份；未按零处理")
	}
	if row.Status != "reconciled_draft" || row.Amounts == nil {
		return u, fmt.Errorf("%s: %s", row.Status, row.Reason)
	}
	for _, amount := range []struct {
		text string
		q    *int64
	}{{row.Amounts.Opening, &u.OpeningQuota}, {row.Amounts.Closing, &u.ClosingQuota}, {row.Amounts.Consume, &u.ConsumeQuota}, {row.Amounts.Refund, &u.RefundQuota}, {row.LogConsume, &u.Accounting.LogConsumeQuota}, {row.LogRefund, &u.Accounting.LogRefundQuota}} {
		q, ok := newMonthlyMoney(amount.text)
		if !ok {
			return u, errors.New("核对金额不能精确转换为 quota")
		}
		*amount.q = q
	}
	if row.Requests != nil {
		u.Requests = *row.Requests
	}
	if row.Tokens != nil {
		u.Tokens = *row.Tokens
	}
	if row.Requests == nil || row.Tokens == nil {
		u.Statistics = &MonthlyBillStatisticsCoverage{RequestsKnown: row.Requests != nil, TokensKnown: row.Tokens != nil}
	}
	if row.Basis == monthlyExactWalletBasis {
		u.Accounting.OpeningSource = row.OpeningSource
		u.Accounting.RefundSource = row.RefundSource
	}
	u.Accounting.ClosingSource = row.AnchorSource
	sort.Slice(a.Spans, func(i, j int) bool { return a.Spans[i].From < a.Spans[j].From })
	sort.Slice(a.MovementCoverage, func(i, j int) bool { return a.MovementCoverage[i].From < a.MovementCoverage[j].From })
	bridge, err := monthlyPreparationRange(a, end.Unix(), row.AnchorAt)
	if err != nil {
		return u, err
	}
	for _, snapshot := range a.Snapshots {
		if snapshot.At == row.AnchorAt && snapshot.Source == row.AnchorSource {
			reverse := big.NewInt(snapshot.BalanceQuota)
			reverse.Sub(reverse, big.NewInt(bridge.increase))
			reverse.Sub(reverse, big.NewInt(bridge.decrease))
			reverse.Add(reverse, big.NewInt(bridge.consume))
			reverse.Sub(reverse, big.NewInt(bridge.refund))
			if !reverse.IsInt64() || reverse.Int64() != u.ClosingQuota {
				return u, errors.New("候选账单与独立快照反向核对不一致")
			}
			u.ReverseClosingQuota = reverse.Int64()
			return u, nil
		}
	}
	return u, errors.New("候选账单的独立余额快照缺失")
}

// PrepareMonthlyCandidateBundle performs no IO. The CLI separately verifies
// every retained file hash. Metadata binds to the exact input bytes; source
// contents and review truthfulness still require an operator's review.
func PrepareMonthlyCandidateBundle(inputRaw, contextRaw []byte, now time.Time) (MonthlyPreparationBundle, error) {
	return prepareMonthlyCandidateBundle(inputRaw, contextRaw, now, false)
}

func prepareMonthlyCandidateBundle(inputRaw, contextRaw []byte, now time.Time, allowSameView bool) (MonthlyPreparationBundle, error) {
	bundle := MonthlyPreparationBundle{}
	input, err := DecodeMonthlyPreparationInput(inputRaw)
	if err != nil {
		return bundle, err
	}
	c, err := decodeMonthlyBundleContext(contextRaw)
	if err != nil {
		return bundle, err
	}
	if !allowSameView {
		for _, account := range c.Accounts {
			for _, r := range account.Reconstructions {
				if r.SameView != nil {
					return bundle, errors.New("同视图重建必须先核验原始资料，JSON 不能授予核验权限")
				}
			}
		}
	}
	if c.InputSHA256 != monthlyDigest(inputRaw) {
		return bundle, errors.New("制单上下文与核对输入的 SHA-256 不一致")
	}
	if err := validateMonthlyBundleContext(c, input, now); err != nil {
		return bundle, err
	}
	report, err := PrepareMonthlyReconciliation(inputRaw, now)
	if err != nil {
		return bundle, err
	}
	bundle = MonthlyPreparationBundle{Version: 1, Mode: "local_candidate_bundle_not_publication", InputSHA256: c.InputSHA256, ContextSHA256: monthlyDigest(contextRaw), SourceIntegrity: "not_checked_by_pure_calculator", Reconciliation: report, Entries: []MonthlyBundleEntry{}}
	sources := map[string]bool{}
	accounts, reports, metadata := map[int64]MonthlyPreparationAccount{}, map[int64]MonthlyPreparationAccountReport{}, map[int64]MonthlyBundleAccountContext{}
	owners, conflicting := map[int64]int64{}, map[int64]bool{}
	for _, s := range input.Sources {
		sources[s.ID] = true
	}
	for _, a := range input.Accounts {
		accounts[a.UserID] = a
		for _, movement := range a.Movements {
			if owner, ok := owners[movement.ID]; ok && owner != a.UserID {
				conflicting[owner], conflicting[a.UserID] = true, true
			}
			owners[movement.ID] = a.UserID
		}
	}
	for _, r := range report.Accounts {
		reports[r.UserID] = r
	}
	for _, a := range c.Accounts {
		metadata[a.UserID] = a
		for _, r := range a.Reconstructions {
			if r.SameView == nil {
				continue
			}
			for _, m := range r.SameView.Funding {
				if owner, ok := owners[m.ID]; ok && owner != a.UserID {
					conflicting[owner], conflicting[a.UserID] = true, true
				}
				owners[m.ID] = a.UserID
			}
		}
	}
	sort.Slice(c.Groups, func(i, j int) bool { return c.Groups[i].GroupID < c.Groups[j].GroupID })
	for _, g := range c.Groups {
		sort.Strings(g.Months)
		sort.Slice(g.Members, func(i, j int) bool { return g.Members[i].UserID < g.Members[j].UserID })
		for _, month := range g.Months {
			entry := MonthlyBundleEntry{GroupID: g.GroupID, Account: g.Name, Month: month, Members: g.Members, Status: "blocked", Issues: []MonthlyBundleIssue{}}
			binding := fmt.Sprintf("input:%s/context:%s/group:%d/month:%s", bundle.InputSHA256, bundle.ContextSHA256, g.GroupID, month)
			candidate := MonthlyBillCandidate{GroupID: g.GroupID, Month: month, SourceReference: binding, Users: []MonthlyBillUserEvidence{}}
			for _, member := range g.Members {
				a, ok := accounts[member.UserID]
				if !ok || conflicting[member.UserID] {
					entry.Issues = append(entry.Issues, MonthlyBundleIssue{UserID: member.UserID, Code: "account_scope", Reason: "成员证据缺失或额度事件跨账号重复"})
					continue
				}
				u, err := monthlyBundleUser(a, reports[a.UserID], metadata[a.UserID], month, binding+fmt.Sprintf("/user:%d", a.UserID), sources, now)
				if err == nil {
					for i := range a.TokenMonths {
						if a.TokenMonths[i].Month == month {
							u.TokenUsage = &a.TokenMonths[i]
						}
					}
				}
				if err != nil {
					entry.Issues = append(entry.Issues, MonthlyBundleIssue{UserID: a.UserID, Code: "account_not_reconciled", Reason: err.Error()})
					continue
				}
				candidate.Users = append(candidate.Users, u)
				for _, reconstruction := range metadata[a.UserID].Reconstructions {
					if reconstruction.Month == month {
						candidate.ReconstructedUsers = append(candidate.ReconstructedUsers, a.UserID)
					}
				}
			}
			if len(entry.Issues) == 0 {
				validationUsers, err := monthlyCandidateTokenUsers(candidate.Users, month)
				// Reuse the publication arithmetic/limits, but discard its DTO and
				// synthetic validation marker. No review record is emitted or saved.
				if err == nil {
					_, _, err = buildPublishedMonthlyBill(MonthlyBillEvidence{Version: monthlyEvidenceVersion(validationUsers), GroupID: g.GroupID, Month: month, ReviewedBy: "internal-validation-only-not-a-review", ReviewedAt: now.Unix(), SourceReference: binding, Users: validationUsers}, now)
				}
				if err != nil {
					entry.Issues = append(entry.Issues, MonthlyBundleIssue{Code: "candidate_validation", Reason: err.Error()})
				} else {
					raw, err := json.Marshal(candidate)
					if err != nil {
						return MonthlyPreparationBundle{}, err
					}
					// Leave room for review fields below the importer's 2MiB cap.
					if len(raw) > (2<<20)-4096 {
						entry.Issues = append(entry.Issues, MonthlyBundleIssue{Code: "candidate_size", Reason: "单份候选账单超过导入大小上限"})
					} else {
						entry.Status, entry.Candidate, entry.CandidateSHA256 = "needs_review", &candidate, monthlyDigest(raw)
					}
				}
			}
			if entry.Candidate == nil {
				bundle.Blocked++
			} else {
				bundle.ReadyForReview++
			}
			bundle.Entries = append(bundle.Entries, entry)
		}
	}
	return bundle, nil
}
