package monitor

// Token consumption is a breakdown of the existing, reconciled user totals.
// It must never recalculate the wallet, distribute gaps, or query a current
// token name/key when reading an already-saved bill.
import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const monthlyTokenPerUserLimit = 1000
const monthlyTokenBillLimit = 10000

// Normalized, retained consumption/refund rows only. No API key or upstream
// credential is accepted. Completeness/source identity must be established by
// the bounded source adapter; matching sums alone cannot attest completeness.
type MonthlyTokenUsageLog struct {
	ID            int64  `json:"id"`
	UserID        int64  `json:"user_id"`
	At            int64  `json:"at"`
	Type          int    `json:"type"`
	BillingSource string `json:"billing_source"`
	TokenID       int64  `json:"token_id"`
	Name          string `json:"name"`
	Quota         int64  `json:"quota"`
}

// PrepareMonthlyTokenUsage is a pure, bounded normalization step, not a source
// capture or permission to publish. Caller-owned rows are never reordered.
// Names come from the last retained log (time, then log ID), never current
// tokens; deleted/renamed tokens therefore retain one stable monthly ID.
func PrepareMonthlyTokenUsage(userID int64, month, source string, logs []MonthlyTokenUsageLog, now time.Time) (MonthlyBillTokenUsage, error) {
	var empty MonthlyBillTokenUsage
	start, end, err := monthlyWindow(month, now)
	if err != nil || userID <= 0 || strings.TrimSpace(source) == "" || len(source) > 512 || logs == nil || len(logs) > 1000000 {
		return empty, errors.New("令牌日志归一化范围或来源无效")
	}
	type aggregate struct {
		row    MonthlyBillTokenEvidence
		at, id int64
	}
	byToken := map[int64]*aggregate{}
	seen := map[int64]bool{}
	for _, log := range logs {
		if log.ID <= 0 || seen[log.ID] || log.UserID != userID || log.At < start.Unix() || log.At >= end.Unix() || (log.Type != 2 && log.Type != 6) || log.BillingSource != "wallet" || log.Quota < 0 || log.TokenID < 0 || log.TokenID > 9007199254740991 || !utf8.ValidString(log.Name) || len(log.Name) > 512 {
			return empty, errors.New("令牌日志重复、越界、含非钱包记录或金额无效")
		}
		seen[log.ID] = true
		a := byToken[log.TokenID]
		if a == nil {
			if len(byToken) >= monthlyTokenPerUserLimit {
				return empty, errors.New("单账号令牌数量超过上限")
			}
			a = &aggregate{row: MonthlyBillTokenEvidence{TokenID: log.TokenID}}
			byToken[log.TokenID] = a
		}
		if log.At > a.at || (log.At == a.at && log.ID > a.id) {
			a.row.Name, a.at, a.id = log.Name, log.At, log.ID
		}
		target := &a.row.ConsumeQuota
		if log.Type == 6 {
			target = &a.row.RefundQuota
		}
		if err := addEconomicsInt64(target, log.Quota); err != nil {
			return empty, err
		}
	}
	result := MonthlyBillTokenUsage{UserID: userID, Month: month, Source: source, Rows: []MonthlyBillTokenEvidence{}}
	for _, a := range byToken {
		a.row.Name = monthlyTokenLabel(a.row.TokenID, a.row.Name)
		result.Rows = append(result.Rows, a.row)
	}
	sort.Slice(result.Rows, func(i, j int) bool { return result.Rows[i].TokenID < result.Rows[j].TokenID })
	return result, nil
}

type MonthlyBillTokenUsage struct {
	UserID int64                      `json:"user_id"`
	Month  string                     `json:"month"`
	Source string                     `json:"source"` // private retained source reference, never a customer field
	Rows   []MonthlyBillTokenEvidence `json:"rows"`
}

type MonthlyBillTokenEvidence struct {
	TokenID      int64  `json:"token_id"` // 0 is an explicit unidentifiable historical-log bucket
	Name         string `json:"name"`     // snapshot label only; ID, not name, is the identity
	ConsumeQuota int64  `json:"consume_quota"`
	RefundQuota  int64  `json:"refund_quota"`
}

type monthlyTokenConsumption struct {
	TokenID int64  `json:"token_id"`
	Name    string `json:"name"`
	Amount  string `json:"amount"`
	Refund  string `json:"refund"`
	Net     string `json:"net"`
}

func monthlyTokenJSONShape(raw []byte, required func(map[string]json.RawMessage, ...string) error) error {
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil {
		return err
	}
	if err := required(shape, "user_id", "month", "source", "rows"); err != nil {
		return err
	}
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(shape["rows"], &rows); err != nil {
		return err
	}
	for _, row := range rows {
		if err := required(row, "token_id", "name", "consume_quota", "refund_quota"); err != nil {
			return err
		}
	}
	return nil
}

func monthlyTokenLabel(id int64, name string) string {
	if id == 0 {
		return "历史未记录令牌"
	}
	if name == "" {
		return "未命名令牌"
	}
	return name
}

func monthlyBuildTokenConsumption(u MonthlyBillUserEvidence, month string) ([]monthlyTokenConsumption, error) {
	fail := errors.New("令牌消费依据缺失、范围无效或与账号日志金额不一致")
	t := u.TokenUsage
	if t == nil || t.UserID != u.UserID || t.Month != month || strings.TrimSpace(t.Source) == "" || len(t.Source) > 512 || t.Rows == nil || len(t.Rows) > monthlyTokenPerUserLimit {
		return nil, fail
	}
	rows := make([]monthlyTokenConsumption, 0, len(t.Rows))
	seen := map[int64]bool{}
	var consume, refund int64
	for _, r := range t.Rows {
		if r.TokenID < 0 || r.TokenID > 9007199254740991 || seen[r.TokenID] || !utf8.ValidString(r.Name) || len(r.Name) > 512 || r.ConsumeQuota < 0 || r.RefundQuota < 0 {
			return nil, fail
		}
		seen[r.TokenID] = true
		if addEconomicsInt64(&consume, r.ConsumeQuota) != nil || addEconomicsInt64(&refund, r.RefundQuota) != nil {
			return nil, fail
		}
		rows = append(rows, monthlyTokenConsumption{TokenID: r.TokenID, Name: monthlyTokenLabel(r.TokenID, r.Name), Amount: monthlyQuotaMoney(r.ConsumeQuota), Refund: monthlyQuotaMoney(r.RefundQuota), Net: monthlyQuotaMoney(r.ConsumeQuota - r.RefundQuota)})
	}
	// A matching net alone can hide simultaneously missing gross/refund rows.
	if consume != u.ConsumeQuota || refund != u.RefundQuota || u.Accounting == nil || consume != u.Accounting.LogConsumeQuota || refund != u.Accounting.LogRefundQuota {
		return nil, fail
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TokenID == 0 {
			return false
		}
		return rows[j].TokenID == 0 || rows[i].TokenID < rows[j].TokenID
	})
	return rows, nil
}

// PRIVATE candidates may still carry the independently observed wallet debit
// before an existing correction policy recognizes a log/wallet difference.
// Validate original token logs separately from that wallet equation. Never
// mutate the candidate's consumption or let this path publish a mismatched
// token breakdown: buildPublishedMonthlyBill remains strict at final issuance.
func monthlyCandidateTokenUsers(users []MonthlyBillUserEvidence, month string) ([]MonthlyBillUserEvidence, error) {
	hasTokens := false
	for _, u := range users {
		hasTokens = hasTokens || u.TokenUsage != nil
	}
	if !hasTokens {
		return users, nil
	}
	validation := append([]MonthlyBillUserEvidence{}, users...)
	tokens := monthlyStatement{MemberDetailsVersion: 1, TokenDetailsVersion: 1}
	for i, u := range users {
		if u.Accounting == nil {
			return nil, errors.New("令牌候选缺少原始日志金额")
		}
		logUser := u
		logUser.ConsumeQuota, logUser.RefundQuota = u.Accounting.LogConsumeQuota, u.Accounting.LogRefundQuota
		rows, err := monthlyBuildTokenConsumption(logUser, month)
		if err != nil {
			return nil, err
		}
		tokens.Members = append(tokens.Members, monthlyMemberStatement{UserID: u.UserID, TokenConsumption: rows, Consumption: monthlyConsumption{Amount: monthlyQuotaMoney(logUser.ConsumeQuota), Refund: monthlyQuotaMoney(logUser.RefundQuota)}})
		validation[i].TokenUsage = nil
	}
	if !monthlyTokensComplete(tokens) {
		return nil, errors.New("令牌候选归属重复或金额不完整")
	}
	return validation, nil
}

// Revalidate saved customer payloads independently of private source metadata.
// A new-version bill must have every user's breakdown; old frozen bills stay
// byte-identical and do not silently acquire guessed token rows.
func monthlyTokensComplete(p monthlyStatement) bool {
	if p.TokenDetailsVersion == 0 {
		for _, m := range p.Members {
			if len(m.TokenConsumption) != 0 {
				return false
			}
		}
		return true
	}
	if p.TokenDetailsVersion != 1 || (p.MemberDetailsVersion != 1 && p.MemberDetailsVersion != 2) || len(p.Members) == 0 {
		return false
	}
	owners := map[int64]int64{}
	count := 0
	for _, m := range p.Members {
		if len(m.TokenConsumption) > monthlyTokenPerUserLimit {
			return false
		}
		seen := map[int64]bool{}
		var consume, refund int64
		for _, r := range m.TokenConsumption {
			count++
			if count > monthlyTokenBillLimit || r.TokenID < 0 || r.TokenID > 9007199254740991 || seen[r.TokenID] || !utf8.ValidString(r.Name) || len(r.Name) > 512 || r.Name != monthlyTokenLabel(r.TokenID, r.Name) {
				return false
			}
			seen[r.TokenID] = true
			if r.TokenID > 0 {
				if owner, exists := owners[r.TokenID]; exists && owner != m.UserID {
					return false
				}
				owners[r.TokenID] = m.UserID
			}
			c, cok := newMonthlyMoney(r.Amount)
			f, fok := newMonthlyMoney(r.Refund)
			n, nok := newMonthlyMoney(r.Net)
			if !cok || !fok || !nok || c < 0 || f < 0 || c-f != n || addEconomicsInt64(&consume, c) != nil || addEconomicsInt64(&refund, f) != nil {
				return false
			}
		}
		if monthlyQuotaMoney(consume) != m.Consumption.Amount || monthlyQuotaMoney(refund) != m.Consumption.Refund {
			return false
		}
	}
	return true
}
