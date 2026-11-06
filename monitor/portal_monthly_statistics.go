package monitor

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/gin-gonic/gin"
)

// An already-open older page uses Number(null) and can present unknown counts
// as zero. Negotiate this display capability, never customer scope or money.
func monthlyClientSupportsStatistics(c *gin.Context, p monthlyStatement) bool {
	if p.DisplayBalances != nil && c.GetHeader("X-Monthly-Display") != "cents-chain-v1" {
		c.JSON(409, gin.H{"error": "账单展示已更新，请刷新页面后重试"})
		return false
	}
	if p.Credits.BalanceCorrection != "" && c.GetHeader("X-Monthly-Accounting") != "balance-correction-v1" {
		c.JSON(409, gin.H{"error": "账单展示已更新，请刷新页面后重试"})
		return false
	}
	if p.MemberDetailsVersion != 2 || c.GetHeader("X-Monthly-Statistics") == "nullable-v1" {
		return true
	}
	c.JSON(409, gin.H{"error": "账单展示已更新，请刷新页面后重试"})
	return false
}

// Optional internal coverage flags. Nil preserves version-2 evidence exactly.
// An explicit unknown statistic uses a zero storage placeholder, NEVER a zero
// in the customer JSON/XLSX. Version 3 is required whenever coverage is partial.
type MonthlyBillStatisticsCoverage struct {
	RequestsKnown bool `json:"requests_known"`
	TokensKnown   bool `json:"tokens_known"`
}

func monthlyEvidenceVersion(users []MonthlyBillUserEvidence) int {
	for _, u := range users {
		if u.TokenUsage != nil {
			return 5
		}
	}
	for _, u := range users {
		if u.BalanceCorrection != nil || u.HistoricalBalance != nil {
			return 4
		}
	}
	if monthlyHasUnknownStatistics(users) {
		return 3
	}
	return 2
}

func monthlyHasBalanceCorrection(users []MonthlyBillUserEvidence) bool {
	for _, u := range users {
		if u.BalanceCorrection != nil {
			return true
		}
	}
	return false
}

func monthlyHasUnknownStatistics(users []MonthlyBillUserEvidence) bool {
	for _, u := range users {
		if u.Statistics != nil {
			return true
		}
	}
	return false
}

func monthlyUserConsumption(u MonthlyBillUserEvidence) monthlyConsumption {
	c := monthlyConsumption{Requests: u.Requests, Tokens: u.Tokens,
		Amount: monthlyQuotaMoney(u.ConsumeQuota), Refund: monthlyQuotaMoney(u.RefundQuota), Net: monthlyQuotaMoney(u.ConsumeQuota - u.RefundQuota)}
	if u.Statistics != nil {
		c.RequestsUnknown, c.TokensUnknown = !u.Statistics.RequestsKnown, !u.Statistics.TokensKnown
	}
	return c
}

func monthlyStatisticsValid(c monthlyConsumption) bool {
	return c.Requests >= 0 && c.Tokens >= 0 && c.Requests <= 9007199254740991 && c.Tokens <= 9007199254740991 &&
		(!c.RequestsUnknown || c.Requests == 0) && (!c.TokensUnknown || c.Tokens == 0)
}

// Unknown member totals cannot be replaced by a partial organization sum.
// Determine coverage before summing, so the result is independent of row order.
func monthlyAggregateStatistics(members []monthlyMemberStatement) (monthlyConsumption, error) {
	var total monthlyConsumption
	for _, m := range members {
		if !monthlyStatisticsValid(m.Consumption) {
			return total, errors.New("账单辅助统计值无效")
		}
		total.RequestsUnknown = total.RequestsUnknown || m.Consumption.RequestsUnknown
		total.TokensUnknown = total.TokensUnknown || m.Consumption.TokensUnknown
	}
	for _, m := range members {
		if !total.RequestsUnknown {
			if err := addEconomicsInt64(&total.Requests, m.Consumption.Requests); err != nil {
				return total, err
			}
		}
		if !total.TokensUnknown {
			if err := addEconomicsInt64(&total.Tokens, m.Consumption.Tokens); err != nil {
				return total, err
			}
		}
	}
	if !monthlyStatisticsValid(total) {
		return total, errors.New("账单辅助统计超过展示范围")
	}
	return total, nil
}

// Preserve the exact legacy JSON field order and values for known statistics.
// The new nullable representation never exposes a partial sum as a true total.
type monthlyConsumptionJSON struct {
	Requests *int64 `json:"requests"`
	Tokens   *int64 `json:"tokens"`
	Amount   string `json:"amount"`
	Refund   string `json:"refund"`
	Net      string `json:"net"`
}

func (c monthlyConsumption) MarshalJSON() ([]byte, error) {
	v := monthlyConsumptionJSON{Requests: &c.Requests, Tokens: &c.Tokens, Amount: c.Amount, Refund: c.Refund, Net: c.Net}
	if c.RequestsUnknown {
		if c.Requests != 0 {
			return nil, errors.New("未知请求数不能含隐含部分合计")
		}
		v.Requests = nil
	}
	if c.TokensUnknown {
		if c.Tokens != 0 {
			return nil, errors.New("未知 Token 不能含隐含部分合计")
		}
		v.Tokens = nil
	}
	return json.Marshal(v)
}

func (c *monthlyConsumption) UnmarshalJSON(raw []byte) error {
	if err := monthlyUniqueJSONKeys(raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, name := range []string{"requests", "tokens", "amount", "refund", "net"} {
		value, ok := fields[name]
		if !ok || (name != "requests" && name != "tokens" && bytes.Equal(bytes.TrimSpace(value), []byte("null"))) {
			return errors.New("账单消费字段缺失；未知辅助统计须显式 null")
		}
	}
	var v monthlyConsumptionJSON
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	*c = monthlyConsumption{Amount: v.Amount, Refund: v.Refund, Net: v.Net, RequestsUnknown: v.Requests == nil, TokensUnknown: v.Tokens == nil}
	if v.Requests != nil {
		c.Requests = *v.Requests
	}
	if v.Tokens != nil {
		c.Tokens = *v.Tokens
	}
	return nil
}
