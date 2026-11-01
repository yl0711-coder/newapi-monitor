package monitor

// Shared customer calendar-month bill DTOs and exact quota conversion.
import (
	"errors"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const monthlyCreditLimit = 2000

type monthlyCredit struct {
	UserID   int64  `json:"user_id,omitempty"`
	Username string `json:"username,omitempty"`
	At       int64  `json:"at"`
	Amount   string `json:"amount"` // decimal CNY balance units; empty means unknown, not zero
	Method   string `json:"method,omitempty"`
	Kind     string `json:"kind,omitempty"`
}

type monthlyCredits struct {
	RechargeCount     *int            `json:"recharge_count,omitempty"`
	RechargeTotal     *string         `json:"recharge_total,omitempty"`
	GiftTotal         *string         `json:"gift_total,omitempty"`
	Available         bool            `json:"available"`
	Rows              []monthlyCredit `json:"rows"`
	KnownTotal        string          `json:"known_total"`
	Unknown           int             `json:"unknown"`
	Adjustment        string          `json:"adjustment"`                   // signed negative manual adjustments, not recharge
	BalanceCorrection string          `json:"balance_correction,omitempty"` // positive, separately disclosed; never consumption/recharge
	Message           string          `json:"message"`
}

type monthlyConsumption struct {
	Requests        int64  `json:"requests"`
	Tokens          int64  `json:"tokens"`
	Amount          string `json:"amount"`
	Refund          string `json:"refund"`
	Net             string `json:"net"`
	RequestsUnknown bool   `json:"-"`
	TokensUnknown   bool   `json:"-"`
}

type monthlyStatement struct {
	Account            string              `json:"account"`
	Published          bool                `json:"published"`
	Publication        string              `json:"publication,omitempty"` // opaque issuance version, not an evidence hash
	Month              string              `json:"month"`
	From               string              `json:"from"`
	To                 string              `json:"to"`
	AsOf               int64               `json:"as_of"`
	CurrentMonth       bool                `json:"current_month"`
	Credits            monthlyCredits      `json:"credits"`
	Consumption        *monthlyConsumption `json:"consumption"`
	ConsumptionMessage string              `json:"consumption_message"`
	ConsumptionThrough int64               `json:"consumption_through"`
	CurrentBalance     *string             `json:"current_balance"`
	BalanceAsOf        int64               `json:"balance_as_of"`
	BalanceMessage     string              `json:"balance_message"`
	// Populated only by the reviewed forward/reverse reconciliation pipeline.
	MonthOpeningBalance *string `json:"month_opening_balance,omitempty"`
	MonthEndBalance     *string `json:"month_end_balance"`
	// Additive payload fields. Legacy publications remain byte-for-byte intact.
	MemberDetailsVersion int                      `json:"member_details_version,omitempty"`
	Members              []monthlyMemberStatement `json:"members,omitempty"`
	TokenDetailsVersion  int                      `json:"token_details_version,omitempty"`
	DisplayBalances      *monthlyDisplayBalances  `json:"display_balances,omitempty"`
}

type monthlyMemberStatement struct {
	UserID            int64                     `json:"user_id"`
	Username          string                    `json:"username"`
	Opening           string                    `json:"opening"`
	CreditTotal       string                    `json:"credit_total"` // recharge plus gifts, before negative adjustments
	Adjustment        string                    `json:"adjustment"`   // signed; never netted against another member
	BalanceCorrection string                    `json:"balance_correction,omitempty"`
	Consumption       monthlyConsumption        `json:"consumption"`
	Closing           string                    `json:"closing"`
	TokenConsumption  []monthlyTokenConsumption `json:"token_consumption,omitempty"`
}

func monthlyQuotaMoney(q int64) string {
	return new(big.Rat).SetFrac(big.NewInt(q), big.NewInt(500000)).FloatString(6)
}

func monthlyWindow(value string, now time.Time) (time.Time, time.Time, error) {
	current := time.Date(now.In(usageCST).Year(), now.In(usageCST).Month(), 1, 0, 0, 0, 0, usageCST)
	if value == "" {
		value = current.AddDate(0, -1, 0).Format("2006-01")
	}
	start, err := time.ParseInLocation("2006-01", value, usageCST)
	if err != nil || start.Format("2006-01") != value || start.Year() < 2020 || !start.Before(current) {
		return time.Time{}, time.Time{}, errors.New("仅支持已结束的自然月（2020年起），不包含当月或未来月份")
	}
	return start, start.AddDate(0, 1, 0), nil
}

// Only unambiguous RC26 quota units. A historical ¥ string may contain an
// unknown historical FX rate; never divide it by today's rate. No float math.
var monthlyQuotaPattern = regexp.MustCompile(`^(?:[＄$]([0-9]+(?:\.[0-9]{1,6})?)(?: 额度)?|([0-9]+) 点额度)$`)

func parseMonthlyQuota(value string) (int64, bool) {
	m := monthlyQuotaPattern.FindStringSubmatch(strings.TrimSpace(value))
	if m == nil {
		return 0, false
	}
	if m[2] != "" {
		n, err := strconv.ParseInt(m[2], 10, 64)
		return n, err == nil
	}
	r, ok := new(big.Rat).SetString(m[1])
	if !ok {
		return 0, false
	}
	r.Mul(r, big.NewRat(500000, 1))
	if !r.IsInt() || !r.Num().IsInt64() {
		return 0, false
	}
	return r.Num().Int64(), true
}

const monthlyStatementPageSize = 3

type monthlyStatementRow struct {
	GiftTotal     *string             `json:"gift_total,omitempty"`
	CreditTotal   *string             `json:"credit_total,omitempty"`
	Month         string              `json:"month"`
	RechargeCount *int                `json:"recharge_count"`
	RechargeTotal *string             `json:"recharge_total"`
	Consumption   *monthlyConsumption `json:"consumption"`
	Downloadable  bool                `json:"downloadable"`
	// API display metadata only. Omitted for two decimals, never stored in bills.
	MoneyDecimalPlaces int `json:"money_decimal_places,omitempty"`
}

func monthlySummary(p monthlyStatement) monthlyStatementRow {
	r := monthlyStatementRow{Month: p.Month, Consumption: p.Consumption}
	if p.Credits.Available && p.Credits.Unknown == 0 {
		count, total := len(p.Credits.Rows), p.Credits.KnownTotal
		r.RechargeCount, r.RechargeTotal = &count, &total
		if p.Credits.RechargeCount != nil && p.Credits.RechargeTotal != nil {
			r.RechargeCount, r.RechargeTotal = p.Credits.RechargeCount, p.Credits.RechargeTotal
			r.GiftTotal, r.CreditTotal = p.Credits.GiftTotal, &p.Credits.KnownTotal
		}
	}
	r.Downloadable = p.Published && !p.CurrentMonth && monthlyStatementAmountsComplete(p)
	if places := monthlySummaryPrecision(p); places > 2 {
		r.MoneyDecimalPlaces = places
	}
	return r
}
