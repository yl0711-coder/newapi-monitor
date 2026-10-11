package monitor

import (
	"encoding/json"
	"errors"
	"math/big"
	"net/url"
	"strconv"
	"strings"
)

// Verified against aipipe's detail and stat endpoints. usage_usd is a ledger
// amount, not an instruction to exchange currencies or apply recharge ratios.
// Only this verified host's consumption records may use this compatibility path.
const aipipeQuotaPerLedgerUnit = 500000
const aipipeNormalizedQuotaDecimals = 12
const aipipeMaxExactFloatInteger = 1<<53 - 1

func usesAipipeUsageUSD(row ChannelUpstreamAccount, logType int) bool {
	if logType != 2 || row.Provider != upstreamProviderNewAPI || row.Domain != "aipipe.cc" {
		return false
	}
	u, err := url.Parse(row.BaseURL)
	return err == nil && u.Scheme == "https" && strings.EqualFold(u.Hostname(), "aipipe.cc") &&
		(u.Port() == "" || u.Port() == "443") && u.User == nil
}

// Bound decimal input before big.Rat parsing, including exponents, so hostile
// values cannot allocate unbounded integers. No rounding is used for conversion.
func aipipeMoneyDecimal(raw json.RawMessage) (*big.Rat, error) {
	text := strings.TrimSpace(string(raw))
	if len(text) == 0 || len(text) > 80 {
		return nil, errors.New("aipipe 金额缺失或精度超限")
	}
	if text[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, errors.New("aipipe 金额格式无效")
		}
		text = strings.TrimSpace(text)
	}
	var number json.Number
	if text == "null" || json.Unmarshal([]byte(text), &number) != nil || number.String() == "" {
		return nil, errors.New("aipipe 金额必须为十进制数值")
	}
	if i := strings.IndexAny(text, "eE"); i >= 0 {
		exponent, err := strconv.Atoi(text[i+1:])
		if err != nil || exponent < -24 || exponent > 24 {
			return nil, errors.New("aipipe 金额指数超限")
		}
	}
	value, ok := new(big.Rat).SetString(text)
	if !ok || value.Sign() < 0 {
		return nil, errors.New("aipipe 金额无效或为负数")
	}
	return value, nil
}

func normalizeAipipeUsageItem(raw json.RawMessage, unit float64) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	amount, hasAmount := fields["usage_usd"]
	if !hasAmount {
		return raw, nil
	} // Original quota contract remains authoritative.
	if unit != aipipeQuotaPerLedgerUnit {
		return nil, errors.New("aipipe usage_usd 换算单位与已核验接口不一致，未发布账单")
	}
	quota, err := aipipeMoneyDecimal(amount)
	if err != nil {
		return nil, err
	}
	quota.Mul(quota, big.NewRat(aipipeQuotaPerLedgerUnit, 1))
	if quota.Cmp(big.NewRat(aipipeMaxExactFloatInteger, 1)) > 0 {
		return nil, errors.New("aipipe 金额超出安全汇总范围")
	}
	if existing, present := fields["quota"]; present {
		original, err := aipipeMoneyDecimal(existing)
		if err != nil || original.Cmp(quota) != 0 {
			return nil, errors.New("aipipe quota 与 usage_usd 不一致，未发布账单")
		}
		return raw, nil // Never add the two amounts or replace a present invalid quota.
	}
	decimal := quota.FloatString(aipipeNormalizedQuotaDecimals)
	check, ok := new(big.Rat).SetString(decimal)
	if !ok || check.Cmp(quota) != 0 {
		return nil, errors.New("aipipe 金额精度超限，未舍入或发布账单")
	}
	decimal = strings.TrimRight(strings.TrimRight(decimal, "0"), ".")
	fields["quota"] = json.RawMessage(decimal)
	return json.Marshal(fields)
}
