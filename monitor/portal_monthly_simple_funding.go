package monitor

import (
	"encoding/json"
	"math/big"
	"strconv"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// simpleFundingRules is the reviewed producer for history written before the
// rc48 upgrade (Chinese RC4/RC26 content). rc48 rows are recognized below.
var simpleFundingRules = monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}

// simpleClassifyFunding classifies one wallet funding log row (types 1/3/4).
// It first applies the existing RC4/RC26 classifier, then the rc48 shapes.
// Anything still unrecognized stays FundingReview: the caller must block the
// bill rather than guess an amount.
func simpleClassifyFunding(row monthlysource.FundingLog) monthlysource.FundingResult {
	r := monthlysource.NormalizeFundingLog(row, simpleFundingRules)
	if r.Status != monthlysource.FundingReview {
		return r
	}
	if out, ok := simpleClassifyRC48(row, r); ok {
		return out
	}
	if r.Reason == "created_user_wallet_requires_evidence" {
		// Account creation is not itself a wallet movement here; any initial
		// wallet it granted is caught by the balance reconciliation (residual).
		r.Status, r.Reason = monthlysource.FundingNonWallet, "created_user_reconciled_by_balance"
	}
	return r
}

type simpleRC48Other struct {
	Op *struct {
		Action string                     `json:"action"`
		Params map[string]json.RawMessage `json:"params"`
	} `json:"op"`
	ContentParts []struct {
		Key    string                     `json:"key"`
		Params map[string]json.RawMessage `json:"params"`
	} `json:"content_parts"`
}

type simpleRC48Template struct {
	logType      int
	key          string
	status, kind string
	method       string
}

// Templates are copied verbatim from new-api v1.0.0-rc.48 (common.NewMessage keys).
var simpleRC48Templates = []simpleRC48Template{
	{1, "Online top-up succeeded, amount added: {{quota}}, payment amount: {{amount}}", monthlysource.FundingMovement, "recharge", "在线充值"},
	{1, "Administrator completed the order, amount added: {{quota}}, payment amount: {{amount}}", monthlysource.FundingMovement, "recharge", "后台充值"},
	{1, "{{provider}} top-up succeeded, amount added: {{quota}}, payment amount: {{amount}}", monthlysource.FundingMovement, "recharge", "在线充值"},
	{1, "Topped up {{quota}} with a redemption code, code ID {{id}}", monthlysource.FundingMovement, "recharge", "兑换码充值"},
	{1, "Subscription purchased, plan: {{plan}}, payment amount: {{amount}}, payment method: {{method}}", monthlysource.FundingSeparate, "", ""},
	{4, "New user sign-up bonus {{quota}}", monthlysource.FundingMovement, "gift", "注册赠送"},
	{4, "Invitation code bonus {{quota}}", monthlysource.FundingMovement, "gift", "邀请赠送"},
	{4, "Daily check-in, received {{quota}}", monthlysource.FundingMovement, "gift", "签到赠送"},
	{4, "Bonus for inviting a user {{quota}}", monthlysource.FundingSeparate, "", ""}, // aff_quota, not the wallet
}

func simpleClassifyRC48(row monthlysource.FundingLog, base monthlysource.FundingResult) (monthlysource.FundingResult, bool) {
	r := base
	var other simpleRC48Other
	if row.Other == "" || json.Unmarshal([]byte(row.Other), &other) != nil {
		return r, false
	}
	move := func(delta int64, kind, method string) (monthlysource.FundingResult, bool) {
		r.Status, r.Reason, r.TargetUserID = monthlysource.FundingMovement, "", row.UserID
		r.DeltaQuota, r.Kind, r.Method = &delta, kind, method
		return r, true
	}
	// rc48 admin quota adjustment: type=1 under the beneficiary with exact
	// integer before/after quotas (controller/user_quota.go).
	if other.Op != nil && row.Type == 1 {
		switch other.Op.Action {
		case "user.quota_add", "user.quota_subtract", "user.quota_override":
		default:
			return r, false
		}
		from, okFrom := simpleJSONInt(other.Op.Params["from"])
		to, okTo := simpleJSONInt(other.Op.Params["to"])
		target, okTarget := simpleJSONInt(other.Op.Params["target_user_id"])
		if !okFrom || !okTo || !okTarget || target != row.UserID {
			return r, false
		}
		delta := to - from
		if q, ok := simpleJSONInt(other.Op.Params["quota"]); ok {
			if (other.Op.Action == "user.quota_add" && q != delta) || (other.Op.Action == "user.quota_subtract" && q != -delta) {
				return r, false
			}
		}
		switch {
		case delta == 0:
			r.Status, r.Reason = monthlysource.FundingNonWallet, "unchanged_quota_override"
			return r, true
		case delta > 0:
			return move(delta, "recharge", "后台充值")
		default:
			return move(delta, "adjustment", "余额扣减")
		}
	}
	if len(other.ContentParts) != 1 {
		return r, false
	}
	part := other.ContentParts[0]
	for _, t := range simpleRC48Templates {
		if t.key != part.Key || t.logType != row.Type {
			continue
		}
		// The stored content is the rendered template; its fixed prefix must match.
		if prefix, _, _ := strings.Cut(t.key, "{{"); prefix != "" && !strings.HasPrefix(row.Content, prefix) {
			return r, false
		}
		if t.status != monthlysource.FundingMovement {
			r.Status, r.Reason = t.status, "rc48_not_wallet_funding"
			return r, true
		}
		q, ok := simpleRC48Quota(part.Params["quota"])
		if !ok || q <= 0 {
			return r, false
		}
		return move(q, t.kind, t.method)
	}
	return r, false
}

// simpleRC48Quota parses logger.FormatQuota output in USD display mode
// ("＄1000.000000"/"$…", exact at 6 decimals) or a raw integer quota (Creem /
// TOKENS display). CNY/custom currency strings depend on an exchange rate and
// are rejected.
func simpleRC48Quota(raw json.RawMessage) (int64, bool) {
	if v, ok := simpleJSONInt(raw); ok {
		return v, true
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return 0, false
	}
	s = strings.TrimSpace(s)
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v, true
	}
	switch {
	case strings.HasPrefix(s, "＄"):
		s = strings.TrimPrefix(s, "＄")
	case strings.HasPrefix(s, "$"):
		s = strings.TrimPrefix(s, "$")
	default:
		return 0, false
	}
	if s == "" || strings.ContainsAny(s, "+-eE, ") {
		return 0, false
	}
	amount, ok := new(big.Rat).SetString(s)
	if !ok {
		return 0, false
	}
	amount.Mul(amount, big.NewRat(500000, 1))
	if !amount.IsInt() || !amount.Num().IsInt64() {
		return 0, false
	}
	return amount.Num().Int64(), true
}

func simpleJSONInt(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var n json.Number
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if dec.Decode(&n) != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(n.String(), 10, 64)
	return v, err == nil
}
