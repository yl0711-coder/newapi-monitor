package monitor

import (
	"errors"
	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"maps"
	"math"
	"math/big"
	"strconv"
)

type tokenPriceResult struct {
	Supported    bool   `json:"supported"`
	Reason       string `json:"reason"`
	Method       string `json:"method"`
	Subject      string `json:"subject"`
	Unit         string `json:"unit"`
	QuotaPerUnit string `json:"quota_per_unit"`
	RawQuota     string `json:"raw_quota"`
	Quota        string `json:"quota"`
	Tier         string `json:"tier"`
	InputScale   string `json:"input_scale"`
}

func tokenRat(p tokenMeteringParsed, key string) (*big.Rat, error) {
	f := tokenMeteringOperand(p, key)
	if f.State != "valid" && f.State != "derived" {
		return nil, errors.New("operand_" + key + "_" + f.State)
	}
	n, ok := new(big.Rat).SetString(f.Value)
	if !ok || n.Sign() < 0 || n.Num().BitLen() > 1024 || n.Denom().BitLen() > 1024 {
		return nil, errors.New("operand_" + key + "_invalid")
	}
	return n, nil
}
func tokenRatio(v string) *big.Rat { r, _ := new(big.Rat).SetString(v); return r }
func tokenPricingOperands(p tokenMeteringParsed) tokenMeteringParsed {
	p.Fields = maps.Clone(p.Fields)
	for key, value := range p.ContractOperands {
		if f := p.Fields[key]; f.State == "missing" || f.State == "" {
			p.Fields[key] = value
		}
	}
	return p
}
func tokenPricingGuard(p tokenMeteringParsed, policy tokenDetectionPolicy) string {
	p = tokenPricingOperands(p)
	if policy.PricingAdapter != "newapi-text-v1" || p.Adapter != "newapi-rc26" && !(p.Adapter == "newapi-rc48" && p.Contract != nil && p.Contract.Version == tokenRC48Contract) {
		return "historical_pricing_adapter_unverified"
	}
	if p.OtherState != "valid" || p.TotalInput == nil {
		return "usage_incomplete"
	}
	if p.Fields["billing_source"].Value != "wallet" || !tokenMeteringFieldUsable(p.Fields["currency"]) || p.Fields["currency"].Value != "USD" {
		return "billing_subject_or_unit_unknown"
	}
	if !tokenMeteringFieldUsable(p.Fields["image"]) || p.Fields["image"].Value != "false" ||
		!tokenMeteringFieldUsable(p.Fields["is_task"]) || p.Fields["is_task"].Value != "false" ||
		!tokenMeteringFieldUsable(p.Fields["request_rules"]) || p.Fields["request_rules"].Value != "[]" && !tokenMeteringRecordedRulesUsable(p) {
		return "special_charges_not_excluded"
	}
	if !tokenMeteringFieldUsable(p.Fields["rounding_mode"]) || p.Fields["rounding_mode"].Value != "half_up" && p.Fields["rounding_mode"].Value != "half_up_min_one" && !(p.Contract != nil && (p.Fields["rounding_mode"].Value == "rc48_ratio" || p.Fields["rounding_mode"].Value == "rc48_tiered")) {
		return "historical_rounding_unknown"
	}
	for _, key := range []string{"violation_fee_code", "subscription_id", "subscription_pre_consumed", "credit_usage", "kiro_billable_input_tokens", "unsupported_cost_context"} {
		f := p.Fields[key]
		if f.State != "missing" && f.State != "" {
			return "unsupported_cost_context"
		}
	}
	return ""
}
func tokenPrice(p tokenMeteringParsed, policy tokenDetectionPolicy, scale *big.Rat) tokenPriceResult {
	p = tokenPricingOperands(p)
	r := tokenPriceResult{Method: "historical_formula_v1", Subject: p.Fields["billing_source"].Value, Unit: p.Fields["currency"].Value, InputScale: scale.RatString()}
	if r.Subject == "wallet" {
		r.Subject = "customer_wallet"
	}
	if unit, err := tokenRat(p, "quota_per_unit"); err == nil && unit.Sign() > 0 {
		r.QuotaPerUnit = unit.RatString()
	}
	if reason := tokenPricingGuard(p, policy); reason != "" {
		r.Reason = reason
		return r
	}
	u, e := tokenRat(p, "quota_per_unit")
	if e != nil || u.Sign() <= 0 {
		r.Reason = "historical_quota_unit_unknown"
		return r
	}
	r.QuotaPerUnit = u.RatString()
	group, e := tokenRat(p, "effective_group_ratio")
	if e != nil {
		r.Reason = "effective_group_ratio_missing"
		return r
	}
	if scale.Sign() < 0 || scale.Cmp(big.NewRat(1, 1)) > 0 {
		r.Reason = "invalid_input_scale"
		return r
	}
	var amount *big.Rat
	switch p.Fields["billing_mode"].Value {
	case "ratio":
		if p.Semantic != "anthropic_native" && !(p.Adapter == "newapi-rc48" && p.Semantic == "openai_inclusive") {
			r.Reason = "ordinary_semantic_unsupported"
			return r
		}
		fixed, err := tokenRat(p, "model_price")
		ratioSentinel := p.Adapter == "newapi-rc48" && p.Fields["model_price"].State == "sentinel" && p.Fields["model_price"].Raw == "-1"
		if !ratioSentinel && (err != nil || fixed.Sign() != 0) {
			r.Reason = "fixed_price_unsupported"
			return r
		}
		a, err := tokenWeightedInput(p)
		if err != nil {
			r.Reason = err.Error()
			return r
		}
		output, err := tokenRat(p, "completion_tokens")
		if err != nil {
			r.Reason = err.Error()
			return r
		}
		completion, err := tokenRat(p, "completion_ratio")
		if err != nil {
			r.Reason = err.Error()
			return r
		}
		model, err := tokenRat(p, "effective_model_ratio")
		if p.Fields["effective_model_ratio"].State == "missing" || p.Fields["effective_model_ratio"].State == "" {
			model, err = tokenRat(p, "model_ratio")
		}
		if err != nil {
			r.Reason = err.Error()
			return r
		}
		amount = new(big.Rat).Add(new(big.Rat).Mul(a, scale), new(big.Rat).Mul(output, completion))
		amount.Mul(amount, model)
	case "tiered_expr":
		if p.Adapter == "newapi-rc48" && p.Fields["billing_unit"].Value != "token" {
			r.Reason = "billing_unit_not_token"
			return r
		}
		if r.Unit != "USD" {
			r.Reason = "tiered_unit_unsupported"
			return r
		}
		if p.Fields["expr_b64"].State != "valid" || p.Fields["expr_b64"].Kind != "tiered_numeric_v1" && !tokenMeteringRecordedRulesUsable(p) || p.Fields["matched_tier"].State != "valid" {
			r.Reason = "tiered_expression_or_tier_unsupported"
			return r
		}
		expr, err := metering.ParsePriceExpression(p.Fields["expr_b64"].Value)
		if err != nil {
			r.Reason = "tiered_expression_unsupported"
			return r
		}
		vars, err := tokenTierVariables(p, expr, scale)
		if err != nil {
			r.Reason = err.Error()
			return r
		}
		amount, r.Tier, err = expr.Evaluate(vars)
		if err != nil {
			r.Reason = "tiered_evaluation_failed"
			return r
		}
		if p.Adapter == "newapi-rc48" {
			floats := make(map[string]float64, len(vars))
			for k, v := range vars {
				floats[k], _ = v.Float64()
			}
			value, tier, err := expr.EvaluateFloat(floats)
			if err != nil {
				r.Reason = "tiered_evaluation_failed"
				return r
			}
			if scale.Cmp(big.NewRat(1, 1)) == 0 && tier != p.Fields["matched_tier"].Value {
				r.Reason = "historical_tier_mismatch"
				return r
			}
			unitFloat, _ := u.Float64()
			groupFloat, _ := group.Float64()
			// Preserve the producer's operation order and final math.Round. An
			// exact-rational half-up at this boundary can differ by one quota.
			value = (value / 1000000 * unitFloat) * groupFloat
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || math.Round(value) > math.MaxInt32 {
				r.Reason = "quota_saturation_unsupported"
				return r
			}
			r.Method, r.Tier = "newapi_rc48_tiered_float64_v1", tier
			if tokenMeteringRecordedRulesUsable(p) {
				r.Method = "newapi_rc48_recorded_rules_float64_v1"
			}
			r.RawQuota, r.Quota = strconv.FormatFloat(value, 'g', -1, 64), strconv.FormatInt(int64(math.Round(value)), 10)
			r.Supported = true
			return r
		}
		if scale.Cmp(big.NewRat(1, 1)) == 0 && r.Tier != p.Fields["matched_tier"].Value {
			r.Reason = "historical_tier_mismatch"
			return r
		}
		amount.Mul(amount, u)
		amount.Quo(amount, big.NewRat(1000000, 1))
	default:
		r.Reason = "billing_mode_unsupported"
		return r
	}
	amount.Mul(amount, group)
	if amount.Sign() < 0 || amount.Num().BitLen() > 4096 || amount.Denom().BitLen() > 4096 {
		r.Reason = "quota_out_of_range"
		return r
	}
	r.RawQuota = amount.RatString()
	q := new(big.Int).Quo(new(big.Int).Add(new(big.Int).Mul(amount.Num(), big.NewInt(2)), amount.Denom()), new(big.Int).Mul(amount.Denom(), big.NewInt(2)))
	if p.Fields["rounding_mode"].Value == "rc48_ratio" {
		output, _ := tokenRat(p, "completion_tokens")
		model, _ := tokenRat(p, "model_ratio")
		billable := *p.TotalInput > 0 || output != nil && output.Sign() > 0
		if !billable {
			q.SetInt64(0)
		} else if q.Sign() == 0 && model != nil && model.Sign() > 0 && group.Sign() > 0 {
			q.SetInt64(1)
		}
	}
	if p.Fields["rounding_mode"].Value == "half_up_min_one" && amount.Sign() > 0 && q.Sign() == 0 {
		q.SetInt64(1)
	}
	if !q.IsInt64() || p.Adapter == "newapi-rc48" && q.Cmp(big.NewInt(2147483647)) > 0 {
		r.Reason = "quota_saturation_unsupported"
		return r
	}
	r.Quota = q.String()
	r.Supported = true
	return r
}
func tokenWeightedInput(p tokenMeteringParsed) (*big.Rat, error) {
	if p.Adapter == "newapi-rc48" && p.Contract != nil {
		return tokenRC48WeightedInput(p)
	}
	if p.Semantic != "anthropic_native" || p.TotalInput == nil || p.UnassignedCacheWrite == nil {
		return nil, errors.New("cache_split_incomplete")
	}
	a, e := tokenRat(p, "prompt_tokens")
	if e != nil {
		return nil, e
	}
	for _, pair := range [][2]string{{"cache_tokens", "cache_ratio"}, {"cache_creation_tokens_5m", "cache_creation_ratio_5m"}, {"cache_creation_tokens_1h", "cache_creation_ratio_1h"}} {
		n, e := tokenRat(p, pair[0])
		if e != nil {
			return nil, e
		}
		price, e := tokenRat(p, pair[1])
		if e != nil {
			return nil, e
		}
		a.Add(a, new(big.Rat).Mul(n, price))
	}
	rest, e := tokenRat(p, "cache_creation_ratio")
	if e != nil {
		return nil, e
	}
	a.Add(a, new(big.Rat).Mul(big.NewRat(*p.UnassignedCacheWrite, 1), rest))
	return a, nil
}
func tokenTierVariables(p tokenMeteringParsed, expr *metering.PriceExpression, scale *big.Rat) (map[string]*big.Rat, error) {
	if p.Adapter == "newapi-rc48" && p.Contract != nil {
		return tokenRC48TierVariables(p, expr, scale)
	}
	if p.Semantic == "openai_inclusive" && p.UnassignedCacheWrite == nil {
		write, e1 := tokenRat(p, "cache_creation_tokens")
		five, e2 := tokenRat(p, "cache_creation_tokens_5m")
		hour, e3 := tokenRat(p, "cache_creation_tokens_1h")
		if e1 == nil && e2 == nil && e3 == nil && write.Cmp(new(big.Rat).Add(five, hour)) == 0 {
			zero := int64(0)
			p.UnassignedCacheWrite = &zero
		}
	}
	if p.TotalInput == nil || p.UnassignedCacheWrite == nil || *p.UnassignedCacheWrite != 0 {
		return nil, errors.New("tiered_cache_split_unsupported")
	}
	vars := map[string]*big.Rat{}
	for name, key := range map[string]string{"p": "prompt_tokens", "c": "completion_tokens", "cr": "cache_tokens", "cc": "cache_creation_tokens_5m", "cc1h": "cache_creation_tokens_1h"} {
		n, e := tokenRat(p, key)
		if e != nil {
			return nil, e
		}
		vars[name] = n
	}
	vars["len"] = big.NewRat(*p.TotalInput, 1)
	switch p.Semantic {
	case "anthropic_native":
		if !expr.Used["cr"] {
			vars["p"].Add(vars["p"], vars["cr"])
		}
	case "openai_inclusive":
		for _, key := range []string{"cr", "cc", "cc1h"} {
			if expr.Used[key] {
				vars["p"].Sub(vars["p"], vars[key])
			}
		}
		if vars["p"].Sign() < 0 {
			return nil, errors.New("tiered_overlapping_cache_unsupported")
		}
	default:
		return nil, errors.New("tiered_semantic_unsupported")
	}
	for key, n := range vars {
		if key != "c" {
			n.Mul(n, scale)
		}
	}
	return vars, nil
}
