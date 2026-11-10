package monitor

import (
	"errors"
	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"math"
	"math/big"
)

// These operands implement the versioned text-only paths in rc.48's
// service/text_quota.go and service/tiered_settle.go. Optional raw fields are
// never rewritten: tokenRat can use only explicitly recorded or contract-
// justified operands; null/invalid values still fail closed.
func tokenRC48WeightedInput(p tokenMeteringParsed) (*big.Rat, error) {
	base, e := tokenRat(p, "prompt_tokens")
	if e != nil {
		return nil, e
	}
	read, e := tokenRat(p, "cache_tokens")
	if e != nil {
		return nil, e
	}
	write, e := tokenRat(p, "cache_creation_tokens")
	if e != nil {
		return nil, e
	}
	sum := new(big.Rat)
	add := func(n *big.Rat, key string) error {
		if n.Sign() == 0 {
			return nil
		}
		price, e := tokenRat(p, key)
		if e != nil {
			return e
		}
		sum.Add(sum, new(big.Rat).Mul(n, price))
		return nil
	}
	if e = add(read, "cache_ratio"); e != nil {
		return nil, e
	}
	switch p.Semantic {
	case "anthropic_native":
		five, e := tokenRat(p, "cache_creation_tokens_5m")
		if e != nil {
			return nil, e
		}
		hour, e := tokenRat(p, "cache_creation_tokens_1h")
		if e != nil {
			return nil, e
		}
		rest := new(big.Rat).Sub(write, new(big.Rat).Add(five, hour))
		if rest.Sign() < 0 {
			return nil, errors.New("cache_ttl_conflict")
		}
		for _, v := range []struct {
			n   *big.Rat
			key string
		}{{rest, "cache_creation_ratio"}, {five, "cache_creation_ratio_5m"}, {hour, "cache_creation_ratio_1h"}} {
			if e := add(v.n, v.key); e != nil {
				return nil, e
			}
		}
	case "openai_inclusive":
		long, e := tokenRat(p, "cache_creation_tokens_12h")
		if e != nil {
			return nil, e
		}
		rest := new(big.Rat).Sub(write, long)
		if rest.Sign() < 0 {
			return nil, errors.New("cache_ttl_conflict")
		}
		if e = add(rest, "cache_creation_ratio"); e != nil {
			return nil, e
		}
		if e = add(long, "cache_creation_ratio_12h"); e != nil {
			return nil, e
		}
		base.Sub(base, read)
		base.Sub(base, write)
		if base.Sign() < 0 {
			base.SetInt64(0)
		}
	default:
		return nil, errors.New("ordinary_semantic_unsupported")
	}
	return sum.Add(sum, base), nil
}

func tokenRC48TierVariables(p tokenMeteringParsed, expr *metering.PriceExpression, scale *big.Rat) (map[string]*big.Rat, error) {
	// BuildTieredTokenParams performs every addition and TTL multiplication
	// in float64, before expression evaluation. Rounding only the final exact
	// rational would change a half-quota boundary (notably the 1h multiplier).
	v := map[string]float64{}
	for name, key := range map[string]string{"p": "prompt_tokens", "c": "completion_tokens", "cr": "cache_tokens", "cc": "cache_creation_tokens", "cc1h": "cache_creation_tokens_1h", "cc12h": "cache_creation_tokens_12h"} {
		n, e := tokenRat(p, key)
		if e != nil {
			return nil, e
		}
		v[name], _ = n.Float64()
	}
	if p.TotalInput == nil {
		return nil, errors.New("usage_incomplete")
	}
	v["len"] = v["p"]
	switch p.Semantic {
	case "anthropic_native":
		// The unlabelled part of total creation is billed as 5-minute writes.
		write := tokenMeteringOperandInt(p, "cache_creation_tokens")
		hour := tokenMeteringOperandInt(p, "cache_creation_tokens_1h")
		if write == nil || hour == nil || *hour > *write {
			return nil, errors.New("cache_ttl_conflict")
		}
		v["cc"] = float64(*write - *hour)
		v["cc12h"] = 0
		v["len"] = v["p"] + v["cr"] + v["cc"] + v["cc1h"]
		if !expr.Used["cr"] {
			v["p"] += v["cr"]
		}
		if !expr.Used["cc1h"] {
			v["cc"] += v["cc1h"] * 1.6
			v["cc1h"] = 0
		}
		if !expr.Used["cc"] {
			v["p"] += v["cc"]
			v["cc"] = 0
		}
	case "openai_inclusive":
		v["cc1h"] = 0
		if expr.Used["cr"] {
			v["p"] -= v["cr"]
		}
		v["cc"] -= v["cc12h"]
		if v["cc"] < 0 {
			return nil, errors.New("cache_ttl_conflict")
		}
		count := v["cc"]
		if expr.Used["cc12h"] {
			v["p"] -= v["cc12h"]
		} else {
			count += v["cc12h"]
			v["cc"] += v["cc12h"] * 2
			v["cc12h"] = 0
		}
		if expr.Used["cc"] {
			v["p"] -= count
		} else {
			v["p"] += v["cc"] - count
			v["cc"] = 0
		}
		if v["p"] < 0 {
			v["p"] = 0
		}
	default:
		return nil, errors.New("tiered_semantic_unsupported")
	}
	factor, _ := scale.Float64()
	vars := make(map[string]*big.Rat, len(v))
	for key, n := range v {
		if key != "c" {
			n *= factor
		}
		if math.IsInf(n, 0) || math.IsNaN(n) || n < 0 {
			return nil, errors.New("tiered_operand_out_of_range")
		}
		vars[key] = new(big.Rat).SetFloat64(n)
	}
	return vars, nil
}
