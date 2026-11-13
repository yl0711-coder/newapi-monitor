package monitor

import (
	"maps"
	"math/big"
	"sort"
)

// This read projection uses one frozen revision. It does not rewrite a saved
// decision/hash or equate HTTP bytes, statistical controls and upstream usage.
type tokenDetectionAssessment struct {
	State                    string   `json:"state"`
	EvidenceLevel            string   `json:"evidence_level"`
	LocalDifferenceConfirmed bool     `json:"local_difference_confirmed"`
	UpstreamConfirmed        bool     `json:"upstream_overcharge_confirmed"`
	UpstreamReason           string   `json:"upstream_reason"`
	Missing                  []string `json:"missing_fields"`
	RequestLength            *string  `json:"request_length_bytes"`
	LengthMeaning            string   `json:"request_length_meaning"`
	Input                    *string  `json:"input_tokens"`
	Output                   *string  `json:"output_tokens"`
	Model                    string   `json:"model"`
	Path                     string   `json:"request_path"`
	QuotaFrom                *string  `json:"normal_quota_from"`
	QuotaTo                  *string  `json:"normal_quota_to"`
	QuotaBasis               string   `json:"normal_quota_basis"`
	ActualQuota              *string  `json:"actual_quota"`
	FormulaDifference        *string  `json:"formula_difference_quota"`
	ReferenceUpper           *string  `json:"reference_input_upper"`
	ReferenceDifference      *string  `json:"reference_input_difference"`
}

func tokenAssessFrozen(f tokenDetectionFrozen) tokenDetectionAssessment {
	in, r := f.Input, f.Result
	u := in.Usage
	u.Fields = maps.Clone(u.Fields)
	if tokenMeteringDropUnverifiedContract(&u) {
		u.Adapter = "unknown"
		normalizeTokenMetering(Settings{}, &u)
	}
	lengthMeaning := in.Fact.RequestLengthMeaning
	if lengthMeaning == "" {
		lengthMeaning = "whole_http_request_not_body_or_token_count"
	}
	a := tokenDetectionAssessment{State: "evidence_insufficient", EvidenceLevel: "unavailable", Missing: []string{},
		UpstreamReason: "upstream_billing_evidence_missing", LengthMeaning: lengthMeaning,
		Model: in.Fact.Model, Input: tokenDisplayInteger(u.TotalInput), Output: tokenDisplayInteger(u.Fields["completion_tokens"].Int64()),
		ActualQuota: tokenDisplayInteger(u.Fields["quota"].Int64())}
	seenMissing := map[string]bool{}
	missing := func(key string) {
		if key != "" && !seenMissing[key] {
			seenMissing[key] = true
			a.Missing = append(a.Missing, key)
		}
	}
	// Formula eligibility depends only on its recorded operands and contract.
	// Optional route/customer metadata still appears in Missing, but cannot
	// erase a calculable logged-price difference or turn it into upstream proof.
	if in.Fact.LengthState == "valid" {
		a.RequestLength = tokenDisplayInteger(in.Fact.RequestLength)
	}
	if a.RequestLength == nil {
		missing("request_length:" + in.Fact.LengthState)
	}
	if in.Fact.LinkState != "unique" || in.Fact.EvidenceLevel != "exact" {
		missing("unique_entry_association:" + in.Fact.LinkState)
	}
	if a.Model == "" || a.Model == "[unsupported]" {
		missing("model")
	}
	if in.Fact.RequestPath != "" {
		a.Path = in.Fact.RequestPath
	} else if tokenMeteringFieldUsable(u.Fields["request_path"]) {
		a.Path = u.Fields["request_path"].Value
	}
	if a.Path == "" {
		missing("request_path:" + u.Fields["request_path"].State)
	}
	if _, ok := tokenComparisonEntry(in); !ok {
		missing("entry_route_status_or_length_version")
	}
	if a.Input == nil {
		missing("input_tokens:" + u.SemanticReason)
	}
	if a.Output == nil {
		missing("completion_tokens:" + u.Fields["completion_tokens"].State)
	}
	if a.ActualQuota == nil {
		missing("quota:" + u.Fields["quota"].State)
	}
	if !r.SourceComplete {
		missing("source_window_incomplete")
	}
	price := tokenPrice(u, f.Policy, big.NewRat(1, 1))
	if !price.Supported {
		missing("pricing_snapshot:" + price.Reason)
	}
	formulaComplete := price.Supported && a.ActualQuota != nil
	for _, item := range tokenMeteringDecodeMissing(in.Fact.MissingFields) {
		missing(item.Name + ":" + item.Reason)
	}
	for _, rule := range r.Rules {
		if (rule.Name == "input_inflation" || rule.Name == "cache_structure") && rule.Eligible && rule.Hit {
			a.State, a.EvidenceLevel = "statistical_candidate", "inferred"
		}
	}
	for _, rule := range r.Rules {
		if rule.Name == "usage_format" && rule.Hit {
			a.State, a.EvidenceLevel = "logged_usage_inconsistency", "exact"
		}
	}
	if price.Supported && a.ActualQuota != nil {
		expected, valid := new(big.Int).SetString(price.Quota, 10)
		actual, _ := new(big.Int).SetString(*a.ActualQuota, 10)
		if valid && expected.Sign() >= 0 {
			delta := new(big.Int).Sub(actual, expected).String()
			a.QuotaFrom, a.QuotaTo, a.FormulaDifference = &price.Quota, &price.Quota, &delta
			a.QuotaBasis = "recorded_usage_historical_formula"
			// A published local confirmation must reproduce the saved operands
			// and result, including rounding. An old unsupported decision remains
			// a logged difference with its exact gap; it never becomes supplier proof.
			reproduced := r.Price.Supported && r.Price.Quota == price.Quota && r.ActualQuota != nil && *r.ActualQuota == *a.ActualQuota && r.FormulaDifference != nil && *r.FormulaDifference == delta
			if !reproduced {
				missing("saved_formula_not_reproduced")
			}
			if delta != "0" {
				a.State, a.EvidenceLevel = "log_formula_difference", "exact"
				if formulaComplete && reproduced {
					a.State, a.LocalDifferenceConfirmed = "local_metering_difference", true
				}
			} else if !r.Candidate && formulaComplete && reproduced {
				a.State, a.EvidenceLevel = "local_formula_consistent", "exact"
			}
		}
	}
	if a.State == "evidence_insufficient" {
		for _, rule := range r.Rules {
			if rule.Eligible && !rule.Hit {
				a.State = "partially_evaluated"
				break
			}
		}
	}
	ref := r.Reference
	if ref.Status == "statistical" && !ref.Truncated && ref.SampleCount > 0 && ref.Upper != nil && *ref.Upper >= 0 && a.Input != nil {
		a.ReferenceUpper = tokenDisplayInteger(ref.Upper)
		delta := new(big.Int).Sub(big.NewInt(*u.TotalInput), big.NewInt(*ref.Upper)).String()
		a.ReferenceDifference = &delta
	} else {
		missing("statistical_reference:" + ref.Reason)
	}
	return a
}

type tokenDetectionRule struct {
	Name          string                 `json:"name"`
	Eligible      bool                   `json:"eligible"`
	Hit           bool                   `json:"hit"`
	FaultClass    string                 `json:"fault_class"`
	EvidenceLevel string                 `json:"evidence_level"`
	Reason        string                 `json:"reason"`
	Missing       []tokenRuleEvidenceGap `json:"missing_fields,omitempty"`
	Operands      map[string]string      `json:"operands,omitempty"`
}
type tokenDetectionResult struct {
	Version           string               `json:"version"`
	Candidate         bool                 `json:"candidate"`
	Quarantined       bool                 `json:"quarantined"`
	Rules             []tokenDetectionRule `json:"rules"`
	Reference         tokenReference       `json:"reference"`
	Price             tokenPriceResult     `json:"price"`
	ReferencePrice    *tokenPriceResult    `json:"reference_price"`
	ActualQuota       *string              `json:"actual_quota"`
	FormulaDifference *string              `json:"formula_difference_quota"`
	EstimatedLoss     *string              `json:"estimated_loss_quota"`
	EstimateReason    string               `json:"estimate_reason"`
	CacheSignal       bool                 `json:"cache_signal"`
	SourceComplete    bool                 `json:"source_complete"`
	Gaps              []string             `json:"gaps"`
}
type tokenDetectionFrozen struct {
	Version           string               `json:"version"`
	Input             tokenDetectionInput  `json:"input"`
	Policy            tokenDetectionPolicy `json:"policy"`
	Stage             int                  `json:"stage"`
	CachePriorSignals int64                `json:"cache_prior_signals"`
	Result            tokenDetectionResult `json:"result"`
}

func tokenRule(name string) tokenDetectionRule {
	return tokenDetectionRule{Name: name, FaultClass: "billing_anomaly", EvidenceLevel: "unavailable", Reason: "cannot_determine"}
}
func tokenFormatRule(u tokenMeteringParsed) tokenDetectionRule {
	r := tokenRule("usage_format")
	r.Eligible = true
	r.EvidenceLevel = "exact"
	r.Reason = "logged_format_consistent_not_usage_truth"
	keys := []string{"prompt_tokens", "completion_tokens", "quota", "is_stream"}
	for _, key := range []string{"input_tokens_total", "cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h"} {
		// TTL detail controls price reconstruction, not a known total input.
		// Keep absent subfields absent; still reject invalid/conflicting values
		// when recorded, without excluding every otherwise usable reference.
		required := u.Semantic == "anthropic_native" && (key == "cache_tokens" || key == "cache_creation_tokens")
		if required || (u.Fields[key].State != "missing" && u.Fields[key].State != "") {
			keys = append(keys, key)
		}
	}
	if f := u.Fields["cache_write_tokens"]; f.State != "missing" && f.State != "" {
		keys = append(keys, "cache_write_tokens")
	}
	gaps := false
	if u.OtherState != "valid" {
		if u.OtherState == "missing" || u.OtherState == "null" || u.OtherState == "size_limit" {
			gaps = true
		} else {
			r.Hit = true
			r.Reason = "other_format_invalid"
		}
	}
	for _, k := range keys {
		f := u.Fields[k]
		if f.State == "invalid" || f.State == "sentinel" {
			r.Hit = true
			r.Reason = "invalid_" + k
		} else if f.State != "valid" {
			gaps = true
		}
	}
	if n := u.Fields["is_stream"].Int64(); n != nil && *n != 0 && *n != 1 {
		r.Hit = true
		r.Reason = "invalid_is_stream"
	}
	if u.SemanticReason == "cache_ttl_conflict" || u.SemanticReason == "cache_total_conflict" || u.SemanticReason == "cache_exceeds_input" || u.SemanticReason == "input_total_overflow" {
		r.Hit = true
		r.Reason = u.SemanticReason
	}
	if recorded := u.Fields["input_tokens_total"].Int64(); recorded != nil && u.TotalInput != nil && *recorded != *u.TotalInput {
		r.Hit, r.Reason = true, "recorded_total_differs_from_usage_components"
	}
	if !r.Hit && gaps {
		r.Eligible = false
		r.FaultClass = "telemetry_gap"
		r.EvidenceLevel = "unavailable"
		r.Reason = "usage_fields_or_semantic_incomplete"
	}
	return r
}
func tokenDetectionEvaluate(in tokenDetectionInput, p tokenDetectionPolicy, ref tokenReference, prior int64, sourceComplete bool) tokenDetectionResult {
	u := in.Usage
	r := tokenDetectionResult{Version: tokenDetectionVersion, Rules: []tokenDetectionRule{}, Reference: ref, Price: tokenPrice(u, p, big.NewRat(1, 1)), EstimateReason: "reference_loss_not_supported", SourceComplete: sourceComplete, Gaps: []string{}}
	if q := u.Fields["quota"].Int64(); q != nil {
		s := big.NewInt(*q).String()
		r.ActualQuota = &s
	}
	format := tokenFormatRule(u)
	r.Rules = append(r.Rules, format)
	input := tokenRule("input_inflation")
	if p.InputRatio == "" {
		input.Reason = "threshold_unconfigured"
	} else if ref.Status != "statistical" || ref.Upper == nil || u.TotalInput == nil || ref.Truncated {
		input.Reason = ref.Reason
	} else {
		input.Eligible = true
		input.EvidenceLevel = "inferred"
		input.Reason = "statistical_comparison_not_token_truth"
		limit := new(big.Rat).Mul(big.NewRat(*ref.Upper, 1), tokenRatio(p.InputRatio))
		input.Hit = *u.TotalInput >= p.MinInput && big.NewRat(*u.TotalInput, 1).Cmp(limit) > 0
		if input.Hit {
			input.Reason = "input_exceeds_frozen_statistical_bound"
		}
	}
	r.Rules = append(r.Rules, input)
	cache := tokenRule("cache_structure")
	if p.CacheShareDrop == "" {
		cache.Reason = "threshold_unconfigured"
	} else {
		share, intensity := tokenCacheMetrics(u)
		if ref.Status != "statistical" || ref.Truncated || share == "" || intensity == "" {
			cache.Reason = "cache_reference_or_price_incomplete"
		} else {
			shares, intensities := []*big.Rat{}, []*big.Rat{}
			for _, s := range ref.Samples {
				if s.ReadShare != "" && s.Intensity != "" {
					shares = append(shares, tokenRatio(s.ReadShare))
					intensities = append(intensities, tokenRatio(s.Intensity))
				}
			}
			if len(shares) < p.MinSamples {
				cache.Reason = "cache_reference_insufficient"
			} else {
				sort.Slice(shares, func(i, j int) bool { return shares[i].Cmp(shares[j]) < 0 })
				sort.Slice(intensities, func(i, j int) bool { return intensities[i].Cmp(intensities[j]) < 0 })
				drop := new(big.Rat).Sub(shares[(len(shares)-1)/2], tokenRatio(share))
				limit := new(big.Rat).Mul(intensities[(len(intensities)-1)/2], tokenRatio(p.CacheIntensityRatio))
				cache.Eligible = true
				cache.EvidenceLevel = "inferred"
				cache.Reason = "cache_comparison_not_prefix_truth"
				r.CacheSignal = drop.Cmp(tokenRatio(p.CacheShareDrop)) >= 0 && tokenRatio(intensity).Cmp(limit) > 0
				cache.Hit = r.CacheSignal && prior+1 >= int64(p.CacheMinSustained)
				if r.CacheSignal {
					cache.Reason = "cache_shift_waiting_for_sustained_samples"
				}
				if cache.Hit {
					cache.Reason = "sustained_cache_cost_structure_shift"
				}
			}
		}
	}
	r.Rules = append(r.Rules, cache)
	formula := tokenRule("historical_price_difference")
	formula.Reason = r.Price.Reason
	if r.Price.Supported && r.ActualQuota != nil {
		formula.Eligible = true
		formula.EvidenceLevel = "exact"
		formula.Reason = "local_formula_consistent_not_upstream_verification"
		q, _ := new(big.Int).SetString(*r.ActualQuota, 10)
		expected, _ := new(big.Int).SetString(r.Price.Quota, 10)
		delta := new(big.Int).Sub(q, expected).String()
		r.FormulaDifference = &delta
		formula.Hit = delta != "0"
		if formula.Hit {
			formula.Reason = "logged_quota_differs_from_historical_formula"
		}
	}
	r.Rules = append(r.Rules, formula)
	if input.Hit && r.Price.Supported && r.ActualQuota != nil && u.TotalInput != nil && *u.TotalInput > 0 {
		scale := big.NewRat(min(*ref.Upper, *u.TotalInput), *u.TotalInput)
		estimate := tokenPrice(u, p, scale)
		r.ReferencePrice = &estimate
		if estimate.Supported {
			actual, _ := new(big.Int).SetString(*r.ActualQuota, 10)
			reference, _ := new(big.Int).SetString(estimate.Quota, 10)
			loss := new(big.Int).Sub(actual, reference)
			if loss.Sign() < 0 {
				loss.SetInt64(0)
			}
			v := loss.String()
			r.EstimatedLoss = &v
			r.EstimateReason = "input_categories_scaled_output_retained_upper_reference_v1"
		} else {
			r.EstimateReason = estimate.Reason
		}
	} else if !r.Price.Supported {
		r.EstimateReason = r.Price.Reason
	} else if cache.Hit {
		r.EstimateReason = "cache_prefix_and_ttl_unverified_no_loss"
	}
	for i := range r.Rules {
		rule := &r.Rules[i]
		rule.Operands = tokenRuleOperands(in, *rule, r)
		if !rule.Eligible {
			rule.Missing = tokenRuleMissingEvidence(in, *rule, ref)
		}
		r.Candidate = r.Candidate || rule.Hit
		if !rule.Eligible {
			r.Gaps = append(r.Gaps, rule.Name+":"+rule.Reason)
		}
	}
	r.Quarantined = r.Candidate || r.CacheSignal || !format.Eligible
	if !sourceComplete {
		r.Gaps = append(r.Gaps, "source_window_incomplete")
	}
	return r
}
