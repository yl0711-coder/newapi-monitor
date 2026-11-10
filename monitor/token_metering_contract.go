package monitor

import (
	"maps"
	"strconv"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
)

// A source binding is evidence about a producer release in an explicit epoch
// and event-time range. It is not a channel's current price configuration.
// The runtime witness and immutable upstream source revision remain visible in
// every frozen decision. Empty/unverified bindings supply no defaults.
type tokenSourceContract struct {
	Version  string `json:"version"`
	Epoch    string `json:"source_epoch"`
	From     int64  `json:"from"`
	Through  int64  `json:"through,omitempty"`
	Evidence string `json:"evidence"`
}

const tokenRC48Contract = "newapi-rc48-text-v1"
const tokenRC48Source = "https://github.com/QuantumNous/new-api/blob/v1.0.0-rc.48/"

func tokenMeteringOperand(p tokenMeteringParsed, name string) metering.Field {
	f := p.Fields[name]
	if f.State == "missing" || f.State == "" {
		if d, ok := p.ContractOperands[name]; ok {
			return d
		}
	}
	return f
}

func tokenMeteringOperandInt(p tokenMeteringParsed, name string) *int64 {
	f := tokenMeteringOperand(p, name)
	if !tokenMeteringFieldUsable(f) {
		return nil
	}
	v, err := strconv.ParseInt(f.Value, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

func tokenApplySourceContract(p *tokenMeteringParsed, c *tokenSourceContract, epoch string, at int64) {
	if c == nil || c.Version != tokenRC48Contract || c.Epoch != epoch || at < c.From || c.Through > 0 && at >= c.Through || p.OtherState != "valid" {
		return
	}
	// These fields are always written by GenerateTextOtherInfo. Sparse or
	// redacted historical exports cannot acquire an emitter contract by name.
	for _, key := range []string{"cache_tokens", "cache_ratio", "model_ratio", "completion_ratio", "group_ratio", "billing_source"} {
		if !tokenMeteringFieldUsable(p.Fields[key]) {
			return
		}
	}
	path := p.Fields["request_path"].Value
	if path != "/v1/messages" && path != "/v1/chat/completions" && path != "/v1/responses" {
		return
	}
	if a := p.Fields["usage_adapter"]; tokenMeteringFieldUsable(a) && a.Value != "newapi-rc48" {
		return
	}
	copy := *c
	p.Contract = &copy
	p.Adapter = "newapi-rc48"
	p.Version = tokenMeteringParserVersion
	p.ContractOperands = map[string]metering.Field{}
	derive := func(k, v, source string) {
		if f := p.Fields[k]; f.State == "missing" || f.State == "" {
			p.ContractOperands[k] = metering.Field{State: "derived", Kind: tokenRC48Contract, Value: v, Reason: tokenRC48Source + source}
		}
	}
	// Absence has meaning ONLY inside this versioned emitter contract. Raw
	// fields remain missing, and the derived operand carries the exact basis.
	derive("usage_semantic", "openai", "service/text_quota.go#PostTextConsumeQuota")
	derive("effective_group_ratio", p.Fields["group_ratio"].Value, "service/text_quota.go#calculateTextQuotaSummary")
	derive("quota_per_unit", "500000", "common/constants.go#QuotaPerUnit")
	derive("currency", "USD", "pkg/billingexpr/settle.go#quotaConversion")
	derive("image", "false", "service/text_quota.go#PostTextConsumeQuota")
	derive("is_task", "false", "service/text_quota.go#PostTextConsumeQuota")
	derive("request_rules", "[]", "service/log_info_generate.go#InjectTieredBillingInfo")
	derive("request_conversion", "[]", "service/log_info_generate.go#appendRequestConversionChain")
	derive("is_model_mapped", "false", "service/log_info_generate.go#GenerateTextOtherInfo")
	derive("is_system_prompt_overwritten", "false", "service/log_info_generate.go#GenerateTextOtherInfo")
	if f := p.Fields["billing_mode"]; f.State == "missing" || f.State == "" {
		// -1 is the source's ratio-mode sentinel. It is neither a negative
		// price nor zero. Preserve it, and select only this documented branch.
		if price := p.Fields["model_price"]; price.State == "sentinel" && price.Raw == "-1" {
			derive("billing_mode", "ratio", "service/text_quota.go#calculateTextQuotaSummary")
		}
	}
	mode := tokenMeteringOperand(*p, "billing_mode").Value
	if mode == "ratio" {
		derive("rounding_mode", "rc48_ratio", "common/quota_math.go#QuotaFromDecimalChecked")
	}
	if mode == "tiered_expr" {
		derive("rounding_mode", "rc48_tiered", "pkg/billingexpr/settle.go#ComputeTieredQuotaWithRequest")
	}
	for _, k := range []string{"cache_creation_tokens_5m", "cache_creation_tokens_1h", "cache_creation_tokens_12h"} {
		derive(k, "0", "service/text_quota.go#PostTextConsumeQuota")
	}
	if tokenMeteringOperand(*p, "usage_semantic").Value == "openai" {
		derive("cache_creation_tokens", "0", "service/text_quota.go#PostTextConsumeQuota")
	}
}

func tokenInputForPolicy(f TokenMeteringFact, policy tokenDetectionPolicy) (tokenDetectionInput, error) {
	in, err := tokenDetectionInputOf(f)
	if err != nil {
		return in, err
	}
	if in.Usage.Contract != nil {
		// A changed/removed binding must not inherit the previous live adapter.
		// Immutable decision snapshots retain their original contract separately.
		in.Usage.Contract, in.Usage.ContractOperands = nil, nil
		in.Usage.Adapter = "unknown"
		normalizeTokenMetering(Settings{}, &in.Usage)
	}
	if policy.SourceContract != nil {
		in.Usage.Fields = maps.Clone(in.Usage.Fields)
		tokenApplySourceContract(&in.Usage, policy.SourceContract, f.SourceEpoch, f.CreatedAt)
		normalizeTokenMetering(Settings{}, &in.Usage)
	}
	return in, nil
}

func tokenSourceContractValid(c *tokenSourceContract) bool {
	return c == nil || c.Version == tokenRC48Contract && tokenMeteringName.MatchString(c.Epoch) && c.From > 0 && (c.Through == 0 || c.Through > c.From) && len(c.Evidence) >= 16 && len(c.Evidence) <= 512 && !strings.ContainsAny(c.Evidence, "\r\n")
}
