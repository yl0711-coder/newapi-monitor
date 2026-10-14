package monitor

// This is an internal compatibility parser, NOT a customer response DTO.
// It deliberately does not replace parseLogOther or calculate/modify quota.
// Presentation must select and escape fields; do not serialize this structure.
import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

type portalEvidenceState string

const (
	portalEvidenceMissing portalEvidenceState = "missing"
	portalEvidenceNull    portalEvidenceState = "null"
	portalEvidenceValid   portalEvidenceState = "valid"
	portalEvidenceInvalid portalEvidenceState = "invalid"
)

type portalEvidenceField struct {
	State portalEvidenceState
	// Text preserves exact numeric JSON spelling, including values beyond 2^53.
	Text string
}

type portalLogEvidence struct {
	State        portalEvidenceState
	Mode         string // unknown, ratio, per_request, tiered_expr
	Numbers      map[string]portalEvidenceField
	Strings      map[string]portalEvidenceField
	Flags        map[string]portalEvidenceField
	Expression   portalEvidenceField // decoded, never evaluated or sent directly
	RulesState   portalEvidenceState
	Rules        []portalEvidenceRule
	ToolsState   portalEvidenceState
	Tools        []portalEvidenceTool
	ExtraBilling bool                  // presence only; never retain raw unknown billing metadata
	Request      portalRequestEvidence // typed allowlist; error text is bounded/redacted
}

type portalEvidenceTool struct {
	Name  portalEvidenceField
	Count portalEvidenceField
	Price portalEvidenceField // rc26 structured items: price per 1000 calls
}

type portalEvidenceRule struct {
	Condition  portalEvidenceField
	Multiplier portalEvidenceField
	Matched    portalEvidenceField
}

const portalEvidenceMaxBytes = 128 << 10

// Preserve the historical upstream wire spelling; this is not a display label.
const portalAudioInputSeparatePriceKey = "audio_input_seperate_price" //nolint:misspell // Upstream JSON compatibility.

// Missing and malformed fields are retained as states, never silently made zero.
// Only allowlisted fields are retained. Unknown/admin/raw content is discarded.
func parsePortalLogEvidence(raw string) portalLogEvidence {
	e := portalLogEvidence{State: portalEvidenceMissing, Mode: "unknown",
		Numbers: map[string]portalEvidenceField{}, Strings: map[string]portalEvidenceField{},
		Flags: map[string]portalEvidenceField{}, Expression: portalEvidenceField{State: portalEvidenceMissing}, RulesState: portalEvidenceMissing, ToolsState: portalEvidenceMissing}
	if strings.TrimSpace(raw) == "" {
		return e
	}
	if len(raw) > portalEvidenceMaxBytes {
		e.State = portalEvidenceInvalid
		return e
	}
	if strings.TrimSpace(raw) == "null" {
		e.State = portalEvidenceNull
		return e
	}
	fields, ok := portalEvidenceObject(raw)
	if !ok {
		e.State = portalEvidenceInvalid
		return e
	}
	e.State = portalEvidenceValid
	e.Request = parsePortalRequestEvidence(fields)
	// rc26 nests saturation evidence in admin_info. Retain ONLY its presence;
	// neither the administrator metadata nor its values reach customer output.
	if admin, valid := portalEvidenceObject(string(fields["admin_info"])); valid {
		if _, exists := admin["quota_saturation"]; exists {
			e.ExtraBilling = true
		}
	}
	for _, key := range []string{"other_ratios", "request_rules", "violation_fee_code", "quota_saturation"} {
		if _, exists := fields[key]; exists {
			e.ExtraBilling = true
		}
	}
	for _, key := range strings.Fields("model_ratio completion_ratio group_ratio user_group_ratio model_price cache_ratio cache_creation_ratio cache_creation_ratio_5m cache_creation_ratio_1h image_ratio audio_ratio audio_completion_ratio web_search_price file_search_price audio_input_price image_generation_call_price") {
		e.Numbers[key] = portalEvidenceNumber(fields[key], false, key == "model_price" || key == "user_group_ratio")
	}
	for _, key := range strings.Fields("cache_tokens cache_creation_tokens cache_creation_tokens_5m cache_creation_tokens_1h audio_input audio_output text_input text_output image_output web_search_call_count file_search_call_count audio_input_token_count image_generation_call_count subscription_id subscription_plan_id subscription_pre_consumed subscription_post_delta subscription_consumed subscription_remain subscription_total subscription_used wallet_quota_deducted") {
		e.Numbers[key] = portalEvidenceNumber(fields[key], true, key == "subscription_post_delta")
	}
	for _, key := range strings.Fields("billing_mode billing_source matched_tier expr_b64 reasoning_effort task_id usage_semantic request_path subscription_plan_title") {
		e.Strings[key] = portalEvidenceScalar(fields[key], "string")
	}
	for _, key := range append(strings.Fields("claude image audio ws is_task web_search file_search image_generation_call is_system_prompt_overwritten"), portalAudioInputSeparatePriceKey) {
		e.Flags[key] = portalEvidenceScalar(fields[key], "bool")
	}
	e.ToolsState, e.Tools = portalEvidenceTools(fields["tool_surcharges"])
	mode := e.Strings["billing_mode"]
	if mode.State == portalEvidenceInvalid || mode.State == portalEvidenceNull {
		return e
	}
	if mode.State == portalEvidenceValid && mode.Text != "" {
		if mode.Text == "tiered_expr" {
			e.Mode = "tiered_expr"
			e.RulesState, e.Rules = portalEvidenceRules(fields["request_rules"])
			expr := e.Strings["expr_b64"]
			e.Expression = expr
			if expr.State == portalEvidenceValid {
				decoded, err := base64.StdEncoding.Strict().DecodeString(expr.Text)
				if err != nil || len(decoded) == 0 || !utf8.Valid(decoded) {
					e.Expression = portalEvidenceField{State: portalEvidenceInvalid}
				} else {
					e.Expression.Text = string(decoded)
				}
			}
		}
		// Unknown explicit modes must not fall back to a plausible ratio formula.
		return e
	}
	price := e.Numbers["model_price"]
	if price.State == portalEvidenceInvalid || price.State == portalEvidenceNull {
		return e
	}
	if price.State == portalEvidenceValid {
		v, _ := new(big.Rat).SetString(price.Text)
		if v.Sign() < 0 && v.Cmp(big.NewRat(-1, 1)) != 0 {
			return e
		}
		if v.Sign() < 0 && (e.Flags["is_task"].Text == "true" || e.Strings["task_id"].Text != "") {
			return e // task settlement requires separate evidence, not a token formula
		}
		if v.Sign() > 0 {
			e.Mode = "per_request"
			return e
		}
	}
	if e.Numbers["model_ratio"].State == portalEvidenceValid {
		e.Mode = "ratio"
	}
	return e
}

func portalEvidenceTools(raw json.RawMessage) (portalEvidenceState, []portalEvidenceTool) {
	if len(raw) == 0 {
		return portalEvidenceMissing, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return portalEvidenceNull, nil
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil || len(items) > 128 {
		return portalEvidenceInvalid, nil
	}
	result := make([]portalEvidenceTool, 0, len(items))
	state := portalEvidenceValid
	for _, item := range items {
		fields, ok := portalEvidenceObject(string(item))
		if !ok {
			return portalEvidenceInvalid, nil
		}
		tool := portalEvidenceTool{
			Name:  portalEvidenceScalar(fields["name"], "string"),
			Count: portalEvidenceNumber(fields["count"], true, false),
			Price: portalEvidenceNumber(fields["price"], false, false),
		}
		if tool.Name.State == portalEvidenceValid && (strings.TrimSpace(tool.Name.Text) == "" || len(tool.Name.Text) > 256) {
			tool.Name = portalEvidenceField{State: portalEvidenceInvalid}
		}
		if tool.Name.State != portalEvidenceValid || tool.Count.State != portalEvidenceValid || tool.Price.State != portalEvidenceValid {
			state = portalEvidenceInvalid
		}
		result = append(result, tool)
	}
	return state, result
}

func portalEvidenceRules(raw json.RawMessage) (portalEvidenceState, []portalEvidenceRule) {
	if len(raw) == 0 {
		return portalEvidenceMissing, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return portalEvidenceNull, nil
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil || len(items) > 128 {
		return portalEvidenceInvalid, nil
	}
	rules := make([]portalEvidenceRule, 0, len(items))
	state := portalEvidenceValid
	for _, item := range items {
		fields, ok := portalEvidenceObject(string(item))
		if !ok {
			return portalEvidenceInvalid, nil
		}
		rule := portalEvidenceRule{
			Condition:  portalEvidenceScalar(fields["cond"], "string"),
			Multiplier: portalEvidenceNumber(fields["multiplier"], false, false),
			Matched:    portalEvidenceScalar(fields["matched"], "bool"),
		}
		if rule.Condition.State != portalEvidenceValid || rule.Multiplier.State != portalEvidenceValid || rule.Matched.State != portalEvidenceValid {
			state = portalEvidenceInvalid
		}
		rules = append(rules, rule)
	}
	return state, rules
}

// Reject duplicate top-level keys (rather than silently using last-write-wins)
// and trailing documents. Neither ambiguity is safe for billing explanations.
func portalEvidenceObject(raw string) (map[string]json.RawMessage, bool) {
	d := json.NewDecoder(strings.NewReader(raw))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return nil, false
	}
	m := map[string]json.RawMessage{}
	for d.More() {
		t, err = d.Token()
		key, ok := t.(string)
		if err != nil || !ok {
			return nil, false
		}
		if _, exists := m[key]; exists {
			return nil, false
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, false
		}
		m[key] = value
	}
	if t, err = d.Token(); err != nil || t != json.Delim('}') {
		return nil, false
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, false
	}
	return m, true
}

func portalEvidenceScalar(raw json.RawMessage, kind string) portalEvidenceField {
	f := portalEvidenceField{State: portalEvidenceMissing}
	if len(raw) == 0 {
		return f
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		f.State = portalEvidenceNull
		return f
	}
	f.State = portalEvidenceInvalid
	if kind == "string" {
		var s string
		if json.Unmarshal(raw, &s) == nil && utf8.Valid(raw) {
			return portalEvidenceField{State: portalEvidenceValid, Text: s}
		}
	} else if kind == "bool" {
		var b bool
		if json.Unmarshal(raw, &b) == nil {
			return portalEvidenceField{State: portalEvidenceValid, Text: strconv.FormatBool(b)}
		}
	}
	return f
}

func portalEvidenceNumber(raw json.RawMessage, integer, signed bool) portalEvidenceField {
	if len(raw) == 0 {
		return portalEvidenceField{State: portalEvidenceMissing}
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return portalEvidenceField{State: portalEvidenceNull}
	}
	bad := portalEvidenceField{State: portalEvidenceInvalid}
	text := strings.TrimSpace(string(raw))
	// Bound exponents and precision before big.Rat allocation. Numeric strings
	// are not silently coerced; add such compatibility only with source evidence.
	if len(text) > 80 || strings.ContainsAny(text, "\"") || !json.Valid(raw) {
		return bad
	}
	if i := strings.IndexAny(text, "eE"); i >= 0 {
		exponent, err := strconv.ParseInt(text[i+1:], 10, 32)
		if err != nil || exponent < -308 || exponent > 308 {
			return bad
		}
	}
	v, ok := new(big.Rat).SetString(text)
	if !ok || (!signed && v.Sign() < 0) {
		return bad
	}
	if integer && (!v.IsInt() || !v.Num().IsInt64()) {
		return bad
	}
	return portalEvidenceField{State: portalEvidenceValid, Text: text}
}
