package monitor

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"sort"
	"strings"
	"unicode"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
)

type tokenMeteringRaw struct {
	ID, CreatedAt, UserID, ChannelID      int64
	Model, Group                          string
	Input, Output, Quota, Stream, UseTime sql.NullString
	Other                                 sql.NullString
	OtherBytes                            int64
	RequestID                             string
}
type tokenMeteringParsed struct {
	Version              string                      `json:"version"`
	Source               string                      `json:"source"`
	Adapter              string                      `json:"adapter"`
	Fields               map[string]metering.Field   `json:"fields"`
	MissingFields        []tokenMeteringMissingField `json:"missing_fields,omitempty"`
	OtherState           string                      `json:"other_state"`
	Semantic             string                      `json:"semantic"`
	SemanticReason       string                      `json:"semantic_reason"`
	TotalInput           *int64                      `json:"total_input"`
	UnassignedCacheWrite *int64                      `json:"unassigned_cache_write"`
	PricingSupport       string                      `json:"pricing_support"`
	Contract             *tokenSourceContract        `json:"source_contract,omitempty"`
	ContractOperands     map[string]metering.Field   `json:"contract_operands,omitempty"`
}

// Missing source fields are kept as a compact, deterministic list.  A state
// such as null/invalid is useful to an operator, but it is not the same as a
// value of zero and must remain visible after a fact is frozen.
type tokenMeteringMissingField struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

func tokenMeteringSourceRoute(f TokenMeteringFact) (path, method string) {
	var usage tokenMeteringParsed
	if json.Unmarshal([]byte(f.SourceFacts), &usage) == nil {
		if field := usage.Fields["request_path"]; field.State == "valid" {
			path = field.Value
		}
		if field := usage.Fields["request_method"]; field.State == "valid" {
			method = field.Value
		}
	}
	return
}

func tokenMeteringMissingReason(field metering.Field, fallback string) string {
	if field.Reason != "" {
		return field.Reason
	}
	if field.State != "" {
		return "source_" + field.State
	}
	return fallback
}

func tokenMeteringAppendMissing(out *[]tokenMeteringMissingField, name, reason string) {
	if name == "" || reason == "" {
		return
	}
	for _, item := range *out {
		if item.Name == name {
			return
		}
	}
	*out = append(*out, tokenMeteringMissingField{Name: name, Reason: reason})
}

func tokenMeteringMissingJSON(items []tokenMeteringMissingField) string {
	if len(items) == 0 {
		return "[]"
	}
	items = append([]tokenMeteringMissingField(nil), items...)
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	body, _ := json.Marshal(items)
	return string(body)
}

func tokenMeteringDecodeMissing(raw string) []tokenMeteringMissingField {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var items []tokenMeteringMissingField
	if json.Unmarshal([]byte(raw), &items) != nil {
		return []tokenMeteringMissingField{{Name: "missing_fields", Reason: "stored_metadata_invalid"}}
	}
	return items
}

// Older facts did not keep a missing-field ledger. Describe only what is
// actually absent from that frozen fact; current user/channel settings cannot
// supply historical provenance. A missing raw ID needs bounded source replay.
func tokenMeteringLegacyMissing(f TokenMeteringFact, p tokenMeteringParsed) string {
	items := tokenMeteringDecodeMissing(f.MissingFields)
	for _, name := range []string{"prompt_tokens", "completion_tokens", "cache_tokens", "cache_creation_tokens"} {
		field := p.Fields[name]
		if field.State != "valid" {
			tokenMeteringAppendMissing(&items, name, tokenMeteringMissingReason(field, "not_recorded_in_fact"))
		}
	}
	if p.TotalInput == nil {
		tokenMeteringAppendMissing(&items, "input_tokens", firstNonEmptyTokenString(p.SemanticReason, "not_recorded_in_fact"))
	}
	if f.RequestID == "" {
		tokenMeteringAppendMissing(&items, "request_id", "legacy_source_replay_required")
	}
	lengthFromEntry := f.RequestLength != nil && f.LengthState == "valid"
	present := map[string]bool{
		"model": f.Model != "", "upstream_model_name": f.UpstreamModelName != "",
		"upstream_request_id": f.UpstreamRequestID != "", "request_path": f.RequestPath != "",
		"request_method": f.RequestMethod != "", "entry_event_id": f.EntryEventID != "",
		"request_length":             lengthFromEntry || p.Fields["request_length"].State == "valid",
		"request_length_version":     lengthFromEntry && f.RequestLengthVersion != "" || p.Fields["request_length_version"].State == "valid",
		"request_length_meaning":     lengthFromEntry && f.RequestLengthMeaning != "" || p.Fields["request_length_meaning"].State == "valid",
		"request_length_observed_at": lengthFromEntry && f.RequestLengthObservedAt > 0 || p.Fields["request_length_observed_at"].State == "valid",
		"pricing_snapshot":           f.PricingSnapshot != "", "pricing_effective_at": f.PricingEffectiveAt > 0,
		"pricing_adapter": f.PricingAdapter != "" && f.PricingAdapter != "unknown",
		"route_snapshot":  f.RouteSnapshot != "", "base_path_snapshot": f.BasePathSnapshot != "",
		"model_mapping_snapshot": f.ModelMappingSnapshot != "", "customer_name_snapshot": f.CustomerNameSnapshot != "",
		"customer_name_source": f.CustomerNameSource != "",
	}
	for name, exists := range present {
		if !exists {
			tokenMeteringAppendMissing(&items, name, "not_recorded_in_fact")
		}
	}
	return tokenMeteringMissingJSON(items)
}

func tokenMeteringMissingSet(raw string, name, reason string, present bool) string {
	items := tokenMeteringDecodeMissing(raw)
	filtered := items[:0]
	for _, item := range items {
		if item.Name != name {
			filtered = append(filtered, item)
		}
	}
	if !present {
		tokenMeteringAppendMissing(&filtered, name, reason)
	}
	return tokenMeteringMissingJSON(filtered)
}

func tokenMeteringOtherText(other map[string]json.RawMessage, keys ...string) (string, string) {
	for _, key := range keys {
		if raw, ok := other[key]; ok {
			field := metering.Text(raw, func(v string) bool {
				return v != "" && len(v) <= 256 && strings.IndexFunc(v, unicode.IsControl) < 0
			})
			if field.State == "valid" {
				return field.Value, "valid"
			}
			return "", tokenMeteringMissingReason(field, "source_text_unusable")
		}
	}
	return "", "source_missing"
}

func tokenMeteringOtherMethod(other map[string]json.RawMessage) (string, string) {
	value, state := tokenMeteringOtherText(other, "request_method", "method")
	if state != "valid" {
		return "", state
	}
	switch value {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD":
		return value, "valid"
	default:
		return "", "unsupported_method"
	}
}

func tokenMeteringOtherInt(other map[string]json.RawMessage, keys ...string) (*int64, string) {
	for _, key := range keys {
		if raw, ok := other[key]; ok {
			field := metering.Number(raw, true, true)
			if value := field.Int64(); value != nil {
				return value, "valid"
			}
			return nil, tokenMeteringMissingReason(field, "source_integer_unusable")
		}
	}
	return nil, "source_missing"
}

func tokenMeteringInt64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func tokenMeteringColumn(v sql.NullString, integer, nonnegative bool) metering.Field {
	if !v.Valid {
		return metering.Field{State: "null"}
	}
	raw, _ := json.Marshal(v.String)
	f := metering.Number(raw, integer, nonnegative)
	f.Kind = "sql_scalar"
	return f
}
func tokenMeteringDimension(s string, max int) string {
	if len(s) > max || strings.IndexFunc(s, unicode.IsControl) >= 0 {
		return "[unsupported]"
	}
	return s
}
func parseTokenMetering(s Settings, r tokenMeteringRaw, now int64) (TokenMeteringFact, []TokenMeteringRef) {
	p := tokenMeteringParsed{Version: tokenMeteringParserVersion, Source: "newapi.logs.type2", Adapter: tokenMeteringEffectiveAdapter(s), Fields: map[string]metering.Field{}, Semantic: "unsupported", SemanticReason: "adapter_unknown", PricingSupport: "operands_only_no_calculation"}
	for name, v := range map[string]sql.NullString{"prompt_tokens": r.Input, "completion_tokens": r.Output, "quota": r.Quota, "is_stream": r.Stream, "use_time": r.UseTime} {
		p.Fields[name] = tokenMeteringColumn(v, true, true)
	}
	other := map[string]json.RawMessage{}
	switch {
	case r.OtherBytes > tokenMeteringOtherBytes:
		p.OtherState = "size_limit"
	case !r.Other.Valid:
		p.OtherState = "null"
	case strings.TrimSpace(r.Other.String) == "":
		p.OtherState = "missing"
	default:
		parsed, err := metering.Object([]byte(r.Other.String), tokenMeteringOtherBytes, 12)
		if err != nil {
			p.OtherState = err.Error()
		} else {
			other = parsed
			p.OtherState = "valid"
		}
	}
	for _, name := range []string{"input_tokens_total", "cache_tokens", "cache_creation_tokens", "cache_write_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h", "cache_creation_tokens_12h", "subscription_id", "subscription_plan_id", "subscription_pre_consumed", "subscription_consumed", "subscription_remain", "subscription_total"} {
		p.Fields[name] = metering.Number(other[name], true, true)
	}
	// cache_write_tokens is a redundant total, never another input bucket.
	// Retain both values to detect conflicts; when only this total is present,
	// preserve its source provenance instead of replacing missing counts by zero.
	if p.Fields["cache_creation_tokens"].State == "missing" {
		if alias := p.Fields["cache_write_tokens"]; alias.State != "missing" {
			alias.Kind = "source_alias"
			alias.Reason = "cache_write_tokens"
			p.Fields["cache_creation_tokens"] = alias
		}
	}
	for _, name := range []string{"subscription_post_delta", "kiro_billable_input_tokens"} {
		p.Fields[name] = metering.Number(other[name], true, false)
	}
	for _, name := range []string{"model_price", "model_ratio", "completion_ratio", "group_ratio", "user_group_ratio", "cache_ratio", "cache_creation_ratio", "cache_creation_ratio_5m", "cache_creation_ratio_1h", "cache_creation_ratio_12h", "image_ratio", "quota_per_unit", "effective_model_ratio", "effective_group_ratio"} {
		p.Fields[name] = metering.Number(other[name], false, true)
	}
	p.Fields["credit_usage"] = metering.Number(other["credit_usage"], false, false)
	for _, name := range []string{"claude", "image", "is_model_mapped", "is_task", "is_system_prompt_overwritten"} {
		p.Fields[name] = metering.Boolean(other[name])
	}
	// Retain source transport/adapter metadata independently of a later edge
	// association. Missing or unknown length semantics cannot become body size.
	p.Fields["request_length"] = metering.Number(other["request_length"], true, true)
	p.Fields["request_length_observed_at"] = metering.Number(other["request_length_observed_at"], true, true)
	for _, name := range []string{"request_length_version", "request_length_meaning", "request_conversion"} {
		p.Fields[name] = metering.Text(other[name], func(v string) bool { return len(v) <= 256 && strings.IndexFunc(v, unicode.IsControl) < 0 })
	}
	if raw := other["request_conversion"]; len(raw) > 0 && p.Fields["request_conversion"].State != "valid" {
		p.Fields["request_conversion"] = metering.Boolean(raw)
		// Some producers record a bounded list of protocol conversion steps.
		// Preserve that source shape instead of declaring every array invalid.
		var steps []string
		if len(raw) <= 2048 && json.Unmarshal(raw, &steps) == nil && steps != nil && len(steps) <= 16 {
			valid := true
			for _, step := range steps {
				valid = valid && len(step) > 0 && len(step) <= 128 && strings.IndexFunc(step, func(r rune) bool {
					return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(" _./:-", r))
				}) < 0
			}
			if valid {
				encoded, _ := json.Marshal(steps)
				p.Fields["request_conversion"] = metering.Field{State: "valid", Kind: "source_conversion_steps", Value: string(encoded)}
			}
		}
	}
	for _, name := range []string{"usage_semantic", "billing_mode", "billing_source", "billing_unit", "currency", "rounding_mode", "violation_fee_code", "matched_tier"} {
		p.Fields[name] = metering.Text(other[name], tokenMeteringName.MatchString)
	}
	if raw := other["matched_tier"]; len(raw) > 0 && p.Fields["matched_tier"].State == "invalid" {
		p.Fields["matched_tier"] = metering.Number(raw, true, true)
	}
	p.Fields["upstream_model_name"] = metering.Text(other["upstream_model_name"], func(v string) bool { return v != "" && tokenMeteringDimension(v, 256) == v })
	p.Fields["response_model"] = metering.Text(other["response_model"], func(v string) bool { return v != "" && tokenMeteringDimension(v, 256) == v })
	if p.Fields["response_model"].State == "invalid" {
		if response, err := metering.Object(other["response_model"], 2048, 2); err == nil {
			field := metering.Text(response["returned_model"], func(v string) bool { return v != "" && tokenMeteringDimension(v, 256) == v })
			field.Kind = "source_response_model.returned_model"
			p.Fields["response_model"] = field
			for name, source := range map[string]string{"upstream_model_name": "upstream_model", "requested_model": "requested_model"} {
				if existing := p.Fields[name]; existing.State != "" && existing.State != "missing" {
					continue
				}
				v := metering.Text(response[source], func(v string) bool { return v != "" && tokenMeteringDimension(v, 256) == v })
				v.Kind = "source_response_model." + source
				p.Fields[name] = v
			}
		}
	}
	p.Fields["request_path"] = metering.Text(other["request_path"], func(v string) bool {
		return strings.HasPrefix(v, "/") && len(v) <= 256 && !strings.ContainsAny(v, "?#") && strings.IndexFunc(v, unicode.IsControl) < 0
	})
	requestMethod, requestMethodState := tokenMeteringOtherMethod(other)
	if requestMethodState == "valid" {
		p.Fields["request_method"] = metering.Field{State: "valid", Kind: "source_text", Value: requestMethod}
	} else {
		p.Fields["request_method"] = metering.Field{State: "missing", Reason: requestMethodState}
	}
	// Presence-only blocker: never retain task IDs, fee payloads or audit text.
	for _, key := range []string{"task_id", "audio_tokens", "image_tokens", "web_search", "file_search", "tool_usage", "quota_clamp", "pre_consume_fallback", "audio", "ws", "audio_input_seperate_price", "fixed_price", "billing_tokens", "image_cache_tokens"} {
		if _, present := other[key]; present {
			p.Fields["unsupported_cost_context"] = metering.Field{State: "invalid", Reason: "special_cost_present"}
		}
	}
	if admin, err := metering.Object(other["admin_info"], tokenMeteringOtherBytes, 8); err == nil {
		if _, present := admin["quota_saturation"]; present {
			p.Fields["unsupported_cost_context"] = metering.Field{State: "invalid", Reason: "quota_saturation_present"}
		}
	}
	p.Fields["expr_b64"] = tokenMeteringExpression(other["expr_b64"])
	p.Fields["request_rules"] = tokenMeteringRules(other["request_rules"])
	if p.Fields["request_rules"].Kind == "recorded_time_rules_v1" {
		p.Fields["expr_b64"] = tokenMeteringRecordedExpression(other["expr_b64"], p.Fields["request_rules"])
	}
	if p.Fields["billing_mode"].Value == "tiered_expr" {
		p.PricingSupport = "tiered_unsupported"
		if p.Fields["expr_b64"].State == "valid" && tokenMeteringFieldUsable(p.Fields["request_rules"]) {
			p.PricingSupport = "tiered_arithmetic_snapshot_only"
		}
	}
	// Unknown extension fields may describe additional charges. Keep their
	// presence only, without retaining names or values, and fail pricing closed.
	// Validate the producer contract before classifying extension fields.
	if adapter, ok := other["usage_adapter"]; ok {
		p.Fields["usage_adapter"] = metering.Text(adapter, func(v string) bool { return v == "newapi-rc26" || v == "newapi-rc48" })
		if p.Fields["usage_adapter"].State == "valid" {
			p.Adapter = p.Fields["usage_adapter"].Value
		}
	}
	for key := range other {
		if _, known := p.Fields[key]; !known && !tokenMeteringMetadataKey(key) {
			p.Fields["unsupported_cost_context"] = metering.Field{State: "invalid", Reason: "unknown_extension_present"}
		}
	}
	if _, present := other["tool_surcharges"]; present {
		p.Fields["unsupported_cost_context"] = metering.Field{State: "invalid", Reason: "tool_surcharges_present"}
	}
	// Copy only bounded, source-provided provenance.  These fields are never
	// looked up from current channel/user configuration; absence remains a
	// durable gap for historical rows.
	upstreamRequestID, upstreamRequestIDState := tokenMeteringOtherText(other, "upstream_request_id", "upstreamRequestID", "upstream_requestId")
	entryEventID, entryEventIDState := tokenMeteringOtherText(other, "event_id", "entry_event_id", "request_event_id")
	if entryEventIDState == "valid" && !nginxEvidenceFileIDPattern.MatchString(entryEventID) {
		entryEventID, entryEventIDState = "", "source_identifier_invalid"
	}
	routeSnapshot, routeSnapshotState := tokenMeteringOtherText(other, "route", "request_route")
	basePathSnapshot, basePathState := tokenMeteringOtherText(other, "base_path", "basePath")
	modelMappingSnapshot, modelMappingState := tokenMeteringOtherText(other, "model_mapping", "modelMapping")
	customerNameSnapshot, customerNameState := tokenMeteringOtherText(other, "customer_name", "username")
	customerNameSource, customerNameSourceState := tokenMeteringOtherText(other, "customer_name_source", "username_source")
	pricingEffectiveAt, pricingEffectiveState := tokenMeteringOtherInt(other, "pricing_effective_at", "price_effective_at")
	pricingAdapter, pricingAdapterState := tokenMeteringOtherText(other, "pricing_adapter", "price_adapter")
	pricingFields := map[string]metering.Field{}
	for _, name := range []string{"model_price", "model_ratio", "effective_model_ratio", "completion_ratio", "effective_group_ratio", "group_ratio", "user_group_ratio", "cache_ratio", "cache_creation_ratio", "cache_creation_ratio_5m", "cache_creation_ratio_1h", "quota_per_unit", "billing_mode", "billing_source", "billing_unit", "currency", "rounding_mode", "matched_tier"} {
		pricingFields[name] = p.Fields[name]
	}
	pricingSnapshotBytes, _ := json.Marshal(pricingFields)
	pricingSnapshot := string(pricingSnapshotBytes)
	if pricingAdapterState != "valid" {
		pricingAdapter = "unknown"
	}
	if requestMethodState == "valid" {
		field := p.Fields["request_method"]
		field.Kind = "source_metadata"
		p.Fields["request_method"] = field
	}
	// Keep the missing list stable and explicit.  It covers both usage
	// operands and the provenance needed to interpret them later.
	for _, name := range []string{"prompt_tokens", "completion_tokens", "quota", "is_stream", "cache_tokens", "cache_creation_tokens"} {
		field := p.Fields[name]
		if field.State != "valid" {
			tokenMeteringAppendMissing(&p.MissingFields, name, tokenMeteringMissingReason(field, "source_field_unusable"))
		}
	}
	if p.Fields["completion_tokens"].State != "valid" {
		tokenMeteringAppendMissing(&p.MissingFields, "output_tokens", tokenMeteringMissingReason(p.Fields["completion_tokens"], "source_field_unusable"))
	}
	if p.Fields["request_path"].State != "valid" {
		tokenMeteringAppendMissing(&p.MissingFields, "request_path", tokenMeteringMissingReason(p.Fields["request_path"], "source_path_missing"))
	}
	if requestMethodState != "valid" {
		tokenMeteringAppendMissing(&p.MissingFields, "request_method", requestMethodState)
	}
	for name, value := range map[string]string{"upstream_request_id": upstreamRequestIDState, "route_snapshot": routeSnapshotState, "base_path_snapshot": basePathState, "model_mapping_snapshot": modelMappingState, "customer_name_snapshot": customerNameState, "customer_name_source": customerNameSourceState, "pricing_effective_at": pricingEffectiveState, "pricing_adapter": pricingAdapterState} {
		if value != "valid" {
			tokenMeteringAppendMissing(&p.MissingFields, name, value)
		}
	}
	requestID := ""
	if r.RequestID != "" && len(r.RequestID) <= 256 && strings.IndexFunc(r.RequestID, unicode.IsControl) < 0 {
		requestID = r.RequestID
	}
	if r.RequestID == "" {
		tokenMeteringAppendMissing(&p.MissingFields, "request_id", "source_missing")
	} else if requestID == "" {
		tokenMeteringAppendMissing(&p.MissingFields, "request_id", "source_identifier_invalid")
	}
	if p.Fields["upstream_model_name"].State != "valid" {
		tokenMeteringAppendMissing(&p.MissingFields, "upstream_model_name", tokenMeteringMissingReason(p.Fields["upstream_model_name"], "source_missing"))
	}
	// A source event id is already an exact evidence identity. If it is absent,
	// the association worker may still fill it from a unique HMAC match; keep
	// that gap explicit until then.
	for _, name := range []string{"request_length", "request_length_version", "request_length_meaning", "request_length_observed_at", "request_conversion"} {
		if p.Fields[name].State != "valid" {
			tokenMeteringAppendMissing(&p.MissingFields, name, tokenMeteringMissingReason(p.Fields[name], "source_missing"))
		}
	}
	if entryEventIDState != "valid" {
		tokenMeteringAppendMissing(&p.MissingFields, "entry_event_id", entryEventIDState)
	}
	if policy, err := parseTokenDetectionPolicy(s.TokenDetectionPolicy); err == nil {
		tokenApplySourceContract(&p, policy.SourceContract, s.TokenFactsSourceEpoch, r.CreatedAt)
	}
	normalizeTokenMetering(s, &p)
	if p.TotalInput == nil {
		tokenMeteringAppendMissing(&p.MissingFields, "input_tokens", p.SemanticReason)
	}
	// Map iteration order must not manufacture a new fact revision on replay.
	sort.Slice(p.MissingFields, func(i, j int) bool { return p.MissingFields[i].Name < p.MissingFields[j].Name })
	body, _ := json.Marshal(p)
	model := tokenMeteringDimension(r.Model, 256)
	fact := TokenMeteringFact{SourceEpoch: s.TokenFactsSourceEpoch, LogID: r.ID, CreatedAt: r.CreatedAt, RequestID: requestID, UserID: r.UserID, ChannelID: r.ChannelID,
		Model: model, UpstreamModelName: p.Fields["upstream_model_name"].Value, UserGroup: tokenMeteringDimension(r.Group, 128), RequestMethod: requestMethod,
		RequestPath: p.Fields["request_path"].Value, UpstreamRequestID: upstreamRequestID, SourceEventID: entryEventID, EntryEventID: entryEventID, SourceFacts: string(body), MissingFields: tokenMeteringMissingJSON(p.MissingFields),
		PricingSnapshot: pricingSnapshot, PricingEffectiveAt: tokenMeteringInt64Value(pricingEffectiveAt), PricingAdapter: pricingAdapter, RouteSnapshot: routeSnapshot,
		BasePathSnapshot: basePathSnapshot, ModelMappingSnapshot: modelMappingSnapshot, CustomerNameSnapshot: customerNameSnapshot, CustomerNameSource: customerNameSource,
		CustomerNameObservedAt: func() int64 {
			if customerNameState == "valid" {
				return r.CreatedAt
			}
			return 0
		}(),
		Semantic: p.Semantic, SemanticReason: p.SemanticReason, ParserVersion: tokenMeteringParserVersion, Revision: 1,
		FirstSeenAt: now, LastSeenAt: now, LinkState: "waiting", LinkReason: "evidence_pending", LinkNextAt: now, LinkDeadline: now + tokenMeteringRepairSeconds, LengthState: "missing",
		RequestLengthMeaning: "whole_http_request_not_body_or_token_count", EvidenceLevel: "unavailable"}
	refs := []TokenMeteringRef{}
	if entryEventIDState == "valid" {
		refs = append(refs, TokenMeteringRef{SourceEpoch: s.TokenFactsSourceEpoch, LogID: r.ID, KeyID: "event-id", Hash: entryEventID, Version: "event-id/v1"})
	}
	// The initial retry window follows event-time retention for every supported
	// setting, including the default seven days. Later evidence changes have a
	// separate durable wake path and can reopen this schedule without deleting
	// an unresolved historical fact.
	fact.LinkDeadline = min(fact.LinkDeadline, fact.CreatedAt+s.tokenMeteringRetentionSeconds())
	if r.RequestID != "" && len(r.RequestID) <= 256 && strings.IndexFunc(r.RequestID, unicode.IsControl) < 0 {
		keys, err := s.tokenMeteringKeys()
		if err == nil {
			for _, key := range keys {
				refs = append(refs, TokenMeteringRef{SourceEpoch: s.TokenFactsSourceEpoch, LogID: r.ID, KeyID: key.ID, Hash: nginxEvidenceIDHMAC(key.Secret, "oneapi-request-id", r.RequestID), Version: "oneapi-request-id/hmac-sha256/v1"})
			}
		}
	}
	if len(refs) == 0 {
		fact.LinkState = "waiting"
		fact.LinkReason = "request_id_event_id_or_key_unavailable"
	}
	tokenMeteringSetFactHash(&fact, refs)
	return fact, refs
}

// Collection and migrations must use the same identity. Reference ordering is
// independent of key rotation/configuration order and of SQL query order.
func tokenMeteringSetFactHash(fact *TokenMeteringFact, refs []TokenMeteringRef) {
	refs = append([]TokenMeteringRef{}, refs...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].KeyID < refs[j].KeyID })
	hashInput, _ := json.Marshal(struct {
		Created, User, Channel, LengthObserved        int64
		Model, Group, Request, UpstreamRequest, Facts string
		Method, Path, UpstreamModel, Missing          string
		Pricing, Route, BasePath, Mapping             string
		PricingEffectiveAt, CustomerObservedAt        int64
		PricingAdapter, CustomerName, CustomerSource  string
		Refs                                          []TokenMeteringRef
	}{fact.CreatedAt, fact.UserID, fact.ChannelID, fact.RequestLengthObservedAt,
		fact.Model, fact.UserGroup, fact.RequestID, fact.UpstreamRequestID, fact.SourceFacts, fact.RequestMethod, fact.RequestPath,
		fact.UpstreamModelName, fact.MissingFields, fact.PricingSnapshot, fact.RouteSnapshot, fact.BasePathSnapshot,
		fact.ModelMappingSnapshot, fact.PricingEffectiveAt, fact.CustomerNameObservedAt, fact.PricingAdapter, fact.CustomerNameSnapshot, fact.CustomerNameSource, refs})
	hash := sha256.Sum256(hashInput)
	fact.FactHash = hex.EncodeToString(hash[:])
	fact.StoredBytes = int64(len(hashInput) + len(fact.EvidenceJSON) + len(fact.MissingFields) + len(fact.PricingSnapshot) + 768)
}

func tokenMeteringFieldUsable(f metering.Field) bool {
	return f.State == "valid" || f.State == "derived"
}

// Older parsers inferred fields from an epoch name or usage semantics from a
// final request protocol. Neither proves the missing source evidence, including
// while the background migration is unfinished.
func tokenMeteringDropUnverifiedContract(p *tokenMeteringParsed) bool {
	changed := p.SemanticReason == "rc26_source_contract" || p.SemanticReason == "rc26_claude_adapter"
	for key, field := range p.Fields {
		if field.Kind == "source_contract_v1" {
			p.Fields[key] = metering.Field{State: "missing"}
			changed = true
		}
	}
	return changed
}

// These keys are operational metadata or source-contract markers.  They do
// not change the billing operands and must not make every otherwise valid
// request fail closed merely because the upstream logs them.
func tokenMeteringMetadataKey(key string) bool {
	switch key {
	case "frt", "admin_info", "reasoning_effort", "request_conversion", "stream_status",
		"request_id", "event_id", "entry_event_id", "request_event_id", "request_path", "request_method", "method", "upstream_request_id", "upstreamRequestID", "upstream_requestId",
		"upstream_model_name", "route", "request_route", "base_path", "basePath", "model_mapping", "modelMapping",
		"customer_name", "username", "customer_name_source", "username_source", "pricing_effective_at", "price_effective_at", "pricing_adapter", "price_adapter", "cache_write_tokens",
		"task_id", "audio_tokens", "image_tokens", "web_search", "file_search", "tool_usage",
		"quota_clamp", "pre_consume_fallback", "tool_surcharges":
		return true
	default:
		return false
	}
}
func normalizeTokenMetering(s Settings, p *tokenMeteringParsed) {
	// Re-normalization must discard a previous successful interpretation before
	// examining corrected/missing evidence (including legacy inferred values).
	p.Semantic, p.SemanticReason = "unsupported", "adapter_unknown"
	p.TotalInput, p.UnassignedCacheWrite = nil, nil
	if p.OtherState != "valid" {
		p.SemanticReason = "other_unusable"
		return
	}
	semantic := tokenMeteringOperand(*p, "usage_semantic")
	bindings, err := s.tokenMeteringBindings()
	if err != nil {
		p.SemanticReason = "bindings_invalid"
		return
	}
	if tokenMeteringFieldUsable(semantic) {
		if v, ok := bindings[semantic.Value]; ok {
			p.Semantic = v
			p.SemanticReason = "explicit_bound_semantic"
		} else if v := tokenMeteringKnownSemantic(semantic.Value); v != "" {
			p.Semantic = v
			p.SemanticReason = "explicit_source_semantic"
			if semantic.State == "derived" {
				p.SemanticReason = "versioned_emitter_semantic"
			}
		} else {
			p.SemanticReason = "explicit_semantic_unsupported"
			return
		}
	} else if semantic.State != "missing" {
		p.SemanticReason = "explicit_semantic_unusable"
		return
	} else {
		// rc26 can log a Claude final protocol with an explicitly supplied
		// OpenAI usage semantic. Its text branch omits usage_semantic, so the
		// protocol flag alone cannot identify how prompt/cache counts overlap.
		return
	}
	input := p.Fields["prompt_tokens"].Int64()
	if input == nil {
		p.SemanticReason = "input_unusable"
		return
	}
	read, write := tokenMeteringOperandInt(*p, "cache_tokens"), tokenMeteringOperandInt(*p, "cache_creation_tokens")
	if redundant := p.Fields["cache_write_tokens"]; redundant.State == "valid" && write != nil && *redundant.Int64() != *write {
		p.SemanticReason = "cache_total_conflict"
		return
	}
	if p.Semantic == "openai_inclusive" {
		// rc26 permits overlapping OpenAI cache read/write prefixes. Validate
		// each subset separately; adding them can falsely flag valid usage.
		if read != nil && *read > *input || write != nil && *write > *input {
			p.SemanticReason = "cache_exceeds_input"
			return
		}
		p.TotalInput = input
		return
	}
	if read == nil || write == nil {
		p.SemanticReason = "cache_fields_unusable"
		return
	}
	total := new(big.Int).Add(big.NewInt(*input), big.NewInt(*read))
	total.Add(total, big.NewInt(*write))
	if !total.IsInt64() {
		p.SemanticReason = "input_total_overflow"
		return
	}
	five, hour := tokenMeteringOperand(*p, "cache_creation_tokens_5m"), tokenMeteringOperand(*p, "cache_creation_tokens_1h")
	// Unknown TTL subfields are not zeros; the total is known but its split is not.
	if tokenMeteringFieldUsable(five) && tokenMeteringFieldUsable(hour) {
		sum := new(big.Int).Add(big.NewInt(*tokenMeteringOperandInt(*p, "cache_creation_tokens_5m")), big.NewInt(*tokenMeteringOperandInt(*p, "cache_creation_tokens_1h")))
		if sum.Cmp(big.NewInt(*write)) > 0 {
			p.SemanticReason = "cache_ttl_conflict"
			return
		}
		rest := *write - sum.Int64()
		p.UnassignedCacheWrite = &rest
	} else if five.State == "invalid" || hour.State == "invalid" || five.State == "sentinel" || hour.State == "sentinel" {
		p.SemanticReason = "cache_ttl_unusable"
		return
	}
	value := total.Int64()
	p.TotalInput = &value
}

func tokenMeteringKnownSemantic(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "anthropic", "anthropic_native":
		return "anthropic_native"
	case "openai", "openai_inclusive":
		return "openai_inclusive"
	default:
		return ""
	}
}
func tokenMeteringRules(raw json.RawMessage) metering.Field {
	if len(raw) == 0 {
		return metering.Field{State: "missing"}
	}
	v := strings.TrimSpace(string(raw))
	if v == "null" {
		return metering.Field{State: "null"}
	}
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) == nil && len(values) == 0 && strings.HasPrefix(v, "[") {
		return metering.Field{State: "valid", Value: "[]"}
	}
	if rules, ok := tokenRecordedRules(raw); ok {
		body, _ := json.Marshal(rules)
		return metering.Field{State: "valid", Kind: "recorded_time_rules_v1", Value: string(body)}
	}
	return metering.Field{State: "invalid", Reason: "rules_not_supported_no_payload_retained"}
}
func tokenMeteringExpression(raw json.RawMessage) metering.Field {
	if len(raw) == 0 {
		return metering.Field{State: "missing"}
	}
	if string(raw) == "null" {
		return metering.Field{State: "null"}
	}
	f := metering.Field{State: "invalid", Reason: "expression_not_supported_no_payload_retained"}
	var encoded string
	if json.Unmarshal(raw, &encoded) != nil || len(encoded) > 2048 {
		return f
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	// Comments are ignored by Go's parser but may contain arbitrary private text.
	// Reject them before parsing; never persist the unvalidated source decoration.
	if err != nil || len(decoded) > 512 || strings.Contains(string(decoded), "/*") || strings.Contains(string(decoded), "//") {
		return f
	}
	if expr, parseErr := metering.ParsePriceExpression(string(decoded)); parseErr == nil {
		return metering.Field{State: "valid", Kind: "tiered_numeric_v1", Value: expr.Canonical}
	}
	expression, err := parser.ParseExpr(string(decoded))
	if err != nil || !tokenMeteringArithmetic(expression) {
		return f
	}
	return metering.Field{State: "valid", Kind: "arithmetic_v1", Value: string(decoded)}
}
func tokenMeteringArithmetic(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.ParenExpr:
		return tokenMeteringArithmetic(x.X)
	case *ast.Ident:
		switch x.Name {
		case "prompt_tokens", "completion_tokens", "cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h":
			return true
		}
	case *ast.BasicLit:
		if x.Kind != token.INT && x.Kind != token.FLOAT {
			return false
		}
		return metering.Number(json.RawMessage(x.Value), false, true).State == "valid"
	case *ast.BinaryExpr:
		switch x.Op {
		case token.ADD, token.SUB, token.MUL, token.QUO:
			return tokenMeteringArithmetic(x.X) && tokenMeteringArithmetic(x.Y)
		}
	case *ast.UnaryExpr:
		return (x.Op == token.ADD || x.Op == token.SUB) && tokenMeteringArithmetic(x.X)
	}
	return false
}
