package usagebill

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// TokenRuleVersion is independent of the money rule and historical price table.
const TokenRuleVersion = "newapi-text-tokens-v1"

// LogContract must be selected from verified source provenance, never a model
// name, request path, current deployment version, or guessed upgrade date.
type LogContract string

const (
	// ContractRC4 refers to the pinned rc.4 text log producer in the design record.
	ContractRC4 LogContract = "newapi-rc4-text"
	// ContractRC26 refers to the pinned rc.26 text log producer in the design record.
	ContractRC26 LogContract = "newapi-rc26-text"
)

// CountState distinguishes absent, explicit null, invalid, and valid zero.
type CountState string

const (
	CountMissing CountState = "missing"
	CountNull    CountState = "null"
	CountInvalid CountState = "invalid"
	CountValid   CountState = "valid"
)

// TokenCount retains a proven nonnegative int64 and how it was obtained.
// Value is meaningful only when State is CountValid.
type TokenCount struct {
	State  CountState
	Value  int64
	Source string // recorded / omitted_zero / derived
}

// SourceTokens contains only original usage counts and bounded log metadata.
// The caller must independently prove text scope and wallet/subscription status.
type SourceTokens struct {
	Contract   LogContract
	Prompt     *int64
	Completion *int64
	Other      string
}

// TokenUsage is not a bill or a completeness proof for source records.
// Split writes remain separate for future historical price matching.
type TokenUsage struct {
	OrdinaryInput TokenCount
	Output        TokenCount
	CacheRead     TokenCount
	CacheWrite    TokenCount
	Write5m       TokenCount
	Write1h       TokenCount
	WriteUnsplit  TokenCount
	Semantic      string
	Complete      bool
	Issue         string
}

func countFromPointer(value *int64) TokenCount {
	if value == nil {
		return TokenCount{State: CountMissing}
	}
	if *value < 0 {
		return TokenCount{State: CountInvalid}
	}
	return TokenCount{State: CountValid, Value: *value, Source: "recorded"}
}

// Reject duplicate top-level keys and trailing documents; do not let JSON's
// last-key-wins rule silently pick one of two conflicting usage records.
func tokenObject(raw string) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 || len(raw) > 128<<10 {
		return nil, false
	}
	d := json.NewDecoder(strings.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, false
		}
		name, ok := key.(string)
		if !ok {
			return nil, false
		}
		if _, exists := fields[name]; exists {
			return nil, false
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, false
		}
		fields[name] = value
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') {
		return nil, false
	}
	var extra json.RawMessage
	if d.Decode(&extra) != io.EOF {
		return nil, false
	}
	return fields, true
}

func countFromJSON(raw json.RawMessage) TokenCount {
	if len(raw) == 0 {
		return TokenCount{State: CountMissing}
	}
	if bytes.Equal(raw, []byte("null")) {
		return TokenCount{State: CountNull}
	}
	invalid := TokenCount{State: CountInvalid}
	// Only JSON numbers, never numeric strings, booleans, NaN or huge exponents.
	if len(raw) > 64 || (raw[0] != '-' && (raw[0] < '0' || raw[0] > '9')) {
		return invalid
	}
	if pos := strings.IndexAny(string(raw), "eE"); pos >= 0 {
		exponent, err := strconv.Atoi(string(raw[pos+1:]))
		if err != nil || exponent < -18 || exponent > 18 {
			return invalid
		}
	}
	value, ok := new(big.Rat).SetString(string(raw))
	if !ok || !value.IsInt() || !value.Num().IsInt64() || value.Sign() < 0 {
		return invalid
	}
	return TokenCount{State: CountValid, Value: value.Num().Int64(), Source: "recorded"}
}

func omittedZero(count TokenCount, known bool) TokenCount {
	if count.State == CountMissing && known {
		return TokenCount{State: CountValid, Source: "omitted_zero"}
	}
	return count
}

// NormalizeTokens implements only the pinned text-producer semantics. Missing
// creation counts may become zero only under its proven zero-omission contract.
// Ambiguous counts never alter source facts or cause an actual charge to vanish.
func NormalizeTokens(source SourceTokens) TokenUsage {
	missing := TokenCount{State: CountMissing}
	out := TokenUsage{Output: countFromPointer(source.Completion), OrdinaryInput: missing, CacheRead: missing, CacheWrite: missing, Write5m: missing, Write1h: missing, WriteUnsplit: missing}
	fields, ok := tokenObject(source.Other)
	if !ok {
		out.Issue = "metadata_invalid"
		return out
	}
	known := source.Contract == ContractRC4 || source.Contract == ContractRC26
	semantic, semanticValid := "openai", true
	semanticRaw, semanticRecorded := fields["usage_semantic"]
	if semanticRecorded {
		semanticValid = !bytes.Equal(semanticRaw, []byte("null")) && json.Unmarshal(semanticRaw, &semantic) == nil && (semantic == "openai" || semantic == "anthropic")
	}
	out.CacheRead = countFromJSON(fields["cache_tokens"])
	total := countFromJSON(fields["cache_creation_tokens"])
	five, hour := countFromJSON(fields["cache_creation_tokens_5m"]), countFromJSON(fields["cache_creation_tokens_1h"])
	// GenerateClaudeOtherInfo always writes its creation total, even at zero.
	// Only GenerateTextOtherInfo's subsequent creation fields omit zero totals.
	if semanticValid && semantic == "anthropic" && total.State == CountMissing {
		out.CacheWrite, out.Write5m, out.Write1h = total, five, hour
		out.Issue = "write_total_missing"
		return out
	}
	// A split without its total is evidence, not proof of the whole write count.
	if total.State == CountMissing && (five.State != CountMissing || hour.State != CountMissing) {
		out.Write5m, out.Write1h, out.CacheWrite = five, hour, total
		out.Issue = "write_total_missing"
		return out
	}
	out.CacheWrite = omittedZero(total, known)
	out.Write5m, out.Write1h = omittedZero(five, known), omittedZero(hour, known)
	if !known {
		out.Issue = "source_contract_unknown"
		return out
	}
	prompt := countFromPointer(source.Prompt)
	for _, count := range []TokenCount{prompt, out.Output, out.CacheRead, out.CacheWrite, out.Write5m, out.Write1h} {
		if count.State != CountValid {
			out.Issue = "token_count_incomplete"
			return out
		}
	}
	if out.Write5m.Value > math.MaxInt64-out.Write1h.Value || out.Write5m.Value+out.Write1h.Value > out.CacheWrite.Value {
		out.Issue = "write_split_exceeds_total"
		return out
	}
	out.WriteUnsplit = TokenCount{State: CountValid, Value: out.CacheWrite.Value - out.Write5m.Value - out.Write1h.Value, Source: "derived"}
	// cache_write_tokens is a redundant UI total, not an extra billable category.
	if raw, exists := fields["cache_write_tokens"]; exists {
		uiTotal := countFromJSON(raw)
		if uiTotal.State != CountValid || uiTotal.Value != out.CacheWrite.Value {
			out.Issue = "write_total_conflict"
			return out
		}
	}
	var claude bool
	if raw, exists := fields["claude"]; exists && (bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &claude) != nil) {
		out.Issue = "format_flag_invalid"
		return out
	}
	if !semanticValid {
		out.Issue = "semantic_unknown"
		return out
	}
	if !semanticRecorded && claude {
		out.Issue = "semantic_missing_for_claude_format"
		return out
	}
	if semantic == "openai" && (claude || out.Write5m.Value != 0 || out.Write1h.Value != 0) {
		// Legacy Claude-derived conversions cannot be distinguished by format alone.
		out.Issue = "semantic_conversion_ambiguous"
		return out
	}
	base := prompt.Value
	if semantic == "openai" {
		// Compare before subtraction: do not clamp invalid/overlapping data to zero.
		if out.CacheRead.Value > base || out.CacheWrite.Value > base-out.CacheRead.Value {
			out.Issue = "cache_exceeds_input"
			return out
		}
		base -= out.CacheRead.Value + out.CacheWrite.Value
	}
	if raw, exists := fields["input_tokens_total"]; exists {
		explicit := countFromJSON(raw)
		expected := new(big.Int).SetInt64(prompt.Value)
		if semantic == "anthropic" {
			expected.Add(expected, big.NewInt(out.CacheRead.Value)).Add(expected, big.NewInt(out.CacheWrite.Value))
		}
		if explicit.State != CountValid || !expected.IsInt64() || explicit.Value != expected.Int64() {
			out.Issue = "input_total_conflict"
			return out
		}
	}
	out.OrdinaryInput = TokenCount{State: CountValid, Value: base, Source: "derived"}
	out.Semantic, out.Complete = semantic, true
	return out
}
