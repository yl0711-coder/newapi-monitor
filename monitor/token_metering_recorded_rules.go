package monitor

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strings"
	"text/scanner"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
)

// Replay only the producer's recorded time-rule branches. We never substitute
// the collector's clock for settlement time or evaluate headers/request bodies.
// rc.48 records cond/matched/multiplier for each instrumented request rule:
// pkg/billingexpr/run.go and pkg/billingexpr/settle.go.
const tokenTimeAtom = `(?:hour|minute|weekday|month|day)\("[A-Za-z_+/\-]{1,64}"\)(?:>=|<=|==|!=|>|<)[0-9]{1,2}`
const tokenTimeCondition = tokenTimeAtom + `(?:(?:&&|\|\|)` + tokenTimeAtom + `)*`

var tokenRecordedCondition = regexp.MustCompile(`^` + tokenTimeCondition + `$`)
var tokenRecordedBranch = regexp.MustCompile(`\((` + tokenTimeCondition + `)\?([0-9]+(?:\.[0-9]+)?):1\)`)

type tokenRecordedRule struct {
	Condition  string      `json:"cond"`
	Matched    *bool       `json:"matched"`
	Multiplier json.Number `json:"multiplier"`
}

func tokenRecordedRules(raw json.RawMessage) ([]tokenRecordedRule, bool) {
	if len(raw) > 4096 {
		return nil, false
	}
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil || len(values) == 0 || len(values) > 8 {
		return nil, false
	}
	out := make([]tokenRecordedRule, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		var r tokenRecordedRule
		d := json.NewDecoder(strings.NewReader(string(value)))
		d.DisallowUnknownFields()
		if d.Decode(&r) != nil || r.Matched == nil {
			return nil, false
		}
		r.Condition = tokenRecordedCompact(r.Condition)
		if !tokenRecordedCondition.MatchString(r.Condition) || seen[r.Condition] {
			return nil, false
		}
		if n := metering.Number([]byte(r.Multiplier), false, true); n.State != "valid" {
			return nil, false
		}
		seen[r.Condition] = true
		out = append(out, r)
	}
	return out, true
}

func tokenMeteringRecordedExpression(raw json.RawMessage, rules metering.Field) metering.Field {
	failed := metering.Field{State: "invalid", Reason: "recorded_price_rules_unbound"}
	traces, valid := tokenRecordedRules([]byte(rules.Value))
	var encoded string
	if !valid || json.Unmarshal(raw, &encoded) != nil || len(encoded) > 4096 {
		return failed
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) > 2048 {
		return failed
	}
	// Only whitespace outside the whitelisted time conditions is normalized;
	// forbidden comments/extra functions still fail the numeric parser below.
	source := tokenRecordedCompact(string(decoded))
	recorded := source
	used := map[string]bool{}
	bad := false
	source = tokenRecordedBranch.ReplaceAllStringFunc(source, func(branch string) string {
		parts := tokenRecordedBranch.FindStringSubmatch(branch)
		for _, trace := range traces {
			if trace.Condition != parts[1] {
				continue
			}
			a, b := metering.Number([]byte(parts[2]), false, true), metering.Number([]byte(trace.Multiplier), false, true)
			if used[trace.Condition] || a.State != "valid" || a.Value != b.Value {
				bad = true
				return branch
			}
			used[trace.Condition] = true
			if *trace.Matched {
				return "(" + parts[2] + ")"
			}
			return "(1)"
		}
		bad = true
		return branch
	})
	if bad || len(used) != len(traces) {
		return failed
	}
	expr, err := metering.ParsePriceExpression(source)
	if err != nil {
		return failed
	}
	return metering.Field{State: "valid", Kind: "tiered_recorded_rules_v1", Raw: recorded, Value: expr.Canonical, Reason: "newapi.logs.other.expr_b64+request_rules;recorded_branch_not_clock_verification"}
}

func tokenRecordedCompact(source string) string {
	if len(source) > 4096 || strings.Contains(source, "//") || strings.Contains(source, "/*") {
		return ""
	}
	var s scanner.Scanner
	s.Init(strings.NewReader(source))
	s.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats | scanner.ScanStrings
	s.Error = func(*scanner.Scanner, string) {}
	var out strings.Builder
	previousWord := false
	for t := s.Scan(); t != scanner.EOF; t = s.Scan() {
		word := t == scanner.Ident || t == scanner.Int || t == scanner.Float || t == scanner.String
		if t == '.' {
			return ""
		}
		if word && previousWord {
			out.WriteByte(' ')
		}
		out.WriteString(s.TokenText())
		previousWord = word
	}
	if s.ErrorCount > 0 {
		return ""
	}
	return out.String()
}

func tokenMeteringRecordedRulesUsable(p tokenMeteringParsed) bool {
	return p.Adapter == "newapi-rc48" && p.Contract != nil && p.Contract.Version == tokenRC48Contract &&
		p.Fields["billing_mode"].Value == "tiered_expr" && p.Fields["expr_b64"].Kind == "tiered_recorded_rules_v1" &&
		p.Fields["expr_b64"].State == "valid" && p.Fields["request_rules"].Kind == "recorded_time_rules_v1"
}
