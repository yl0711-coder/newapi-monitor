// Package financecredit classifies NewAPI credit events without depending on
// Monitor storage or HTTP code. Monetary values use fixed six-decimal units so
// historical quota evidence never passes through binary floating point.
package financecredit

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
)

const (
	ActionAdd      = "quota_add"
	ActionSubtract = "quota_subtract"
	ActionOverride = "quota_override"

	UnitUSD         = "USD_quota"
	UnitCNY         = "CNY_quota"
	UnitPoints      = "quota_points"
	UnitUnspecified = "unspecified_quota"
	UnitCustom      = "custom_currency_quota"
)

type Adjustment struct {
	Action      string
	Unit        string
	ValueMicro  int64
	BeforeMicro int64
	AfterMicro  int64
}

type StructuredAdjustment struct {
	Adjustment
	TargetUserID int64
}

var (
	deltaPattern     = regexp.MustCompile(`^管理员(增加|减少)用户额度 ([^0-9+-]*)([0-9]+(?:\.[0-9]+)?) (额度|点额度)$`)
	overridePattern  = regexp.MustCompile(`^管理员覆盖用户额度从 ([^0-9+-]*)([+-]?[0-9]+(?:\.[0-9]+)?) (?:额度|点额度) 为 ([^0-9+-]*)([+-]?[0-9]+(?:\.[0-9]+)?) (额度|点额度)$`)
	displayedPattern = regexp.MustCompile(`^([^0-9+-]*)([+-]?[0-9]+(?:\.[0-9]+)?) (额度|点额度)$`)
)

func ParseLegacyAdjustment(content string) (Adjustment, bool) {
	if match := deltaPattern.FindStringSubmatch(content); len(match) == 5 {
		value, err := parseFixedSix(match[3])
		if err != nil {
			return Adjustment{}, false
		}
		action := ActionAdd
		if match[1] == "减少" {
			action = ActionSubtract
		}
		return Adjustment{Action: action, Unit: NormalizeUnit(match[2], match[4]), ValueMicro: value}, true
	}
	if match := overridePattern.FindStringSubmatch(content); len(match) == 6 {
		before, beforeErr := parseFixedSix(match[2])
		after, afterErr := parseFixedSix(match[4])
		beforeUnit := NormalizeUnit(match[1], match[5])
		afterUnit := NormalizeUnit(match[3], match[5])
		if beforeErr != nil || afterErr != nil || beforeUnit != afterUnit {
			return Adjustment{}, false
		}
		return Adjustment{Action: ActionOverride, Unit: beforeUnit, BeforeMicro: before, AfterMicro: after}, true
	}
	return Adjustment{}, false
}

// ParseStructuredAdjustment accepts the language-neutral audit payload used
// by newer NewAPI versions. The log owner is the operator, so callers must use
// TargetUserID when present and only fall back to the log user when it is zero.
func ParseStructuredAdjustment(other string) (StructuredAdjustment, bool) {
	var envelope struct {
		Op struct {
			Action string         `json:"action"`
			Params map[string]any `json:"params"`
		} `json:"op"`
	}
	decoder := json.NewDecoder(strings.NewReader(other))
	decoder.UseNumber()
	if decoder.Decode(&envelope) != nil || envelope.Op.Params == nil {
		return StructuredAdjustment{}, false
	}
	target, _ := jsonInt64(envelope.Op.Params["target_user_id"])
	switch envelope.Op.Action {
	case "user.quota_add", "user.quota_subtract":
		unit, value, ok := ParseDisplayedQuota(jsonString(envelope.Op.Params["quota"]))
		// add/subtract encode the operation direction in Action; accepting a
		// negative operand here would silently invert its financial meaning.
		if !ok || value < 0 {
			return StructuredAdjustment{}, false
		}
		action := ActionAdd
		if envelope.Op.Action == "user.quota_subtract" {
			action = ActionSubtract
		}
		return StructuredAdjustment{Adjustment: Adjustment{Action: action, Unit: unit, ValueMicro: value}, TargetUserID: target}, true
	case "user.quota_override":
		beforeUnit, before, beforeOK := ParseDisplayedQuota(jsonString(envelope.Op.Params["from"]))
		afterUnit, after, afterOK := ParseDisplayedQuota(jsonString(envelope.Op.Params["to"]))
		if !beforeOK || !afterOK || beforeUnit != afterUnit {
			return StructuredAdjustment{}, false
		}
		return StructuredAdjustment{Adjustment: Adjustment{Action: ActionOverride, Unit: beforeUnit, BeforeMicro: before, AfterMicro: after}, TargetUserID: target}, true
	default:
		return StructuredAdjustment{}, false
	}
}

func ParseDisplayedQuota(raw string) (string, int64, bool) {
	match := displayedPattern.FindStringSubmatch(strings.TrimSpace(raw))
	if len(match) != 4 {
		return "", 0, false
	}
	value, err := parseFixedSix(match[2])
	if err != nil {
		return "", 0, false
	}
	return NormalizeUnit(match[1], match[3]), value, true
}

// PotentialAdjustment reports whether an unparsed management log still looks
// like a quota change. Callers use it to fail closed: unrelated admin actions
// may be ignored, but a changed producer format for money must not silently
// turn a gift into paid revenue.
func PotentialAdjustment(content, other string) bool {
	content = strings.TrimSpace(content)
	for _, prefix := range []string{"管理员增加用户额度", "管理员减少用户额度", "管理员覆盖用户额度"} {
		if strings.HasPrefix(content, prefix) {
			return true
		}
	}
	var envelope struct {
		Op struct {
			Action string `json:"action"`
		} `json:"op"`
	}
	if json.Unmarshal([]byte(other), &envelope) != nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(envelope.Op.Action), "user.quota_")
}

func jsonString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func jsonInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil && parsed > 0
	case float64:
		parsed := int64(typed)
		return parsed, typed == float64(parsed) && parsed > 0
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil && parsed > 0
	default:
		return 0, false
	}
}

func (a Adjustment) NetChangeMicro() int64 {
	switch a.Action {
	case ActionAdd:
		return a.ValueMicro
	case ActionSubtract:
		return -a.ValueMicro
	case ActionOverride:
		return a.AfterMicro - a.BeforeMicro
	default:
		return 0
	}
}

func Cohort(a Adjustment, userCreatedAt, eventAt int64) string {
	net := a.NetChangeMicro()
	if net <= 0 {
		return "non_positive"
	}
	if userCreatedAt <= 0 || eventAt < userCreatedAt {
		return "registration_time_unknown"
	}
	age := eventAt - userCreatedAt
	trialSized := a.Unit == UnitUSD && net >= 100_000_000 && net <= 200_000_000
	if age <= 24*3600 {
		if trialSized {
			return "trial_candidate_within_24h"
		}
		return "other_positive_within_24h"
	}
	if age <= 7*24*3600 {
		if trialSized {
			return "trial_candidate_within_7d"
		}
		return "other_positive_within_7d"
	}
	return "positive_after_7d"
}

func NormalizeUnit(symbol, suffix string) string {
	if suffix == "点额度" {
		return UnitPoints
	}
	switch symbol {
	case "＄", "$":
		return UnitUSD
	case "¥", "￥":
		return UnitCNY
	case "":
		return UnitUnspecified
	default:
		return UnitCustom
	}
}

func parseFixedSix(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	sign := int64(1)
	if strings.HasPrefix(raw, "-") {
		sign = -1
		raw = strings.TrimPrefix(raw, "-")
	} else if strings.HasPrefix(raw, "+") {
		raw = strings.TrimPrefix(raw, "+")
	}
	parts := strings.Split(raw, ".")
	if len(parts) > 2 || parts[0] == "" {
		return 0, errors.New("invalid fixed decimal")
	}
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || whole < 0 || whole > math.MaxInt64/1_000_000 {
		return 0, errors.New("fixed decimal out of range")
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > 6 {
		return 0, errors.New("fixed decimal exceeds six places")
	}
	for len(fraction) < 6 {
		fraction += "0"
	}
	fractionValue := int64(0)
	if fraction != "" {
		fractionValue, err = strconv.ParseInt(fraction, 10, 64)
		if err != nil {
			return 0, errors.New("invalid fixed decimal fraction")
		}
	}
	magnitude := whole*1_000_000 + fractionValue
	if magnitude < 0 || (whole == math.MaxInt64/1_000_000 && fractionValue > math.MaxInt64%1_000_000) {
		return 0, errors.New("fixed decimal out of range")
	}
	return sign * magnitude, nil
}
