// Package metering preserves source field states without floating point coercion.
package metering

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Field is a whitelisted source scalar. Raw contains numeric lexemes only;
// arbitrary invalid source text is intentionally never retained.
type Field struct {
	State  string `json:"state"`
	Kind   string `json:"kind,omitempty"`
	Raw    string `json:"raw,omitempty"`
	Value  string `json:"value,omitempty"`
	Reason string `json:"reason,omitempty"`
}

var numberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]{1,3})?$`)

// Number parses bounded exact numbers. Integers must fit int64, and decimals
// remain rational strings. Missing, null, sentinels and malformed values differ.
func Number(raw json.RawMessage, integer, nonnegative bool) Field {
	if len(raw) == 0 {
		return Field{State: "missing"}
	}
	text := strings.TrimSpace(string(raw))
	if text == "null" {
		return Field{State: "null"}
	}
	f := Field{State: "invalid", Kind: "number", Reason: "invalid_number"}
	if strings.HasPrefix(text, "\"") {
		f.Kind = "string"
		if json.Unmarshal(raw, &text) != nil {
			return f
		}
		text = strings.TrimSpace(text)
	}
	if text == "" || text == "-" {
		f.State, f.Reason = "sentinel", "empty_or_dash"
		return f
	}
	if len(text) > 128 || !numberPattern.MatchString(text) {
		return f
	}
	f.Raw = text
	if i := strings.IndexAny(text, "eE"); i >= 0 {
		exp, err := strconv.Atoi(text[i+1:])
		if err != nil || exp < -100 || exp > 100 {
			f.Reason = "exponent_out_of_range"
			return f
		}
	}
	n, ok := new(big.Rat).SetString(text)
	if !ok {
		return f
	}
	if nonnegative && n.Sign() < 0 {
		if n.Cmp(big.NewRat(-1, 1)) == 0 {
			f.State = "sentinel"
			f.Reason = "minus_one"
		} else {
			f.Reason = "negative"
		}
		return f
	}
	if integer {
		if !n.IsInt() {
			f.Reason = "fractional_count"
			return f
		}
		if !n.Num().IsInt64() {
			f.Reason = "overflow"
			return f
		}
		f.Value = n.Num().String()
	} else {
		f.Value = n.RatString()
	}
	f.State, f.Reason = "valid", ""
	return f
}

// Int64 returns a value only for a valid int64 scalar.
func (f Field) Int64() *int64 {
	if f.State != "valid" {
		return nil
	}
	n, err := strconv.ParseInt(f.Value, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

// Text preserves only bounded allowlisted scalar metadata.
func Text(raw json.RawMessage, allow func(string) bool) Field {
	if len(raw) == 0 {
		return Field{State: "missing"}
	}
	if string(bytes.TrimSpace(raw)) == "null" {
		return Field{State: "null"}
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || !allow(value) {
		return Field{State: "invalid", Reason: "unsupported_text"}
	}
	return Field{State: "valid", Kind: "string", Value: value}
}

// Boolean does not equate missing or null to false.
func Boolean(raw json.RawMessage) Field {
	if len(raw) == 0 {
		return Field{State: "missing"}
	}
	v := string(bytes.TrimSpace(raw))
	if v == "null" {
		return Field{State: "null"}
	}
	if v != "true" && v != "false" {
		return Field{State: "invalid", Reason: "invalid_boolean"}
	}
	return Field{State: "valid", Kind: "bool", Value: v}
}

// Object rejects duplicate keys at every depth, trailing JSON, excessive depth,
// and excessive size before a whitelist consumer can interpret the object.
func Object(raw []byte, maxBytes, maxDepth int) (map[string]json.RawMessage, error) {
	if len(raw) > maxBytes {
		return nil, errors.New("json_size_limit")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := walk(d, 0, maxDepth); err != nil {
		return nil, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("json_trailing")
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(raw, &result) != nil || result == nil {
		return nil, errors.New("json_not_object")
	}
	return result, nil
}

func walk(d *json.Decoder, depth, maxDepth int) error {
	if depth > maxDepth {
		return errors.New("json_depth_limit")
	}
	t, err := d.Token()
	if err != nil {
		return errors.New("json_invalid")
	}
	switch v := t.(type) {
	case json.Delim:
		switch v {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return errors.New("json_invalid")
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return errors.New("json_duplicate_key")
				}
				seen[s] = true
				if err := walk(d, depth+1, maxDepth); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := walk(d, depth+1, maxDepth); err != nil {
					return err
				}
			}
		default:
			return errors.New("json_invalid")
		}
		if _, err := d.Token(); err != nil {
			return errors.New("json_invalid")
		}
	}
	return nil
}

// RequestLength reads a whole HTTP request length, never a body-only length.
func RequestLength(raw json.RawMessage) (*int64, string) {
	f := Number(raw, true, true)
	return f.Int64(), f.State
}

// ValidLength validates optional wire fields, including old payloads with no
// capability metadata. A valid zero must have a non-nil value.
func ValidLength(value *int64, state, version string) bool {
	if state == "" && version == "" {
		return value == nil
	}
	if version != "request-length-v1" {
		return false
	}
	switch state {
	case "valid":
		return value != nil && *value >= 0
	case "missing", "null", "sentinel", "invalid":
		return value == nil
	default:
		return false
	}
}
