package monitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// Token IDs are optional metadata, not credentials, group names or channel IDs.
// Keep the canonical value as text at the UI/API boundary: JavaScript numbers
// cannot represent every ID that the upstream's signed 64-bit integer can.
// An omitted field must be handled separately from an explicit empty value by
// the eventual configuration handler; this function only validates a value.
func canonicalUpstreamTokenID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if len(value) > 19 {
		return "", errors.New("上游令牌 ID 必须为正整数，且不超过 64 位整数范围")
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return "", errors.New("上游令牌 ID 必须为正整数，不是令牌名称或密钥")
		}
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return "", errors.New("上游令牌 ID 必须为正整数，且不超过 64 位整数范围")
	}
	return strconv.FormatInt(id, 10), nil
}

// decodeOptionalUpstreamTokenID deliberately does not return a fatal decoding
// error. Missing/malformed identity cannot invalidate otherwise usable account
// billing evidence. The attribution layer must require the explicit valid state
// and never infer an ID from the nested api_key object or its mutable name.
func decodeOptionalUpstreamTokenID(raw json.RawMessage) (string, string) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", "missing"
	}
	text := string(raw)
	if raw[0] == '"' {
		if json.Unmarshal(raw, &text) != nil {
			return "", "invalid"
		}
	}
	id, err := canonicalUpstreamTokenID(text)
	if err != nil || id == "" {
		return "", "invalid"
	}
	return id, "valid"
}
