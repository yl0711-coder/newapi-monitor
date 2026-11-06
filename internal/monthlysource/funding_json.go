package monthlysource

import (
	"encoding/json"
	"io"
	"strings"
)

// Validate duplicate keys (also escaped equivalents), depth and trailing data
// before decoding any monetary fields. encoding/json alone accepts last-wins
// duplicates, which is unsuitable for selecting an account or a money value.
func fundingObject(raw string, emptyAllowed bool) (map[string]json.RawMessage, bool) {
	trimmed := strings.TrimSpace(raw)
	if emptyAllowed && (trimmed == "" || trimmed == "null") {
		return nil, true
	}
	if len(raw) > 65536 || !strings.HasPrefix(trimmed, "{") {
		return nil, false
	}
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	if !fundingJSONValue(decoder, 0) {
		return nil, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, false
	}
	var result map[string]json.RawMessage
	if json.Unmarshal([]byte(trimmed), &result) != nil {
		return nil, false
	}
	return result, true
}

func fundingJSONValue(decoder *json.Decoder, depth int) bool {
	if depth > 16 {
		return false
	}
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return true
	}
	switch delim {
	case '{':
		keys := make(map[string]bool)
		for decoder.More() {
			token, err := decoder.Token()
			key, ok := token.(string)
			if err != nil || !ok || keys[key] {
				return false
			}
			keys[key] = true
			if !fundingJSONValue(decoder, depth+1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim('}')
	case '[':
		for decoder.More() {
			if !fundingJSONValue(decoder, depth+1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim(']')
	default:
		return false
	}
}
