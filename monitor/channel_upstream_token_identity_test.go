package monitor

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestUpstreamTokenIDOptionalCanonicalInput(t *testing.T) {
	for _, tt := range []struct{ input, want string }{
		{"", ""}, {"  ", ""}, {"575", "575"}, {" 00575 ", "575"},
		{"9007199254740993", "9007199254740993"},
		{"9223372036854775807", "9223372036854775807"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := canonicalUpstreamTokenID(tt.input)
			if err != nil || got != tt.want {
				t.Fatalf("got %q, error %v, want %q", got, err, tt.want)
			}
		})
	}
	for _, input := range []string{"0", "000", "-1", "+575", "5.75", "5e2", "５７５", "57 5", "575\x00", "9223372036854775808", "18446744073709551615", "codex", "sk-do-not-echo-this"} {
		t.Run("invalid/"+input, func(t *testing.T) {
			got, err := canonicalUpstreamTokenID(input)
			if err == nil || got != "" {
				t.Fatal("invalid input must not produce an identity")
			}
			if strings.Contains(err.Error(), "sk-do-not-echo-this") {
				t.Fatal("validation must not echo a mistakenly pasted credential")
			}
		})
	}
}

func TestUpstreamTokenIDOptionalJSONIdentity(t *testing.T) {
	for _, tt := range []struct{ raw, want, state string }{
		{"", "", "missing"}, {" null ", "", "missing"},
		{"575", "575", "valid"}, {`"00575"`, "575", "valid"},
		{` "9007199254740993" `, "9007199254740993", "valid"},
		{"9223372036854775807", "9223372036854775807", "valid"},
		{`""`, "", "invalid"}, {"0", "", "invalid"}, {"-1", "", "invalid"},
		{"575.0", "", "invalid"}, {"5.75e2", "", "invalid"},
		{"true", "", "invalid"}, {"{}", "", "invalid"}, {"[]", "", "invalid"},
		{`"broken`, "", "invalid"}, {"9223372036854775808", "", "invalid"},
	} {
		t.Run(tt.raw, func(t *testing.T) {
			got, state := decodeOptionalUpstreamTokenID(json.RawMessage(tt.raw))
			if got != tt.want || state != tt.state {
				t.Fatalf("got %q/%s, want %q/%s", got, state, tt.want, tt.state)
			}
		})
	}
}
