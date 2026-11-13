package metering

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMeteringExactScalarStates(t *testing.T) {
	cases := []struct{ raw, state, value string }{
		{"", "missing", ""}, {"null", "null", ""}, {"0", "valid", "0"}, {`"0"`, "valid", "0"},
		{"9007199254740993", "valid", "9007199254740993"}, {`"1e3"`, "valid", "1000"},
		{"1.0", "valid", "1"}, {"1.25", "invalid", ""}, {"9223372036854775808", "invalid", ""},
		{"-1", "sentinel", ""}, {`"-"`, "sentinel", ""}, {`""`, "sentinel", ""}, {"-2", "invalid", ""},
		{"true", "invalid", ""}, {"{}", "invalid", ""}, {"[]", "invalid", ""}, {`"secret-value"`, "invalid", ""},
		{"1e999", "invalid", ""},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			f := Number(json.RawMessage(tc.raw), true, true)
			if f.State != tc.state || f.Value != tc.value {
				t.Fatalf("%+v", f)
			}
			if strings.Contains(f.Raw, "secret") {
				t.Fatal("unsafe raw retained")
			}
			if (f.Int64() != nil) != (tc.state == "valid") {
				t.Fatal("unknown became number")
			}
		})
	}
	if f := Number(json.RawMessage(`"0.1"`), false, true); f.Value != "1/10" {
		t.Fatalf("decimal %v", f)
	}
	if f := Number(json.RawMessage("-20"), true, false); f.Value != "-20" {
		t.Fatalf("signed extension %v", f)
	}
}
func TestMeteringStrictJSONBoundsAndDuplicates(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `{"a":{"k":1,"k":2}}`, `{"a":1} {}`, `null`, `{"a":[[[[[]]]]]}`, strings.Repeat(" ", 1025)} {
		if _, err := Object([]byte(raw), 1024, 3); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	if _, err := Object([]byte(`{"a":0,"b":null,"c":"0"}`), 1024, 3); err != nil {
		t.Fatal(err)
	}
}
func TestMeteringLengthWireCompatibility(t *testing.T) {
	zero := int64(0)
	if !ValidLength(nil, "", "") || !ValidLength(&zero, "valid", "request-length-v1") {
		t.Fatal("old or zero rejected")
	}
	if ValidLength(nil, "valid", "request-length-v1") || ValidLength(&zero, "missing", "request-length-v1") {
		t.Fatal("inconsistent wire accepted")
	}
}
