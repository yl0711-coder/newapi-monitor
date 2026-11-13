package main

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestRequestLengthCollectorRetainsStatesAndMinuteEvidence(t *testing.T) {
	for _, tc := range []struct{ value, state string }{{"", "missing"}, {"null", "null"}, {"0", "valid"}, {`"2048"`, "valid"}, {`"-"`, "sentinel"}, {"-2", "invalid"}, {"1.5", "invalid"}, {`"secret"`, "invalid"}} {
		t.Run(tc.state+tc.value, func(t *testing.T) {
			raw := `{"log_schema":2,"upstream_status":"200","oneapi_request_id":"synthetic","request_completion":"OK"`
			if tc.value != "" {
				raw += `,"request_length":` + tc.value
			}
			raw += "}"
			data := []byte(raw)
			digest := sha256.Sum256(data)
			event, ok := makeEvidenceEvent(config{node: "test", evidenceMode: "pilot", evidenceHMACKey: []byte(strings.Repeat("k", 32))}, data, rawLine{Method: "POST"}, sample{Route: "/v1/responses", Method: "POST", Status: 200, UpstreamStatus: 200}, 1, 1, digest)
			if !ok || event.RequestLengthState != tc.state || (event.RequestLength != nil) != (tc.state == "valid") {
				t.Fatalf("%+v %v", event, ok)
			}
		})
	}
}
