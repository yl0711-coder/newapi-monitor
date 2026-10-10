package observability

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestContractVocabulary(t *testing.T) {
	if err := ValidateSchemaVersion("observability.v1"); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"", "observability.v0", "observability.v2", "observability.v1.0"} {
		if err := ValidateSchemaVersion(invalid); err == nil {
			t.Fatalf("accepted unsupported version %q", invalid)
		}
	}
	if len(FaultClasses()) != 24 || len(EvidenceLevels()) != 4 || len(GovernanceStates()) != 5 || len(Severities()) != 4 {
		t.Fatal("shared vocabulary does not match 01.3")
	}
	for _, level := range EvidenceLevels() {
		if err := level.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, fault := range FaultClasses() {
		if err := fault.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, state := range GovernanceStates() {
		if err := state.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, severity := range Severities() {
		if err := severity.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, level := range []EvidenceLevel{"", "ambiguous", "high", "medium", "low", "EXACT"} {
		if err := level.Validate(); err == nil {
			t.Fatalf("accepted non-contract evidence level %q", level)
		}
	}
	for _, fault := range []FaultClass{"", "provider_error", "configuration_drift", "Unknown"} {
		if err := fault.Validate(); err == nil {
			t.Fatalf("accepted non-contract fault class %q", fault)
		}
	}
	for _, state := range []GovernanceState{"", "enabled", "disabled", "HEALTHY_STATE"} {
		if err := state.Validate(); err == nil {
			t.Fatalf("accepted non-contract governance state %q", state)
		}
	}
	for _, severity := range []Severity{"", "M0", "M1", "critical", "SEV4"} {
		if err := severity.Validate(); err == nil {
			t.Fatalf("accepted non-contract runtime severity %q", severity)
		}
	}
	levels := EvidenceLevels()
	levels[0] = "corrupted"
	if !EvidenceExact.Valid() || EvidenceLevel("corrupted").Valid() {
		t.Fatal("caller mutated shared vocabulary")
	}
}

func TestTimestampRequiresRFC3339UTC(t *testing.T) {
	for _, valid := range []string{"2026-10-08T04:35:00Z", "2026-10-08T04:35:00.123456789Z", "2026-10-08T04:35:00+00:00"} {
		if err := ValidateTimestamp(valid); err != nil {
			t.Fatalf("valid UTC timestamp rejected: %q: %v", valid, err)
		}
	}
	for _, invalid := range []string{
		"", "2026-10-08 04:35:00", "2026-10-08T12:35:00+08:00",
		"2026-10-08T04:35:00-00:00", "2026-10-08t04:35:00z", "2026-10-08T4:35:00Z",
		"2026-10-08T04:35:00,123Z", "2026-02-30T04:35:00Z", "2026-10-08T24:00:00Z",
	} {
		if err := ValidateTimestamp(invalid); err == nil {
			t.Fatalf("invalid timestamp accepted: %q", invalid)
		}
	}
}

func TestHMACReferenceRequiresKeyAndRejectsRawIdentifiers(t *testing.T) {
	valid := HMACReference{Hash: strings.Repeat("a1", 32), KeyID: "monitor-key-2026-10"}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var decoded HMACReference
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != valid {
		t.Fatalf("HMAC reference did not round-trip: %#v: %v", decoded, err)
	}
	for _, invalid := range []HMACReference{
		{}, {Hash: valid.Hash}, {Hash: valid.Hash, KeyID: "bad key"},
		{Hash: valid.Hash, KeyID: "bad\nkey"}, {Hash: "raw-request-123", KeyID: valid.KeyID},
		{Hash: strings.Repeat("g", 64), KeyID: valid.KeyID},
	} {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("accepted invalid HMAC reference: %#v", invalid)
		}
		if _, err := json.Marshal(invalid); err == nil {
			t.Fatal("marshalled invalid HMAC reference")
		}
	}
	for _, raw := range []string{
		`null`, `{}`, `{"hash":"raw-request-123","key_id":"key-1"}`,
		`{"hash":"` + valid.Hash + `"}`,
		`{"hash":"` + valid.Hash + `","key_id":"key-1","request_id":"private-request-id"}`,
		`{"hash":"` + valid.Hash + `","key_id":"key-1","gateway_request_id":"private-request-id"}`,
	} {
		if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
			t.Fatalf("accepted missing/sensitive reference fields: %s", raw)
		} else if strings.Contains(err.Error(), "private-request-id") {
			t.Fatal("validation error leaked raw ID")
		}
	}
}

func TestUnknownMetricsRetainUnavailableNotZero(t *testing.T) {
	metrics := UnknownMetrics()
	if err := metrics.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(metrics)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]MetricMeasurement
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 7 {
		t.Fatalf("got %d metric fields, want 7", len(fields))
	}
	for name, metric := range fields {
		if metric.ValueMS != nil || metric.EvidenceLevel != EvidenceUnavailable {
			t.Fatalf("%s fabricated an observation: %#v", name, metric)
		}
	}
	if err := (Metrics{}).Validate(); err == nil {
		t.Fatal("zero value silently became a valid all-unknown metrics document")
	}
	// An additional optional field is compatible; it cannot fill a missing
	// mandatory field, and legacy FRT must never become TTFE or TTFT.
	var decoded Metrics
	if err := json.Unmarshal([]byte(`{"frt":123,"firstTokenMs":456}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.TTFT.ValueMS != nil || decoded.TTFE.ValueMS != nil || decoded.TTFC.ValueMS != nil {
		t.Fatal("legacy mixed observations were mapped to a shared metric")
	}
	if err := decoded.Validate(); err == nil {
		t.Fatal("missing metric fields accepted")
	}
}

func TestMetricMeasurementValidation(t *testing.T) {
	negative, zero, positive := int64(-1), int64(0), int64(123)
	for _, valid := range []MetricMeasurement{
		{EvidenceLevel: EvidenceUnavailable}, {ValueMS: &zero, EvidenceLevel: EvidenceExact},
		{ValueMS: &positive, EvidenceLevel: EvidenceCorrelated}, {ValueMS: &positive, EvidenceLevel: EvidenceInferred},
	} {
		if err := valid.Validate(); err != nil {
			t.Fatalf("valid measurement rejected: %#v: %v", valid, err)
		}
	}
	for _, invalid := range []MetricMeasurement{
		{}, {EvidenceLevel: EvidenceExact}, {ValueMS: &zero, EvidenceLevel: EvidenceUnavailable},
		{ValueMS: &negative, EvidenceLevel: EvidenceExact}, {ValueMS: &positive, EvidenceLevel: "high"},
	} {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid measurement accepted: %#v", invalid)
		}
	}
	for _, raw := range []string{
		`null`, `{}`, `{"evidence_level":"unavailable"}`, `{"value_ms":null}`,
		`{"value_ms":null,"evidence_level":"exact"}`, `{"value_ms":0,"evidence_level":"unavailable"}`,
		`{"value_ms":-1,"evidence_level":"exact"}`, `{"value_ms":1,"evidence_level":"high"}`,
		`{"value_ms":1.5,"evidence_level":"exact"}`,
	} {
		var measurement MetricMeasurement
		if err := json.Unmarshal([]byte(raw), &measurement); err == nil {
			t.Fatalf("invalid wire measurement accepted: %s", raw)
		}
	}
	var optional MetricMeasurement
	if err := json.Unmarshal([]byte(`{"value_ms":null,"evidence_level":"unavailable","future_optional":true}`), &optional); err != nil {
		t.Fatalf("same-major optional field rejected: %v", err)
	}
	// Every shared metric field must participate in validation.
	for index := 0; index < reflect.TypeOf(Metrics{}).NumField(); index++ {
		metrics := UnknownMetrics()
		reflect.ValueOf(&metrics).Elem().Field(index).Set(reflect.ValueOf(MetricMeasurement{ValueMS: &negative, EvidenceLevel: EvidenceExact}))
		if err := metrics.Validate(); err == nil {
			t.Fatalf("metric field %d was not validated", index)
		}
	}
}
