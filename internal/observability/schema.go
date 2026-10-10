package observability

import (
	_ "embed"
	"encoding/json"
	"reflect"
)

//go:embed schema/observability.v1.schema.json
var schemaAsset []byte

// SchemaJSON returns the machine-readable shared-definition library. Consumers
// validate a particular definition via #/$defs/<name>; this is not an Eval or
// Incident transport envelope. The returned copy cannot mutate the asset.
func SchemaJSON() []byte { return append([]byte(nil), schemaAsset...) }

// generatedSchemaJSON derives enum and metric-field definitions from Go types.
// The committed asset is checked against this output, so it cannot silently
// diverge from the vocabulary used by runtime validation.
func generatedSchemaJSON() ([]byte, error) {
	type object = map[string]any
	metricType := reflect.TypeOf(Metrics{})
	metricProperties := object{}
	metricFields := make([]string, 0, metricType.NumField())
	for i := range metricType.NumField() {
		name := metricType.Field(i).Tag.Get("json")
		metricFields = append(metricFields, name)
		metricProperties[name] = object{"$ref": "#/$defs/metric_measurement"}
	}
	observedLevels := make([]EvidenceLevel, 0, len(evidenceLevels)-1)
	for _, level := range evidenceLevels {
		if level != EvidenceUnavailable {
			observedLevels = append(observedLevels, level)
		}
	}
	document := object{
		"$schema":  "https://json-schema.org/draft/2020-12/schema",
		"$id":      "urn:nexusapi:observability:v1:definitions",
		"title":    "observability.v1 shared definitions (01.3 document v2.0)",
		"$comment": "Definition library only: validate instances against an explicit $defs reference. No Incident or Eval transport envelope is defined. HMAC encoding is the existing Monitor SHA-256 profile. Validate RFC3339 calendar values with date-time format assertion enabled.",
		"$defs": object{
			"schema_version":   object{"type": "string", "const": SchemaVersion},
			"evidence_level":   object{"type": "string", "enum": evidenceLevels},
			"fault_class":      object{"type": "string", "enum": faultClasses},
			"governance_state": object{"type": "string", "enum": governanceStates},
			"severity":         object{"type": "string", "enum": severities},
			"timestamp":        object{"type": "string", "format": "date-time", "pattern": utcTimestampPattern},
			"hmac_reference": object{
				"type": "object", "additionalProperties": false,
				"required": []string{"hash", "key_id"},
				"properties": object{
					"hash":   object{"type": "string", "pattern": hmacHashPattern},
					"key_id": object{"type": "string", "pattern": hmacKeyIDPattern},
				},
			},
			"metric_measurement": object{
				"type":     "object",
				"required": []string{"value_ms", "evidence_level"},
				"properties": object{
					"value_ms":       object{"type": []string{"integer", "null"}, "minimum": 0, "maximum": int64(1<<63 - 1)},
					"evidence_level": object{"$ref": "#/$defs/evidence_level"},
				},
				"oneOf": []any{
					object{"properties": object{"value_ms": object{"type": "null"}, "evidence_level": object{"const": EvidenceUnavailable}}},
					object{"properties": object{"value_ms": object{"type": "integer"}, "evidence_level": object{"enum": observedLevels}}},
				},
			},
			"metrics": object{"type": "object", "required": metricFields, "properties": metricProperties},
		},
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}
