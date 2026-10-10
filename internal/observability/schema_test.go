package observability

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestMachineSchemaMatchesRuntimeVocabulary(t *testing.T) {
	generated, err := generatedSchemaJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(generated), bytes.TrimSpace(SchemaJSON())) {
		t.Fatalf("machine schema drifted from runtime vocabulary; expected:\n%s", generated)
	}
	var schema struct {
		Definitions map[string]struct {
			Enum       []string                  `json:"enum"`
			Const      string                    `json:"const"`
			Required   []string                  `json:"required"`
			Properties map[string]map[string]any `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(SchemaJSON(), &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Definitions["schema_version"].Const != SchemaVersion {
		t.Fatal("schema version does not match runtime")
	}
	for _, name := range []string{"evidence_level", "fault_class", "governance_state", "severity"} {
		seen := map[string]bool{}
		for _, value := range schema.Definitions[name].Enum {
			if seen[value] {
				t.Fatalf("duplicate %s enum: %q", name, value)
			}
			seen[value] = true
		}
	}
	metricType := reflect.TypeOf(Metrics{})
	metricDefinition := schema.Definitions["metrics"]
	if len(metricDefinition.Required) != metricType.NumField() || len(metricDefinition.Properties) != metricType.NumField() {
		t.Fatal("machine schema does not require all seven metric fields")
	}
	for index := range metricType.NumField() {
		field := metricType.Field(index).Tag.Get("json")
		if metricDefinition.Required[index] != field || metricDefinition.Properties[field]["$ref"] != "#/$defs/metric_measurement" {
			t.Fatalf("metric schema field mismatch: %s", field)
		}
	}
	asset := SchemaJSON()
	asset[0] = '!'
	if SchemaJSON()[0] != '{' {
		t.Fatal("caller mutated embedded machine schema")
	}
}
