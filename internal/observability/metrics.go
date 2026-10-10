package observability

import (
	"encoding/json"
	"fmt"
)

// MetricMeasurement contains an observed duration and its own evidence level.
// No observation is represented as null/unavailable, never zero. A real zero
// duration is allowed if there is supporting evidence.
type MetricMeasurement struct {
	ValueMS       *int64        `json:"value_ms"`
	EvidenceLevel EvidenceLevel `json:"evidence_level"`
}

// UnmarshalJSON requires an explicit null for an unobserved metric. Unknown
// optional fields are allowed under the same-version additive compatibility
// rule, but missing mandatory fields or unknown evidence enums are not.
func (m *MetricMeasurement) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("observability: invalid metric object")
	}
	if _, ok := fields["value_ms"]; !ok {
		return fmt.Errorf("observability: metric requires value_ms")
	}
	if _, ok := fields["evidence_level"]; !ok {
		return fmt.Errorf("observability: metric requires evidence_level")
	}
	type wireMeasurement MetricMeasurement
	var decoded wireMeasurement
	if err := json.Unmarshal(data, &decoded); err != nil {
		return fmt.Errorf("observability: invalid metric field")
	}
	validated := MetricMeasurement(decoded)
	if err := validated.Validate(); err != nil {
		return err
	}
	*m = validated
	return nil
}

func (m MetricMeasurement) Validate() error {
	if err := m.EvidenceLevel.Validate(); err != nil {
		return err
	}
	if m.ValueMS == nil {
		if m.EvidenceLevel != EvidenceUnavailable {
			return fmt.Errorf("observability: missing metric must be unavailable")
		}
		return nil
	}
	if m.EvidenceLevel == EvidenceUnavailable {
		return fmt.Errorf("observability: unavailable metric must not contain a value")
	}
	if *m.ValueMS < 0 {
		return fmt.Errorf("observability: metric duration must not be negative")
	}
	return nil
}

// Metrics separates the seven observation points in section 7 of 01.3.
// NewAPI other.frt and legacy firstTokenMs are not valid substitutes for TTFE,
// TTFC or TTFT. Without the corresponding event, leave that metric unavailable.
// Total requires a legal terminal event or a directly determined failure, not
// just HTTP 200, a closed connection, partial output, or billing activity.
type Metrics struct {
	TTFB               MetricMeasurement `json:"ttfb_ms"`
	TTFE               MetricMeasurement `json:"ttfe_ms"`
	TTFC               MetricMeasurement `json:"ttfc_ms"`
	TTFT               MetricMeasurement `json:"ttft_ms"`
	FirstThinkingEvent MetricMeasurement `json:"first_thinking_event_ms"`
	FirstToolCall      MetricMeasurement `json:"first_tool_call_ms"`
	Total              MetricMeasurement `json:"total_ms"`
}

func UnknownMetrics() Metrics {
	unavailable := MetricMeasurement{EvidenceLevel: EvidenceUnavailable}
	return Metrics{
		TTFB: unavailable, TTFE: unavailable, TTFC: unavailable, TTFT: unavailable,
		FirstThinkingEvent: unavailable, FirstToolCall: unavailable, Total: unavailable,
	}
}

func (m Metrics) Validate() error {
	for _, field := range []struct {
		name  string
		value MetricMeasurement
	}{
		{"ttfb_ms", m.TTFB}, {"ttfe_ms", m.TTFE}, {"ttfc_ms", m.TTFC}, {"ttft_ms", m.TTFT},
		{"first_thinking_event_ms", m.FirstThinkingEvent}, {"first_tool_call_ms", m.FirstToolCall},
		{"total_ms", m.Total},
	} {
		if err := field.value.Validate(); err != nil {
			return fmt.Errorf("observability: %s: %w", field.name, err)
		}
	}
	return nil
}
