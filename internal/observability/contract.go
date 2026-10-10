// Package observability holds the shared observability.v1 vocabulary from
// 01.3 (document version v2.0). It does not define transport endpoints, infer
// protocol success, or grant authority to change NewAPI configuration.
package observability

import "fmt"

const SchemaVersion = "observability.v1"

type EvidenceLevel string

const (
	EvidenceExact       EvidenceLevel = "exact"
	EvidenceCorrelated  EvidenceLevel = "correlated"
	EvidenceInferred    EvidenceLevel = "inferred"
	EvidenceUnavailable EvidenceLevel = "unavailable"
)

var evidenceLevels = [...]EvidenceLevel{EvidenceExact, EvidenceCorrelated, EvidenceInferred, EvidenceUnavailable}

type FaultClass string

const (
	FaultTransportConnect                  FaultClass = "transport_connect"
	FaultTransportTimeout                  FaultClass = "transport_timeout"
	FaultUpstreamFirstByteSlow             FaultClass = "upstream_first_byte_slow"
	FaultStreamFirstEventSlow              FaultClass = "stream_first_event_slow"
	FaultTextFirstContentSlow              FaultClass = "text_first_content_slow"
	FaultStreamInterruptedBeforeFirstEvent FaultClass = "stream_interrupted_before_first_event"
	FaultStreamInterruptedMidstream        FaultClass = "stream_interrupted_midstream"
	FaultClientGone                        FaultClass = "client_gone"
	FaultProtocolInvalid                   FaultClass = "protocol_invalid"
	FaultResponsesIncomplete               FaultClass = "responses_incomplete"
	FaultThinkingSignatureInvalid          FaultClass = "thinking_signature_invalid"
	FaultToolCallInvalid                   FaultClass = "tool_call_invalid"
	FaultRouteNoChannel                    FaultClass = "route_no_channel"
	FaultModelNotFound                     FaultClass = "model_not_found"
	FaultAuthQuotaAccount                  FaultClass = "auth_quota_account"
	FaultRateLimitCapacity                 FaultClass = "rate_limit_capacity"
	FaultUpstream5xx                       FaultClass = "upstream_5xx"
	FaultEmptyOrTruncatedOutput            FaultClass = "empty_or_truncated_output"
	FaultBillingAnomaly                    FaultClass = "billing_anomaly"
	FaultSemanticQuality                   FaultClass = "semantic_quality"
	FaultClientRetryLoop                   FaultClass = "client_retry_loop"
	FaultClientCompaction                  FaultClass = "client_compaction"
	FaultTelemetryGap                      FaultClass = "telemetry_gap"
	FaultUnknown                           FaultClass = "unknown"
)

var faultClasses = [...]FaultClass{
	FaultTransportConnect, FaultTransportTimeout, FaultUpstreamFirstByteSlow,
	FaultStreamFirstEventSlow, FaultTextFirstContentSlow, FaultStreamInterruptedBeforeFirstEvent,
	FaultStreamInterruptedMidstream, FaultClientGone, FaultProtocolInvalid,
	FaultResponsesIncomplete, FaultThinkingSignatureInvalid, FaultToolCallInvalid,
	FaultRouteNoChannel, FaultModelNotFound, FaultAuthQuotaAccount, FaultRateLimitCapacity,
	FaultUpstream5xx, FaultEmptyOrTruncatedOutput, FaultBillingAnomaly, FaultSemanticQuality,
	FaultClientRetryLoop, FaultClientCompaction, FaultTelemetryGap, FaultUnknown,
}

type GovernanceState string

const (
	StateHealthy           GovernanceState = "HEALTHY"
	StateSuspect           GovernanceState = "SUSPECT"
	StateDegraded          GovernanceState = "DEGRADED"
	StateQuarantined       GovernanceState = "QUARANTINED"
	StateRecoveryProbation GovernanceState = "RECOVERY_PROBATION"
)

var governanceStates = [...]GovernanceState{StateHealthy, StateSuspect, StateDegraded, StateQuarantined, StateRecoveryProbation}

type Severity string

const (
	SeveritySEV0 Severity = "SEV0"
	SeveritySEV1 Severity = "SEV1"
	SeveritySEV2 Severity = "SEV2"
	SeveritySEV3 Severity = "SEV3"
)

var severities = [...]Severity{SeveritySEV0, SeveritySEV1, SeveritySEV2, SeveritySEV3}

// Return copies so callers cannot mutate the vocabulary used by validation.
func EvidenceLevels() []EvidenceLevel { return append([]EvidenceLevel(nil), evidenceLevels[:]...) }
func FaultClasses() []FaultClass      { return append([]FaultClass(nil), faultClasses[:]...) }
func GovernanceStates() []GovernanceState {
	return append([]GovernanceState(nil), governanceStates[:]...)
}
func Severities() []Severity              { return append([]Severity(nil), severities[:]...) }
func (v EvidenceLevel) Valid() bool       { return contains(evidenceLevels[:], v) }
func (v FaultClass) Valid() bool          { return contains(faultClasses[:], v) }
func (v GovernanceState) Valid() bool     { return contains(governanceStates[:], v) }
func (v Severity) Valid() bool            { return contains(severities[:], v) }
func (v EvidenceLevel) Validate() error   { return validateEnum("evidence_level", v.Valid()) }
func (v FaultClass) Validate() error      { return validateEnum("fault_class", v.Valid()) }
func (v GovernanceState) Validate() error { return validateEnum("governance_state", v.Valid()) }
func (v Severity) Validate() error        { return validateEnum("severity", v.Valid()) }

func contains[T ~string](values []T, value T) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func validateEnum(field string, valid bool) error {
	if !valid {
		// Do not echo invalid input; it might contain a sensitive identifier.
		return fmt.Errorf("observability: invalid required %s", field)
	}
	return nil
}

// ValidateSchemaVersion rejects unsupported versions, including unspecified
// versions. An unknown version must not silently become the current contract.
func ValidateSchemaVersion(version string) error {
	return validateEnum("schema_version", version == SchemaVersion)
}
