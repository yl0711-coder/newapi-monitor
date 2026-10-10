package monitor

import (
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/observability"
)

// An additive read model, not an Incident or an Eval transport. Explicit v1
// callers get safe identity references and the shared vocabulary. Legacy
// consumers keep their existing fields; no source facts are rewritten.
type investigationObservability struct {
	SchemaVersion string                          `json:"schema_version"`
	From          string                          `json:"from"`
	To            string                          `json:"to"`
	Summary       investigationObservationSummary `json:"summary"`
	Events        []investigationObservation      `json:"events"`
	SourceQuality []observabilitySourceQuality    `json:"source_quality"`
	MetricNote    string                          `json:"metric_note"`
}

type investigationObservationSummary struct {
	FaultClass    *observability.FaultClass   `json:"fault_class"`
	EvidenceLevel observability.EvidenceLevel `json:"evidence_level"`
	Candidate     bool                        `json:"candidate"`
	Conclusion    string                      `json:"conclusion"`
}

type investigationObservation struct {
	EventID       string                       `json:"event_id"`
	SourceEventID string                       `json:"source_event_id"`
	OccurredAt    string                       `json:"occurred_at"`
	Source        string                       `json:"source"`
	EvidenceRef   string                       `json:"evidence_ref"`
	HMACKeyID     string                       `json:"key_id"`
	GatewayRef    *observability.HMACReference `json:"gateway_request_id_hash,omitempty"`
	FaultClass    *observability.FaultClass    `json:"fault_class"`
	EvidenceLevel observability.EvidenceLevel  `json:"evidence_level"`
	Candidate     bool                         `json:"candidate"`
	Complete      bool                         `json:"complete"`
	Metrics       observability.Metrics        `json:"metrics"`
	FRTMs         *int64                       `json:"frt_ms"`
	FRTSemantics  string                       `json:"frt_semantics,omitempty"`
}

func sharedInvestigationLevel(legacy string) (observability.EvidenceLevel, bool) {
	level := observability.EvidenceLevel(legacy)
	if level.Valid() {
		return level, false
	}
	// ambiguous is a match state, never a fifth shared evidence level.
	return observability.EvidenceUnavailable, legacy == "ambiguous"
}

func observationFault(value string) *observability.FaultClass {
	fault := observability.FaultClass(value)
	return &fault
}

func sharedInvestigationSummary(s logChainInvestigationSummary) investigationObservationSummary {
	level, candidate := sharedInvestigationLevel(s.EvidenceLevel)
	out := investigationObservationSummary{EvidenceLevel: level, Candidate: candidate, Conclusion: s.Conclusion}
	switch s.Classification {
	case "business_completed", "application_reached":
		// Normal consumption and HTTP completion are not protocol success.
	case "platform_routing_rejection":
		out.FaultClass = observationFault("route_no_channel")
	case "client_disconnect_at_edge", "client_or_network_disconnect", "downstream_disconnect":
		out.FaultClass = observationFault("client_gone")
	case "upstream_5xx":
		out.FaultClass = observationFault("upstream_5xx")
	case "inconclusive", "telemetry_unavailable", "":
		out.FaultClass = observationFault("telemetry_gap")
	default:
		// A generic platform/database/upstream failure has no narrower class
		// proven by this summary. Preserve its explanation, not a guessed class.
		out.FaultClass = observationFault("unknown")
	}
	return out
}

func (m *Monitor) observationGatewayReference(hash, keyID string) *observability.HMACReference {
	if hash == "" || keyID != m.cfg.CloudWatchEvidenceHMACKeyID {
		return nil
	}
	ref := observability.HMACReference{Hash: hash, KeyID: keyID}
	if ref.Validate() != nil {
		return nil
	}
	return &ref
}

func sharedEvidenceFault(e cloudWatchStructuredEvidence) *observability.FaultClass {
	if e.Kind == cwEvidenceNginxAccess || e.Kind == cwEvidenceCloudFrontAccess || e.Kind == cwEvidenceCloudFrontDiagnostic {
		if e.Status != nil && (*e.Status == 499 || *e.Status == 0) {
			return observationFault("client_gone")
		}
		if e.Status != nil && *e.Status >= 400 {
			// Edge/Nginx upstream is the application, not a model supplier.
			return observationFault("unknown")
		}
		return nil
	}
	// These parser categories share the historical NewAPIError kind with
	// failures. The kind names a log source, not proof of a business fault.
	switch e.Category {
	case "request_completed", "stream_ended", "billing_recorded", "quota_precheck":
		return nil
	case "invalid_token", "token_disabled", "quota_account", "model_forbidden":
		// Legacy auth_quota_account means a customer-side pre-route refusal
		// here. The shared class requires a proven upstream account failure.
		return observationFault("unknown")
	}
	if e.FaultClass != "" && observability.FaultClass(e.FaultClass).Valid() {
		return observationFault(e.FaultClass)
	}
	if e.Category == "route_no_channel" {
		return observationFault("route_no_channel")
	}
	if e.Kind == cwEvidenceNewAPIError || e.Kind == cwEvidenceMasterError || e.Category == "database_error" {
		return observationFault("unknown")
	}
	return nil
}

func (m *Monitor) buildInvestigationObservability(result logChainInvestigationResult) *investigationObservability {
	from, _ := time.Parse(time.RFC3339, result.Scope.FromUTC)
	to, _ := time.Parse(time.RFC3339, result.Scope.ToUTC)
	out := &investigationObservability{
		SchemaVersion: observability.SchemaVersion, From: result.Scope.FromUTC, To: result.Scope.ToUTC,
		Summary: sharedInvestigationSummary(result.Summary), Events: []investigationObservation{},
		SourceQuality: buildInvestigationSourceQuality(result.SourceStatus, from.Unix(), to.Unix(), time.Now()),
		MetricNote:    "FRT 只表示 NewAPI 观测到首个 data: 行，不代表首个有效 token。缺少协议级观察点，TTFB/TTFE/TTFC/TTFT、合法终态和客户端最终交付均不可确认。事件数不是独立用户请求数。",
	}
	hasBusinessFailure, hasBusinessConsumption := false, false
	for _, row := range result.Requests {
		if row.ID <= 0 || row.CreatedAt <= 0 {
			continue
		}
		ref := m.investigationDigest("newapi-log-row", strconv.FormatInt(row.ID, 10))
		level, candidate := observability.EvidenceExact, false
		if (result.Scope.CloudFrontRef != "" && result.Scope.NewAPIRequestRef == "") ||
			(result.Scope.NewAPIRequestRef != "" && (row.RequestID == "" || m.investigationDigest("oneapi-request-id", row.RequestID) != result.Scope.NewAPIRequestRef)) {
			level, candidate = observability.EvidenceUnavailable, true
		}
		e := investigationObservation{
			EventID: "newapi_database:" + ref, SourceEventID: ref, OccurredAt: time.Unix(row.CreatedAt, 0).UTC().Format(time.RFC3339Nano),
			Source: "newapi_database", EvidenceRef: ref, HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID, EvidenceLevel: level, Candidate: candidate,
			Complete: result.CandidatesComplete, Metrics: observability.UnknownMetrics(),
		}
		if row.RequestID != "" {
			e.GatewayRef = m.observationGatewayReference(m.investigationDigest("oneapi-request-id", row.RequestID), m.cfg.CloudWatchEvidenceHMACKeyID)
		}
		if row.Type == 5 || len(row.AnomalyTags) > 0 {
			hasBusinessFailure = hasBusinessFailure || !candidate
			e.FaultClass = observationFault("unknown")
			if row.UpstreamStatusCode >= 500 && row.UpstreamStatusCode <= 599 {
				e.FaultClass = observationFault("upstream_5xx")
			}
		}
		if row.Type == 2 && len(row.AnomalyTags) == 0 && !candidate {
			hasBusinessConsumption = true
		}
		if row.IsStream && row.Type == 2 && row.FirstByteMs > 0 {
			frt := row.FirstByteMs
			e.FRTMs, e.FRTSemantics = &frt, "NewAPI other.frt: first data line; not verified content/token"
		}
		out.Events = append(out.Events, e)
	}
	complete := map[cloudWatchLogSourceID]bool{}
	for _, s := range result.SourceStatus {
		complete[s.Source] = investigationSourceComplete(s)
	}
	for _, group := range result.Evidence {
		for _, item := range group.Evidence {
			if item.EventMS <= 0 || item.EventRef == "" {
				continue
			}
			level, candidate := sharedInvestigationLevel(item.EvidenceLevel)
			out.Events = append(out.Events, investigationObservation{
				EventID: string(group.Source) + ":" + item.EventRef, SourceEventID: item.EventRef,
				OccurredAt: time.UnixMilli(item.EventMS).UTC().Format(time.RFC3339Nano), Source: string(group.Source), EvidenceRef: item.EventRef, HMACKeyID: item.HMACKeyID,
				GatewayRef: m.observationGatewayReference(item.OneAPIIDHMAC, item.HMACKeyID), FaultClass: sharedEvidenceFault(item),
				EvidenceLevel: level, Candidate: candidate, Complete: complete[group.Source], Metrics: observability.UnknownMetrics(),
			})
		}
	}
	if result.Summary.Classification == "inconclusive" {
		// An observed but unclassified failure is unknown, not a missing
		// observation point. Unrelated candidates cannot establish this fact.
		for _, event := range out.Events {
			if event.Candidate || event.FaultClass == nil || *event.FaultClass == observability.FaultTelemetryGap ||
				(event.EvidenceLevel != observability.EvidenceExact && event.EvidenceLevel != observability.EvidenceCorrelated) {
				continue
			}
			out.Summary.FaultClass = observationFault("unknown")
			out.Summary.Candidate = false
			if out.Summary.EvidenceLevel != observability.EvidenceExact {
				out.Summary.EvidenceLevel = event.EvidenceLevel
			}
		}
	}
	// Protect versioned responses from older/cached normal-consumption
	// summaries too. The associated failure fact must survive the projection.
	if hasBusinessFailure && (result.Summary.Classification == "business_completed" ||
		result.Summary.Classification == "application_reached" || result.Summary.Classification == "inconclusive" || result.Summary.Classification == "") {
		out.Summary.FaultClass = observationFault("unknown")
		out.Summary.EvidenceLevel, out.Summary.Candidate = observability.EvidenceExact, false
		out.Summary.Conclusion = investigationUnknownFailureConclusion(hasBusinessConsumption)
	}
	if len(out.Events) == 0 && len(result.SourceStatus) == 0 && (result.Status == "queued" || result.Status == "running" || result.Status == "cancelled") {
		// Task lifecycle is not a business observation or a telemetry failure.
		out.Summary.FaultClass = nil
		out.Summary.EvidenceLevel, out.Summary.Candidate = observability.EvidenceUnavailable, false
	}
	return out
}

func (m *Monitor) investigationResponse(result logChainInvestigationResult) logChainInvestigationResult {
	if result.SchemaVersion != observability.SchemaVersion {
		return result
	}
	result.Observability = m.buildInvestigationObservability(result)
	result.Summary.EvidenceLevel = string(result.Observability.Summary.EvidenceLevel)
	result.Summary.Conclusion = result.Observability.Summary.Conclusion
	if result.Observability.Summary.FaultClass != nil && *result.Observability.Summary.FaultClass == observability.FaultUnknown &&
		(result.Summary.Classification == "business_completed" || result.Summary.Classification == "application_reached") {
		result.Summary.Classification = "inconclusive"
	}
	result.Timeline = append([]logChainInvestigationTimelineEvent(nil), result.Timeline...)
	for i := range result.Timeline {
		level, candidate := sharedInvestigationLevel(result.Timeline[i].EvidenceLevel)
		result.Timeline[i].EvidenceLevel, result.Timeline[i].Candidate = string(level), candidate
	}
	// Explicit v1 consumers must not receive raw SQL rows or source user IDs.
	// Preserve safe summary/timeline fields for the existing renderer only.
	result.Requests, result.Evidence = nil, nil
	result.Scope.UserID, result.Scope.Model, result.Scope.Group = 0, "", ""
	result.Scope.Path = cwRoute(result.Scope.Path)
	return result
}
