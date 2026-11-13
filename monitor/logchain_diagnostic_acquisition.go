package monitor

import (
	"regexp"
	"strconv"
	"strings"
)

var logChainDiagnosticStreamEnd = regexp.MustCompile(`^stream ended:\s*reason=[a-z_]+\s+end_error=("(?:\\.|[^"\\])*")(?:,|$)`)

// The same stream error can appear in its end event and in the subsequent
// consumption record. A timestamp/log prefix or an empty JSON wrapper is not
// another attempt. Distinct response bodies/codes and explicit retry IDs still
// retain their original isolation checks.
func logChainDiagnosticAttemptError(e LogChainDiagnosticError) string {
	text := strings.TrimSpace(e.Message)
	if match := cwNewAPIPipeContext.FindStringSubmatch(text); len(match) == 2 {
		parts := strings.SplitN(match[1], "|", 5)
		for len(parts) > 1 {
			head := strings.TrimSpace(parts[0])
			if !cwNewAPIRequestIDSeg.MatchString(head) && !cwNewAPIUserSeg.MatchString(head) && head != "relay" {
				break
			}
			parts = parts[1:]
		}
		text = strings.TrimSpace(strings.Join(parts, "|"))
	}
	if e.Code != "" || e.Type != "" || e.Status != 0 || e.Param != "" {
		return text + "\x00" + e.Code + "\x00" + e.Type + "\x00" + e.Param + "\x00" + strconv.Itoa(e.Status)
	}
	if strings.HasPrefix(text, "record consume log:") && strings.HasSuffix(text, "params=") {
		return "" // Only the wrapper; nested structured errors are separate entries.
	}
	if match := logChainDiagnosticStreamEnd.FindStringSubmatch(text); len(match) == 2 {
		if inner, err := strconv.Unquote(match[1]); err == nil {
			return strings.TrimSpace(inner)
		}
	}
	return text
}

// One field may be present at more than one hop. Asking only Nginx for elapsed
// time, for example, discarded the application's existing stream/error record.
func logChainDiagnosticFieldSources(field string) []cloudWatchLogSourceID {
	switch field {
	case "request_length", "request_method", "request_path", "connect_ms", "first_byte_ms", "total_ms", "layer_statuses", "node":
		return []cloudWatchLogSourceID{cwSourceWorkerNginx, cwSourceWorkerNewAPI}
	default:
		return []cloudWatchLogSourceID{cwSourceWorkerNewAPI}
	}
}

func logChainDeliveryDiagnostic(r LogChainRow, fault logChainFault) *LogChainDiagnosis {
	if r.Type != 2 || len(r.AnomalyTags) == 0 {
		return nil
	}
	fields := logChainRowDiagnosticFacts(r)
	d := &LogChainDiagnosis{RuleID: "DELIVERY-EVIDENCE-v1", Version: "customer-diagnostic-acquisition-v1",
		Reason: fault.Reason, Confidence: fault.Confidence, CauseKnown: false,
		Responsibility:     map[string]string{faultUnknown: "待查", faultOurs: "平台侧", faultDownstream: "客户侧", faultUpstream: "上游侧"}[fault.Fault],
		ResponsibleParties: []string{fault.Fault}, ErrorLayer: "交付异常；需要分层证据定位", Basis: fault.Why,
		Action: "按同一 Request ID 与 attempt_id 补取应用流结束记录和入口状态；保留已有用量与已知原因", ActionOwners: []string{faultOurs}, Profile: "delivery", Missing: []LogChainDiagnosticMissing{}}
	if logChainVerifiedClientDisconnect(r) {
		d.CauseKnown = true
		d.ErrorLayer = "客户端至入口连接"
	}
	for _, key := range []string{"error_response", "stream_end_reason", "attempt_id", "upstream_request_id", "layer_statuses", "connect_ms", "first_byte_ms", "node"} {
		if fields.value(key) != "" || key == "upstream_request_id" && fields.hasUpstreamIdentity() {
			continue
		}
		d.Missing = append(d.Missing, LogChainDiagnosticMissing{Field: key, Reason: "当前交付异常未记录该证据", RecordAt: logChainDiagnosticRecordAt(key)})
	}
	return d
}

// Source status is a statement about this bounded lookup, not about all logs
// in production. Empty or rejected retry identities must never erase a cause.
func logChainDiagnosticAcquisitionResult(r *LogChainRow, evidence []cloudWatchStructuredEvidence, statuses []logChainCloudWatchSourceStatus, ref string) {
	if r.Diagnosis == nil {
		return
	}
	for i := range r.Diagnosis.Missing {
		gap := &r.Diagnosis.Missing[i]
		gap.Sources = logChainDiagnosticFieldSources(gap.Field)
		gap.Status = "not_queried"
		gap.SourceResults = map[cloudWatchLogSourceID]string{}
		priority := 0
		for _, source := range gap.Sources {
			for _, s := range statuses {
				if s.Source != source || s.Status == "skipped" {
					continue
				}
				state, rank := "source_query_"+s.Status, 3
				switch s.Status {
				case "found", "empty":
					state, rank = "no_exact_request_evidence", 1
					for _, e := range evidence {
						if e.Source == source && e.EvidenceLevel == "exact" && e.OneAPIIDHMAC == ref {
							state = "not_recorded_in_queried_evidence"
							break
						}
					}
				}
				if s.Truncated {
					state, rank = "source_query_truncated", 3
				}
				gap.SourceResults[source] = state
				if rank >= priority {
					gap.Status, priority = state, rank
				}
			}
		}
		for _, e := range evidence {
			if e.OneAPIIDHMAC != ref || e.Diagnostic == nil {
				continue
			}
			if e.Diagnostic.value(gap.Field) != "" || gap.Field == "upstream_request_id" && e.Diagnostic.hasUpstreamIdentity() {
				gap.Status = "identity_or_attempt_not_proven"
				gap.SourceResults[e.Source] = gap.Status
			}
		}
	}
}

func logChainApplyDiagnosticEntry(r *LogChainRow, f *LogChainDiagnosticFacts, e cloudWatchStructuredEvidence) {
	// Called only after the existing exact identity, time, unique-event and
	// single-attempt guards. Preserve each hop rather than reporting a provider
	// HTTP status from Nginx's upstream(NewAPI) status.
	f.put("node_reference", e.StreamRef, "cloudwatch:log_stream_ref", e.EventMS/1000, "request")
	status := ""
	if e.Status != nil {
		status = "nginx=" + strconv.Itoa(*e.Status)
	}
	if len(e.UpstreamStatuses) == 1 {
		status += ";newapi=" + strconv.Itoa(e.UpstreamStatuses[0])
	}
	if status != "" {
		f.put("entry_layer_statuses", status, "cloudwatch:nginx", e.EventMS/1000, "request")
		f.put("layer_statuses", status, "cloudwatch:nginx", e.EventMS/1000, "request")
	}
	if e.Status == nil {
		return
	}
	edge := &LogChainEdgeEvidence{EventMS: e.EventMS, Route: e.Route, Status: *e.Status, RequestLength: e.RequestLength, Completion: e.Completion}
	if old := r.EdgeEvidence; r.EdgeEvidenceVerified && old != nil && old.EventMS == e.EventMS && old.Route == e.Route && old.Status == *e.Status {
		copy := *old
		edge = &copy
		if e.RequestLength != nil {
			edge.RequestLength = e.RequestLength
		}
		if e.Completion != "" {
			edge.Completion = e.Completion
		}
	}
	for target, source := range map[*int64]*int64{&edge.RequestMS: e.RequestMS, &edge.UpstreamMS: e.UpstreamMS, &edge.ConnectMS: e.ConnectMS, &edge.HeaderMS: e.HeaderMS, &edge.BytesSent: e.BytesSent} {
		if source != nil {
			*target = *source
		}
	}
	if e.RequestLength != nil {
		edge.RequestLengthSemantics = "whole_http_request_not_body_or_token_count"
	}
	if len(e.UpstreamStatuses) == 1 {
		edge.UpstreamStatus = e.UpstreamStatuses[0]
		edge.UpstreamAttempts = 1
		edge.UpstreamPresent = true
	}
	r.EdgeEvidence, r.EdgeEvidenceVerified = edge, true
}
