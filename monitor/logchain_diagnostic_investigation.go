package monitor

import (
	"strconv"
	"strings"
)

func (m *Monitor) customerDiagnosticEvidenceParser(allowSensitive bool) (*cloudWatchEvidenceParser, error) {
	p, err := m.cloudWatchEvidenceParser(allowSensitive)
	if err == nil {
		p.customerDiagnostics = true
	}
	return p, err
}

// Administrator-only result projection. Versioned observability responses
// retain no raw SQL rows or request IDs; this reference binds a correction to
// exactly one list row, including its request identity and occurrence time.
type logChainRequestDiagnosis struct {
	Reference       string                   `json:"ref"`
	Diagnosis       *LogChainDiagnosis       `json:"diagnosis"`
	DiagnosticFacts *LogChainDiagnosticFacts `json:"diagnostic_facts"`
	Fault           string                   `json:"fault"`
	FaultReason     string                   `json:"fault_reason"`
	FaultConfidence string                   `json:"fault_confidence"`
	FaultWhy        string                   `json:"fault_why"`
}

func (m *Monitor) logChainDiagnosticReference(r LogChainRow) string {
	if r.RequestID == "" || len(m.cfg.CloudWatchEvidenceHMACKey) < 32 {
		return ""
	}
	return m.investigationDigest("logchain-diagnosis", strconv.FormatInt(r.ID, 10)+":"+strconv.FormatInt(r.CreatedAt, 10)+":"+r.RequestID)
}

func (m *Monitor) logChainRequestDiagnoses(rows []LogChainRow) []logChainRequestDiagnosis {
	var out []logChainRequestDiagnosis
	for _, r := range rows {
		ref := m.logChainDiagnosticReference(r)
		if ref == "" || r.Diagnosis == nil {
			continue
		}
		redact := func(s string) string { return strings.ReplaceAll(s, r.RequestID, "<request-ref>") }
		d := *r.Diagnosis
		d.Reason, d.Basis = redact(d.Reason), redact(d.Basis)
		d.Variables = make(map[string]string, len(r.Diagnosis.Variables))
		for k, v := range r.Diagnosis.Variables {
			d.Variables[k] = redact(v)
		}
		f := logChainRowDiagnosticFacts(r)
		delete(f.Fields, "request_id")
		for k, v := range f.Fields {
			v.Value = redact(v.Value)
			f.Fields[k] = v
		}
		for i := range f.Errors {
			e := &f.Errors[i]
			e.Message, e.Code, e.Type, e.Param = redact(e.Message), redact(e.Code), redact(e.Type), redact(e.Param)
			e.AttemptID = redact(e.AttemptID)
		}
		for i := range f.RequestIDs {
			id := &f.RequestIDs[i]
			id.Value, id.Source, id.AttemptID, id.Reason = redact(id.Value), redact(id.Source), redact(id.AttemptID), redact(id.Reason)
		}
		out = append(out, logChainRequestDiagnosis{Reference: ref, Diagnosis: &d,
			DiagnosticFacts: f, Fault: r.Fault, FaultReason: redact(r.FaultReason),
			FaultConfidence: r.FaultConfidence, FaultWhy: redact(r.FaultWhy)})
	}
	return out
}

func logChainDiagnosticNeedsSource(rows []LogChainRow, requestID string, source cloudWatchLogSourceID) bool {
	found := false
	for _, r := range rows {
		if r.RequestID != requestID {
			continue
		}
		found = true
		if r.Diagnosis == nil {
			return true
		}
		for _, missing := range r.Diagnosis.Missing {
			for _, candidate := range logChainDiagnosticFieldSources(missing.Field) {
				if source == candidate {
					return true
				}
			}
		}
	}
	return !found
}

// Request equality is necessary but not sufficient when an ID has retries.
// Only explicit attempt/provider identity may bridge overlapping attempts.
func (m *Monitor) rediagnoseInvestigationRequests(result *logChainInvestigationResult) {
	counts := map[string]int{}
	for _, r := range result.Requests {
		counts[r.RequestID]++
	}
	rows := append([]LogChainRow(nil), result.Requests...)
	evidence := flattenInvestigationEvidence(result.Evidence)
	for i := range rows {
		r := &rows[i]
		f := logChainRowDiagnosticFacts(*r)
		ref := m.investigationDigest("oneapi-request-id", r.RequestID)
		for _, e := range evidence {
			if r.RequestID == "" || e.EvidenceLevel != "exact" || e.OneAPIIDHMAC != ref || e.HMACKeyID != m.cfg.CloudWatchEvidenceHMACKeyID {
				continue
			}
			if e.Kind == cwEvidenceNginxAccess && counts[r.RequestID] == 1 && len(e.UpstreamStatuses) <= 1 &&
				!logChainEvidenceHasMultipleAttempts(evidence, ref) &&
				logChainUniqueDiagnosticNginx(evidence, ref, e.HMACKeyID) &&
				(r.CreatedAt == 0 || (e.EventMS >= r.CreatedAt*1000-2000 && e.EventMS <= (r.CreatedAt+r.UseTime+2)*1000)) {
				f.put("request_path", e.Route, "cloudwatch:nginx", e.EventMS/1000, "request")
				f.put("request_method", e.Method, "cloudwatch:nginx", e.EventMS/1000, "request")
				for key, value := range map[string]*int64{"request_length": e.RequestLength, "connect_ms": e.ConnectMS, "first_byte_ms": e.HeaderMS, "total_ms": e.RequestMS} {
					if value != nil {
						f.put("nginx_"+key, strconv.FormatInt(*value, 10), "cloudwatch:nginx:"+key, e.EventMS/1000, "request")
						f.put(key, strconv.FormatInt(*value, 10), "cloudwatch:nginx:"+key, e.EventMS/1000, "request")
					}
				}
				logChainApplyDiagnosticEntry(r, f, e)
			}
			if e.Diagnostic == nil {
				continue
			}
			extra := logChainCloneDiagnosticFacts(e.Diagnostic)
			extra.resolveRequestIDs(r.RequestID)
			attempt, otherAttempt := f.value("attempt_id"), extra.value("attempt_id")
			if attempt != "" && otherAttempt != "" && attempt != otherAttempt {
				continue
			}
			identityMatch, identityConflict := logChainDiagnosticIdentityRelation(f, extra)
			if identityConflict {
				continue
			}
			proven := (attempt != "" && attempt == otherAttempt) || identityMatch
			if counts[r.RequestID] > 1 && !proven {
				continue
			}
			// Even if the result contains one row, other attempts may have been
			// outside its bounded SQL page. Conflicting attempt metadata rejects
			// all unbound application errors, rather than selecting one by time.
			if !proven && logChainEvidenceHasMultipleAttempts(evidence, ref) {
				continue
			}
			if !proven && r.CreatedAt > 0 && (e.EventMS < r.CreatedAt*1000-2000 || e.EventMS > (r.CreatedAt+r.UseTime+2)*1000) {
				continue
			}
			seen := map[LogChainDiagnosticError]bool{}
			for _, existing := range f.Errors {
				seen[existing] = true
			}
			for _, next := range extra.Errors {
				if seen[next] {
					continue
				}
				if len(f.Errors) >= 64 {
					f.Truncated = true
					break
				}
				f.Errors = append(f.Errors, next)
				seen[next] = true
			}
			f.mergeRequestIDs(extra)
			for key, v := range extra.Fields {
				if old, ok := f.Fields[key]; !ok || (old.Scope == "current_config" && v.Scope == "request") {
					f.Fields[key] = v
				}
			}
			f.resolveRequestIDs(r.RequestID)
		}
		r.DiagnosticFacts = f
		logChainApplyFault(r, logChainAttributeFaultWithEvidence(*r, r.AnomalyTags))
		logChainDiagnosticAcquisitionResult(r, evidence, result.SourceStatus, ref)
	}
	result.Requests = rows
}

func logChainEvidenceHasMultipleAttempts(evidence []cloudWatchStructuredEvidence, ref string) bool {
	ids := map[string]bool{}
	providers := map[string]map[string]bool{}
	errors := map[string]bool{}
	for _, e := range evidence {
		if e.OneAPIIDHMAC != ref || e.EvidenceLevel != "exact" || e.Diagnostic == nil {
			continue
		}
		f := logChainCloneDiagnosticFacts(e.Diagnostic)
		f.resolveRequestIDs("")
		if f.identityConflict() {
			return true
		}
		if a := f.value("attempt_id"); a != "" {
			ids["attempt:"+a] = true
		}
		for _, id := range f.RequestIDs {
			if id.Status == "bound" && id.Owner != "platform" {
				if providers[id.Owner] == nil {
					providers[id.Owner] = map[string]bool{}
				}
				providers[id.Owner][id.Value] = true
			}
		}
		// Do not count the two identity namespaces as different attempts.
		var messages []string
		for _, er := range e.Diagnostic.Errors {
			if signature := logChainDiagnosticAttemptError(er); signature != "" {
				messages = append(messages, signature)
			}
		}
		if len(messages) > 0 {
			errors[strings.Join(messages, "\n")] = true
		}
	}
	if len(ids) > 1 {
		return true
	}
	for _, values := range providers {
		if len(values) > 1 {
			return true
		}
	}
	// Without attempt IDs, two distinct error responses cannot safely be
	// collapsed into one retry, even when their timestamps are adjacent.
	return len(ids) == 0 && len(providers) == 0 && len(errors) > 1
}

func logChainUniqueDiagnosticNginx(evidence []cloudWatchStructuredEvidence, ref, keyID string) bool {
	seen := map[string]bool{}
	for _, e := range evidence {
		if e.Kind != cwEvidenceNginxAccess || e.OneAPIIDHMAC != ref || e.HMACKeyID != keyID || e.EvidenceLevel != "exact" {
			continue
		}
		key := e.EventRef
		if key == "" {
			key = strconv.FormatInt(e.EventMS, 10)
			if e.Status != nil {
				key += ":" + strconv.Itoa(*e.Status)
			}
		}
		seen[key] = true
	}
	return len(seen) == 1
}

func logChainDiagnosticInvestigationSummary(rows []LogChainRow, evidence []cloudWatchStructuredEvidence, in logChainInvestigationInput) (logChainInvestigationSummary, bool) {
	var diagnosed []LogChainRow
	for _, r := range rows {
		if in.NewAPIRequestID != "" && r.RequestID != in.NewAPIRequestID {
			continue
		}
		if in.CloudFrontRequestID != "" && in.NewAPIRequestID == "" {
			continue
		}
		if r.Diagnosis != nil {
			diagnosed = append(diagnosed, r)
		}
	}
	if len(diagnosed) == 0 {
		// SQL unavailability must not hide a specific error from an exact
		// application event. Diagnose each event separately, never merge retries.
		seen := map[string]bool{}
		for _, e := range evidence {
			if in.NewAPIRequestID == "" || e.OneAPIRequestID != in.NewAPIRequestID || e.EvidenceLevel != "exact" || e.Diagnostic == nil || len(e.Diagnostic.Errors) == 0 {
				continue
			}
			r := LogChainRow{Type: 5, RequestID: in.NewAPIRequestID, CreatedAt: e.EventMS / 1000, DiagnosticFacts: e.Diagnostic}
			if r.Diagnosis = logChainDiagnose(r); r.Diagnosis == nil {
				continue
			}
			key := r.Diagnosis.Reason + "\x00" + r.Diagnosis.Responsibility
			if !seen[key] {
				diagnosed = append(diagnosed, r)
				seen[key] = true
			}
		}
		if len(diagnosed) == 0 {
			return logChainInvestigationSummary{}, false
		}
	}
	s := logChainInvestigationSummary{Classification: "request_diagnosis", EvidenceLevel: "exact", CustomerImpact: "single_request"}
	if len(diagnosed) == 1 {
		s.Diagnosis = diagnosed[0].Diagnosis
		s.Conclusion = s.Diagnosis.Reason + "；责任方：" + s.Diagnosis.Responsibility
		return s, true
	}
	var notes []string
	for _, r := range diagnosed {
		if len(notes) >= 5 {
			break
		}
		label := "日志 " + strconv.FormatInt(r.ID, 10)
		if r.ID == 0 {
			label = "应用错误事件"
		}
		notes = append(notes, label+"："+r.Diagnosis.Reason+"（"+r.Diagnosis.Responsibility+"）")
	}
	s.Conclusion = "分别诊断各条请求/重试记录：" + strings.Join(notes, "；")
	s.CustomerImpact = "multiple_candidates"
	return s, true
}
