package monitor

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

// Optional administrator-captured, pseudonymized SQL page. No network access;
// all production reads stay in the existing bounded HTTP query workflow.
func TestCustomerDiagnosticReviewCapturedDayReplay(t *testing.T) {
	file := os.Getenv("CUSTOMER_DIAGNOSTIC_REPLAY")
	if file == "" {
		t.Skip("no captured day page supplied")
	}
	var sample struct {
		Rows []struct {
			LogChainRow
			OldMissing bool `json:"old_missing_upstream_id"`
		}
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &sample); err != nil {
		t.Fatal(err)
	}
	ids := regexp.MustCompile(`(?i)\(request[ _]id\s*:\s*([^\s()]+)\)`)
	errors, bound, corrected, ambiguous, rawIDRows := 0, 0, 0, 0, 0
	for _, s := range sample.Rows {
		r := s.LogChainRow
		if r.Type != 5 {
			continue
		}
		errors++
		f := logChainRowDiagnosticFacts(r)
		mentions := map[string]bool{}
		for _, match := range ids.FindAllStringSubmatch(r.Content, -1) {
			if match[1] != r.RequestID {
				mentions[match[1]] = true
			}
		}
		d := logChainDiagnose(r)
		if len(mentions) > 0 {
			rawIDRows++
		}
		for value := range mentions {
			found := false
			for _, candidate := range f.RequestIDs {
				if candidate.Value == value && candidate.Source != "" {
					found = true
				}
			}
			if !found {
				t.Fatalf("log %d: an original ID or its provenance was lost", r.ID)
			}
		}
		if len(mentions) == 1 && logChainFaultFromUpstream(r.Content) {
			if !f.hasUpstreamIdentity() || logChainDiagnosticJoinKey(r) == "" {
				t.Fatalf("log %d: unique recorded ID not bound", r.ID)
			}
			if d != nil && reviewMissing(d, "upstream_request_id") {
				t.Fatalf("log %d: recorded ID still missing", r.ID)
			}
			bound++
			if s.OldMissing {
				corrected++
			}
		}
		if len(mentions) > 1 && logChainFaultFromUpstream(r.Content) {
			if !f.identityConflict() || logChainDiagnosticJoinKey(r) != "" {
				t.Fatalf("log %d: picked one of multiple unscoped IDs", r.ID)
			}
			if d != nil && reviewMissing(d, "upstream_request_id") {
				for _, missing := range d.Missing {
					if missing.Field == "upstream_request_id" && !strings.Contains(missing.Reason, "原文已有") {
						t.Fatalf("log %d: existing ambiguous ID reported as absent", r.ID)
					}
				}
			}
			ambiguous++
		}
		if d != nil {
			basic, withEvidence := logChainAttributeFault(r, nil), logChainAttributeFaultWithEvidence(r, nil)
			if basic.Reason != d.Reason || withEvidence.Reason != d.Reason || basic.Fault != withEvidence.Fault {
				t.Fatalf("log %d: entry mismatch", r.ID)
			}
			logChainApplyFault(&r, withEvidence)
			summary, ok := logChainDiagnosticInvestigationSummary([]LogChainRow{r}, nil, logChainInvestigationInput{})
			if !ok || summary.Diagnosis.Reason != d.Reason {
				t.Fatalf("log %d: summary mismatch", r.ID)
			}
		}
	}
	if errors == 0 || bound == 0 {
		t.Fatal("capture contains no applicable error/ID cases")
	}
	t.Logf("captured_rows=%d error_rows=%d original_id_rows=%d unique_original_ids_bound=%d previously_missing_corrected=%d multiple_ids_retained_and_flagged=%d", len(sample.Rows), errors, rawIDRows, bound, corrected, ambiguous)
}

func reviewMissing(d *LogChainDiagnosis, key string) bool {
	for _, m := range d.Missing {
		if m.Field == key {
			return true
		}
	}
	return false
}

func TestCustomerDiagnosticReviewRepeatedStreamFacts(t *testing.T) {
	r := LogChainRow{Type: 2, RequestID: "platform-stream", CreatedAt: 100, EndError: "status_code=502, openai_error (request_id: upstream-stream)", AnomalyTags: []string{"stream"}}
	rows := []LogChainRow{r}
	logChainRefreshFaults(rows)
	before, _ := json.Marshal(rows)
	for range 8 {
		logChainRefreshFaults(rows)
	}
	after, _ := json.Marshal(rows)
	if string(before) != string(after) {
		t.Fatal("refresh duplicated stream errors or request ID facts")
	}
}

func TestCustomerDiagnosticReviewCodePriority(t *testing.T) {
	for _, tc := range []struct{ code, wrapper, cause, party string }{
		{"array_above_max_length", "request failed", "数组长度超限", "downstream"},
		{"upstream_http2_stream_error", "openai_error", "HTTP/2 流错误", "upstream"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			for _, structured := range []bool{false, true} {
				r := LogChainRow{Type: 5, Content: "status_code=502, " + tc.wrapper, UpstreamErrorCode: tc.code}
				if structured {
					r.UpstreamErrorCode = ""
					r.Content = `status_code=502, {"error":{"message":"{\"error\":{\"message\":\"` + tc.wrapper + `\",\"code\":\"` + tc.code + `\",\"param\":\"tools[37]\"}}"}}`
				}
				for _, entry := range []func(LogChainRow, []string) logChainFault{logChainAttributeFault, logChainAttributeFaultWithEvidence} {
					f := entry(r, nil)
					if f.Fault != tc.party || f.Diagnosis == nil || !f.Diagnosis.CauseKnown || !strings.Contains(f.Reason, tc.cause) {
						t.Fatalf("structured=%v: %+v", structured, f)
					}
					if !strings.Contains(f.Why, tc.code) || !strings.Contains(f.Why, "来源") {
						t.Fatal("missing provenance")
					}
					if logChainHumanFaultReason(r, f) != f.Reason || logChainRefineUnknownFault(r, logChainFault{Fault: faultUnknown}).Reason != f.Reason {
						t.Fatal("entry points disagree")
					}
					logChainApplyFault(&r, f)
					s, ok := logChainDiagnosticInvestigationSummary([]LogChainRow{r}, nil, logChainInvestigationInput{})
					if !ok || s.Diagnosis.Reason != f.Reason || !strings.Contains(sharedInvestigationSummary(s).Conclusion, tc.cause) {
						t.Fatal("summary differs")
					}
				}
			}
			untrusted := LogChainRow{Type: 5, Content: tc.wrapper, UpstreamErrorCode: tc.code}
			if d := logChainDiagnose(untrusted); d != nil && strings.HasPrefix(d.RuleID, "LC-CODE-") {
				t.Fatal("unattributed code trusted")
			}
			untrusted.Content = "status_code=502, " + tc.wrapper
			untrusted.DiagnosticFacts = logChainParseDiagnosticFacts(untrusted.Content, `{"error_source":"platform"}`, "logs", 100)
			if d := logChainDiagnose(untrusted); d != nil && strings.HasPrefix(d.RuleID, "LC-CODE-") {
				t.Fatal("explicit platform origin overridden by status wrapper")
			}
		})
	}
	// An explicit catalogue cause is stronger than a wrapper's generic code.
	for _, text := range []string{"system cpu overloaded (current 96%, threshold 80%)", "future-model requires adaptive thinking", "prompt is too long: 987654 tokens > 456789 maximum"} {
		d := logChainDiagnose(LogChainRow{Type: 5, Content: "status_code=500, " + text, UpstreamErrorCode: "upstream_http2_stream_error"})
		if d == nil || strings.HasPrefix(d.RuleID, "LC-CODE-") || !d.CauseKnown {
			t.Fatalf("specific catalogue lost: %+v", d)
		}
	}
	conflict := LogChainRow{Type: 5, Content: `status_code=502, {"error":{"code":"upstream_http2_stream_error","message":"openai_error"},"cause":{"code":"array_above_max_length","message":"request failed"}}`}
	if d := logChainDiagnose(conflict); d != nil && strings.HasPrefix(d.RuleID, "LC-CODE-") {
		t.Fatal("picked a conflicting code")
	}
	retry := LogChainRow{Type: 5, Content: `status_code=502, {"attempt_id":"try-current","message":"openai_error","cause":{"attempt_id":"try-old","code":"upstream_http2_stream_error"}}`}
	if d := logChainDiagnose(retry); d != nil && strings.HasPrefix(d.RuleID, "LC-CODE-") {
		t.Fatal("old attempt code used")
	}
	retry.Content = `status_code=502, {"attempt_id":"try-current","message":"openai_error","cause":{"attempt_id":"try-old","code":"context_length_exceeded","message":"prompt is too long: 9999 tokens > 1234 maximum"}}`
	if d := logChainDiagnose(retry); d != nil && d.CauseKnown {
		t.Fatal("old attempt's specific semantics used")
	}
}

func TestCustomerDiagnosticReviewActualPath(t *testing.T) {
	for _, path := range []string{"/v1/alpha/search", "/v1/responses", "/custom/chat/completions"} {
		t.Run(path, func(t *testing.T) {
			r := LogChainRow{ID: 3, Type: 5, RequestID: "business", Content: `status_code=500, {"error":{"message":"Channel does not support ` + path + `?api_key=hidden)"}}`}
			logChainApplyFault(&r, logChainAttributeFaultWithEvidence(r, nil))
			want := "通道不支持 " + path
			if r.Diagnosis == nil || r.Diagnosis.RuleID != "LC-119" || r.FaultReason != want || r.Diagnosis.Variables["forward_path"] != path {
				t.Fatalf("%+v", r.Diagnosis)
			}
			s, _ := logChainDiagnosticInvestigationSummary([]LogChainRow{r}, nil, logChainInvestigationInput{NewAPIRequestID: r.RequestID})
			if s.Diagnosis.Reason != want || logChainAttributeFault(r, nil).Reason != want {
				t.Fatal("list and investigation differ")
			}
			b, _ := json.Marshal(r.Diagnosis)
			if strings.Contains(string(b), "hidden") {
				t.Fatal("path credential leaked")
			}
		})
	}
	for _, suffix := range []string{"", " /%zz", " //user:secret@example.test/path"} {
		d := logChainDiagnose(LogChainRow{Type: 5, Content: "status_code=500, Channel does not support" + suffix})
		if d == nil || d.Reason != "通道不支持本次请求的接口" || !reviewMissing(d, "forward_path") {
			t.Fatalf("unreliable path: %+v", d)
		}
	}
}

func TestCustomerDiagnosticReviewOriginalRequestIDs(t *testing.T) {
	for _, label := range []string{"request id", "request_id"} {
		t.Run(label, func(t *testing.T) {
			text := "status_code=502, openai_error (" + label + ": upstream-123)"
			for _, content := range []string{text, `status_code=502, {"error":{"message":"openai_error (` + label + `: upstream-123)"}}`, `status_code=502, {"error":{"message":"{\"error\":{\"message\":\"openai_error (` + label + `: upstream-123)\"}}"}}`} {
				r := LogChainRow{Type: 5, RequestID: "platform-123", Content: content, CreatedAt: 100}
				logChainApplyFault(&r, logChainAttributeFault(r, nil))
				if !r.DiagnosticFacts.hasUpstreamIdentity() || r.DiagnosticFacts.value("upstream_request_id") != "upstream-123" || reviewMissing(r.Diagnosis, "upstream_request_id") || logChainDiagnosticJoinKey(r) != "upstream-123" {
					t.Fatalf("ID not consumed: %+v", r.DiagnosticFacts)
				}
				id := r.DiagnosticFacts.RequestIDs[0]
				if id.Source == "" || id.ObservedAt != 100 || id.Owner != "upstream" || id.Status != "bound" {
					t.Fatalf("provenance: %+v", id)
				}
			}
		})
	}
	for _, tc := range []struct{ content, id, status string }{
		{"openai_error (request id: unowned-1)", "", "unattributed"},
		{"status_code=502, openai_error (request id: platform-1)", "platform-1", "bound"},
		{"status_code=502, openai_error (request id: first) (request_id: second)", "", "ambiguous"},
		{`status_code=502, {"attempt_id":"current","message":"openai_error","cause":{"attempt_id":"old","message":"openai_error (request id: other-try)"}}`, "", "retry_conflict"},
	} {
		r := LogChainRow{Type: 5, RequestID: tc.id, Content: tc.content}
		f := logChainRowDiagnosticFacts(r)
		if len(f.RequestIDs) == 0 {
			t.Fatalf("identities: %+v", f.RequestIDs)
		}
		for _, id := range f.RequestIDs {
			if id.Status != tc.status {
				t.Fatalf("%s: %+v", tc.status, id)
			}
		}
		if f.hasUpstreamIdentity() || logChainDiagnosticJoinKey(r) != "" {
			t.Fatal("unsafe identity selected")
		}
		d := logChainDiagnose(r)
		if d == nil || !reviewMissing(d, "upstream_request_id") {
			t.Fatal("unresolved identity hidden")
		}
	}
	// Two known layers remain separate; no arbitrary canonical upstream ID.
	f := logChainParseDiagnosticFacts("openai_error (relay request id: relay-1) (provider request_id: provider-1)", "", "logs", 100)
	if !f.hasUpstreamIdentity() || f.value("upstream_request_id") != "" || f.value("upstream_relay_request_id") != "relay-1" || f.value("model_provider_request_id") != "provider-1" {
		t.Fatalf("namespaces lost: %+v", f)
	}
	g := logChainParseDiagnosticFacts("openai_error (provider request id: relay-1)", "", "logs", 100)
	if match, _ := logChainDiagnosticIdentityRelation(f, g); match {
		t.Fatal("same bytes across different layers matched")
	}
	before, _ := json.Marshal(f)
	f.resolveRequestIDs("")
	after, _ := json.Marshal(f)
	if string(before) != string(after) {
		t.Fatal("normalization not stable")
	}
	f = logChainParseDiagnosticFacts("openai_error (relay request id: repeated) (provider request id: repeated)", "", "logs", 100)
	g = logChainParseDiagnosticFacts("status_code=502, openai_error (request id: repeated)", "", "logs", 100)
	if matched, _ := logChainDiagnosticIdentityRelation(f, g); matched {
		t.Fatal("unknown layer joined to colliding namespaces")
	}
	if logChainDiagnosticJoinKey(LogChainRow{Type: 5, DiagnosticFacts: f}) != "" {
		t.Fatal("ambiguous namespace chosen for local join")
	}
}

func TestCustomerDiagnosticReviewRequestIDSupplementIsolation(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	id := "shared-platform"
	row := func(idNumber int64, upstream, attempt string) LogChainRow {
		r := LogChainRow{ID: idNumber, Type: 5, CreatedAt: 100, RequestID: id, Content: "status_code=502, openai_error (request_id: " + upstream + ")"}
		r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, `{"attempt_id":"`+attempt+`"}`, "logs", 100)
		logChainApplyFault(&r, logChainAttributeFault(r, nil))
		return r
	}
	r1, r2 := row(1, "up-a", "try-a"), row(2, "up-b", "try-b")
	e := cloudWatchStructuredEvidence{Kind: cwEvidenceNewAPIError, EvidenceLevel: "exact", OneAPIIDHMAC: m.investigationDigest("oneapi-request-id", id), HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID, EventMS: 100000,
		Diagnostic: logChainParseDiagnosticFacts("status_code=502, future-model requires adaptive thinking (request id: up-b)", "", "cloudwatch:newapi", 100)}
	run := func(e cloudWatchStructuredEvidence) []LogChainRow {
		result := logChainInvestigationResult{Requests: []LogChainRow{r1, r2}, Evidence: []logChainCloudWatchEvidence{{Source: cwSourceWorkerNewAPI, Evidence: []cloudWatchStructuredEvidence{e}}}}
		m.rediagnoseInvestigationRequests(&result)
		return result.Requests
	}
	before, _ := json.Marshal(e)
	got := run(e)
	if got[0].Diagnosis.CauseKnown || !got[1].Diagnosis.CauseKnown || !strings.Contains(got[1].FaultReason, "thinking") {
		t.Fatal("raw ID failed to bind correct retry")
	}
	after, _ := json.Marshal(e)
	if string(before) != string(after) {
		t.Fatal("shared evidence mutated")
	}
	e.Diagnostic = logChainParseDiagnosticFacts("status_code=502, future-model requires adaptive thinking (request id: up-b)", `{"attempt_id":"try-a"}`, "cloudwatch:newapi", 100)
	got = run(e)
	if got[0].Diagnosis.CauseKnown || got[1].Diagnosis.CauseKnown {
		t.Fatal("conflicting ID/attempt was accepted")
	}
	e.Diagnostic = logChainParseDiagnosticFacts("status_code=502, future-model requires adaptive thinking (request id: up-a) (request_id: up-b)", "", "cloudwatch:newapi", 100)
	got = run(e)
	if got[0].Diagnosis.CauseKnown || got[1].Diagnosis.CauseKnown {
		t.Fatal("ambiguous IDs picked first")
	}
	e.OneAPIIDHMAC = m.investigationDigest("oneapi-request-id", "other-platform")
	e.Diagnostic = logChainParseDiagnosticFacts("status_code=502, future-model requires adaptive thinking (request id: up-b)", "", "cloudwatch:newapi", 100)
	got = run(e)
	if got[1].Diagnosis.CauseKnown {
		t.Fatal("nearby different platform request supplemented")
	}
}

func TestCustomerDiagnosticReviewExactCodeAndIdentityBounds(t *testing.T) {
	r := LogChainRow{ID: 1, Type: 5, RequestID: "platform-a", Content: "status_code=502, openai_error (request id: provider-a)"}
	r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, `{"attempt_id":"current"}`, "logs", 100)
	r.UpstreamMatch = &LogChainUpstreamMatch{Confidence: correlateExact, JoinKey: "provider-a", UpstreamRequestID: "relay-a", UpstreamContent: "openai_error", UpstreamErrorCode: "upstream_http2_stream_error", UpstreamCreatedAt: 100}
	f := logChainAttributeFaultWithEvidence(r, nil)
	if f.Diagnosis == nil || !strings.Contains(f.Reason, "HTTP/2") || f.Fault != faultUpstream {
		t.Fatalf("exact code lost: %+v", f)
	}
	rows := []LogChainRow{r}
	logChainRefreshFaults(rows)
	if rows[0].DiagnosticFacts.value("upstream_relay_request_id") != "relay-a" {
		t.Fatal("relay identity lost")
	}
	before, _ := json.Marshal(rows)
	logChainRefreshFaults(rows)
	after, _ := json.Marshal(rows)
	if string(before) != string(after) {
		t.Fatal("refresh duplicated evidence or changed namespace")
	}
	for _, variant := range []string{"wrong-key", "probable", "other-attempt"} {
		copyRow, match := r, *r.UpstreamMatch
		switch variant {
		case "wrong-key":
			match.JoinKey = "other"
		case "probable":
			match.Confidence = correlateProbable
		case "other-attempt":
			match.UpstreamContent = `{"attempt_id":"older","message":"openai_error"}`
		}
		copyRow.UpstreamMatch = &match
		if f := logChainAttributeFaultWithEvidence(copyRow, nil); f.Diagnosis != nil && f.Diagnosis.CauseKnown {
			t.Fatalf("unsafe exact evidence %s: %+v", variant, f)
		}
	}
	text := "status_code=502, openai_error "
	for range 33 {
		text += "(request id: id-a) "
	}
	// Duplicate mentions at the same source are not distinct candidates.
	if f := logChainParseDiagnosticFacts(text, "", "logs", 100); f.RequestIDsTruncated || len(f.RequestIDs) != 1 {
		t.Fatal("duplicates treated as conflicting candidates")
	}
	facts := logChainParseDiagnosticFacts("status_code=502, openai_error "+strings.Repeat("x", logChainDiagnosticMaxText)+" (request id: hidden-tail)", "", "logs", 100)
	if !facts.RequestIDsTruncated || facts.hasUpstreamIdentity() {
		t.Fatal("truncated identities declared unique")
	}
	explicit := LogChainRow{Type: 5, RequestID: "same", Content: "status_code=502, openai_error (provider request id: same)"}
	facts = logChainRowDiagnosticFacts(explicit)
	if !facts.identityConflict() || facts.hasUpstreamIdentity() || logChainDiagnosticJoinKey(explicit) != "" {
		t.Fatal("platform/provider collision accepted")
	}
}
