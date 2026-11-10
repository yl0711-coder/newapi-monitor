package monitor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCustomerDiagnosticAcquisitionCapturedRequests(t *testing.T) {
	dir := os.Getenv("CUSTOMER_ACQUISITION_CAPTURE")
	if dir == "" {
		t.Skip("bounded real investigation capture not supplied")
	}
	var bindings map[string]struct {
		Digest string `json:"digest"`
		KeyID  string `json:"key_id"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "customer-bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &bindings); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "customer-evidence-*.json"))
	if err != nil || len(files) != 2 {
		t.Fatal("expected two exact bounded captures", err)
	}
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	proof := []map[string]any{}
	for _, file := range files {
		var result logChainInvestigationResult
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(b, &result); err != nil {
			t.Fatal(err)
		}
		// Replace the runtime secret namespace with the test namespace only
		// for hashes independently verified against the original Request ID.
		// Preserve every equality/conflict across rows, retries and sources.
		for i := range result.Evidence {
			for j := range result.Evidence[i].Evidence {
				e := &result.Evidence[i].Evidence[j]
				for id, binding := range bindings {
					if e.HMACKeyID == binding.KeyID && e.OneAPIIDHMAC == binding.Digest {
						e.OneAPIIDHMAC = m.investigationDigest("oneapi-request-id", id)
						e.HMACKeyID = m.cfg.CloudWatchEvidenceHMACKeyID
						break
					}
				}
			}
		}
		m.rediagnoseInvestigationRequests(&result)
		for _, row := range result.Requests {
			if row.Diagnosis == nil {
				t.Fatal("no acquisition result", row.ID)
			}
			if row.Type == 2 {
				f := row.DiagnosticFacts
				if f.value("request_length") != "889673" || f.value("connect_ms") != "0" || f.value("first_byte_ms") != "6486" || f.value("nginx_first_byte_ms") != "6533" || !row.EdgeEvidenceVerified {
					t.Fatal("exact entry was not used", row.ID, f)
				}
				for _, gap := range row.Diagnosis.Missing {
					if gap.Field == "connect_ms" || gap.Field == "first_byte_ms" || gap.Field == "layer_statuses" {
						t.Fatal("resolved field still missing", gap)
					}
				}
			} else if row.Diagnosis.CauseKnown {
				t.Fatal("generic repeated attempts became a fabricated diagnosis", row.Diagnosis)
			}
			proof = append(proof, map[string]any{"log_id": row.ID, "request_id": row.RequestID, "diagnosis": row.Diagnosis, "facts": row.DiagnosticFacts, "reason": row.FaultReason})
		}
	}
	b, _ = json.MarshalIndent(proof, "", "  ")
	if err = os.WriteFile(filepath.Join(dir, "customer-after-replay.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCustomerDiagnosticAcquisitionEmptyDelivery(t *testing.T) {
	r := LogChainRow{Type: 2, ID: 1, RequestID: "delivery-request", CreatedAt: 100, EndReason: "upstream_timeout", AnomalyTags: []string{"stream"}}
	logChainApplyFault(&r, logChainAttributeFaultWithEvidence(r, r.AnomalyTags))
	if r.Diagnosis == nil || !logChainDiagnosticNeedsSource([]LogChainRow{r}, r.RequestID, cwSourceWorkerNewAPI) || !logChainDiagnosticNeedsSource([]LogChainRow{r}, r.RequestID, cwSourceWorkerNginx) {
		t.Fatal("empty delivery failed to request both hops", r.Diagnosis)
	}
	for _, gap := range r.Diagnosis.Missing {
		if len(gap.Sources) == 0 || gap.Status == "" || gap.AvailableFrom == "" {
			t.Fatal("capture plan absent", gap)
		}
	}
}

func TestCustomerDiagnosticAcquisitionStreamErrorAndRetryBoundary(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	id := "01a123456789abcd123456789abcd1234"
	r := LogChainRow{Type: 2, ID: 1, RequestID: id, CreatedAt: 100, AnomalyTags: []string{"stream"}, DiagnosticFacts: logChainParseDiagnosticFacts("", `{"attempt_id":"a"}`, "logs", 100)}
	logChainApplyFault(&r, logChainAttributeFaultWithEvidence(r, r.AnomalyTags))
	facts := logChainParseDiagnosticAcquisitionFacts("", `{"attempt_id":"a","stream_status":{"end_error":"Channel does not support /v1/responses","end_reason":"scanner_error","errors":["maximum context length is 128000 tokens"]}}`, "cloudwatch:newapi", 100)
	if len(facts.Errors) < 2 {
		t.Fatal("existing stream errors were not read", facts)
	}
	e := cloudWatchStructuredEvidence{Source: cwSourceWorkerNewAPI, Kind: cwEvidenceNewAPIError, EvidenceLevel: "exact", OneAPIIDHMAC: m.investigationDigest("oneapi-request-id", id), HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID, EventMS: 100000, Diagnostic: facts}
	result := logChainInvestigationResult{Requests: []LogChainRow{r}, Evidence: []logChainCloudWatchEvidence{{Source: cwSourceWorkerNewAPI, Evidence: []cloudWatchStructuredEvidence{e}}}, SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceWorkerNewAPI, Status: "found"}}}
	m.rediagnoseInvestigationRequests(&result)
	if result.Requests[0].Diagnosis == nil || !result.Requests[0].Diagnosis.CauseKnown {
		t.Fatal("same attempt did not refine", result.Requests[0].Diagnosis)
	}
	before := len(result.Requests[0].DiagnosticFacts.Errors)
	m.rediagnoseInvestigationRequests(&result)
	if len(result.Requests[0].DiagnosticFacts.Errors) != before {
		t.Fatal("repeated supplement duplicated errors")
	}
	e.Diagnostic = logChainParseDiagnosticFacts("", `{"attempt_id":"b","error":{"message":"Channel does not support /v1/responses"}}`, "cloudwatch:newapi", 100)
	result.Requests = []LogChainRow{r}
	result.Evidence[0].Evidence = []cloudWatchStructuredEvidence{e}
	m.rediagnoseInvestigationRequests(&result)
	if strings.Contains(result.Requests[0].FaultReason, "/v1/responses") {
		t.Fatal("mixed retries")
	}
}

func TestCustomerDiagnosticAcquisitionFailurePreservesKnownCause(t *testing.T) {
	r := LogChainRow{Type: 5, RequestID: "req", Content: "status_code=503, system cpu overloaded (current 99%, threshold 90%)"}
	logChainApplyFault(&r, logChainAttributeFaultWithEvidence(r, nil))
	before := r.FaultReason
	logChainDiagnosticAcquisitionResult(&r, nil, []logChainCloudWatchSourceStatus{{Source: cwSourceWorkerNewAPI, Status: "timeout"}}, "ref")
	if r.FaultReason != before || r.Fault != faultOurs || !r.Diagnosis.CauseKnown {
		t.Fatal("failed supplement erased cause")
	}
	found := false
	for _, gap := range r.Diagnosis.Missing {
		found = found || gap.Status == "source_query_timeout"
	}
	if !found {
		t.Fatal("query outcome not attached")
	}
}

func TestCustomerDiagnosticAcquisitionSharedParserBoundary(t *testing.T) {
	other := `{"stream_status":{"end_reason":"scanner_error","end_error":"array_above_max_length","errors":["upstream_http2_stream_error"]}}`
	shared := logChainParseDiagnosticFacts("", other, "logs", 100)
	admin := logChainParseDiagnosticAcquisitionFacts("", other, "logs", 100)
	if len(shared.Errors) != 0 || len(admin.Errors) != 2 || shared.value("stream_end_reason") != admin.value("stream_end_reason") {
		t.Fatal("shared classification boundary changed", shared, admin)
	}
}

func TestCustomerDiagnosticAcquisitionSnapshotIsolation(t *testing.T) {
	diagnosis := &LogChainDiagnosis{Missing: []LogChainDiagnosticMissing{{Field: "connect_ms", Status: "retained_snapshot", SourceResults: map[cloudWatchLogSourceID]string{cwSourceWorkerNginx: "old"}}}}
	r := LogChainRow{}
	logChainApplyFault(&r, logChainFault{Diagnosis: diagnosis})
	logChainDiagnosticAcquisitionResult(&r, nil, []logChainCloudWatchSourceStatus{{Source: cwSourceWorkerNginx, Status: "found"}, {Source: cwSourceWorkerNewAPI, Status: "timeout"}}, "ref")
	gap := r.Diagnosis.Missing[0]
	if gap.Status != "source_query_timeout" || gap.SourceResults[cwSourceWorkerNginx] != "no_exact_request_evidence" || gap.SourceResults[cwSourceWorkerNewAPI] != "source_query_timeout" {
		t.Fatal("one source hid another source failure", gap)
	}
	if diagnosis.Missing[0].Status != "retained_snapshot" || diagnosis.Missing[0].SourceResults[cwSourceWorkerNginx] != "old" {
		t.Fatal("list snapshot mutated by investigation")
	}
}

func TestCustomerDiagnosticAcquisitionLifecycleIsNotAnotherAttempt(t *testing.T) {
	ref := "exact-request"
	wrap := func(message string) cloudWatchStructuredEvidence {
		return cloudWatchStructuredEvidence{EvidenceLevel: "exact", OneAPIIDHMAC: ref, Diagnostic: &LogChainDiagnosticFacts{Errors: []LogChainDiagnosticError{{Message: message}}}}
	}
	events := []cloudWatchStructuredEvidence{
		wrap(`[ERR] 2026/11/09 - 15:40:13 | 202611090740064265652308268d9d6SrhZqgJ7 | stream ended: reason=client_gone end_error="context canceled", received=3`),
		wrap(`[INFO] 2026/11/09 - 15:40:13 | 202611090740064265652308268d9d6SrhZqgJ7 | record consume log: userId=102, params=`),
		wrap(`context canceled`),
	}
	if logChainEvidenceHasMultipleAttempts(events, ref) {
		t.Fatal("one stream end was classified as multiple attempts")
	}
	events = append(events, wrap(`status_code=500, different upstream error`))
	if !logChainEvidenceHasMultipleAttempts(events, ref) {
		t.Fatal("different response merged into the stream attempt")
	}
}
