package monitor

import (
	"context"
	"testing"
)

func linkageEdge(m *Monitor, id string, status int) cloudWatchStructuredEvidence {
	ms := int64(1000)
	return cloudWatchStructuredEvidence{Source: cwSourceCloudFrontAccess, Kind: cwEvidenceCloudFrontAccess,
		EventRef: id, CloudFrontIDHMAC: m.investigationDigest("cloudfront-request-id", id), HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID,
		EventMS: 100000, Route: "/v1/responses", Method: "POST", Host: "test.example", Status: &status, RequestMS: &ms}
}

func TestInvestigationExactBusinessFailureBeatsUnrelatedDisconnects(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, withID := range []bool{false, true} {
		nginxStatus := 499
		nginx := cloudWatchStructuredEvidence{Source: cwSourceWorkerNginx, Kind: cwEvidenceNginxAccess, EventRef: "nginx-other", Status: &nginxStatus, HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID}
		if withID {
			nginx.OneAPIIDHMAC = m.investigationDigest("oneapi-request-id", "other-request")
		}
		edge := linkageEdge(m, "other-edge", 0)
		edge.Route = "/v1/chat/completions"
		result := logChainInvestigationResult{CandidatesComplete: true,
			Requests: []LogChainRow{{ID: 1, RequestID: "this-request", CreatedAt: 100, RequestPath: "/v1/responses", UseTime: 1, UseTimeKnown: true, Fault: "upstream"}},
			SourceStatus: []logChainCloudWatchSourceStatus{
				{Source: cwSourceCloudFrontAccess, Status: "found", Linkage: "exact"},
				{Source: cwSourceWorkerNginx, Status: "found", Linkage: "exact"}},
			Evidence: []logChainCloudWatchEvidence{{Source: cwSourceCloudFrontAccess, Evidence: []cloudWatchStructuredEvidence{edge}},
				{Source: cwSourceWorkerNginx, Evidence: []cloudWatchStructuredEvidence{nginx}}}}
		in := logChainInvestigationInput{NewAPIRequestID: "this-request"}
		m.associateInvestigationEvidence(&result, in)
		summary := summarizeInvestigation(result.Requests, flattenInvestigationEvidence(result.Evidence), in)
		if summary.Classification != "upstream_error" || summary.EvidenceLevel != "exact" {
			t.Fatalf("unrelated disconnect replaced the business evidence: %+v", summary)
		}
		for _, s := range result.SourceStatus {
			if s.Linkage != "ambiguous" {
				t.Fatalf("source falsely certified unrelated events: %+v", s)
			}
		}
		for _, e := range m.investigationTimeline(result.Requests, result.Evidence, result.SourceStatus, in) {
			if e.Source != "newapi_database" && e.EvidenceLevel != "ambiguous" {
				t.Fatalf("timeline upgraded unrelated event: %+v", e)
			}
		}
		// Without the business row, the no-ID or mismatched-ID 499 cannot turn
		// into an exact request failure either.
		summary = summarizeInvestigation(nil, flattenInvestigationEvidence(result.Evidence), in)
		if summary.Classification != "inconclusive" {
			t.Fatalf("candidate was used as a verdict: %+v", summary)
		}
	}
}

func TestCloudFrontAssociationRequiresBothSidesUniqueAndNoConflict(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	baseRow := LogChainRow{ID: 1, RequestID: "request-a", CreatedAt: 100, RequestPath: "/v1/responses", UseTime: 1, UseTimeKnown: true}
	for _, name := range []string{"unique", "two_business_requests", "two_edge_requests", "same_request_retry", "path_conflict", "time_conflict", "duration_conflict", "explicit_status_conflict", "missing_path", "no_business_anchor", "incomplete", "conflicting_duplicate"} {
		t.Run(name, func(t *testing.T) {
			rows := []LogChainRow{baseRow}
			evidence := []cloudWatchStructuredEvidence{linkageEdge(m, "edge-a", 200)}
			in, complete, want := logChainInvestigationInput{}, true, "ambiguous"
			switch name {
			case "unique":
				want = "correlated"
			case "two_business_requests":
				other := baseRow
				other.ID = 2
				other.RequestID = "request-b"
				rows = append(rows, other)
			case "two_edge_requests":
				evidence = append(evidence, linkageEdge(m, "edge-b", 200))
			case "same_request_retry":
				other := baseRow
				other.ID = 2
				rows = append(rows, other)
				want = "correlated"
			case "path_conflict":
				evidence[0].Route = "/v1/chat/completions"
			case "time_conflict":
				evidence[0].EventMS += 6000
			case "duration_conflict":
				n := int64(90000)
				evidence[0].RequestMS = &n
			case "explicit_status_conflict":
				in.Status = 500
			case "missing_path":
				rows[0].RequestPath = ""
			case "no_business_anchor":
				rows = nil
			case "incomplete":
				complete = false
			case "conflicting_duplicate":
				other := evidence[0]
				other.EventRef = "duplicate"
				other.Method = "GET"
				evidence = append(evidence, other)
			}
			if got := m.cloudFrontEvidenceLevels(rows, evidence, in, complete)["edge-a"]; got != want {
				t.Fatalf("level=%s want=%s", got, want)
			}
		})
	}
}

func TestCloudFrontAllViewsShareFailClosedAssociation(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, name := range []string{"complete", "partial", "truncated", "parse_failure", "business_incomplete", "business_truncated"} {
		t.Run(name, func(t *testing.T) {
			edge := linkageEdge(m, "edge-a", 0)
			diagnostic := edge
			diagnostic.Source = cwSourceCloudFrontDiagnostic
			diagnostic.Kind = cwEvidenceCloudFrontDiagnostic
			diagnostic.EventRef = "diagnostic"
			result := logChainInvestigationResult{CandidatesComplete: true,
				Requests:     []LogChainRow{{RequestID: "request-a", CreatedAt: 100, RequestPath: edge.Route, UseTime: 1, UseTimeKnown: true}},
				SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceCloudFrontAccess, Status: "found", Linkage: "exact"}, {Source: cwSourceCloudFrontDiagnostic, Status: "found", Linkage: "exact"}},
				Evidence:     []logChainCloudWatchEvidence{{Source: cwSourceCloudFrontAccess, Evidence: []cloudWatchStructuredEvidence{edge}}, {Source: cwSourceCloudFrontDiagnostic, Evidence: []cloudWatchStructuredEvidence{diagnostic}}}}
			result.associationCandidates = append([]LogChainRow(nil), result.Requests...)
			result.associationCandidatesComplete, result.associationEdgesComplete = true, true
			want := "ambiguous"
			switch name {
			case "complete":
				want = "correlated"
			case "partial":
				result.SourceStatus[0].Partial = true
			case "truncated":
				result.SourceStatus[0].Truncated = true
			case "parse_failure":
				result.SourceStatus[0].ParseFailed = 1
			case "business_incomplete":
				result.CandidatesComplete = false
			case "business_truncated":
				result.CandidateTruncated = true
			}
			in := logChainInvestigationInput{NewAPIRequestID: "request-a", CloudFrontRequestID: "edge-a"}
			m.associateInvestigationEvidence(&result, in)
			for _, s := range result.SourceStatus {
				if s.Linkage != want {
					t.Fatalf("card=%+v want=%s", s, want)
				}
			}
			for _, e := range m.investigationTimeline(result.Requests, result.Evidence, result.SourceStatus, in) {
				if e.Source != "newapi_database" && e.EvidenceLevel != want {
					t.Fatalf("timeline=%+v want=%s", e, want)
				}
			}
			summary := summarizeInvestigation(result.Requests, flattenInvestigationEvidence(result.Evidence), in)
			if summary.EvidenceLevel != want {
				t.Fatalf("summary=%+v want=%s", summary, want)
			}
			if want == "ambiguous" && summary.Classification != "inconclusive" {
				t.Fatalf("partial candidate became verdict: %+v", summary)
			}
		})
	}
}

func TestInvestigationExactIDsAreVerifiedPerEvent(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, name := range []string{"edge_id", "different_edge", "both_ids", "old_key", "partial"} {
		t.Run(name, func(t *testing.T) {
			e := linkageEdge(m, "edge-a", 0)
			in := logChainInvestigationInput{CloudFrontRequestID: "edge-a"}
			complete, want := true, "ambiguous"
			switch name {
			case "edge_id":
				want = "exact"
			case "different_edge":
				in.CloudFrontRequestID = "edge-b"
			case "both_ids":
				in.NewAPIRequestID = "request-a"
			case "old_key":
				e.HMACKeyID = "old-key"
			case "partial":
				complete = false
			}
			if got := m.cloudFrontEvidenceLevels(nil, []cloudWatchStructuredEvidence{e}, in, complete)[e.EventRef]; got != want {
				t.Fatalf("got=%s want=%s", got, want)
			}
		})
	}
	status := 499
	e := cloudWatchStructuredEvidence{Source: cwSourceWorkerNginx, Kind: cwEvidenceNginxAccess, EventRef: "nginx", Status: &status,
		HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID, OneAPIIDHMAC: m.investigationDigest("oneapi-request-id", "request-a")}
	r := logChainInvestigationResult{Evidence: []logChainCloudWatchEvidence{{Source: e.Source, Evidence: []cloudWatchStructuredEvidence{e}}},
		SourceStatus: []logChainCloudWatchSourceStatus{{Source: e.Source, Status: "found"}}}
	in := logChainInvestigationInput{NewAPIRequestID: "request-a"}
	m.associateInvestigationEvidence(&r, in)
	if s := summarizeInvestigation(nil, flattenInvestigationEvidence(r.Evidence), in); s.Classification != "client_or_network_disconnect" || s.EvidenceLevel != "exact" {
		t.Fatalf("verified ID lost exact relation: %+v", s)
	}
}

func TestInvestigationTruncatedOrUnparsedSourceIsPartial(t *testing.T) {
	for _, s := range []logChainCloudWatchSourceStatus{{Status: "found", Truncated: true}, {Status: "found", ParseFailed: 1}} {
		if got := investigationCompletionStatus(context.Background(), logChainInvestigationResult{CandidatesComplete: true, SourceStatus: []logChainCloudWatchSourceStatus{s}}); got != "partial" {
			t.Fatalf("status=%s", got)
		}
	}
}
