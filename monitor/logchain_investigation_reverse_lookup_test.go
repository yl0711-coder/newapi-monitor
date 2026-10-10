package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

func TestInvestigationReverseLookupDoesNotHideOtherCustomers(t *testing.T) {
	for _, name := range []string{"unique", "other_customer", "missing_competitor_path", "truncated", "cancelled", "deadline", "query_error", "clipped_edge_window", "oversized_window"} {
		t.Run(name, func(t *testing.T) {
			seed := []logChainSeedRow{{ID: 1, CreatedAt: 100, Type: 2, UserID: 1, ModelName: "selected-model", Group: "selected-group", RequestID: "request-a", UseTime: 1, Other: `{"request_path":"/v1/responses"}`}}
			if name == "other_customer" || name == "missing_competitor_path" || name == "truncated" {
				count := 1
				if name == "truncated" {
					count = logChainInvestigationCandidateLimit
				}
				for i := 0; i < count; i++ {
					other := seed[0]
					other.ID, other.UserID = int64(i+2), 2
					other.ModelName, other.Group, other.RequestID = "private-other-model", "private-other-group", fmt.Sprintf("private-other-request-%d", i)
					if name == "missing_competitor_path" {
						other.Other = `{}`
					}
					seed = append(seed, other)
				}
			}
			m := newLogChainExecMonitor(t, seed)
			if name == "query_error" {
				if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
					t.Fatal(err)
				}
			}
			m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = logChainCloudWatchTestKey, "fixture-v1"
			in := logChainInvestigationInput{From: time.Unix(80, 0), To: time.Unix(260, 0), NewAPIRequestID: "request-a", UserID: 1, Model: "selected-model", Group: "selected-group"}
			if name == "clipped_edge_window" {
				in.From = time.Unix(98, 0)
			}
			edge := linkageEdge(m, "edge-a", 0)
			result := logChainInvestigationResult{CandidatesComplete: true, associationEdgesComplete: true,
				Requests:     []LogChainRow{{RequestID: "request-a", CreatedAt: 100, RequestPath: edge.Route, UseTime: 1, UseTimeKnown: true, UserID: 1, ModelName: in.Model, Group: in.Group}},
				SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceCloudFrontAccess, Status: "found"}},
				Evidence:     []logChainCloudWatchEvidence{{Source: cwSourceCloudFrontAccess, Evidence: []cloudWatchStructuredEvidence{edge}}}}
			if name == "oversized_window" {
				other := result.Requests[0]
				other.CreatedAt = 240
				result.Requests = append(result.Requests, other)
				otherEdge := linkageEdge(m, "edge-b", 0)
				otherEdge.EventMS = 240000
				result.Evidence[0].Evidence = append(result.Evidence[0].Evidence, otherEdge)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if name == "cancelled" {
				cancel()
			}
			if name == "deadline" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			defer cancel()
			m.loadInvestigationAssociationCandidates(ctx, &result, in)
			m.associateInvestigationEvidence(&result, in)
			want := "ambiguous"
			if name == "unique" {
				want = "correlated"
			}
			if got := result.Evidence[0].Evidence[0].EvidenceLevel; got != want {
				t.Fatalf("association=%s want=%s; reverse candidates=%+v", got, want, result.associationCandidates)
			}
			if name == "other_customer" || name == "missing_competitor_path" {
				if !result.associationCandidatesComplete || len(result.associationCandidates) != 2 {
					t.Fatalf("reverse lookup inherited target filters: %+v", result.associationCandidates)
				}
			}
			if name == "truncated" || name == "cancelled" || name == "deadline" || name == "query_error" || name == "oversized_window" || name == "clipped_edge_window" {
				if !result.associationCandidatesRequired || result.associationCandidatesComplete || len(result.BlindSpots) == 0 {
					t.Fatalf("incomplete reverse query had no visible gap: %+v", result)
				}
				if got := investigationCompletionStatus(context.Background(), result); got != "partial" {
					t.Fatalf("reverse lookup gap advertised complete result: %s", got)
				}
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "private-other-") {
				t.Fatal("private reverse-lookup rows leaked into the API")
			}
		})
	}
}

func TestInvestigationCandidateGraphDoesNotPrefilterSelectedIDs(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	row := LogChainRow{RequestID: "request-a", CreatedAt: 100, RequestPath: "/v1/responses", UseTime: 1, UseTimeKnown: true}
	for _, name := range []string{"other_business_id", "other_edge_id", "other_edge_status", "only_wrong_business_id", "conflicting_competitor", "missing_edge_id", "wrong_edge_key"} {
		t.Run(name, func(t *testing.T) {
			rows := []LogChainRow{row}
			edges := []cloudWatchStructuredEvidence{linkageEdge(m, "edge-a", 200)}
			in := logChainInvestigationInput{NewAPIRequestID: "request-a", CloudFrontRequestID: "edge-a", Status: 200}
			switch name {
			case "other_business_id":
				other := row
				other.RequestID = "request-b"
				rows = append(rows, other)
			case "only_wrong_business_id":
				rows[0].RequestID = "request-b"
			case "other_edge_id":
				edges = append(edges, linkageEdge(m, "edge-b", 200))
			case "other_edge_status":
				edges = append(edges, linkageEdge(m, "edge-b", 0))
			case "conflicting_competitor":
				other := linkageEdge(m, "edge-b", 200)
				conflict := linkageEdge(m, "edge-b", 0)
				conflict.EventRef = "edge-b-conflict"
				edges = append(edges, other, conflict)
			case "missing_edge_id":
				other := linkageEdge(m, "edge-b", 200)
				other.CloudFrontIDHMAC = ""
				edges = append(edges, other)
			case "wrong_edge_key":
				other := linkageEdge(m, "edge-b", 200)
				other.HMACKeyID = "unknown-key"
				edges = append(edges, other)
			}
			if level := m.cloudFrontEvidenceLevels(rows, edges, in, true)["edge-a"]; level != "ambiguous" {
				t.Fatalf("prefiltered IDs created false uniqueness: %s", level)
			}
		})
	}
}

func TestInvestigationCloudFrontOnlyCannotPromoteUnrelatedBusiness(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	edge := linkageEdge(m, "edge-a", 0)
	status := 499
	worker := cloudWatchStructuredEvidence{Source: cwSourceWorkerNginx, Kind: cwEvidenceNginxAccess, EventRef: "nginx-unrelated", Status: &status,
		HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID, OneAPIIDHMAC: m.investigationDigest("oneapi-request-id", "unrelated")}
	result := logChainInvestigationResult{CandidatesComplete: true,
		Requests:     []LogChainRow{{RequestID: "unrelated", CreatedAt: 100, RequestPath: edge.Route, Fault: "upstream"}},
		SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceCloudFrontAccess, Status: "found"}, {Source: cwSourceWorkerNginx, Status: "found"}},
		Evidence:     []logChainCloudWatchEvidence{{Source: cwSourceCloudFrontAccess, Evidence: []cloudWatchStructuredEvidence{edge}}, {Source: cwSourceWorkerNginx, Evidence: []cloudWatchStructuredEvidence{worker}}}}
	in := logChainInvestigationInput{CloudFrontRequestID: "edge-a"}
	m.associateInvestigationEvidence(&result, in)
	if result.Evidence[0].Evidence[0].EvidenceLevel != "exact" || result.Evidence[1].Evidence[0].EvidenceLevel != "ambiguous" {
		t.Fatalf("CF identity was confused with business identity: %+v", result.Evidence)
	}
	for _, event := range m.investigationTimeline(result.Requests, result.Evidence, result.SourceStatus, in) {
		if event.Source != cwSourceCloudFrontAccess && event.EvidenceLevel != "ambiguous" {
			t.Fatalf("timeline falsely matched business to CF ID: %+v", event)
		}
	}
	if summary := summarizeInvestigation(result.Requests, flattenInvestigationEvidence(result.Evidence), in); summary.Classification != "client_disconnect_at_edge" || summary.EvidenceLevel != "exact" {
		t.Fatalf("unrelated business overrode the selected edge event: %+v", summary)
	}
	if got := investigationCompletionStatus(context.Background(), result); got != "complete" {
		t.Fatalf("CF-only task without a required reverse lookup was degraded: %s", got)
	}
}

func TestInvestigationAssociationRejectsKnownWorkerKeyConflict(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, field := range []string{"method", "route", "host"} {
		t.Run(field, func(t *testing.T) {
			edge := linkageEdge(m, "edge-a", 0)
			worker := edge
			worker.Source, worker.Kind, worker.EventRef = cwSourceWorkerNginx, cwEvidenceNginxAccess, "worker"
			worker.OneAPIIDHMAC = m.investigationDigest("oneapi-request-id", "request-a")
			switch field {
			case "method":
				worker.Method = "GET"
			case "route":
				worker.Route = "/v1/chat/completions"
			case "host":
				worker.Host = "other.example"
			}
			row := LogChainRow{RequestID: "request-a", CreatedAt: 100, RequestPath: edge.Route, UseTime: 1, UseTimeKnown: true}
			result := logChainInvestigationResult{CandidatesComplete: true, Requests: []LogChainRow{row},
				associationCandidates: []LogChainRow{row}, associationCandidatesComplete: true, associationEdgesComplete: true,
				SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceCloudFrontAccess, Status: "found"}, {Source: cwSourceWorkerNginx, Status: "found"}},
				Evidence:     []logChainCloudWatchEvidence{{Source: cwSourceCloudFrontAccess, Evidence: []cloudWatchStructuredEvidence{edge}}, {Source: cwSourceWorkerNginx, Evidence: []cloudWatchStructuredEvidence{worker}}}}
			m.associateInvestigationEvidence(&result, logChainInvestigationInput{NewAPIRequestID: "request-a"})
			if result.Evidence[0].Evidence[0].EvidenceLevel != "ambiguous" {
				t.Fatalf("known %s conflict was ignored: %+v", field, result.Evidence)
			}
		})
	}
}

func TestInvestigationExplicitIDsStillQueryOtherEdgeCandidates(t *testing.T) {
	at := time.Now().UTC().Add(-2 * time.Hour)
	message := fmt.Sprintf(`{"timestamp(ms)":%d,"x-edge-request-id":"edge-a","cs-method":"POST","x-host-header":"test.example","cs-uri-stem":"/v1/responses","sc-status":"200","time-taken":"1.000"}`, at.UnixMilli())
	client := &fakeCloudWatchLogsClient{filterFn: func(context.Context, *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{EventId: aws.String("edge-a-event"), Timestamp: aws.Int64(at.UnixMilli()), Message: aws.String(message)}}}, nil
	}}
	m := newLogChainCloudWatchTestMonitor(t, client)
	parser, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		t.Fatal(err)
	}
	runner := &logChainInvestigationRunner{m: m, parser: parser, statuses: make(map[cloudWatchLogSourceID]logChainCloudWatchSourceStatus), evidence: make(map[cloudWatchLogSourceID][]cloudWatchStructuredEvidence)}
	runner.queryCloudFront(context.Background(), logChainInvestigationInput{NewAPIRequestID: "request-a", CloudFrontRequestID: "edge-a", From: at.Add(-time.Minute), To: at.Add(time.Minute)}, nil)
	client.mu.Lock()
	queries := append([]*cloudwatchlogs.StartQueryInput(nil), client.startInputs...)
	client.mu.Unlock()
	if !runner.cloudFrontCandidateSetQueried || len(queries) != 1 || !strings.Contains(aws.ToString(queries[0].QueryString), "/v1/responses") {
		t.Fatalf("two selected IDs skipped the independent edge universe: %+v", queries)
	}
}

func TestInvestigationShowsEvidenceVolumeRecoveryGap(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	m.cfg.CloudWatchNginxEnabled, m.cfg.NginxEvidenceMode = true, "verified"
	result := m.executeLogChainInvestigation(context.Background(), "recovery-gap", logChainInvestigationInput{NewAPIRequestID: "request-a", From: time.Now().Add(-3 * time.Hour), To: time.Now().Add(-2 * time.Hour)})
	if !strings.Contains(strings.Join(result.BlindSpots, "\n"), "本地 Nginx 请求证据存在缺口") {
		t.Fatal("investigation hid the replaced evidence-store coverage gap")
	}
}

func TestInvestigationSourceWithoutEvidenceCannotRetainExactAssociation(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, state := range []string{"empty", "unavailable", "found", "skipped"} {
		t.Run(state, func(t *testing.T) {
			result := logChainInvestigationResult{CandidatesComplete: true,
				SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceWorkerNginx, Status: state, Linkage: "exact"}}}
			m.associateInvestigationEvidence(&result, logChainInvestigationInput{NewAPIRequestID: "request-a"})
			want := "ambiguous"
			if state == "skipped" {
				want = "skipped"
			}
			if result.SourceStatus[0].Linkage != want {
				t.Fatalf("selector masqueraded as evidence: %+v", result.SourceStatus[0])
			}
		})
	}
}

func TestInvestigationCompletionRequiresCompleteBusinessCandidates(t *testing.T) {
	for _, tc := range []struct {
		name      string
		complete  bool
		truncated bool
		want      string
	}{
		{name: "business_query_failed", want: "partial"},
		{name: "business_query_truncated", complete: true, truncated: true, want: "partial"},
		{name: "business_query_succeeded_without_rows", complete: true, want: "complete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := logChainInvestigationResult{CandidatesComplete: tc.complete, CandidateTruncated: tc.truncated,
				Requests: []LogChainRow{}, SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceWorkerNginx, Status: "empty"}}}
			if got := investigationCompletionStatus(context.Background(), result); got != tc.want {
				t.Fatalf("status=%s want=%s", got, tc.want)
			}
		})
	}
}
