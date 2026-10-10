package monitor

import (
	"context"
	"testing"
	"time"
)

func TestCloudFrontZeroAndMissingDurationsCannotBypassAssociation(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, tc := range []struct {
		name          string
		businessKnown bool
		businessSec   int64
		edgeMS        *int64
		couldMatch    bool
		want          string
	}{
		{name: "real_zero_conflicts_with_90_seconds", businessKnown: true, edgeMS: durationTestMS(90000), want: "ambiguous"},
		{name: "real_zero_matches_zero", businessKnown: true, edgeMS: durationTestMS(0), couldMatch: true, want: "correlated"},
		{name: "real_zero_at_tolerance", businessKnown: true, edgeMS: durationTestMS(2000), couldMatch: true, want: "correlated"},
		{name: "real_zero_beyond_tolerance", businessKnown: true, edgeMS: durationTestMS(2001), want: "ambiguous"},
		{name: "missing_business_duration", edgeMS: durationTestMS(90000), couldMatch: true, want: "ambiguous"},
		{name: "missing_edge_duration", businessKnown: true, businessSec: 1, couldMatch: true, want: "ambiguous"},
		{name: "both_missing", couldMatch: true, want: "ambiguous"},
		{name: "invalid_negative_business_duration", businessKnown: true, businessSec: -1, edgeMS: durationTestMS(1000), couldMatch: true, want: "ambiguous"},
		{name: "invalid_negative_edge_duration", businessKnown: true, businessSec: 1, edgeMS: durationTestMS(-1), couldMatch: true, want: "ambiguous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := LogChainRow{RequestID: "request-a", CreatedAt: 100, RequestPath: "/v1/responses", UseTime: tc.businessSec, UseTimeKnown: tc.businessKnown}
			edge := linkageEdge(m, "edge-a", 200)
			edge.RequestMS = tc.edgeMS
			if got := cloudFrontCouldMatchBusiness(edge, row); got != tc.couldMatch {
				t.Fatalf("candidate=%v want=%v", got, tc.couldMatch)
			}
			if got := m.cloudFrontEvidenceLevels([]LogChainRow{row}, []cloudWatchStructuredEvidence{edge}, logChainInvestigationInput{}, true)["edge-a"]; got != tc.want {
				t.Fatalf("association=%s want=%s", got, tc.want)
			}
		})
	}
}

func durationTestMS(value int64) *int64 { return &value }

func TestCloudFrontMissingDurationsRemainCompetingCandidatesBothWays(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, side := range []string{"business", "edge"} {
		t.Run(side, func(t *testing.T) {
			rows := []LogChainRow{{RequestID: "request-a", CreatedAt: 100, RequestPath: "/v1/responses", UseTime: 1, UseTimeKnown: true}}
			edges := []cloudWatchStructuredEvidence{linkageEdge(m, "edge-a", 200)}
			if side == "business" {
				rows = append(rows, LogChainRow{RequestID: "request-b", CreatedAt: 100, RequestPath: "/v1/responses"})
			} else {
				other := linkageEdge(m, "edge-b", 200)
				other.RequestMS = nil
				edges = append(edges, other)
			}
			in := logChainInvestigationInput{NewAPIRequestID: "request-a", CloudFrontRequestID: "edge-a"}
			if got := m.cloudFrontEvidenceLevels(rows, edges, in, true)["edge-a"]; got != "ambiguous" {
				t.Fatalf("missing %s duration manufactured unique association: %s", side, got)
			}
		})
	}
}

func TestLogChainQueryPreservesZeroVersusNullDuration(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 100, Type: 2, RequestID: "real-zero", UseTime: 0},
		{ID: 2, CreatedAt: 100, Type: 2, RequestID: "missing-duration", UseTime: 0},
	})
	if _, err := m.prodDB.Exec("UPDATE logs SET use_time = NULL WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	rows, more, err := m.queryLogChain(context.Background(), logChainScope{FromTs: 80, ToTs: 120, Asc: true, Limit: 10}, nil)
	if err != nil || more || len(rows) != 2 {
		t.Fatalf("rows=%+v more=%v err=%v", rows, more, err)
	}
	if rows[0].UseTime != 0 || !rows[0].UseTimeKnown || rows[1].UseTime != 0 || rows[1].UseTimeKnown {
		t.Fatalf("SQL zero and NULL collapsed into the same duration: %+v", rows)
	}
}

func TestInvestigationReverseLookupRetainsSQLNullDurationCompetitor(t *testing.T) {
	m := newLogChainExecMonitor(t, []logChainSeedRow{
		{ID: 1, CreatedAt: 100, Type: 2, RequestID: "request-a", UseTime: 1, Other: `{"request_path":"/v1/responses"}`},
		{ID: 2, CreatedAt: 100, Type: 2, RequestID: "request-b", UseTime: 0, Other: `{"request_path":"/v1/responses"}`},
	})
	if _, err := m.prodDB.Exec("UPDATE logs SET use_time = NULL WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = logChainCloudWatchTestKey, "fixture-v1"
	edge := linkageEdge(m, "edge-a", 200)
	in := logChainInvestigationInput{NewAPIRequestID: "request-a", From: time.Unix(80, 0), To: time.Unix(120, 0)}
	result := logChainInvestigationResult{
		CandidatesComplete: true, associationEdgesComplete: true,
		Requests:     []LogChainRow{{RequestID: "request-a", CreatedAt: 100, RequestPath: edge.Route, UseTime: 1, UseTimeKnown: true}},
		SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceCloudFrontAccess, Status: "found"}},
		Evidence:     []logChainCloudWatchEvidence{{Source: cwSourceCloudFrontAccess, Evidence: []cloudWatchStructuredEvidence{edge}}},
	}
	m.loadInvestigationAssociationCandidates(context.Background(), &result, in)
	m.associateInvestigationEvidence(&result, in)
	if !result.associationCandidatesComplete || len(result.associationCandidates) != 2 {
		t.Fatalf("NULL competitor dropped from reverse lookup: %+v", result.associationCandidates)
	}
	if got := result.Evidence[0].Evidence[0].EvidenceLevel; got != "ambiguous" {
		t.Fatalf("SQL NULL competitor falsely discarded: %s", got)
	}
}
