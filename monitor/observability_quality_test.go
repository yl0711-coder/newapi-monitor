package monitor

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestObservabilitySourceQualityUnknownIsNotZeroOrHealthy(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))
	ready := readyStatusResponse{
		Status: "ready", ImageVersion: "v118", GitSHA: "abc123",
		Config:     readyEffectiveConfig{SourceWorkerEnabled: true},
		Collectors: map[string]readyCoverageStatus{"source_worker": {}},
	}
	got := buildObservabilitySourceQuality(ready, now)
	if len(got) != 1 {
		t.Fatalf("sources = %d", len(got))
	}
	q := got[0]
	if q.CoverageComplete != nil || q.LagSeconds != nil || q.ParseFailures != nil || q.DroppedEvents != nil || q.CredentialExpiresAt != nil || q.MetadataComplete {
		t.Fatalf("uninstrumented source looks complete: %+v", q)
	}
	if q.ObservedAt == nil || *q.ObservedAt != "2026-10-08T04:00:00Z" {
		t.Fatalf("observed_at must be RFC3339 UTC: %+v", q.ObservedAt)
	}
	if q.CollectorVersion == nil || *q.CollectorVersion != "v118" || q.SourceVersion != nil {
		t.Fatal("Monitor image must not stand in for source node version")
	}
	data, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"from", "through", "target", "coverage_complete", "lag_seconds", "parse_failures", "dropped_events", "owner", "credential_expires_at", "clock_skew_ms"} {
		if !strings.Contains(string(data), `"`+field+`":null`) {
			t.Fatalf("missing explicit null for %s: %s", field, data)
		}
	}
}

func TestObservabilitySourceQualityCoverageBoundariesAndDeterministicOrder(t *testing.T) {
	now := time.Unix(10_000, 0)
	tests := []struct {
		name     string
		coverage readyCoverageStatus
		complete *bool
		lag      *int64
	}{
		{"caught up", readyCoverageStatus{FromTs: 100, ThroughTs: 500, TargetTs: 500}, qualityBool(true), qualityInt(0)},
		{"behind", readyCoverageStatus{FromTs: 100, ThroughTs: 300, TargetTs: 500}, qualityBool(false), qualityInt(200)},
		{"target earlier than through", readyCoverageStatus{FromTs: 100, ThroughTs: 600, TargetTs: 500}, qualityBool(true), qualityInt(0)},
		{"explicit unknown", readyCoverageStatus{FromTs: 100, ThroughTs: 500, TargetTs: 500, CoverageStatus: "unknown"}, nil, nil},
		{"explicit incomplete", readyCoverageStatus{FromTs: 100, ThroughTs: 500, TargetTs: 500, CoverageStatus: "incomplete"}, qualityBool(false), qualityInt(0)},
		{"unknown future status", readyCoverageStatus{FromTs: 100, ThroughTs: 500, TargetTs: 500, CoverageStatus: "new_status"}, nil, nil},
		{"no origin", readyCoverageStatus{ThroughTs: 500, TargetTs: 500}, nil, nil},
		{"no prefix yet", readyCoverageStatus{FromTs: 100, ThroughTs: 100, TargetTs: 500}, qualityBool(false), qualityInt(400)},
		{"reversed interval", readyCoverageStatus{FromTs: 500, ThroughTs: 100, TargetTs: 500}, qualityBool(false), nil},
		{"empty target", readyCoverageStatus{FromTs: 100, ThroughTs: 500, TargetTs: 100}, qualityBool(false), nil},
		{"future coverage", readyCoverageStatus{FromTs: 100, ThroughTs: 20_000, TargetTs: 20_000}, qualityBool(false), nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := buildObservabilitySourceQuality(readyStatusResponse{
				Collectors: map[string]readyCoverageStatus{"z": test.coverage, "a": test.coverage},
			}, now)
			if got[0].SourceID != "a" || got[1].SourceID != "z" {
				t.Fatalf("unstable collector order: %+v", got)
			}
			q := got[0]
			if !reflect.DeepEqual(q.CoverageComplete, test.complete) || !reflect.DeepEqual(q.LagSeconds, test.lag) {
				t.Fatalf("coverage=%v lag=%v want coverage=%v lag=%v", q.CoverageComplete, q.LagSeconds, test.complete, test.lag)
			}
			if q.QueryComplete != nil || q.MetadataComplete {
				t.Fatal("collector coverage cannot prove on-demand query or complete metadata")
			}
			if test.coverage.TargetTs == 500 && (q.TargetAgeSeconds == nil || *q.TargetAgeSeconds != 9500) {
				t.Fatal("a caught-up stale target must retain its age")
			}
		})
	}
}

func TestObservabilitySourceQualityEvidenceRecoveryOverridesOldWatermark(t *testing.T) {
	for _, test := range []struct {
		name       string
		recovery   cloudWatchNginxEvidenceRecoveryStatus
		complete   bool
		throughNil bool
	}{
		{"replaced volume", cloudWatchNginxEvidenceRecoveryStatus{Incomplete: true}, false, true},
		{"verified flag without proof", cloudWatchNginxEvidenceRecoveryStatus{Verified: true}, false, true},
		{"proof lags main store", cloudWatchNginxEvidenceRecoveryStatus{Verified: true, ThroughTs: 400}, false, false},
		{"verified ongoing repair", cloudWatchNginxEvidenceRecoveryStatus{Verified: true, ThroughTs: 500, Incomplete: true}, false, false},
		{"historical gap retained", cloudWatchNginxEvidenceRecoveryStatus{Verified: true, ThroughTs: 500, FromTs: 100, ToTs: 300, DetectedAt: 400}, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := buildObservabilitySourceQuality(readyStatusResponse{
				Config:          readyEffectiveConfig{NginxEvidenceMode: "verified"},
				CloudWatchNginx: cloudWatchNginxReadyStatus{Enabled: true, EvidenceRecovery: test.recovery},
				Collectors:      map[string]readyCoverageStatus{"nginx_evidence": {FromTs: 100, ThroughTs: 500, TargetTs: 500}},
			}, time.Unix(1000, 0))[0]
			if q.CoverageComplete == nil || *q.CoverageComplete != test.complete || (q.Through == nil) != test.throughNil {
				t.Fatalf("bad evidence proof projection: %+v", q)
			}
			if test.recovery.DetectedAt > 0 && (q.RecoveryDetectedAt == nil || q.RecoveryFrom == nil || q.RecoveryTo == nil) {
				t.Fatal("repair must not hide historical recovery gap")
			}
		})
	}
}

func TestObservabilitySourceQualityRuntimeMeaningsStaySeparate(t *testing.T) {
	ready := readyStatusResponse{
		Config:         readyEffectiveConfig{SourceWorkerEnabled: true, StabilityEnabled: true},
		Source:         sourceReadyStatus{WorkerRunning: true, LastSuccessAt: 999, LastFailureAt: 888},
		SampledAt:      700,
		MetricFinalize: metricFinalizeReadyStatus{LastSuccessAt: 800, LastFailureAt: 850},
		Collectors: map[string]readyCoverageStatus{
			"source_worker": {}, "frt_replay": {}, "metric_finalize": {}, "problem_source": {},
		},
	}
	got := buildObservabilitySourceQuality(ready, time.Unix(1000, 0))
	for _, q := range got {
		switch q.SourceID {
		case "source_worker":
			if !reflect.DeepEqual(q.LastSuccessAt, qualityTime(700)) || q.LastFailureAt != nil {
				t.Fatal("source probe timestamps must not be relabeled as sample attempts")
			}
		case "frt_replay":
			if q.LastSuccessAt != nil || q.LastFailureAt != nil {
				t.Fatal("request finalizer success must not become FRT replay success")
			}
		case "metric_finalize":
			if !slices.Contains(q.ReasonCodes, "collector_last_attempt_failed") {
				t.Fatal("failure newer than success must remain visible")
			}
		case "problem_source":
			if q.Enabled == nil || !*q.Enabled || q.Running == nil || !*q.Running {
				t.Fatal("standard worker collects problem facts without independent source switch")
			}
		}
	}
}

func TestInvestigationSourceQualitySuccessfulQueryIsNotContinuousCoverage(t *testing.T) {
	for _, status := range []string{"found", "empty"} {
		t.Run(status, func(t *testing.T) {
			got := buildInvestigationSourceQuality([]logChainCloudWatchSourceStatus{{
				Source: cwSourceCloudFrontAccess, Status: status,
				Note: "RAW-SENSITIVE-NOTE", LogGroup: "NOT-PROJECTED",
			}}, 100, 200, time.Unix(300, 0))
			q := got[0]
			if q.QueryComplete == nil || !*q.QueryComplete || q.ParseFailures == nil || *q.ParseFailures != 0 {
				t.Fatalf("complete query must expose known zero failures: %+v", q)
			}
			if q.CoverageComplete != nil || q.Through != nil || q.Target != nil || q.LagSeconds != nil || q.DroppedEvents != nil || q.MetadataComplete {
				t.Fatal("query completion cannot stand in for collector or source health")
			}
			data, err := json.Marshal(q)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "RAW-SENSITIVE") || strings.Contains(string(data), "NOT-PROJECTED") {
				t.Fatal("quality projection must not copy arbitrary raw notes or log group values")
			}
		})
	}
}

func TestInvestigationSourceQualityIncompleteQueries(t *testing.T) {
	tests := []struct {
		name           string
		source         logChainCloudWatchSourceStatus
		countersKnown  bool
		expectedReason string
	}{
		{"partial", logChainCloudWatchSourceStatus{Status: "found", Partial: true}, true, "query_partial"},
		{"truncated", logChainCloudWatchSourceStatus{Status: "found", Truncated: true}, true, "query_truncated"},
		{"parse failures", logChainCloudWatchSourceStatus{Status: "found", Parsed: 2, ParseFailed: 3}, true, "query_parse_failed"},
		{"all malformed", logChainCloudWatchSourceStatus{Status: "unavailable", ParseFailed: 3}, true, "query_parse_failed"},
		{"access denied", logChainCloudWatchSourceStatus{Status: "access_denied"}, false, "query_incomplete"},
		{"disabled", logChainCloudWatchSourceStatus{Status: "disabled"}, false, "source_disabled"},
		{"not queried", logChainCloudWatchSourceStatus{Status: "skipped"}, false, "query_incomplete"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := buildInvestigationSourceQuality([]logChainCloudWatchSourceStatus{test.source}, 100, 200, time.Unix(300, 0))[0]
			if q.QueryComplete == nil || *q.QueryComplete || q.CoverageComplete != nil {
				t.Fatalf("incomplete query promoted to complete: %+v", q)
			}
			if (q.ParseFailures != nil) != test.countersKnown || (q.ParsedEvents != nil) != test.countersKnown {
				t.Fatal("unknown query counters must remain null")
			}
			if !slices.Contains(q.ReasonCodes, test.expectedReason) {
				t.Fatalf("missing reason %q: %v", test.expectedReason, q.ReasonCodes)
			}
		})
	}
}

func TestInvestigationSourceQualityRejectsInvalidWindow(t *testing.T) {
	q := buildInvestigationSourceQuality([]logChainCloudWatchSourceStatus{{Status: "empty"}}, 200, 100, time.Unix(300, 0))[0]
	if q.QueryComplete == nil || *q.QueryComplete || !slices.Contains(q.ReasonCodes, "query_window_invalid") {
		t.Fatal("invalid requested interval marked as complete")
	}
}

func qualityInt(value int64) *int64 { return &value }
