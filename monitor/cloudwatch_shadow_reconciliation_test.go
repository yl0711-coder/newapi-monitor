package monitor

import (
	"context"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func shadowInt64(value int64) *int64 { return &value }

func shadowStatus(value int) *int { return &value }

func TestCompareShadowNginxAggregatesAcrossLegacyNodes(t *testing.T) {
	from, to := int64(1200), int64(1260)
	old := []NginxMinuteSample{
		{BucketTs: 1200, Node: "worker-a", Route: "/v1/responses", Method: "POST", Status: 200, UpstreamStatus: 200, Count: 2, RequestTimeSumMS: 300, RequestTimeMaxMS: 200, UpstreamTimeSumMS: 250, RequestIDPresent: 2},
		{BucketTs: 1200, Node: "worker-b", Route: "/v1/responses", Method: "POST", Status: 200, UpstreamStatus: 200, Count: 1, RequestTimeSumMS: 100, RequestTimeMaxMS: 100, UpstreamTimeSumMS: 80, RequestIDPresent: 1},
	}
	newEvidence := []cloudWatchStructuredEvidence{
		{Kind: cwEvidenceNginxAccess, EventMS: 1200100, Route: "/v1/responses?redacted", Method: "POST", Status: shadowStatus(200), UpstreamStatuses: []int{200}, RequestMS: shadowInt64(100), UpstreamMS: shadowInt64(90), OneAPIIDHMAC: "id-1"},
		{Kind: cwEvidenceNginxAccess, EventMS: 1200200, Route: "/v1/responses", Method: "POST", Status: shadowStatus(200), UpstreamStatuses: []int{200}, RequestMS: shadowInt64(200), UpstreamMS: shadowInt64(160), OneAPIIDHMAC: "id-2"},
		{Kind: cwEvidenceNginxAccess, EventMS: 1200300, Route: "/v1/responses", Method: "POST", Status: shadowStatus(200), UpstreamStatuses: []int{200}, RequestMS: shadowInt64(100), UpstreamMS: shadowInt64(80), OneAPIIDHMAC: "id-3"},
	}
	// The old collector had 250+80=330ms upstream time.  The three events
	// below deliberately use the same total so a matching window is clean.
	if got := compareShadowNginx(old, newEvidence, from, to, "fixture-shadow-key", 1300); len(got) != 0 {
		t.Fatalf("matching request aggregate should have no differences: got=%+v", got)
	}

	newEvidence[2].UpstreamMS = shadowInt64(0)
	newEvidence[2].OneAPIIDHMAC = ""
	diffs := compareShadowNginx(old, newEvidence, from, to, "fixture-shadow-key", 1300)
	if len(diffs) != 1 || !strings.Contains(diffs[0].DifferenceMask, "request_id_coverage") {
		t.Fatalf("missing request ID coverage must be explicit: %+v", diffs)
	}
	if strings.Contains(diffs[0].DimensionHMAC, "/v1/responses") {
		t.Fatal("persisted dimension reference must not contain the route")
	}
}

func TestCompareShadowNginxEqualAggregateHasNoDiff(t *testing.T) {
	old := []NginxMinuteSample{{BucketTs: 1200, Node: "a", Route: "/v1/responses", Method: "POST", Status: 200, UpstreamStatus: 200, Count: 2, RequestTimeSumMS: 300, RequestTimeMaxMS: 200, UpstreamTimeSumMS: 250, RequestIDPresent: 2}}
	newEvidence := []cloudWatchStructuredEvidence{
		{Kind: cwEvidenceNginxAccess, EventMS: 1200100, Route: "/v1/responses", Method: "POST", Status: shadowStatus(200), UpstreamStatuses: []int{200}, RequestMS: shadowInt64(100), UpstreamMS: shadowInt64(100), OneAPIIDHMAC: "id-1"},
		{Kind: cwEvidenceNginxAccess, EventMS: 1200200, Route: "/v1/responses", Method: "POST", Status: shadowStatus(200), UpstreamStatuses: []int{200}, RequestMS: shadowInt64(200), UpstreamMS: shadowInt64(150), OneAPIIDHMAC: "id-2"},
	}
	if got := compareShadowNginx(old, newEvidence, 1200, 1260, "fixture-shadow-key", 1300); len(got) != 0 {
		t.Fatalf("equal aggregate produced differences: %+v", got)
	}
}

func TestCompareShadowRejectionsNormalizesReasonsAndExcludesOrdinaryErrors(t *testing.T) {
	old := []RejectionSample{{BucketTs: 1200, Node: "worker-a", Reason: "no_available_channel", Model: "m1", Grp: "g1", UserID: 7, Count: 2}, {BucketTs: 1200, Node: "worker-a", Reason: "upstream_5xx", Model: "m1", Grp: "g1", UserID: 7, Count: 10}}
	newEvidence := []cloudWatchStructuredEvidence{
		{Kind: cwEvidenceNewAPIError, EventMS: 1200100, Category: "route_no_channel", Model: "m1", Group: "g1", UserID: shadowInt64(7)},
		{Kind: cwEvidenceNewAPIError, EventMS: 1200200, Category: "route_no_channel", Model: "m1", Group: "g1", UserID: shadowInt64(7)},
		{Kind: cwEvidenceNewAPIError, EventMS: 1200300, Category: "upstream_5xx", Model: "m1", Group: "g1", UserID: shadowInt64(7)},
	}
	if got := compareShadowRejections(old, newEvidence, 1200, 1260, "fixture-shadow-key", 1300); len(got) != 0 {
		t.Fatalf("normalized rejection aggregate should match and ignore ordinary errors: %+v", got)
	}

	newEvidence = newEvidence[:2]
	newEvidence = append(newEvidence, cloudWatchStructuredEvidence{Kind: cwEvidenceNewAPIError, EventMS: 1200400, Category: "route_no_channel", Model: "m1", Group: "g1", UserID: shadowInt64(7)})
	diffs := compareShadowRejections(old, newEvidence, 1200, 1260, "fixture-shadow-key", 1300)
	if len(diffs) != 1 || diffs[0].OldCount != 2 || diffs[0].NewCount != 3 || diffs[0].Delta != 1 {
		t.Fatalf("rejection count difference not persisted correctly: %+v", diffs)
	}
}

func TestCanonicalShadowRejectionReasonKeepsLegacyQuotaReasonsInTheClosedSet(t *testing.T) {
	for _, value := range []string{"user_quota_insufficient", "token_quota_insufficient", "pre_consume_failed"} {
		if got := canonicalShadowRejectionReason(value); got != "quota_account" || !isShadowRejectionCategory(value) {
			t.Fatalf("legacy quota reason %q normalized to %q or excluded", value, got)
		}
	}
}

func TestPersistCloudWatchShadowComparisonIsAtomicAndRedacted(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&CloudWatchShadowReconciliationRun{}, &CloudWatchShadowReconciliationDiff{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{CloudWatchEvidenceHMACKey: strings.Repeat("k", 32), CloudWatchEvidenceHMACKeyID: "fixture-v1"}}
	run := CloudWatchShadowReconciliationRun{ID: "shadow_fixture", WindowFrom: 1200, WindowTo: 1260, Status: cloudWatchShadowStatusComplete, NginxStatus: cloudWatchShadowStatusComplete, RejectionStatus: cloudWatchShadowStatusComplete, CreatedAtUnix: 1300}
	diff := CloudWatchShadowReconciliationDiff{Lane: cloudWatchShadowLaneRejection, BucketTs: 1200, DimensionClass: "minute_reason_model_group_user", DimensionHMAC: strings.Repeat("a", 64), OldCount: 1, NewCount: 2, Delta: 1, DifferenceMask: "count"}
	if err := m.persistCloudWatchShadowComparison(context.Background(), run, []CloudWatchShadowReconciliationDiff{diff}); err != nil {
		t.Fatal(err)
	}
	var gotRun CloudWatchShadowReconciliationRun
	if err := db.First(&gotRun, "id = ?", run.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotRun.DifferenceCount != 1 || gotRun.Status != cloudWatchShadowStatusComplete {
		t.Fatalf("unexpected persisted run: %+v", gotRun)
	}
	var gotDiff CloudWatchShadowReconciliationDiff
	if err := db.First(&gotDiff, "run_id = ?", run.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotDiff.DimensionHMAC != strings.Repeat("a", 64) || gotDiff.Delta != 1 {
		t.Fatalf("unexpected persisted diff: %+v", gotDiff)
	}

	bad := run
	bad.ID = "shadow_bad"
	bad.WindowTo = bad.WindowFrom
	if err := m.persistCloudWatchShadowComparison(context.Background(), bad, nil); err == nil {
		t.Fatal("invalid window must be rejected before writing")
	}
	var count int64
	if err := db.Model(&CloudWatchShadowReconciliationRun{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("invalid run changed durable state: count=%d err=%v", count, err)
	}
}

func TestCloudWatchShadowPlanUsesNewMigrationVersion(t *testing.T) {
	if !strings.Contains(preMigrationPlanID, "v52") || !strings.Contains(preMigrationPlanID, "cloudwatch-investigation-audit-shadow-reconciliation-preroute-cursor-nginx-cursor-v1-nginx-repair-cursor-v1") {
		t.Fatalf("shadow tables added without a new migration plan: %s", preMigrationPlanID)
	}
	if !strings.Contains(preMigrationCombinedPlanID, "cloudwatch-investigation-audit-shadow-reconciliation-preroute-cursor-nginx-cursor-v1-nginx-repair-cursor-v1") ||
		!strings.Contains(preMigrationCombinedPlanID, "nginx-evidence-backfill-v1-nginx-source-v2") {
		t.Fatalf("combined migration plan missing shadow schema: %s", preMigrationCombinedPlanID)
	}
}

func TestCloudWatchShadowSettingsStayFailClosedUntilExplicitlyReady(t *testing.T) {
	if err := validateCloudWatchShadowSettings(Settings{}); err != nil {
		t.Fatalf("disabled Shadow must remain inert: %v", err)
	}
	base := Settings{CloudWatchLogsEnabled: true, CloudWatchShadowEnabled: true, CloudWatchShadowNginxContractReady: true, CloudWatchShadowIntervalMinutes: 15, CloudWatchShadowLookbackMinutes: 15, CloudWatchShadowRetentionDays: 14}
	if err := validateCloudWatchShadowSettings(base); err != nil {
		t.Fatalf("valid Shadow settings rejected: %v", err)
	}
	for _, bad := range []Settings{
		{CloudWatchShadowEnabled: true, CloudWatchShadowIntervalMinutes: 15, CloudWatchShadowLookbackMinutes: 15, CloudWatchShadowRetentionDays: 14},
		{CloudWatchLogsEnabled: true, CloudWatchShadowEnabled: true, CloudWatchShadowNginxContractReady: true, CloudWatchShadowIntervalMinutes: 1, CloudWatchShadowLookbackMinutes: 15, CloudWatchShadowRetentionDays: 14},
		{CloudWatchLogsEnabled: true, CloudWatchShadowEnabled: true, CloudWatchShadowNginxContractReady: true, CloudWatchShadowIntervalMinutes: 15, CloudWatchShadowLookbackMinutes: 121, CloudWatchShadowRetentionDays: 14},
		{CloudWatchLogsEnabled: true, CloudWatchShadowEnabled: true, CloudWatchShadowNginxContractReady: true, CloudWatchShadowIntervalMinutes: 15, CloudWatchShadowLookbackMinutes: 15, CloudWatchShadowRetentionDays: 6},
		{CloudWatchLogsEnabled: true, CloudWatchShadowEnabled: true, CloudWatchShadowNginxContractReady: false, CloudWatchShadowIntervalMinutes: 15, CloudWatchShadowLookbackMinutes: 15, CloudWatchShadowRetentionDays: 14},
	} {
		if err := validateCloudWatchShadowSettings(bad); err == nil {
			t.Fatalf("invalid Shadow settings accepted: %+v", bad)
		}
	}
}
