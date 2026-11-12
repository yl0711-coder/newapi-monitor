package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// seedUsageCheckpointPair publishes two no-history members (31, 32) that pass
// the checkpoint, mirroring TestProvenMismatchReopensUnaffectedPublishedMembers.
func seedUsageCheckpointPair(t *testing.T) (*Monitor, time.Time, int64, int64) {
	t.Helper()
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsReadEnabled = true
	prepareUsageHistoryCommitMember(t, m, 31, 1)
	prepareUsageHistoryCommitMember(t, m, 32, 1)
	now := time.Date(2026, 5, 22, 1, 0, 0, 0, usageCST)
	through := time.Date(2026, 5, 22, 0, 0, 0, 0, usageCST).Unix()
	floor := through - 10*usageFactDaySeconds
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	nowUnix := now.Unix()
	for _, userID := range []int64{31, 32} {
		if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
			"source_floor_hour": floor, "source_first_log_hour": nil, "source_ceiling_hour": nil,
			"source_floor_checked_at": nowUnix, "source_history_status": "no_history", "source_epoch": epoch,
			"coverage_status": "ready", "coverage_through_hour": through, "tail_through_hour": through,
			"verification_status": "complete", "verify_next_hour": through, "verified_through_hour": through,
			"verified_at": nowUnix, "classification_version": userTrafficClassificationVersion,
			"query_semantics_version": usageFactQuerySemanticsVersion, "updated_at": nowUnix,
		}).Error; err != nil {
			t.Fatal(err)
		}
		uid := userID
		if err := m.usageFactsStore().Create(&UsageFactJob{
			ID: usageFactHistoryJobID(uid, 1), Kind: usageFactHistoryKindVerify, UserID: &uid,
			TrackedRevision: 1, SourceEpoch: epoch, FromTs: floor, ThroughTs: through,
			NextHour: through, VerifyNextHour: through, TotalHours: (through - floor) / usageFactHourSeconds,
			CompletedHours: (through - floor) / usageFactHourSeconds, VerifiedHours: (through - floor) / usageFactHourSeconds,
			Status: usageFactHistoryJobComplete, CreatedAt: nowUnix, UpdatedAt: nowUnix, CompletedAt: nowUnix,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
			UserID: uid, TrackedRevision: 1, SourceEpoch: epoch,
			ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
			SourceFloorHour: floor, VerifiedThroughHour: through, PublishedAt: nowUnix,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{31, 32}),
		"published_window_days": 10, "published_range_start": floor, "published_through": through,
		"published_at": nowUnix, "generation": 20, "serving_generation": 20,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.publishUsageFactGenerations(20, 20)
	m.refreshUsageFactsReadiness(context.Background(), now)
	if !m.usageFactsReadEnabled() {
		t.Fatal("fixture must start readable")
	}
	return m, now, floor, through
}

// breakMemberDayProof makes member 31 take the recent-service path and puts a
// fact inside its proven-empty range, so the checkpoint names member 31 and a
// closed day.
func breakMemberDayProof(t *testing.T, m *Monitor, floor, through int64) int64 {
	t.Helper()
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 31).Updates(map[string]any{
		"source_floor_hour": floor - usageFactDaySeconds,
		"live_from_hour":    floor, "live_through_hour": through, "live_target_hour": through,
	}).Error; err != nil {
		t.Fatal(err)
	}
	bad := through - 3*usageFactDaySeconds
	if err := m.usageFactsStore().Create(&UsageDailyFact{DateTs: bad, UserID: 31, Requests: 1, ConsumeQuota: 7}).Error; err != nil {
		t.Fatal(err)
	}
	return bad
}

func TestCheckpointMemberDayFailureRevokesOnlyThatMember(t *testing.T) {
	m, now, floor, through := seedUsageCheckpointPair(t)
	bad := breakMemberDayProof(t, m, floor, through)
	if err := m.validateUsageFactFullHistoryCheckpoint(context.Background(), through); err == nil {
		t.Fatal("fixture did not break member 31's checkpoint")
	}

	m.refreshUsageFactsReadiness(context.Background(), now)

	if !m.usageFactsReadEnabled() || m.usageFactsReadyThrough.Load() != through {
		t.Fatalf("one member's day mismatch closed the whole page: ready=%v through=%d",
			m.usageFactsReadEnabled(), m.usageFactsReadyThrough.Load())
	}
	var publishedIDs []int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Order("user_id").Pluck("user_id", &publishedIDs).Error; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(publishedIDs) != "[32]" {
		t.Fatalf("published members after isolation=%v, want [32]", publishedIDs)
	}
	var repair UsageFactJob
	if err := m.usageFactsStore().First(&repair, "id = ?", usageFactRepairJobID(31, 1, bad)).Error; err != nil {
		t.Fatalf("exact member-day repair not queued: %v", err)
	}
	if repair.Kind != usageFactHistoryKindRepair || repair.FromTs != bad || repair.ThroughTs != bad+usageFactDaySeconds {
		t.Fatalf("repair job=%+v", repair)
	}
}

func TestCheckpointFailureStaysGlobalInReadOnlySnapshot(t *testing.T) {
	m, now, floor, through := seedUsageCheckpointPair(t)
	breakMemberDayProof(t, m, floor, through)
	m.cfg.LocalSnapshotOnly = true
	m.cfg.UsageFactsLocalReadOnly = true
	prod := m.prodDB
	m.prodDB = nil // a mounted offline snapshot has no source connection
	t.Cleanup(func() { m.prodDB = prod })
	if !m.usageFactsLocalSnapshotReadOnly() {
		t.Fatal("fixture did not enter local read-only snapshot mode")
	}
	m.refreshUsageFactsFullHistoryReadiness(context.Background(), now, UsageFactsStatus{
		SnapshotUsable: true, ReadEnabled: true, PublishedRangeStart: floor, PublishedThrough: through,
	})
	if m.usageFactsReadEnabled() {
		t.Fatal("read-only snapshot must keep the fail-closed global gate")
	}
	var n int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Count(&n).Error; err != nil || n != 2 {
		t.Fatalf("read-only snapshot was mutated: published=%d err=%v", n, err)
	}
}

// Once the per-refresh isolation budget is spent, a further failure keeps the
// conservative whole-page close (widespread problems are likely global).
func TestCheckpointIsolationIsBoundedPerRefresh(t *testing.T) {
	m, now, floor, through := seedUsageCheckpointPair(t)
	breakMemberDayProof(t, m, floor, through)
	ctx := context.Background()
	for i := 0; i < usageFactCheckpointIsolationLimit; i++ {
		ctx = withUsageFactCheckpointIsolationDepth(ctx)
	}
	m.refreshUsageFactsReadiness(ctx, now)
	if m.usageFactsReadEnabled() {
		t.Fatal("isolation budget exhausted but the gate reopened")
	}
	var n int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Count(&n).Error; err != nil || n != 2 {
		t.Fatalf("member revoked beyond the isolation budget: published=%d err=%v", n, err)
	}
}
