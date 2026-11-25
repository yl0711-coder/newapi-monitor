package monitor

import (
	"context"
	"testing"
	"time"
)

func TestUsageFactMultiDayPauseReturnsToOriginalTailAndFinishes(t *testing.T) {
	ctx := context.Background()
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	m.cfg.BackgroundSourceMinStartIntervalMS = -1 // synthetic SQLite only
	const uid int64 = 7101
	prepareUsageHistoryCommitMember(t, m, uid, 1)
	db := m.usageFactsStore()
	day := time.Date(2026, 11, 22, 0, 0, 0, 0, usageCST).Unix()
	cursor := day + 23*usageFactHourSeconds
	now := time.Unix(day+2*usageFactDaySeconds+2*usageFactHourSeconds+20*60, 0)
	target := m.usageFactFinalizedHour(now)
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	state := UsageFactMemberState{UserID: uid, Active: true, TrackedRevision: 1, SourceEpoch: epoch,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour: int64Ptr(day), SourceFirstLogHour: int64Ptr(day), SourceFloorCheckedAt: now.Unix(), SourceHistoryStatus: "complete_hot",
		TailThroughHour: int64Ptr(cursor), CoverageThroughHour: int64Ptr(day), VerifiedThroughHour: int64Ptr(day),
		CoverageStatus: "failed", VerificationStatus: "failed"}
	if err := db.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	for hour := day; hour < cursor; hour += usageFactHourSeconds {
		if err := db.Create(&UsageFactMemberHourState{UserID: uid, HourTs: hour, SourceEpoch: epoch, Status: "complete", ContentHash: usageFactContentHash(nil)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(1,?,1,?,2,'model',5000,3,2,'fixture',1,'fixture')", uid, cursor+5); err != nil {
		t.Fatal(err)
	}
	job := UsageFactJob{ID: usageFactHistoryJobID(uid, 1), UserID: ptrInt64(uid), TrackedRevision: 1, SourceEpoch: epoch,
		Kind: usageFactHistoryKindVerify, Status: usageFactHistoryJobPaused, FromTs: day, ThroughTs: cursor + 3600,
		NextHour: cursor, VerifyNextHour: day, Attempts: 1, LastError: usageFactMissingEpochHoursPrefix + " got=23 want=24"}
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.reconcileUsageFactHistoryJobs(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Kind != usageFactHistoryKindTail || job.Status != usageFactHistoryJobQueued || job.NextHour != cursor || job.VerifyNextHour != day {
		t.Fatalf("recovery skipped original cursor: %+v", job)
	}
	for turn := 0; turn < 120 && job.Status != usageFactHistoryJobComplete; turn++ {
		claim, err := m.claimUsageFactHistoryJobs(ctx, job.Kind, "multiday", 1, 1, now)
		if err != nil || len(claim.Jobs) != 1 {
			t.Fatalf("claim: %v %+v", err, claim)
		}
		switch job.Kind {
		case usageFactHistoryKindTail:
			err = m.executeUsageFactHistoryTailRawPages(ctx, claim, now)
		case usageFactHistoryKindBackfill:
			err = m.executeUsageFactHistoryBackfillRawPages(ctx, claim, now)
		case usageFactHistoryKindVerify:
			err = m.executeUsageFactHistoryVerify(ctx, claim, now)
		default:
			t.Fatalf("unexpected stage: %s", job.Kind)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := db.First(&job, "id = ?", job.ID).Error; err != nil {
			t.Fatal(err)
		}
		if job.Status == usageFactHistoryJobPaused {
			t.Fatalf("recovery paused: %+v", job)
		}
	}
	if job.Status != usageFactHistoryJobComplete || job.NextHour != target {
		t.Fatalf("did not finish bounded recovery: %+v", job)
	}
	var published UsageFactSyncState
	if err := db.First(&published, 1).Error; err != nil {
		t.Fatal(err)
	}
	if published.PublishedThrough != target {
		t.Fatalf("worker finished but page publication stalled: %+v", published)
	}
	matrix, err := m.computeUsageMatrixForReadRange(ctx, []int64{uid}, day, target, target)
	if err != nil {
		t.Fatal(err)
	}
	var quota int64
	for _, cell := range matrix.Cells {
		quota += cell.ConsumeQuota
	}
	if quota != 5000 {
		t.Fatalf("lost/duplicated consumption: %d", quota)
	}
}

func TestUsageFactMultiDayRecoveryDoesNotUnlockUnrelatedPause(t *testing.T) {
	for _, kind := range []string{"bad hash", "owned lease", "cursor conflict"} {
		t.Run(kind, func(t *testing.T) {
			m := newUsageHistoryTestMonitor(t)
			m.cfg.UsageFactsRawPageImportEnabled = true
			const uid int64 = 7102
			prepareUsageHistoryCommitMember(t, m, uid, 1)
			db := m.usageFactsStore()
			day := time.Date(2026, 11, 22, 0, 0, 0, 0, usageCST).Unix()
			cursor := day + 23*usageFactHourSeconds
			now := time.Unix(day+2*usageFactDaySeconds+80*60, 0)
			epoch := m.cfg.UsageFactsHistorySourceEpoch
			state := UsageFactMemberState{UserID: uid, Active: true, TrackedRevision: 1, SourceEpoch: epoch, ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion, SourceFloorHour: int64Ptr(day), SourceFirstLogHour: int64Ptr(day), SourceFloorCheckedAt: now.Unix(), SourceHistoryStatus: "complete_hot", TailThroughHour: int64Ptr(cursor), CoverageThroughHour: int64Ptr(day), VerifiedThroughHour: int64Ptr(day)}
			if err := db.Save(&state).Error; err != nil {
				t.Fatal(err)
			}
			job := UsageFactJob{ID: usageFactHistoryJobID(uid, 1), UserID: ptrInt64(uid), TrackedRevision: 1, SourceEpoch: epoch, Kind: usageFactHistoryKindVerify, Status: usageFactHistoryJobPaused, FromTs: day, ThroughTs: cursor + 3600, NextHour: cursor, VerifyNextHour: day, LastError: usageFactMissingEpochHoursPrefix + " got=23 want=24"}
			switch kind {
			case "bad hash":
				job.LastError = "当前日成员小时事实语义不一致"
			case "owned lease":
				job.LeaseOwner = "another-worker"
			case "cursor conflict":
				job.NextHour++
			}
			if err := db.Create(&job).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.reconcileUsageFactHistoryJobs(context.Background(), now); err != nil {
				t.Fatal(err)
			}
			if err := db.First(&job, "id = ?", job.ID).Error; err != nil {
				t.Fatal(err)
			}
			if job.Kind != usageFactHistoryKindVerify || job.Status != usageFactHistoryJobPaused {
				t.Fatalf("unsafe pause unlocked: %+v", job)
			}
		})
	}
}
