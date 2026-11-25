package monitor

import (
	"context"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestUsageVerifyLiveTailRecoversCrossMidnightPublication(t *testing.T) {
	for _, tc := range []struct {
		name         string
		legacyPaused bool
		badHash      bool
		hasUsage     bool
		localTimeout bool
		restart      bool
	}{
		{name: "wait for live then publish"},
		{name: "resume transient legacy pause with real-shaped consumption", legacyPaused: true, hasUsage: true},
		{name: "never publish a mismatched hour", legacyPaused: true, badHash: true},
		{name: "local verification timeout resumes without losing attempts", legacyPaused: true, hasUsage: true, localTimeout: true},
		{name: "repeated local timeouts survive store restart", legacyPaused: true, hasUsage: true, localTimeout: true, restart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m := newUsageHistoryTestMonitor(t)
			m.cfg.UsageFactsReadEnabled = true
			m.cfg.UsageFactsRawPageImportEnabled = true
			const uid int64 = 6001
			prepareUsageHistoryCommitMember(t, m, uid, 1)
			db := m.usageFactsStore()
			day := time.Date(2026, 11, 25, 0, 0, 0, 0, usageCST).Unix()
			now := time.Unix(day+80*60, 0)
			target := m.usageFactFinalizedHour(now)
			yesterday := day - usageFactDaySeconds
			older := day - usageFactHourSeconds
			epoch := m.cfg.UsageFactsHistorySourceEpoch
			member := UsageFactMemberState{
				UserID: uid, Active: true, TrackedRevision: 1, SourceEpoch: epoch,
				SourceFloorHour: int64Ptr(yesterday), SourceFloorCheckedAt: older,
				SourceHistoryStatus: "no_history", ClassificationVersion: userTrafficClassificationVersion,
				QuerySemanticsVersion: usageFactQuerySemanticsVersion, TailThroughHour: int64Ptr(older),
				CoverageThroughHour: int64Ptr(yesterday), VerifiedThroughHour: int64Ptr(yesterday),
				VerificationStatus: "complete", CoverageStatus: "ready",
			}
			emptyHash := usageFactContentHash(nil)
			if tc.hasUsage {
				// The former serving snapshot is an independently verified empty
				// prefix ending at yesterday 23:00. There is a real hour fact
				// after midnight, which must be included in today's result.
				member.SourceHistoryStatus, member.SourceFirstLogHour = "complete_hot", int64Ptr(day)
				emptyDay, err := m.fetchUsageFactHistoryRange(ctx, yesterday, day, []int64{uid})
				if err != nil {
					t.Fatal(err)
				}
				if err := m.commitUsageFactHistoryRange(ctx, emptyDay, "fixture-closed-day", map[int64]int64{uid: 1}); err != nil {
					t.Fatal(err)
				}
				for hour := yesterday; hour < day; hour += usageFactHourSeconds {
					if err := db.Create(&UsageFactMemberHourState{UserID: uid, HourTs: hour, Status: "complete", ContentHash: emptyHash, SourceEpoch: epoch}).Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := db.Save(&member).Error; err != nil {
				t.Fatal(err)
			}
			id := uid
			job := UsageFactJob{
				ID: usageFactHistoryJobID(uid, 1), UserID: &id, TrackedRevision: 1, Kind: usageFactHistoryKindVerify,
				SourceEpoch: epoch, Status: usageFactHistoryJobComplete, FromTs: yesterday,
				ThroughTs: older, NextHour: older, VerifyNextHour: yesterday, CompletedAt: older,
			}
			if err := db.Create(&job).Error; err != nil {
				t.Fatal(err)
			}
			first, err := m.publishUsageFactFullHistorySnapshot(ctx, time.Unix(older+20*60, 0))
			if err != nil || first.PublishedThrough != older {
				t.Fatalf("yesterday fixture: through=%d err=%v", first.PublishedThrough, err)
			}
			member.TailThroughHour, member.CoverageThroughHour, member.VerifiedThroughHour = int64Ptr(day), int64Ptr(day), int64Ptr(day)
			member.LiveFromHour, member.LiveThroughHour, member.LiveTargetHour = int64Ptr(day), int64Ptr(day), int64Ptr(target)
			member.VerificationStatus, member.CoverageStatus, member.LiveStatus = "running", "verifying", "catching_up"
			if err := db.Save(&member).Error; err != nil {
				t.Fatal(err)
			}
			job.Status, job.ThroughTs, job.NextHour, job.VerifyNextHour = usageFactHistoryJobQueued, target, day, day
			if tc.legacyPaused {
				job.Status, job.Attempts = usageFactHistoryJobPaused, 1
				job.LastError = usageFactMissingEpochHoursPrefix + " got=0 want=1"
			}
			if err := db.Save(&job).Error; err != nil {
				t.Fatal(err)
			}
			turn := func(at time.Time) {
				t.Helper()
				claim, err := m.claimUsageFactHistoryJobs(ctx, usageFactHistoryKindVerify, "fixture", 1, 1, at)
				if err != nil || len(claim.Jobs) != 1 {
					t.Fatalf("need one claimed verify: %+v, %v", claim, err)
				}
				if err := m.executeUsageFactHistoryVerify(ctx, claim, at); err != nil {
					t.Fatal(err)
				}
				if err := db.First(&job, "id = ?", job.ID).Error; err != nil {
					t.Fatal(err)
				}
			}
			if tc.legacyPaused {
				if err := m.reconcileUsageFactHistoryJobs(ctx, now); err != nil {
					t.Fatal(err)
				}
				if err := db.First(&job, "id = ?", job.ID).Error; err != nil || job.Status != usageFactHistoryJobPaused {
					t.Fatalf("missing live hour reopened an old pause: status=%s err=%v", job.Status, err)
				}
			} else {
				turn(now)
				if job.Status != usageFactHistoryJobQueued || job.Attempts != 0 || job.NextRetryAt <= now.Unix() || job.NextHour != day {
					t.Fatalf("early verify should wait without marking failed: %+v", job)
				}
			}
			for hour := day; hour < target; hour += usageFactHourSeconds {
				proof := UsageFactMemberHourState{UserID: uid, HourTs: hour, Status: "complete", ContentHash: emptyHash, SourceEpoch: epoch}
				if tc.hasUsage && hour == day {
					row := UsageHourFact{UserID: uid, HourTs: hour, DayTs: day, ChannelID: 1, Grp: "fixture", ModelName: "model", TokenID: 1, Requests: 1, ConsumeQuota: 5000}
					if err := db.Create(&row).Error; err != nil {
						t.Fatal(err)
					}
					proof.Rows, proof.Requests, proof.ContentHash = 1, 1, usageFactContentHash([]UsageHourFact{row})
				}
				if tc.badHash {
					proof.ContentHash = strings.Repeat("a", 64)
				}
				if err := db.Create(&proof).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Model(&UsageFactMemberState{}).Where("user_id = ?", uid).
				Updates(map[string]any{"live_through_hour": target, "live_status": "ready"}).Error; err != nil {
				t.Fatal(err)
			}
			later := now.Add(2 * time.Minute)
			if err := m.reconcileUsageFactHistoryJobs(ctx, later); err != nil {
				t.Fatal(err)
			}
			if tc.localTimeout {
				const callback = "recovery-test:local-audit-timeout"
				failOnce := true
				if err := db.Callback().Query().Before("gorm:query").Register(callback, func(tx *gorm.DB) {
					if failOnce && tx.Statement.Table == "usage_fact_member_hour_states" {
						failOnce = false
						_ = tx.AddError(context.DeadlineExceeded) // GORM carries the injected error in tx.Error.
					}
				}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Callback().Query().Remove(callback) })
				for attempt := 0; attempt < 3; attempt++ {
					failOnce = true
					turn(later)
					if job.Status != usageFactHistoryJobQueued || job.Attempts != 1 || job.NextHour != day || job.NextRetryAt <= later.Unix() {
						t.Fatalf("local audit timeout became a data failure: %+v", job)
					}
					later = later.Add(2 * time.Minute)
				}
				if err := db.Callback().Query().Remove(callback); err != nil {
					t.Fatal(err)
				}
				if tc.restart {
					cfg := m.cfg
					m.Close()
					m = &Monitor{cfg: cfg, chNames: map[string]string{}}
					// Keep the production read-intent condition, but point it at
					// a new EMPTY synthetic source. Any fallback loses the 5000
					// retained amount and fails the assertion below.
					m.prodDB = newFakeProdDB(t)
					m.usageDayExpr = "CAST((created_at + 28800) / 86400 AS INTEGER)"
					if err := m.openStore(cfg.StorePath); err != nil {
						t.Fatal("reopen durable queued verification", err)
					}
					t.Cleanup(m.Close)
					db = m.usageFactsStore()
				}
			}
			turn(later)
			var published UsageFactSyncState
			if err := db.First(&published, 1).Error; err != nil {
				t.Fatal(err)
			}
			if tc.badHash {
				if job.Status != usageFactHistoryJobPaused || published.PublishedThrough != older || job.NextHour != day {
					t.Fatalf("invalid proof advanced history/publish: job=%+v through=%d", job, published.PublishedThrough)
				}
				return
			}
			if job.Status != usageFactHistoryJobComplete || published.PublishedThrough != target ||
				published.PublishedRangeStart != first.PublishedRangeStart || job.NextHour != target {
				t.Fatalf("verified live tail didn't publish without changing historical floor: status=%s next=%d published=%d floor=%d",
					job.Status, job.NextHour, published.PublishedThrough, published.PublishedRangeStart)
			}
			if tc.hasUsage {
				matrix, err := m.computeUsageMatrixForReadRange(ctx, []int64{uid}, day, target, target)
				if err != nil || len(matrix.Cells) != 1 || matrix.Cells[0].ConsumeQuota != 5000 {
					t.Fatalf("today's published consumption not readable: %+v, %v", matrix, err)
				}
			}
		})
	}
}
