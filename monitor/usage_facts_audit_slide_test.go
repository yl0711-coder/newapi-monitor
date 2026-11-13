package monitor

import (
	"testing"
	"time"

	"gorm.io/gorm"
)

// 2026-11-12: every recent 14-day source audit was restarted from its left
// edge at each midnight and never reached the newest week. A forward move of
// the left edge must keep the cursor and drop only the hours that left.
func TestUsageFactRecentAuditWindowSlidesInsteadOfRestarting(t *testing.T) {
	uid := int64(42)
	day := time.Date(2026, 11, 1, 0, 0, 0, 0, usageCST).Unix()
	now := time.Unix(day+15*usageFactDaySeconds+600, 0)
	base := func(next int64) UsageFactJob {
		return UsageFactJob{
			ID: usageFactRecentAuditJobID(uid, 1), IdempotencyKey: usageFactMaintenanceJobKey(usageFactRecentAuditJobID(uid, 1)),
			Kind: usageFactHistoryKindRecentAudit, Priority: 30, UserID: &uid, TrackedRevision: 1, SourceEpoch: "e1",
			FromTs: day, ThroughTs: day + 14*usageFactDaySeconds, NextHour: next,
			TotalHours: 336, CompletedHours: max(next-day, 0) / usageFactHourSeconds,
			Status: usageFactHistoryJobQueued, CreatedAt: day, UpdatedAt: day,
		}
	}
	desiredNextDay := func() UsageFactJob {
		d := base(day + usageFactDaySeconds)
		d.FromTs, d.ThroughTs, d.NextHour = day+usageFactDaySeconds, day+15*usageFactDaySeconds, day+usageFactDaySeconds
		d.TotalHours, d.CompletedHours = 336, 0
		return d
	}
	run := func(t *testing.T, current, desired UsageFactJob) UsageFactJob {
		t.Helper()
		m := newUsageHistoryTestMonitor(t)
		if err := m.usageFactsStore().Create(&current).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
			return upsertUsageFactAuditJob(tx, desired, usageFactHistoryRecentAuditCycle, now)
		}); err != nil {
			t.Fatal(err)
		}
		var got UsageFactJob
		if err := m.usageFactsStore().First(&got, "id = ?", current.ID).Error; err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("cursor inside the new window is kept", func(t *testing.T) {
		got := run(t, base(day+96*usageFactHourSeconds), desiredNextDay())
		if got.FromTs != day+usageFactDaySeconds || got.ThroughTs != day+15*usageFactDaySeconds || got.NextHour != day+96*usageFactHourSeconds ||
			got.TotalHours != 336 || got.CompletedHours != 72 || got.Status != usageFactHistoryJobQueued || got.CreatedAt != day {
			t.Fatalf("window did not slide: %+v", got)
		}
	})
	t.Run("cursor that left the window moves to the new left edge", func(t *testing.T) {
		got := run(t, base(day+10*usageFactHourSeconds), desiredNextDay())
		if got.NextHour != day+usageFactDaySeconds || got.CompletedHours != 0 || got.FromTs != day+usageFactDaySeconds {
			t.Fatalf("stale cursor kept: %+v", got)
		}
	})
	t.Run("a running lease is not taken away", func(t *testing.T) {
		current := base(day + 96*usageFactHourSeconds)
		current.Status, current.LeaseOwner, current.LeaseUntil = usageFactHistoryJobRunning, "audit-worker", now.Unix()+60
		got := run(t, current, desiredNextDay())
		// Deferred while leased: the worker advances from the range it claimed.
		if got.Status != usageFactHistoryJobRunning || got.LeaseOwner != "audit-worker" || got.NextHour != day+96*usageFactHourSeconds || got.FromTs != day {
			t.Fatalf("running audit was disturbed: %+v", got)
		}
	})
	t.Run("an expired lease slides", func(t *testing.T) {
		current := base(day + 96*usageFactHourSeconds)
		current.Status, current.LeaseOwner, current.LeaseUntil = usageFactHistoryJobRunning, "gone-worker", now.Unix()-60
		got := run(t, current, desiredNextDay())
		if got.FromTs != day+usageFactDaySeconds || got.NextHour != day+96*usageFactHourSeconds {
			t.Fatalf("abandoned lease blocked the slide: %+v", got)
		}
	})
	t.Run("a paused job is still reset and re-queued", func(t *testing.T) {
		current := base(day + 96*usageFactHourSeconds)
		current.Status, current.Attempts, current.LastError = usageFactHistoryJobPaused, 5, "source timeout"
		got := run(t, current, desiredNextDay())
		if got.Status != usageFactHistoryJobQueued || got.Attempts != 0 || got.LastError != "" || got.NextHour != day+usageFactDaySeconds {
			t.Fatalf("paused audit was not recovered: %+v", got)
		}
	})
	t.Run("an earlier left edge still restarts", func(t *testing.T) {
		desired := base(day - usageFactDaySeconds)
		desired.FromTs = day - usageFactDaySeconds
		got := run(t, base(day+96*usageFactHourSeconds), desired)
		if got.FromTs != day-usageFactDaySeconds || got.NextHour != day-usageFactDaySeconds || got.CompletedHours != 0 {
			t.Fatalf("earlier history was not re-audited: %+v", got)
		}
	})
	t.Run("a new revision still restarts", func(t *testing.T) {
		desired := desiredNextDay()
		desired.TrackedRevision = 2
		got := run(t, base(day+96*usageFactHourSeconds), desired)
		if got.TrackedRevision != 2 || got.NextHour != day+usageFactDaySeconds || got.CompletedHours != 0 {
			t.Fatalf("revision change kept old progress: %+v", got)
		}
	})
	t.Run("the 30-day source audit keeps its restart semantics", func(t *testing.T) {
		current := base(day + 96*usageFactHourSeconds)
		current.ID, current.Kind = usageFactSourceAuditJobID(uid, 1), usageFactHistoryKindSourceAudit
		desired := desiredNextDay()
		desired.ID, desired.Kind = current.ID, usageFactHistoryKindSourceAudit
		got := run(t, current, desired)
		if got.NextHour != day+usageFactDaySeconds || got.CompletedHours != 0 {
			t.Fatalf("source audit semantics changed: %+v", got)
		}
	})
}
