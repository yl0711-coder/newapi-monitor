package monitor

import (
	"context"
	"errors"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

func TestSumUsageFactDayControlInSlicesSplitsOnTimeoutAndSumsExactly(t *testing.T) {
	day := time.Date(2026, 11, 17, 0, 0, 0, 0, usageCST).Unix()
	// One row per hour: hour h has h+1 requests and (h+1)*10 quota.
	hourly := func(from, through int64) usageFactHistoryControl {
		var c usageFactHistoryControl
		for h := (from - day) / usageFactHourSeconds; h < (through-day)/usageFactHourSeconds; h++ {
			c.SourceRows += h + 1
			c.Requests += h + 1
			c.ConsumeQuota += (h + 1) * 10
			c.PromptTokens += 1
		}
		return c
	}
	var calls [][2]int64
	fetch := func(_ context.Context, uid, from, through int64) (usageFactHistoryControl, error) {
		calls = append(calls, [2]int64{from, through})
		// The morning slice is too heavy as a whole and as halves of 3 hours.
		if from < day+6*usageFactHourSeconds && through-from > usageFactHourSeconds {
			return usageFactHistoryControl{}, &mysqlDriver.MySQLError{Number: 3024}
		}
		return hourly(from, through), nil
	}
	got, err := sumUsageFactDayControlInSlices(context.Background(), 7, day, fetch, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	want := hourly(day, day+usageFactDaySeconds)
	if got.SourceRows != want.SourceRows || got.Requests != want.Requests || got.ConsumeQuota != want.ConsumeQuota ||
		got.PromptTokens != 24 || got.UserID != 7 || got.DateTs != day {
		t.Fatalf("sum mismatch: got %+v want %+v", got, want)
	}
	covered := map[int64]int{}
	for _, c := range calls {
		if c[0]%usageFactHourSeconds != 0 || c[1]%usageFactHourSeconds != 0 {
			t.Fatalf("slice not hour aligned: %v", c)
		}
	}
	for _, c := range calls {
		ok := !(c[0] < day+6*usageFactHourSeconds && c[1]-c[0] > usageFactHourSeconds)
		if ok {
			for h := c[0]; h < c[1]; h += usageFactHourSeconds {
				covered[h]++
			}
		}
	}
	for h := day; h < day+usageFactDaySeconds; h += usageFactHourSeconds {
		if covered[h] != 1 {
			t.Fatalf("hour %d counted %d times", (h-day)/3600, covered[h])
		}
	}

	// A single hour that still times out is an error, not a split loop.
	stuck := func(context.Context, int64, int64, int64) (usageFactHistoryControl, error) {
		return usageFactHistoryControl{}, context.DeadlineExceeded
	}
	if _, err := sumUsageFactDayControlInSlices(context.Background(), 7, day, stuck, time.Time{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline error, got %v", err)
	}
	// The time budget stops the pre-check.
	if _, err := sumUsageFactDayControlInSlices(context.Background(), 7, day, fetch, time.Now().Add(-time.Second)); !errors.Is(err, errUsageFactRecentControlBudget) {
		t.Fatalf("want budget error, got %v", err)
	}
	// An inconsistent slice is rejected.
	bad := func(context.Context, int64, int64, int64) (usageFactHistoryControl, error) {
		return usageFactHistoryControl{SourceRows: 2, Requests: 1}, nil
	}
	if _, err := sumUsageFactDayControlInSlices(context.Background(), 7, day, bad, time.Time{}); err == nil {
		t.Fatal("inconsistent slice control accepted")
	}
}

func TestRecentAuditControlPrecheckAdvancesUnchangedDayAndRecordsChanges(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const uid = int64(1)
	prepareUsageHistoryCommitMember(t, m, uid, 1)
	day := time.Date(2026, 11, 15, 0, 0, 0, 0, usageCST).Unix()
	insert := func(id, at, quota int64) {
		t.Helper()
		if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES(?,?,10,?,2,'m-a',?,100,20,'g-a',5,'token-a','')`, id, uid, at, quota); err != nil {
			t.Fatal(err)
		}
	}
	insert(1, day+60, 1000)
	insert(2, day+13*usageFactHourSeconds, 500)
	insert(3, day+23*usageFactHourSeconds+3599, 7)
	insert(4, day+usageFactDaySeconds, 999) // next day, outside the audited day
	result, err := m.fetchUsageFactHistoryRange(context.Background(), day, day+usageFactDaySeconds, []int64{uid})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.commitUsageFactHistoryRange(context.Background(), result, "history-job-1", map[int64]int64{uid: 1}); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", uid).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(day+3*usageFactDaySeconds, 0)
	claimFor := func(t *testing.T) usageFactHistoryClaim {
		t.Helper()
		id := usageFactRecentAuditJobID(uid, 1)
		m.usageFactsStore().Where("id = ?", id).Delete(&UsageFactJob{})
		u := uid
		job := UsageFactJob{ID: id, IdempotencyKey: usageFactMaintenanceJobKey(id), Kind: usageFactHistoryKindRecentAudit,
			Priority: 30, UserID: &u, TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
			FromTs: day, ThroughTs: day + 2*usageFactDaySeconds, NextHour: day, TotalHours: 48,
			Status: usageFactHistoryJobRunning, LeaseOwner: "w1", LeaseUntil: now.Unix() + 120, CreatedAt: day, UpdatedAt: day}
		if err := m.usageFactsStore().Create(&job).Error; err != nil {
			t.Fatal(err)
		}
		return usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: "w1", From: day, Through: day + usageFactDaySeconds}
	}
	loadJob := func(t *testing.T) UsageFactJob {
		t.Helper()
		var j UsageFactJob
		if err := m.usageFactsStore().First(&j, "id = ?", usageFactRecentAuditJobID(uid, 1)).Error; err != nil {
			t.Fatal(err)
		}
		return j
	}

	t.Run("unchanged day advances without raw pages", func(t *testing.T) {
		claim := claimFor(t)
		handled, err := m.verifyRecentUsageFactAuditByControl(context.Background(), claim, now)
		if err != nil || !handled {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
		j := loadJob(t)
		if j.NextHour != day+usageFactDaySeconds || j.Status != usageFactHistoryJobQueued || j.LeaseOwner != "" || j.Kind != usageFactHistoryKindRecentAudit {
			t.Fatalf("job not advanced: %+v", j)
		}
		var n int64
		m.usageFactsStore().Model(&UsageFactPageIngestState{}).Count(&n)
		if n != 0 {
			t.Fatalf("raw pages were read: %d page states", n)
		}
	})

	t.Run("a late log is recorded and handed to the hourly path", func(t *testing.T) {
		insert(5, day+5*usageFactHourSeconds, 250)
		claim := claimFor(t)
		handled, err := m.verifyRecentUsageFactAuditByControl(context.Background(), claim, now)
		if err != nil || handled {
			t.Fatalf("changed day must fall through: handled=%v err=%v", handled, err)
		}
		j := loadJob(t)
		if j.NextHour != day || j.Status != usageFactHistoryJobRunning || j.LeaseOwner != "w1" {
			t.Fatalf("claim must be left for the hourly path: %+v", j)
		}
		var f UsageFactAuditFinding
		if err := m.usageFactsStore().First(&f, "user_id = ? AND date_ts = ?", uid, day).Error; err != nil {
			t.Fatal(err)
		}
		if f.SignedSourceRows != 3 || f.SourceRows != 4 || f.SignedConsumeQuota != 1507 || f.SourceConsumeQuota != 1757 ||
			f.Occurrences != 1 || f.Source != usageFactRecentControlFindingSource || f.FirstDetectedAt != now.Unix() {
			t.Fatalf("finding wrong: %+v", f)
		}
		// Seen again: one row, counted twice.
		if _, err := m.verifyRecentUsageFactAuditByControl(context.Background(), claimFor(t), now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().First(&f, "user_id = ? AND date_ts = ?", uid, day).Error; err != nil {
			t.Fatal(err)
		}
		if f.Occurrences != 2 || f.FirstDetectedAt != now.Unix() || f.LastDetectedAt != now.Add(time.Hour).Unix() {
			t.Fatalf("repeat detection wrong: %+v", f)
		}
	})

	t.Run("no signed proof or another epoch falls through untouched", func(t *testing.T) {
		claim := claimFor(t)
		claim.From, claim.Through = day+usageFactDaySeconds, day+2*usageFactDaySeconds
		if handled, err := m.verifyRecentUsageFactAuditByControl(context.Background(), claim, now); handled || err != nil {
			t.Fatalf("no proof: handled=%v err=%v", handled, err)
		}
		claim = claimFor(t)
		claim.Jobs[0].SourceEpoch = "other"
		if handled, err := m.verifyRecentUsageFactAuditByControl(context.Background(), claim, now); handled || err != nil {
			t.Fatalf("other epoch: handled=%v err=%v", handled, err)
		}
		claim = claimFor(t)
		claim.Jobs[0].Kind = usageFactHistoryKindSourceAudit
		if handled, err := m.verifyRecentUsageFactAuditByControl(context.Background(), claim, now); handled || err != nil {
			t.Fatalf("full source audit must keep raw pages: handled=%v err=%v", handled, err)
		}
	})

	t.Run("a busy source releases the claim without an attempt", func(t *testing.T) {
		claim := claimFor(t)
		busy := func(context.Context, int64, int64, int64) (usageFactHistoryControl, error) {
			return usageFactHistoryControl{}, errUsageFactSourceBusy
		}
		handled, err := m.verifyRecentUsageFactAuditByControlWith(context.Background(), claim, now, busy)
		if !handled || !errors.Is(err, errUsageFactSourceBusy) {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
		j := loadJob(t)
		if j.Status != usageFactHistoryJobQueued || j.LeaseOwner != "" || j.Attempts != 0 || j.NextHour != day {
			t.Fatalf("busy release wrong: %+v", j)
		}
	})
}
