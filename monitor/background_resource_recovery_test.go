package monitor

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestIncidentLocalResourceWaitPreservesRetryBudget(t *testing.T) {
	_, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	row := IncidentIngestWindowState{DedupKey: "resource-test", WindowFromTs: 1000, WindowToTs: 1300, Attempts: ingest.MaxAttempts - 1}
	for i := 0; i < ingest.MaxAttempts+2; i++ {
		if err := m.markIncidentWindowFailure(context.Background(), row, syscall.ENOSPC, ingest, 2000); err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?", row.DedupKey, row.WindowToTs).First(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.ProcessState != incidentProcessFailed || row.Attempts != ingest.MaxAttempts-1 || row.NextRetryAt <= 2000 || row.DataComplete {
			t.Fatalf("resource wait consumed/approved window: %+v", row)
		}
	}
	// Non-resource engine/data failure retains the original permanent boundary.
	if err := m.markIncidentWindowFailure(context.Background(), row, errors.New("data invalid"), ingest, 2200); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?", row.DedupKey, row.WindowToTs).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.ProcessState != incidentProcessFailedPermanent {
		t.Fatalf("permanent data failure was retried: %+v", row)
	}
}

func TestStabilityBackfillResourceFailureResumesSameJob(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.StabilityBackfillSourceDutyPercent = 100
	base := time.Date(2026, 11, 24, 0, 0, 0, 0, cstLocation).Unix()
	job := StabilityBackfillJob{ID: "resource-resume", FromTs: base, ToTs: base + 7200, Status: "queued", TotalHours: 2, CurrentBatchHours: 1}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: base + 3600, Status: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("resource-test-full", func(tx *gorm.DB) {
		if tx.Statement.Table == "stability_hour_ingest_states" {
			tx.AddError(syscall.ENOSPC)
		}
	}); err != nil {
		t.Fatal(err)
	}
	var ranges [][2]int64
	fetch := func(_ context.Context, from, to int64) (stabilityRangeResult, error) {
		ranges = append(ranges, [2]int64{from, to})
		return stabilityRangeResult{Hours: map[int64]stabilityHourTraffic{}, SourceQueries: 1}, nil
	}
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfillWithFetcher(context.Background(), job.ID, fetch)
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "queued" {
		t.Fatalf("full disk terminally paused task: %+v", job)
	}
	if err := m.storeDB.Callback().Update().Remove("resource-test-full"); err != nil {
		t.Fatal(err)
	}
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfillWithFetcher(context.Background(), job.ID, fetch)
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "complete" || job.CompletedHours != 2 {
		t.Fatalf("same task failed to resume: %+v", job)
	}
	for _, r := range ranges {
		if r != [2]int64{base, base + 3600} {
			t.Fatalf("re-read already committed hour: %v", ranges)
		}
	}
	// Another replay must observe both committed proofs and do no source work.
	n := len(ranges)
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfillWithFetcher(context.Background(), job.ID, fetch)
	if len(ranges) != n {
		t.Fatal("completed task duplicated source collection")
	}
}

func TestStabilityResourceWaitClassification(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{syscall.ENOSPC, "queued"}, {syscall.EDQUOT, "queued"},
		{context.DeadlineExceeded, "queued"}, {syscall.EIO, "paused"},
		{errStabilityControlMismatch, "paused"}, {syscall.EACCES, "paused"},
	} {
		if got := stabilityLocalFailureStatus(tc.err); got != tc.want {
			t.Fatalf("%v: %s != %s", tc.err, got, tc.want)
		}
	}
}

func TestStabilityProblemMigrationLocalResourcePreservesSourceBudget(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	state := StabilityProblemClassificationMigration{ID: 1, TrafficClassVersion: stabilityTrafficClassificationVersion, FromTs: 1000, ThroughTs: 1600, NextTs: 1000, Status: "running", Attempts: 4, CurrentSpanMinutes: 3}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		_ = m.recordStabilityProblemMigrationFailure(&state, syscall.ENOSPC, 2000)
		if err := m.storeDB.First(&state, 1).Error; err != nil {
			t.Fatal(err)
		}
		if state.Attempts != 4 || state.Status == "paused" || state.NextRetryAt <= 2000 || state.NextTs != 1000 {
			t.Fatalf("local resource consumed source budget/cursor: %+v", state)
		}
	}
	_ = m.recordStabilityProblemMigrationFailure(&state, context.DeadlineExceeded, 2200)
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.Attempts != 5 || state.Status != "paused" {
		t.Fatal("real source timeout bypassed budget", state)
	}
}

func TestStabilityResourceWaitIsDueButManualPauseIsNot(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.sourceLifecycleInitialized.Store(true)
	// Block launch at the source boundary: queued state proves that the normal
	// resumer selected the resource wait; no source SQL or worker can run.
	base := time.Now().Truncate(time.Hour).Add(-3 * time.Hour).Unix()
	for _, status := range []string{"paused", "queued"} {
		job := StabilityBackfillJob{ID: status, FromTs: base, ToTs: base + 3600, Status: status, LastError: "disk pressure", UpdatedAt: time.Now().Add(-2 * time.Minute).Unix()}
		if err := m.storeDB.Create(&job).Error; err != nil {
			t.Fatal(err)
		}
	}
	m.resumeStabilityBackfill()
	var manual, resource StabilityBackfillJob
	if err := m.storeDB.First(&manual, "id = ?", "paused").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&resource, "id = ?", "queued").Error; err != nil {
		t.Fatal(err)
	}
	if manual.Status != "paused" || resource.Status != "queued" || resource.UpdatedAt < time.Now().Add(-time.Minute).Unix() {
		t.Fatalf("resumer changed wrong job: manual=%s resource=%s", manual.Status, resource.Status)
	}
}
