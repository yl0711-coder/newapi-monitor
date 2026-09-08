package monitor

import (
	"testing"
	"time"
)

func TestStabilityRetentionPreservesUnfinishedBackfillJobs(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, cstLocation).Unix()
	cutoff := stabilityRetentionCutoff(now, 181)
	jobCutoff := cutoff - 30*86400
	for _, status := range []string{"paused", "complete", "queued", "running", "partial"} {
		for _, age := range []struct {
			name string
			ts   int64
		}{{"old", jobCutoff - 1}, {"boundary", jobCutoff}, {"recent", now}} {
			job := StabilityBackfillJob{ID: status + "-" + age.name, Status: status, UpdatedAt: age.ts, FromTs: now - 48*3600, ToTs: now - 24*3600}
			if err := m.storeDB.Create(&job).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.pruneStabilityOlderThan(cutoff); err != nil {
		t.Fatal(err)
	}
	var jobs []StabilityBackfillJob
	if err := m.storeDB.Find(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 14 {
		t.Fatalf("retained %d jobs, want 14", len(jobs))
	}
	for _, job := range jobs {
		if job.ID == "complete-old" {
			t.Fatal("expired completed audit was not pruned")
		}
	}
}
