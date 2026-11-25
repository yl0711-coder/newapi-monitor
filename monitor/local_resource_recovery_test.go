package monitor

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"
)

func TestLocalResourceRecoveryClassificationAndUpstreamBudget(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		wait bool
	}{
		{"disk full", fmt.Errorf("commit: %w", syscall.ENOSPC), true},
		{"quota", syscall.EDQUOT, true},
		{"capacity gate", errAICodeWithRecordCapacity, true},
		{"permission", syscall.EACCES, false},
		{"io error", syscall.EIO, false},
		{"remote full text", errors.New("upstream: database or disk is full"), false},
		{"hash mismatch", errors.New("content hash mismatch"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := localResourceTemporarilyUnavailable(tc.err); got != tc.wait {
				t.Fatalf("resource wait=%v", got)
			}
			row := ChannelUpstreamAccount{Domain: "test.invalid", UsageBackfillConsecutiveFails: 4}
			applyUpstreamUsageBackfillResult(&row, tc.err, 1000, Settings{})
			if tc.wait && (row.UsageBackfillConsecutiveFails != 4 || row.UsageBackfillNextSyncAt <= 1000 || row.UsageBackfillNextSyncAt == upstreamAccountIsolatedUntil) {
				t.Fatalf("local wait charged upstream budget: %+v", row)
			}
		})
	}
}

func TestUsageLocalAuditDeadlineDoesNotChangeSourceFailurePolicy(t *testing.T) {
	if !usageFactLocalAuditResourceWait(context.DeadlineExceeded) || localResourceTemporarilyUnavailable(context.DeadlineExceeded) {
		t.Fatal("deadline classification escaped its local-only boundary")
	}
	for _, err := range []error{context.Canceled, syscall.EIO, syscall.EACCES, errUsageMemberControlIntegrity} {
		if usageFactLocalAuditResourceWait(err) {
			t.Fatal("permanent failure or shutdown treated as local audit wait", err)
		}
	}
	row := ChannelUpstreamAccount{Domain: "test.invalid", UsageBackfillConsecutiveFails: 4}
	applyUpstreamUsageBackfillResult(&row, context.DeadlineExceeded, 1000, Settings{})
	if row.UsageBackfillConsecutiveFails != 5 {
		t.Fatalf("real source timeout stopped consuming its bounded budget: %+v", row)
	}
}

func TestUsageHistoryResourceWaitDoesNotExhaustAttempts(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	db := m.usageFactsStore()
	now := time.Now()
	job := UsageFactJob{ID: "resource-wait", Kind: usageFactHistoryKindTail, Status: usageFactHistoryJobRunning, LeaseOwner: "test", NextHour: 123, Attempts: 4}
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if err := db.Model(&UsageFactJob{}).Where("id = ?", job.ID).Updates(map[string]any{"status": usageFactHistoryJobRunning, "lease_owner": "test"}).Error; err != nil {
			t.Fatal(err)
		}
		claim := usageFactHistoryClaim{LeaseOwner: "test", Jobs: []UsageFactJob{job}}
		if err := m.releaseUsageFactHistoryClaim(context.Background(), claim, syscall.ENOSPC, now, false); err != nil {
			t.Fatal(err)
		}
		if err := db.First(&job, "id = ?", job.ID).Error; err != nil {
			t.Fatal(err)
		}
		if job.Status != usageFactHistoryJobQueued || job.Attempts != 4 || job.NextHour != 123 || job.NextRetryAt <= now.Unix() || job.LeaseOwner != "" {
			t.Fatalf("resource wait lost cursor/budget: %+v", job)
		}
	}
	// A real semantic failure must retain the original bounded pause behavior.
	if err := db.Model(&UsageFactJob{}).Where("id = ?", job.ID).Update("lease_owner", "test").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.releaseUsageFactHistoryClaim(context.Background(), usageFactHistoryClaim{LeaseOwner: "test", Jobs: []UsageFactJob{job}}, errors.New("hash mismatch"), now, false); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != usageFactHistoryJobPaused || job.Attempts != 5 {
		t.Fatalf("integrity failure was auto-retried: %+v", job)
	}
}

func TestLocalResourceRecoveryRecognizesRealSQLiteFull(t *testing.T) {
	m := newTestMonitor(t)
	db := m.storeDB
	if err := db.Exec("CREATE TABLE resource_full_test(payload BLOB)").Error; err != nil {
		t.Fatal(err)
	}
	var pages int64
	if err := db.Raw("PRAGMA page_count").Scan(&pages).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages+1)).Error; err != nil {
		t.Fatal(err)
	}
	err := db.Exec("INSERT INTO resource_full_test VALUES (zeroblob(131072))").Error
	if err == nil || !localResourceTemporarilyUnavailable(err) {
		t.Fatalf("expected typed SQLITE_FULL: %v", err)
	}
	if err := db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages+64)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO resource_full_test VALUES (zeroblob(131072))").Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Raw("SELECT count(*) FROM resource_full_test").Scan(&count).Error; err != nil || count != 1 {
		t.Fatalf("failed write leaked or retry lost: count=%d err=%v", count, err)
	}
}
