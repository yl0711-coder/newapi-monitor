package monitor

import (
	"context"
	"testing"
	"time"
)

func TestECSLogArchiveHealthNeverInventsFinalCoverage(t *testing.T) {
	m := newECSLogTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	if m.ecsArchiveHealth(ctx, now) != nil {
		t.Fatal("disabled archive performed status work")
	}
	m.cfg.ECSArchiveEnabled = true
	if h := m.ecsArchiveHealth(ctx, now); h.Status != "awaiting_first_scan" || h.CoverageStatus != "awaiting_final_boundary" {
		t.Fatalf("unknown scan green %+v", h)
	}
	if err := m.storeDB.Create(&ECSLogArchiveScan{ID: 1, Binding: "fixture", LastSuccess: now}).Error; err != nil {
		t.Fatal(err)
	}
	for _, status := range []int{200, 425, 409, 410} {
		if err := m.storeDB.Create(&ECSLogArchiveReceipt{ObjectKey: time.Unix(int64(status), 0).String(), Status: status}).Error; err != nil {
			t.Fatal(err)
		}
	}
	h := m.ecsArchiveHealth(ctx, now)
	if h.Status != "recovery_pending" || h.Accepted != 1 || h.Pending != 1 || h.Conflicts != 1 || h.Rejected != 1 || h.CoverageStatus != "awaiting_final_boundary" {
		t.Fatalf("incomplete delivery hidden %+v", h)
	}
	if err := m.storeDB.Where("status <> 200").Delete(&ECSLogArchiveReceipt{}).Error; err != nil {
		t.Fatal(err)
	}
	h = m.ecsArchiveHealth(ctx, now)
	if h.Status != "scanned" || h.CoverageStatus != "awaiting_final_boundary" {
		t.Fatalf("successful replay fabricated producer EOF %+v", h)
	}
	if err := m.storeDB.Model(&ECSLogArchiveScan{}).Where("id = 1").Update("last_error", "archive_list_unavailable").Error; err != nil {
		t.Fatal(err)
	}
	if h = m.ecsArchiveHealth(ctx, now); h.Status != "scan_failed" {
		t.Fatalf("AWS failure hidden %+v", h)
	}
	if err := m.storeDB.Create(&ECSLogArchiveReceipt{ObjectKey: "pending-during-outage", Status: 425}).Error; err != nil {
		t.Fatal(err)
	}
	if h = m.ecsArchiveHealth(ctx, now); h.Status != "scan_failed" || h.Pending != 1 || h.LastError == "" {
		t.Fatalf("pending count masks scanner failure: %+v", h)
	}
}

func TestECSLogArchiveProgressDoesNotInventAnOutage(t *testing.T) {
	m := newECSLogTestMonitor(t)
	m.cfg.ECSArchiveEnabled = true
	now := time.Now().Unix()
	if err := m.storeDB.Create(&ECSLogArchiveScan{ID: 1, Binding: "fixture", LastProgress: now, NextToken: "next-page"}).Error; err != nil {
		t.Fatal(err)
	}
	for _, initial := range []string{"reporting", "degraded"} {
		h := &ecsLogHealth{Status: initial}
		m.attachECSArchiveHealth(context.Background(), h, now)
		if h.Status != initial || h.Archive.Status != "scan_progressing" || h.Archive.CoverageStatus != "awaiting_final_boundary" {
			t.Fatalf("normal progress changed existing health or invented coverage: %+v", h)
		}
	}
	h := &ecsLogHealth{Status: "reporting"}
	m.attachECSArchiveHealth(context.Background(), h, now+int64(3*ecsArchivePollInterval/time.Second)+1)
	if h.Status != "degraded" || h.Archive.Status != "scan_stale" {
		t.Fatalf("stalled pagination hidden: %+v", h)
	}
}
