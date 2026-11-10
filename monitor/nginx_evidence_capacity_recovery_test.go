package monitor

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestNginxEvidenceFullThenExpandedRetriesWithoutDuplicateOrFalseCoverage(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "evidence.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &CloudWatchNginxEvidenceCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	if err := configureNginxEvidencePageLimit(db, 1); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{nginxEvidenceDB: db, cfg: Settings{NginxEvidenceMode: "verified", NginxEvidenceRetentionHours: 168}}
	from := time.Now().Unix()/60*60 - 1200
	makeRows := func(window int) []cloudWatchStructuredEvidence {
		rows := make([]cloudWatchStructuredEvidence, 2500)
		for i := range rows {
			status, ms := 502, int64(10)
			rows[i] = cloudWatchStructuredEvidence{Kind: cwEvidenceNginxAccess, EventRef: fmt.Sprintf("%064x", window*10000+i+1), EventMS: (from + int64(window)*60 + int64(i%60)) * 1000, HMACKeyID: "cw-1", NginxIDHMAC: fmt.Sprintf("%064x", window*10000+i+100000), OneAPIIDHMAC: fmt.Sprintf("%064x", window*10000+i+200000), Method: "POST", Route: "/v1/responses", Status: &status, RequestMS: &ms, UpstreamMS: &ms, UpstreamStatuses: []int{502}, Completion: "complete_at_edge"}
		}
		return rows
	}
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), makeRows(0), from, from+60, "full"); err == nil || !strings.Contains(err.Error(), "full") {
		t.Fatalf("expected real SQLITE_FULL, got %v", err)
	}
	var proof CloudWatchNginxEvidenceCheckpoint
	if err := db.First(&proof).Error; err != nil {
		t.Fatal(err)
	}
	if proof.ThroughTs > from || proof.PendingFromTs != from || proof.PendingToTs != from+60 {
		t.Fatalf("failed interval was certified: %+v", proof)
	}
	// Expanding capacity keeps the existing data and resumes the original window.
	if err := configureNginxEvidencePageLimit(db, 8); err != nil {
		t.Fatal(err)
	}
	for window := 0; window < 3; window++ {
		start := from + int64(window)*60
		for retry := 0; retry < 2; retry++ {
			if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), makeRows(window), start, start+60, "retry"); err != nil {
				t.Fatal(err)
			}
			var count int64
			if err := db.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			if count != int64((window+1)*2500) {
				t.Fatalf("window=%d retry=%d count=%d", window, retry, count)
			}
		}
		if err := db.First(&proof).Error; err != nil {
			t.Fatal(err)
		}
		if proof.ThroughTs != start+60 || proof.PendingFromTs != 0 {
			t.Fatalf("recovered window not complete: %+v", proof)
		}
	}
	var latest NginxRequestEvidence
	if err := db.First(&latest, "event_id = ?", makeRows(2)[0].EventRef).Error; err != nil {
		t.Fatal(err)
	}
	if latest.Status != 502 {
		t.Fatal("new evidence is not readable")
	}
}
