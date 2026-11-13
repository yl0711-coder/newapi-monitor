package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenDetectionActiveReferenceIndexPreservesLegacyAndTransactionalCounts(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	db := m.tokenStore()
	// Match the measured 70-MB reference payload without copying the 19-GB
	// runtime database. NULL, tombstone and invalid snapshots exercise the
	// original count semantics; stored_bytes is deliberately not a proxy.
	payload := `{"evidence":"` + strings.Repeat("x", 64<<10) + `"}`
	for start := 0; start < 1024; start += 32 {
		rows := make([]TokenDetectionReference, 0, 32)
		for i := start; i < start+32; i++ {
			rows = append(rows, TokenDetectionReference{ID: fmt.Sprintf("active-%04d", i), ExpiresAt: now + 60, SnapshotJSON: payload, StoredBytes: int64(len(payload))})
		}
		if err := db.Create(&rows).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec(`INSERT INTO token_detection_references(id,expires_at,snapshot_json,invalid_reason,stored_bytes) VALUES
		('null-reason',?,'{}',NULL,0),('null-snapshot',?,NULL,'',0),
		('tombstone',?,'','',7),('invalid',?,'{}','invalid',2),('expired',?,'{}','',2)`, now+60, now+60, now+60, now+60, now).Error; err != nil {
		t.Fatal(err)
	}
	count := func(reader *gorm.DB, at int64, want int64) time.Duration {
		t.Helper()
		started := time.Now()
		var got int64
		if err := tokenDetectionActiveReferences(reader, at).Count(&got).Error; err != nil || got != want {
			t.Fatal("active reference count changed", got, want, err)
		}
		return time.Since(started)
	}
	legacy := count(m.tokenReadStore(), now, 1025)
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	indexed := count(m.tokenReadStore(), now, 1025)
	var n int64
	query := tokenDetectionActiveReferences(db.Session(&gorm.Session{DryRun: true}), now).Count(&n)
	var plan []struct{ Detail string }
	if err := db.Raw("EXPLAIN QUERY PLAN "+query.Statement.SQL.String(), query.Statement.Vars...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	if len(plan) != 1 || !strings.Contains(plan[0].Detail, "idx_token_detection_active_reference_v1") || !strings.Contains(plan[0].Detail, "expires_at>?") {
		t.Fatal("active count rereads frozen snapshot payloads", plan)
	}
	rollback := errors.New("reference state rollback")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&TokenDetectionReference{}).Where("id='active-0000'").Update("invalid_reason", "invalid").Error; err != nil {
			return err
		}
		if err := tx.Model(&TokenDetectionReference{}).Where("id='active-0001'").Update("snapshot_json", "").Error; err != nil {
			return err
		}
		count(tx, now, 1023)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	count(db, now, 1025)
	count(db, now+60, 0)
	// A read-only legacy file has no migration permission and must still count
	// exactly. The shared query must not require an INDEXED BY hint.
	if err := db.Exec("DROP INDEX idx_token_detection_active_reference_v1").Error; err != nil {
		t.Fatal(err)
	}
	count(m.tokenReadStore(), now, 1025)
	t.Logf("reference_payload_bytes=%d legacy_count=%s indexed_count=%s plan=%+v", 1024*len(payload), legacy, indexed, plan)
}
