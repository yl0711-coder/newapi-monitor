package monitor

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestTokenEvidenceChangeBatchesWakeCommits(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	if err := initNginxEvidenceChanges(m.nginxEvidenceDB); err != nil {
		t.Fatal(err)
	}
	if _, err := m.refreshTokenMeteringEvidence(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 32; id++ {
		f, _ := parseTokenMetering(m.cfg, tokenMeteringRawFixture(`{}`), now)
		f.LogID = id
		f.RequestID = fmt.Sprintf("batch-evidence-%d", id)
		f.LinkNextAt, f.LinkDeadline = 0, now-1
		if err := m.storeDB.Create(&f).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.nginxEvidenceDB.Create(&NginxRequestEvidence{EventID: fmt.Sprintf("batch-event-%d", id), OneAPIRequestID: f.RequestID, EventMS: now * 1000}).Error; err != nil {
			t.Fatal(err)
		}
	}
	commits := 0
	if err := m.storeDB.Callback().Update().After("gorm:update").Register("evidence_cursor_budget", func(tx *gorm.DB) {
		if tx.Statement.Table == tokenEvidenceCursorTable {
			commits++
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer m.storeDB.Callback().Update().Remove("evidence_cursor_budget")
	if _, err := m.refreshTokenMeteringEvidence(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var woken int64
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("link_next_at=?", now).Count(&woken).Error; err != nil || woken != 32 || commits > 2 {
		t.Fatalf("woken=%d cursor commits=%d error=%v", woken, commits, err)
	}
	t.Logf("32 exact owners awakened with %d cursor commit", commits)
}

func TestTokenEvidenceChangeReopensExpiredLinkAndDetection(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 1, now-100, 1, 20000, 0, 0)
	if err := m.storeDB.Model(&f).Updates(map[string]any{"link_state": "expired", "link_next_at": 0, "link_deadline": now - 1, "link_attempts": 24, "evidence_json": "[]", "request_length": nil, "length_state": "missing"}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	before, _ := detectionHead(t, m, 1)
	if err := initNginxEvidenceChanges(m.nginxEvidenceDB); err != nil {
		t.Fatal(err)
	}
	if _, err := m.refreshTokenMeteringEvidence(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	tokenMeteringEvidence(t, m, "late-entry", f.CreatedAt, 1200, f.RequestID)
	if n, _, err := m.linkTokenMeteringBatchProgress(context.Background(), now+1); err != nil || n != 1 {
		t.Fatalf("late evidence did not wake closed association: %d %v", n, err)
	}
	after := tokenMeteringFactRead(t, m, 1)
	if after.LinkState != "unique" || after.LinkRevision <= f.LinkRevision || after.RequestLength == nil || *after.RequestLength != 1200 {
		t.Fatalf("late association not revised: %+v", after)
	}
	detectionRun(t, m, now+2)
	head, _ := detectionHead(t, m, 1)
	if head.Revision <= before.Revision || head.LinkRevision != after.LinkRevision {
		t.Fatal("late evidence did not requeue detection")
	}
	// Another update to the same event must move its durable sequence forward.
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id=1").Update("link_next_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("event_id=?", "late-entry").Update("request_length", 1500).Error; err != nil {
		t.Fatal(err)
	}
	// A fresh Monitor instance uses the persisted cursor, not any wake channel.
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, nginxEvidenceDB: m.nginxEvidenceDB}
	if n, _, err := restarted.linkTokenMeteringBatchProgress(context.Background(), now+3); err != nil || n != 1 {
		t.Fatalf("updated event did not resume: %d %v", n, err)
	}
	if got := tokenMeteringFactRead(t, m, 1); got.LinkRevision <= after.LinkRevision || got.RequestLength == nil || *got.RequestLength != 1500 {
		t.Fatal("enriched event did not replace link revision")
	}
}

func TestTokenEvidenceChangeCursorCommitsOnlyScheduledPrefix(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	if err := initNginxEvidenceChanges(m.nginxEvidenceDB); err != nil {
		t.Fatal(err)
	}
	// Finish the one-time bootstrap before the incremental test event exists.
	if _, err := m.refreshTokenMeteringEvidence(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 70; id++ {
		f := TokenMeteringFact{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, CreatedAt: now - 100, RequestID: "shared-request", LinkState: "expired"}
		if err := m.storeDB.Create(&f).Error; err != nil {
			t.Fatal(err)
		}
	}
	e := NginxRequestEvidence{EventID: "raw-entry", EventMS: (now - 100) * 1000, OneAPIRequestID: "shared-request", Route: "/v1/responses", Method: "POST"}
	if err := m.nginxEvidenceDB.Create(&e).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER fail_evidence_wake BEFORE UPDATE OF link_next_at ON token_metering_facts BEGIN SELECT RAISE(ABORT,'wake_fixture'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.refreshTokenMeteringEvidence(context.Background(), now); err == nil {
		t.Fatal("failed fact scheduling was treated as acknowledged evidence")
	}
	var cursor tokenEvidenceCursor
	if err := m.storeDB.Table(tokenEvidenceCursorTable).Take(&cursor).Error; err != nil || cursor.Generation != 0 || cursor.OwnerID != 0 {
		t.Fatal("failed transaction advanced cursor", cursor, err)
	}
	if err := m.storeDB.Exec(`DROP TRIGGER fail_evidence_wake`).Error; err != nil {
		t.Fatal(err)
	}
	for round, want := range []int64{32, 64, 70} {
		restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, nginxEvidenceDB: m.nginxEvidenceDB}
		if _, err := restarted.refreshTokenMeteringEvidence(context.Background(), now+int64(round)); err != nil {
			t.Fatal(err)
		}
		var queued int64
		if err := m.storeDB.Model(&TokenMeteringFact{}).Where("link_next_at>0").Count(&queued).Error; err != nil || queued != want {
			t.Fatalf("prefix after restart: got=%d want=%d err=%v", queued, want, err)
		}
	}
	if err := m.storeDB.Table(tokenEvidenceCursorTable).Take(&cursor).Error; err != nil || cursor.Generation != 1 || cursor.OwnerID != 0 {
		t.Fatal("completed prefix not acknowledged", cursor, err)
	}
}

func TestTokenEvidenceChangeBootstrapNoopAndRetention(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	f := TokenMeteringFact{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 1, RequestID: "old-request", LinkState: "expired"}
	if err := m.storeDB.Create(&f).Error; err != nil {
		t.Fatal(err)
	}
	e := NginxRequestEvidence{EventID: "existing-entry", EventMS: now * 1000, OneAPIRequestID: f.RequestID, Route: "/v1/responses", Method: "POST"}
	if err := m.nginxEvidenceDB.Create(&e).Error; err != nil {
		t.Fatal(err)
	}
	if err := initNginxEvidenceChanges(m.nginxEvidenceDB); err != nil {
		t.Fatal(err)
	}
	if _, err := m.refreshTokenMeteringEvidence(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got := tokenMeteringFactRead(t, m, 1); got.LinkNextAt != now {
		t.Fatal("existing evidence missed by bounded bootstrap")
	}
	for _, length := range []int64{100, 200, 200} {
		if err := m.nginxEvidenceDB.Model(&e).Update("request_length", length).Error; err != nil {
			t.Fatal(err)
		}
	}
	var rows, generation int64
	if err := m.nginxEvidenceDB.Table("nginx_evidence_changes_v1").Count(&rows).Error; err != nil || rows != 1 {
		t.Fatal("updates appended unbounded change rows", rows, err)
	}
	if err := m.nginxEvidenceDB.Table("nginx_evidence_change_clock_v1").Select("generation").Scan(&generation).Error; err != nil || generation != 2 {
		t.Fatal("no-op changed evidence generation", generation, err)
	}
	if err := m.nginxEvidenceDB.Delete(&e).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.nginxEvidenceDB.Table("nginx_evidence_changes_v1").Count(&rows).Error; err != nil || rows != 0 {
		t.Fatal("expired evidence left orphan change rows", rows, err)
	}
}

func TestTokenEvidenceChangeUsesExactOwnerIndexes(t *testing.T) {
	m, _, _ := tokenMeteringFixture(t)
	defer m.Close()
	var plans []struct{ Detail string }
	err := m.storeDB.Raw(`EXPLAIN QUERY PLAN SELECT log_id FROM token_metering_facts WHERE request_id=? AND source_epoch=? AND log_id>? ORDER BY log_id LIMIT 33`, "fixture", m.cfg.TokenFactsSourceEpoch, 0).Scan(&plans).Error
	if err != nil {
		t.Fatal(err)
	}
	if text := fmt.Sprint(plans); !strings.Contains(text, "COVERING INDEX idx_token_fact_request_owner_v1") || !strings.Contains(text, "log_id>?") {
		t.Fatal("owner lookup lost bounded covering seek", text)
	}
}
