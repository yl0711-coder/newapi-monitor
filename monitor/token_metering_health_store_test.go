package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"gorm.io/gorm"
)

func TestTokenHealthUpgradeReusesCompatibleRetainedLedger(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if err := initTokenMeteringHealthCounts(m.storeDB); err != nil {
		t.Fatal(err)
	}
	// v157 already publishes these exact dimensions atomically under v1.
	// A trigger/rebuild implementation upgrade must not invalidate that data.
	if err := m.storeDB.Model(&tokenMeteringHealthCount{}).Where("dimension='schema'").Update("value", "v1").Error; err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"CREATE TABLE retained_health_writes (n INTEGER)",
		"CREATE TRIGGER retained_health_delete AFTER DELETE ON token_metering_health_counts BEGIN INSERT INTO retained_health_writes VALUES(1); END",
		"CREATE TRIGGER retained_health_update AFTER UPDATE ON token_metering_health_counts BEGIN INSERT INTO retained_health_writes VALUES(1); END",
		"CREATE TRIGGER retained_health_insert AFTER INSERT ON token_metering_health_counts BEGIN INSERT INTO retained_health_writes VALUES(1); END",
	} {
		if err := m.storeDB.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := ensureTokenMeteringHealthCountsSchema(m.storeDB); err != nil {
		t.Fatal(err)
	}
	if done, err := m.maintainTokenMeteringHealthCounts(context.Background(), false); err != nil || !done {
		t.Fatal("compatible retained counts became unavailable", done, err)
	}
	var writes int64
	if err := m.storeDB.Table("retained_health_writes").Count(&writes).Error; err != nil || writes != 0 {
		t.Fatal("compatible upgrade rewrote the retained ledger", writes, err)
	}
	assertTokenHealthCounts(t, m, now)
	if h, err := m.tokenMeteringHealth(context.Background(), "", now); err != nil || !h.Available {
		t.Fatal("compatible upgrade hid ready health", h, err)
	}
}

func TestTokenHealthRebuildResumesWithConcurrentFactChanges(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	rows := make([]TokenMeteringFact, 299)
	for i := range rows {
		rows[i] = fact
		rows[i].LogID = int64(i + 2)
	}
	if err := m.storeDB.CreateInBatches(rows, 64).Error; err != nil {
		t.Fatal(err)
	}
	if err := ensureTokenMeteringHealthCountsSchema(m.storeDB); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if done, err := m.maintainTokenMeteringHealthCounts(ctx, false); err != nil || done {
		t.Fatal("first bounded page", done, err)
	}
	var progress tokenMeteringHealthRebuild
	if err := m.storeDB.First(&progress).Error; err != nil || progress.CursorID != 128 || !progress.Active {
		t.Fatal(progress, err)
	}
	h, err := m.tokenMeteringHealth(ctx, "", now)
	if err != nil || h.Available || h.HealthCountsState != "rebuilding" {
		t.Fatal("partial ledger published", h, err)
	}
	body, _ := json.Marshal(h)
	var wire map[string]any
	if err := json.Unmarshal(body, &wire); err != nil || wire["facts"] != nil || wire["pending_links"] != nil {
		t.Fatal("unknown counts became zero", wire, err)
	}
	// Update/delete both sides of the durable boundary, insert behind and
	// ahead, and move an already-counted identity into the unscanned suffix.
	for _, query := range []string{
		"DELETE FROM token_metering_facts WHERE log_id IN (1,200)",
		"UPDATE token_metering_facts SET link_next_at=99,link_state='waiting',stored_bytes=stored_bytes+17 WHERE log_id IN (2,201)",
		"UPDATE token_metering_facts SET log_id=600 WHERE log_id=3",
	} {
		if err := m.storeDB.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{1, 1000} {
		f := fact
		f.LogID = id
		if err := m.storeDB.Create(&f).Error; err != nil {
			t.Fatal(err)
		}
	}
	rollback := errors.New("rollback rebuild page")
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if _, err := rebuildTokenMeteringHealthCountsBatch(tx, false); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	var unchanged tokenMeteringHealthRebuild
	if err := m.storeDB.First(&unchanged).Error; err != nil || unchanged != progress {
		t.Fatal("rollback lost cursor", unchanged, err)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB}
	for turn := 0; turn < 5; turn++ {
		done, err := restarted.maintainTokenMeteringHealthCounts(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			assertTokenHealthCounts(t, m, now)
			return
		}
	}
	t.Fatal("rebuild failed to finish from durable prefix")
}

func TestTokenHealthMaintenanceRepairsReadyDriftAndYieldsWriter(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if err := initTokenMeteringHealthCounts(m.storeDB); err != nil {
		t.Fatal(err)
	}
	// Removing a whole field dimension must not pass an empty-map sum check.
	if err := m.storeDB.Where("dimension=?", "field:quota").Delete(&tokenMeteringHealthCount{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.tokenMeteringHealth(context.Background(), "", now); !errors.Is(err, errTokenHealthCountsInconsistent) {
		t.Fatal(err)
	}
	m.tokenLocalWriteMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err := m.maintainTokenMeteringHealthCounts(ctx, false)
	cancel()
	m.tokenLocalWriteMu.Unlock()
	var wait *tokenLocalWaitError
	if !errors.As(err, &wait) {
		t.Fatal("rebuild bypassed writer", err)
	}
	if done, err := m.maintainTokenMeteringHealthCounts(context.Background(), false); err != nil || !done {
		t.Fatal(done, err)
	}
	assertTokenHealthCounts(t, m, now)
	// Entirely missing counts look like a consistent zero population unless
	// the periodic probe compares the identity count independently.
	if err := m.storeDB.Where("dimension<>'schema'").Delete(&tokenMeteringHealthCount{}).Error; err != nil {
		t.Fatal(err)
	}
	if h, err := m.tokenMeteringHealth(context.Background(), "", now); !errors.Is(err, errTokenHealthCountsInconsistent) || h.Available {
		t.Fatal("missing ledger published a healthy zero", h, err)
	}
	if done, err := m.maintainTokenMeteringHealthCounts(context.Background(), false); err != nil || !done {
		t.Fatal(done, err)
	}
	assertTokenHealthCounts(t, m, now)
}

func assertTokenHealthCounts(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	h, err := m.tokenMeteringHealth(context.Background(), m.cfg.TokenFactsSourceEpoch, now)
	if err != nil {
		t.Fatal(err)
	}
	db := m.tokenStore()
	var independent struct{ Facts, Bytes, Pending, Closed int64 }
	if err := db.Raw(`SELECT COUNT(*) AS facts,COALESCE(SUM(stored_bytes),0) AS bytes,COALESCE(SUM(link_next_at>0),0) AS pending,COALESCE(SUM(link_next_at=0),0) AS closed FROM token_metering_facts WHERE source_epoch=?`, m.cfg.TokenFactsSourceEpoch).Scan(&independent).Error; err != nil {
		t.Fatal(err)
	}
	if h.Facts != independent.Facts || h.StoredBytes != independent.Bytes || h.PendingLinks != independent.Pending || h.ClosedLinkReviews != independent.Closed {
		t.Fatalf("health counters disagree: %+v %+v", h, independent)
	}
	if h.UnresolvedLinks+h.LinkStates["unique"] != h.Facts || h.PendingLinksBasis != "scheduled_initial_or_late_evidence_review" {
		t.Fatal("scheduled review confused with unresolved evidence", h)
	}
	for column, got := range map[string]map[string]int64{"link_state": h.LinkStates, "link_reason": h.LinkReasons, "length_state": h.LengthStates, "semantic_reason": h.SemanticReasons, "json_extract(source_facts, '$.pricing_support')": h.PricingSupport, "json_extract(source_facts, '$.other_state')": h.OtherStates} {
		want := map[string]int64{}
		if err := tokenMeteringHealthGroups(db, m.cfg.TokenFactsSourceEpoch, column, want); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("health dimension %s: got=%+v want=%+v", column, got, want)
		}
	}
	fields, err := tokenMeteringHealthFields(db, m.cfg.TokenFactsSourceEpoch)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range fields {
		got := h.FieldStates[name]
		if len(want) == 0 && len(got) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("field %s: %+v %+v", name, got, want)
		}
	}
}

func TestTokenMeteringHealthCountsExactForBackfillUpdateRollbackCleanupAndRestart(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if err := initTokenMeteringHealthCounts(m.storeDB); err != nil {
		t.Fatal(err)
	}
	assertTokenHealthCounts(t, m, now)
	second := fact
	second.LogID = 2
	if err := m.storeDB.Create(&second).Error; err != nil {
		t.Fatal(err)
	}
	assertTokenHealthCounts(t, m, now)
	u := tokenMeteringDecode(t, fact)
	u.Fields["completion_tokens"] = metering.Field{State: "missing"}
	if err := m.storeDB.Model(&fact).Updates(map[string]any{"source_facts": tokenDetectionJSON(u), "stored_bytes": fact.StoredBytes + 17, "link_state": "waiting", "link_reason": "evidence_pending", "link_next_at": now + 60}).Error; err != nil {
		t.Fatal(err)
	}
	assertTokenHealthCounts(t, m, now)
	rollback := errors.New("fixture rollback")
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&second).Error; err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	assertTokenHealthCounts(t, m, now)
	if err := m.storeDB.Delete(&second).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenMeteringHealthCounts(m.storeDB); err != nil {
		t.Fatal(err)
	}
	assertTokenHealthCounts(t, m, now)
	if err := m.storeDB.Delete(&fact).Error; err != nil {
		t.Fatal(err)
	}
	assertTokenHealthCounts(t, m, now)
}

func TestTokenMeteringHealthRetryDoesNotRewriteUnchangedCounters(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	fact.LinkNextAt = now + 60
	if err := m.storeDB.Save(&fact).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenMeteringHealthCounts(m.storeDB); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"CREATE TABLE health_write_audit (n INTEGER)",
		"CREATE TRIGGER health_audit_insert AFTER INSERT ON token_metering_health_counts BEGIN INSERT INTO health_write_audit VALUES(1); END",
		"CREATE TRIGGER health_audit_update AFTER UPDATE ON token_metering_health_counts BEGIN INSERT INTO health_write_audit VALUES(1); END",
	} {
		if err := m.storeDB.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&fact).Updates(map[string]any{"link_next_at": now + 120, "stored_bytes": fact.StoredBytes, "link_reason": fact.LinkReason}).Error; err != nil {
		t.Fatal(err)
	}
	var writes int64
	if err := m.storeDB.Table("health_write_audit").Count(&writes).Error; err != nil || writes != 0 {
		t.Fatalf("unchanged association caused %d health writes: %v", writes, err)
	}
	if err := m.storeDB.Model(&fact).Update("link_next_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Table("health_write_audit").Count(&writes).Error; err != nil || writes != 4 {
		t.Fatalf("closing review should change only pending/closed counters: %d %v", writes, err)
	}
	assertTokenHealthCounts(t, m, now)
}
