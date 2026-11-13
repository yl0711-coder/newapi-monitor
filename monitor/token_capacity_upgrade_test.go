package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestTokenCapacityUpgradeSuppressesUnchangedBytes(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	head := TokenDetectionCurrent{SourceEpoch: "upgrade", LogID: 1, ResultJSON: `{"x":1}`}
	rev := TokenDetectionRevision{SourceEpoch: "upgrade", LogID: 1, Revision: 1, FrozenJSON: "{}", StoredBytes: 42}
	ref := TokenDetectionReference{ID: "upgrade", SnapshotJSON: "{}", StoredBytes: 71}
	for _, row := range []any{&head, &rev, &ref} {
		if err := db.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Install the old unconditional UPDATE definitions on a populated store.
	for _, spec := range []struct{ table, value, ledger string }{
		{"token_detection_currents", "length(CAST(%s.result_json AS BLOB))", "head_bytes"},
		{"token_detection_revisions", "%s.stored_bytes", "revision_bytes"},
		{"token_detection_references", "%s.stored_bytes", "reference_bytes"},
	} {
		name := spec.table + "_capacity_UPDATE_v1"
		for _, sql := range []string{"DROP TRIGGER " + name, fmt.Sprintf("CREATE TRIGGER %s AFTER UPDATE ON %s BEGIN UPDATE token_detection_capacity SET %s=%s+(%s)-(%s) WHERE id=1; END", name, spec.table, spec.ledger, spec.ledger, fmt.Sprintf(spec.value, "NEW"), fmt.Sprintf(spec.value, "OLD"))} {
			if err := db.Exec(sql).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, sql := range []string{"CREATE TABLE capacity_audit (id INTEGER PRIMARY KEY)", "CREATE TRIGGER audit_capacity_update AFTER UPDATE ON token_detection_capacity BEGIN INSERT INTO capacity_audit VALUES(NULL); END"} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	restarted := &Monitor{storeDB: db}
	if err := restarted.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	var definitions []struct{ Name, SQL string }
	if err := db.Raw("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND name LIKE 'token_detection_%_capacity_%_v1'").Scan(&definitions).Error; err != nil || len(definitions) != 9 {
		t.Fatal("upgrade left missing or duplicate capacity triggers", definitions, err)
	}
	for _, definition := range definitions {
		if strings.Contains(definition.Name, "_UPDATE_") && (!strings.Contains(definition.SQL, "UPDATE OF ") || !strings.Contains(definition.SQL, " WHEN ")) {
			t.Fatal("old update trigger survived upgrade", definition)
		}
	}
	count := func(want int64) {
		t.Helper()
		var n int64
		if err := db.Table("capacity_audit").Count(&n).Error; err != nil || n != want {
			t.Fatal("unexpected capacity writes", n, want, err)
		}
		assertTokenCapacityLedger(t, db)
	}
	for _, sql := range []string{
		"UPDATE token_detection_currents SET next_at=60, evaluated_at=123, result_json=result_json",
		`UPDATE token_detection_currents SET result_json='{"x":2}'`,
		"UPDATE token_detection_revisions SET expires_at=123,stored_bytes=stored_bytes",
		"UPDATE token_detection_references SET expires_at=123,stored_bytes=stored_bytes",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	count(0)
	for _, sql := range []string{
		`UPDATE token_detection_currents SET result_json='{"x":"中文"}'`,
		"UPDATE token_detection_revisions SET stored_bytes=43",
		"UPDATE token_detection_references SET stored_bytes=70",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	count(3)
	rollback := errors.New("abort capacity transaction")
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("UPDATE token_detection_references SET stored_bytes=9000").Error; err != nil {
			return err
		}
		if err := tx.Delete(&head).Error; err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	count(3)
	for _, row := range []any{&head, &rev, &ref} {
		if err := db.Delete(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	count(6)
	if err := db.Create(&TokenDetectionCurrent{SourceEpoch: "upgrade", LogID: 2, ResultJSON: "{}"}).Error; err != nil {
		t.Fatal(err)
	}
	count(7)
}

func TestTokenCapacityUpgradeRollsBackDDL(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	read := func() string {
		var sql string
		if err := db.Raw("SELECT sql FROM sqlite_master WHERE name='token_detection_currents_capacity_UPDATE_v1'").Scan(&sql).Error; err != nil {
			t.Fatal(err)
		}
		return sql
	}
	before := read()
	failure := errors.New("injected trigger install failure")
	if err := db.Callback().Raw().Before("gorm:raw").Register("fail_capacity_trigger", func(tx *gorm.DB) {
		if strings.HasPrefix(tx.Statement.SQL.String(), "CREATE TRIGGER IF NOT EXISTS token_detection_currents_capacity_UPDATE_v1") {
			_ = tx.AddError(failure)
		}
	}); err != nil {
		t.Fatal(err)
	}
	restarted := &Monitor{storeDB: db}
	err := restarted.ensureTokenDetectionCapacity(context.Background())
	_ = db.Callback().Raw().Remove("fail_capacity_trigger")
	if !errors.Is(err, failure) || restarted.tokenDetectionCapacityReady.Load() || read() != before {
		t.Fatal("failed upgrade lost the installed trigger or published readiness", err)
	}
	if err := restarted.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTokenCapacityLedger(t, db)
}
