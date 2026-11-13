package monitor

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"gorm.io/gorm"
)

func TestTokenDetectionCurrentWritePreservesAllFieldsAndRollback(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	current := TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 77}
	value := reflect.ValueOf(&current).Elem()
	typeOf := value.Type()
	for i := 0; i < value.NumField(); i++ {
		if typeOf.Field(i).Name == "SourceEpoch" || typeOf.Field(i).Name == "LogID" {
			continue
		}
		switch field := value.Field(i); field.Kind() {
		case reflect.String:
			field.SetString("initial")
		case reflect.Int, reflect.Int64:
			field.SetInt(2)
		case reflect.Bool:
			field.SetBool(true)
		default:
			t.Fatal("new persistent head type needs a round-trip case", typeOf.Field(i).Name)
		}
	}
	current.ResultJSON = `{}`
	stored := func(row TokenDetectionCurrent) *tokenDetectionStoredCurrent {
		return &tokenDetectionStoredCurrent{TokenDetectionCurrent: row}
	}
	read := func() TokenDetectionCurrent {
		t.Helper()
		var row TokenDetectionCurrent
		if err := m.tokenStore().First(&row, "source_epoch=? AND log_id=?", current.SourceEpoch, current.LogID).Error; err != nil {
			t.Fatal(err)
		}
		return row
	}
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error { return saveTokenDetectionCurrent(tx, nil, current) }); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != current {
		t.Fatal("first-result fields changed", got)
	}
	next := TokenDetectionCurrent{SourceEpoch: current.SourceEpoch, LogID: current.LogID, Revision: current.Revision + 1}
	rollback := errors.New("rollback head changes")
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		if err := saveTokenDetectionCurrent(tx, stored(current), next); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) || read() != current {
		t.Fatal("head rollback changed committed data", err)
	}
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error { return saveTokenDetectionCurrent(tx, stored(current), next) }); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != next {
		t.Fatal("zero, false or empty fields were not persisted", got)
	}
	if err := saveTokenDetectionCurrent(m.tokenStore(), stored(next), next); err != nil {
		t.Fatal("unchanged result", err)
	}
	changedIdentity := next
	changedIdentity.LogID++
	if err := saveTokenDetectionCurrent(m.tokenStore(), stored(next), changedIdentity); err == nil || read() != next {
		t.Fatal("changed request identity was accepted")
	}
	// Existing legacy heads may have revision zero. Existence comes from the
	// fenced read, never from a decision field that can be zero.
	zeroRevision := next
	zeroRevision.Revision = 0
	if err := m.tokenStore().Save(&zeroRevision).Error; err != nil {
		t.Fatal(err)
	}
	if err := saveTokenDetectionCurrent(m.tokenStore(), stored(zeroRevision), next); err != nil || read() != next {
		t.Fatal("existing zero-revision head was treated as an insertion", err)
	}
	if err := m.tokenStore().Delete(&next).Error; err != nil {
		t.Fatal(err)
	}
	if err := saveTokenDetectionCurrent(m.tokenStore(), stored(zeroRevision), next); err == nil {
		t.Fatal("missing existing head was silently recreated")
	}
}

func TestTokenDetectionCurrentWriteAvoidsUnchangedIndexes(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	current := TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 1, Revision: 1, CreatedAt: 100, EvaluatedAt: 200, ResultJSON: `{}`}
	if err := m.tokenStore().Create(&current).Error; err != nil {
		t.Fatal(err)
	}
	type query struct {
		sql  string
		args []any
	}
	var queries []query
	if err := m.tokenStore().Callback().Update().After("gorm:update").Register("head_update_plan", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_detection_currents" {
			queries = append(queries, query{tx.Statement.SQL.String(), append([]any(nil), tx.Statement.Vars...)})
		}
	}); err != nil {
		t.Fatal(err)
	}
	current.EvaluatedAt++
	if err := m.tokenStore().Save(&current).Error; err != nil {
		t.Fatal(err)
	}
	next := current
	next.EvaluatedAt++
	if err := saveTokenDetectionCurrent(m.tokenStore(), &tokenDetectionStoredCurrent{TokenDetectionCurrent: current}, next); err != nil {
		t.Fatal(err)
	}
	_ = m.tokenStore().Callback().Update().Remove("head_update_plan")
	if len(queries) != 2 {
		t.Fatal("missing executed update statements", len(queries))
	}
	var writes [2]int
	for n, query := range queries {
		var plan []struct{ Opcode string }
		if err := m.tokenStore().Raw("EXPLAIN "+query.sql, query.args...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		for _, step := range plan {
			if step.Opcode == "IdxInsert" || step.Opcode == "IdxDelete" {
				writes[n]++
			}
		}
	}
	t.Logf("unchanged-index write opcodes: whole-head save=%d changed-fields update=%d", writes[0], writes[1])
	if writes[0] == 0 || writes[1] != 0 {
		t.Fatal("unchanged head indexes are still rewritten", writes)
	}
}

func TestTokenDetectionCurrentWriteNormalizesLegacyNullsAndKeepsProjections(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-100, 3, 20000, 0, 0)
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	// An incomplete source window produces false, equal to the Go value read
	// from NULL. It must still persist SQL false after this real evaluation.
	legacy := TokenDetectionCurrent{SourceEpoch: fact.SourceEpoch, LogID: fact.LogID}
	if err := m.tokenStore().Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&legacy).Updates(map[string]any{"source_complete": nil, "quarantined": nil, "cache_signal": nil, "stage": nil}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	var nulls int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where(tokenDetectionCurrentNullCheck).Count(&nulls).Error; err != nil || nulls != 0 {
		t.Fatal("legacy NULL fields survived the evaluation", nulls, err)
	}
	head, _ := detectionHead(t, m, fact.LogID)
	if head.Revision != 1 || head.SourceComplete {
		t.Fatal("legacy identity or incomplete coverage changed", head.Revision, head.SourceComplete)
	}
	// A real late correction must update the queue projection, the display
	// projection, capacity and frozen result in the same transaction.
	if err := m.tokenStore().Model(&fact).Update("link_revision", fact.LinkRevision+1).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+1)
	q := tokenDetectionQuery{SourceEpoch: fact.SourceEpoch, From: fact.CreatedAt, To: fact.CreatedAt + 1}
	pending, err := tokenReportPendingCount(m.tokenReadStore(), q, tokenDetectionPolicyHash(detectionPolicy()))
	if err != nil || pending != 0 {
		t.Fatal("updated head left a stale queue projection", pending, err)
	}
	var heads, projected []TokenDetectionCurrent
	if err := tokenDetectionRange(m.tokenStore(), q).Find(&heads).Error; err != nil {
		t.Fatal(err)
	}
	if err := tokenReportSummaryRange(m.tokenStore(), q).Find(&projected).Error; err != nil {
		t.Fatal(err)
	}
	want, e1 := tokenDetectionSummarize(heads)
	got, e2 := tokenDetectionSummarize(projected)
	if e1 != nil || e2 != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("updated summary projection differs", e1, e2)
	}
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	var capacity tokenDetectionCapacity
	var bytes int64
	if err := m.tokenStore().First(&capacity, 1).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Select("SUM(length(CAST(result_json AS BLOB)))").Scan(&bytes).Error; err != nil || capacity.CurrentRows != 1 || capacity.HeadBytes != bytes {
		t.Fatal("narrow head update changed the capacity ledger", capacity, bytes, err)
	}
}
