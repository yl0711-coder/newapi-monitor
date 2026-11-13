package monitor

import (
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestTokenReportPendingAlignedAndPartialEdgesUseRangeSeeks(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	from := (now/300)*300 - 1200
	for id := int64(1); id <= 40; id++ {
		detectionFact(t, m, id, from+id*23, 3, 20000, 0, 0)
	}
	detectionFact(t, m, 41, from+3, 3, 20000, 0, 0)
	detectionFact(t, m, 42, from+1197, 3, 20000, 0, 0)
	var statement string
	var args []any
	if err := db.Callback().Row().After("gorm:row").Register("pending_edge_plan", func(tx *gorm.DB) {
		if sql := tx.Statement.SQL.String(); strings.HasPrefix(sql, "SELECT (SELECT COALESCE(SUM(g.requests)") {
			statement, args = sql, append([]any(nil), tx.Statement.Vars...)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Callback().Row().Remove("pending_edge_plan"); err != nil {
			t.Error(err)
		}
	}()
	hash := tokenDetectionPolicyHash(detectionPolicy())
	for _, bounds := range [][2]int64{{from, from + 1200}, {from + 7, from + 1200}, {from, from + 1193}, {from + 7, from + 1193}} {
		q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: bounds[0], To: bounds[1]}
		want, err := tokenReportLegacyPendingCount(db, q, hash)
		if err != nil {
			t.Fatal(err)
		}
		statement = ""
		got, err := tokenReportPendingCount(db, q, hash)
		if err != nil || got != want || statement == "" {
			t.Fatal("range count differs from authoritative facts", bounds, got, want, err)
		}
		var plan []struct{ Detail string }
		if err := db.Raw("EXPLAIN QUERY PLAN "+statement, args...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		for _, row := range plan {
			if strings.Contains(row.Detail, "CORRELATED SCALAR SUBQUERY") {
				t.Fatalf("range count repeats a dependency query per group: bounds=%v plan=%v", bounds, plan)
			}
			if strings.HasPrefix(row.Detail, "SCAN g") || strings.HasPrefix(row.Detail, "SCAN "+tokenReportPendingMembers) {
				t.Fatalf("a bounded edge scans the entire retained projection: bounds=%v plan=%v", bounds, plan)
			}
		}
	}
}
