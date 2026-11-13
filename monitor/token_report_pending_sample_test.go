package monitor

import (
	"context"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type tokenHealthSQLLogger struct {
	logger.Interface
	statements *[]string
}

func (l tokenHealthSQLLogger) Trace(_ context.Context, _ time.Time, query func() (string, int64), _ error) {
	sql, _ := query()
	if strings.HasPrefix(sql, "WITH prefix") {
		*l.statements = append(*l.statements, sql)
	}
}

func TestTokenReportPendingSampleGroupedBoundsAndValidity(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	for id := int64(1); id <= 200; id++ {
		fact := detectionFact(t, m, id, now-600, 3, 20000, 0, 0)
		if err := db.Model(&fact).Update("user_id", id).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	var statements []string
	logged := db.Session(&gorm.Session{Logger: tokenHealthSQLLogger{Interface: logger.Default, statements: &statements}})
	rows, err := tokenDetectionPendingSample(logged, epoch, hash, now, 50)
	if err != nil || len(rows) != 50 || len(statements) != 3 {
		t.Fatal("fresh backlog should satisfy its bounded sample directly", len(rows), len(statements), err)
	}
	seen := map[int64]bool{}
	for _, row := range rows {
		if seen[row.LogID] || row.LogID < 1 || row.LogID > 200 || row.SourceEpoch != epoch {
			t.Fatal("sample duplicated or invented an identity", row)
		}
		seen[row.LogID] = true
	}
	var plan []struct{ Detail string }
	if err := db.Raw("EXPLAIN QUERY PLAN " + statements[0]).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	seek := false
	for _, p := range plan {
		seek = seek || strings.Contains(p.Detail, "COVERING INDEX idx_token_detection_discovery_v1")
	}
	if !seek {
		t.Fatal("health discovery fetches unbounded or noncovering members", plan)
	}
	// Keep all identities, move every group into the current state, then
	// invalidate a suffix. Empty and sparse-stale results must stay exact.
	if err := db.Exec(`INSERT INTO token_detection_currents
		(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,fact_revision,link_revision,policy_hash,reference_generation,reference_bucket,source_complete,candidate,next_at,result_json,detection_state)
		SELECT source_epoch,log_id,created_at,user_id,channel_id,model,user_group,revision,link_revision,?,0,'',0,0,0,'{"rules":[]}','evaluated' FROM token_metering_facts`, hash).Error; err != nil {
		t.Fatal(err)
	}
	check := func(want int, valid func(int64) bool) {
		t.Helper()
		rows, err := tokenDetectionPendingSample(db, epoch, hash, now, 50)
		if err != nil || len(rows) != want {
			t.Fatal("sample count changed", len(rows), want, err)
		}
		for _, row := range rows {
			if !valid(row.LogID) {
				t.Fatal("healthy or backed-off identity reported as pending", row.LogID)
			}
		}
	}
	check(0, func(int64) bool { return false })
	if err := db.Exec("UPDATE token_detection_currents SET policy_hash='obsolete' WHERE log_id>=185").Error; err != nil {
		t.Fatal(err)
	}
	check(16, func(id int64) bool { return id >= 185 && id <= 200 })
	for _, id := range []int64{1, 2} {
		retry := TokenDetectionRetry{SourceEpoch: epoch, LogID: id, FactRevision: 1, LinkRevision: 1, PolicyHash: hash, NextAt: now + 3600}
		if id == 2 {
			retry.NextAt = now - 1
		}
		if err := db.Create(&retry).Error; err != nil {
			t.Fatal(err)
		}
	}
	check(17, func(id int64) bool { return id == 2 || id >= 185 && id <= 200 })
}
