package monitor

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Retained only as an independent query-plan oracle. Production dispatch uses
// the bounded durable lanes and must not fall back to this filtered head scan.
func tokenDetectionDueCoveredJobs(db *gorm.DB, epoch, hash string, now int64, limit int) ([]TokenMeteringFact, error) {
	var rows []TokenMeteringFact
	due := `SELECT f.source_epoch,f.log_id FROM ` + tokenReportPendingMembers + ` f INDEXED BY idx_token_detection_pending_due_v1 ` + tokenDetectionHeadJoin(db) + `
		LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket
		LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300
		WHERE f.source_epoch=? AND f.retry_present=0 AND f.head_present=1 AND f.next_at>0 AND f.next_at<=?
		AND c.fact_revision=f.fact_revision AND c.link_revision=f.link_revision AND c.policy_hash=?
		AND c.reference_generation=COALESCE(b.generation,0)
		AND c.source_complete=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END
		ORDER BY f.next_at,f.created_at,f.log_id LIMIT ?`
	err := db.Raw(due, epoch, now, hash, limit).Scan(&rows).Error
	return rows, err
}

func TestTokenDetectionDueCoveredQueueMatchesAuthoritativeFacts(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 9; id++ {
		detectionFact(t, m, id, now-600-id, 3, 20000, 0, 0)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now); err != nil || n != 9 {
		t.Fatal(n, err)
	}
	// First evaluations advance the shared peer bucket. Settle those revisions
	// before introducing the specific stale/unknown heads below; otherwise the
	// only current head is the final first-evaluation, which this fixture edits.
	if n, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal("settle initial peer generations", n, err)
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	for _, sql := range []string{
		`UPDATE token_detection_currents SET next_at=` + tokenDetectionJSON(now-1),
		`UPDATE token_metering_facts SET revision=revision+1 WHERE log_id=1`,
		`UPDATE token_detection_currents SET policy_hash=NULL WHERE log_id=2`,
		`UPDATE token_detection_currents SET reference_bucket='due-changed',reference_generation=0 WHERE log_id=3`,
		`INSERT INTO token_detection_buckets(id,generation) VALUES('due-changed',1)`,
		`UPDATE token_detection_currents SET source_complete=1 WHERE log_id=4`,
		`UPDATE token_detection_currents SET link_revision=NULL WHERE log_id=6`,
		`UPDATE token_detection_currents SET next_at=` + tokenDetectionJSON(now+1) + ` WHERE log_id=7`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&TokenDetectionRetry{SourceEpoch: epoch, LogID: 5, NextAt: now - 1}).Error; err != nil {
		t.Fatal(err)
	}
	var statement string
	var args []any
	if err := db.Callback().Row().After("gorm:row").Register("capture_due_covered", func(tx *gorm.DB) {
		if sql := tx.Statement.SQL.String(); strings.HasPrefix(sql, "SELECT f.source_epoch,f.log_id FROM "+tokenReportPendingMembers) {
			statement, args = sql, append([]any(nil), tx.Statement.Vars...)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Callback().Row().Remove("capture_due_covered"); err != nil {
			t.Error(err)
		}
	}()
	verify := func(db *gorm.DB) {
		t.Helper()
		for _, limit := range []int{1, 250} {
			var want []TokenMeteringFact
			err := db.Table("token_detection_currents c").Select("f.source_epoch,f.log_id").
				Joins("JOIN token_metering_facts f ON f.source_epoch=c.source_epoch AND f.log_id=c.log_id").
				Joins("LEFT JOIN token_detection_retries r ON r.source_epoch=f.source_epoch AND r.log_id=f.log_id").
				Joins("LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket").
				Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300").
				Where(`c.source_epoch=? AND r.log_id IS NULL AND c.next_at>0 AND c.next_at<=? AND NOT (c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR c.policy_hash!=? OR c.reference_generation!=COALESCE(b.generation,0) OR c.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END)`, epoch, now, hash).
				Order("c.next_at,f.created_at,f.log_id").Limit(limit).Scan(&want).Error
			if err != nil {
				t.Fatal(err)
			}
			got, err := tokenDetectionDueCoveredJobs(db, epoch, hash, now, limit)
			if err != nil || !reflect.DeepEqual(got, want) || len(got) != min(limit, 2) {
				t.Fatal("due queue differs", got, want, err)
			}
		}
	}
	verify(db)
	rollback := errors.New("rollback due order")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("UPDATE token_detection_currents SET next_at=next_at-1 WHERE log_id=8").Error; err != nil {
			return err
		}
		verify(tx)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	verify(db)
	var plan []struct{ Detail string }
	if err := db.Raw("EXPLAIN QUERY PLAN "+statement, args...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	coveredDue, coveredHead := false, false
	for _, row := range plan {
		coveredDue = coveredDue || strings.Contains(row.Detail, "COVERING INDEX idx_token_detection_pending_due_v1")
		coveredHead = coveredHead || strings.Contains(row.Detail, "COVERING INDEX idx_token_detection_work_head")
		if strings.Contains(row.Detail, "MATERIALIZE") || strings.Contains(row.Detail, "TEMP B-TREE") {
			t.Fatal("due read rebuilds or sorts retained groups", plan)
		}
	}
	if !coveredDue || !coveredHead {
		t.Fatal("due read fetches wide rows", plan)
	}
}

func TestTokenDetectionDueRetainedReadCost(t *testing.T) {
	path := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if path == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to an immutable stopped backup")
	}
	db, err := gorm.Open(sqlite.Open("file:"+path+"?mode=ro&immutable=1&_pragma=query_only(1)&_pragma=cache_size(-131072)"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	pool.SetMaxOpenConns(1)
	defer pool.Close()
	var state TokenMeteringState
	var detection TokenDetectionState
	if err := db.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&detection).Error; err != nil {
		t.Fatal(err)
	}
	now := state.ScanThrough + 3600
	old := `WITH eligible AS MATERIALIZED (
	 SELECT g.group_key FROM token_report_pending_groups_v1 g INDEXED BY idx_token_detection_pending_group_v1
	 LEFT JOIN token_detection_buckets b ON b.id=g.reference_bucket
	 LEFT JOIN token_metering_windows w ON w.source_epoch=g.source_epoch AND w.start_at=g.start_at
	 WHERE g.source_epoch=? AND g.retry_present=0 AND g.head_present=1 AND NOT (` + tokenReportPendingJoinedPredicate("g") + `))
	 SELECT f.source_epoch,f.log_id FROM token_report_pending_members_v1 f INDEXED BY idx_token_detection_pending_due_v1 ` + tokenDetectionHeadJoin(db) + `
	 WHERE f.source_epoch=? AND f.retry_present=0 AND f.head_present=1 AND f.next_at>0 AND f.next_at<=?
	 AND EXISTS(SELECT 1 FROM eligible) AND f.group_key IN (SELECT group_key FROM eligible)
	 AND c.fact_revision=f.fact_revision AND c.link_revision=f.link_revision ORDER BY f.next_at,f.created_at,f.log_id LIMIT ?`
	for round := 0; round < 2; round++ {
		var want []TokenMeteringFact
		began := time.Now()
		if err := db.Raw(old, state.SourceEpoch, detection.PolicyHash, state.SourceEpoch, now, 250).Scan(&want).Error; err != nil {
			t.Fatal(err)
		}
		t.Logf("round=%d materialized_due=%s rows=%d", round, time.Since(began), len(want))
		began = time.Now()
		got, err := tokenDetectionDueCoveredJobs(db, state.SourceEpoch, detection.PolicyHash, now, 250)
		t.Logf("round=%d covered_due=%s rows=%d", round, time.Since(began), len(got))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("retained due membership changed", len(got), len(want), err)
		}
	}
}
