package monitor

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Freeze the previous SQL as an independent identity/order oracle. This does
// not evaluate requests or change the database under comparison.
func tokenLegacyDetectionJobs(db *gorm.DB, epoch, hash string, now int64, limit, cadence int) ([]TokenMeteringFact, error) {
	query := func(identity bool) *gorm.DB {
		table := "token_metering_facts AS f"
		if identity {
			table = tokenDetectionIdentityFactTable(db)
		}
		return db.Table(table).Select("f.source_epoch, f.log_id, f.created_at").Joins(tokenDetectionHeadJoin(db)).
			Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300").
			Joins("LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket").
			Joins("LEFT JOIN token_detection_retries r ON r.source_epoch=f.source_epoch AND r.log_id=f.log_id").Where("f.source_epoch=?", epoch)
	}
	const changed = "(c.log_id IS NULL OR COALESCE(c.detection_state,'')='' OR c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR COALESCE(c.policy_hash,'')!=? OR COALESCE(c.reference_generation,-1)!=COALESCE(b.generation,0) OR COALESCE(c.source_complete,-1)!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END)"
	const retryChanged = "(r.fact_revision!=f.revision OR r.link_revision!=f.link_revision OR r.policy_hash!=?)"
	queries := []*gorm.DB{
		query(false).Where("c.log_id IS NULL AND r.log_id IS NULL").Order("f.created_at,f.log_id"),
		query(false).Where("(r.log_id IS NULL AND c.log_id IS NOT NULL AND "+changed+") OR (r.log_id IS NOT NULL AND "+retryChanged+")", hash, hash).Order("f.created_at,f.log_id"),
		query(true).Where("r.log_id IS NOT NULL AND NOT "+retryChanged+" AND r.next_at<=?", hash, now).Order("r.next_at,f.created_at,f.log_id"),
		query(true).Where("r.log_id IS NULL AND c.next_at>0 AND c.next_at<=? AND NOT "+changed, now, hash).Order("c.next_at,f.created_at,f.log_id"),
	}
	lanes := make([][]TokenMeteringFact, 4)
	for i, q := range queries {
		if err := q.Limit(limit).Scan(&lanes[i]).Error; err != nil {
			return nil, err
		}
	}
	// Compare the legacy SQL identities using the current scheduling policy;
	// weighted reservations and starvation are checked independently.
	return tokenDetectionInterleaveJobs(lanes, now, limit, cadence), nil
}

type tokenQueueSQLLogger struct {
	logger.Interface
	statements *[]string
}

func (l tokenQueueSQLLogger) Trace(_ context.Context, _ time.Time, query func() (string, int64), _ error) {
	sql, _ := query()
	if strings.HasPrefix(sql, "SELECT f.source_epoch, f.log_id FROM") || strings.HasPrefix(sql, "SELECT f.source_epoch,f.log_id FROM") || strings.HasPrefix(sql, "SELECT f.source_epoch,f.log_id,f.created_at FROM") || strings.HasPrefix(sql, "WITH prefix AS MATERIALIZED") || strings.HasPrefix(sql, "WITH due AS MATERIALIZED") || strings.HasPrefix(sql, "SELECT source_epoch,log_id") {
		*l.statements = append(*l.statements, sql)
	}
}

func TestTokenDetectionScheduledQueriesMatchLegacy(t *testing.T) {
	m, now, _, fact := detectionFairnessFixtureSize(t, 64)
	db, hash := m.tokenStore(), tokenDetectionPolicyHash(detectionPolicy())
	for _, sql := range []string{
		"UPDATE token_detection_currents SET fact_revision=0 WHERE log_id%10=1",
		"UPDATE token_detection_currents SET next_at=" + tokenDetectionJSON(now-1) + " WHERE log_id%10=2",
		"UPDATE token_detection_currents SET policy_hash=NULL WHERE log_id=3",
		"UPDATE token_metering_facts SET link_revision=NULL WHERE log_id=13",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	check := func() {
		t.Helper()
		for rotation := int64(0); rotation < 4; rotation++ {
			for _, limit := range []int{1, 17, 250} {
				at := now + rotation*15
				old, err := tokenLegacyDetectionJobs(db, fact.SourceEpoch, hash, at, limit, 15)
				if err != nil {
					t.Fatal(err)
				}
				got, err := tokenDetectionJobsBounded(db, fact.SourceEpoch, hash, at, limit, 15)
				if err != nil {
					t.Fatal(err)
				}
				if limit < 32 {
					all, err := tokenLegacyDetectionJobs(db, fact.SourceEpoch, hash, at, 250, 15)
					if err != nil {
						t.Fatal(err)
					}
					eligible := map[int64]bool{}
					for _, job := range all {
						eligible[job.LogID] = true
					}
					seen := map[int64]bool{}
					for _, job := range got {
						if !eligible[job.LogID] || seen[job.LogID] {
							t.Fatal("small recent queue changed eligibility or duplicated work", job)
						}
						seen[job.LogID] = true
					}
					if len(got) != min(limit, len(all)) {
						t.Fatal("small queue lost capacity", len(got), limit)
					}
					continue
				}
				// This fixture's eligible population fits in 250 rows. Recent
				// corrections deliberately interleave both ends now, so compare
				// the full identity set, including uniqueness. Retry and due
				// lanes still have to preserve their previous schedule order.
				if len(old) >= limit || len(got) >= limit {
					rows, _ := tokenDetectionScanPrefix(db, fact.SourceEpoch, hash, at, 0, 0, false)
					t.Fatalf("legacy eligibility comparison requires an untruncated fixture: old=%d got=%d sample=%+v", len(old), len(got), rows[:min(8, len(rows))])
				}
				ids := func(rows []TokenMeteringFact, lane int64) []int64 {
					out := []int64{}
					for _, row := range rows {
						if row.SourceEpoch != fact.SourceEpoch || row.CreatedAt != 0 {
							t.Fatal("nonidentity queue entry", row.SourceEpoch, row.LogID)
						}
						if lane == 0 || row.LogID%10 == lane {
							out = append(out, row.LogID)
						}
					}
					if lane == 0 {
						slices.Sort(out)
					}
					return out
				}
				for _, lane := range []int64{0} {
					if !reflect.DeepEqual(ids(old, lane), ids(got, lane)) {
						t.Fatalf("queue eligibility or unchanged schedule differs rotation=%d lane=%d old=%v got=%v", rotation, lane, ids(old, lane), ids(got, lane))
					}
				}
			}
		}
	}
	check()
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	check() // The maintained projection preserves eligibility and unchanged lane ordering.
	var statements []string
	logged := db.Session(&gorm.Session{Logger: tokenQueueSQLLogger{Interface: logger.Default, statements: &statements}})
	if _, err := tokenDetectionJobsBounded(logged, fact.SourceEpoch, hash, now, 250, 15); err != nil {
		t.Fatal(err)
	}
	if len(statements) < 5 {
		t.Fatalf("missing bounded lane queries: %d", len(statements))
	}
	for _, statement := range statements {
		if !strings.Contains(statement, "LIMIT") || strings.Contains(statement, "token_metering_facts") {
			t.Fatal("unbounded scheduling query", statement)
		}
		var plan []struct{ Detail string }
		if err := db.Raw("EXPLAIN QUERY PLAN " + statement).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		for _, p := range plan {
			if strings.Contains(p.Detail, "SCAN token_report_pending_members") || strings.Contains(p.Detail, "SCAN c") {
				t.Fatal("unbounded scheduling plan", plan)
			}
		}
	}
	if err := db.Migrator().DropIndex(&TokenDetectionCurrent{}, "idx_token_detection_due_head"); err != nil {
		t.Fatal(err)
	}
	check() // Existing read-only snapshots can lack the additive schedule index.
}

func TestTokenDetectionScheduledSnapshotProgress(t *testing.T) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a verified stopped-writer snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	var state TokenMeteringState
	if err := m.tokenStore().First(&state).Error; err != nil {
		t.Fatal(err)
	}
	var revision TokenDetectionRevision
	if err := m.tokenStore().First(&revision).Error; err != nil {
		t.Fatal(err)
	}
	var frozen tokenDetectionFrozen
	if err := json.Unmarshal([]byte(revision.FrozenJSON), &frozen); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsSourceEpoch = state.SourceEpoch
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(frozen.Policy)
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 15
	m.cfg.TokenDetectionMaxRows, m.cfg.TokenDetectionMaxBytes = 1200000, 24<<30
	hash, now := tokenDetectionPolicyHash(frozen.Policy), time.Now().Unix()
	var old, got []TokenMeteringFact
	if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
		started := time.Now()
		var err error
		old, err = tokenLegacyDetectionJobs(tx, state.SourceEpoch, hash, now, 250, 15)
		t.Logf("saved facts=%d legacy_queue_elapsed=%s jobs=%d", state.FactCount, time.Since(started), len(old))
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		started = time.Now()
		got, err = tokenDetectionJobsBounded(tx.WithContext(ctx), state.SourceEpoch, hash, now, 250, 15)
		t.Logf("scheduled_queue_elapsed=%s jobs=%d error=%v", time.Since(started), len(got), err)
		return err
	}); err != nil || !reflect.DeepEqual(old, got) {
		t.Fatalf("saved snapshot queue changed identity/order: %v", err)
	}
	var initial, final int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&initial).Error; err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 4; round++ {
		started := time.Now()
		n, err := m.runTokenDetectionRound(context.Background(), now+int64(round*15))
		if readErr := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&final).Error; readErr != nil {
			t.Fatal(readErr)
		}
		t.Logf("scheduled snapshot round=%d committed=%d new_heads=%d elapsed=%s error=%v", round, n, final-initial, time.Since(started), err)
		if n == 0 {
			t.Fatal("saved retained backlog did not commit progress", err)
		}
	}
	if final <= initial {
		t.Fatal("no new identities processed from the saved backlog")
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}
