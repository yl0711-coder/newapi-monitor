package monitor

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"gorm.io/gorm"
)

func TestTokenDetectionQueueProjectionMigrationAndRollback(t *testing.T) {
	m, now, _, fact := detectionFairnessFixtureSize(t, 32)
	defer m.Close()
	db, hash := m.tokenStore(), tokenDetectionPolicyHash(detectionPolicy())
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	// Recreate the persisted v137 projection, retaining its populated rows and
	// all authoritative facts/heads/retries. The upgrade must not require empty
	// stores, and a failed schema transaction must preserve the old projection.
	err := db.Transaction(func(tx *gorm.DB) error {
		memberFields := "source_epoch,log_id,created_at,user_id,channel_id,model,user_group,start_at,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,fact_revision,link_revision,retry_present,group_key"
		groupFields := "group_key,source_epoch,start_at,user_id,channel_id,model,user_group,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,retry_present,requests"
		for _, pair := range [][2]string{{tokenReportPendingMembers, memberFields}, {tokenReportPendingGroups, groupFields}} {
			if err := tx.Exec("CREATE TEMP TABLE saved_" + pair[0] + " AS SELECT " + pair[1] + " FROM " + pair[0]).Error; err != nil {
				return err
			}
		}
		for _, source := range []string{"fact", "head", "retry"} {
			for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
				if err := tx.Exec(fmt.Sprintf("DROP TRIGGER token_report_pending_%s_%s_v1", source, event)).Error; err != nil {
					return err
				}
			}
		}
		if err := tx.Migrator().DropTable(tokenReportPendingMembers, tokenReportPendingGroups); err != nil {
			return err
		}
		for _, sql := range []string{
			`CREATE TABLE ` + tokenReportPendingMembers + ` (source_epoch TEXT NOT NULL,log_id INTEGER NOT NULL,created_at INTEGER,user_id INTEGER,channel_id INTEGER,model TEXT,user_group TEXT,start_at INTEGER,base_pending INTEGER NOT NULL,policy_hash TEXT,reference_bucket TEXT,reference_generation INTEGER,source_complete INTEGER,fact_revision INTEGER,link_revision INTEGER,retry_present INTEGER NOT NULL,group_key TEXT NOT NULL,PRIMARY KEY(source_epoch,log_id))`,
			`CREATE TABLE ` + tokenReportPendingGroups + ` (group_key TEXT PRIMARY KEY,source_epoch TEXT NOT NULL,start_at INTEGER,user_id INTEGER,channel_id INTEGER,model TEXT,user_group TEXT,base_pending INTEGER NOT NULL,policy_hash TEXT,reference_bucket TEXT,reference_generation INTEGER,source_complete INTEGER,retry_present INTEGER NOT NULL,requests INTEGER NOT NULL)`,
			`INSERT INTO ` + tokenReportPendingMembers + ` SELECT * FROM saved_` + tokenReportPendingMembers,
			`INSERT INTO ` + tokenReportPendingGroups + ` SELECT * FROM saved_` + tokenReportPendingGroups,
		} {
			if err := tx.Exec(sql).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("injected migration rollback")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := initTokenReportPendingIndex(tx); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if db.Migrator().HasColumn(tokenReportPendingMembers, "head_present") {
		t.Fatal("rolled-back migration leaked schema")
	}
	if db.Migrator().HasIndex(tokenReportPendingGroups, "idx_token_detection_pending_count_v1") {
		t.Fatal("rolled-back migration leaked queue count index")
	}
	if err := db.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal(err)
	}
	// Startup commits only a bounded prefix. Complete the durable migration
	// before comparing the entire projected population with the legacy oracle.
	for turns := 0; ; turns++ {
		ready, err := tokenPendingProjectionReady(db)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
		if turns > 20 {
			t.Fatal("pending migration failed to advance")
		}
		if err := db.Transaction(func(tx *gorm.DB) error { _, _, err := migrateTokenPendingBatch(tx); return err }); err != nil {
			t.Fatal(err)
		}
	}
	if !db.Migrator().HasIndex(tokenReportPendingGroups, "idx_token_detection_pending_count_v1") {
		t.Fatal("populated-store migration omitted queue count index")
	}
	check := func() {
		t.Helper()
		for rotation := int64(0); rotation < 4; rotation++ {
			for _, limit := range []int{1, 17, 250} {
				at := now + rotation*15
				want, err := tokenLegacyDetectionJobs(db, fact.SourceEpoch, hash, at, limit, 15)
				if err != nil {
					t.Fatal(err)
				}
				got, err := tokenDetectionJobsBounded(db, fact.SourceEpoch, hash, at, limit, 15)
				if err != nil || len(got) > limit {
					t.Fatalf("migrated queue is not bounded: count=%d limit=%d error=%v", len(got), limit, err)
				}
				seen := map[int64]bool{}
				for _, job := range got {
					if seen[job.LogID] {
						t.Fatal("duplicate queue identity", job.LogID)
					}
					seen[job.LogID] = true
				}
				if limit == 250 {
					// Due retries now retain their own lane even when their input
					// changed. Compare the entire eligible set, not the old ordering
					// that could starve these tasks behind a full correction prefix.
					less := func(a, b TokenMeteringFact) int {
						if a.LogID < b.LogID {
							return -1
						}
						if a.LogID > b.LogID {
							return 1
						}
						return 0
					}
					slices.SortFunc(want, less)
					slices.SortFunc(got, less)
					if !reflect.DeepEqual(want, got) {
						t.Fatalf("migrated eligibility differs: want=%v got=%v", want, got)
					}
				}
			}
		}
	}
	check()
	for _, sql := range []string{
		`UPDATE token_detection_currents SET policy_hash=NULL,reference_bucket=NULL,reference_generation=NULL,source_complete=NULL,fact_revision=0 WHERE log_id=3`,
		`UPDATE token_metering_facts SET created_at=created_at+301,link_revision=link_revision+1 WHERE log_id=13`,
		`DELETE FROM token_detection_currents WHERE log_id=5`,
		`UPDATE token_detection_retries SET fact_revision=0,next_at=0 WHERE log_id%2=0`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
		check()
	}
	if err := db.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestTokenDetectionQueueRequeuesHeadWithEmptyDetectionState(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	f := detectionFact(t, m, 901, now-120, 3, 20000, 0, 0)
	hash := tokenDetectionPolicyHash(detectionPolicy())
	if err := m.storeDB.Create(&TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: f.LogID, CreatedAt: f.CreatedAt, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: hash, DetectionState: "", ResultJSON: `{}`}).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(m.storeDB); err != nil {
		t.Fatal(err)
	}
	jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), f.SourceEpoch, hash, now, 16, 15)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, job := range jobs {
		if job.LogID == f.LogID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("empty detection state was treated as completed: jobs=%v", jobs)
	}
}
