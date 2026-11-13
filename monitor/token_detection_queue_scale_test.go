package monitor

import (
	"context"
	"os"
	"testing"
	"time"
)

// Exercise the opposite end of catch-up as well: a large processed prefix,
// sparse tail work, and rechecks invalidated by a policy change. These are
// synthetic scheduling identities, not four days of real detection evidence.
func TestTokenDetectionRetainedQueueShapes(t *testing.T) {
	if os.Getenv("TOKEN_QUEUE_SCALE") != "1" {
		t.Skip("set TOKEN_QUEUE_SCALE=1 for the retained-identity scheduling check")
	}
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	const count = 303897
	hash := tokenDetectionPolicyHash(detectionPolicy())
	started := time.Now()
	// Bulk seed before installing projections; every projection is then built
	// by the actual migration from authoritative fact/head identities.
	if err := db.Exec(`WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<?)
		INSERT INTO token_metering_facts(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,revision,link_revision)
		SELECT ?,n,?-?/5+n/5,n%200+1,n%5+1,'synthetic-queue','synthetic-group',1,1 FROM ids`, count, epoch, now-3600, count).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO token_detection_currents
		(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,fact_revision,link_revision,policy_hash,reference_bucket,reference_generation,source_complete,next_at,candidate,result_json)
		SELECT source_epoch,log_id,created_at,user_id,channel_id,model,user_group,revision,link_revision,?,'',0,0,?,0,'{"rules":[]}' FROM token_metering_facts`, hash, now+60).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	t.Logf("synthetic retained queue identities=%d setup=%s", count, time.Since(started))
	check := func(name, policy string, at int64, want int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		started := time.Now()
		rows, err := tokenDetectionJobsBounded(db.WithContext(ctx), epoch, policy, at, 250, 15)
		cancel()
		t.Logf("retained queue shape=%s jobs=%d elapsed=%s error=%v", name, len(rows), time.Since(started), err)
		if err != nil || len(rows) != want {
			t.Fatalf("queue shape %s: want=%d got=%d error=%v", name, want, len(rows), err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		started = time.Now()
		rows, err = tokenDetectionPendingSample(db.WithContext(ctx), epoch, policy, at, 50)
		cancel()
		t.Logf("retained health shape=%s pending=%d elapsed=%s error=%v", name, len(rows), time.Since(started), err)
		if err != nil || len(rows) != min(want, 50) {
			t.Fatalf("health shape %s: want=%d got=%d error=%v", name, min(want, 50), len(rows), err)
		}
	}
	check("settled-prefix-empty", hash, now, 0)
	check("all-due-valid", hash, now+60, 250)
	check("all-due-stale", "changed-policy", now+60, 250)
	if err := db.Exec("UPDATE token_detection_currents SET fact_revision=0 WHERE log_id>?", count-17).Error; err != nil {
		t.Fatal(err)
	}
	check("sparse-correction-tail", hash, now, 17)
	if err := db.Exec("DELETE FROM token_detection_currents WHERE log_id>?", count-17).Error; err != nil {
		t.Fatal(err)
	}
	check("sparse-fresh-tail", hash, now, 17)
}
