package monitor

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Optional retained-data comparison, using a private projection-only snapshot.
// The snapshot has the original group rows/indexes plus the proposed narrow
// index. No source connection, mutable coverage or detection payload is needed.
func TestTokenPendingAggregatesRetainedReadPlan(t *testing.T) {
	path := os.Getenv("TOKEN_PENDING_PROBE_SNAPSHOT")
	if path == "" {
		t.Skip("set TOKEN_PENDING_PROBE_SNAPSHOT to a private projection snapshot")
	}
	db, err := gorm.Open(sqlite.Open("file:"+path+"?mode=ro&immutable=1&_pragma=query_only(1)&_pragma=cache_size(-131072)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
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
	validity := `g.base_pending=1 OR g.policy_hash!=? OR g.reference_generation!=COALESCE(b.generation,0) OR g.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END`
	joins := ` LEFT JOIN token_detection_buckets b ON b.id=g.reference_bucket LEFT JOIN token_metering_windows w ON w.source_epoch=g.source_epoch AND w.start_at=g.start_at`
	var wanted []int64
	var reportPending *int64
	for round := 0; round < 2; round++ {
		for _, index := range []string{"idx_token_detection_pending_group_v1", "idx_token_detection_pending_count_v1"} {
			query := `SELECT COALESCE(SUM(CASE WHEN (` + validity + `) THEN g.requests ELSE 0 END),0) AS stale,COALESCE(SUM(CASE WHEN NOT (` + validity + `) THEN g.requests ELSE 0 END),0) AS valid,COALESCE(SUM(g.requests),0) AS heads FROM token_report_pending_groups_v1 g INDEXED BY ` + index + joins + ` WHERE g.source_epoch=? AND g.retry_present=0 AND g.head_present=1`
			var counts struct{ Stale, Valid, Heads int64 }
			began := time.Now()
			if err := db.Raw(query, detection.PolicyHash, detection.PolicyHash, state.SourceEpoch).Scan(&counts).Error; err != nil {
				t.Fatal(err)
			}
			t.Logf("round=%d index=%s elapsed=%s counts=%+v", round, index, time.Since(began), counts)
			got := []int64{counts.Stale, counts.Valid, counts.Heads}
			if wanted == nil {
				wanted = got
			} else if !reflect.DeepEqual(wanted, got) {
				t.Fatal("counts changed", wanted, got)
			}
		}
		for _, joined := range []bool{false, true} {
			query := `SELECT COALESCE(SUM(g.requests),0) FROM token_report_pending_groups_v1 g`
			predicate := tokenReportPendingPredicate("g")
			if joined {
				query += joins
				predicate = validity
			}
			query += ` WHERE g.source_epoch=? AND g.start_at>=? AND g.start_at<? AND (` + predicate + `)`
			var count int64
			began := time.Now()
			if err := db.Raw(query, state.SourceEpoch, state.StartAt, state.ScanThrough, detection.PolicyHash).Scan(&count).Error; err != nil {
				t.Fatal(err)
			}
			t.Logf("round=%d report_joined=%t elapsed=%s pending=%d", round, joined, time.Since(began), count)
			if reportPending == nil {
				v := count
				reportPending = &v
			} else if *reportPending != count {
				t.Fatal("joined range count changed", *reportPending, count)
			}
		}
	}
}
