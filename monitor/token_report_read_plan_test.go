package monitor

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestTokenReportRetainedSummaryReadPlan(t *testing.T) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a verified read-only snapshot")
	}
	// This input is the verified, checkpointed stopped-writer snapshot. Its
	// read-only mount must not need to create WAL coordination side files.
	db, err := gorm.Open(sqlite.Open("file:"+snapshot+"?mode=ro&immutable=1&_pragma=query_only(1)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	pool.SetMaxOpenConns(1)
	defer pool.Close()
	var state TokenMeteringState
	if err := db.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: state.SourceEpoch, From: state.StartAt, To: state.ScanThrough, Limit: 25}
	for _, forced := range []bool{false, true} {
		tx := db
		if forced {
			tx = tx.Table("token_detection_currents INDEXED BY idx_token_detection_report_range")
		}
		query := tokenDetectionRange(tx, q).Select("user_id,channel_id,model,user_group,created_at,result_json").Order("created_at, log_id").Limit(5001)
		var rows []TokenDetectionCurrent
		dry := query.Session(&gorm.Session{DryRun: true}).Find(&rows)
		var plan []struct{ Detail string }
		if err := db.Raw("EXPLAIN QUERY PLAN "+dry.Statement.SQL.String(), dry.Statement.Vars...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		t.Logf("retained summary forced=%t plan=%v", forced, plan)
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		began := time.Now()
		err := query.WithContext(ctx).Find(&rows).Error
		stop()
		readTime := time.Since(began)
		began = time.Now()
		if _, err := tokenDetectionSummarize(rows[:min(5000, len(rows))]); err != nil {
			t.Fatal(err)
		}
		if _, err := tokenReportAnalyze(rows[:min(5000, len(rows))]); err != nil {
			t.Fatal(err)
		}
		var bytes int
		for _, row := range rows {
			bytes += len(row.ResultJSON)
		}
		t.Logf("retained summary forced=%t rows=%d bytes=%d read=%s decode=%s error=%v", forced, len(rows), bytes, readTime, time.Since(began), err)
		if forced && (err != nil || strings.Contains(plan[0].Detail, "SCAN")) {
			t.Fatal("forced range read failed", err)
		}
	}
}
