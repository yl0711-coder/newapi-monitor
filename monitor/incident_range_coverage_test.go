package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestIncidentPageCoverageSeparatesRangeFromHistoricalGaps(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	const day int64 = 1793548800 // 2026-11-02 00:00 Asia/Shanghai.
	end := day + 9*3600
	global := day - 6000 // Previously reported as a 38,400-second range lag.
	if err := m.storeDB.Create(&IncidentIngestCursor{ID: 1, ScanThroughTs: end, CompleteThroughTs: global}).Error; err != nil {
		t.Fatal(err)
	}
	var windows []IncidentIngestWindowState
	for at := day; at < end; at += 300 {
		windows = append(windows, IncidentIngestWindowState{DedupKey: "processed", WindowFromTs: at, WindowToTs: at + 300, DataComplete: true, ProcessState: incidentProcessDone, WindowState: incidentWindowStateNoSample})
	}
	for n := 0; n < 19; n++ {
		windows = append(windows, IncidentIngestWindowState{DedupKey: fmt.Sprintf("history-%d", n), WindowFromTs: global, WindowToTs: global + 300, ProcessState: "unattributable"})
	}
	if err := m.storeDB.Create(&windows).Error; err != nil {
		t.Fatal(err)
	}
	now := end + int64(ingest.FinalizeLagSeconds) + 30
	check := func(from, to, actual, lag int64, complete bool, status string) {
		t.Helper()
		got, err := m.incidentPageCoverage(context.Background(), from, to, core, now)
		if err != nil || got["complete"] != complete || got["status"] != status || got["actual_through_ts"] != actual || got["lag_seconds"] != lag || got["global_complete_through_ts"] != global {
			t.Fatal("incorrect range coverage", got, err)
		}
	}
	check(day, end, end, 0, true, "complete")
	check(day+75, end-75, end-75, 0, true, "complete")
	check(day, end+60, end, 0, false, "finalizing")
	check(end+60, end+120, 0, 0, false, "finalizing")
	if err := m.storeDB.Create(&IncidentIngestGapSummary{DedupKey: "inside", GapFromTs: day + 3600, GapThroughTs: day + 3900}).Error; err != nil {
		t.Fatal(err)
	}
	check(day, end, day+3600, 8*3600, false, "incomplete")
	check(day+3600, end, 0, 8*3600, false, "incomplete")
	check(day+3900, end, end, 0, true, "complete")
	var history int64
	if err := m.storeDB.Model(&IncidentIngestWindowState{}).Where("process_state='unattributable' AND data_complete=0").Count(&history).Error; err != nil || history != 19 {
		t.Fatal("range read changed historical gaps", history, err)
	}
	var cursor IncidentIngestCursor
	if err := m.storeDB.First(&cursor, 1).Error; err != nil || cursor.CompleteThroughTs != global {
		t.Fatal("range proof rewrote global completeness", cursor, err)
	}
}

// Opt-in read-only verification against retained runtime records. This never
// migrates the supplied store or changes a cursor, incident or gap.
func TestIncidentPageReadonlyRangeReconciliation(t *testing.T) {
	path := os.Getenv("INCIDENT_COVERAGE_READONLY_DB")
	if path == "" {
		t.Skip("set INCIDENT_COVERAGE_READONLY_DB and explicit fixed range")
	}
	from, e1 := strconv.ParseInt(os.Getenv("INCIDENT_COVERAGE_FROM"), 10, 64)
	to, e2 := strconv.ParseInt(os.Getenv("INCIDENT_COVERAGE_TO"), 10, 64)
	if e1 != nil || e2 != nil || from <= 0 || to <= from || to-from > 86400 {
		t.Fatal("a fixed, bounded range is required")
	}
	core, _ := incidentIngestTestEnv(t)
	db, err := gorm.Open(sqlite.Open("file:"+path+"?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(1000)"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		m := &Monitor{storeDB: tx}
		got, err := m.incidentPageCoverage(ctx, from, to, core, time.Now().Unix())
		if err != nil {
			return err
		}
		var outside int64
		if err := tx.Model(&IncidentIngestWindowState{}).Where("process_state='unattributable' AND data_complete=0 AND window_to_ts<=?", from).Count(&outside).Error; err != nil {
			return err
		}
		body, _ := json.Marshal(got)
		t.Logf("range_proof=%s historical_unattributable_rows=%d", body, outside)
		if got["complete"] != true || got["actual_through_ts"] != to || got["lag_seconds"] != int64(0) || outside < 19 {
			t.Fatal("runtime range is not independently complete or historical gaps are absent", got, outside)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
