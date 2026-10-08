package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in acceptance on a CLOSED local snapshot. No migrations, background
// workers, RDS connection or upstream clients are started. Only a disposable
// copy is writable; logs contain counts/timings, never account data.
func TestStabilityClosedSnapshotAcceptance(t *testing.T) {
	source := os.Getenv("MONITOR_STABILITY_ACCEPTANCE_SNAPSHOT")
	if source == "" {
		t.Skip("requires an explicit closed local snapshot")
	}
	if err := giftLocalClosedBackup(source); err != nil {
		t.Fatal(err)
	}
	input, err := giftLocalOpenRegular(source, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	path := filepath.Join(t.TempDir(), "acceptance.db")
	output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy: %v %v", copyErr, closeErr)
	}
	uri := (&url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=busy_timeout(1000)"}).String()
	db, err := gorm.Open(sqlite.Open(uri), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatalf("quick_check: %q %v", integrity, err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{StabilityEnabled: true, RetentionDays: 7, StabilityRetentionDays: 90}}
	var proof MetricFinalizeState
	if err := db.First(&proof, "id=1").Error; err != nil {
		t.Fatal(err)
	}
	// Snapshot-relative time makes replay independent of when this test is run.
	now := max(proof.UpdatedAt, proof.LastSuccessAt, proof.TargetThroughTs) + 3600
	from := (proof.TTFTCoverageFromTs + 3599) / 3600 * 3600
	to := min(proof.TTFTCoverageThroughTs, proof.NextTs, proof.TargetThroughTs, finalizedStabilityHourTo(now)) / 3600 * 3600
	var candidates []StabilityHourIngestState
	if proof.SemanticsVersion == stabilityTrafficClassificationVersion && proof.TTFTSemanticsVersion == ttftCoverageSemanticsVersion && proof.TTFTCoverageFromTs > 0 && from < to {
		if err := db.Where("hour_ts>=? AND hour_ts<? AND status='complete' AND traffic_class_version=? AND COALESCE(ttft_semantics_version,0)<>?", from, to, stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion).Order("hour_ts").Find(&candidates).Error; err != nil {
			t.Fatal(err)
		}
	}
	if len(candidates) > 168 {
		t.Fatal("snapshot exceeds bounded one-week acceptance scope")
	}
	protected := stabilitySnapshotDigests(t, db, true)
	repaired, blocked := 0, 0
	for _, candidate := range candidates {
		beforeCoverage := m.stabilityDataCoverage(context.Background(), candidate.HourTs, candidate.HourTs+3600, now)
		if beforeCoverage.FRTComplete {
			t.Fatal("legacy candidate unexpectedly had complete FRT coverage")
		}
		started := time.Now()
		err := m.repairOneStabilityFRTFromMinutes(context.Background(), now)
		if errors.Is(err, errStabilityFRTMismatch) {
			blocked++
			t.Logf("unmodified evidence mismatch hour=%d: %v", candidate.HourTs, err)
		} else if err != nil {
			t.Fatal(err)
		} else {
			repaired++
		}
		var receipt StabilityHourIngestState
		if err := db.First(&receipt, "hour_ts=?", candidate.HourTs).Error; err != nil {
			t.Fatal(err)
		}
		if (receipt.TTFTSemanticsVersion == ttftCoverageSemanticsVersion) != (err == nil) || m.stabilityFRTRepairAfter.Load() != candidate.HourTs {
			t.Fatal("repair outcome or scan order differs from receipt")
		}
		afterCoverage := m.stabilityDataCoverage(context.Background(), candidate.HourTs, candidate.HourTs+3600, now)
		if afterCoverage.FRTComplete != (err == nil) {
			t.Fatal("coverage does not reflect actual repair outcome")
		}
		if err == nil {
			verifyStabilitySnapshotRepairedHour(t, m, candidate, now)
		}
		t.Logf("repair turn elapsed=%s", time.Since(started))
	}
	if after := stabilitySnapshotDigests(t, db, true); !reflect.DeepEqual(protected, after) {
		for table, digest := range protected {
			if after[table] != digest {
				t.Errorf("non-FRT data changed in table %s", table)
			}
		}
		t.Fatal("protected database contents changed")
	}
	// Reset only the in-memory cursor (a restart); successful hours must not be
	// changed again, and unprovable hours must remain unmodified.
	all := stabilitySnapshotDigests(t, db, false)
	m.stabilityFRTRepairAfter.Store(0)
	for turn := 0; turn < max(1, len(candidates)); turn++ {
		if err := m.repairOneStabilityFRTFromMinutes(context.Background(), now); err != nil && !errors.Is(err, errStabilityFRTMismatch) {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(all, stabilitySnapshotDigests(t, db, false)) {
		t.Fatal("restart/retry was not idempotent")
	}
	// Real seven-day SQL readback must preserve the direct SQL error counters.
	scope := stabilityScope{FromTs: proof.TargetThroughTs - 7*86400, ToTs: proof.TargetThroughTs}
	where, args := scope.sqlWhere("sh")
	var expected struct{ Four, Five int64 }
	if err := db.Raw("SELECT COALESCE(SUM(sh.err_4xx),0) four,COALESCE(SUM(sh.err_5xx),0) five FROM stability_hour_samples sh"+where, args...).Scan(&expected).Error; err != nil {
		t.Fatal(err)
	}
	dims, truncated, err := m.queryStabilityDims(context.Background(), scope, 30000)
	if err != nil || truncated {
		t.Fatalf("real dimensions: %v truncated=%v", err, truncated)
	}
	var four, five int64
	for _, dim := range dims {
		four += dim.Err4xx
		five += dim.Err5xx
	}
	if four != expected.Four || five != expected.Five {
		t.Fatal("real SQL readback lost error counters")
	}
	t.Logf("tables protected=%d eligible=%d repaired=%d blocked=%d; seven-day 4xx=%d 5xx=%d", len(protected), len(candidates), repaired, blocked, four, five)
	if repaired == 0 {
		t.Log("no real hour repaired: this validates safety/readback only, not successful real-data FRT recovery")
	}
	if os.Getenv("MONITOR_STABILITY_ACCEPTANCE_REQUIRE_REPAIR") == "1" && repaired == 0 {
		t.Fatal("successful real-data recovery required but no old hour was repaired")
	}
}

func verifyStabilitySnapshotRepairedHour(t *testing.T, m *Monitor, receipt StabilityHourIngestState, now int64) {
	t.Helper()
	// Independent positional SQL scan avoids sharing GORM aliases/projection
	// code with the implementation being checked.
	const totals = `COALESCE(SUM(ttft_500),0),COALESCE(SUM(ttft_1k),0),COALESCE(SUM(ttft_2k),0),
		COALESCE(SUM(ttft_5k),0),COALESCE(SUM(ttft_10k),0),COALESCE(SUM(ttft_inf),0),
		COALESCE(SUM(ttft_observed),0),COALESCE(SUM(ttft_over_3s),0),COALESCE(MAX(ttft_max_ms),0)`
	read := func(query string, args ...any) [9]int64 {
		var v [9]int64
		if err := m.storeDB.Raw(query, args...).Row().Scan(&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8]); err != nil {
			t.Fatal(err)
		}
		return v
	}
	expected := read("SELECT "+totals+" FROM metric_samples WHERE bucket_ts>=? AND bucket_ts<?", receipt.HourTs, receipt.HourTs+3600)
	actual := read("SELECT "+totals+" FROM stability_hour_samples WHERE hour_ts=?", receipt.HourTs)
	if actual != expected {
		t.Fatal("repaired FRT histogram differs from minute evidence")
	}
	scope := stabilityScope{FromTs: receipt.HourTs, ToTs: receipt.HourTs + 3600, RangeHours: 1}
	report, err := m.buildStabilityReportWithDetails(context.Background(), scope, now, true)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Meta.DataCoverage.FRTComplete || report.Summary.FRTObserved != expected[6] || report.Summary.FRTOver3s != expected[7] || report.Summary.FRTMaxMs != expected[8] || report.Summary.Requests != receipt.Requests {
		t.Fatal("report does not publish repaired hour consistently")
	}
	var expectedView = computeTTFTMetricView([6]int64{expected[0], expected[1], expected[2], expected[3], expected[4], expected[5]}, expected[6], expected[7], expected[8])
	if report.Summary.FRTP95Ms != expectedView.P95Ms {
		t.Fatal("report percentile differs from source histogram")
	}
	t.Logf("repaired hour=%d dimensions=%d requests=%d FRT observed=%d over3s=%d p95_ms=%.2f coverage_complete=true", receipt.HourTs, receipt.Rows, receipt.Requests, expected[6], expected[7], report.Summary.FRTP95Ms)
}

// Hash every table deterministically without disclosing its contents. Only
// the explicitly allowed FRT columns in the two projection tables may differ.
func stabilitySnapshotDigests(t *testing.T, db *gorm.DB, omitFRT bool) map[string]string {
	t.Helper()
	quote := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	var tables []string
	if err := db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").Scan(&tables).Error; err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string, len(tables))
	for _, table := range tables {
		var columns []struct{ Name string }
		if err := db.Raw("PRAGMA table_info(" + quote(table) + ")").Scan(&columns).Error; err != nil {
			t.Fatal(err)
		}
		var selected []string
		for _, col := range columns {
			if omitFRT && (table == "stability_hour_samples" || table == "stability_hour_ingest_states") && strings.HasPrefix(col.Name, "ttft_") {
				continue
			}
			selected = append(selected, quote(col.Name))
		}
		rows, err := db.Raw("SELECT " + strings.Join(selected, ",") + " FROM " + quote(table) + " ORDER BY rowid").Rows()
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		encoder := json.NewEncoder(h)
		values, pointers := make([]any, len(selected)), make([]any, len(selected))
		for i := range values {
			pointers[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if err := encoder.Encode(values); err != nil {
				rows.Close()
				t.Fatal(err)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		result[table] = fmt.Sprintf("%x", h.Sum(nil))
	}
	return result
}
