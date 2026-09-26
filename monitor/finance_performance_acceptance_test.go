package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Explicit opt-in: reads a closed local main snapshot and copies the closed
// facts snapshot into t.TempDir before simulating writes. No source/HTTP client
// is initialized, and no production database or upstream can be contacted.
func TestFinancePerformanceLocalAcceptance(t *testing.T) {
	durationText := os.Getenv("MONITOR_FINANCE_PERF_DURATION")
	if durationText == "" {
		t.Skip("requires explicit local performance acceptance duration")
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil || duration < time.Second || duration > time.Hour {
		t.Fatal("duration must be between 1s and 1h")
	}
	mainPath, factsPath := os.Getenv("MONITOR_FINANCE_ACCEPTANCE_MAIN_SNAPSHOT"), os.Getenv("MONITOR_FINANCE_ACCEPTANCE_FACTS_SNAPSHOT")
	if mainPath == "" || factsPath == "" {
		t.Fatal("closed local snapshots required")
	}
	for _, path := range []string{mainPath, factsPath} {
		if info, err := os.Stat(path + "-wal"); err == nil && info.Size() > 0 {
			t.Fatal("refusing a snapshot with live WAL")
		}
	}
	dir := t.TempDir()
	factsCopy := filepath.Join(dir, "facts.db")
	input, err := os.Open(factsPath)
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.OpenFile(factsCopy, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		input.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	input.Close()
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("snapshot copy failed: %v %v", copyErr, closeErr)
	}
	open := func(path, query string) *gorm.DB {
		u := url.URL{Scheme: "file", Path: path, RawQuery: query}
		db, err := gorm.Open(sqlite.Open(u.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	absMain, err := filepath.Abs(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: open(absMain, "mode=ro&immutable=1"), usageFactsDB: open(factsCopy, "_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"), cfg: Settings{
		StorePath: filepath.Join(dir, "cache-only.db"), UsageFactsStorePath: factsCopy,
		FinanceEnabled: true, FinanceStartDate: "2026-05-01", ChannelEconomicsReportEnabled: true,
		UsageFactsReadEnabled: true, UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: "newapi-hotlogs-complete-20260817-v1",
	}}
	defer m.Close()
	pool, err := m.usageFactsDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	paths := []string{
		"/finance/report?from=2026-05-01&to=2026-09-19",
		"/finance/report?from=2026-05-01&to=2026-05-31",
		"/finance/report?from=2026-09-01&to=2026-09-19",
	}
	serve := func(ctx context.Context, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
		m.serveFinanceOperatingReport(c)
		return w
	}
	baselines := make([]financeOperatingReport, len(paths))
	for i, path := range paths {
		started := time.Now()
		w := serve(context.Background(), path)
		if w.Code != 200 {
			t.Fatalf("baseline %d failed: %d", i, w.Code)
		}
		if err := json.Unmarshal(w.Body.Bytes(), &baselines[i]); err != nil {
			t.Fatal(err)
		}
		t.Logf("cold baseline range=%d elapsed=%s bytes=%d", i, time.Since(started), w.Body.Len())
	}
	if err := m.usageFactsDB.Exec("CREATE TABLE finance_perf_probe(id INTEGER PRIMARY KEY,value INTEGER NOT NULL)").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsDB.Exec("INSERT INTO finance_perf_probe VALUES(1,0)").Error; err != nil {
		t.Fatal(err)
	}
	// This transaction simulates local commit contention, not RDS traffic.
	writeOnce := func(ctx context.Context, revision int64) error {
		return m.usageFactsDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec("UPDATE finance_perf_probe SET value=value+1 WHERE id=1").Error; err != nil {
				return err
			}
			if revision > 0 {
				if err := tx.Exec("UPDATE finance_gift_boundary_states SET updated_at=? WHERE rowid=(SELECT rowid FROM finance_gift_boundary_states LIMIT 1)", revision).Error; err != nil {
					return err
				}
			}
			timer := time.NewTimer(10 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		})
	}
	var baselineWriterRate float64
	measure := func(name string, clients int, span time.Duration) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), span)
		defer cancel()
		var wg sync.WaitGroup
		var writes, writeErrors, responses, readErrors, mismatches, peak, refreshed atomic.Int64
		var timingsMu sync.Mutex
		var timings []time.Duration
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			n := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					n++
					var revision int64
					if n%100 == 0 {
						revision = time.Now().Unix()
					}
					if err := writeOnce(ctx, revision); err != nil {
						if ctx.Err() == nil {
							writeErrors.Add(1)
						}
					} else {
						writes.Add(1)
					}
				}
			}
		}()
		for client := 0; client < clients; client++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				tick := time.NewTicker(500 * time.Millisecond)
				defer tick.Stop()
				n := 0
				for {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
						which := (index + n) % len(paths)
						path := paths[which]
						n++
						if n%20 == 0 {
							path += "&fresh=1"
						}
						started := time.Now()
						w := serve(ctx, path)
						elapsed := time.Since(started)
						if ctx.Err() != nil {
							return
						}
						responses.Add(1)
						if w.Code != 200 {
							readErrors.Add(1)
							continue
						}
						var got financeOperatingReport
						if json.Unmarshal(w.Body.Bytes(), &got) != nil || !financePerformanceAmountsEqual(got, baselines[which]) {
							mismatches.Add(1)
						}
						if got.GeneratedAt > baselines[which].GeneratedAt {
							refreshed.Add(1)
						}
						timingsMu.Lock()
						timings = append(timings, elapsed)
						timingsMu.Unlock()
						active := int64(m.financeAsyncQueue.stats().Running)
						if active > peak.Load() {
							peak.Store(active)
						}
					}
				}
			}(client)
		}
		wg.Wait()
		m.financeAsyncQueue.mu.Lock()
		done := m.financeAsyncQueue.done
		m.financeAsyncQueue.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-time.After(40 * time.Second):
				t.Fatal("report worker did not finish after load")
			}
		}
		sort.Slice(timings, func(i, j int) bool { return timings[i] < timings[j] })
		p95 := time.Duration(0)
		if len(timings) > 0 {
			p95 = timings[(len(timings)-1)*95/100]
		}
		walBytes := int64(0)
		if info, err := os.Stat(factsCopy + "-wal"); err == nil {
			walBytes = info.Size()
		}
		t.Logf("phase=%s clients=%d duration=%s responses=%d errors=%d mismatches=%d p95=%s writer_commits=%d writer_errors=%d peak_report_jobs=%d wal_bytes=%d updated_responses=%d", name, clients, span, responses.Load(), readErrors.Load(), mismatches.Load(), p95, writes.Load(), writeErrors.Load(), peak.Load(), walBytes, refreshed.Load())
		writerRate := float64(writes.Load()) / span.Seconds()
		if clients == 0 {
			baselineWriterRate = writerRate
		}
		if mismatches.Load() != 0 {
			t.Fatal("amounts changed under concurrent writes")
		}
		if m.cfg.FinanceFastSnapshotEnabled && (readErrors.Load() != 0 || writeErrors.Load() != 0 || peak.Load() > 1 || p95 > time.Second) {
			t.Fatal("optimized performance acceptance failed")
		}
		if m.cfg.FinanceFastSnapshotEnabled && writerRate < baselineWriterRate*0.9 {
			t.Fatalf("writer throughput regressed: %.2f/s baseline %.2f/s", writerRate, baselineWriterRate)
		}
		if m.cfg.FinanceFastSnapshotEnabled && span >= time.Minute && refreshed.Load() == 0 {
			t.Fatal("continuous source revisions never produced an updated report")
		}
	}
	measure("writer-only", 0, 3*time.Second)
	for _, clients := range []int{1, 5, 10} {
		measure(fmt.Sprintf("baseline-%d", clients), clients, 3*time.Second)
	}
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	m.cfg.FinanceFactsReadIsolationEnabled = true
	for _, clients := range []int{1, 5, 10} {
		measure(fmt.Sprintf("optimized-%d", clients), clients, 3*time.Second)
	}
	measure("optimized-soak", 10, duration)
	if err := m.usageFactsDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)").Error; err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(factsCopy + "-wal"); err == nil && info.Size() != 0 {
		t.Fatal("WAL did not truncate after all readers/writers finished")
	}
}

func financePerformanceAmountsEqual(got, want financeOperatingReport) bool {
	return got.From == want.From && got.To == want.To &&
		reflect.DeepEqual(got.Statement, want.Statement) &&
		reflect.DeepEqual(got.Days, want.Days) && reflect.DeepEqual(got.Periods, want.Periods) &&
		reflect.DeepEqual(got.UserCoverage, want.UserCoverage) &&
		reflect.DeepEqual(got.GiftCoverage, want.GiftCoverage) && reflect.DeepEqual(got.UpstreamCoverage, want.UpstreamCoverage) &&
		reflect.DeepEqual(got.CostDetails, want.CostDetails) && reflect.DeepEqual(got.PairingAudit, want.PairingAudit) &&
		reflect.DeepEqual(got.CURCost, want.CURCost)
}
