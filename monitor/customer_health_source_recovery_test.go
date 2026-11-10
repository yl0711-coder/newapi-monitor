package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestCustomerHealthSourceRetriesStartupAndTodayWithoutSkippingGap(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CustomerHealthSourceEnabled = true
	now := chTestNow()
	day, target := customerHealthSourceRange(now)
	previous := day - 86400
	if err := m.saveCustomerHealthSourceCursor(previous, day-120); err != nil {
		t.Fatal(err)
	}
	// Earlier days are already proven so the test can observe the moving
	// current-day cadence after it repairs the previous day's failed tail.
	for d := day - customerHealthHistoryDays*86400; d < previous; d += 86400 {
		if err := m.storeDB.Save(&CustomerHealthDayCoverage{DayTs: d, ThroughTs: d + 86400,
			SemanticsVersion: customerHealthStabilityPolicyVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
			TTFTCoverageFromTs: d, TTFTCoverageThroughTs: d + 86400}).Error; err != nil {
			t.Fatal(err)
		}
	}
	chSeedCompany(t, m, "恢复客户", 101)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startupFailed, todayFailed, advanced := false, false, false
	failedFrom, failedTo := int64(0), int64(0)
	committedThrough := map[int64]int64{previous: day - 120, day: day}
	waits := 0
	m.runCustomerHealthSource(ctx, func() time.Time { return now }, func(_ context.Context, from, to int64) (int, error) {
		if !startupFailed || from == day+3600 && !todayFailed {
			if startupFailed {
				todayFailed = true
			}
			startupFailed = true
			failedFrom, failedTo = from, to
			return 0, errSourceNotReady
		}
		if failedTo != 0 && from >= day {
			if from != failedFrom || to != failedTo {
				t.Fatalf("failed interval was skipped: [%d,%d) -> [%d,%d)", failedFrom, failedTo, from, to)
			}
			failedTo = 0
		}
		sliceDay := customerHealthDayStart(from)
		if from > committedThrough[sliceDay] {
			t.Fatalf("unread gap before successful slice: %d > %d", from, committedThrough[sliceDay])
		}
		committedThrough[sliceDay] = max(committedThrough[sliceDay], to)
		if from >= day {
			row := CapacityUserMinuteSample{BucketTs: from, UserID: 101, ChannelID: 7, ModelName: "recovered", Grp: "g",
				TrafficClassVersion: stabilityTrafficClassificationVersion, CustomerHealthVersion: customerHealthStabilityPolicyVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
				Success: 1, TtftObserved: 1, FRTSumMs: ptrInt64(500), FRTSumObserved: 1, FRTSumVersion: frtSumSemanticsVersion}
			if err := m.storeDB.Save(&row).Error; err != nil {
				t.Fatal(err)
			}
		}
		return 1, nil
	}, func(_ context.Context, delay time.Duration) bool {
		waits++
		if waits > 6 || !m.customerHealthSourceRunning.Load() {
			t.Fatal("worker stopped or did not converge")
		}
		var cursor CustomerHealthSourceCursor
		if err := m.storeDB.First(&cursor, 1).Error; err != nil {
			t.Fatal(err)
		}
		if failedTo != 0 {
			if cursor.DayTs == customerHealthDayStart(failedFrom) && cursor.ThroughTs > failedFrom {
				t.Fatal("failure advanced the durable watermark", cursor)
			}
			if delay != 15*time.Second {
				t.Fatal("missing bounded retry", delay)
			}
			now = now.Add(time.Second)
			return true
		}
		if cursor.DayTs != day || cursor.ThroughTs != target || cursor.TTFTCoverageThroughTs != target {
			t.Fatal("today's request/FRT coverage did not recover", cursor)
		}
		if !advanced {
			advanced = true
			now = now.Add(time.Minute)
			target += 60
			return true
		}
		cancel()
		return false
	})
	if !startupFailed || !todayFailed || !advanced || m.customerHealthSourceRunning.Load() {
		t.Fatal("startup recovery, today's retry or continued polling was not exercised")
	}
	var previousProof CustomerHealthDayCoverage
	if err := m.storeDB.First(&previousProof, "day_ts=?", previous).Error; err != nil || previousProof.ThroughTs != day || previousProof.TTFTCoverageThroughTs != day {
		t.Fatal("previous day was replaced without closing its request/FRT gap", previousProof, err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), now)
	if err != nil || !report.Collection.MetricsAvailable || report.ToTs != target || len(report.Rows) != 1 || report.Rows[0].Total == 0 || len(report.Rows[0].PrimaryModels) == 0 {
		t.Fatal("recovered local facts were not available to shared report consumers", report, err)
	}
}

func TestCustomerHealthSourceNarrowsTimeoutAndCommitsOnlyReadPrefix(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	day, _ := customerHealthSourceRange(chTestNow())
	left, right := day, day+3600
	attempts := 0
	sample := func(_ context.Context, from, to int64) (int, error) {
		attempts++
		if from != left {
			t.Fatal("timeout skipped left edge")
		}
		if to-from > 900 {
			return 0, context.DeadlineExceeded
		}
		return 1, nil
	}
	for _, width := range []int64{3600, 1800, 900} {
		_, through, err := m.sampleCustomerHealthSourcePrefix(context.Background(), left, right, sample)
		if through != left+width || (err == nil) != (width == 900) {
			t.Fatalf("unexpected retry [%d,%d), error=%v", left, through, err)
		}
		if err == nil {
			if err := m.saveCustomerHealthSourceCursor(day, through); err != nil {
				t.Fatal(err)
			}
		}
	}
	var saved CustomerHealthSourceCursor
	if err := m.storeDB.First(&saved, 1).Error; err != nil || saved.ThroughTs != left+900 || saved.TTFTCoverageThroughTs != left+900 || attempts != 3 {
		t.Fatal("timeout certified unread suffix", saved, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := m.customerHealthSourceReadSeconds.Load()
	m.sampleCustomerHealthSourcePrefix(ctx, left, right, func(context.Context, int64, int64) (int, error) { return 0, ctx.Err() })
	if m.customerHealthSourceReadSeconds.Load() != before {
		t.Fatal("source epoch cancellation changed query retry width")
	}
}

func TestCustomerHealthSourceRetriesCursorReadFailureAndHonorsCancellation(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	injected := false
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register("customer_health_init_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "customer_health_source_cursors" && !injected {
			injected = true
			tx.AddError(errors.New("temporary local read failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer m.storeDB.Callback().Query().Remove("customer_health_init_failure")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads, waits := 0, 0
	m.runCustomerHealthSource(ctx, chTestNow, func(context.Context, int64, int64) (int, error) {
		reads++
		cancel()
		return 0, ctx.Err()
	}, func(context.Context, time.Duration) bool { waits++; return true })
	if !injected || waits != 1 || reads != 1 || m.customerHealthSourceRunning.Load() {
		t.Fatalf("cursor initialization retry or cancellation failed: injected=%v waits=%d reads=%d", injected, waits, reads)
	}
}

func TestCustomerHealthPreviousDayResumesPartialTailAcrossSourceEpochs(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	day, _ := customerHealthSourceRange(chTestNow())
	previous, initialThrough := day-86400, day-3600
	if err := m.saveCustomerHealthSourceCursor(previous, initialThrough); err != nil {
		t.Fatal(err)
	}
	wantFrom := initialThrough - customerHealthSourceReplaySeconds
	outage := errors.New("source connection interrupted after a committed minute")
	for attempt := 0; attempt < 22; attempt++ {
		// A new monitor object also discards every volatile retry hint. Only
		// the same durable database survives this simulated process restart.
		restarted := &Monitor{storeDB: m.storeDB, cfg: m.cfg}
		restarted.customerHealthSourceReadSeconds.Store(60)
		calls := 0
		err := restarted.finishPreviousCustomerHealthDayWith(context.Background(), day,
			func(_ context.Context, from, to int64) (int, error) {
				calls++
				if calls > 1 {
					return 0, outage
				}
				if from != wantFrom || to != from+60 {
					t.Fatalf("retry %d reread committed tail or skipped a gap: got [%d,%d), want [%d,%d)", attempt, from, to, wantFrom, wantFrom+60)
				}
				return 1, nil
			})
		if !errors.Is(err, outage) || calls != 2 {
			t.Fatalf("retry %d did not preserve the failed slice: calls=%d err=%v", attempt, calls, err)
		}
		wantFrom += 60
		var saved CustomerHealthSourceCursor
		if err := m.storeDB.First(&saved, 1).Error; err != nil {
			t.Fatal(err)
		}
		if saved.ThroughTs != max(initialThrough, wantFrom) || saved.TTFTCoverageThroughTs != saved.ThroughTs {
			t.Fatalf("replay progress certified unread data or lost existing coverage: %+v", saved)
		}
	}
}

func TestCustomerHealthReadCheckpointPreservesAtomicityAndLateReplay(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	day, _ := customerHealthSourceRange(chTestNow())
	through, target := day+3600, day+7200
	if err := m.saveCustomerHealthSourceCursor(day, through); err != nil {
		t.Fatal(err)
	}
	next := through - customerHealthSourceReplaySeconds + 60
	if err := m.saveCustomerHealthSourceReadCursor(day, next, target); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("checkpoint commit interrupted")
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("fail_read_checkpoint", func(tx *gorm.DB) {
		if tx.Statement.Table == "customer_health_source_cursors" {
			tx.AddError(injected)
		}
	}); err != nil {
		t.Fatal(err)
	}
	err := m.saveCustomerHealthSourceReadCursor(day, through+60, target)
	m.storeDB.Callback().Update().Remove("fail_read_checkpoint")
	if !errors.Is(err, injected) {
		t.Fatalf("checkpoint failure was hidden: %v", err)
	}
	resume, covered, err := m.loadCustomerHealthSourceCursor(day, target)
	if err != nil || resume != next || covered != through {
		t.Fatalf("failed transaction advanced a checkpoint or coverage: %d %d %v", resume, covered, err)
	}
	// The standard realtime writer may advance its own proven prefix, but
	// must preserve the backfill lane's unfinished replay checkpoint.
	if err := m.saveCustomerHealthSourceCursor(day, through+60); err != nil {
		t.Fatal(err)
	}
	resume, covered, err = m.loadCustomerHealthSourceCursor(day, target)
	if err != nil || resume != next || covered != through+60 {
		t.Fatalf("realtime publication erased retry progress: %d %d %v", resume, covered, err)
	}
	if err := m.saveCustomerHealthSourceReadCursor(day, target, target); err != nil {
		t.Fatal(err)
	}
	resume, _, err = m.loadCustomerHealthSourceCursor(day, target)
	if err != nil || resume != target {
		t.Fatalf("completed fixed target was restarted: %d %v", resume, err)
	}
	resume, _, err = m.loadCustomerHealthSourceCursor(day, target+60)
	if err != nil || resume != target-customerHealthSourceReplaySeconds {
		t.Fatalf("new cycle stopped replaying late rows: %d %v", resume, err)
	}
	if err := m.storeDB.Model(&CustomerHealthSourceCursor{}).Where("id=1").Update("ttft_semantics_version", ttftCoverageSemanticsVersion-1).Error; err != nil {
		t.Fatal(err)
	}
	resume, covered, err = m.loadCustomerHealthSourceCursor(day, target+60)
	if err != nil || resume != day || covered != day {
		t.Fatalf("old read checkpoint masked FRT semantics replay: %d %d %v", resume, covered, err)
	}
}

func TestCustomerHealthReadCheckpointMigratesLegacyCursor(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "legacy-cursor.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := db.Exec(`CREATE TABLE customer_health_source_cursors (
		id INTEGER PRIMARY KEY, day_ts INTEGER, through_ts INTEGER, semantics_version INTEGER,
		ttft_semantics_version INTEGER, ttft_coverage_from_ts INTEGER, ttft_coverage_through_ts INTEGER, updated_at INTEGER)`).Error; err != nil {
		t.Fatal(err)
	}
	day, _ := customerHealthSourceRange(chTestNow())
	through := day + 3600
	if err := db.Exec(`INSERT INTO customer_health_source_cursors VALUES (1,?,?,?,?,?,?,?)`,
		day, through, customerHealthStabilityPolicyVersion, ttftCoverageSemanticsVersion, day, through, through).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&CustomerHealthSourceCursor{}, &CustomerHealthDayCoverage{}); err != nil {
		t.Fatal(err)
	}
	var migrated CustomerHealthSourceCursor
	if err := db.First(&migrated, 1).Error; err != nil || migrated.DayTs != day || migrated.ThroughTs != through ||
		migrated.TTFTCoverageFromTs != day || migrated.TTFTCoverageThroughTs != through ||
		migrated.ReadNextTs != 0 || migrated.ReadTargetTs != 0 {
		t.Fatalf("additive migration changed existing proof: %+v %v", migrated, err)
	}
	m := &Monitor{storeDB: db}
	if err := m.saveCustomerHealthSourceReadCursor(day, through-600, through+3600); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&migrated, 1).Error; err != nil || migrated.ReadNextTs != through-600 || migrated.ThroughTs != through {
		t.Fatalf("migrated cursor cannot independently save read progress: %+v %v", migrated, err)
	}
}
