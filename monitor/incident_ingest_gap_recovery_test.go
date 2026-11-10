package monitor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gorm.io/gorm"
)

// 2026-10-12 交接中的三个故障复现，纳入正式回归；只使用合成事实与临时 SQLite。
func TestIncidentB3CoverageStillMissingMustRemainBlocked(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: reviewRegBase}
	now := reviewRegBase + 300 + int64(ingest.FinalizeLagSeconds)
	if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now); err != nil {
		t.Fatal(err)
	}
	before := reviewRegState(t, m, incidentIngestWindowCoverageKey, reviewRegBase+300)
	if before.DataComplete || before.ProcessState != incidentProcessAwaitingSource {
		t.Fatalf("fixture is not a blocked window: %+v", before)
	}
	// Neither coverage nor source facts have arrived before the next queue turn.
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+3600); err != nil {
		t.Fatal(err)
	}
	after := reviewRegState(t, m, incidentIngestWindowCoverageKey, reviewRegBase+300)
	if after.DataComplete || after.ProcessState != incidentProcessAwaitingSource || after.WindowState == incidentWindowStateNoSample {
		t.Errorf("coverage still absent but blocker became process=%s window=%s complete=%v", after.ProcessState, after.WindowState, after.DataComplete)
	}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatal(err)
	}
	if cursor.CompleteThroughTs > reviewRegBase {
		t.Errorf("complete watermark crossed uncovered window: complete=%d start=%d", cursor.CompleteThroughTs, reviewRegBase)
	}
	if after.SourceCheckAttempts <= before.SourceCheckAttempts || after.NextSourceCheckAt <= now+3600 {
		t.Errorf("missing coverage must retain retry progress and backoff: %+v", after)
	}
}

func TestIncidentB3UnfilledGapMustStayInSourceQueue(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	// Use the previous day's last window. A same-day live cursor itself supplies
	// a valid coverage proof, so it would not be a genuine missing-coverage case.
	from := customerHealthDayStart(reviewRegBase) - 300
	d := reviewRegDecision(t, core, ingest, from, true)
	now := from + 300 + int64(ingest.FinalizeLagSeconds)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
		t.Fatal(err)
	}
	row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	reviewRegSeedFacts(t, m, from, 10, 90)
	// Live ingestion has advanced, but there is still no continuous coverage proof.
	live := CustomerHealthSourceCursor{ID: 1, DayTs: customerHealthDayStart(reviewRegBase),
		ThroughTs: reviewRegBase + 900, SemanticsVersion: customerHealthStabilityPolicyVersion}
	if err := m.storeDB.Save(&live).Error; err != nil {
		t.Fatal(err)
	}
	proof, err := m.incidentRequestCoverage(ctx, d.WindowFromTs, d.WindowToTs)
	if err != nil {
		t.Fatal(err)
	}
	if proof.RequestsComplete {
		t.Fatal("invalid fixture: expected a genuine previous-day coverage gap")
	}
	if err := m.advanceOneIncidentWindow(ctx, core, ingest, row, now+60); err != nil {
		t.Fatal(err)
	}
	after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if after.GapRecheckClosed || after.DataComplete {
		t.Errorf("unfilled gap closed before 7-day deadline: closed=%v complete=%v measured=%s", after.GapRecheckClosed, after.DataComplete, after.MeasuredWindowState)
	}
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if backlog != 1 {
		t.Errorf("unfilled gap must remain queued: pending_backlog=%d want=1", backlog)
	}
	var revisions int64
	if err := m.storeDB.Model(&IncidentIngestRevision{}).Where("dedup_key = ? AND kind IN (?, ?)",
		d.DedupKey, "gap_backfilled", "gap_backfilled_anomalous").Count(&revisions).Error; err != nil {
		t.Fatal(err)
	}
	if revisions != 0 {
		t.Errorf("unfilled gap was recorded as backfilled: revisions=%d", revisions)
	}
}

func TestIncidentB3FailureRecordWriteErrorMustStopScan(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	const queryCB = "handoff:coverage_query_failure"
	const writeCB = "handoff:failure_record_write_failure"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(queryCB, func(tx *gorm.DB) {
		if tx.Statement.Table == "customer_health_day_coverages" {
			_ = tx.AddError(fmt.Errorf("injected coverage query failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(writeCB, func(tx *gorm.DB) {
		if tx.Statement.Table == "incident_ingest_window_states" {
			_ = tx.AddError(fmt.Errorf("injected failure-record persistence failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = m.storeDB.Callback().Query().Remove(queryCB)
		_ = m.storeDB.Callback().Create().Remove(writeCB)
	})
	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: reviewRegBase}
	now := reviewRegBase + 300 + int64(ingest.FinalizeLagSeconds)
	err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now)
	if err == nil {
		t.Error("scan returned success although both source read and failure persistence failed")
	}
	if cursor.ScanThroughTs > reviewRegBase {
		t.Errorf("scan consumed an unpersisted window: scan=%d original=%d", cursor.ScanThroughTs, reviewRegBase)
	}
	var rows int64
	if err := m.storeDB.Model(&IncidentIngestWindowState{}).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("persisted window states=%d", rows)
}

func TestIncidentB3CoverageBlockTransfersOnlyAfterPersistence(t *testing.T) {
	for _, scenario := range []string{"no_sample", "awaiting_source", "mixed_version", "write_failure"} {
		t.Run(scenario, func(t *testing.T) {
			core, ingest := incidentIngestTestEnv(t)
			m := openIncidentTestStore(t)
			ctx := context.Background()
			now := reviewRegBase + 300 + int64(ingest.FinalizeLagSeconds)
			cursor := IncidentIngestCursor{ScanThroughTs: reviewRegBase}
			if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now); err != nil {
				t.Fatal(err)
			}
			key, err := incidentKeyForFacts(reviewRegFacts())
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "no_sample" {
				reviewRegSeedFacts(t, m, reviewRegBase, 10, 90)
			}
			if scenario != "awaiting_source" {
				seedDayCoverage(t, m, reviewRegBase, reviewRegBase+300)
			}
			if scenario == "mixed_version" {
				f := reviewRegFacts()
				seedCapacitySample(t, m, CapacityUserMinuteSample{BucketTs: reviewRegBase + 60,
					UserID: 2, ChannelID: f.ChannelID, ModelName: f.ModelName, Grp: f.Grp,
					Success: 1, TrafficClassVersion: stabilityTrafficClassificationVersion - 1})
			}
			const cb = "b3:real_window_write_failure"
			if scenario == "write_failure" {
				if err := m.storeDB.Callback().Create().Before("gorm:create").Register(cb, func(tx *gorm.DB) {
					if row, ok := tx.Statement.Dest.(*IncidentIngestWindowState); ok && row.DedupKey == key.DedupKey() {
						_ = tx.AddError(errors.New("injected real-key write failure"))
					}
				}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = m.storeDB.Callback().Create().Remove(cb) })
			}
			if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+3600); err != nil {
				t.Fatal(err)
			}
			placeholder := reviewRegState(t, m, incidentIngestWindowCoverageKey, reviewRegBase+300)
			if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "no_sample":
				if !placeholder.DataComplete || placeholder.WindowState != incidentWindowStateNoSample || cursor.CompleteThroughTs != reviewRegBase+300 {
					t.Fatalf("proven empty window did not finish: %+v cursor=%+v", placeholder, cursor)
				}
			case "write_failure":
				if placeholder.DataComplete || cursor.CompleteThroughTs != reviewRegBase {
					t.Fatalf("released placeholder before real-key state persisted: %+v", placeholder)
				}
				if err := m.storeDB.Callback().Create().Remove(cb); err != nil {
					t.Fatal(err)
				}
				if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+7200); err != nil {
					t.Fatal(err)
				}
				real := reviewRegState(t, m, key.DedupKey(), reviewRegBase+300)
				if real.ProcessState != incidentProcessDone || !real.DataComplete {
					t.Fatalf("real-key write did not recover: %+v", real)
				}
			default:
				real := reviewRegState(t, m, key.DedupKey(), reviewRegBase+300)
				if !placeholder.DataComplete || placeholder.ProcessState != incidentProcessDone ||
					placeholder.WindowState == incidentWindowStateNoSample || real.DataComplete ||
					real.ProcessState != incidentProcessAwaitingSource || cursor.CompleteThroughTs != reviewRegBase {
					t.Fatalf("real-key blocker must take over without claiming no traffic: placeholder=%+v real=%+v cursor=%+v", placeholder, real, cursor)
				}
			}
		})
	}
}

func TestIncidentB3ReadFailureTransfersToCoverageBlock(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	now := reviewRegBase + 300 + int64(ingest.FinalizeLagSeconds)
	if err := m.reportIncidentIngestWindowError(ctx, reviewRegBase, reviewRegBase+300,
		errors.New("previous source read failure"), ingest, now); err != nil {
		t.Fatal(err)
	}
	cursor := IncidentIngestCursor{ScanThroughTs: reviewRegBase + 300}
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+3600); err != nil {
		t.Fatal(err)
	}
	failure := reviewRegState(t, m, incidentIngestWindowErrorKey, reviewRegBase+300)
	coverage := reviewRegState(t, m, incidentIngestWindowCoverageKey, reviewRegBase+300)
	if failure.ProcessState != incidentProcessDone || coverage.ProcessState != incidentProcessAwaitingSource || coverage.DataComplete {
		t.Fatalf("read recovery must hand off to a durable coverage blocker: failure=%+v coverage=%+v", failure, coverage)
	}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatal(err)
	}
	if cursor.CompleteThroughTs != reviewRegBase {
		t.Fatalf("read recovery is not coverage recovery: %+v", cursor)
	}
}

func TestIncidentB3GapWaitsForUsableBackfill(t *testing.T) {
	for _, state := range []string{incidentWindowStateWaitCoverage, incidentWindowStateGapObserved, incidentWindowStateMixedVersion} {
		t.Run(state, func(t *testing.T) {
			core, ingest := incidentIngestTestEnv(t)
			m := openIncidentTestStore(t)
			ctx := context.Background()
			from := customerHealthDayStart(reviewRegBase) - 300
			d := reviewRegDecision(t, core, ingest, from, true)
			now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
			if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
				t.Fatal(err)
			}
			original := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			reviewRegSeedFacts(t, m, from, 10, 90)
			if state == incidentWindowStateGapObserved {
				live := CustomerHealthSourceCursor{ID: 1, DayTs: customerHealthDayStart(reviewRegBase),
					ThroughTs: reviewRegBase + 900, SemanticsVersion: customerHealthStabilityPolicyVersion}
				if err := m.storeDB.Save(&live).Error; err != nil {
					t.Fatal(err)
				}
			}
			old := CapacityUserMinuteSample{BucketTs: from + 60, UserID: 2,
				ChannelID: reviewRegFacts().ChannelID, ModelName: reviewRegFacts().ModelName,
				Grp: reviewRegFacts().Grp, Success: 1, TrafficClassVersion: stabilityTrafficClassificationVersion - 1}
			if state == incidentWindowStateMixedVersion {
				seedDayCoverage(t, m, from, from+300)
				seedCapacitySample(t, m, old)
			}
			cursor := IncidentIngestCursor{ScanThroughTs: from + 300}
			for turn := int64(1); turn <= 2; turn++ {
				checkAt := now + turn*3600
				if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, checkAt); err != nil {
					t.Fatal(err)
				}
				row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				if row.GapRecheckClosed || row.DataComplete || row.MeasuredWindowState != state ||
					row.NextSourceCheckAt <= checkAt || row.SourceCheckAttempts != int(turn) || row.RecheckAttempts != 0 ||
					row.WindowState != original.WindowState || row.FinalizedAt != original.FinalizedAt || row.Action != original.Action {
					t.Fatalf("unfinished backfill lost queue/snapshot semantics: %+v", row)
				}
				if backlog, err := m.countIncidentPendingBacklog(ctx); err != nil || backlog != 1 {
					t.Fatalf("backlog=%d err=%v", backlog, err)
				}
				revisions := b3ReviewRevisions(t, m, d)
				want := 0
				if state != incidentWindowStateGapObserved {
					want = 1
				}
				if len(revisions) != want {
					t.Fatalf("incomplete-state revision count=%d want=%d", len(revisions), want)
				}
				for _, revision := range revisions {
					if revision.Kind != "revised_after_finalize" || revision.NewWindowState != state {
						t.Fatalf("unfilled gap recorded as successful backfill: %+v", revision)
					}
				}
			}
			seedDayCoverage(t, m, from, from+300)
			if state == incidentWindowStateMixedVersion {
				old.TrafficClassVersion = stabilityTrafficClassificationVersion
				seedCapacitySample(t, m, old)
			}
			if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+10800); err != nil {
				t.Fatal(err)
			}
			after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			if !after.GapRecheckClosed || !after.DataComplete || after.MeasuredWindowState != incidentWindowStateAnomaly ||
				after.WindowState != original.WindowState || after.Action != original.Action || after.FinalizedAt != original.FinalizedAt {
				t.Fatalf("usable backfill must close without changing original decision: %+v", after)
			}
			revisions := b3ReviewRevisions(t, m, d)
			want := 1
			if state != incidentWindowStateGapObserved {
				want = 2
			}
			if len(revisions) != want || revisions[len(revisions)-1].Kind != "gap_backfilled_anomalous" {
				t.Fatalf("usable backfill must add one successful revision: %+v", revisions)
			}
			assertIncidentB3Count(t, m, &Incident{}, 0)
			assertIncidentB3Count(t, m, &IncidentObservation{}, 0)
			if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil || cursor.CompleteThroughTs != from+300 {
				t.Fatalf("completed backfill must release watermark: %+v err=%v", cursor, err)
			}
		})
	}
}

func TestIncidentB3FinalizedGapExpiresWithoutChangingDecision(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	d := reviewRegDecision(t, core, ingest, reviewRegBase, true)
	now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
		t.Fatal(err)
	}
	original := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	cursor := IncidentIngestCursor{ScanThroughTs: d.WindowToTs}
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, incidentGapRecheckDeadline(d.WindowToTs)+1); err != nil {
		t.Fatal(err)
	}
	after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if !after.GapRecheckClosed || after.DataComplete || after.FinalizedAt != original.FinalizedAt || after.Action != original.Action {
		t.Fatalf("expiry must preserve the finalized gap: %+v", after)
	}
	if backlog, err := m.countIncidentPendingBacklog(ctx); err != nil || backlog != 0 {
		t.Fatalf("expired gap should leave source queue: %d err=%v", backlog, err)
	}
	if gaps, err := m.countIncidentTerminalGaps(ctx); err != nil || gaps != 1 {
		t.Fatalf("expired gap remains a terminal gap: %d err=%v", gaps, err)
	}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil || cursor.CompleteThroughTs != reviewRegBase {
		t.Fatalf("expiry must not erase coverage gap: %+v err=%v", cursor, err)
	}
	assertIncidentB3Count(t, m, &IncidentIngestRevision{}, 0)
	assertIncidentB3Count(t, m, &IncidentObservation{}, 0)
}

// 整窗重扫也是一个提交入口，必须遵守已定稿缺口不追溯建单的规则。
func TestIncidentB3RescanPreservesFinalizedGap(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	d := reviewRegDecision(t, core, ingest, reviewRegBase, true)
	now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
		t.Fatal(err)
	}
	original := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	reviewRegSeedFacts(t, m, reviewRegBase, 10, 90)
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+300)
	if err := m.processIncidentWindow(ctx, core, ingest, reviewRegBase, reviewRegBase+300, now+3600); err != nil {
		t.Fatal(err)
	}
	assertIncidentB3Count(t, m, &Incident{}, 0)
	assertIncidentB3Count(t, m, &IncidentObservation{}, 0)
	assertIncidentB3Count(t, m, &IncidentIngestRevision{}, 1)
	after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if after.WindowState != original.WindowState || after.Action != original.Action || !after.DataComplete {
		t.Fatalf("rescan changed finalized decision: %+v", after)
	}
}

func TestIncidentB3ScanStopsAtLastDurableWindowAndRecoversAfterRestart(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	ctx := context.Background()
	path := t.TempDir() + "/restart.db"
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(path); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 3; i++ {
		reviewRegSeedFacts(t, m, reviewRegBase+i*300, 10, 90)
	}
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+900)
	if err := m.saveIncidentIngestCursor(ctx, IncidentIngestCursor{ScanThroughTs: reviewRegBase}); err != nil {
		t.Fatal(err)
	}
	readErr, writeErr := errors.New("source unavailable"), errors.New("cannot persist retry")
	queries := 0
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register("b3:second_window_read", func(tx *gorm.DB) {
		if tx.Statement.Table == "customer_health_day_coverages" {
			queries++
			if queries > 2 { // Each successful window reads its proof and global frontier.
				_ = tx.AddError(readErr)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("b3:retry_record_write", func(tx *gorm.DB) {
		if row, ok := tx.Statement.Dest.(*IncidentIngestWindowState); ok && row.DedupKey == incidentIngestWindowErrorKey {
			_ = tx.AddError(writeErr)
		}
	}); err != nil {
		t.Fatal(err)
	}
	now := reviewRegBase + 900 + int64(ingest.FinalizeLagSeconds)
	err := m.runIncidentIngestTurn(ctx, core, ingest, now)
	if err == nil {
		t.Fatal("turn must report failed persistence")
	}
	cursor, err := m.loadIncidentIngestCursor(ctx)
	if err != nil || cursor.ScanThroughTs != reviewRegBase+300 || cursor.CompleteThroughTs != reviewRegBase+300 {
		t.Fatalf("cursor must stop at last durable window: %+v err=%v", cursor, err)
	}
	assertIncidentB3Count(t, m, &IncidentIngestWindowState{}, 1)
	db, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := restarted.openStore(path); err != nil {
		t.Fatal(err)
	}
	if err := restarted.runIncidentIngestTurn(ctx, core, ingest, now); err != nil {
		t.Fatal(err)
	}
	cursor, err = restarted.loadIncidentIngestCursor(ctx)
	if err != nil || cursor.ScanThroughTs != reviewRegBase+900 || cursor.CompleteThroughTs != reviewRegBase+900 {
		t.Fatalf("restart must resume unconsumed window: %+v err=%v", cursor, err)
	}
	assertIncidentB3Count(t, restarted, &IncidentIngestWindowState{}, 3)
	assertIncidentB3Count(t, restarted, &Incident{}, 1)
	assertIncidentB3Count(t, restarted, &IncidentObservation{}, 3)
}

func TestIncidentB3PersistedFailureDoesNotStopOtherWindows(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+600)
	reviewRegSeedFacts(t, m, reviewRegBase+300, 10, 90)
	failed := false
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register("b3:one_read_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "customer_health_day_coverages" && !failed {
			failed = true
			_ = tx.AddError(errors.New("one source read failed"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove("b3:one_read_failure") })
	cursor := IncidentIngestCursor{ScanThroughTs: reviewRegBase}
	now := reviewRegBase + 600 + int64(ingest.FinalizeLagSeconds)
	if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now); err != nil {
		t.Fatal(err)
	}
	failure := reviewRegState(t, m, incidentIngestWindowErrorKey, reviewRegBase+300)
	key, err := incidentKeyForFacts(reviewRegFacts())
	if err != nil {
		t.Fatal(err)
	}
	later := reviewRegState(t, m, key.DedupKey(), reviewRegBase+600)
	if failure.ProcessState != incidentProcessFailed || failure.LastError == "" ||
		later.ProcessState != incidentProcessDone || cursor.ScanThroughTs != reviewRegBase+600 {
		t.Fatalf("persisted failure should be isolated: failure=%+v later=%+v cursor=%+v", failure, later, cursor)
	}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil || cursor.CompleteThroughTs != reviewRegBase {
		t.Fatalf("isolated failure must still block completeness: %+v err=%v", cursor, err)
	}
}

func assertIncidentB3Count(t *testing.T, m *Monitor, model any, want int64) {
	t.Helper()
	var n int64
	if err := m.storeDB.Model(model).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Errorf("%T rows=%d want=%d", model, n, want)
	}
}

func TestIncidentB3InitialScanFailureKeepsOriginalStart(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	const queryCB = "b3:initial_source_failure"
	const writeCB = "b3:initial_record_failure"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(queryCB, func(tx *gorm.DB) {
		if tx.Statement.Table == "customer_health_day_coverages" {
			_ = tx.AddError(errors.New("source read failed on first start"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(writeCB, func(tx *gorm.DB) {
		if tx.Statement.Table == "incident_ingest_window_states" {
			_ = tx.AddError(errors.New("initial failure record cannot be saved"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = m.storeDB.Callback().Query().Remove(queryCB)
		_ = m.storeDB.Callback().Create().Remove(writeCB)
	})
	now := reviewRegBase + int64(ingest.RecheckWindowSeconds+ingest.FinalizeLagSeconds)
	for turn := int64(0); turn < 2; turn++ {
		if err := m.runIncidentIngestTurn(ctx, core, ingest, now+turn*300); err == nil {
			t.Fatal("first-start persistence failure must be reported")
		}
		cursor, err := m.loadIncidentIngestCursor(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if cursor.ScanThroughTs != reviewRegBase {
			t.Fatalf("failed initial window must retain original start across turns: got=%d want=%d", cursor.ScanThroughTs, reviewRegBase)
		}
	}
	assertIncidentB3Count(t, m, &IncidentIngestWindowState{}, 0)
}
