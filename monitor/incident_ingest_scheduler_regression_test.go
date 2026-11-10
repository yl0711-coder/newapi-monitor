package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
)

// Run the actual turn, fail either completeness query, reopen SQLite, and then
// resume through a still-incomplete turn and a successful source backfill.
func TestIncidentB3SchedulerTurnPreservesWatermarkRollback(t *testing.T) {
	for _, query := range []string{"windows", "gap_summary"} {
		for _, saveFails := range []bool{false, true} {
			name := query
			if saveFails {
				name += "/cursor_write_failure"
			}
			t.Run(name, func(t *testing.T) {
				core, ingest := incidentIngestTestEnv(t)
				storePath := filepath.Join(t.TempDir(), "scheduler.db")
				m := openIncidentTestStore2(t, storePath)
				defer func() { closeTestStore(m) }()
				ctx := context.Background()
				d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
				now := d.WindowToTs + int64(ingest.FinalizeLagSeconds) + 360
				reviewRegSeedFacts(t, m, d.WindowFromTs, 10, 90)
				seedDayCoverage(t, m, d.WindowFromTs, d.WindowToTs+3600)
				if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now-360); err != nil {
					t.Fatal(err)
				}
				original := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				observations := b3ReviewObservations(t, m)
				cursor := IncidentIngestCursor{ScanThroughTs: d.WindowToTs,
					CompleteThroughTs: d.WindowToTs, TickCount: int64(ingest.RecheckEveryTicks) - 1}
				if err := m.saveIncidentIngestCursor(ctx, cursor); err != nil {
					t.Fatal(err)
				}
				b3ReviewSeedMixed(t, m, d.WindowFromTs)
				queryFault := errors.New("scheduler watermark read failure")
				writeFault := errors.New("scheduler cursor persistence failure")
				reads, writes := 0, 0
				const queryCB = "b3-scheduler:watermark-read"
				const writeCB = "b3-scheduler:cursor-write"
				oldDB := m.storeDB
				if err := oldDB.Callback().Row().Before("gorm:row").Register(queryCB, func(tx *gorm.DB) {
					table, column := "incident_ingest_window_states", "MIN(window_from_ts)"
					if query == "gap_summary" {
						table, column = "incident_ingest_gap_summaries", "MIN(gap_from_ts)"
					}
					if tx.Statement.Table == table && strings.Contains(strings.Join(tx.Statement.Selects, " "), column) {
						reads++
						_ = tx.AddError(queryFault)
					}
				}); err != nil {
					t.Fatal(err)
				}
				failWrite := func(tx *gorm.DB) {
					// The revision's conservative column update must still commit.
					// Only fail the final turn-level cursor save.
					if _, ok := tx.Statement.Dest.(*IncidentIngestCursor); ok && saveFails {
						writes++
						_ = tx.AddError(writeFault)
					}
				}
				if err := oldDB.Callback().Create().Before("gorm:create").Register(writeCB, failWrite); err != nil {
					t.Fatal(err)
				}
				if err := oldDB.Callback().Update().Before("gorm:update").Register(writeCB, failWrite); err != nil {
					t.Fatal(err)
				}
				cleaned := false
				restore := func() {
					if !cleaned {
						_ = oldDB.Callback().Row().Remove(queryCB)
						_ = oldDB.Callback().Create().Remove(writeCB)
						_ = oldDB.Callback().Update().Remove(writeCB)
						cleaned = true
					}
				}
				t.Cleanup(restore)
				err := m.runIncidentIngestTurn(ctx, core, ingest, now)
				if reads != 1 || err == nil || !strings.Contains(err.Error(), queryFault.Error()) {
					t.Fatalf("watermark failure was not propagated: reads=%d err=%v", reads, err)
				}
				if saveFails && (writes != 1 || !strings.Contains(err.Error(), writeFault.Error())) {
					t.Fatalf("cursor save failure was swallowed: writes=%d err=%v", writes, err)
				}
				row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				if row.DataComplete || row.MeasuredWindowState != incidentWindowStateMixedVersion {
					t.Fatalf("late degradation did not persist: %+v", row)
				}
				after, err := m.loadIncidentIngestCursor(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if after.CompleteThroughTs != d.WindowFromTs {
					t.Errorf("turn overwrote committed rollback: complete=%d want=%d", after.CompleteThroughTs, d.WindowFromTs)
				}
				if !saveFails && (after.ScanThroughTs != d.WindowToTs+300 || after.TickCount != cursor.TickCount+1 || after.UpdatedAt < now) {
					t.Errorf("failed completeness query lost durable scan progress: %+v", after)
				}
				restore()
				closeTestStore(m)
				m = openIncidentTestStore2(t, storePath)
				reopened, err := m.loadIncidentIngestCursor(ctx)
				if err != nil || !reflect.DeepEqual(after, reopened) {
					t.Fatalf("reopened cursor differs: %+v err=%v", reopened, err)
				}
				waitAt := row.NextSourceCheckAt + 1
				if err := m.runIncidentIngestTurn(ctx, core, ingest, waitAt); err != nil {
					t.Fatal(err)
				}
				stillWaiting := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				recoveredCursor, err := m.loadIncidentIngestCursor(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if stillWaiting.DataComplete || recoveredCursor.CompleteThroughTs != d.WindowFromTs {
					t.Fatal("successful recomputation must retain the unresolved blocker")
				}
				if err := m.storeDB.Where("user_id = ?", 2).Delete(&CapacityUserMinuteSample{}).Error; err != nil {
					t.Fatal(err)
				}
				reviewRegSeedFacts(t, m, d.WindowFromTs, 9999, 1)
				if err := m.runIncidentIngestTurn(ctx, core, ingest, stillWaiting.NextSourceCheckAt+1); err != nil {
					t.Fatal(err)
				}
				complete := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				finalCursor, err := m.loadIncidentIngestCursor(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !complete.DataComplete || finalCursor.CompleteThroughTs != finalCursor.ScanThroughTs || finalCursor.CompleteThroughTs < d.WindowToTs {
					t.Fatalf("usable backfill failed to restore complete watermark: %+v", finalCursor)
				}
				if complete.SampleTotal == nil || *complete.SampleTotal != 100 || complete.MeasuredSampleTotal == nil || *complete.MeasuredSampleTotal != 10000 ||
					complete.Action != original.Action || complete.Outcome != original.Outcome ||
					!reflect.DeepEqual(observations, b3ReviewObservations(t, m)) || len(b3ReviewRevisions(t, m, d)) != 2 {
					t.Fatal("turn recovery lost the original decision or duplicated revisions")
				}
			})
		}
	}
}

func TestIncidentB3SchedulerBackfillIsRevisedOnce(t *testing.T) {
	for _, order := range []string{"placeholder_first", "key_first"} {
		t.Run(order, func(t *testing.T) {
			core, ingest := incidentIngestTestEnv(t)
			ingest.PendingBatch = incidentIngestMinPendingBatch
			m := openIncidentTestStore(t)
			defer closeTestStore(m)
			ctx := context.Background()
			d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
			now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
			reviewRegSeedFacts(t, m, d.WindowFromTs, 10, 90)
			seedDayCoverage(t, m, d.WindowFromTs, d.WindowToTs)
			if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
				t.Fatal(err)
			}
			original := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			observations := b3ReviewObservations(t, m)
			b3ReviewSeedMixed(t, m, d.WindowFromTs)
			if err := m.recheckOneIncidentWindow(ctx, core, ingest, original, now+1); err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Where("user_id = ?", 2).Delete(&CapacityUserMinuteSample{}).Error; err != nil {
				t.Fatal(err)
			}
			reviewRegSeedFacts(t, m, d.WindowFromTs, 9999, 1)
			cursor := b3SchedulerRetryCursor(t, m, ingest, d, now, order)
			if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+600); err != nil {
				t.Fatal(err)
			}
			row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			revisions := b3ReviewRevisions(t, m, d)
			if !row.DataComplete || row.MeasuredSampleTotal == nil || *row.MeasuredSampleTotal != 10000 ||
				len(revisions) != 2 || revisions[1].Kind != "gap_backfilled" {
				t.Fatalf("one degradation and one backfill need two revisions: row=%+v revisions=%+v", row, revisions)
			}
			if row.Action != original.Action || row.Outcome != original.Outcome || row.SampleTotal == nil || *row.SampleTotal != 100 ||
				!reflect.DeepEqual(observations, b3ReviewObservations(t, m)) {
				t.Fatal("backfill rewrote original engine results")
			}
			if got := impactSampleTotal(t, m, row.IncidentID, incidentImpactDimChannel, "777"); got != 10000 {
				t.Fatalf("impact=%d want=10000", got)
			}
			if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+1200); err != nil {
				t.Fatal(err)
			}
			if len(b3ReviewRevisions(t, m, d)) != 2 {
				t.Fatal("completed task produced another revision")
			}
		})
	}
}

func TestIncidentB3SchedulerFrozenResultsSurviveQueueOrder(t *testing.T) {
	for _, order := range []string{"placeholder_first", "key_first"} {
		for _, stage := range []string{incidentProcessInputFrozen, incidentProcessEngineDone} {
			t.Run(order+"/"+stage, func(t *testing.T) {
				core, ingest := incidentIngestTestEnv(t)
				ingest.PendingBatch = incidentIngestMinPendingBatch
				m := openIncidentTestStore(t)
				defer closeTestStore(m)
				ctx := context.Background()
				d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
				now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
				row := incidentWindowStateRowFrom(d, 7, now)
				row.ProcessState, row.FinalizedAt = stage, now
				if stage == incidentProcessEngineDone {
					result, err := m.IngestIncidentObservation(ctx, d.observationInputFor(core.MinSample))
					if err != nil {
						t.Fatal(err)
					}
					row.IncidentID, row.SegmentID = result.IncidentID, result.SegmentID
					row.Severity, row.Outcome, row.Action = result.Severity, result.Outcome, result.Action
				}
				if err := m.saveIncidentWindowState(ctx, row); err != nil {
					t.Fatal(err)
				}
				reviewRegSeedFacts(t, m, d.WindowFromTs, 9999, 1)
				seedDayCoverage(t, m, d.WindowFromTs, d.WindowToTs)
				cursor := b3SchedulerRetryCursor(t, m, ingest, d, now, order)
				if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+600); err != nil {
					t.Fatal(err)
				}
				after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				if after.ProcessState != incidentProcessDone || after.IncidentID == "" || after.Action != "created" ||
					after.SampleTotal == nil || *after.SampleTotal != 100 || after.MeasuredAt != 0 {
					t.Fatalf("stale task replaced the recovered engine result: %+v", after)
				}
				if stage == incidentProcessEngineDone && (after.IncidentID != row.IncidentID || after.SegmentID != row.SegmentID ||
					after.Severity != row.Severity || after.Outcome != row.Outcome || after.Action != row.Action) {
					t.Fatal("engine_done recovery overwrote saved result fields")
				}
				observations := b3ReviewObservations(t, m)
				if len(observations) != 1 || len(b3ReviewRevisions(t, m, d)) != 0 {
					t.Fatal("recovery must have one observation and no unaudited new facts")
				}
				if got := impactSampleTotal(t, m, after.IncidentID, incidentImpactDimChannel, "777"); got != 100 {
					t.Fatalf("recovery impact=%d want frozen 100", got)
				}
				if err := m.recheckOneIncidentWindow(ctx, core, ingest, after, now+660); err != nil {
					t.Fatal(err)
				}
				revised := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				if revised.Action != after.Action || revised.MeasuredSampleTotal == nil || *revised.MeasuredSampleTotal != 10000 ||
					len(b3ReviewRevisions(t, m, d)) != 1 || !reflect.DeepEqual(observations, b3ReviewObservations(t, m)) {
					t.Fatal("late facts must use a separate revision and retain engine results")
				}
			})
		}
	}
}

// State changes after selection are possible within a single turn. Exercise
// eligibility changes directly as well as the whole-window retry above.
func TestIncidentB3SchedulerReloadHonorsLatestEligibility(t *testing.T) {
	for _, change := range []string{"source_deferred", "retry_deferred", "permanent", "gap_closed", "deleted"} {
		t.Run(change, func(t *testing.T) {
			core, ingest := incidentIngestTestEnv(t)
			m := openIncidentTestStore(t)
			defer closeTestStore(m)
			ctx := context.Background()
			d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
			now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
			row := incidentWindowStateRowFrom(d, 7, now)
			row.ProcessState, row.FinalizedAt = incidentProcessInputFrozen, now
			if err := m.saveIncidentWindowState(ctx, row); err != nil {
				t.Fatal(err)
			}
			const cb = "b3-scheduler:state-after-selection"
			changed := false
			var expected IncidentIngestWindowState
			if err := m.storeDB.Callback().Query().After("gorm:query").Register(cb, func(tx *gorm.DB) {
				if _, ok := tx.Statement.Dest.(*[]IncidentIngestWindowState); !ok || changed {
					return
				}
				changed = true
				expected = row
				expected.UpdatedAt = now + 1
				switch change {
				case "source_deferred":
					expected.NextSourceCheckAt = now + 300
				case "retry_deferred":
					expected.NextRetryAt = now + 300
				case "permanent":
					expected.ProcessState = incidentProcessFailedPermanent
				case "gap_closed":
					expected.ProcessState = incidentProcessDone
					expected.GapRecheckClosed = true
				case "deleted":
					if err := m.storeDB.Where("dedup_key = ?", d.DedupKey).Delete(&IncidentIngestWindowState{}).Error; err != nil {
						_ = tx.AddError(err)
					}
					return
				}
				if err := m.saveIncidentWindowState(ctx, expected); err != nil {
					_ = tx.AddError(err)
					return
				}
				// GORM owns UpdatedAt; compare with the actual persisted newer row.
				if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?", d.DedupKey, d.WindowToTs).Take(&expected).Error; err != nil {
					_ = tx.AddError(err)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(cb) })
			cursor := IncidentIngestCursor{ScanThroughTs: d.WindowToTs}
			if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+1); err != nil {
				t.Fatal(err)
			}
			if !changed || cursor.LastScheduledDedupKey != d.DedupKey {
				t.Fatal("fixture was not scheduled or fairness cursor was lost")
			}
			if change == "deleted" {
				assertIncidentB3Count(t, m, &IncidentIngestWindowState{}, 0)
			} else if got := reviewRegState(t, m, d.DedupKey, d.WindowToTs); !reflect.DeepEqual(got, expected) {
				t.Fatalf("outdated selected state was executed: got=%+v want=%+v", got, expected)
			}
			assertIncidentB3Count(t, m, &Incident{}, 0)
			assertIncidentB3Count(t, m, &IncidentObservation{}, 0)
			assertIncidentB3Count(t, m, &IncidentIngestRevision{}, 0)
		})
	}
}

func TestIncidentB3SchedulerReloadFailureCannotExecuteOldTask(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	defer closeTestStore(m)
	ctx := context.Background()
	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
	row := incidentWindowStateRowFrom(d, 7, now)
	row.ProcessState, row.FinalizedAt = incidentProcessInputFrozen, now
	if err := m.saveIncidentWindowState(ctx, row); err != nil {
		t.Fatal(err)
	}
	const cb = "b3-scheduler:reload-read"
	const selectedCB = "b3-scheduler:reload-selected"
	injected := errors.New("scheduler latest state unreadable")
	selected, reads := false, 0
	if err := m.storeDB.Callback().Query().After("gorm:query").Register(selectedCB, func(tx *gorm.DB) {
		if _, ok := tx.Statement.Dest.(*[]IncidentIngestWindowState); ok {
			selected = true
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(cb, func(tx *gorm.DB) {
		if _, ok := tx.Statement.Dest.(*IncidentIngestWindowState); ok && selected {
			reads++
			_ = tx.AddError(injected)
		}
	}); err != nil {
		t.Fatal(err)
	}
	cursor := IncidentIngestCursor{ScanThroughTs: d.WindowToTs}
	err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+1)
	_ = m.storeDB.Callback().Query().Remove(cb)
	_ = m.storeDB.Callback().Query().Remove(selectedCB)
	if err == nil || !strings.Contains(err.Error(), injected.Error()) || reads != 1 {
		t.Fatalf("reload failure was not returned: reads=%d err=%v", reads, err)
	}
	if got := reviewRegState(t, m, d.DedupKey, d.WindowToTs); !reflect.DeepEqual(row, got) {
		t.Fatalf("unreadable task was processed from stale data: %+v", got)
	}
	assertIncidentB3Count(t, m, &Incident{}, 0)
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+2); err != nil {
		t.Fatal(err)
	}
	if got := reviewRegState(t, m, d.DedupKey, d.WindowToTs); got.ProcessState != incidentProcessDone || got.Action != "created" {
		t.Fatalf("task did not recover after read restored: %+v", got)
	}
}

func b3SchedulerRetryCursor(t *testing.T, m *Monitor, ingest IncidentIngestConfig,
	d incidentWindowDecision, now int64, order string) IncidentIngestCursor {
	t.Helper()
	if err := m.reportIncidentIngestWindowError(context.Background(), d.WindowFromTs, d.WindowToTs,
		errors.New("whole-window retry fixture"), ingest, now); err != nil {
		t.Fatal(err)
	}
	last := d.DedupKey
	if order == "key_first" {
		last = incidentIngestWindowErrorKey
	}
	return IncidentIngestCursor{ScanThroughTs: d.WindowToTs, LastScheduledDedupKey: last}
}
