package monitor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"gorm.io/gorm"
)

// Exercise the production scheduler, including a whole-window placeholder that
// retries before the frozen key is due. Recovery and late revision are separate.
func TestIncidentB3WholeWindowRetryUsesFrozenSnapshot(t *testing.T) {
	for _, entry := range []string{"source_queue", "scan"} {
		for _, stage := range []string{"input_frozen", "engine_committed", "engine_done"} {
			t.Run(entry+"/"+stage, func(t *testing.T) {
				core, ingest := incidentIngestTestEnv(t)
				m := openIncidentTestStore(t)
				ctx := context.Background()
				d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
				now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
				row := incidentWindowStateRowFrom(d, 7, now)
				row.ProcessState, row.FinalizedAt = incidentProcessInputFrozen, now
				row.NextSourceCheckAt = now + 3600
				if stage != "input_frozen" {
					result, err := m.IngestIncidentObservation(ctx, d.observationInputFor(core.MinSample))
					if err != nil {
						t.Fatal(err)
					}
					if result.IncidentID == "" {
						t.Fatal("fixture did not create an incident")
					}
					if stage == "engine_done" {
						row.ProcessState = incidentProcessEngineDone
						row.IncidentID, row.SegmentID = result.IncidentID, result.SegmentID
						row.Severity, row.Outcome, row.Action = result.Severity, result.Outcome, result.Action
					}
				}
				if err := m.saveIncidentWindowState(ctx, row); err != nil {
					t.Fatal(err)
				}
				reviewRegSeedFacts(t, m, reviewRegBase, 9999, 1)
				seedDayCoverage(t, m, reviewRegBase, d.WindowToTs)
				cursor := IncidentIngestCursor{ScanThroughTs: d.WindowFromTs}
				if entry == "source_queue" {
					placeholder := IncidentIngestWindowState{
						DedupKey: incidentIngestWindowErrorKey, WindowFromTs: d.WindowFromTs,
						WindowToTs: d.WindowToTs, ProcessState: incidentProcessFailed,
						CreatedAt: now, UpdatedAt: now,
					}
					if err := m.saveIncidentWindowState(ctx, placeholder); err != nil {
						t.Fatal(err)
					}
					if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+1); err != nil {
						t.Fatal(err)
					}
				} else if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now); err != nil {
					t.Fatal(err)
				}
				recovered := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				if recovered.ProcessState != incidentProcessDone || recovered.SampleTotal == nil || *recovered.SampleTotal != 100 || recovered.IncidentID == "" {
					t.Fatalf("recovery replaced or lost the frozen decision: %+v", recovered)
				}
				if got := impactSampleTotal(t, m, recovered.IncidentID, incidentImpactDimChannel, "777"); got != 100 {
					t.Fatalf("recovery contribution=%d want frozen 100", got)
				}
				original := b3ReviewObservations(t, m)
				if len(original) != 1 || original[0].SampleTotal == nil || *original[0].SampleTotal != 100 {
					t.Fatalf("engine must retain exactly one frozen observation: %+v", original)
				}
				if err := m.recheckOneIncidentWindow(ctx, core, ingest, recovered, now+60); err != nil {
					t.Fatal(err)
				}
				revised := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				if revised.SampleTotal == nil || *revised.SampleTotal != 100 || revised.MeasuredSampleTotal == nil || *revised.MeasuredSampleTotal != 10000 {
					t.Fatalf("revision must separate original and latest: %+v", revised)
				}
				revisions := b3ReviewRevisions(t, m, d)
				if len(revisions) != 1 || revisions[0].Kind != "revised_after_finalize" || revisions[0].OldSampleTotal == nil || *revisions[0].OldSampleTotal != 100 || revisions[0].NewSampleTotal == nil || *revisions[0].NewSampleTotal != 10000 {
					t.Fatalf("changed facts have no correct revision: %+v", revisions)
				}
				if got := impactSampleTotal(t, m, recovered.IncidentID, incidentImpactDimChannel, "777"); got != 10000 {
					t.Fatalf("revised impact=%d", got)
				}
				if !reflect.DeepEqual(original, b3ReviewObservations(t, m)) {
					t.Fatal("late revision rewrote engine observations")
				}
			})
		}
	}
}

func TestIncidentB3LateIncompleteWindowWaitsThenRecovers(t *testing.T) {
	for _, reason := range []string{"mixed_version", "wait_coverage", "no_sample_coverage_loss"} {
		t.Run(reason, func(t *testing.T) {
			core, ingest := incidentIngestTestEnv(t)
			m := openIncidentTestStore(t)
			ctx := context.Background()
			d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
			now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
			reviewRegSeedFacts(t, m, reviewRegBase, 10, 90)
			seedDayCoverage(t, m, reviewRegBase, d.WindowToTs)
			if reason == "no_sample_coverage_loss" {
				if err := m.storeDB.Where("bucket_ts = ?", reviewRegBase).Delete(&CapacityUserMinuteSample{}).Error; err != nil {
					t.Fatal(err)
				}
				var err error
				d, err = m.decideIncidentWindowForExistingRow(core, ingest, incidentWindowStateRowFrom(d, 7, now), incidentWindowFacts{}, false, coverageComplete(d.WindowFromTs, d.WindowToTs), d.WindowToTs)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
				t.Fatal(err)
			}
			before := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			before.RecheckAttempts = ingest.RecheckMaxAttempts - 1
			if err := m.saveIncidentWindowState(ctx, before); err != nil {
				t.Fatal(err)
			}
			observations := b3ReviewObservations(t, m)
			cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: d.WindowToTs, CompleteThroughTs: d.WindowToTs}
			if err := m.saveIncidentIngestCursor(ctx, cursor); err != nil {
				t.Fatal(err)
			}
			if reason == "mixed_version" {
				b3ReviewSeedMixed(t, m, d.WindowFromTs)
			} else if err := m.storeDB.Where("1 = 1").Delete(&CustomerHealthDayCoverage{}).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.recheckIncidentFinalizedWindows(ctx, core, ingest, now+1); err != nil {
				t.Fatal(err)
			}
			row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			if row.DataComplete || row.GapRecheckClosed || row.ProcessState != incidentProcessDone || row.WindowState != before.WindowState {
				t.Errorf("late incomplete data must reopen source review and preserve original: %+v", row)
			}
			b3ReviewAssertCompleteness(t, m, &cursor, before.IncidentID, false, 1, d.WindowFromTs)
			persisted, err := m.loadIncidentIngestCursor(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.CompleteThroughTs != d.WindowFromTs {
				t.Error("persisted watermark did not roll back with the revision")
			}
			if row.MeasuredWindowState != reason && !(reason == "no_sample_coverage_loss" && row.MeasuredWindowState == incidentWindowStateWaitCoverage) {
				t.Errorf("measured state=%s want=%s", row.MeasuredWindowState, reason)
			}
			// Already used the last late-review attempt. Source review must remain
			// live after both the six-attempt limit and the two-hour horizon.
			for i := int64(1); i <= 2; i++ {
				when := now + i*3*3600
				if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, when); err != nil {
					t.Fatal(err)
				}
				waiting := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
				if waiting.DataComplete || waiting.GapRecheckClosed || waiting.SourceCheckAttempts <= row.SourceCheckAttempts || waiting.NextSourceCheckAt <= when {
					t.Errorf("incomplete window stopped waiting: before=%+v after=%+v", row, waiting)
				}
				if waiting.RecheckAttempts != row.RecheckAttempts {
					t.Fatal("source review consumed late-review attempts")
				}
				row = waiting
			}
			if revisions := b3ReviewRevisions(t, m, d); len(revisions) != 1 {
				t.Errorf("unchanged waiting must not duplicate revisions: %+v", revisions)
			}
			if reason == "mixed_version" {
				if err := m.storeDB.Where("user_id = ?", 2).Delete(&CapacityUserMinuteSample{}).Error; err != nil {
					t.Fatal(err)
				}
			} else {
				seedDayCoverage(t, m, d.WindowFromTs, d.WindowToTs)
			}
			if reason != "no_sample_coverage_loss" {
				reviewRegSeedFacts(t, m, d.WindowFromTs, 9999, 1)
			}
			if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+9*3600); err != nil {
				t.Fatal(err)
			}
			after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			if !after.DataComplete || !after.GapRecheckClosed || after.WindowState != before.WindowState || !reflect.DeepEqual(after.SampleTotal, before.SampleTotal) || after.Action != before.Action || after.Outcome != before.Outcome || after.Severity != before.Severity || after.IncidentID != before.IncidentID {
				t.Errorf("backfill must complete latest facts without changing original decision: %+v", after)
			}
			b3ReviewAssertCompleteness(t, m, &cursor, before.IncidentID, true, 0, d.WindowToTs)
			if len(b3ReviewRevisions(t, m, d)) != 2 {
				t.Error("actual backfill needs exactly one additional revision")
			}
			if !reflect.DeepEqual(observations, b3ReviewObservations(t, m)) {
				t.Error("source backfill rewrote or created engine observations")
			}
		})
	}
}

func TestIncidentB3LateIncompleteRevisionIsAtomic(t *testing.T) {
	for _, table := range []string{"incident_ingest_revisions", "incident_impact_windows", "incident_ingest_window_states", "incident_pending_summaries", "incident_ingest_cursors"} {
		t.Run(table, func(t *testing.T) {
			core, ingest := incidentIngestTestEnv(t)
			m := openIncidentTestStore(t)
			ctx := context.Background()
			d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
			now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
			reviewRegSeedFacts(t, m, d.WindowFromTs, 10, 90)
			seedDayCoverage(t, m, d.WindowFromTs, d.WindowToTs)
			if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
				t.Fatal(err)
			}
			before := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			if err := m.saveIncidentIngestCursor(ctx, IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: d.WindowToTs, CompleteThroughTs: d.WindowToTs}); err != nil {
				t.Fatal(err)
			}
			b3ReviewSeedMixed(t, m, d.WindowFromTs)
			injected := errors.New("injected revision persistence failure")
			const callback = "b3-review:atomic-revision"
			failWrite := func(tx *gorm.DB) {
				if tx.Statement.Table == table {
					_ = tx.AddError(injected)
				}
			}
			if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, failWrite); err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Callback().Update().Before("gorm:update").Register(callback, failWrite); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = m.storeDB.Callback().Create().Remove(callback)
				_ = m.storeDB.Callback().Update().Remove(callback)
			})
			if err := m.recheckOneIncidentWindow(ctx, core, ingest, before, now+1); !errors.Is(err, injected) {
				t.Errorf("must propagate persistence failure: %v", err)
			}
			after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
			if !reflect.DeepEqual(before, after) || len(b3ReviewRevisions(t, m, d)) != 0 {
				t.Error("failed revision partially committed window or audit")
			}
			var impact IncidentImpact
			if err := m.storeDB.Where("incident_id = ? AND dimension = ?", before.IncidentID, incidentImpactDimChannel).Take(&impact).Error; err != nil {
				t.Fatal(err)
			}
			if !impact.CoverageComplete || impact.Partial {
				t.Error("failed revision partially committed impact")
			}
			if err := m.storeDB.Callback().Create().Remove(callback); err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Callback().Update().Remove(callback); err != nil {
				t.Fatal(err)
			}
			if err := m.recheckOneIncidentWindow(ctx, core, ingest, before, now+60); err != nil {
				t.Fatal(err)
			}
			if len(b3ReviewRevisions(t, m, d)) != 1 {
				t.Error("successful retry must write exactly one audit revision")
			}
		})
	}
}

func b3ReviewSeedMixed(t *testing.T, m *Monitor, from int64) {
	t.Helper()
	f := reviewRegFacts()
	seedCapacitySample(t, m, CapacityUserMinuteSample{BucketTs: from + 60, UserID: 2,
		ChannelID: f.ChannelID, ModelName: f.ModelName, Grp: f.Grp, Success: 50,
		TrafficClassVersion: stabilityTrafficClassificationVersion - 1})
}

func b3ReviewRevisions(t *testing.T, m *Monitor, d incidentWindowDecision) []IncidentIngestRevision {
	t.Helper()
	var rows []IncidentIngestRevision
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?", d.DedupKey, d.WindowToTs).Order("revision_seq").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func b3ReviewObservations(t *testing.T, m *Monitor) []IncidentObservation {
	t.Helper()
	var rows []IncidentObservation
	if err := m.storeDB.Order("id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func b3ReviewAssertCompleteness(t *testing.T, m *Monitor, cursor *IncidentIngestCursor, incidentID string, complete bool, pending, watermark int64) {
	t.Helper()
	ctx := context.Background()
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if backlog != pending {
		t.Errorf("pending=%d want=%d", backlog, pending)
	}
	if err := m.recomputeIncidentCompleteWatermark(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	if cursor.CompleteThroughTs != watermark {
		t.Errorf("complete watermark=%d want=%d", cursor.CompleteThroughTs, watermark)
	}
	if incidentID == "" {
		return
	}
	var impact IncidentImpact
	if err := m.storeDB.Where("incident_id = ? AND dimension = ?", incidentID, incidentImpactDimChannel).Take(&impact).Error; err != nil {
		t.Fatal(err)
	}
	if impact.CoverageComplete != complete || impact.Partial == complete {
		t.Errorf("impact completeness inconsistent: %+v", impact)
	}
	if !complete && (impact.SampleTotal == nil || *impact.SampleTotal != 100) {
		t.Error("incomplete coverage must retain known current-version facts")
	}
	if impact.DistinctRequests != nil || impact.VisibleAttempts != nil || impact.LogRows != nil {
		t.Error("sample counts must not masquerade as other counting units")
	}
}

// Expiration and archival must retain the original anomaly and the latest gap.
func TestIncidentB3LateIncompleteExpiresWithoutRejudging(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
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
	cursor := IncidentIngestCursor{ScanThroughTs: d.WindowToTs}
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, incidentGapRecheckDeadline(d.WindowToTs)+1); err != nil {
		t.Fatal(err)
	}
	closed := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if closed.DataComplete || !closed.GapRecheckClosed || !closed.MixedVersionClosed || closed.WindowState != original.WindowState || closed.MeasuredWindowState != incidentWindowStateMixedVersion || closed.Action != original.Action || closed.Outcome != original.Outcome {
		t.Fatalf("expiry changed the original decision or claimed completeness: %+v", closed)
	}
	b3ReviewAssertCompleteness(t, m, &cursor, original.IncidentID, false, 0, d.WindowFromTs)
	terminal, err := m.countIncidentTerminalGaps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if terminal != 1 {
		t.Fatalf("terminal gaps=%d want=1", terminal)
	}
	if !reflect.DeepEqual(observations, b3ReviewObservations(t, m)) {
		t.Fatal("expiration reran the engine")
	}
	if err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
		_, err := m.archiveIncidentGapWindows(tx, 7, d.WindowToTs+8*86400)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var summaries []IncidentIngestGapSummary
	if err := m.storeDB.Find(&summaries).Error; err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].WindowCount != 1 || summaries[0].GapObservedCount != 1 {
		t.Fatalf("late gap was not archived accurately: %+v", summaries)
	}
	b3ReviewAssertCompleteness(t, m, &cursor, original.IncidentID, false, 0, d.WindowFromTs)
}

func TestIncidentB3LateGapCanReopenAfterBackfill(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	now := d.WindowToTs + int64(ingest.FinalizeLagSeconds)
	reviewRegSeedFacts(t, m, d.WindowFromTs, 10, 90)
	seedDayCoverage(t, m, d.WindowFromTs, d.WindowToTs)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
		t.Fatal(err)
	}
	cursor := IncidentIngestCursor{ScanThroughTs: d.WindowToTs}
	for i := int64(0); i < 2; i++ {
		b3ReviewSeedMixed(t, m, d.WindowFromTs)
		row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
		if err := m.recheckOneIncidentWindow(ctx, core, ingest, row, now+i*600+1); err != nil {
			t.Fatal(err)
		}
		b3ReviewAssertCompleteness(t, m, &cursor, row.IncidentID, false, 1, d.WindowFromTs)
		if err := m.storeDB.Where("user_id = ?", 2).Delete(&CapacityUserMinuteSample{}).Error; err != nil {
			t.Fatal(err)
		}
		// Whole-window retry is another entrance to the same audited backfill.
		if err := m.processIncidentWindow(ctx, core, ingest, d.WindowFromTs, d.WindowToTs, now+i*600+300); err != nil {
			t.Fatal(err)
		}
		b3ReviewAssertCompleteness(t, m, &cursor, row.IncidentID, true, 0, d.WindowToTs)
	}
	if len(b3ReviewRevisions(t, m, d)) != 4 {
		t.Fatal("both completeness transitions require audit records")
	}
}

// B2 lint changes only error identity, including wrapped database sentinels.
func TestIncidentB3LintFixPreservesWrappedErrorIdentity(t *testing.T) {
	m := openIncidentTestStore(t)
	const cb = "b3-review:wrapped-not-found"
	wrapped := fmt.Errorf("storage context: %w", gorm.ErrRecordNotFound)
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(cb, func(tx *gorm.DB) { _ = tx.AddError(wrapped) }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(cb) })
	finalized, err := m.windowAlreadyFinalized(m.storeDB, "missing", 300)
	if err != nil || finalized {
		t.Fatalf("wrapped missing row should remain absent: %v %v", finalized, err)
	}
	_, err = m.resolveEvidenceSegment(m.storeDB, "missing", "missing", IncidentObservationInput{WindowFromTs: 0, WindowToTs: 300})
	if !errors.Is(err, ErrObservationUnattributed) || !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("both attribution sentinel and storage cause must survive: %v", err)
	}
}
