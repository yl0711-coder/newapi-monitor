package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func retentionSubmitVersion(t *testing.T, f generationFixture, version int64) monthlytasks.Task {
	t.Helper()
	f.plan.Owner.AuthVersion = version
	return generationSubmit(t, f)
}

func retentionReady(t *testing.T, f generationFixture, version int64) monthlytasks.Task {
	t.Helper()
	j := retentionSubmitVersion(t, f, version)
	a, err := f.g.Run(context.Background(), j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, a)
	j, err = f.g.tasks.Get(context.Background(), j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func retentionItem(t *testing.T, report RetentionInspection, j monthlytasks.Task, disposition string) RetentionItem {
	t.Helper()
	for _, item := range report.Entries {
		if item.InputSHA256 == j.Request.InputSHA256 {
			if item.Disposition != disposition || !item.KeepTaskMetadata {
				t.Fatalf("task %s disposition %q (%s), want %q", j.ID, item.Disposition, item.Reason, disposition)
			}
			return item
		}
	}
	t.Fatal("task missing from inventory")
	return RetentionItem{}
}

func TestRetentionReadonlyClassifiesWithoutDeletingOrRecalculating(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	older, latest := retentionReady(t, f, 1), retentionReady(t, f, 2)
	invalidated := retentionSubmitVersion(t, f, 3)
	if err := f.g.tasks.Invalidate(ctx, invalidated.Request.Owner, invalidated.ID); err != nil {
		t.Fatal(err)
	}
	var failed []monthlytasks.Task
	for _, version := range []int64{4, 5} {
		j := retentionSubmitVersion(t, f, version)
		_, lease, err := f.g.tasks.Claim(ctx, j.Request.Owner, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.g.tasks.Fail(ctx, lease, "internal_failure"); err != nil {
			t.Fatal(err)
		}
		failed = append(failed, j)
	}
	// White-box final-attempt fixture; other task tests exercise real retries.
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_draft_tasks SET attempts=3 WHERE id=?`, failed[0].ID); err != nil {
		t.Fatal(err)
	}
	running := retentionSubmitVersion(t, f, 6)
	if _, _, err := f.g.tasks.Claim(ctx, running.Request.Owner, running.ID); err != nil {
		t.Fatal(err)
	}
	orphan := f.plan
	orphan.Owner.AuthVersion = 7
	_, planRaw, _, err := canonicalGeneration(orphan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, planRaw, f.raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	missingRequest := running.Request
	missingRequest.Owner.GroupID = 2
	missingRequest.InputSHA256 = strings.Repeat("e", 64)
	missing, err := f.g.tasks.Submit(ctx, missingRequest)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	ro := generationDB(t, f.receiverPath, true)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	report, err := InspectGenerationRetention(ctx, ro, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if report.TaskCount != 7 || report.InputCount != 7 || len(report.Entries) != 8 || report.ActiveTasks != 2 || report.RetryableTasks != 1 ||
		report.ArchiveCopyCandidates != 3 || report.ReviewRequired != 2 || report.DeleteAllowed || report.SourceEvidenceChecked {
		t.Fatalf("unexpected inventory summary: %+v", report)
	}
	retentionItem(t, report, older, "archive_copy_candidate")
	retentionItem(t, report, latest, "keep_latest_ready")
	if !retentionItem(t, report, invalidated, "archive_copy_candidate").KeepInvalidation {
		t.Fatal("invalidation tombstone not protected")
	}
	retentionItem(t, report, failed[0], "archive_copy_candidate")
	retentionItem(t, report, failed[1], "keep_retryable")
	retentionItem(t, report, running, "keep_active") // Even though lease would be expired at asOf.
	if retentionItem(t, report, missing, "manual_review").Reason != "missing_retained_input" {
		t.Fatal("missing input was hidden")
	}
	again, err := InspectGenerationRetention(ctx, ro, asOf)
	if err != nil || !reflect.DeepEqual(report, again) {
		t.Fatal("read-only inventory is unstable", err)
	}
	after, err := os.ReadFile(f.receiverPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("inventory wrote receiver", err)
	}
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("inventory changed funding evidence", err)
	}
	raw, err := json.Marshal(report)
	if err != nil || strings.Contains(string(raw), f.root) || strings.Contains(string(raw), "1000.000000") ||
		strings.Contains(string(raw), "lease_token") || strings.Contains(string(raw), "independent-evidence") {
		t.Fatal("inventory leaked private draft content", err)
	}
	if _, err := f.g.Submit(ctx, GenerationPlan{Version: 1}, f.raw); err == nil {
		t.Fatal("inventory relaxed generation validation")
	}
	p := f.plan
	p.Owner.AuthVersion = 3
	if _, err := f.g.Submit(ctx, p, f.raw); !errors.Is(err, monthlytasks.ErrStale) {
		t.Fatal("inventory resurrected invalidated inputs", err)
	}
}

func TestRetentionAgeBoundaryAndClockSafety(t *testing.T) {
	f := generationFixtureNew(t)
	j := generationSubmit(t, f)
	ctx := context.Background()
	if err := f.g.tasks.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	j, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		offset time.Duration
		want   string
	}{
		{-time.Millisecond, "manual_review"},
		{0, "keep_recent"},
		{retentionReviewAge - time.Millisecond, "keep_recent"},
		{retentionReviewAge, "archive_copy_candidate"},
	} {
		r, err := InspectGenerationRetention(ctx, f.receiver, time.UnixMilli(j.UpdatedAtMS).Add(tc.offset))
		if err != nil {
			t.Fatal(err)
		}
		retentionItem(t, r, j, tc.want)
	}
}

func TestRetentionBrokenNewerDraftDoesNotUnpinIntactOlderDraft(t *testing.T) {
	for _, corrupt := range []string{"artifact", "missing", "binding"} {
		t.Run(corrupt, func(t *testing.T) {
			f := generationFixtureNew(t)
			older, newer := retentionReady(t, f, 1), retentionReady(t, f, 2)
			switch corrupt {
			case "artifact":
				if _, err := f.receiver.Exec(`DROP TRIGGER monthly_generation_no_replace;
UPDATE portal_monthly_generation_inputs SET artifact='{}' WHERE id=?`, newer.Request.InputSHA256); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if _, err := f.receiver.Exec(`DROP TRIGGER monthly_generation_no_delete;
DELETE FROM portal_monthly_generation_inputs WHERE id=?`, newer.Request.InputSHA256); err != nil {
					t.Fatal(err)
				}
			case "binding":
				wrong := newer.Request
				wrong.Owner.GroupID++
				if _, err := f.g.tasks.Submit(context.Background(), wrong); err != nil {
					t.Fatal(err)
				}
			}
			r, err := InspectGenerationRetention(context.Background(), f.receiver, time.Now().Add(45*24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			retentionItem(t, r, older, "keep_latest_ready")
			retentionItem(t, r, newer, "manual_review")
			if r.ArchiveCopyCandidates != 0 {
				t.Fatal("broken new draft made last intact draft disposable")
			}
		})
	}
}

func TestRetentionSingleSnapshotAcrossConcurrentAdmissions(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	if _, err := f.receiver.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	ro := generationDB(t, f.receiverPath, true)
	tx, err := ro.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var version int
	if err := tx.QueryRow(`SELECT version FROM portal_monthly_draft_task_control WHERE id=1`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	j := generationSubmit(t, f) // Atomic writer commits AFTER the read snapshot.
	before, err := inspectRetentionSnapshot(ctx, tx, time.Now())
	if err != nil || before.TaskCount != 0 || before.InputCount != 0 {
		t.Fatal("inventory mixed old task rows with new manifests", before, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	after, err := InspectGenerationRetention(ctx, ro, time.Now())
	if err != nil || after.TaskCount != 1 || after.InputCount != 1 || before.SnapshotSHA256 == after.SnapshotSHA256 {
		t.Fatal("new snapshot did not observe complete admission", after, err)
	}
	retentionItem(t, after, j, "keep_active")
	if err := f.g.tasks.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	invalidated, err := InspectGenerationRetention(ctx, ro, time.Now())
	if err != nil || invalidated.SnapshotSHA256 == after.SnapshotSHA256 {
		t.Fatal("state change was not reflected in inventory digest", err)
	}
}

func TestRetentionBoundedFailureDoesNotInitializeOrReturnPartialResult(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	if r, err := InspectGenerationRetention(ctx, nil, now); !errors.Is(err, ErrGenerationInput) || !reflect.DeepEqual(r, RetentionInspection{}) {
		t.Fatal("nil receiver accepted", err)
	}
	db := generationDB(t, filepath.Join(t.TempDir(), "empty.sqlite"), false)
	if _, err := db.Exec(`CREATE TABLE unrelated (id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if r, err := InspectGenerationRetention(ctx, db, now); err == nil || !reflect.DeepEqual(r, RetentionInspection{}) {
		t.Fatal("uninitialized receiver silently accepted", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'portal_monthly_%'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("inspection initialized tables", count, err)
	}
	if err := InitGenerationReceiver(ctx, db); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if r, err := InspectGenerationRetention(canceled, db, now); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(r, RetentionInspection{}) {
		t.Fatal("canceled inspection returned report", err)
	}
	retentionInspectionSlots <- struct{}{}
	_, err := InspectGenerationRetention(ctx, db, now)
	<-retentionInspectionSlots
	if !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("inspection fan-out not bounded", err)
	}
	for i := range generationRecordLimit + 1 {
		id := generationHash([]byte{byte(i)})
		if _, err := db.Exec(`INSERT INTO portal_monthly_generation_inputs(id,plan,preparation) VALUES(?,?,?)`, id, []byte(`{}`), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := InspectGenerationRetention(ctx, db, now); !errors.Is(err, ErrGenerationCapacity) || !reflect.DeepEqual(r, RetentionInspection{}) {
		t.Fatal("oversize inventory truncated or partially returned", err)
	}
}

func TestRetentionFullReceiverPayloadPerformance(t *testing.T) {
	if raceDetector {
		// The 5-second production bound is sized for native speed; hashing the
		// maximum record set under the race detector takes longer even on an
		// idle CI runner. Widen only this test's bound, never the production value.
		inspect := retentionInspectionTimeout
		retentionInspectionTimeout = 6 * inspect
		t.Cleanup(func() { retentionInspectionTimeout = inspect })
	}
	f := generationFixtureNew(t)
	ctx := context.Background()
	// Exercise the full RECORD bound with large payloads, leaving CI headroom
	// for race instrumentation. Byte overflow is separately rejected pre-decode.
	raw := append(bytes.Clone(f.raw), bytes.Repeat([]byte(" "), 512<<10)...)
	for i := range generationRecordLimit {
		p := f.plan
		p.Owner.AuthVersion = int64(i + 1)
		p.PreparationSHA256 = generationHash(raw)
		_, plan, _, err := canonicalGeneration(p, raw, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
			return retainGenerationPlan(ctx, tx, plan, raw, false)
		}); err != nil {
			t.Fatal(err)
		}
	}
	ro := generationDB(t, f.receiverPath, true)
	start := time.Now()
	r, err := InspectGenerationRetention(ctx, ro, start)
	if err != nil || r.InputCount != 32 || r.ReviewRequired != 32 || r.RetainedPayloadBytes < 16<<20 || r.ArchiveCopyCandidates != 0 {
		t.Fatal("full receiver inspection failed", err)
	}
	t.Logf("32 retained inputs, %d bytes: read-only structural inventory %s (not bill generation)", r.RetainedPayloadBytes, time.Since(start))
}

func TestRetentionByteLimitsRejectBeforeDecodingPayloads(t *testing.T) {
	for _, kind := range []string{"total", "single_artifact"} {
		t.Run(kind, func(t *testing.T) {
			db := generationDB(t, filepath.Join(t.TempDir(), "oversize.sqlite"), false)
			if err := InitGenerationReceiver(context.Background(), db); err != nil {
				t.Fatal(err)
			}
			if kind == "total" {
				for i := range 9 {
					if _, err := db.Exec(`INSERT INTO portal_monthly_generation_inputs(id,plan,preparation) VALUES(?,X'7b7d',zeroblob(8388608))`, generationHash([]byte{byte(i)})); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				// Explicitly corrupted synthetic receiver: the read guard must
				// still catch oversize blobs even if schema constraints were bypassed.
				db.SetMaxOpenConns(1)
				if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO portal_monthly_generation_inputs(id,plan,preparation,artifact) VALUES(?,X'7b7d',X'7b7d',zeroblob(8388609))`, generationHash([]byte(kind))); err != nil {
					t.Fatal(err)
				}
			}
			r, err := InspectGenerationRetention(context.Background(), db, time.Now())
			if !errors.Is(err, ErrGenerationCapacity) || !reflect.DeepEqual(r, RetentionInspection{}) {
				t.Fatal("byte guard parsed/truncated oversize payload", err)
			}
		})
	}
}
