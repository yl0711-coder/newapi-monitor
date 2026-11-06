package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func capacityFixture(t *testing.T, f displaySeriesFixture, directory string) *publishedCapacityMaintenance {
	t.Helper()
	m, err := newPublishedCapacityMaintenance(f.g, f.auth, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.close() })
	if err := InitPublishedGenerationRetention(t.Context(), f.g.receiver); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCapacityMaintenanceAtRealCountLimitReleasesOnlySavedPayload(t *testing.T) {
	f, task, bill := publishedArchiveFixture(t)
	journal, err := loadPublication(t.Context(), f.g.receiver, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	old, err := f.g.loadRetainedGeneration(t.Context(), task.Request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	funding, err := os.ReadFile(f.fundingPath)
	if err != nil {
		t.Fatal(err)
	}
	// Capacity metadata sentinels ONLY, not financial evidence. The chosen
	// candidate above is a real fully checked, saved synthetic customer bill.
	for i := 0; i < 31; i++ {
		if _, err := f.g.receiver.Exec(`INSERT INTO portal_monthly_generation_inputs(id,plan,preparation) VALUES(?,?,?)`, generationHash([]byte(fmt.Sprint("capacity sentinel ", i))), []byte(`{}`), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	m := capacityFixture(t, f, privateCapturedOutput(t))
	request, own, foreign := publishedCustomerHTTP(t, f)
	page, book := publishedCustomerSnapshot(t, request, own, foreign)
	if worked, err := m.step(t.Context()); err != nil || !worked {
		t.Fatal("pressure did not release one verified input", worked, err)
	}
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 31 || evictionCount(t, f.g.receiver, "portal_monthly_generation_archives") != 1 || publishedPermitCount(t, f) != 0 {
		t.Fatal("released wrong inputs or leaked permit")
	}
	after, err := f.g.loadRetainedGeneration(t.Context(), task.Request, time.Now())
	if err != nil || !reflect.DeepEqual(after, old) {
		t.Fatal("cold content changed", err)
	}
	assertPublishedArchiveUnchanged(t, f, task, bill, journal)
	nextPage, nextBook := publishedCustomerSnapshot(t, request, own, foreign)
	if !bytes.Equal(page, nextPage) || !reflect.DeepEqual(book, nextBook) {
		t.Fatal("customer amount or download changed")
	}
	nextFunding, err := os.ReadFile(f.fundingPath)
	if err != nil || !bytes.Equal(nextFunding, funding) {
		t.Fatal("original observations changed", err)
	}
	if worked, err := m.step(t.Context()); err != nil || worked {
		t.Fatal("unpublished sentinel treated as saved bill", worked, err)
	}
}

func TestCapacityMaintenanceCopyCommitFailurePausesAndRestartReusesExactCopy(t *testing.T) {
	f, task, _ := publishedArchiveFixture(t)
	dir := privateCapturedOutput(t)
	m := capacityFixture(t, f, dir)
	m.hotCount = 1
	if _, err := f.g.receiver.Exec(`CREATE TRIGGER synthetic_capacity_release_failure BEFORE DELETE ON portal_monthly_generation_inputs BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
		t.Fatal(err)
	}
	if worked, err := m.step(t.Context()); err == nil || worked || !m.paused.Load() {
		t.Fatal("commit failure not stopped", worked, err)
	}
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 || evictionCount(t, f.g.receiver, "portal_monthly_generation_archives") != 0 || publishedPermitCount(t, f) != 0 {
		t.Fatal("partial release committed")
	}
	path := filepath.Join(dir, "input-"+task.Request.InputSHA256, "manifest.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("durable copy missing", err)
	}
	if _, err := m.step(t.Context()); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("failure loop restarted work", err)
	}
	if err := m.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.receiver.Exec(`DROP TRIGGER synthetic_capacity_release_failure`); err != nil {
		t.Fatal(err)
	}
	resumed := capacityFixture(t, f, dir)
	resumed.hotCount = 1
	if worked, err := resumed.step(t.Context()); err != nil || !worked {
		t.Fatal("restart could not verify/reuse complete copy", worked, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("restart rewrote retained manifest", err)
	}
}

func TestCapacityMaintenanceRejectsPartialUnsafeAndOverBudgetColdStorage(t *testing.T) {
	for _, kind := range []string{"partial", "symlink", "public", "bytes", "directories", "withdrawn", "corrupt_bill"} {
		t.Run(kind, func(t *testing.T) {
			f, task, _ := publishedArchiveFixture(t)
			dir := privateCapturedOutput(t)
			m := capacityFixture(t, f, dir)
			m.hotCount = 1
			name := filepath.Join(dir, "input-"+task.Request.InputSHA256)
			switch kind {
			case "partial":
				if err := os.Mkdir(name, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(name, task.Request.InputSHA256+".plan"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(privateCapturedOutput(t), name); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Mkdir(name, 0755); err != nil {
					t.Fatal(err)
				}
			case "bytes":
				m.coldBytes = 1
			case "directories":
				for i := 0; i < 256; i++ {
					if err := os.Mkdir(filepath.Join(dir, "input-"+generationHash([]byte(fmt.Sprint("failed copy ", i)))), 0700); err != nil {
						t.Fatal(err)
					}
				}
			case "withdrawn":
				if err := f.auth.Exec(`DELETE FROM portal_monthly_bills WHERE group_id=1`).Error; err != nil {
					t.Fatal(err)
				}
			case "corrupt_bill":
				if err := f.auth.Exec(`UPDATE portal_monthly_bills SET payload='{}' WHERE group_id=1`).Error; err != nil {
					t.Fatal(err)
				}
			}
			if worked, err := m.step(t.Context()); err == nil || worked || !m.paused.Load() {
				t.Fatal("unsafe release accepted", worked, err)
			}
			if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 || evictionCount(t, f.g.receiver, "portal_monthly_generation_archives") != 0 {
				t.Fatal("failed maintenance removed evidence")
			}
		})
	}
}

func TestCapacityMaintenanceNoPressureAndCancellationDoNotCreateFiles(t *testing.T) {
	f, _, _ := publishedArchiveFixture(t)
	m := capacityFixture(t, f, privateCapturedOutput(t))
	if worked, err := m.step(t.Context()); err != nil || worked {
		t.Fatal(worked, err)
	}
	if n, b, err := m.inventory(t.Context()); err != nil || n != 0 || b != 0 {
		t.Fatal(n, b, err)
	}
	m.hotCount = 1
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.step(ctx); !errors.Is(err, context.Canceled) || m.paused.Load() {
		t.Fatal("shutdown latched permanent failure", err)
	}
	if n, _, err := m.inventory(t.Context()); err != nil || n != 0 {
		t.Fatal("cancel created output", n, err)
	}
	m.hotBytes = 0 // Byte pressure must work independently of the count threshold.
	if worked, err := m.step(t.Context()); err != nil || !worked {
		t.Fatal("byte pressure ignored", worked, err)
	}
}

func TestCapacityMaintenanceOwnsDirectoryAndRejectsUnknownFiles(t *testing.T) {
	f, _, _ := publishedArchiveFixture(t)
	dir := privateCapturedOutput(t)
	m := capacityFixture(t, f, dir)
	if _, err := newPublishedCapacityMaintenance(f.g, f.auth, dir); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("two owners accepted", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unrelated.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.inventory(t.Context()); err == nil {
		t.Fatal("unknown files ignored")
	}
}

func TestCapacityMaintenanceLaterMonthUsesColdPredecessorWithoutRehydrating(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	var first monthlytasks.Task
	for i := 0; i < 2; i++ {
		task, err := f.g.Submit(t.Context(), f.plans[i], f.inputs[i])
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = task
		}
		if worked, err := w.step(t.Context()); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	m := capacityFixture(t, f, privateCapturedOutput(t))
	m.hotCount = 2
	if worked, err := m.step(t.Context()); err != nil || !worked {
		t.Fatal(worked, err)
	}
	if _, err := loadGeneration(t.Context(), f.g.receiver, first.Request, time.Now()); !errors.Is(err, ErrGenerationArchived) {
		t.Fatal("oldest input not cold", err)
	}
	if _, err := f.g.Submit(t.Context(), f.plans[2], f.inputs[2]); err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(t.Context()); err != nil || !worked || w.Status().Published != 3 {
		t.Fatal("later actual save blocked by cold predecessor", worked, err, w.Status())
	}
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 2 {
		t.Fatal("cold predecessor secretly restored")
	}
	displaySeriesCount(t, f, 3)
}

func TestCapacityMaintenancePreparationHookDoesNotConsumeTaskBudgetsOrIdleWrites(t *testing.T) {
	f, task, _ := publishedArchiveFixture(t)
	r, err := NewLocalRuntime(f.g, f.auth)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.EnableOnDemandPreparation(t.Context(), f.auth, func(context.Context, SourcePreparationRequest) error { return nil }, func(context.Context, SourcePreparationRequest) (bool, error) {
		t.Fatal("maintenance dispatched source")
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	d := &Deployment{g: f.g, runtime: r, receiver: f.g.receiver, capturedPreparation: &capturedPreparationResources{}}
	if err := d.EnablePublishedCapacityMaintenance(t.Context(), privateCapturedOutput(t)); err != nil {
		t.Fatal(err)
	}
	m := d.capacity
	t.Cleanup(func() { _ = m.close() })
	m.hotCount = 1
	// A real saved journal with an old unacknowledged preparation row is a
	// supported crash state; inject only the queue descriptor as setup.
	req := SourcePreparationRequest{Owner: task.Request.Owner, Month: task.Request.Month}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if _, err := f.g.receiver.Exec(`INSERT INTO portal_monthly_source_preparation(key,group_id,payload,state,created_at,next_at) VALUES(?,?,?,'queued',?,?)`, req.Key(), req.Owner.GroupID, raw, now, now); err != nil {
		t.Fatal(err)
	}
	if job, err := r.preparation.claim(t.Context()); err != nil || job.key != "" {
		t.Fatal("maintenance reserved source lease", job, err)
	}
	state, steps, failures := preparationState(t, f.g.receiver)
	if state != "queued" || steps != 0 || failures != 0 || evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 0 {
		t.Fatal(state, steps, failures)
	}
	// No due queue: even a paused maintainer must not add idle writes/I/O.
	if _, err := f.g.receiver.Exec(`UPDATE portal_monthly_source_preparation SET state='submitted'`); err != nil {
		t.Fatal(err)
	}
	m.paused.Store(true)
	var before, after int
	if err := f.g.receiver.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if job, err := r.preparation.claim(t.Context()); err != nil || job.key != "" {
		t.Fatal(job, err)
	}
	if err := f.g.receiver.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || after != before {
		t.Fatal("idle maintenance wrote receiver", err)
	}
	req.Month = "2026-08"
	raw, err = json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.receiver.Exec(`INSERT INTO portal_monthly_source_preparation(key,group_id,payload,state,created_at,next_at) VALUES(?,?,?,'queued',?,?)`, req.Key(), req.Owner.GroupID, raw, now, now); err != nil {
		t.Fatal(err)
	}
	if view, err := r.preparation.ReadMonthly(t.Context(), 1, 1, req.Month); err != nil || view.State != "needs_review" {
		t.Fatal("stopped maintenance misreported progress", view, err)
	}
}

func TestCapacityMaintenanceStartupIsOptInAndShutdownClosesDirectoryOwner(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	addCapturedHistoryReconstruction(t, &f)
	var m *publishedCapacityMaintenance
	dir := privateCapturedOutput(t)
	testCapturedHistoryPublicationAdapter(t, f, true, true, true, true, func(d *Deployment) {
		if d.capacity != nil || d.runtime.preparation.capacity != nil {
			t.Fatal("default startup enabled maintenance")
		}
		if err := d.EnablePublishedCapacityMaintenance(t.Context(), d.capturedPreparation.spool.directory); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("empty raw spool accepted as cold root", err)
		}
		if err := d.EnablePublishedCapacityMaintenance(t.Context(), filepath.Join(d.capturedPreparation.spool.directory, "cold")); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("cold root allowed beneath raw inventory", err)
		}
		if err := d.EnablePublishedCapacityMaintenance(t.Context(), dir); err != nil {
			t.Fatal(err)
		}
		m = d.capacity
		if err := d.EnablePublishedCapacityMaintenance(t.Context(), dir); !errors.Is(err, ErrGenerationBusy) {
			t.Fatal("duplicate installation accepted", err)
		}
	})
	if m == nil {
		t.Fatal("mode not installed")
	}
	if _, err := m.root.Stat("."); !errors.Is(err, os.ErrClosed) {
		t.Fatal("shutdown leaked archive owner", err)
	}
	root, lock, err := openCapturedOutput(dir)
	if err != nil {
		t.Fatal("shutdown kept directory locked", err)
	}
	if err := errors.Join(root.Close(), lock.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestCapacityMaintenanceStaleScopeDoesNotQuarantineOtherEligibleMonth(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := f.g.Submit(t.Context(), f.plans[i], f.inputs[i]); err != nil {
			t.Fatal(err)
		}
		if worked, err := w.step(t.Context()); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	m := capacityFixture(t, f, privateCapturedOutput(t))
	m.hotCount = 2
	verify := f.g.verifyCurrent
	f.g.verifyCurrent = func(ctx context.Context, req monthlytasks.Request) error {
		if req.Month == "2026-07" {
			return ErrGenerationChanged
		}
		return verify(ctx, req)
	}
	if worked, err := m.step(t.Context()); err != nil || !worked || m.paused.Load() {
		t.Fatal("stale candidate blocked valid later month", worked, err)
	}
	if _, err := loadGeneration(t.Context(), f.g.receiver, f.requests[0], time.Now()); err != nil {
		t.Fatal("stale candidate released", err)
	}
	if _, err := loadGeneration(t.Context(), f.g.receiver, f.requests[1], time.Now()); !errors.Is(err, ErrGenerationArchived) {
		t.Fatal("eligible candidate not archived", err)
	}
}

func TestCapacityMaintenanceBusyCooldownIsBoundedAndCannotMultiplyCopies(t *testing.T) {
	for _, kind := range []string{"stop", "recover"} {
		t.Run(kind, func(t *testing.T) {
			f, _, _ := publishedArchiveFixture(t)
			m := capacityFixture(t, f, privateCapturedOutput(t))
			m.hotCount = 1
			archiveSlots <- struct{}{}
			held := true
			defer func() {
				if held {
					<-archiveSlots
				}
			}()
			for i := 1; i <= 3; i++ {
				if _, err := m.step(t.Context()); !errors.Is(err, ErrGenerationBusy) {
					t.Fatal("expected synthetic bounded contention", err)
				}
				if m.transientFailures != i {
					t.Fatal("wrong failure count", m.transientFailures)
				}
				if i == 3 {
					break
				}
				if _, err := m.step(t.Context()); !errors.Is(err, ErrGenerationBusy) || m.transientFailures != i {
					t.Fatal("cooldown repeated audit", err)
				}
				m.retryAt = time.Now().Add(-time.Second) // deterministic time advance, no real wait
				if kind == "recover" {
					<-archiveSlots
					held = false
					if worked, err := m.step(t.Context()); err != nil || !worked || m.paused.Load() || m.transientFailures != 0 {
						t.Fatal("transient recovery failed", worked, err)
					}
					return
				}
			}
			if !m.paused.Load() {
				t.Fatal("third contention did not stop")
			}
			if _, err := m.step(t.Context()); !errors.Is(err, ErrGenerationCapacity) {
				t.Fatal(err)
			}
			if n, b, err := m.inventory(t.Context()); err != nil || n != 1 || b != 0 {
				t.Fatal("busy retries amplified retained output", n, b, err)
			}
			if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 {
				t.Fatal("contention released hot input")
			}
		})
	}
}

func TestCapacityMaintenanceCloseTimeoutRetainsResourcesUntilAuditDrains(t *testing.T) {
	f, task, _ := publishedArchiveFixture(t)
	request, own, foreign := publishedCustomerHTTP(t, f)
	beforePage, beforeBook := publishedCustomerSnapshot(t, request, own, foreign)
	r, err := NewLocalRuntime(f.g, f.auth)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.EnableOnDemandPreparation(t.Context(), f.auth, func(context.Context, SourcePreparationRequest) error { return nil },
		func(context.Context, SourcePreparationRequest) (bool, error) {
			t.Error("shutdown reached source collection")
			return false, nil
		}); err != nil {
		t.Fatal(err)
	}
	d := &Deployment{g: f.g, runtime: r, receiver: f.g.receiver, capturedPreparation: &capturedPreparationResources{}}
	dir := privateCapturedOutput(t)
	if err := d.EnablePublishedCapacityMaintenance(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	d.capacity.hotCount = 1
	req := SourcePreparationRequest{Owner: task.Request.Owner, Month: task.Request.Month}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if _, err := f.g.receiver.Exec(`INSERT INTO portal_monthly_source_preparation(key,group_id,payload,state,created_at,next_at) VALUES(?,?,?,'queued',?,?)`, req.Key(), req.Owner.GroupID, raw, now, now); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var started, unblock sync.Once
	guard := f.g.verifyCurrent
	f.g.verifyCurrent = func(ctx context.Context, req monthlytasks.Request) error {
		started.Do(func() { close(entered) })
		<-release // Explicit synthetic uninterruptible audit, not real I/O.
		if err := ctx.Err(); err != nil {
			return err
		}
		return guard(ctx, req)
	}
	t.Cleanup(func() { unblock.Do(func() { close(release) }); _ = d.Close(context.Background()) })
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance did not enter audit")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	err = d.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close did not wait for maintenance", err)
	}
	if _, err := d.capacity.root.Stat("."); err != nil {
		t.Fatal("timed-out close destroyed cold handle", err)
	}
	if err := f.g.receiver.PingContext(t.Context()); err != nil {
		t.Fatal("timed-out close destroyed receiver", err)
	}
	if d.closed {
		t.Fatal("incomplete drain marked closed")
	}
	unblock.Do(func() { close(release) })
	ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := d.Close(ctx); err != nil {
		t.Fatal("drained close failed", err)
	}
	if _, err := d.capacity.root.Stat("."); !errors.Is(err, os.ErrClosed) {
		t.Fatal("closed owner leaked cold handle", err)
	}
	if d.capacity.paused.Load() {
		t.Fatal("shutdown cancellation became permanent financial failure")
	}
	afterPage, afterBook := publishedCustomerSnapshot(t, request, own, foreign)
	if !bytes.Equal(beforePage, afterPage) || !reflect.DeepEqual(beforeBook, afterBook) {
		t.Fatal("stopping generation changed customer bill or workbook")
	}
	root, lock, err := openCapturedOutput(dir)
	if err != nil {
		t.Fatal("drained owner retained lock", err)
	}
	if err := errors.Join(root.Close(), lock.Close()); err != nil {
		t.Fatal(err)
	}
}
