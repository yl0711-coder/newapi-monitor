package monthlytasks

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

type fixture struct {
	store *Store
	db    *sql.DB
	path  string
	clock *atomic.Int64
}

func openTestStore(t *testing.T, path string, clock *atomic.Int64) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return time.UnixMilli(clock.Load()) }
	return s, db
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	clock := &atomic.Int64{}
	clock.Store(time.Date(2026, 10, 21, 12, 0, 0, 0, monthZone).UnixMilli())
	path := filepath.Join(t.TempDir(), "tasks.sqlite")
	s, db := openTestStore(t, path, clock)
	if err := Init(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return fixture{s, db, path, clock}
}

func requestFixture() Request {
	return Request{
		Owner: Owner{GroupID: 7, AuthVersion: 3, MemberFingerprint: strings.Repeat("a", 32), MemberManifest: strings.Repeat("b", 64)},
		Month: "2026-09", SourceIdentity: strings.Repeat("c", 64), InputSHA256: strings.Repeat("d", 64), RulesSHA256: strings.Repeat("e", 64),
	}
}

func submit(t *testing.T, s *Store, r Request) Task {
	t.Helper()
	j, err := s.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func claim(t *testing.T, s *Store, job Task) Lease {
	t.Helper()
	r, lease, err := s.Claim(context.Background(), job.Request.Owner, job.ID)
	if err != nil || r.State != Running || r.Attempts < 1 {
		t.Fatal(r, err)
	}
	return lease
}

func requireError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

func TestTaskDurableReopenAndDuplicateDoesNotRegenerate(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	r := requestFixture()
	initial := submit(t, f.store, r)
	for range 5 {
		if got := submit(t, f.store, r); !reflect.DeepEqual(got, initial) {
			t.Fatal("duplicate request changed task", got)
		}
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	s, db := openTestStore(t, f.path, f.clock)
	if err := Init(ctx, db); err != nil {
		t.Fatal("idempotent initialization", err)
	}
	if got := submit(t, s, r); !reflect.DeepEqual(got, initial) {
		t.Fatal("restart lost queued task")
	}
	l := claim(t, s, initial)
	f.clock.Add(10 * time.Second.Milliseconds())
	if err := s.Renew(ctx, l); err != nil {
		t.Fatal(err)
	}
	running := submit(t, s, r)
	if running.State != Running || running.Attempts != 1 {
		t.Fatal("refresh started work again", running)
	}
	digest := hash([]byte("retained internal draft, not a publication"))
	if err := s.Complete(ctx, l, digest); err != nil {
		t.Fatal(err)
	}
	requireError(t, s.Complete(ctx, l, digest), ErrLeaseLost)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ = openTestStore(t, f.path, f.clock)
	ready := submit(t, s, r)
	if ready.ID != initial.ID || ready.State != DraftReady || ready.ResultSHA256 != digest || ready.Attempts != 1 {
		t.Fatal("completed state not durable", ready)
	}
	got, err := s.Get(ctx, r.Owner, ready.ID)
	if err != nil || !reflect.DeepEqual(got, ready) {
		t.Fatal("status lost", err)
	}
	raw, err := json.Marshal(got)
	if err != nil || strings.Contains(string(raw), l.token) || strings.Contains(string(raw), "publication") {
		t.Fatal("status leaked lease or publication", err)
	}
	if _, err := json.Marshal(l); err == nil {
		t.Fatal("lease should not be persisted in task DTO")
	}
	if err := json.Unmarshal([]byte(`{}`), &l); err == nil || l != (Lease{}) {
		t.Fatal("lease deserialization must fail and erase previous value")
	}
}

func TestTaskInputIdentityScopeAndMonth(t *testing.T) {
	mutations := map[string]func(*Request){
		"group":                 func(r *Request) { r.Owner.GroupID++ },
		"auth revision":         func(r *Request) { r.Owner.AuthVersion++ },
		"members":               func(r *Request) { r.Owner.MemberFingerprint = strings.Repeat("1", 32) },
		"remove readd revision": func(r *Request) { r.Owner.MemberManifest = strings.Repeat("2", 64) },
		"month":                 func(r *Request) { r.Month = "2026-08" },
		"source":                func(r *Request) { r.SourceIdentity = strings.Repeat("3", 64) },
		"evidence":              func(r *Request) { r.InputSHA256 = strings.Repeat("4", 64) },
		"rules format":          func(r *Request) { r.RulesSHA256 = strings.Repeat("5", 64) },
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			r := requestFixture()
			old := submit(t, f.store, r)
			if err := f.store.Complete(context.Background(), claim(t, f.store, old), strings.Repeat("f", 64)); err != nil {
				t.Fatal(err)
			}
			change(&r)
			fresh := submit(t, f.store, r)
			if fresh.ID == old.ID || fresh.Key == old.Key || fresh.State != Queued || fresh.ResultSHA256 != "" {
				t.Fatal("different inputs reused old output")
			}
			if r.Owner != old.Request.Owner {
				_, err := f.store.Get(context.Background(), r.Owner, old.ID)
				requireError(t, err, ErrNotFound)
				requireError(t, f.store.Invalidate(context.Background(), r.Owner, old.ID), ErrNotFound)
			}
		})
	}
	invalid := map[string]func(*Request){
		"zero group":                   func(r *Request) { r.Owner.GroupID = 0 },
		"negative revision":            func(r *Request) { r.Owner.AuthVersion = -1 },
		"missing members":              func(r *Request) { r.Owner.MemberFingerprint = "" },
		"wrong fingerprint convention": func(r *Request) { r.Owner.MemberFingerprint = strings.Repeat("a", 64) },
		"name as manifest":             func(r *Request) { r.Owner.MemberManifest = "member1" },
		"uppercase digest":             func(r *Request) { r.InputSHA256 = strings.Repeat("A", 64) },
		"bad source":                   func(r *Request) { r.SourceIdentity = strings.Repeat("g", 64) },
		"missing rules":                func(r *Request) { r.RulesSHA256 = "" },
		"current month":                func(r *Request) { r.Month = "2026-10" },
		"future month":                 func(r *Request) { r.Month = "2027-01" },
		"pre 2020":                     func(r *Request) { r.Month = "2019-12" },
		"non canonical month":          func(r *Request) { r.Month = "2026-9" },
	}
	for name, change := range invalid {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			r := requestFixture()
			change(&r)
			_, err := f.store.Submit(context.Background(), r)
			requireError(t, err, ErrInvalid)
			var count int
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM portal_monthly_draft_tasks`).Scan(&count); err != nil || count != 0 {
				t.Fatal("invalid request was persisted", count, err)
			}
		})
	}
	// Exact UTC+8 month boundary, not UTC or an approximate observation time.
	f := newFixture(t)
	f.clock.Store(time.Date(2026, 10, 1, 0, 0, 0, 0, monthZone).UnixMilli() - 1)
	_, err := f.store.Submit(context.Background(), requestFixture())
	requireError(t, err, ErrInvalid)
	f.clock.Add(1)
	_ = submit(t, f.store, requestFixture())
}

func TestTaskConcurrentStoresReuseAndSingleWorker(t *testing.T) {
	f := newFixture(t)
	s2, _ := openTestStore(t, f.path, f.clock)
	stores := []*Store{f.store, s2}
	const clicks = 20
	jobs := make(chan Task, clicks)
	errs := make(chan error, clicks)
	var wg sync.WaitGroup
	for i := range clicks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := stores[i%2].Submit(context.Background(), requestFixture())
			jobs <- j
			errs <- err
		}()
	}
	wg.Wait()
	close(jobs)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	first := <-jobs
	for j := range jobs {
		if j.ID != first.ID || j.Key != first.Key {
			t.Fatal("concurrent clicks created duplicates")
		}
	}
	r2 := requestFixture()
	r2.Owner.GroupID++
	second := submit(t, s2, r2)
	var wins atomic.Int64
	var winningLease Lease
	var mu sync.Mutex
	for i, j := range []Task{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, lease, err := stores[i].Claim(context.Background(), j.Request.Owner, j.ID)
			if err == nil {
				wins.Add(1)
				mu.Lock()
				winningLease = lease
				mu.Unlock()
			} else if !errors.Is(err, ErrBusy) {
				t.Errorf("claim: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatal("cross-connection worker limit failed", wins.Load())
	}
	if err := s2.Complete(context.Background(), winningLease, strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
}

func TestTaskCrashLeaseFencingAndAttemptExhaustion(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	j := submit(t, f.store, requestFixture())
	old := claim(t, f.store, j)
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ := openTestStore(t, f.path, f.clock)
	_, _, err := s.Claim(ctx, j.Request.Owner, j.ID)
	requireError(t, err, ErrBusy)
	f.clock.Add(leaseDuration.Milliseconds())
	requireError(t, s.Renew(ctx, old), ErrLeaseLost)
	requireError(t, s.Complete(ctx, old, strings.Repeat("f", 64)), ErrLeaseLost)
	fresh := claim(t, s, j)
	if fresh.token == old.token || fresh.attempt != 2 {
		t.Fatal("reclaim did not fence prior worker")
	}
	requireError(t, s.Fail(ctx, old, "source_unavailable"), ErrLeaseLost)
	forged := fresh
	forged.attempt--
	requireError(t, s.Renew(ctx, forged), ErrLeaseLost)
	f.clock.Add(leaseDuration.Milliseconds())
	third := claim(t, s, j)
	if third.attempt != 3 {
		t.Fatal("lost attempt count")
	}
	f.clock.Add(leaseDuration.Milliseconds())
	_, _, err = s.Claim(ctx, j.Request.Owner, j.ID)
	requireError(t, err, ErrExhausted)
	got, err := s.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || got.State != Failed || got.Attempts != 3 || got.FailureCode != "attempts_exhausted" || got.ResultSHA256 != "" {
		t.Fatal("exhaustion state not durable", got, err)
	}
	requireError(t, s.Retry(ctx, j.Request.Owner, j.ID), ErrExhausted)
	if got := submit(t, s, j.Request); got.State != Failed || got.Attempts != 3 {
		t.Fatal("refresh restarted exhausted job")
	}
	other := requestFixture()
	other.Owner.GroupID++
	_ = claim(t, s, submit(t, s, other))
}

func TestTaskFailureCooldownExplicitRetryAndMaxDuration(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	j := submit(t, f.store, requestFixture())
	l := claim(t, f.store, j)
	requireError(t, f.store.Fail(ctx, l, "password=secret"), ErrInvalid)
	requireError(t, f.store.Complete(ctx, l, "partial"), ErrInvalid)
	if err := f.store.Fail(ctx, l, "source_unavailable"); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		got := submit(t, f.store, j.Request)
		if got.State != Failed || got.Attempts != 1 || got.ResultSHA256 != "" {
			t.Fatal("refresh retried failed task", got)
		}
	}
	requireError(t, f.store.Retry(ctx, j.Request.Owner, j.ID), ErrCooldown)
	f.clock.Add(retryDelay.Milliseconds() - 1)
	requireError(t, f.store.Retry(ctx, j.Request.Owner, j.ID), ErrCooldown)
	f.clock.Add(1)
	if err := f.store.Retry(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	requireError(t, f.store.Retry(ctx, j.Request.Owner, j.ID), ErrState)
	l = claim(t, f.store, j)
	start := f.clock.Load()
	for f.clock.Load()+10*time.Second.Milliseconds() < start+attemptLimit.Milliseconds() {
		f.clock.Add(10 * time.Second.Milliseconds())
		if err := f.store.Renew(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	f.clock.Store(start + attemptLimit.Milliseconds())
	requireError(t, f.store.Renew(ctx, l), ErrLeaseLost)
	requireError(t, f.store.Complete(ctx, l, strings.Repeat("f", 64)), ErrLeaseLost)
	third := claim(t, f.store, j)
	if err := f.store.Fail(ctx, third, "reconciliation_failed"); err != nil {
		t.Fatal(err)
	}
	f.clock.Add(retryDelay.Milliseconds())
	requireError(t, f.store.Retry(ctx, j.Request.Owner, j.ID), ErrExhausted)
}

func TestTaskInvalidationDoesNotTouchBillsOrOtherAccounts(t *testing.T) {
	for _, state := range []string{Queued, Running, DraftReady, Failed} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			if _, err := f.db.Exec(`CREATE TABLE existing_bill (payload TEXT); INSERT INTO existing_bill VALUES ('immutable customer bill'); CREATE TABLE source_balance (quota INTEGER); INSERT INTO source_balance VALUES (909785)`); err != nil {
				t.Fatal(err)
			}
			j := submit(t, f.store, requestFixture())
			var l Lease
			if state != Queued {
				l = claim(t, f.store, j)
			}
			switch state {
			case DraftReady:
				if err := f.store.Complete(ctx, l, strings.Repeat("f", 64)); err != nil {
					t.Fatal(err)
				}
			case Failed:
				if err := f.store.Fail(ctx, l, "evidence_incomplete"); err != nil {
					t.Fatal(err)
				}
			}
			other := requestFixture()
			other.Owner.GroupID++
			untouched := submit(t, f.store, other)
			for range 2 {
				if err := f.store.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
					t.Fatal(err)
				}
			}
			got, err := f.store.Get(ctx, j.Request.Owner, j.ID)
			if err != nil || got.State != Invalidated || got.ResultSHA256 != "" {
				t.Fatal("invalidation kept a usable draft", got, err)
			}
			requireError(t, f.store.Complete(ctx, l, strings.Repeat("f", 64)), ErrLeaseLost)
			_, err = f.store.Submit(ctx, j.Request)
			requireError(t, err, ErrStale)
			requireError(t, f.store.Retry(ctx, j.Request.Owner, j.ID), ErrState)
			fresh := j.Request
			fresh.InputSHA256 = strings.Repeat("1", 64)
			if got := submit(t, f.store, fresh); got.ID == j.ID || got.State != Queued {
				t.Fatal("new source reused stale task")
			}
			if got := submit(t, f.store, other); !reflect.DeepEqual(got, untouched) {
				t.Fatal("another organization changed")
			}
			var payload string
			var quota int64
			if err := f.db.QueryRow(`SELECT payload FROM existing_bill`).Scan(&payload); err != nil || payload != "immutable customer bill" {
				t.Fatal("existing bill changed", err)
			}
			if err := f.db.QueryRow(`SELECT quota FROM source_balance`).Scan(&quota); err != nil || quota != 909785 {
				t.Fatal("money changed", err)
			}
		})
	}
}

func TestTaskCapacityBoundsAndNoEviction(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	var jobs []Task
	for i := range maxActive {
		r := requestFixture()
		r.Owner.GroupID = int64(i + 1)
		jobs = append(jobs, submit(t, f.store, r))
	}
	r := requestFixture()
	r.Owner.GroupID = 999
	_, err := f.store.Submit(ctx, r)
	requireError(t, err, ErrBusy)
	r = jobs[0].Request
	r.Month = "2026-08"
	_, err = f.store.Submit(ctx, r)
	requireError(t, err, ErrBusy)
	for _, j := range jobs {
		if err := f.store.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
			t.Fatal(err)
		}
	}
	for i := maxActive; i < maxTasks; i++ {
		r := requestFixture()
		r.InputSHA256 = hash([]byte(fmt.Sprint(i)))
		j := submit(t, f.store, r)
		if err := f.store.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
			t.Fatal(err)
		}
	}
	fresh := requestFixture()
	fresh.InputSHA256 = hash([]byte("not previously stored"))
	_, err = f.store.Submit(ctx, fresh)
	requireError(t, err, ErrCapacity)
	if got, err := f.store.Get(ctx, jobs[0].Request.Owner, jobs[0].ID); err != nil || got.State != Invalidated {
		t.Fatal("retention erased old status", err)
	}
	var count int
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM portal_monthly_draft_tasks`).Scan(&count); err != nil || count != maxTasks {
		t.Fatal("task bound", count, err)
	}
}

func TestTaskOSProcessExitAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.sqlite")
	cmd := exec.Command(os.Args[0], "-test.run=^TestTaskCrashChild$")
	cmd.Env = append(os.Environ(), "MONTHLY_TASK_CRASH_FIXTURE="+path)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("crash fixture: %v %s", err, out)
	}
	clock := &atomic.Int64{}
	clock.Store(time.Date(2026, 10, 21, 12, 0, 0, 0, monthZone).UnixMilli())
	s, db := openTestStore(t, path, clock)
	j := submit(t, s, requestFixture())
	if j.State != Running || j.Attempts != 1 {
		t.Fatal("OS exit lost running state", j)
	}
	old, err := load(context.Background(), db, "id", j.ID)
	if err != nil {
		t.Fatal(err)
	}
	oldLease := Lease{old.ID, old.Key, old.token, old.Attempts}
	clock.Add(leaseDuration.Milliseconds())
	newLease := claim(t, s, j)
	requireError(t, s.Complete(context.Background(), oldLease, strings.Repeat("a", 64)), ErrLeaseLost)
	if err := s.Complete(context.Background(), newLease, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
}

func TestTaskSeparateProcessesCannotDoubleClaim(t *testing.T) {
	f := newFixture(t)
	j := submit(t, f.store, requestFixture())
	var wg sync.WaitGroup
	var winners, busy atomic.Int64
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestTaskCrashChild$")
			cmd.Env = append(os.Environ(), "MONTHLY_TASK_CRASH_FIXTURE="+f.path)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Errorf("worker exit: %v %s", err, out)
				return
			}
			switch exit.ExitCode() {
			case 23:
				winners.Add(1)
			case 24:
				busy.Add(1)
			default:
				t.Errorf("worker exit: %v %s", err, out)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 || busy.Load() != 3 {
		t.Fatal("process fencing", winners.Load(), busy.Load())
	}
	got, err := f.store.Get(context.Background(), j.Request.Owner, j.ID)
	if err != nil || got.Attempts != 1 || got.State != Running {
		t.Fatal("duplicate work persisted", got, err)
	}
}

// Run only by the parent with its private synthetic database. os.Exit bypasses
// DB.Close and cleanup intentionally, exercising actual OS lease recovery.
func TestTaskCrashChild(t *testing.T) {
	path := os.Getenv("MONTHLY_TASK_CRASH_FIXTURE")
	if path == "" {
		t.Skip("child process fixture invoked by TestTaskOSProcessExitAndRecovery")
	}
	clock := &atomic.Int64{}
	clock.Store(time.Date(2026, 10, 21, 12, 0, 0, 0, monthZone).UnixMilli())
	s, db := openTestStore(t, path, clock)
	if err := Init(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	j := submit(t, s, requestFixture())
	_, _, err := s.Claim(context.Background(), j.Request.Owner, j.ID)
	if errors.Is(err, ErrBusy) {
		os.Exit(24)
	}
	if err != nil {
		t.Fatal(err)
	}
	os.Exit(23)
}

func TestTaskLockedDatabaseIsBoundedAndRecoverable(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	j := submit(t, f.store, requestFixture())
	_, blocker := openTestStore(t, f.path, f.clock)
	tx, err := blocker.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_draft_task_control SET version=version WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	got, l, err := f.store.Claim(ctx, j.Request.Owner, j.ID)
	if err == nil || got != (Task{}) || l != (Lease{}) || time.Since(start) > 3*time.Second {
		t.Fatal("SQLite lock wait returned partial work or exceeded budget", err, time.Since(start))
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	_ = claim(t, f.store, j)
}

func TestTaskFailedWritesNeverReturnHalfSuccess(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	j := submit(t, f.store, requestFixture())
	if _, err := f.db.Exec(`CREATE TRIGGER reject_task_update BEFORE UPDATE ON portal_monthly_draft_tasks BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`); err != nil {
		t.Fatal(err)
	}
	got, l, err := f.store.Claim(ctx, j.Request.Owner, j.ID)
	if err == nil || got != (Task{}) || l != (Lease{}) {
		t.Fatal("failed write returned running capability")
	}
	if got := submit(t, f.store, j.Request); !reflect.DeepEqual(got, j) {
		t.Fatal("failed claim changed task")
	}
	if _, err := f.db.Exec(`DROP TRIGGER reject_task_update`); err != nil {
		t.Fatal(err)
	}
	l = claim(t, f.store, j)
	if _, err := f.db.Exec(`CREATE TRIGGER reject_task_result BEFORE UPDATE ON portal_monthly_draft_tasks WHEN NEW.state='draft_ready' BEGIN SELECT RAISE(ABORT,'synthetic result failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Complete(ctx, l, strings.Repeat("f", 64)); err == nil {
		t.Fatal("failed draft update passed")
	}
	got, err = f.store.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || got.State != Running || got.ResultSHA256 != "" {
		t.Fatal("partial result visible", got, err)
	}
	if _, err := f.db.Exec(`DROP TRIGGER reject_task_result`); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	requireError(t, f.store.Complete(canceled, l, strings.Repeat("f", 64)), context.Canceled)
	if err := f.store.Complete(ctx, l, strings.Repeat("f", 64)); err != nil {
		t.Fatal("safe replay after definite failure", err)
	}
}

func TestTaskCompletionInvalidationRaceAndRetryCompetition(t *testing.T) {
	f := newFixture(t)
	s2, _ := openTestStore(t, f.path, f.clock)
	ctx := context.Background()
	j := submit(t, f.store, requestFixture())
	l := claim(t, f.store, j)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := f.store.Complete(ctx, l, strings.Repeat("f", 64))
		if err != nil && !errors.Is(err, ErrLeaseLost) {
			t.Errorf("completion: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := s2.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
			t.Errorf("invalidation: %v", err)
		}
	}()
	wg.Wait()
	got, err := f.store.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || got.State != Invalidated || got.ResultSHA256 != "" {
		t.Fatal("late completion escaped invalidation", got, err)
	}
	r := requestFixture()
	r.InputSHA256 = strings.Repeat("1", 64)
	failed := submit(t, f.store, r)
	if err := f.store.Fail(ctx, claim(t, f.store, failed), "internal_failure"); err != nil {
		t.Fatal(err)
	}
	r.Month = "2026-08"
	_ = submit(t, s2, r)
	f.clock.Add(retryDelay.Milliseconds())
	requireError(t, f.store.Retry(ctx, failed.Request.Owner, failed.ID), ErrBusy)
	got, err = f.store.Get(ctx, failed.Request.Owner, failed.ID)
	if err != nil || got.State != Failed || got.Attempts != 1 {
		t.Fatal("retry competition changed failed task", got, err)
	}
}

func TestTaskCanceledBoundedAndClockRegression(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.store.Submit(ctx, requestFixture())
	requireError(t, err, context.Canceled)
	j := submit(t, f.store, requestFixture())
	f.db.SetMaxOpenConns(1)
	conn, err := f.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	start := time.Now()
	_, _, err = f.store.Claim(short, j.Request.Owner, j.ID)
	requireError(t, err, context.DeadlineExceeded)
	if time.Since(start) > time.Second {
		t.Fatal("pool wait unbounded")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	f.clock.Add(-1)
	_, _, err = f.store.Claim(context.Background(), j.Request.Owner, j.ID)
	requireError(t, err, ErrClock)
	f.clock.Add(1)
	l := claim(t, f.store, j)
	f.clock.Add(-1)
	requireError(t, f.store.Renew(context.Background(), l), ErrLeaseLost)
	f.clock.Add(1)
	if err := f.store.Complete(context.Background(), l, strings.Repeat("f", 64)); err != nil {
		t.Fatal("clock recovery", err)
	}
}

func TestTaskCorruptionAndNoImplicitInitialization(t *testing.T) {
	_, err := New(nil)
	requireError(t, err, ErrInvalid)
	requireError(t, Init(context.Background(), nil), ErrInvalid)
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixMilli())
	s, _ := openTestStore(t, filepath.Join(t.TempDir(), "uninitialized.sqlite"), clock)
	if _, err := s.Submit(context.Background(), requestFixture()); err == nil {
		t.Fatal("constructor must not initialize storage implicitly")
	}
	queries := []string{
		`UPDATE portal_monthly_draft_tasks SET descriptor='{}'`,
		`UPDATE portal_monthly_draft_tasks SET group_id=100`,
		`UPDATE portal_monthly_draft_tasks SET task_key='0000000000000000000000000000000000000000000000000000000000000000'`,
		`UPDATE portal_monthly_draft_tasks SET attempts=3`,
		`UPDATE portal_monthly_draft_tasks SET result_sha256='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'`,
		`UPDATE portal_monthly_draft_tasks SET lease_until=1`,
		`UPDATE portal_monthly_draft_tasks SET updated_at=0`,
	}
	for i, query := range queries {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			f := newFixture(t)
			j := submit(t, f.store, requestFixture())
			if _, err := f.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			_, err := f.store.Get(context.Background(), j.Request.Owner, j.ID)
			requireError(t, err, ErrCorrupt)
			_, _, err = f.store.Claim(context.Background(), j.Request.Owner, j.ID)
			requireError(t, err, ErrCorrupt)
		})
	}
	f := newFixture(t)
	if _, err := f.db.Exec(`UPDATE portal_monthly_draft_task_control SET version=2`); err != nil {
		t.Fatal(err)
	}
	requireError(t, Init(context.Background(), f.db), ErrCorrupt)
	_, err = f.store.Submit(context.Background(), requestFixture())
	requireError(t, err, ErrCorrupt)
}
