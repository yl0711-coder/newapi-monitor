package monthlytasks

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"testing"
)

func admissionTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS admission_fixture (id TEXT PRIMARY KEY,payload BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
}

func admissionCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var tasks, inputs int
	if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM portal_monthly_draft_tasks),
(SELECT COUNT(*) FROM admission_fixture)`).Scan(&tasks, &inputs); err != nil || tasks != want || inputs != want {
		t.Fatal("partial admission", tasks, inputs, want, err)
	}
}

func attachFixture(ctx context.Context, tx *sql.Tx, existing bool) error {
	if existing {
		var body string
		if err := tx.QueryRowContext(ctx, `SELECT payload FROM admission_fixture WHERE id='input'`).Scan(&body); err != nil {
			return err
		}
		if body != "immutable inputs" {
			return ErrCorrupt
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO admission_fixture(id,payload) VALUES('input','immutable inputs')`)
	return err
}

func TestTaskAdmissionInputsRollbackAndReuse(t *testing.T) {
	f := newFixture(t)
	admissionTable(t, f.db)
	ctx := context.Background()
	r := requestFixture()
	if j, err := f.store.SubmitWithInputs(ctx, r, nil); !errors.Is(err, ErrInvalid) || j != (Task{}) {
		t.Fatal("nil preparation accepted", j, err)
	}
	injected := errors.New("synthetic attachment failure")
	for _, cancelAfterWrite := range []bool{false, true} {
		work, cancel := context.WithCancel(ctx)
		j, err := f.store.SubmitWithInputs(work, r, func(ctx context.Context, tx *sql.Tx, existing bool) error {
			if err := attachFixture(ctx, tx, existing); err != nil {
				return err
			}
			if cancelAfterWrite {
				cancel()
				return nil
			}
			return injected
		})
		cancel()
		want := injected
		if cancelAfterWrite {
			want = context.Canceled
		}
		if !errors.Is(err, want) || j != (Task{}) {
			t.Fatal("uncommitted task returned", j, err)
		}
		admissionCount(t, f.db, 0)
	}
	j, err := f.store.SubmitWithInputs(ctx, r, attachFixture)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		got, err := f.store.SubmitWithInputs(ctx, r, attachFixture)
		if err != nil || got != j {
			t.Fatal("exact request did not reuse input/task", got, err)
		}
	}
	admissionCount(t, f.db, 1)
	// Reuse validation failure must not change the task, even if a buggy
	// trusted storage callback performed an input write before returning error.
	if got, err := f.store.SubmitWithInputs(ctx, r, func(ctx context.Context, tx *sql.Tx, existing bool) error {
		if !existing {
			return ErrState
		}
		if _, err := tx.ExecContext(ctx, `UPDATE admission_fixture SET payload='bad replacement'`); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) || got != (Task{}) {
		t.Fatal("reuse validation failure returned task", got, err)
	}
	if got, err := f.store.SubmitWithInputs(ctx, r, attachFixture); err != nil || got != j {
		t.Fatal("failed reuse changed input/task", got, err)
	}
}

func TestTaskAdmissionRejectedRequestsDoNotPrepareInputs(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	r := requestFixture()
	j := submit(t, f.store, r)
	calls := 0
	prepare := func(context.Context, *sql.Tx, bool) error {
		calls++
		return nil
	}
	busy := r
	busy.Month = "2026-08"
	_, err := f.store.SubmitWithInputs(ctx, busy, prepare)
	requireError(t, err, ErrBusy)
	invalid := r
	invalid.Owner.GroupID = 0
	_, err = f.store.SubmitWithInputs(ctx, invalid, prepare)
	requireError(t, err, ErrInvalid)
	for i := range maxActive - 1 {
		other := r
		other.Owner.GroupID = int64(100 + i)
		_ = submit(t, f.store, other)
	}
	full := r
	full.Owner.GroupID = 999
	_, err = f.store.SubmitWithInputs(ctx, full, prepare)
	requireError(t, err, ErrBusy)
	if err := f.store.Invalidate(ctx, r.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	_, err = f.store.SubmitWithInputs(ctx, r, prepare)
	requireError(t, err, ErrStale)
	if calls != 0 {
		t.Fatal("rejected admission ran input preparation", calls)
	}
}

func TestTaskAdmissionCommitFailureDoesNotPoisonConnection(t *testing.T) {
	f := newFixture(t)
	f.db.SetMaxOpenConns(1)
	admissionTable(t, f.db)
	for _, query := range []string{
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE admission_parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE admission_deferred (parent_id INTEGER REFERENCES admission_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
	} {
		if _, err := f.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	j, err := f.store.SubmitWithInputs(context.Background(), requestFixture(), func(ctx context.Context, tx *sql.Tx, existing bool) error {
		if err := attachFixture(ctx, tx, existing); err != nil {
			return err
		}
		// The insert succeeds, but COMMIT itself fails. This is different from
		// a statement error or context cancellation before the commit point.
		_, err := tx.ExecContext(ctx, `INSERT INTO admission_deferred(parent_id) VALUES(99)`)
		return err
	})
	if err == nil || j != (Task{}) {
		t.Fatal("failed commit returned success", j, err)
	}
	admissionCount(t, f.db, 0)
	if _, err := f.store.SubmitWithInputs(context.Background(), requestFixture(), attachFixture); err != nil {
		t.Fatal("failed commit left receiver connection unusable", err)
	}
	admissionCount(t, f.db, 1)
}

func TestTaskAdmissionOSExitRollsBackBothRecords(t *testing.T) {
	f := newFixture(t)
	admissionTable(t, f.db)
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestTaskAdmissionCrashChild$")
	cmd.Env = append(os.Environ(), "MONTHLY_TASK_ADMISSION_CRASH="+f.path)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 31 {
		t.Fatalf("admission crash fixture: %v %s", err, out)
	}
	s, db := openTestStore(t, f.path, f.clock)
	admissionCount(t, db, 0)
	if _, err := s.SubmitWithInputs(context.Background(), requestFixture(), attachFixture); err != nil {
		t.Fatal("recovered receiver cannot admit task", err)
	}
	admissionCount(t, db, 1)
}

func TestTaskAdmissionReaderBlocksCommitAndRecovers(t *testing.T) {
	f := newFixture(t)
	admissionTable(t, f.db)
	ctx := context.Background()
	if _, err := f.db.Exec(`PRAGMA journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback() }()
	var before int
	if err := reader.QueryRow(`SELECT COUNT(*) FROM portal_monthly_draft_tasks`).Scan(&before); err != nil || before != 0 {
		t.Fatal("reader did not acquire its snapshot", before, err)
	}
	// The writer can reserve and insert, but cannot commit while this reader
	// retains its SHARED lock. The configured short busy timeout must fail.
	j, err := f.store.SubmitWithInputs(ctx, requestFixture(), attachFixture)
	if err == nil || j != (Task{}) {
		t.Fatal("blocked commit returned success", j, err)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	admissionCount(t, f.db, 0)
	if _, err := f.store.SubmitWithInputs(ctx, requestFixture(), attachFixture); err != nil {
		t.Fatal("lock release did not restore admission", err)
	}
	admissionCount(t, f.db, 1)
}

// Invoked by its parent only against a temporary synthetic SQLite. Exit after
// both inserts but before Commit, bypassing deferred rollback/DB.Close.
func TestTaskAdmissionCrashChild(t *testing.T) {
	path := os.Getenv("MONTHLY_TASK_ADMISSION_CRASH")
	if path == "" {
		t.Skip("subprocess fixture invoked by TestTaskAdmissionOSExitRollsBackBothRecords")
	}
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitWithInputs(context.Background(), requestFixture(), func(ctx context.Context, tx *sql.Tx, existing bool) error {
		if existing {
			return ErrState
		}
		// Force enough dirty pages for rollback-journal recovery to matter;
		// this is not a balance, customer bill, source file or business table.
		if _, err := tx.ExecContext(ctx, `INSERT INTO admission_fixture(id,payload) VALUES('input',zeroblob(4194304))`); err != nil {
			return err
		}
		os.Exit(31)
		return nil
	})
	t.Fatal("crash point not reached", err)
}
