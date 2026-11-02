package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestAdaptiveTokenDeadlineClassificationDoesNotForkUnrelatedErrors(t *testing.T) {
	for _, tc := range []struct {
		stage string
		cause error
		want  bool
	}{
		{"user_totals", &mysql.MySQLError{Number: 3024}, true},
		{"token_groups", context.DeadlineExceeded, true},
		{"wallet", &mysql.MySQLError{Number: 3024}, false},
		{"token_groups", context.Canceled, false},
		{"token_groups", &mysql.MySQLError{Number: 1045}, false},
		{"token_groups", &mysql.MySQLError{Number: 1146}, false},
		{"validation", errTokenObservationMismatch, false},
		{"commit", context.DeadlineExceeded, false},
		{"transaction", context.DeadlineExceeded, false},
	} {
		if got := adaptiveTokenCanSplit(&tokenUsageCaptureFailure{stage: tc.stage, cause: tc.cause}); got != tc.want {
			t.Fatal("unsafe split classification", tc.stage, got)
		}
	}
	if adaptiveTokenCanSplit(context.DeadlineExceeded) {
		t.Fatal("unattributed caller deadline activated splitting")
	}
}

func TestAdaptiveTokenMoneyChangeIsRetainedAndNeverRetriedOrDivided(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	p, err := f.advance(t)
	if err != nil {
		t.Fatal(err)
	}
	f.now = time.UnixMilli(p.NextEligibleMS)
	baseCapture := f.ops.capture
	f.ops.capture = func(ctx context.Context, db *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
		o, err := baseCapture(ctx, db, r)
		if err != nil {
			return o, err
		}
		// A genuine different second result, internally consistent with its
		// SAME-TX user totals. It must not be replaced by the old amount.
		o.Totals[0].Quota++
		o.Rows[0].Quota++
		o.Totals[0].MaxQuota++
		o.Rows[0].MaxQuota++
		if err := ValidateTokenUsageWindowObservation(o); err != nil {
			t.Fatal("invalid changed-source fixture", err)
		}
		return o, nil
	}
	if p, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenIntegrity) || p.Complete {
		t.Fatal("changed money approved", p, err)
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_observations`) != 2 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM portal_monthly_usage_observations`) != 2 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_splits`) != 0 {
		t.Fatal("changed evidence lost, repaired or split")
	}
	queries := f.queries()
	if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenStopped) || queries != f.queries() {
		t.Fatal("money mismatch retried source", err)
	}
	if _, err := LoadAdaptiveTokenWindowPairs(t.Context(), f.store, f.job, f.root); err == nil {
		t.Fatal("changed originals returned as an accepted pair")
	}
}

func TestAdaptiveTokenCaptureAndProjectionAreOneTransaction(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	if _, err := f.store.Exec(`CREATE TRIGGER synthetic_projection_failure BEFORE INSERT ON portal_monthly_usage_observations BEGIN SELECT RAISE(ABORT,'synthetic local failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.advance(t); err == nil {
		t.Fatal("local write failure ignored")
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_observations`) != 0 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM portal_monthly_usage_observations`) != 0 {
		t.Fatal("partial token/total record survived rollback")
	}
}

func TestAdaptiveTokenBudgetsAndCancellationStopBeforeSource(t *testing.T) {
	for _, scenario := range []string{"canceled", "expired", "full_bytes", "invalid_scope"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAdaptiveTokenFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch scenario {
			case "canceled":
				cancel()
			case "expired":
				if err := ensureAdaptiveTokenRoot(ctx, f.store, f.job, f.root, f.now); err != nil {
					t.Fatal(err)
				}
				f.now = f.now.Add(25 * time.Hour)
			case "full_bytes":
				// Damaged/abandoned other-job bytes still occupy the global
				// budget. They cannot be ignored to allocate a new source read.
				tx, err := f.store.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback() }()
				for i := 0; i < 32; i++ {
					id := digest([]byte("abandoned synthetic root " + strconv.Itoa(i)))
					_, err := tx.Exec(`INSERT INTO monthly_adaptive_token_observations VALUES(?,1,2,0,?,?,1,2,zeroblob(2097152),x'00')`, id, id, id)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "invalid_scope":
				f.root.UserID = 0
			}
			_, err := advanceAdaptiveTokenWindow(ctx, f.source, f.store, f.job, f.root, f.ops)
			if err == nil || f.queries() != 0 {
				t.Fatal("invalid/budgeted request reached source", err)
			}
			if scenario == "full_bytes" {
				if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenStopped) || f.queries() != 0 {
					t.Fatal("full-byte budget did not persist a stop", err)
				}
			}
		})
	}
}

func TestAdaptiveTokenEvidenceTablesAreImmutableAndScopeBound(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	f.fail = func(r UsageWindowRequest) error {
		if r == f.root {
			return &mysql.MySQLError{Number: 3024}
		}
		return nil
	}
	if _, err := f.advance(t); err != nil {
		t.Fatal(err)
	}
	if _, err := f.advance(t); err != nil {
		t.Fatal(err)
	}
	f.fail = func(UsageWindowRequest) error { return &mysql.MySQLError{Number: 1045} }
	if _, err := f.advance(t); err == nil {
		t.Fatal("synthetic source fault missing")
	}
	for _, tc := range []struct{ table, column string }{{"roots", "created_at_ms"}, {"splits", "phase"}, {"observations", "sha"}, {"faults", "code"}} {
		table := "monthly_adaptive_token_" + tc.table
		if adaptiveTokenCount(t, f.store, "SELECT count(*) FROM "+table) == 0 {
			t.Fatal("immutability fixture did not create evidence", table)
		}
		if _, err := f.store.Exec("UPDATE " + table + " SET " + tc.column + "=" + tc.column); err == nil {
			t.Fatal("evidence update permitted", table)
		}
		if _, err := f.store.Exec("DELETE FROM " + table); err == nil {
			t.Fatal("evidence deletion permitted", table)
		}
	}
	other := f.root
	other.UserID++
	if _, err := LoadAdaptiveTokenWindowPairs(t.Context(), f.store, f.job, other); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("another account reused this root", err)
	}
}

func TestAdaptiveTokenRootLimitAllowsExistingRootReuse(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	for i := 0; i < adaptiveTokenJobs; i++ {
		job := digest([]byte(strconv.Itoa(i)))
		if err := ensureAdaptiveTokenRoot(t.Context(), f.store, job, f.root, f.now); err != nil {
			t.Fatal(err)
		}
	}
	if err := ensureAdaptiveTokenRoot(t.Context(), f.store, f.job, f.root, f.now); !errors.Is(err, ErrAdaptiveTokenBudget) {
		t.Fatal("root budget ignored", err)
	}
	if err := ensureAdaptiveTokenRoot(t.Context(), f.store, digest([]byte("0")), f.root, f.now); err != nil {
		t.Fatal("valid existing root could not be reused", err)
	}
}

func TestAdaptiveTokenPublicEntryUsesPacedOriginalReadOnlyProtocol(t *testing.T) {
	source, d, req := tokenUsageSource(t)
	store, _ := walletTestStore(t)
	if err := InitAdaptiveTokenCaptureStore(t.Context(), store); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	p, err := AdvanceAdaptiveTokenWindow(t.Context(), source, store, digest([]byte("synthetic public entry")), req)
	if err != nil || !p.Captured || p.Complete || p.Observations != 1 || time.Since(start) < time.Second {
		t.Fatal("public collector omitted pacing or source phase", p, err)
	}
	if len(d.usage.s.queries) != 4 || !d.usage.s.options.ReadOnly || source.Stats().InUse != 0 || store.Stats().InUse != 0 {
		t.Fatal("public entry changed SQL protocol or retained a connection")
	}
}

func TestAdaptiveTokenConcurrentOwnerAndCancellationReleaseWithoutSourceFault(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	entered := make(chan struct{})
	f.driver.usage.block = entered
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := advanceAdaptiveTokenWindow(ctx, f.source, f.store, f.job, f.root, f.ops)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("source capture did not enter")
	}
	if _, err := f.advance(t); !errors.Is(err, ErrWalletCaptureBusy) {
		t.Fatal("concurrent source owner accepted", err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("caller cancellation was not preserved", err)
		}
	case <-time.After(time.Second):
		t.Fatal("source capture did not cancel")
	}
	// database/sql may return ErrTxDone while its cancellation goroutine is
	// still returning the connection. Prove both one-connection pools are
	// reusable within a bound; do not mistake that transient for a leaked slot.
	released, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	for _, db := range []*sql.DB{f.source, f.store} {
		conn, err := db.Conn(released)
		if err != nil {
			t.Fatal("cancelled transaction did not release its connection", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_faults`) != 0 || f.source.Stats().InUse != 0 || f.store.Stats().InUse != 0 {
		t.Fatal("cancellation leaked a connection or consumed source failure budget")
	}
	f.driver.usage.block = nil
	if p, err := f.advance(t); err != nil || !p.Captured {
		t.Fatal("cancelled owner retained the capture slot", p, err)
	}
}

func TestAdaptiveTokenPublicEntryDoesNotInitializeMissingStore(t *testing.T) {
	source, d, req := tokenUsageSource(t)
	store, _ := walletTestStore(t)
	if _, err := AdvanceAdaptiveTokenWindow(t.Context(), source, store, digest([]byte("missing-store-synthetic")), req); err == nil || len(d.usage.s.queries) != 0 {
		t.Fatal("missing store accepted or source contacted", err)
	}
	if adaptiveTokenCount(t, store, `SELECT count(*) FROM sqlite_master WHERE name LIKE 'monthly_adaptive_token_%'`) != 0 {
		t.Fatal("public collector initialized schema implicitly")
	}
}

func TestAdaptiveTokenSeparateHandlesCannotAliasCaptureAndSource(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	alias, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer alias.Close()
	if _, err := AdvanceAdaptiveTokenWindow(t.Context(), alias, f.store, f.job, f.root); !errors.Is(err, ErrAdaptiveTokenIntegrity) {
		t.Fatal("SQLite alias accepted as MySQL source", err)
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_roots`) != 0 {
		t.Fatal("aliased source modified before rejection")
	}
}
