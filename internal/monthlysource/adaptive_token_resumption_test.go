package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func resumeFixture(t *testing.T, original bool) *adaptiveTokenFixture {
	t.Helper()
	f := newAdaptiveTokenFixture(t)
	if err := InitAdaptiveTokenResumptionStore(t.Context(), f.store); err != nil {
		t.Fatal(err)
	}
	if original {
		p, err := f.advance(t)
		if err != nil || !p.Captured {
			t.Fatal(p, err)
		}
		f.now = time.UnixMilli(p.NextEligibleMS)
	}
	exhaustAdaptiveConnection(t, f)
	return f
}

func exhaustAdaptiveConnection(t *testing.T, f *adaptiveTokenFixture) {
	t.Helper()
	f.fail = func(UsageWindowRequest) error { return io.EOF }
	for i := 0; i < 3; i++ {
		if _, err := f.advance(t); !errors.Is(err, io.EOF) {
			t.Fatal("expected actual query connection failure", i, err)
		}
		f.now = f.now.Add(31 * time.Second)
	}
	queries := f.queries()
	if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenStopped) || queries != f.queries() {
		t.Fatal("exhausted root queried source", err)
	}
}

func previewResumeFixture(t *testing.T, f *adaptiveTokenFixture) AdaptiveTokenResumePlan {
	t.Helper()
	p, err := previewAdaptiveTokenResume(t.Context(), f.store, f.job, f.root, f.now)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func applyResumeFixture(t *testing.T, f *adaptiveTokenFixture, p AdaptiveTokenResumePlan) AdaptiveTokenResumeResult {
	t.Helper()
	if f.now.UnixMilli() < p.EarliestMS {
		f.now = time.UnixMilli(p.EarliestMS)
	}
	r, err := applyAdaptiveTokenResume(t.Context(), f.store, p, f.now)
	if err != nil || !r.Applied || !validDigest(r.ReceiptSHA256) {
		t.Fatal("resume failed", r, err)
	}
	return r
}

func TestAdaptiveTokenResumeConnectionClassification(t *testing.T) {
	network := &net.OpError{Op: "read", Net: "tcp", Err: io.ErrUnexpectedEOF}
	for _, tc := range []struct {
		stage string
		err   error
		want  bool
	}{
		{"token_groups", io.EOF, true}, {"user_totals", driver.ErrBadConn, true},
		{"transaction", network, true}, {"commit", network, true},
		{"validation", network, false}, {"", network, false},
		{"token_groups", context.DeadlineExceeded, false}, {"commit", context.Canceled, false},
		{"token_groups", &mysql.MySQLError{Number: 1045}, false},
		{"token_groups", &mysql.MySQLError{Number: 1146}, false},
		{"token_groups", &mysql.MySQLError{Number: 3024}, false},
	} {
		if got := adaptiveTokenRetryableSource(&tokenUsageCaptureFailure{stage: tc.stage, cause: tc.err}); got != tc.want {
			t.Fatal(tc.stage, tc.err, got)
		}
	}
	if adaptiveTokenConnectionError(context.DeadlineExceeded) || adaptiveTokenConnectionError(&mysql.MySQLError{Number: 1045}) {
		t.Fatal("deadline/authentication classified as connection")
	}
}

func TestAdaptiveTokenResumePreservesOriginalMoneyAndRestarts(t *testing.T) {
	f := resumeFixture(t, true)
	s, err := loadAdaptiveTokenStateMode(t.Context(), f.store, f.job, f.root, f.now, true)
	if err != nil {
		t.Fatal(err)
	}
	original, err := loadAdaptiveTokenRecord(t.Context(), f.store, s, f.root, 0)
	if err != nil {
		t.Fatal(err)
	}
	queries, changes := f.queries(), adaptiveTokenCount(t, f.store, `SELECT total_changes()`)
	p := previewResumeFixture(t, f)
	if queries != f.queries() || changes != adaptiveTokenCount(t, f.store, `SELECT total_changes()`) {
		t.Fatal("preview queried source or wrote")
	}
	if _, err := applyAdaptiveTokenResume(t.Context(), f.store, p, f.now); !errors.Is(err, ErrAdaptiveTokenResumeCooldown) {
		t.Fatal("cooldown bypassed", err)
	}
	r := applyResumeFixture(t, f, p)
	if queries != f.queries() || r.AlreadyApplied || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_faults`) != 3 {
		t.Fatal("apply recaptured source or cleared old failures")
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = sql.Open("sqlite", f.path+"?_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	f.store.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = f.store.Close() })
	f.fail = nil
	finishAdaptiveFixture(t, f)
	s, err = loadAdaptiveTokenState(t.Context(), f.store, f.job, f.root, f.now)
	if err != nil || s.epoch != 1 || s.created != p.RootCreatedMS {
		t.Fatal(s.epoch, err)
	}
	retained, err := loadAdaptiveTokenRecord(t.Context(), f.store, s, f.root, 0)
	if err != nil || !reflect.DeepEqual(original, retained) {
		t.Fatal("original replaced", err)
	}
	pairs, err := LoadAdaptiveTokenWindowPairs(t.Context(), f.store, f.job, f.root)
	if err != nil || len(pairs) != 1 || pairs[0].Newer.Rows[0].Quota != 11604 || !TokenUsageWindowsMatch(pairs[0].Older, pairs[0].Newer) {
		t.Fatal("resumed money changed", pairs, err)
	}
	if _, err := previewAdaptiveTokenResume(t.Context(), f.store, f.job, f.root, f.now); !errors.Is(err, ErrAdaptiveTokenResumeDenied) {
		t.Fatal("complete root granted another epoch", err)
	}
}

func TestAdaptiveTokenResumeEpochLimitAndReplayNeverResetFaults(t *testing.T) {
	f := resumeFixture(t, false)
	first := previewResumeFixture(t, f)
	applyResumeFixture(t, f, first)
	exhaustAdaptiveConnection(t, f)
	queries := f.queries()
	r := applyResumeFixture(t, f, first)
	if !r.AlreadyApplied || f.queries() != queries {
		t.Fatal("replay reactivated or queried")
	}
	if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenStopped) {
		t.Fatal("replay reset failures", err)
	}
	second := previewResumeFixture(t, f)
	if second.Ordinal != 2 || second.ParentSHA256 != r.ReceiptSHA256 || second.RootCreatedMS != first.RootCreatedMS {
		t.Fatal("audit chain or age reset")
	}
	applyResumeFixture(t, f, second)
	exhaustAdaptiveConnection(t, f)
	if _, err := previewAdaptiveTokenResume(t.Context(), f.store, f.job, f.root, f.now); !errors.Is(err, ErrAdaptiveTokenResumeDenied) {
		t.Fatal("unlimited epochs", err)
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_faults`) != 3 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resume_faults`) != 6 {
		t.Fatal("lifetime failure evidence lost")
	}
	for _, q := range []string{
		`UPDATE monthly_adaptive_token_resumptions SET sha256=sha256`,
		`DELETE FROM monthly_adaptive_token_resumptions`,
		`UPDATE monthly_adaptive_token_resume_faults SET code=code`,
		`DELETE FROM monthly_adaptive_token_resume_faults`,
	} {
		if _, err := f.store.Exec(q); err == nil {
			t.Fatal("mutable audit", q)
		}
	}
}

func TestAdaptiveTokenResumeRejectsUnsafeFailuresAndExpiry(t *testing.T) {
	for _, code := range []string{"transport", "source", "pair_changed", "minimum_window", "byte_budget", "expired", "terminal", "cooldown_exceeds_age"} {
		t.Run(code, func(t *testing.T) {
			f := newAdaptiveTokenFixture(t)
			if err := InitAdaptiveTokenResumptionStore(t.Context(), f.store); err != nil {
				t.Fatal(err)
			}
			if err := ensureAdaptiveTokenRoot(t.Context(), f.store, f.job, f.root, f.now); err != nil {
				t.Fatal(err)
			}
			if code == "cooldown_exceeds_age" {
				f.now = f.now.Add(24*time.Hour - 70*time.Second)
			}
			for i := 0; i < 3; i++ {
				s, err := loadAdaptiveTokenStateMode(t.Context(), f.store, f.job, f.root, f.now, true)
				if err != nil {
					t.Fatal(err)
				}
				failure := code
				if code == "expired" || code == "terminal" || code == "cooldown_exceeds_age" {
					failure = "retryable_source"
				}
				if err := saveAdaptiveTokenFault(t.Context(), f.store, s, failure, code == "terminal", f.now); err != nil {
					t.Fatal(err)
				}
			}
			if code == "expired" {
				f.now = f.now.Add(24 * time.Hour)
			}
			if _, err := previewAdaptiveTokenResume(t.Context(), f.store, f.job, f.root, f.now); !errors.Is(err, ErrAdaptiveTokenResumeDenied) {
				t.Fatal("unsafe failure resumed", err)
			}
			if f.queries() != 0 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resumptions`) != 0 {
				t.Fatal("denial touched source/audit")
			}
		})
	}
}

func TestAdaptiveTokenResumeSchemaIsExplicitAndPartialSchemaFailsClosed(t *testing.T) {
	f := newAdaptiveTokenFixture(t)
	exhaustAdaptiveConnection(t, f)
	if _, err := previewAdaptiveTokenResume(t.Context(), f.store, f.job, f.root, f.now); !errors.Is(err, ErrAdaptiveTokenResumeDenied) {
		t.Fatal(err)
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM sqlite_master WHERE name='monthly_adaptive_token_resumptions'`) != 0 {
		t.Fatal("implicit migration")
	}
	if _, err := f.store.Exec(`CREATE TABLE monthly_adaptive_token_resumptions(root_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenIntegrity) {
		t.Fatal("partial migration silently ignored", err)
	}
	bare, _ := walletTestStore(t)
	if err := InitAdaptiveTokenResumptionStore(t.Context(), bare); !errors.Is(err, ErrAdaptiveTokenIntegrity) {
		t.Fatal("created missing base store", err)
	}
	if adaptiveTokenCount(t, bare, `SELECT count(*) FROM sqlite_master WHERE name='monthly_adaptive_token_resumptions'`) != 0 {
		t.Fatal("failed init wrote schema")
	}
}

func TestAdaptiveTokenResumeRollbackStalePlanAndConcurrentApply(t *testing.T) {
	f := resumeFixture(t, false)
	p := previewResumeFixture(t, f)
	queries := f.queries()
	f.now = time.UnixMilli(p.EarliestMS)
	bad := p
	bad.EvidenceSHA256 = digest([]byte("forged"))
	if _, err := applyAdaptiveTokenResume(t.Context(), f.store, bad, f.now); !errors.Is(err, ErrAdaptiveTokenResumeChanged) {
		t.Fatal(err)
	}
	if _, err := f.store.Exec(`CREATE TRIGGER synthetic_resume_insert BEFORE INSERT ON monthly_adaptive_token_resumptions BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := applyAdaptiveTokenResume(t.Context(), f.store, p, f.now); err == nil {
		t.Fatal("failed insert accepted")
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resumptions`) != 0 {
		t.Fatal("partial resume committed")
	}
	if _, err := f.store.Exec(`DROP TRIGGER synthetic_resume_insert`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan AdaptiveTokenResumeResult, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Go(func() { r, err := applyAdaptiveTokenResume(t.Context(), f.store, p, f.now); results <- r; errs <- err })
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	applied := 0
	for r := range results {
		if r.Applied && !r.AlreadyApplied {
			applied++
		}
	}
	if applied != 1 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resumptions`) != 1 || f.queries() != queries {
		t.Fatal("concurrent resume duplicated or queried", applied, f.queries())
	}
}

func TestAdaptiveTokenResumeRejectsLostOriginalAndProjection(t *testing.T) {
	for _, scenario := range []string{"raw", "projection", "usage", "receipt", "orphan_fault", "missing_trigger"} {
		t.Run(scenario, func(t *testing.T) {
			f := resumeFixture(t, true)
			p := previewResumeFixture(t, f)
			if scenario == "receipt" {
				applyResumeFixture(t, f, p)
			}
			queries := f.queries()
			var statements []string
			switch scenario {
			case "raw":
				statements = []string{`DROP TRIGGER adaptive_token_observations_no_UPDATE`, `UPDATE monthly_adaptive_token_observations SET payload=x'00'`}
			case "projection":
				statements = []string{`DROP TRIGGER adaptive_token_observations_no_UPDATE`, `UPDATE monthly_adaptive_token_observations SET projection=x'00'`}
			case "usage":
				statements = []string{`DROP TABLE portal_monthly_usage_observations`}
			case "receipt":
				statements = []string{`DROP TRIGGER adaptive_resumption_no_update`, `UPDATE monthly_adaptive_token_resumptions SET sha256=printf('%064d',0)`, `CREATE TRIGGER adaptive_resumption_no_update BEFORE UPDATE ON monthly_adaptive_token_resumptions BEGIN SELECT RAISE(ABORT,'adaptive resumptions are immutable'); END`}
			case "orphan_fault":
				statements = []string{`INSERT INTO monthly_adaptive_token_resume_faults SELECT id,1,0,'retryable_source',created_at_ms,0 FROM monthly_adaptive_token_roots`}
			case "missing_trigger":
				statements = []string{`DROP TRIGGER adaptive_resume_fault_no_delete`}
			}
			for _, q := range statements {
				if _, err := f.store.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			f.now = time.UnixMilli(p.EarliestMS)
			if _, err := applyAdaptiveTokenResume(t.Context(), f.store, p, f.now); err == nil {
				t.Fatal("damaged evidence resumed")
			}
			if f.queries() != queries {
				t.Fatal("damage triggered new source read")
			}
		})
	}
}

func TestAdaptiveTokenResumePublicPreviewAndStaleEpochFaultWrite(t *testing.T) {
	f := resumeFixture(t, false)
	p := previewResumeFixture(t, f)
	queries := f.queries()
	public, err := PreviewAdaptiveTokenResumption(t.Context(), f.store, f.job, f.root)
	if err != nil || public != p || f.queries() != queries {
		t.Fatal("public preview changed scope or snapshot", err)
	}
	stale, err := loadAdaptiveTokenStateMode(t.Context(), f.store, f.job, f.root, f.now, true)
	if err != nil {
		t.Fatal(err)
	}
	applyResumeFixture(t, f, p)
	if err := saveAdaptiveTokenFault(t.Context(), f.store, stale, "retryable_source", false, f.now); !errors.Is(err, ErrAdaptiveTokenIntegrity) {
		t.Fatal("stale epoch wrote a fault", err)
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resume_faults`) != 0 {
		t.Fatal("stale epoch consumed new budget")
	}
	for _, scenario := range []string{"nil_context", "canceled", "nil_db"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			db := f.store
			switch scenario {
			case "nil_context":
				ctx = nil
			case "canceled":
				cancel()
			case "nil_db":
				db = nil
			}
			if _, err := PreviewAdaptiveTokenResumption(ctx, db, f.job, f.root); err == nil {
				t.Fatal("invalid preview accepted")
			}
			if _, err := ApplyAdaptiveTokenResumption(ctx, db, p); err == nil {
				t.Fatal("invalid apply accepted")
			}
		})
	}
}

func TestAdaptiveTokenResumeAuditSharesCaptureCapacity(t *testing.T) {
	f := resumeFixture(t, false)
	p := previewResumeFixture(t, f)
	r := applyResumeFixture(t, f, p)
	used, err := adaptiveTokenRetainedBytes(t.Context(), f.store, false)
	if err != nil || used <= 0 || !r.Applied {
		t.Fatal("audit bytes not charged", used, err)
	}
	// Unrelated abandoned raw data occupies the SAME pool. Fill to one byte
	// below the cap; the next legitimate capture must roll back, not overflow.
	tx, err := f.store.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	remaining := adaptiveTokenBytes - used - 1
	for i := 0; remaining > 0; i++ {
		n := min(remaining, int64(2097153))
		id := digest([]byte("abandoned resume capacity " + strconv.Itoa(i)))
		if _, err := tx.Exec(`INSERT INTO monthly_adaptive_token_observations VALUES(?,1,2,0,?,?,1,2,zeroblob(?),x'00')`, id, id, id, n-1); err != nil {
			t.Fatal(err)
		}
		remaining -= n
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f.fail = nil
	if _, err := f.advance(t); !errors.Is(err, ErrAdaptiveTokenBudget) {
		t.Fatal("post-resume capture exceeded pool", err)
	}
	used, err = adaptiveTokenRetainedBytes(t.Context(), f.store, false)
	if err != nil || used != adaptiveTokenBytes-1 {
		t.Fatal("failed capture wrote partial raw/projection", used, err)
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resume_faults WHERE code='byte_budget' AND terminal=1`) != 1 {
		t.Fatal("capacity failure not stopped")
	}
}

func TestAdaptiveTokenResumeMonthSchedulerPreservesBindingAndChildMoney(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	if err := InitAdaptiveTokenResumptionStore(t.Context(), m.f.store); err != nil {
		t.Fatal(err)
	}
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	} // real daily timeout -> bound split
	base := m.ops.capture
	m.ops.capture = func(ctx context.Context, db *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
		m.f.driver.queryErr = io.EOF
		m.f.driver.usage.data = nil
		return captureTokenUsageWindow(ctx, db, r, func() time.Time { return m.f.now })
	}
	for i := 0; i < 3; i++ {
		if _, err := m.advance(t); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		m.f.now = m.f.now.Add(31 * time.Second)
	}
	if _, err := m.advance(t); !errors.Is(err, ErrAdaptiveTokenStopped) {
		t.Fatal(err)
	}
	p := previewResumeFixture(t, m.f)
	queries := m.f.queries()
	applyResumeFixture(t, m.f, p)
	if m.f.queries() != queries || m.parentCalls != 1 {
		t.Fatal("activation reread source/parent")
	}
	m.ops.capture = base
	finishRecoveringMonth(t, m)
	pairs, _, err := LoadRecoveredTokenMonth(t.Context(), m.f.store, m.f.job, m.scope, m.f.root.UserID)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, pair := range pairs {
		for _, row := range pair.Newer.Rows {
			total += row.Quota
		}
	}
	if total != 11604 || m.parentCalls != 1 || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_token_recovery_bindings`) != 1 {
		t.Fatal("binding/child total changed", total, m.parentCalls)
	}
}

func TestAdaptiveTokenResumeApplyRejectsFullJointPoolWithoutReceipt(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	f := m.f
	if err := InitAdaptiveTokenResumptionStore(t.Context(), f.store); err != nil {
		t.Fatal(err)
	}
	exhaustAdaptiveConnection(t, f)
	p := previewResumeFixture(t, f)
	f.now = time.UnixMilli(p.EarliestMS)
	fillRecoveringMonthBudget(t, m)
	queries := f.queries()
	if _, err := applyAdaptiveTokenResume(t.Context(), f.store, p, f.now); !errors.Is(err, ErrAdaptiveTokenBudget) {
		t.Fatal("full shared pool resumed", err)
	}
	if f.queries() != queries || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resumptions`) != 0 {
		t.Fatal("full-pool apply wrote/queried")
	}
}
