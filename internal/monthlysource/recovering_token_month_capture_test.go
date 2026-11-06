package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type recoveringMonthFixture struct {
	f           *adaptiveTokenFixture
	scope       OrganizationMonthScope
	plan        FundingMonthCapturePlan
	ops         adaptiveTokenCaptureOps
	requests    []UsageWindowRequest
	parentCalls int
	second      bool
}

func newRecoveringMonthFixture(t *testing.T, second bool) *recoveringMonthFixture {
	t.Helper()
	f := newAdaptiveTokenFixture(t)
	db, path, key, scope, plan := tokenCaptureFixture(t, f.root.SourceIdentity)
	f.store, f.path, f.job = db, path, key
	if err := InitRecoveringTokenMonthStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := InitWalletMonthCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	fillCapturedSelectionFundsAndWallets(t, db, scope, plan, false)
	m := &recoveringMonthFixture{f: f, scope: scope, plan: plan, second: second, ops: f.ops}
	base := f.ops.capture
	m.ops.capture = func(ctx context.Context, source *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
		m.requests = append(m.requests, r)
		if r == f.root {
			m.parentCalls++
			failAt := 1
			if second {
				failAt = 2
			}
			if m.parentCalls >= failAt {
				f.fail = func(UsageWindowRequest) error {
					return &mysql.MySQLError{Number: 3024, Message: "synthetic daily deadline"}
				}
			} else {
				f.fail = nil
			}
		} else {
			f.fail = nil
		}
		if r.From >= f.root.From && r.To <= f.root.To {
			return base(ctx, source, r)
		}
		f.driver.queryErr = nil
		tokenCaptureRowsForDay(f.driver, r.From, true)
		return captureTokenUsageWindow(ctx, source, r, func() time.Time { at := f.now; f.now = f.now.Add(time.Millisecond); return at })
	}
	return m
}

func (m *recoveringMonthFixture) advance(t *testing.T) (TokenMonthCaptureProgress, error) {
	t.Helper()
	return captureNextRecoveringTokenMonthSlot(t.Context(), m.f.source, m.f.store, m.f.job, m.scope, m.ops)
}

func finishRecoveringMonth(t *testing.T, m *recoveringMonthFixture) TokenMonthCaptureProgress {
	t.Helper()
	for i := 0; i < 180; i++ {
		p, err := m.advance(t)
		if err != nil {
			t.Fatal("month advance", i, p, err)
		}
		if p.TraversalsFinished {
			return p
		}
		if p.NextEligibleMS > m.f.now.UnixMilli() {
			m.f.now = time.UnixMilli(p.NextEligibleMS)
		}
	}
	t.Fatal("month exceeded bounded work steps")
	return TokenMonthCaptureProgress{}
}

func TestRecoveringMonthSchedulerHandoffRestartAndOriginalMoney(t *testing.T) {
	for _, second := range []bool{false, true} {
		t.Run(map[bool]string{false: "first_sweep", true: "second_sweep"}[second], func(t *testing.T) {
			m := newRecoveringMonthFixture(t, second)
			for i := 0; i < 70; i++ {
				p, err := m.advance(t)
				if err != nil {
					t.Fatal(p, err)
				}
				if adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_token_recovery_bindings`) == 1 {
					break
				}
				if p.NextEligibleMS > m.f.now.UnixMilli() {
					m.f.now = time.UnixMilli(p.NextEligibleMS)
				}
			}
			if n := adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_token_recovery_bindings`); n != 1 {
				t.Fatal("no durable failed-window handoff", n)
			}
			if n := adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_adaptive_token_observations`); n != 0 {
				t.Fatal("failed parent fabricated a child", n)
			}
			queries := m.f.queries()
			if _, err := CaptureNextTokenMonthSlot(t.Context(), m.f.source, m.f.store, m.f.job, m.scope); !errors.Is(err, ErrTokenMonthCaptureIntegrity) || m.f.queries() != queries {
				t.Fatal("legacy collector resumed bound failed parent", err)
			}
			if err := m.f.store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", m.f.path+"?_pragma=busy_timeout(500)")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			m.f.store = db
			p := finishRecoveringMonth(t, m)
			if p.Completed != 60 || p.Total != 60 || !p.TraversalsFinished {
				t.Fatal(p)
			}
			wantParents := 1
			if second {
				wantParents = 2
			}
			if m.parentCalls != wantParents {
				t.Fatal("requeried failed parent", m.parentCalls, wantParents)
			}
			pairs, report, err := LoadRecoveredTokenMonth(t.Context(), db, m.f.job, m.scope, 201)
			if err != nil || len(pairs) != 31 || report.ConsumeQuota != 11604 || report.RefundQuota != 0 {
				t.Fatal("lost or duplicated money", report, err)
			}
			if len(m.requests) != 62+wantParents {
				t.Fatal("reread unrelated days", len(m.requests))
			}
			// Every selected child has a real independent second transaction.
			for _, pair := range pairs {
				if !TokenUsageWindowsMatch(pair.Older, pair.Newer) {
					t.Fatal("unmatched source originals")
				}
			}
			assembled, err := BuildRecoveredCapturedMonthSelection(t.Context(), db, m.f.job, m.scope)
			if err != nil {
				t.Fatal(err)
			}
			out, _ := emptyCapturedCopy(t)
			copied, err := CopyRecoveredCapturedMonthObservationsBounded(t.Context(), db, out, m.f.job, m.scope, 64<<20)
			if err != nil || !reflect.DeepEqual(copied, assembled) {
				t.Fatal("scheduler did not reach sealed handoff", err)
			}
			queries = m.f.queries()
			p, err = m.advance(t)
			if err != nil || !p.TraversalsFinished || m.f.queries() != queries {
				t.Fatal("completed month was recollected", p, err)
			}
		})
	}
}

func TestRecoveringMonthWaitDoesNotQueryWriteOrHoldConnection(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	var p TokenMonthCaptureProgress
	for i := 0; i < 80; i++ {
		var err error
		p, err = m.advance(t)
		if err != nil {
			t.Fatal(err)
		}
		if p.NextEligibleMS > m.f.now.UnixMilli() {
			break
		}
	}
	if p.NextEligibleMS <= m.f.now.UnixMilli() {
		t.Fatal("no independent wait", p)
	}
	queries := m.f.queries()
	changes := adaptiveTokenCount(t, m.f.store, `SELECT total_changes()`)
	for i := 0; i < 3; i++ {
		got, err := m.advance(t)
		if err != nil || got.Captured || got.TraversalsFinished || got.NextEligibleMS != p.NextEligibleMS {
			t.Fatal(got, err)
		}
	}
	if m.f.queries() != queries || adaptiveTokenCount(t, m.f.store, `SELECT total_changes()`) != changes || m.f.source.Stats().InUse != 0 || m.f.store.Stats().InUse != 0 {
		t.Fatal("wait consumed source/write/connection")
	}
}

func TestRecoveringMonthChildFailuresStopDurablyWithoutParentRetry(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	} // parent deadline -> binding
	base := m.ops.capture
	m.ops.capture = func(ctx context.Context, db *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
		if r.From >= m.f.root.From && r.To <= m.f.root.To {
			m.f.driver.queryErr = errors.New("synthetic authorization failure")
			m.f.driver.usage.data = nil
			return captureTokenUsageWindow(ctx, db, r, func() time.Time { return m.f.now })
		}
		return base(ctx, db, r)
	}
	for failure := 0; failure < 3; failure++ {
		if _, err := m.advance(t); err == nil {
			t.Fatal("source failure accepted")
		}
		m.f.now = m.f.now.Add(31 * time.Second)
	}
	queries := m.f.queries()
	if _, err := m.advance(t); !errors.Is(err, ErrAdaptiveTokenStopped) || m.f.queries() != queries {
		t.Fatal("failure budget did not stop", err)
	}
	if m.parentCalls != 1 || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_adaptive_token_splits`) != 1 {
		t.Fatal("auth failure caused more splits or parent reread")
	}
}

func fillRecoveringMonthBudget(t *testing.T, m *recoveringMonthFixture) {
	t.Helper()
	for i := 0; i < 32; i++ {
		if _, err := m.f.store.Exec(`INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload) VALUES(?,?,201,?,?,1,2,zeroblob(2097152))`, digest([]byte("unrelated retained capacity fixture")), i, digest([]byte("old raw")), digest([]byte("old projection"))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoveringMonthJointCapacityBeforeAndAfterSourceIsAtomic(t *testing.T) {
	for _, child := range []bool{false, true} {
		for _, after := range []bool{false, true} {
			name := map[bool]string{false: "daily", true: "child"}[child] + map[bool]string{false: "_before", true: "_after"}[after]
			t.Run(name, func(t *testing.T) {
				m := newRecoveringMonthFixture(t, !child)
				if _, err := m.advance(t); err != nil {
					t.Fatal(err)
				}
				old := adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM portal_monthly_token_capture_slots WHERE job_key='`+m.f.job+`'`)
				projected := adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM portal_monthly_usage_observations`)
				if after {
					base := m.ops.capture
					m.ops.capture = func(ctx context.Context, db *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
						o, err := base(ctx, db, r)
						if err == nil {
							fillRecoveringMonthBudget(t, m)
						}
						return o, err
					}
				} else {
					fillRecoveringMonthBudget(t, m)
				}
				queries := m.f.queries()
				_, err := m.advance(t)
				if !errors.Is(err, ErrTokenMonthCaptureBudget) && !errors.Is(err, ErrAdaptiveTokenBudget) {
					t.Fatal("shared budget bypassed", err)
				}
				if !after && m.f.queries() != queries {
					t.Fatal("full store still queried source")
				}
				if adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM portal_monthly_token_capture_slots WHERE job_key='`+m.f.job+`'`) != old || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM portal_monthly_usage_observations`) != projected || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_adaptive_token_observations`) != 0 {
					t.Fatal("partial raw/projection retained after budget failure")
				}
			})
		}
	}
}

func TestRecoveringMonthChangedDailyOriginalIsRetainedAndStops(t *testing.T) {
	m := newRecoveringMonthFixture(t, true)
	for i := 0; i < 30; i++ {
		if _, err := m.advance(t); err != nil {
			t.Fatal(err)
		}
	}
	p, err := m.advance(t)
	if err != nil || p.NextEligibleMS <= m.f.now.UnixMilli() {
		t.Fatal("missing first sweep wait", p, err)
	}
	m.f.now = time.UnixMilli(p.NextEligibleMS)
	m.ops.capture = func(ctx context.Context, db *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
		m.f.driver.queryErr = nil
		tokenCaptureRowsForDay(m.f.driver, r.From, false)
		return captureTokenUsageWindow(ctx, db, r, func() time.Time { at := m.f.now; m.f.now = m.f.now.Add(time.Millisecond); return at })
	}
	if _, err := m.advance(t); !errors.Is(err, ErrTokenMonthCaptureIntegrity) {
		t.Fatal("changed daily totals accepted", err)
	}
	if _, err := loadTokenCaptureSlot(t.Context(), m.f.store, m.plan, 30); err != nil {
		t.Fatal("changed second original discarded", err)
	}
	queries := m.f.queries()
	if _, err := m.advance(t); !errors.Is(err, ErrTokenMonthCaptureIntegrity) || m.f.queries() != queries {
		t.Fatal("changed data retried/split", err)
	}
	if adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_token_recovery_bindings`) != 0 {
		t.Fatal("money drift was treated as deadline")
	}
}

func TestRecoveringMonthConcurrentOwnerCancellationAndNoSourceFault(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	result := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m.ops.capture = func(ctx context.Context, _ *sql.DB, _ UsageWindowRequest) (TokenUsageWindowObservation, error) {
		close(entered)
		<-ctx.Done()
		return TokenUsageWindowObservation{}, ctx.Err()
	}
	go func() {
		_, err := captureNextRecoveringTokenMonthSlot(ctx, m.f.source, m.f.store, m.f.job, m.scope, m.ops)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("source never entered")
	}
	if _, err := m.advance(t); !errors.Is(err, ErrWalletCaptureBusy) {
		t.Fatal("concurrent monthly capture admitted", err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not drain")
	}
	if len(recoveringTokenMonthSlots) != 0 || len(adaptiveTokenAdvanceSlots) != 0 || m.f.store.Stats().InUse != 0 || m.f.source.Stats().InUse != 0 || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_adaptive_token_faults`) != 0 {
		t.Fatal("cancel leaked slots/connection or consumed a source retry")
	}
}

func TestRecoveringMonthMissingSchemaCanceledAndAliasNeverQuery(t *testing.T) {
	for _, scenario := range []string{"schema", "cancel", "alias", "orphan_root", "tampered_binding"} {
		t.Run(scenario, func(t *testing.T) {
			m := newRecoveringMonthFixture(t, false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := m.f.source
			switch scenario {
			case "schema":
				if _, err := m.f.store.Exec(`DROP TABLE monthly_token_recovery_bindings`); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			case "alias":
				source = m.f.store
			case "orphan_root":
				if err := ensureAdaptiveTokenRoot(ctx, m.f.store, m.f.job, m.f.root, m.f.now); err != nil {
					t.Fatal(err)
				}
			case "tampered_binding":
				if _, err := m.advance(t); err != nil {
					t.Fatal(err)
				}
				if _, err := m.f.store.Exec(`DROP TRIGGER token_recovery_binding_no_update; UPDATE monthly_token_recovery_bindings SET ordinal=2`); err != nil {
					t.Fatal(err)
				}
			}
			queries := m.f.queries()
			if _, err := captureNextRecoveringTokenMonthSlot(ctx, source, m.f.store, m.f.job, m.scope, m.ops); err == nil || m.f.queries() != queries {
				t.Fatal("invalid scope/schema touched source", err)
			}
		})
	}
}

func TestRecoveringMonthBindingsAreImmutableAndHandoffAtomic(t *testing.T) {
	for _, failure := range []string{"none", "binding", "split"} {
		t.Run(failure, func(t *testing.T) {
			m := newRecoveringMonthFixture(t, false)
			if failure != "none" {
				table := "monthly_token_recovery_bindings"
				if failure == "split" {
					table = "monthly_adaptive_token_splits"
				}
				if _, err := m.f.store.Exec(`CREATE TRIGGER fail_handoff BEFORE INSERT ON ` + table + ` BEGIN SELECT RAISE(ABORT,'synthetic handoff failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			_, err := m.advance(t)
			if failure != "none" {
				if err == nil || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_adaptive_token_roots`) != 0 || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_token_recovery_bindings`) != 0 {
					t.Fatal("partial handoff retained", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, q := range []string{`UPDATE monthly_token_recovery_bindings SET created_at_ms=created_at_ms`, `DELETE FROM monthly_token_recovery_bindings`} {
				if _, err := m.f.store.Exec(q); err == nil {
					t.Fatal("binding mutable")
				}
			}
		})
	}
}
