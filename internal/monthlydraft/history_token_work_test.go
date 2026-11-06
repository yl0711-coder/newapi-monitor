package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

type historyWorkFixture struct {
	f      capturedHistoryFixture
	r      *LocalRuntime
	p      *automaticHistoryPreparation
	parent SourcePreparationRequest
	source SourcePreparationRequest
	state  *sourcePreparationDriver
	calls  int
}

func newHistoryWorkFixture(t *testing.T) *historyWorkFixture {
	t.Helper()
	f, _, _ := automaticHistoryOriginalFixture(t, "2026-07")
	parent := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-07"}
	source := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, parent, false)
	seedHistoryBoundaryForMonth(t, f, parent)
	seedHistoryEmptyMonth(t, f, source, true)
	state := f.wallet.funding.source.Driver().(*walletTailPreparationDriver).base
	db := sql.OpenDB(capturedPreparationTestConnector{&historyPreparationDriver{base: state}})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	f.wallet.funding.source, f.wallet.funding.accountTokenWork = db, true
	x := &historyWorkFixture{f: f, parent: parent, source: source, state: state}
	x.open(t)
	state.onQuery = func() error {
		if f.f.receiver.Stats().InUse != 0 || f.wallet.funding.capture.Stats().InUse != 0 {
			return errors.New("local transaction/connection held during source reads")
		}
		return nil
	}
	if _, err := x.r.preparation.SubmitMonthly(t.Context(), parent.Owner.GroupID, parent.Owner.AuthVersion, parent.Month); err != nil {
		t.Fatal(err)
	}
	return x
}

func (x *historyWorkFixture) open(t *testing.T) {
	t.Helper()
	r, err := NewControlledLocalRuntime(x.f.f.g, x.f.wallet.funding.authorization, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: x.parent.Owner.GroupID, Month: x.parent.Month}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.EnableOnDemandPreparation(t.Context(), x.f.wallet.funding.authorization, func(_ context.Context, req SourcePreparationRequest) error {
		if req != x.parent {
			return ErrGenerationChanged
		}
		return nil
	}, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil }); err != nil {
		t.Fatal(err)
	}
	r.preparation.accountTokenWork = true
	p, err := newAutomaticHistoryPreparation(x.f.wallet, x.f.spool)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.bindWorkAccounting(r); err != nil {
		t.Fatal(err)
	}
	r.preparation.step, err = p.Before(func(ctx context.Context, req SourcePreparationRequest, ref monthlysource.MonthlyObservationFile) (bool, error) {
		if req != x.parent || r.scope.allows(req.Owner.GroupID, x.source.Month) || r.permitsDependencyPreparation(ctx, req.Owner.GroupID, x.source.Month) {
			t.Fatal("source-only capability widened financial permission")
		}
		_, err := x.f.f.g.auditAutomaticHistorySources(ctx, ref, req)
		if err == nil {
			x.calls++
		}
		return false, err // no normalized input or customer publication yet
	})
	if err != nil {
		t.Fatal(err)
	}
	x.r, x.p = r, p
}

func (x *historyWorkFixture) tick(t *testing.T) {
	t.Helper()
	if _, err := x.f.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET next_at=0 WHERE key=? AND state='queued'`, x.parent.Key()); err != nil {
		t.Fatal(err)
	}
	if err := x.r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (x *historyWorkFixture) firstCredit(t *testing.T) {
	t.Helper()
	for range 6 {
		x.tick(t)
		var n int
		if err := x.f.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			return
		}
	}
	t.Fatal("later original capture never received a bounded work record")
}

func (x *historyWorkFixture) grant(t *testing.T) (tokenWorkGrant, []byte) {
	t.Helper()
	var raw []byte
	if err := x.f.f.receiver.QueryRow(`SELECT payload FROM portal_monthly_token_work_grants WHERE parent_key=? AND source_key=?`, x.parent.Key(), x.source.Key()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var g tokenWorkGrant
	if json.Unmarshal(raw, &g) != nil {
		t.Fatal("invalid retained work grant")
	}
	return g, raw
}

func TestHistoryTokenWorkLaterMonthCountsExactlyOnceAndReopens(t *testing.T) {
	x := newHistoryWorkFixture(t)
	x.firstCredit(t)
	g, original := x.grant(t)
	if g.Version != 2 || g.HistoryScope == nil || g.Source.Month <= g.Parent.Month || g.Baseline != 122 || g.Limit != 124 || !validTokenWorkGrantVersion(g, true) || validTokenWorkGrantVersion(g, false) {
		t.Fatal("later work lost original bound or became legacy permission", g)
	}
	_, steps, failures := preparationState(t, x.f.f.receiver)
	observed, credited := workCounts(t, x.f.f)
	if failures != 0 || observed != 123 || credited != 1 {
		t.Fatal("first work credit changed source/budget", steps, failures, observed, credited)
	}
	// Reconstruct the runtime/producer against retained stores, not an in-memory
	// receipt. No background thread is running in either instance.
	x.open(t)
	x.tick(t)
	_, afterSteps, failures := preparationState(t, x.f.f.receiver)
	observed, credited = workCounts(t, x.f.f)
	if failures != 0 || observed != 124 || credited != 2 || afterSteps != steps {
		t.Fatal("restart forgot/duplicated credit or reset coordinator budget", afterSteps, steps, failures, observed, credited)
	}
	for range 8 {
		if x.calls != 0 {
			break
		}
		x.tick(t)
	}
	_, retained := x.grant(t)
	if x.calls != 1 || !bytes.Equal(original, retained) || x.state.writes.Load() != 0 {
		t.Fatal("raw completion replaced grant or did not reach original audit", x.calls)
	}
	observed, credited = workCounts(t, x.f.f)
	if observed != 124 || credited != 2 {
		t.Fatal("reusing completed originals minted more work credit")
	}
	var created, grants, jobs int64
	if err := x.f.f.receiver.QueryRow(`SELECT created_at FROM portal_monthly_source_preparation WHERE key=?`, x.parent.Key()).Scan(&created); err != nil || created != g.ParentCreatedMS {
		t.Fatal("parent lifetime reset", err)
	}
	if err := x.f.f.receiver.QueryRow(`SELECT (SELECT count(*) FROM portal_monthly_token_work_grants),(SELECT count(*) FROM portal_monthly_source_preparation)`).Scan(&grants, &jobs); err != nil || grants != 1 || jobs != 1 {
		t.Fatal("data dependency created more jobs/credits", grants, jobs, err)
	}
	generationAdmissionCounts(t, x.f.f.receiver, 0, 0)
}

func TestHistoryTokenWorkCapabilityCannotBeBorrowedByDirectCaller(t *testing.T) {
	x := newHistoryWorkFixture(t)
	scope, err := x.p.raw.funding.scope(t.Context(), x.source)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := monthlysource.ReadTokenWorkCheckpoint(t.Context(), x.p.raw.funding.capture, x.source.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	d := &tokenWorkDispatch{p: x.r.preparation, job: sourcePreparationJob{request: x.parent, key: x.parent.Key()}}
	ctx := context.WithValue(t.Context(), tokenDispatchKey{}, d)
	queries := x.state.queries.Load()
	if err := checkTokenCheckpoint(ctx, x.source, cp); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("dispatch alone authorized later source work", err)
	}
	token, err := NewTokenSourcePreparation(x.p.raw.funding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := token.Advance(ctx, x.source); !errors.Is(err, ErrGenerationIntegrity) || x.state.queries.Load() != queries {
		t.Fatal("unbound later capture reached source", err)
	}
	step, err := x.p.Before(func(context.Context, SourcePreparationRequest, monthlysource.MonthlyObservationFile) (bool, error) {
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := step(t.Context(), x.parent); ready || !errors.Is(err, ErrGenerationInput) || x.state.queries.Load() != queries {
		t.Fatal("direct coordinator bypassed the real queue", ready, err)
	}
	if x.r.permitsDependencyPreparation(ctx, x.parent.Owner.GroupID, x.source.Month) || x.r.scope.allows(x.parent.Owner.GroupID, x.source.Month) {
		t.Fatal("source context widened actual bill eligibility")
	}
	months, err := x.p.months(ctx, x.parent)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := x.p.workContext(ctx, x.parent, months)
	if err != nil {
		t.Fatal(err)
	}
	capability := bound.Value(historyTokenWorkKey{}).(historyTokenWorkCapability)
	if _, err := json.Marshal(capability); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("private capability became editable JSON", err)
	}
	if err := json.Unmarshal([]byte(`{}`), &capability); !errors.Is(err, ErrGenerationInput) || capability.producer != nil {
		t.Fatal("JSON input retained a process-local capability", err)
	}
}

func TestHistoryTokenWorkPartialRestoresAndModeLossStopBeforeSource(t *testing.T) {
	for _, kind := range []string{"producer_disabled", "accounting_disabled", "missing_count", "older_token_store", "missing_registered_slot", "changed_source", "changed_scope", "changed_limit", "changed_schedule", "changed_registration"} {
		t.Run(kind, func(t *testing.T) {
			x := newHistoryWorkFixture(t)
			x.firstCredit(t)
			queries := x.state.queries.Load()
			switch kind {
			case "producer_disabled":
				x.r.preparation.historySourceWork = nil
			case "accounting_disabled":
				x.r.preparation.accountTokenWork = false
			case "missing_count":
				if _, err := x.f.f.receiver.Exec(`DROP TRIGGER token_work_count_no_delete; DELETE FROM portal_monthly_token_work_counts`); err != nil {
					t.Fatal(err)
				}
				if _, err := x.f.f.receiver.Exec(`CREATE TRIGGER token_work_count_no_delete BEFORE DELETE ON portal_monthly_token_work_counts BEGIN SELECT RAISE(ABORT,'token work counters retained'); END`); err != nil {
					t.Fatal(err)
				}
			case "older_token_store":
				if _, err := x.f.wallet.funding.capture.Exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots WHERE job_key=? AND ordinal=122`, x.source.Key()); err != nil {
					t.Fatal(err)
				}
				if _, err := x.f.wallet.funding.capture.Exec(`CREATE TRIGGER monthly_token_capture_no_delete BEFORE DELETE ON portal_monthly_token_capture_slots BEGIN SELECT RAISE(ABORT,'token captures are immutable'); END`); err != nil {
					t.Fatal(err)
				}
			case "missing_registered_slot":
				if _, err := x.f.wallet.funding.capture.Exec(`DROP TRIGGER monthly_registered_capture_no_delete; DELETE FROM portal_monthly_registered_wallet_capture_slots WHERE job_key=?`, x.parent.Key()); err != nil {
					t.Fatal(err)
				}
				if _, err := x.f.wallet.funding.capture.Exec(`CREATE TRIGGER monthly_registered_capture_no_delete BEFORE DELETE ON portal_monthly_registered_wallet_capture_slots BEGIN SELECT RAISE(ABORT,'registered wallet checkpoints are immutable'); END`); err != nil {
					t.Fatal(err)
				}
			case "changed_source":
				x.f.wallet.funding.sourceIdentity = strings.Repeat("f", 64)
			case "changed_scope", "changed_limit", "changed_schedule", "changed_registration":
				g, _ := x.grant(t)
				switch kind {
				case "changed_scope":
					g.HistoryScope.Through = "2099-01"
				case "changed_limit":
					g.Limit = 20336
				case "changed_schedule":
					g.Schedule.PlanSHA256 = strings.Repeat("f", 64)
				case "changed_registration":
					g.HistoryScope.RegisteredID = strings.Repeat("f", 64)
				}
				raw := materialPreparationJSON(t, g)
				if _, err := x.f.f.receiver.Exec(`DROP TRIGGER token_work_grant_no_update; UPDATE portal_monthly_token_work_grants SET payload=?,sha256=?`, raw, generationHash(raw)); err != nil {
					t.Fatal(err)
				}
				if _, err := x.f.f.receiver.Exec(`CREATE TRIGGER token_work_grant_no_update BEFORE UPDATE ON portal_monthly_token_work_grants BEGIN SELECT RAISE(ABORT,'token work grant immutable'); END`); err != nil {
					t.Fatal(err)
				}
			}
			x.tick(t)
			state, _, _ := preparationState(t, x.f.f.receiver)
			if state != "failed" || x.state.queries.Load() != queries {
				t.Fatal("damaged/missing mode still read source or renewed work", state, x.state.queries.Load(), queries)
			}
			generationAdmissionCounts(t, x.f.f.receiver, 0, 0)
		})
	}
}

func TestHistoryTokenWorkRollbackKeepsUnknownCommitConservative(t *testing.T) {
	x := newHistoryWorkFixture(t)
	// First tick seals July, second finishes the last August funds page. The
	// next step captures a real token original, then its receiver write fails.
	x.tick(t)
	x.tick(t)
	if _, err := x.f.f.receiver.Exec(`CREATE TRIGGER synthetic_history_work_failure BEFORE INSERT ON portal_monthly_token_work_counts BEGIN SELECT RAISE(ABORT,'synthetic work count failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := x.r.preparation.runOne(t.Context()); err == nil {
		// Respect the existing queue's delay without changing original times.
		if _, err := x.f.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET next_at=0 WHERE state='queued'`); err != nil {
			t.Fatal(err)
		}
		if err := x.r.preparation.runOne(t.Context()); err == nil {
			t.Fatal("receiver failure did not reach the atomic work update")
		}
	}
	var grants int
	if err := x.f.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&grants); err != nil || grants != 0 {
		t.Fatal("partial grant committed", grants, err)
	}
	if _, err := x.f.f.receiver.Exec(`DROP TRIGGER synthetic_history_work_failure`); err != nil {
		t.Fatal(err)
	}
	x.r.preparation.now = func() time.Time { return time.Now().Add(61 * time.Second) }
	x.tick(t)
	g, _ := x.grant(t)
	observed, credited := workCounts(t, x.f.f)
	_, _, failures := preparationState(t, x.f.f.receiver)
	if observed != 124 || credited != 1 || g.Baseline != 123 || failures != 1 {
		t.Fatal("lost commit erased work, duplicated credit or reset failure", observed, credited, g.Baseline, failures)
	}
	generationAdmissionCounts(t, x.f.f.receiver, 0, 0)
}

func TestHistoryTokenWorkLegacyBytesAndScopedModeRollback(t *testing.T) {
	x := newHistoryWorkFixture(t)
	x.firstCredit(t)
	g, original := x.grant(t)
	err := monthlytasks.WriteReceiver(t.Context(), x.f.f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return checkTokenWorkMode(ctx, tx, x.parent.Key(), true) // older/default policy refuses v2
	})
	if !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("legacy mode silently accepted later-month work", err)
	}
	_, unchanged := x.grant(t)
	if !bytes.Equal(original, unchanged) {
		t.Fatal("refusing old/default mode rewrote retained scope")
	}
	type legacyGrant struct {
		Version         int                                `json:"version"`
		Parent          SourcePreparationRequest           `json:"parent"`
		Source          SourcePreparationRequest           `json:"source"`
		ParentCreatedMS int64                              `json:"parent_created_ms"`
		Schedule        monthlysource.TokenCaptureSegments `json:"schedule"`
		Baseline        int                                `json:"baseline"`
		Limit           int                                `json:"limit"`
	}
	g.Version, g.HistoryScope, g.Parent = 1, nil, g.Source
	legacy := legacyGrant{g.Version, g.Parent, g.Source, g.ParentCreatedMS, g.Schedule, g.Baseline, g.Limit}
	if !bytes.Equal(materialPreparationJSON(t, g), materialPreparationJSON(t, legacy)) || !validTokenWorkGrantVersion(g, false) {
		t.Fatal("legacy v1 serialization changed")
	}
	x.tick(t) // restoring the correct policy can continue the unmodified work
	_, credited := workCounts(t, x.f.f)
	if credited != 2 {
		t.Fatal("read-only rollback inspection destroyed resumable progress", credited)
	}
}

func TestHistoryTokenWorkExistingBudgetsDoNotRenew(t *testing.T) {
	for _, kind := range []string{"coordinator", "age", "three_failures"} {
		t.Run(kind, func(t *testing.T) {
			x := newHistoryWorkFixture(t)
			x.firstCredit(t)
			_, original := x.grant(t)
			queries := x.state.queries.Load()
			switch kind {
			case "coordinator":
				if _, err := x.f.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=4096 WHERE key=?`, x.parent.Key()); err != nil {
					t.Fatal(err)
				}
				x.tick(t)
			case "age":
				// Advance the queue clock, not source observation timestamps.
				x.r.preparation.now = func() time.Time { return time.Now().Add(25 * time.Hour) }
				x.tick(t)
			case "three_failures":
				x.state.onQuery = func() error { return context.DeadlineExceeded }
				for range 3 {
					x.tick(t)
				}
			}
			state, _, failures := preparationState(t, x.f.f.receiver)
			if state != "failed" || kind == "three_failures" && failures != 3 {
				t.Fatal("old lifetime/error budget was renewed", state, failures)
			}
			if kind != "three_failures" && x.state.queries.Load() != queries {
				t.Fatal("exhausted budget still read source")
			}
			queries = x.state.queries.Load()
			x.tick(t)
			_, retained := x.grant(t)
			observed, credited := workCounts(t, x.f.f)
			if !bytes.Equal(original, retained) || observed != 123 || credited != 1 || x.state.queries.Load() != queries {
				t.Fatal("failed job restarted or changed original work")
			}
			generationAdmissionCounts(t, x.f.f.receiver, 0, 0)
		})
	}
}

func TestHistoryTokenWorkStrictRetainedShapeAndBinding(t *testing.T) {
	x := newHistoryWorkFixture(t)
	x.firstCredit(t)
	_, original := x.grant(t)
	for _, kind := range []string{"unknown_root", "unknown_scope", "scope_alias", "null_scope", "version_downgrade", "wrong_parent"} {
		t.Run(kind, func(t *testing.T) {
			raw := bytes.Clone(original)
			switch kind {
			case "unknown_root":
				raw = append(raw[:len(raw)-1], []byte(`,"approved":true}`)...)
			case "unknown_scope":
				raw = append(raw[:len(raw)-2], []byte(`,"complete":true}}`)...)
			case "scope_alias":
				raw = bytes.Replace(raw, []byte(`"history_scope":`), []byte(`"History_Scope":`), 1)
			case "null_scope":
				var originalGrant tokenWorkGrant
				if err := json.Unmarshal(original, &originalGrant); err != nil {
					t.Fatal(err)
				}
				raw = bytes.Replace(raw, materialPreparationJSON(t, originalGrant.HistoryScope), []byte("null"), 1)
			case "version_downgrade":
				raw = bytes.Replace(raw, []byte(`"version":2`), []byte(`"version":1`), 1)
			case "wrong_parent":
				raw = bytes.Replace(raw, []byte(`"month":"2026-07"`), []byte(`"month":"2026-06"`), 1)
			}
			if bytes.Equal(raw, original) {
				t.Fatal("shape fixture did not change intended field")
			}
			if _, err := x.f.f.receiver.Exec(`DROP TRIGGER token_work_grant_no_update;UPDATE portal_monthly_token_work_grants SET payload=?,sha256=?`, raw, generationHash(raw)); err != nil {
				t.Fatal(err)
			}
			if _, err := x.f.f.receiver.Exec(`CREATE TRIGGER token_work_grant_no_update BEFORE UPDATE ON portal_monthly_token_work_grants BEGIN SELECT RAISE(ABORT,'token work grant immutable'); END`); err != nil {
				t.Fatal(err)
			}
			err := monthlytasks.WriteReceiver(t.Context(), x.f.f.receiver, func(ctx context.Context, tx *sql.Tx) error {
				return checkTokenWorkModePolicy(ctx, tx, x.parent.Key(), true, x.p)
			})
			if !errors.Is(err, ErrGenerationIntegrity) {
				t.Fatal("editable/aliased retained scope accepted", err)
			}
		})
	}
}

func TestHistoryTokenWorkRangeAdvanceKeepsOriginalBudgetAndRejectsForgedRange(t *testing.T) {
	owner := monthlytasks.Owner{GroupID: 1, AuthVersion: 1, MemberFingerprint: strings.Repeat("a", 32), MemberManifest: strings.Repeat("b", 64)}
	parent := SourcePreparationRequest{Owner: owner, Month: "2026-07"}
	zone := time.FixedZone("UTC+8", 8*3600)
	septEnd := time.Date(2026, 10, 1, 0, 0, 0, 0, zone).Unix()
	old := tokenHistoryWorkScope{Version: 1, From: "2026-07", Through: "2026-08", Boundary: time.Date(2026, 9, 1, 0, 0, 0, 0, zone).Unix(), RegisteredID: strings.Repeat("a", 64), SourceIdentity: strings.Repeat("b", 64), ParentScopeSHA: strings.Repeat("c", 64)}
	next := old
	next.Through, next.Boundary = "2026-09", septEnd
	if !next.permitsRetained(old, parent, "2026-08") || old.permitsRetained(next, parent, "2026-08") {
		t.Fatal("rollover replaced/contracted the original source range")
	}
	for _, kind := range []string{"missing_registered", "wrong_identity", "wrong_parent", "future", "too_many_months", "wrong_boundary", "source_outside", "parent_outside"} {
		t.Run(kind, func(t *testing.T) {
			bad := next
			source := "2026-08"
			switch kind {
			case "missing_registered":
				bad.RegisteredID = ""
			case "wrong_identity":
				bad.SourceIdentity = strings.Repeat("d", 64)
			case "wrong_parent":
				bad.ParentScopeSHA = strings.Repeat("d", 64)
			case "future":
				bad.Through = "2099-01"
			case "too_many_months":
				bad.From = "2000-01"
			case "wrong_boundary":
				bad.Boundary++
			case "source_outside":
				source = "2026-10"
			case "parent_outside":
				bad.From = "2026-08"
			}
			if bad.permitsRetained(old, parent, source) {
				t.Fatal("unrelated or excessive work scope admitted")
			}
		})
	}
}
