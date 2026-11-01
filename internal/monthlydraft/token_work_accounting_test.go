package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func workFixture(t *testing.T) (generationFixture, *LocalRuntime, *TokenSourcePreparation, *time.Time) {
	t.Helper()
	f, _, r, p, _ := tokenPreparationFixture(t)
	r.preparation.accountTokenWork = true
	p.funding.accountTokenWork = true
	driver := p.funding.source.Driver().(*tokenPreparationDriver).base
	driver.onQuery = func() error {
		if f.receiver.Stats().InUse != 0 || p.funding.capture.Stats().InUse != 0 {
			return errors.New("local transaction held during source SQL")
		}
		return nil
	}
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	now := time.Now().Add(-10 * time.Minute)
	r.preparation.now = func() time.Time { return now }
	if _, err := r.preparation.SubmitMonthly(t.Context(), 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	return f, r, p, &now
}

func workCounts(t *testing.T, f generationFixture) (int, int) {
	t.Helper()
	var observed, credited int
	if err := f.receiver.QueryRow(`SELECT observed,credited FROM portal_monthly_token_work_counts LIMIT 1`).Scan(&observed, &credited); err != nil {
		t.Fatal(err)
	}
	return observed, credited
}

func TestTokenWorkSuccessfulOriginalsRetainActualWorkAndDoNotIssueBills(t *testing.T) {
	f, r, p, now := workFixture(t)
	var created int64
	if err := f.receiver.QueryRow(`SELECT created_at FROM portal_monthly_source_preparation`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err := r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(1100 * time.Millisecond)
		state, steps, failures := preparationState(t, f.receiver)
		observed, credited := workCounts(t, f)
		if state != "queued" || steps != 0 || failures != 0 || observed != i || credited != i {
			t.Fatal("work lost or charged twice", state, steps, observed, credited)
		}
	}
	var grants int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&grants); err != nil || grants != 1 {
		t.Fatal(grants, err)
	}
	var after int64
	if err := f.receiver.QueryRow(`SELECT created_at FROM portal_monthly_source_preparation`).Scan(&after); err != nil || after != created {
		t.Fatal("age reset", err)
	}
	if _, err := p.Advance(t.Context(), SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("direct call borrowed worker accounting", err)
	}
	var selected int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_generation_selections`).Scan(&selected); err != nil || selected != 0 {
		t.Fatal("work receipts approved a bill", selected, err)
	}
}

func TestTokenWorkFailureAndFinalWriteRollbackStayConservative(t *testing.T) {
	f, r, _, now := workFixture(t)
	if _, err := f.receiver.Exec(`CREATE TRIGGER synthetic_work_failure BEFORE INSERT ON portal_monthly_token_work_counts BEGIN SELECT RAISE(ABORT,'synthetic count write failed'); END`); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(t.Context()); err == nil {
		t.Fatal("expected receiver failure")
	}
	state, steps, failures := preparationState(t, f.receiver)
	var grants int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if state != "running" || steps != 1 || failures != 0 || grants != 0 {
		t.Fatal("partial work grant committed", state, steps, failures, grants)
	}
	if _, err := f.receiver.Exec(`DROP TRIGGER synthetic_work_failure`); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(61 * time.Second)
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, steps, failures = preparationState(t, f.receiver)
	observed, credited := workCounts(t, f)
	if state != "queued" || steps != 1 || failures != 1 || observed != 2 || credited != 1 {
		t.Fatal("lost commit refunded/forgot old work", state, steps, failures, observed, credited)
	}
}

func TestTokenWorkLedgerModeLossAndPartialRestoreFailBeforeSource(t *testing.T) {
	for _, kind := range []string{"disabled", "missing_count", "orphan_count", "decrease", "orphan_task"} {
		t.Run(kind, func(t *testing.T) {
			f, r, p, now := workFixture(t)
			if err := r.preparation.runOne(t.Context()); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(1100 * time.Millisecond)
			var before int
			if err := p.funding.capture.QueryRow(`SELECT count(*) FROM portal_monthly_token_capture_slots`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "disabled":
				r.preparation.accountTokenWork = false
			case "missing_count":
				if _, err := f.receiver.Exec(`DROP TRIGGER token_work_count_no_delete; DELETE FROM portal_monthly_token_work_counts`); err != nil {
					t.Fatal(err)
				}
			case "orphan_count":
				if _, err := f.receiver.Exec(`DROP TRIGGER token_work_grant_no_delete; DELETE FROM portal_monthly_token_work_grants`); err != nil {
					t.Fatal(err)
				}
			case "decrease":
				if _, err := f.receiver.Exec(`UPDATE portal_monthly_token_work_counts SET credited=0`); err == nil {
					t.Fatal("counter reset allowed")
				}
				return
			case "orphan_task":
				if _, err := f.receiver.Exec(`DELETE FROM portal_monthly_source_preparation`); err != nil {
					t.Fatal(err)
				}
				if _, err := r.preparation.SubmitMonthly(t.Context(), 1, 1, f.plan.Funding.Month); err == nil {
					t.Fatal("recreated work-accounted task")
				}
				return
			}
			if err := r.preparation.runOne(t.Context()); err != nil {
				t.Fatal(err)
			}
			state, _, _ := preparationState(t, f.receiver)
			var after int
			if err := p.funding.capture.QueryRow(`SELECT count(*) FROM portal_monthly_token_capture_slots`).Scan(&after); err != nil || state != "failed" || after != before {
				t.Fatal("unsafe mode/restore still queried source", state, after, err)
			}
		})
	}
}

func TestTokenWorkEnabledCapturedQueuePublishesOriginalAmounts(t *testing.T) {
	x := newBoundResumeFixture(t)
	closeDeploymentTest(t, x.d)
	x.d = openBoundFixtureWithWork(t, x.f, true)
	x.d.runtime.preparation.now = func() time.Time { return x.now }
	p := previewBound(t, x)
	x.now = time.UnixMilli(p.EarliestMS)
	if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err != nil || !r.Applied {
		t.Fatal(r, err)
	}
	if err := x.d.runtime.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	observedBefore, creditedBefore := workCounts(t, x.f.f)
	if observedBefore != 121 || creditedBefore != 1 {
		t.Fatal("first original not checkpointed", observedBefore, creditedBefore)
	}
	var originalGrant []byte
	if err := x.f.f.receiver.QueryRow(`SELECT payload FROM portal_monthly_token_work_grants`).Scan(&originalGrant); err != nil {
		t.Fatal(err)
	}
	closeDeploymentTest(t, x.d)
	x.d = openBoundFixtureWithWork(t, x.f, true)
	x.d.runtime.preparation.now = time.Now
	request, own, foreign := capturedHistoryHTTP(t, x.f, x.d)
	if err := x.d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(25 * time.Second)
	var record monitor.PortalMonthlyBill
	for {
		err := x.f.auth.First(&record, "group_id=1 AND month=?", p.Queue.Request.Month).Error
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			state, steps, failures := preparationState(t, x.f.f.receiver)
			t.Fatal("accounted queue did not publish", state, steps, failures, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, x.d)
	assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	observed, credited := workCounts(t, x.f.f)
	if observed != 122 || credited != 2 {
		t.Fatal("root/old originals not accounted", observed, credited)
	}
	var restoredGrant []byte
	if err := x.f.f.receiver.QueryRow(`SELECT payload FROM portal_monthly_token_work_grants`).Scan(&restoredGrant); err != nil || string(restoredGrant) != string(originalGrant) {
		t.Fatal("restart rewrote work origin/baseline", err)
	}
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("invalid bill")
	}
	for _, u := range evidence.Users {
		if u.OpeningQuota != 500000000 || u.ClosingQuota != 499000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("work accounting changed money", u.UserID)
		}
	}
}

func TestTokenWorkLargeRosterCrossesSegmentWithoutResettingCoordinator(t *testing.T) {
	f, auth, r, p, driver := tokenPreparationFixture(t)
	for id := int64(203); id <= 400; id++ {
		if err := auth.Create(&monitor.TrackedUser{UserID: id, GroupID: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	owner, ids, err := LoadUsageGenerationOwner(t.Context(), p.funding.authorization, 1, 1)
	if err != nil || len(ids) != 200 {
		t.Fatal("large fixture roster", len(ids), err)
	}
	req := SourcePreparationRequest{Owner: owner, Month: f.plan.Funding.Month}
	r.preparation.admit = func(_ context.Context, got SourcePreparationRequest) error {
		if got != req {
			return ErrGenerationChanged
		}
		return nil
	}
	r.preparation.accountTokenWork = true
	p.funding.accountTokenWork = true
	scope, err := p.funding.scope(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), p.funding.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	// SETUP ONLY: a valid retained zero-usage prefix, not 4095 measured source
	// queries. The THREE new observations below use the real bounded collector.
	tx, err := p.funding.capture.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	base := time.Now().Add(-2 * time.Hour).UnixMilli()
	for i := 0; i < 4095; i++ {
		scan := plan.Scans[i/len(ids)]
		zero := int64(0)
		o := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary", Request: monthlysource.UsageWindowRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: ids[i%len(ids)], From: scan.From, To: scan.To}, StartedAtMS: base + int64(i), CompletedAtMS: base + int64(i) + 1, Wallet: monthlysource.WalletUser{UserID: ids[i%len(ids)], Exists: true, BalanceQuota: &zero, UsedQuota: &zero}, Totals: []monthlysource.UsageWindowRow{}, Rows: []monthlysource.TokenUsageWindowRow{}}
		u, err := monthlysource.UsageObservationFromToken(o)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		projection, err := json.Marshal(u)
		if err != nil {
			t.Fatal(err)
		}
		uid := generationHash(projection)
		if _, err := tx.Exec(`INSERT INTO portal_monthly_usage_observations(id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?)`, uid, u.Request.SourceIdentity, u.Request.UserID, u.Request.From, u.Request.To, u.StartedAtMS, u.CompletedAtMS, projection); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO portal_monthly_token_capture_slots VALUES(?,?,?,?,?,?,?,?)`, req.Key(), i, o.Request.UserID, generationHash(raw), uid, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	step, err := p.Before(func(context.Context, SourcePreparationRequest) (bool, error) { return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	r.preparation.step = step
	now := time.Now().Add(-10 * time.Minute)
	r.preparation.now = func() time.Time { return now }
	if _, err := r.preparation.SubmitMonthly(t.Context(), 1, 1, req.Month); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=4095`); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err := r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(1100 * time.Millisecond)
		state, steps, failures := preparationState(t, f.receiver)
		observed, credited := workCounts(t, f)
		if state != "queued" || steps != 4095 || failures != 0 || observed != 4095+i || credited != i {
			t.Fatal("segment reset/lost total or old budget blocked progress", state, steps, failures, observed, credited)
		}
	}
	if driver.queries.Load() != 12 {
		t.Fatal("collector query count/scope changed", driver.queries.Load())
	}
	var body []byte
	if err := f.receiver.QueryRow(`SELECT payload FROM portal_monthly_token_work_grants`).Scan(&body); err != nil {
		t.Fatal(err)
	}
	var grant tokenWorkGrant
	if json.Unmarshal(body, &grant) != nil || grant.Baseline != 4095 || grant.Schedule.TotalSlots != 12000 || grant.Schedule.Segments[0].ToOrdinal != 4096 || grant.Parent.Owner != owner || grant.Source.Owner != owner {
		t.Fatal("wrong persisted plan or organization")
	}
	var selections int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_generation_selections`).Scan(&selections); err != nil || selections != 0 {
		t.Fatal("partial large roster became a bill", selections, err)
	}
}

func TestTokenWorkSourceFailuresStillStopAtThree(t *testing.T) {
	f, r, p, now := workFixture(t)
	d := p.funding.source.Driver().(*tokenPreparationDriver).base
	d.onQuery = func() error { return errors.New("synthetic source outage") }
	for range 3 {
		if err := r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(31 * time.Second)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "failed" || steps != 3 || failures != 3 {
		t.Fatal("work ledger bypassed failure budget", state, steps, failures)
	}
	var n int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&n); err != nil || n != 0 {
		t.Fatal("failed query issued accounting credits", n, err)
	}
	queries := d.queries.Load()
	for range 2 {
		if _, err := r.preparation.SubmitMonthly(t.Context(), 1, 1, f.plan.Funding.Month); err != nil {
			t.Fatal(err)
		}
		if err := r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if d.queries.Load() != queries {
		t.Fatal("failed task retried without resumption authority")
	}
}

func TestTokenWorkOlderSourceRestoreStopsBeforeSourceQuery(t *testing.T) {
	f, r, p, now := workFixture(t)
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(1100 * time.Millisecond)
	d := p.funding.source.Driver().(*tokenPreparationDriver).base
	queries := d.queries.Load()
	// SETUP ONLY: independently restored older capture DB, receiver is newer.
	if _, err := p.funding.capture.Exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots; CREATE TRIGGER monthly_token_capture_no_delete BEFORE DELETE ON portal_monthly_token_capture_slots BEGIN SELECT RAISE(ABORT,'token captures are immutable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, _, failures := preparationState(t, f.receiver)
	if state != "failed" || failures != 1 || d.queries.Load() != queries {
		t.Fatal("lost source checkpoint was repaired by new reads", state, failures)
	}
	observed, credited := workCounts(t, f)
	if observed != 1 || credited != 1 {
		t.Fatal("original accounting overwritten")
	}
}

func TestTokenWorkPostCapturePolicyChangeRetainsNormalFailureState(t *testing.T) {
	f, r, _, _ := workFixture(t)
	originalStep, originalAdmit := r.preparation.step, r.preparation.admit
	revoked := false
	r.preparation.admit = func(ctx context.Context, req SourcePreparationRequest) error {
		if revoked {
			return ErrGenerationChanged
		}
		return originalAdmit(ctx, req)
	}
	r.preparation.step = func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		ready, err := originalStep(ctx, req)
		revoked = true
		return ready, err
	}
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	var grants int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || steps != 1 || failures != 1 || grants != 0 {
		t.Fatal("post-capture authority change earned credit or lost original error", state, steps, failures, grants)
	}
}

func TestTokenWorkLostLeaseCannotCommitCounters(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "first_grant", true: "existing_grant"}[existing], func(t *testing.T) {
			f, r, _, now := workFixture(t)
			if existing {
				if err := r.preparation.runOne(t.Context()); err != nil {
					t.Fatal(err)
				}
				*now = now.Add(1100 * time.Millisecond)
			}
			original := r.preparation.step
			r.preparation.step = func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
				ready, err := original(ctx, req)
				if err == nil {
					_, err = f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET lease='abababababababababababababababab' WHERE state='running'`)
				}
				return ready, err
			}
			if err := r.preparation.runOne(t.Context()); err == nil {
				t.Fatal("stale worker finalized token work")
			}
			state, steps, failures := preparationState(t, f.receiver)
			if state != "running" || steps != 1 || failures != 0 {
				t.Fatal("stale worker changed current lease", state, steps, failures)
			}
			if existing {
				observed, credited := workCounts(t, f)
				if observed != 1 || credited != 1 {
					t.Fatal("stale worker advanced durable counters", observed, credited)
				}
			} else {
				var grants int
				if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&grants); err != nil || grants != 0 {
					t.Fatal("stale worker committed first grant", grants, err)
				}
			}
		})
	}
}
