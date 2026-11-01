package monthlydraft

// All databases, identities, checkpoints and outages here are synthetic and
// local. Exhaustion is injected as setup, not claimed as 4096 real SQL queries.
import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type continuationFixture struct {
	f     generationFixture
	r     *LocalRuntime
	token *TokenSourcePreparation
	d     *Deployment
	now   *time.Time
	plan  SourcePreparationContinuationPlan
}

func newContinuationFixture(t *testing.T) continuationFixture {
	t.Helper()
	return continuationAtLimit(t, false)
}

func continuationAtLimit(t *testing.T, age bool) continuationFixture {
	t.Helper()
	f, r, token, now := workFixture(t)
	if age {
		scope, err := token.funding.scope(t.Context(), SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := monthlysource.EnsureFundingMonthCapture(t.Context(), token.funding.capture, SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}.Key(), scope); err != nil {
			t.Fatal(err)
		}
		if _, err := f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET created_at=?,next_at=?`, now.Add(-25*time.Hour).UnixMilli(), now.Add(-25*time.Hour).UnixMilli()); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(2 * time.Second)
		if _, err := f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=4096`); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	wallet, err := NewWalletSourcePreparation(token.funding)
	if err != nil {
		t.Fatal(err)
	}
	spool, err := NewRetainedCapturedSourceSpool(wallet, privateCapturedOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	d := &Deployment{g: f.g, runtime: r, receiver: f.receiver, capturedPreparation: &capturedPreparationResources{funding: token.funding, capture: token.funding.capture, spool: spool}}
	if err := d.EnableSourcePreparationContinuations(); err != nil {
		t.Fatal(err)
	}
	plan, err := d.PreviewSourcePreparationContinuation(t.Context(), 1, 1, f.plan.Funding.Month)
	if err != nil {
		t.Fatal(err)
	}
	return continuationFixture{f, r, token, d, now, plan}
}

func TestContinuationOldAgeLimitUsesSamePlanWithoutRewritingCreation(t *testing.T) {
	x := continuationAtLimit(t, true)
	if x.plan.Observed != 0 {
		t.Fatal("unobserved plan not retained")
	}
	applyContinuation(t, x)
	if err := x.r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := readContinuation(t, x)
	state, steps, failures := preparationState(t, x.f.receiver)
	if state != "queued" || steps != 0 || failures != 0 || c.claims != 1 || c.exempt != 1 || c.grant.Original.CreatedMS != x.now.Add(-25*time.Hour).UnixMilli() {
		t.Fatal("age renewal erased origin", state, steps, failures, c)
	}
}

func TestContinuationConcurrentGrantAndCustomerClicksCannotMultiplyBudget(t *testing.T) {
	x := newContinuationFixture(t)
	var wg sync.WaitGroup
	results := make(chan string, 8)
	errorsFound := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			sha, err := x.d.ContinueSourcePreparation(t.Context(), x.plan)
			results <- sha
			errorsFound <- err
		})
	}
	wg.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	want := ""
	for sha := range results {
		if want == "" {
			want = sha
		}
		if sha != want {
			t.Fatal("duplicate grants")
		}
	}
	before := readContinuation(t, x)
	for range 3 {
		if _, err := x.r.preparation.SubmitMonthly(t.Context(), 1, 1, x.plan.Request.Month); err != nil {
			t.Fatal(err)
		}
	}
	if readContinuation(t, x) != before {
		t.Fatal("customer click renewed budget")
	}
}

func TestContinuationSpoolFailedOrCompleteOutputCannotBeRecaptured(t *testing.T) {
	for _, kind := range []string{"failed", "complete", "capacity"} {
		t.Run(kind, func(t *testing.T) {
			x := newContinuationFixture(t)
			s := x.d.capturedPreparation.spool
			if kind == "capacity" {
				s.limit = 4 << 20
			} else {
				job := filepath.Join(s.directory, s.jobName(x.plan.Source))
				if err := os.Mkdir(job, 0700); err != nil {
					t.Fatal(err)
				}
				count := 3
				if kind == "complete" {
					count = 1
				}
				for i := 1; i <= count; i++ {
					dir := filepath.Join(job, "seal-"+strconv.Itoa(i))
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
					if kind == "complete" {
						if err := os.WriteFile(filepath.Join(dir, "materials.json"), []byte(`{}`), 0600); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if _, err := x.d.PreviewSourcePreparationContinuation(t.Context(), 1, 1, x.plan.Request.Month); err == nil {
				t.Fatal("unsafe raw output permitted recapture")
			}
			if _, err := x.d.ContinueSourcePreparation(t.Context(), x.plan); err == nil {
				t.Fatal("stale raw capacity accepted")
			}
		})
	}
}

func TestContinuationUnsafeOldFailuresRemainStopped(t *testing.T) {
	for _, kind := range []string{"three_failures", "hard_failure", "active_lease", "not_exhausted"} {
		t.Run(kind, func(t *testing.T) {
			x := newContinuationFixture(t)
			var q string
			switch kind {
			case "three_failures":
				q = `UPDATE portal_monthly_source_preparation SET failures=3`
			case "hard_failure":
				q = `UPDATE portal_monthly_source_preparation SET failure_code='needs_review'`
			case "active_lease":
				q = `UPDATE portal_monthly_source_preparation SET state='running',lease='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',lease_until=next_at+60000`
			case "not_exhausted":
				q = `UPDATE portal_monthly_source_preparation SET steps=1`
			}
			if _, err := x.f.receiver.Exec(q); err != nil {
				t.Fatal(err)
			}
			before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.plan.Request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := x.d.PreviewSourcePreparationContinuation(t.Context(), 1, 1, x.plan.Request.Month); err == nil {
				t.Fatal("unsafe failure offered grant")
			}
			if _, err := x.d.ContinueSourcePreparation(t.Context(), x.plan); err == nil {
				t.Fatal("stale failure accepted")
			}
			after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.plan.Request)
			if err != nil || after != before {
				t.Fatal("rejection mutated old job", err)
			}
		})
	}
}

func TestContinuationDoesNotRenewExistingAdaptiveRoots(t *testing.T) {
	x := newBoundResumeFixture(t)
	p := x.d.runtime.preparation
	p.accountTokenWork = true
	if _, err := x.f.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=4096,failures=2,failure_code='budget_exhausted'`); err != nil {
		t.Fatal(err)
	}
	if err := x.d.EnableSourcePreparationContinuations(); err != nil {
		t.Fatal(err)
	}
	if _, err := x.d.PreviewSourcePreparationContinuation(t.Context(), 1, 1, x.f.req.Month); !errors.Is(err, ErrPreparationResumeDenied) {
		t.Fatal("bound roots accepted continuation", err)
	}
	if rootResumeCount(t, x) != 0 {
		t.Fatal("preview resumed root")
	}
}

func applyContinuation(t *testing.T, x continuationFixture) string {
	t.Helper()
	sha, err := x.d.ContinueSourcePreparation(t.Context(), x.plan)
	if err != nil || !generationHex(sha, 32) {
		t.Fatal(sha, err)
	}
	return sha
}

func readContinuation(t *testing.T, x continuationFixture) preparationContinuation {
	t.Helper()
	c, err := readPreparationContinuation(t.Context(), x.f.receiver, x.plan.Request.Key())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestContinuationActualMissingObservationsKeepOriginalBudgetAndMoneyUnapproved(t *testing.T) {
	x := newContinuationFixture(t)
	original, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.plan.Request)
	if err != nil {
		t.Fatal(err)
	}
	sha := applyContinuation(t, x)
	for i := 2; i <= 4; i++ {
		if err := x.r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		*x.now = x.now.Add(2 * time.Second)
		state, steps, failures := preparationState(t, x.f.receiver)
		observed, credited := workCounts(t, x.f)
		c := readContinuation(t, x)
		if state != "queued" || steps != 4096 || failures != 0 || observed != i || credited != i || c.claims != i-1 || c.exempt != i-1 || c.failures != 0 || c.grant.Original != original {
			t.Fatal("old budget reset, source duplicated or continuation lost", state, steps, failures, observed, credited, c)
		}
	}
	before := readContinuation(t, x)
	if replay := applyContinuation(t, x); replay != sha || readContinuation(t, x) != before {
		t.Fatal("replay renewed grant")
	}
	if _, err := x.d.PreviewSourcePreparationContinuation(t.Context(), 1, 1, x.plan.Request.Month); err == nil {
		t.Fatal("second grant offered")
	}
	var selections int
	if err := x.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_generation_selections`).Scan(&selections); err != nil || selections != 0 {
		t.Fatal("work approved money", selections, err)
	}
}

func TestContinuationApplyFailureRollsBackAndReturnsNoReceipt(t *testing.T) {
	x := newContinuationFixture(t)
	if _, err := x.f.receiver.Exec(`CREATE TRIGGER synthetic_continuation_failure BEFORE INSERT ON portal_monthly_preparation_continuation_counts BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
		t.Fatal(err)
	}
	sha, err := x.d.ContinueSourcePreparation(t.Context(), x.plan)
	if err == nil || sha != "" {
		t.Fatal("rollback returned success receipt", sha, err)
	}
	var n int
	if err := x.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_preparation_continuations`).Scan(&n); err != nil || n != 0 {
		t.Fatal("partial grant", n, err)
	}
	state, steps, failures := preparationState(t, x.f.receiver)
	if state != "failed" || steps != 4096 || failures != 0 {
		t.Fatal(state, steps, failures)
	}
	if _, err := x.f.receiver.Exec(`DROP TRIGGER synthetic_continuation_failure`); err != nil {
		t.Fatal(err)
	}
	applyContinuation(t, x)
}

func TestContinuationDefaultOffPausedAndRestartDoNotRenew(t *testing.T) {
	x := newContinuationFixture(t)
	x.r.preparation.continuationEnabled = false
	if _, err := x.d.ContinueSourcePreparation(t.Context(), x.plan); err == nil {
		t.Fatal("default-off grant accepted")
	}
	if err := x.d.EnableSourcePreparationContinuations(); err != nil {
		t.Fatal(err)
	}
	applyContinuation(t, x)
	before := readContinuation(t, x)
	queries := x.token.funding.source.Driver().(*tokenPreparationDriver).base.queries.Load()
	x.r.preparation.continuationEnabled = false
	if err := x.r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if view, err := x.r.preparation.ReadMonthly(t.Context(), 1, 1, x.plan.Request.Month); err != nil || view.State != "needs_review" {
		t.Fatal("paused grant falsely reports progress", view, err)
	}
	if readContinuation(t, x) != before || x.token.funding.source.Driver().(*tokenPreparationDriver).base.queries.Load() != queries {
		t.Fatal("paused continuation ran")
	}
	// Fresh owner wrapper over the same durable stores: opt-in changes no grant.
	d := &Deployment{g: x.d.g, runtime: x.r, receiver: x.f.receiver, capturedPreparation: x.d.capturedPreparation}
	if err := d.EnableSourcePreparationContinuations(); err != nil {
		t.Fatal(err)
	}
	if err := x.r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := readContinuation(t, x)
	if after.grant != before.grant || after.claims != before.claims+1 {
		t.Fatal("re-enable reset time or budget")
	}
}

func TestContinuationCannotAuthorizePartiallyRestoredWorkLedger(t *testing.T) {
	x := newContinuationFixture(t)
	if _, err := x.f.receiver.Exec(`DROP TRIGGER token_work_count_no_delete; DELETE FROM portal_monthly_token_work_counts`); err != nil {
		t.Fatal(err)
	}
	if sha, err := x.d.ContinueSourcePreparation(t.Context(), x.plan); err == nil || sha != "" {
		t.Fatal("inconsistent work authorized", sha, err)
	}
	var grants int
	if err := x.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_preparation_continuations`).Scan(&grants); err != nil || grants != 0 {
		t.Fatal("invalid grant persisted", grants, err)
	}
}

func TestContinuationLeaseCrashAndLateWriterCannotEraseFailures(t *testing.T) {
	x := newContinuationFixture(t)
	applyContinuation(t, x)
	job, err := x.r.preparation.claim(t.Context())
	if err != nil || job.continuation == "" {
		t.Fatal(job, err)
	}
	*x.now = x.now.Add(61 * time.Second)
	if err := x.r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := readContinuation(t, x)
	if c.claims != 2 || c.exempt != 1 || c.failures != 1 {
		t.Fatal("crash did not cost failure and coordinator work", c)
	}
	if _, err := x.r.preparation.checkContinuationLease(t.Context(), x.f.receiver, job); err == nil {
		t.Fatal("stale lease accepted")
	}
	state, steps, failures := preparationState(t, x.f.receiver)
	if state != "queued" || steps != 4096 || failures != 0 {
		t.Fatal("original budget changed", state, steps, failures)
	}
}

func TestContinuationLimitsStopWithoutSourceOrRenewal(t *testing.T) {
	for _, kind := range []string{"age", "dispatch", "coordinator", "failures"} {
		t.Run(kind, func(t *testing.T) {
			x := newContinuationFixture(t)
			applyContinuation(t, x)
			if err := x.r.preparation.runOne(t.Context()); err != nil {
				t.Fatal(err)
			}
			*x.now = x.now.Add(2 * time.Second)
			switch kind {
			case "age":
				*x.now = x.now.Add(24 * time.Hour)
			case "dispatch":
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_preparation_continuation_counts SET claims=100000,exempt=99999`); err != nil {
					t.Fatal(err)
				}
			case "coordinator":
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_preparation_continuation_counts SET claims=4097`); err != nil {
					t.Fatal(err)
				}
			case "failures":
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_preparation_continuation_counts SET failures=3`); err != nil {
					t.Fatal(err)
				}
			}
			queries := x.token.funding.source.Driver().(*tokenPreparationDriver).base.queries.Load()
			before := readContinuation(t, x)
			if err := x.r.preparation.runOne(t.Context()); err != nil {
				t.Fatal(err)
			}
			state, steps, _ := preparationState(t, x.f.receiver)
			if state != "failed" || steps != 4096 || readContinuation(t, x) != before || x.token.funding.source.Driver().(*tokenPreparationDriver).base.queries.Load() != queries {
				t.Fatal("exhausted continuation ran or reset", state, steps)
			}
			applyContinuation(t, x) // idempotent acknowledgment, not activation
			state, _, _ = preparationState(t, x.f.receiver)
			if state != "failed" {
				t.Fatal("replay restarted exhausted task")
			}
		})
	}
}

func TestContinuationIntegrityAndPartialRestoreNeverReachSource(t *testing.T) {
	for _, kind := range []string{"missing_count", "missing_guard", "orphan_count", "orphan_parent", "old_binary_reset", "stale_plan", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			x := newContinuationFixture(t)
			if kind == "stale_plan" {
				x.plan.CheckpointSHA256 = strings.Repeat("f", 64)
				if _, err := x.d.ContinueSourcePreparation(t.Context(), x.plan); err == nil {
					t.Fatal("stale checkpoint accepted")
				}
				return
			}
			if kind == "revoked" {
				auth, err := x.r.preparation.auth.DB()
				if err != nil {
					t.Fatal(err)
				}
				var seq int
				var name, path string
				if err := auth.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
					t.Fatal(err)
				}
				writer := generationDB(t, path, false)
				if _, err := writer.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`); err != nil {
					t.Fatal(err)
				}
				if _, err := x.d.ContinueSourcePreparation(t.Context(), x.plan); err == nil {
					t.Fatal("revoked grant accepted")
				}
				return
			}
			applyContinuation(t, x)
			var q string
			switch kind {
			case "missing_count":
				q = `DROP TRIGGER continuation_counts_no_delete; DELETE FROM portal_monthly_preparation_continuation_counts`
			case "missing_guard":
				q = `DROP TRIGGER continuation_counts_monotonic`
			case "orphan_count":
				q = `DROP TRIGGER continuation_grant_no_delete; DELETE FROM portal_monthly_preparation_continuations`
			case "orphan_parent":
				q = `DROP TRIGGER continuation_original_no_delete; DELETE FROM portal_monthly_source_preparation`
			case "old_binary_reset":
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=0,failures=0,created_at=created_at+1`); err == nil {
					t.Fatal("old binary erased budget")
				}
				return
			}
			if _, err := x.f.receiver.Exec(q); err != nil {
				t.Fatal(err)
			}
			queries := x.token.funding.source.Driver().(*tokenPreparationDriver).base.queries.Load()
			if kind == "orphan_parent" {
				if _, err := x.r.preparation.SubmitMonthly(t.Context(), 1, 1, x.plan.Request.Month); err == nil {
					t.Fatal("partial restore recreated parent")
				}
				return
			}
			if err := x.r.preparation.runOne(t.Context()); err != nil {
				t.Fatal(err)
			}
			state, _, _ := preparationState(t, x.f.receiver)
			if state != "failed" || queries != x.token.funding.source.Driver().(*tokenPreparationDriver).base.queries.Load() {
				t.Fatal("corrupt restore queried source", state)
			}
		})
	}
}

func TestContinuationFailureBudgetIsCombinedNotReset(t *testing.T) {
	x := newContinuationFixture(t)
	if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET failures=2`); err != nil {
		t.Fatal(err)
	}
	plan, err := x.d.PreviewSourcePreparationContinuation(t.Context(), 1, 1, x.plan.Request.Month)
	if err != nil {
		t.Fatal(err)
	}
	x.plan = plan
	applyContinuation(t, x)
	x.token.funding.source.Driver().(*tokenPreparationDriver).base.onQuery = func() error { return errors.New("synthetic unavailable") }
	if err := x.r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := readContinuation(t, x)
	state, steps, failures := preparationState(t, x.f.receiver)
	if state != "failed" || steps != 4096 || failures != 2 || c.failures != 1 {
		t.Fatal("failure budget renewed", state, steps, failures, c.failures)
	}
}

func TestContinuationCancelledWorkerLeavesRecoverableLease(t *testing.T) {
	x := newContinuationFixture(t)
	applyContinuation(t, x)
	ctx, cancel := context.WithCancel(t.Context())
	x.token.funding.source.Driver().(*tokenPreparationDriver).base.onQuery = func() error { cancel(); return context.Canceled }
	if err := x.r.preparation.runOne(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	state, _, _ := preparationState(t, x.f.receiver)
	if state != "running" {
		t.Fatal("cancel erased lease", state)
	}
	x.token.funding.source.Driver().(*tokenPreparationDriver).base.onQuery = nil
	*x.now = x.now.Add(61 * time.Second)
	if err := x.r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if readContinuation(t, x).failures != 1 {
		t.Fatal("cancel lost bounded failure")
	}
}
