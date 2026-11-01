package monthlydraft

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Complete test protocol: registration/current-wallet plus token queries.
// The old wallet-only fixture deliberately rejects token SQL, so cannot be
// used to prove actual child recapture after a stopped root is reactivated.
type boundResumeDriver struct{ base *sourcePreparationDriver }
type boundResumeConn struct{ *sourcePreparationConn }

func (d *boundResumeDriver) Open(string) (driver.Conn, error) {
	return &boundResumeConn{&sourcePreparationConn{d: d.base}}, nil
}
func (c *boundResumeConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "FROM users WHERE id IN") {
		return (&walletPreparationConn{c.sourcePreparationConn}).QueryContext(ctx, q, args)
	}
	return (&tokenPreparationConn{c.sourcePreparationConn}).QueryContext(ctx, q, args)
}

// Synthetic setup only: original empty child windows plus a stopped newer
// sweep. No production records are removed, rewritten or copied into tests.
func newBoundResumeFixture(t *testing.T) *capturedResumeFixture {
	t.Helper()
	f := newCapturedHistoryFixture(t)
	syntheticCapturedRecoveryWithRecentRoot(t, &f, true)
	db := f.wallet.funding.capture
	if err := monthlysource.InitAdaptiveTokenResumptionStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`DROP TRIGGER adaptive_token_observations_no_DELETE`,
		`DELETE FROM monthly_adaptive_token_observations WHERE phase=1`,
		`CREATE TRIGGER adaptive_token_observations_no_DELETE BEFORE DELETE ON monthly_adaptive_token_observations BEGIN SELECT RAISE(ABORT,'adaptive token records are immutable'); END`,
		`INSERT INTO monthly_adaptive_token_faults SELECT id,0,'retryable_source',created_at_ms+1000,0 FROM monthly_adaptive_token_roots`,
		`INSERT INTO monthly_adaptive_token_faults SELECT id,1,'retryable_source',created_at_ms+32000,0 FROM monthly_adaptive_token_roots`,
		`INSERT INTO monthly_adaptive_token_faults SELECT id,2,'retryable_source',created_at_ms+63000,0 FROM monthly_adaptive_token_roots`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	d := openBoundFixture(t, f)
	x := &capturedResumeFixture{f: f, d: d, now: time.Now().Add(-10 * time.Minute)}
	p := d.runtime.preparation
	p.now = func() time.Time { return x.now }
	actual := p.step
	p.step = func(context.Context, SourcePreparationRequest) (bool, error) {
		return false, errors.New("synthetic source connection outage")
	}
	if _, err := p.SubmitMonthly(t.Context(), 1, 1, f.req.Month); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := p.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		x.now = x.now.Add(31 * time.Second)
	}
	p.step = actual
	return x
}

func openBoundFixture(t *testing.T, f capturedHistoryFixture) *Deployment {
	return openBoundFixtureWithWork(t, f, false)
}

func openBoundFixtureWithWork(t *testing.T, f capturedHistoryFixture, work bool) *Deployment {
	t.Helper()
	d := f.open(t)
	cfg, ref := capturedPreparationConfigFixture(t, f)
	cfg.RecoverTokenWindows = true
	cfg.AccountTokenWork = work
	raw := materialPreparationJSON(t, cfg)
	if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ref.SHA256 = generationHash(raw)
	baseDriver := f.wallet.funding.source.Driver().(*walletPreparationDriver).base
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, func(*mysql.Config) (*sql.DB, error) {
		db := sql.OpenDB(capturedPreparationTestConnector{&boundResumeDriver{baseDriver}})
		db.SetMaxOpenConns(1)
		return db, nil
	}); err != nil {
		t.Fatal(err)
	}
	return d
}

func boundCount(t *testing.T, x *capturedResumeFixture, q string) int {
	t.Helper()
	var n int
	if err := x.f.f.receiver.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func rootResumeCount(t *testing.T, x *capturedResumeFixture) int {
	t.Helper()
	var n int
	if err := x.f.wallet.funding.capture.QueryRow(`SELECT count(*) FROM monthly_adaptive_token_resumptions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func previewBound(t *testing.T, x *capturedResumeFixture) BoundPreparationResumePlan {
	t.Helper()
	p, err := x.d.PreviewBoundSourcePreparationResume(t.Context(), 1, 1, x.f.req.Month)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBoundResumeIntentBeforeRootsAndQueueReleaseLast(t *testing.T) {
	x := newBoundResumeFixture(t)
	p := previewBound(t, x)
	if boundCount(t, x, `SELECT count(*) FROM portal_monthly_bound_resume_intents`) != 0 || rootResumeCount(t, x) != 0 {
		t.Fatal("preview mutated")
	}
	if _, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); !errors.Is(err, ErrPreparationResumeCooldown) {
		t.Fatal("cooldown bypassed", err)
	}
	x.now = time.UnixMilli(p.EarliestMS)
	// Fail ONLY the final queue commit. The intent/root append must persist,
	// while the customer task remains stopped until exactly that plan resumes.
	if _, err := x.f.f.receiver.Exec(`CREATE TRIGGER synthetic_bound_release BEFORE INSERT ON portal_monthly_source_resumptions BEGIN SELECT RAISE(ABORT,'synthetic final commit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err == nil || r.Applied {
		t.Fatal(r, err)
	}
	state, _, failures := preparationState(t, x.f.f.receiver)
	if state != "failed" || failures != 3 || rootResumeCount(t, x) != 1 || resumeReceiptCount(t, x.f.f.receiver) != 0 || boundCount(t, x, `SELECT count(*) FROM portal_monthly_bound_resume_intents`) != 1 {
		t.Fatal("unsafe partial state", state, failures)
	}
	closeDeploymentTest(t, x.d)
	x.d = openBoundFixture(t, x.f)
	x.d.runtime.preparation.now = func() time.Time { return x.now }
	recovered := previewBound(t, x)
	a, _ := json.Marshal(p)
	b, _ := json.Marshal(recovered)
	if string(a) != string(b) {
		t.Fatal("interrupted plan replaced")
	}
	if _, err := x.f.f.receiver.Exec(`DROP TRIGGER synthetic_bound_release`); err != nil {
		t.Fatal(err)
	}
	r, err := x.d.ResumeBoundSourcePreparation(t.Context(), recovered)
	if err != nil || !r.Applied || r.AlreadyApplied {
		t.Fatal(r, err)
	}
	state, steps, failures := preparationState(t, x.f.f.receiver)
	if state != "queued" || failures != 0 || steps != 3 || rootResumeCount(t, x) != 1 {
		t.Fatal(state, steps, failures)
	}
	for range 2 {
		r, err = x.d.ResumeBoundSourcePreparation(t.Context(), p)
		if err != nil || !r.AlreadyApplied {
			t.Fatal(r, err)
		}
	}
	if _, err := x.d.ResumeSourcePreparation(t.Context(), p.Queue); !errors.Is(err, ErrPreparationResumeDenied) {
		t.Fatal("queue-only bypass", err)
	}
	for _, q := range []string{`UPDATE portal_monthly_bound_resume_intents SET sha256=sha256`, `DELETE FROM portal_monthly_bound_resume_intents`} {
		if _, err := x.f.f.receiver.Exec(q); err == nil {
			t.Fatal("mutable intent")
		}
	}
}

func TestBoundResumeFailuresBeforeRootNeverReleaseQueue(t *testing.T) {
	for _, scenario := range []string{"intent_write", "source_write", "revoked", "changed_plan", "damaged_original", "running", "partial_restore"} {
		t.Run(scenario, func(t *testing.T) {
			x := newBoundResumeFixture(t)
			p := previewBound(t, x)
			x.now = time.UnixMilli(p.EarliestMS)
			switch scenario {
			case "intent_write":
				_, err := x.f.f.receiver.Exec(`CREATE TRIGGER synthetic_intent_failure BEFORE INSERT ON portal_monthly_bound_resume_intents BEGIN SELECT RAISE(ABORT,'synthetic intent failure'); END`)
				if err != nil {
					t.Fatal(err)
				}
			case "source_write":
				_, err := x.d.capturedPreparation.capture.Exec(`CREATE TRIGGER synthetic_root_failure BEFORE INSERT ON monthly_adaptive_token_resumptions BEGIN SELECT RAISE(ABORT,'synthetic source failure'); END`)
				if err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := x.f.auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
					t.Fatal(err)
				}
			case "changed_plan":
				p.Capture.SnapshotSHA256 = generationHash([]byte("not the pinned source"))
			case "damaged_original":
				_, err := x.d.capturedPreparation.capture.Exec(`DROP TRIGGER adaptive_token_observations_no_UPDATE; UPDATE monthly_adaptive_token_observations SET payload=x'00'`)
				if err != nil {
					t.Fatal(err)
				}
			case "running":
				if err := x.d.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "partial_restore":
				_, err := x.f.f.receiver.Exec(`DELETE FROM portal_monthly_source_preparation`)
				if err != nil {
					t.Fatal(err)
				}
			}
			if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err == nil || r.Applied {
				t.Fatal("unsafe release", r, err)
			}
			if rootResumeCount(t, x) != 0 || resumeReceiptCount(t, x.f.f.receiver) != 0 {
				t.Fatal("denial advanced source or queue")
			}
		})
	}
}

func TestBoundResumeConcurrentAndOrphanIntentCannotRecreateTask(t *testing.T) {
	x := newBoundResumeFixture(t)
	p := previewBound(t, x)
	x.now = time.UnixMilli(p.EarliestMS)
	var wg sync.WaitGroup
	results := make(chan SourcePreparationResumeResult, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Go(func() { r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); results <- r; errs <- err })
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for r := range results {
		if r.Applied && !r.AlreadyApplied {
			n++
		}
	}
	if n != 1 || rootResumeCount(t, x) != 1 || resumeReceiptCount(t, x.f.f.receiver) != 1 {
		t.Fatal("duplicate activation", n)
	}
	// SETUP ONLY: retained intent without its queue row after partial restore.
	if _, err := x.f.f.receiver.Exec(`DELETE FROM portal_monthly_source_preparation`); err != nil {
		t.Fatal(err)
	}
	if _, err := x.d.runtime.preparation.SubmitMonthly(t.Context(), 1, 1, p.Queue.Request.Month); err == nil {
		t.Fatal("orphan intent reset lifetime")
	}
	if boundCount(t, x, `SELECT count(*) FROM portal_monthly_source_preparation`) != 0 {
		t.Fatal("recreated task")
	}
}

func TestBoundResumePendingIntentRestoreAndRevocationStayStopped(t *testing.T) {
	for _, scenario := range []string{"orphan_intent", "early_queue", "revoked_after_root"} {
		t.Run(scenario, func(t *testing.T) {
			x := newBoundResumeFixture(t)
			p := previewBound(t, x)
			x.now = time.UnixMilli(p.EarliestMS)
			if _, err := x.f.f.receiver.Exec(`CREATE TRIGGER synthetic_bound_release BEFORE INSERT ON portal_monthly_source_resumptions BEGIN SELECT RAISE(ABORT,'synthetic final commit failure'); END`); err != nil {
				t.Fatal(err)
			}
			if _, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err == nil {
				t.Fatal("expected final commit failure")
			}
			base := x.f.wallet.funding.source.Driver().(*walletPreparationDriver).base
			queries := base.queries.Load()
			switch scenario {
			case "orphan_intent":
				if _, err := x.f.f.receiver.Exec(`DELETE FROM portal_monthly_source_preparation`); err != nil {
					t.Fatal(err)
				}
				if _, err := x.d.runtime.preparation.SubmitMonthly(t.Context(), 1, 1, p.Queue.Request.Month); err == nil {
					t.Fatal("orphan pending intent recreated task")
				}
				if boundCount(t, x, `SELECT count(*) FROM portal_monthly_source_preparation`) != 0 {
					t.Fatal("task recreated")
				}
			case "early_queue":
				if _, err := x.f.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET state='queued',failures=0`); err != nil {
					t.Fatal(err)
				}
				if err := x.d.runtime.preparation.runOne(t.Context()); err != nil {
					t.Fatal(err)
				}
				state, _, _ := preparationState(t, x.f.f.receiver)
				if state != "failed" {
					t.Fatal("unreleased restored queue ran", state)
				}
			case "revoked_after_root":
				if err := x.f.auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
					t.Fatal(err)
				}
				if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err == nil || r.Applied {
					t.Fatal("revoked root released queue", r, err)
				}
			}
			if rootResumeCount(t, x) != 1 || resumeReceiptCount(t, x.f.f.receiver) != 0 || base.queries.Load() != queries {
				t.Fatal("partial recovery read source or released queue")
			}
		})
	}
}

func TestBoundResumeIntentCapacityStopsBeforeRootAppend(t *testing.T) {
	x := newBoundResumeFixture(t)
	p := previewBound(t, x)
	x.now = time.UnixMilli(p.EarliestMS)
	for i := 0; i < 32; i++ {
		id := generationHash([]byte("unrelated retained recovery intent " + strconv.Itoa(i)))
		if _, err := x.f.f.receiver.Exec(`INSERT INTO portal_monthly_bound_resume_intents VALUES(?,1,?,zeroblob(262144))`, id, id); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); !errors.Is(err, ErrGenerationCapacity) || r.Applied {
		t.Fatal("intent pool overflowed", r, err)
	}
	if rootResumeCount(t, x) != 0 || resumeReceiptCount(t, x.f.f.receiver) != 0 {
		t.Fatal("full intent pool advanced roots/queue")
	}
	state, _, failures := preparationState(t, x.f.f.receiver)
	if state != "failed" || failures != 3 {
		t.Fatal("capacity reset queue", state, failures)
	}
}

func TestBoundResumeActualQueuePublishesAndDownloadsExactMoney(t *testing.T) {
	x := newBoundResumeFixture(t)
	p := previewBound(t, x)
	x.now = time.UnixMilli(p.EarliestMS)
	if r, err := x.d.ResumeBoundSourcePreparation(t.Context(), p); err != nil || !r.Applied {
		t.Fatal(r, err)
	}
	x.d.runtime.preparation.now = time.Now
	request, own, foreign := capturedHistoryHTTP(t, x.f, x.d)
	if err := x.d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var record monitor.PortalMonthlyBill
	for {
		err := x.f.auth.First(&record, "group_id=? AND month=?", 1, p.Queue.Request.Month).Error
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			state, steps, failures := preparationState(t, x.f.f.receiver)
			t.Fatal("resumed source never published", state, steps, failures, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, x.d)
	assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("invalid bill")
	}
	for _, u := range evidence.Users {
		if u.OpeningQuota != 500000000 || u.ClosingQuota != 499000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("financial values changed", u.UserID)
		}
	}
	if rootResumeCount(t, x) != 1 {
		t.Fatal("original root epoch lost")
	}
}
