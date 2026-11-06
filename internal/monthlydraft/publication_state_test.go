package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func publicationFixture(t *testing.T) (generationFixture, monthlytasks.Task, *publicationStore) {
	t.Helper()
	f := generationFixtureNew(t)
	job := generationSubmit(t, f)
	if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	job, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newPublicationStore(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	return f, job, s
}

func TestPublicationJournalRetrySurvivesReopenAndPinsEvidence(t *testing.T) {
	f, job, s := publicationFixture(t)
	ctx := context.Background()
	now := time.Now()
	s.now = func() time.Time { return now }
	l, ok, err := s.claim(ctx, job)
	if err != nil || !ok || l.attempt != 1 {
		t.Fatal("first claim", ok, err)
	}
	if _, err := json.Marshal(l); err == nil {
		t.Fatal("lease serialized")
	}
	var leaked publicationLease
	if err := json.Unmarshal([]byte(`{}`), &leaked); err == nil {
		t.Fatal("lease restored from JSON")
	}
	if err := s.bind(ctx, l, []byte("synthetic frozen evidence")); err != nil {
		t.Fatal(err)
	}
	if err := s.bind(ctx, l, []byte("changed evidence")); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("changed evidence accepted", err)
	}
	if err := s.finish(ctx, l, errors.New("synthetic temporary failure")); err != nil {
		t.Fatal(err)
	}
	other, err := newPublicationStore(generationDB(t, f.receiverPath, false))
	if err != nil {
		t.Fatal(err)
	}
	other.now = func() time.Time { return now }
	if _, ok, err := other.claim(ctx, job); err != nil || ok {
		t.Fatal("restart bypassed cooldown", ok, err)
	}
	now = now.Add(publicationRetryTime)
	l2, ok, err := other.claim(ctx, job)
	if err != nil || !ok || l2.attempt != 2 || l2.token == l.token {
		t.Fatal("durable retry", ok, err)
	}
	if err := s.finish(ctx, l, nil); !errors.Is(err, monthlytasks.ErrLeaseLost) {
		t.Fatal("old worker finished another attempt", err)
	}
	if err := other.bind(ctx, l2, []byte("changed evidence")); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("restart forgot frozen evidence", err)
	}
	if err := other.bind(ctx, l2, []byte("synthetic frozen evidence")); err != nil {
		t.Fatal(err)
	}
	if err := other.finish(ctx, l2, nil); err != nil {
		t.Fatal(err)
	}
	r, err := s.read(ctx, job.ID)
	if err != nil || r.state != publicationSaved || r.attempts != 2 {
		t.Fatal("success not durable", r, err)
	}
	if _, ok, err := s.claim(ctx, job); err != nil || ok {
		t.Fatal("saved job reclaimed", ok, err)
	}
	if _, err := f.receiver.Exec(`DELETE FROM portal_monthly_publication_jobs WHERE task_id=?`, job.ID); err == nil {
		t.Fatal("journal deleted")
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_publication_jobs SET state='retry' WHERE task_id=?`, job.ID); err == nil {
		t.Fatal("terminal job reactivated")
	}
	if body, err := os.ReadFile(f.path); err != nil || generationHash(body) != f.sourceHash {
		t.Fatal("funding source changed", err)
	}
}

func TestPublicationJournalCrashBudgetAndExpiredWorkerFencing(t *testing.T) {
	f, job, s := publicationFixture(t)
	ctx := context.Background()
	now := time.Now()
	var last publicationLease
	for attempt := 1; attempt <= 3; attempt++ {
		// A genuinely new store/connection models process restart.
		s, err := newPublicationStore(generationDB(t, f.receiverPath, false))
		if err != nil {
			t.Fatal(err)
		}
		s.now = func() time.Time { return now }
		l, ok, err := s.claim(ctx, job)
		if err != nil || !ok || l.attempt != attempt {
			t.Fatal("crash attempt", attempt, ok, err)
		}
		if attempt > 1 {
			if err := s.bind(ctx, last, []byte("late old worker")); !errors.Is(err, monthlytasks.ErrLeaseLost) {
				t.Fatal("expired worker rebound", err)
			}
		}
		if _, ok, err := s.claim(ctx, job); err != nil || ok {
			t.Fatal("live lease stolen", err)
		}
		last = l
		now = now.Add(publicationLeaseTime)
		if err := s.finish(ctx, l, errors.New("too late")); !errors.Is(err, monthlytasks.ErrLeaseLost) {
			t.Fatal("expired worker completed", err)
		}
	}
	s.now = func() time.Time { return now }
	if _, ok, err := s.claim(ctx, job); err != nil || ok {
		t.Fatal("fourth crash attempt", ok, err)
	}
	r, err := s.read(ctx, job.ID)
	if err != nil || r.state != publicationPaused || r.attempts != 3 {
		t.Fatal("crash budget reset", r, err)
	}
	reopened, err := newPublicationStore(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = func() time.Time { return now.Add(time.Hour) }
	if _, ok, err := reopened.claim(ctx, job); err != nil || ok {
		t.Fatal("paused job resumed by restart", ok, err)
	}
}

func TestPublicationJournalConcurrentClaimHasSingleOwner(t *testing.T) {
	f, job, s := publicationFixture(t)
	other, err := newPublicationStore(generationDB(t, f.receiverPath, false))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	start := make(chan struct{})
	type result struct {
		lease publicationLease
		ok    bool
		err   error
	}
	out := make(chan result, 2)
	var wg sync.WaitGroup
	for _, store := range []*publicationStore{s, other} {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; l, ok, err := store.claim(ctx, job); out <- result{l, ok, err} }()
	}
	close(start)
	wg.Wait()
	close(out)
	owners := 0
	for r := range out {
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.ok {
			owners++
			if r.lease.attempt != 1 {
				t.Fatal("attempt incremented by loser")
			}
		}
	}
	if owners != 1 {
		t.Fatal("duplicate journal owners", owners)
	}
}

func TestPublicationJournalUnknownSchemaAndClockFailClosed(t *testing.T) {
	for _, kind := range []string{"missing_control", "unknown_version", "bad_lease", "wrong_task", "backwards_clock"} {
		t.Run(kind, func(t *testing.T) {
			f, job, s := publicationFixture(t)
			ctx := context.Background()
			now := time.Now()
			s.now = func() time.Time { return now }
			l, ok, err := s.claim(ctx, job)
			if err != nil || !ok {
				t.Fatal(err)
			}
			switch kind {
			case "missing_control":
				_, err = f.receiver.Exec(`DELETE FROM portal_monthly_publication_control WHERE id=1`)
			case "unknown_version":
				_, err = f.receiver.Exec(`UPDATE portal_monthly_publication_control SET version=2 WHERE id=1`)
			case "bad_lease":
				_, err = f.receiver.Exec(`UPDATE portal_monthly_publication_jobs SET lease_until=0 WHERE task_id=?`, job.ID)
			case "wrong_task":
				job.Request.Owner.GroupID++
			case "backwards_clock":
				now = now.Add(-time.Second)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.claim(ctx, job); err == nil {
				t.Fatal("invalid journal claimed")
			}
			if kind != "wrong_task" {
				if err := s.bind(ctx, l, []byte("evidence")); err == nil {
					t.Fatal("invalid journal reached side-effect boundary")
				}
			}
		})
	}
}

func TestPublicationJournalUpgradeIsExplicitAndDraftCompatible(t *testing.T) {
	f := generationFixtureNew(t)
	for _, q := range []string{`DROP TABLE portal_monthly_publication_jobs`, `DROP TABLE portal_monthly_publication_control`} {
		if _, err := f.receiver.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewLocalWorker(f.g); err != nil {
		t.Fatal("legacy draft worker broke", err)
	}
	load := func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error) {
		t.Fatal("constructor collected")
		return AutomaticSourceContext{}, nil
	}
	publish := func(context.Context, monthlytasks.Request, []byte) error {
		t.Fatal("constructor published")
		return nil
	}
	if _, err := NewAutomaticLocalWorker(f.g, load, publish); err == nil {
		t.Fatal("missing journal auto-created")
	}
	var count int
	if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='portal_monthly_publication_jobs'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("constructor migrated", count, err)
	}
	if err := InitAutomaticPublicationReceiver(context.Background(), f.receiver); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAutomaticLocalWorker(f.g, load, publish); err != nil {
		t.Fatal("explicit upgrade unusable", err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_publication_control SET version=2 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if err := InitAutomaticPublicationReceiver(context.Background(), f.receiver); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("unknown journal version reset", err)
	}
}

func TestGenerationAutomaticCommitLostResponseRecoversIdenticalBill(t *testing.T) {
	f := generationFixtureNew(t)
	auth, ro, _ := usageAuthorityFixture(t)
	if err := auth.AutoMigrate(&monitor.PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	job := generationSubmit(t, f)
	meta := handoffContext(t, f)
	publishes := 0
	load := func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error) {
		return AutomaticSourceContext{BundleContext: meta}, nil
	}
	publish := func(ctx context.Context, req monthlytasks.Request, raw []byte) error {
		publishes++
		_, err := monitor.ImportMonthlyBillForAuthorization(ctx, auth, raw, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, time.Now())
		if err != nil {
			return err
		}
		if publishes == 1 {
			return errors.New("synthetic response lost after commit")
		}
		return nil
	}
	w, err := NewAutomaticLocalWorker(f.g, load, publish)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.step(context.Background()); err != nil {
		t.Fatal(err)
	}
	var before monitor.PortalMonthlyBill
	if err := auth.First(&before, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	r, err := w.publications.read(context.Background(), job.ID)
	if err != nil || r.state != publicationRetry || r.evidence == "" {
		t.Fatal("ambiguous commit not recoverable", r, err)
	}
	restarted, err := NewAutomaticLocalWorker(f.g, load, publish)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := restarted.step(context.Background()); err != nil || worked || publishes != 1 {
		t.Fatal("restart bypassed backoff", worked, err, publishes)
	}
	restarted.publications.now = func() time.Time { return time.UnixMilli(r.retry) }
	if worked, err := restarted.step(context.Background()); err != nil || !worked || publishes != 2 || restarted.Status().Published != 1 {
		t.Fatal("ambiguous publication recovery", worked, err, publishes, restarted.Status())
	}
	var after monitor.PortalMonthlyBill
	if err := auth.First(&after, "group_id=1 AND month='2026-09'").Error; err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("recovery changed bill/version", err)
	}
	// The same task cannot be restarted after a terminal journal transition.
	if _, err := restarted.publications.db.Exec(`UPDATE portal_monthly_publication_jobs SET state='retry' WHERE task_id=?`, job.ID); err == nil {
		t.Fatal("successful journal reset")
	}
}

// Keep database/sql imported in this store test as an explicit assertion that
// an absent per-task row differs from an unavailable/unknown journal schema.
func TestPublicationJournalMissingRowIsNotSchemaFailure(t *testing.T) {
	_, _, s := publicationFixture(t)
	if _, err := s.read(context.Background(), "no-such-task"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
}

func TestGenerationAutomaticRetryCannotChangeFrozenEvidence(t *testing.T) {
	f, job, _ := publicationFixture(t)
	ctx := context.Background()
	meta := handoffContext(t, f)
	load := func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error) {
		return AutomaticSourceContext{BundleContext: meta}, nil
	}
	publishes := 0
	publish := func(context.Context, monthlytasks.Request, []byte) error {
		publishes++
		return errors.New("synthetic ambiguous response")
	}
	w, err := NewAutomaticLocalWorker(f.g, load, publish)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.step(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := w.publications.read(ctx, job.ID)
	if err != nil || before.state != publicationRetry || before.evidence == "" || publishes != 1 {
		t.Fatal(before, err, publishes)
	}
	var changed monitor.MonthlyBundleContext
	if err := json.Unmarshal(meta, &changed); err != nil {
		t.Fatal(err)
	}
	changed.Groups[0].Name = "different synthetic organization display name"
	meta, err = json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAutomaticLocalWorker(f.g, load, publish)
	if err != nil {
		t.Fatal(err)
	}
	restarted.publications.now = func() time.Time { return time.UnixMilli(before.retry) }
	if _, err := restarted.step(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := restarted.publications.read(ctx, job.ID)
	if err != nil || after.state != publicationPaused || after.evidence != before.evidence || publishes != 1 {
		t.Fatal("retry published changed evidence", after, err, publishes)
	}
}

func TestGenerationAutomaticCancellationRetainsCooldownBeforeShutdown(t *testing.T) {
	f, job, _ := publicationFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	load := func(ctx context.Context, _ monthlytasks.Request) (AutomaticSourceContext, error) {
		cancel()
		return AutomaticSourceContext{}, ctx.Err()
	}
	w, err := NewAutomaticLocalWorker(f.g, load, func(context.Context, monthlytasks.Request, []byte) error {
		t.Fatal("canceled work published")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.step(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	other, err := newPublicationStore(generationDB(t, f.receiverPath, false))
	if err != nil {
		t.Fatal(err)
	}
	r, err := other.read(context.Background(), job.ID)
	if err != nil || r.state != publicationRetry || r.attempts != 1 || r.token != "" || r.retry <= r.updated {
		t.Fatal("shutdown lost recovery state", r, err)
	}
	if _, ok, err := other.claim(context.Background(), job); err != nil || ok {
		t.Fatal("new process bypassed cancellation cooldown", ok, err)
	}
}

func TestPublicationJournalInitializationRejectsPartialOrMissingControl(t *testing.T) {
	for _, damage := range []string{"missing_control_table", "missing_jobs_table", "missing_version_row"} {
		t.Run(damage, func(t *testing.T) {
			f, _, _ := publicationFixture(t)
			query := map[string]string{
				"missing_control_table": "DROP TABLE portal_monthly_publication_control",
				"missing_jobs_table":    "DROP TABLE portal_monthly_publication_jobs",
				"missing_version_row":   "DELETE FROM portal_monthly_publication_control",
			}[damage]
			if _, err := f.receiver.Exec(query); err != nil {
				t.Fatal(err)
			}
			if err := InitAutomaticPublicationReceiver(context.Background(), f.receiver); !errors.Is(err, ErrGenerationIntegrity) {
				t.Fatal("damaged recovery metadata silently recreated", err)
			}
		})
	}
}
