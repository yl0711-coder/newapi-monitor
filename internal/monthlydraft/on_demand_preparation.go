package monthlydraft

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// SourcePreparationRequest is server-derived authority, NOT financial evidence.
// Key is stable across restarts so a trusted producer can resume its private
// checkpoints. Customers cannot supply paths, member IDs, hashes or amounts.
type SourcePreparationRequest struct {
	Owner monthlytasks.Owner `json:"owner"`
	Month string             `json:"month"`
}

func (r SourcePreparationRequest) Key() string {
	raw, _ := json.Marshal(r)
	return generationHash(raw)
}

// SourcePreparationStep must honor cancellation and durably checkpoint partial
// progress before returning false. It must not publish a bill. A true result is
// independently checked against the existing validated source-selection ledger.
// No source work runs in HTTP or while holding a receiver transaction.
type SourcePreparationStep func(context.Context, SourcePreparationRequest) (bool, error)

// A trusted source stage may defer an independent repeat observation. This
// carries no evidence or customer state and never relaxes source time limits.
type sourcePreparationPause struct{ until time.Time }

func (*sourcePreparationPause) Error() string {
	return "source preparation waiting for independent observation"
}

// SourcePreparationAdmission must be a cheap, read-only server policy for the
// supported historical range. It is not a replacement for Usage authorization.
type SourcePreparationAdmission func(context.Context, SourcePreparationRequest) error

type onDemandPreparation struct {
	capacity            *publishedCapacityMaintenance
	db                  *sql.DB
	auth                *gorm.DB
	base                monitor.MonthlyGenerationService
	admit               SourcePreparationAdmission
	step                SourcePreparationStep
	wake                chan struct{}
	now                 func() time.Time
	scope               *generationScope
	dependencies        *registeredSeriesPreparation
	accountTokenWork    bool                         // explicit captured-source opt-in; old budgets unchanged otherwise
	historySourceWork   *automaticHistoryPreparation // explicit source-only work producer; never a financial scope grant
	continuationEnabled bool                         // one explicit bounded continuation; never a customer override
}

type sourcePreparationJob struct {
	request      SourcePreparationRequest
	key, lease   string
	continuation string
}

// EnableOnDemandPreparation is explicit LOCAL opt-in before Start. Construction
// installs only a queue and trusted producer seam; no production source adapter
// or broader organization/month rollout is implicitly enabled.
func (r *LocalRuntime) EnableOnDemandPreparation(ctx context.Context, auth *gorm.DB, admit SourcePreparationAdmission, step SourcePreparationStep) error {
	if ctx == nil || auth == nil || admit == nil || step == nil {
		return ErrGenerationInput
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != "disabled" || r.preparation != nil {
		return ErrGenerationBusy
	}
	base, ok := r.service.(*usageHTTPService)
	if !ok || base.authorization != auth {
		return ErrGenerationInput
	}
	p := &onDemandPreparation{db: base.g.receiver, auth: auth, base: base, admit: admit, step: step, wake: make(chan struct{}, 1), now: time.Now, scope: r.scope}
	if err := monthlytasks.WriteReceiver(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
		for _, query := range []string{
			`CREATE TABLE IF NOT EXISTS portal_monthly_source_preparation (
key TEXT PRIMARY KEY CHECK(length(key)=64),group_id INTEGER NOT NULL CHECK(group_id>0),
payload BLOB NOT NULL CHECK(length(payload)<=1024),
state TEXT NOT NULL CHECK(state IN ('queued','running','submitted','failed')),
steps INTEGER NOT NULL DEFAULT 0 CHECK(steps BETWEEN 0 AND 4096),
failures INTEGER NOT NULL DEFAULT 0 CHECK(failures BETWEEN 0 AND 3),
failure_code TEXT NOT NULL DEFAULT '' CHECK(length(failure_code)<=32),
created_at INTEGER NOT NULL,next_at INTEGER NOT NULL,lease TEXT NOT NULL DEFAULT '',lease_until INTEGER NOT NULL DEFAULT 0)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_preparation_active_group ON portal_monthly_source_preparation(group_id) WHERE state IN ('queued','running')`,
		} {
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
		if err := initSourcePreparationResumptions(ctx, tx); err != nil {
			return err
		}
		if err := initTokenWorkAccounting(ctx, tx); err != nil {
			return err
		}
		return initPreparationContinuations(ctx, tx)
	}); err != nil {
		return err
	}
	r.preparation, r.service = p, p
	return nil
}

func (p *onDemandPreparation) request(ctx context.Context, gid, version int64, month string) (SourcePreparationRequest, error) {
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	start, err := time.ParseInLocation("2006-01", month, zone)
	if !validSelectionScope(gid, month) || err != nil || start.AddDate(0, 1, 0).After(p.now()) {
		return SourcePreparationRequest{}, ErrGenerationInput
	}
	owner, _, err := LoadUsageGenerationOwner(ctx, p.auth, gid, version)
	return SourcePreparationRequest{Owner: owner, Month: month}, err
}

func (p *onDemandPreparation) check(ctx context.Context, req SourcePreparationRequest) error {
	if !p.scope.allows(req.Owner.GroupID, req.Month) {
		return ErrGenerationChanged
	}
	current, err := p.request(ctx, req.Owner.GroupID, req.Owner.AuthVersion, req.Month)
	if err != nil {
		return err
	}
	if current != req {
		return ErrGenerationChanged
	}
	if p.scope != nil && p.scope.registered {
		if p.scope.authorize == nil {
			return ErrGenerationInput
		}
		if err := p.scope.authorize(ctx, monthlytasks.Request{Owner: req.Owner, Month: req.Month}); err != nil {
			return err
		}
	}
	return p.admit(ctx, req)
}

func (p *onDemandPreparation) selected(ctx context.Context, req SourcePreparationRequest) (bool, error) {
	s, err := ReadGenerationSelection(ctx, p.db, req.Owner.GroupID, req.Month)
	if err != nil {
		return false, err
	}
	if s.Head == "" {
		return false, nil
	}
	if s.Request == nil || s.Request.Owner != req.Owner {
		return false, ErrGenerationChanged
	}
	if !currentGenerationRules(*s.Request) {
		return false, ErrGenerationRulesChanged
	}
	if p.dependencies != nil {
		return p.dependencies.ready(ctx, req)
	}
	return true, nil
}

func preparationFailure(err error) error {
	return monitor.MonthlyGenerationFailure(PublicUsageGenerationProblem(err).Code)
}

func (p *onDemandPreparation) SubmitMonthly(parent context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	req, err := p.request(ctx, gid, version, month)
	if err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	if p.dependencies != nil {
		attempted, e := p.dependencies.hasPublicationAttempt(ctx, req)
		if e != nil {
			return monitor.MonthlyGenerationView{}, preparationFailure(e)
		}
		if attempted {
			return p.base.ReadMonthly(ctx, gid, version, month)
		}
	}
	ready, err := p.selected(ctx, req)
	if err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	if ready {
		return p.base.SubmitMonthly(ctx, gid, version, month)
	}
	if err := p.check(ctx, req); err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	err = monthlytasks.WriteReceiver(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
		var exists, count, active int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_source_preparation WHERE key=?`, req.Key()).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			return nil
		} // Never reset a durable retry budget.
		// A retained activation audit without its task is inconsistent, not a
		// new first request. Do not reset lifetime budgets after partial restore.
		var resumed int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_source_resumptions WHERE job_key=?`, req.Key()).Scan(&resumed); err != nil {
			return err
		}
		if resumed != 0 {
			return ErrGenerationIntegrity
		}
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM portal_monthly_preparation_continuations WHERE job_key=?)+(SELECT count(*) FROM portal_monthly_preparation_continuation_counts WHERE job_key=?)`, req.Key(), req.Key()).Scan(&resumed); err != nil {
			return err
		}
		if resumed != 0 {
			return ErrGenerationIntegrity
		}
		var work int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM portal_monthly_token_work_grants WHERE parent_key=?)+(SELECT count(*) FROM portal_monthly_token_work_counts WHERE parent_key=?)`, req.Key(), req.Key()).Scan(&work); err != nil {
			return err
		}
		if work != 0 {
			return ErrGenerationIntegrity
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_bound_resume_intents WHERE job_key=?`, req.Key()).Scan(&resumed); err != nil {
			return err
		}
		if resumed != 0 {
			return ErrGenerationIntegrity
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(state IN ('queued','running')),0) FROM portal_monthly_source_preparation`).Scan(&count, &active); err != nil {
			return err
		}
		if count >= 256 || active >= 8 {
			return ErrGenerationCapacity
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_source_preparation WHERE group_id=? AND state IN ('queued','running')`, gid).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return ErrGenerationBusy
		}
		now := p.now().UnixMilli()
		_, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_source_preparation(key,group_id,payload,state,created_at,next_at) VALUES(?,?,?,'queued',?,?)`, req.Key(), gid, raw, now, now)
		return err
	})
	if err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	if err := p.check(ctx, req); err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return p.ReadMonthly(ctx, gid, version, month)
}

// Status reads never create jobs, capture financial sources or retry work.
// Registered eligibility may perform a bounded registration metadata lookup.
// Published statements and the underlying generation state retain precedence.
func (p *onDemandPreparation) ReadMonthly(parent context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	view, err := p.base.ReadMonthly(ctx, gid, version, month)
	if errors.Is(err, monitor.MonthlyGenerationFailure("state_changed")) {
		// First source selection may commit between the base reader's two
		// metadata snapshots. Retry ONLY that read once; it independently
		// rechecks the same authenticated scope and selection. Never retry
		// submission/source I/O or override an authorization failure here.
		view, err = p.base.ReadMonthly(ctx, gid, version, month)
	}
	if err == nil && view.State == "finalizing" && p.dependencies != nil {
		// A previously selected/drafted target may predate dependency intake.
		// Offer its first preparation request only if final publication has
		// NEVER been attempted. Do not reset a paused/uncertain write journal.
		req, e := p.request(ctx, gid, version, month)
		if e != nil {
			return monitor.MonthlyGenerationView{}, preparationFailure(e)
		}
		ready, e := p.dependencies.ready(ctx, req)
		if e != nil {
			return monitor.MonthlyGenerationView{}, preparationFailure(e)
		}
		if !ready {
			attempted, e := p.dependencies.hasPublicationAttempt(ctx, req)
			if e != nil {
				return monitor.MonthlyGenerationView{}, preparationFailure(e)
			}
			if !attempted {
				view.State = "not_requested"
			}
		}
	}
	if err != nil || (view.State != "not_prepared" && view.State != "not_requested") {
		return view, err
	}
	req, err := p.request(ctx, gid, version, month)
	if err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	var state string
	var continued bool
	err = p.db.QueryRowContext(ctx, `SELECT q.state,EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations c WHERE c.job_key=q.key) FROM portal_monthly_source_preparation q WHERE q.key=?`, req.Key()).Scan(&state, &continued)
	if errors.Is(err, sql.ErrNoRows) {
		if view.State != "not_prepared" {
			return view, nil // Existing selected inputs retain their original policy.
		}
		// Missing inputs are the reason to offer on-demand preparation, not a
		// reason to disable the customer's FIRST click. Only this explicitly
		// installed producer may advertise it, after the same cheap server
		// admission policy used by Submit. It never queues financial collection;
		// registration eligibility may use a bounded metadata lookup.
		if err := p.check(ctx, req); err != nil {
			return monitor.MonthlyGenerationView{}, preparationFailure(err)
		}
		// Admission may have raced a revocation, withdrawal or first selection.
		// Re-read the real base status; never turn a withdrawal into a fresh
		// request or replace a newly running/saved task with an idle prompt.
		view, err = p.base.ReadMonthly(ctx, gid, version, month)
		if err == nil && view.State == "not_prepared" {
			view.State = "not_requested"
		}
		return view, err
	}
	if err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	switch state {
	case "queued":
		view.State = "queued"
		if (continued && !p.continuationEnabled) || (p.capacity != nil && p.capacity.paused.Load()) {
			view.State = "needs_review"
		}
	case "running":
		view.State = "processing"
		if (continued && !p.continuationEnabled) || (p.capacity != nil && p.capacity.paused.Load()) {
			view.State = "needs_review"
		}
	case "failed":
		// A selected input may have committed before a crash prevented the
		// preparation queue's final acknowledgment. Submit already delegates
		// ready selections to the original generator without resuming source
		// work. Do not hide that same first-click path behind an obsolete
		// transient/budget failure. This read changes no counters or evidence.
		if view.State == "not_requested" {
			snapshot, e := failedPreparationSnapshot(ctx, p.db, req)
			if e != nil {
				return monitor.MonthlyGenerationView{}, preparationFailure(e)
			}
			if selectedHandoffFailure(snapshot, p.now()) {
				ready, e := p.selected(ctx, req)
				if e != nil {
					return monitor.MonthlyGenerationView{}, preparationFailure(e)
				}
				if ready {
					// Recheck live ownership/selection/task state, including a
					// concurrent withdrawal. Never manufacture a success flag.
					return p.base.ReadMonthly(ctx, gid, version, month)
				}
			}
		}
		view.State = "needs_review"
	case "submitted": // A job descriptor is never proof of a published bill.
	default:
		return monitor.MonthlyGenerationView{}, preparationFailure(ErrGenerationIntegrity)
	}
	current, err := p.request(ctx, gid, version, month)
	if err != nil {
		return monitor.MonthlyGenerationView{}, preparationFailure(err)
	}
	if current != req {
		return monitor.MonthlyGenerationView{}, preparationFailure(ErrGenerationChanged)
	}
	return view, nil
}

func (p *onDemandPreparation) claim(ctx context.Context) (sourcePreparationJob, error) {
	var job sourcePreparationJob
	// Idle polling must not reserve SQLite's writer. The transaction below
	// rechecks everything; this cheap bounded hint is not claim authority.
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	var due bool
	err := p.db.QueryRowContext(readCtx, `SELECT EXISTS(SELECT 1 FROM portal_monthly_source_preparation q WHERE
(? OR NOT EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations c WHERE c.job_key=q.key)) AND
((q.state='queued' AND q.next_at<=? AND NOT EXISTS(SELECT 1 FROM portal_monthly_source_preparation a WHERE a.state='running' AND (? OR NOT EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations c WHERE c.job_key=a.key)))) OR (q.state='running' AND q.lease_until<=?)))`, p.continuationEnabled, p.now().UnixMilli(), p.continuationEnabled, p.now().UnixMilli()).Scan(&due)
	cancel()
	if err != nil || !due {
		return job, err
	}
	if p.capacity != nil {
		worked, err := p.capacity.step(ctx)
		if worked || err != nil {
			return job, err
		}
	}
	err = monthlytasks.WriteReceiver(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
		now := p.now().UnixMilli()
		// Lease expiry, including a process crash, consumes the same bounded
		// failure budget. Normal resumable steps do not consume retries.
		if err := p.expireContinuations(ctx, tx, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET failures=failures+1,state=CASE WHEN failures>=2 THEN 'failed' ELSE 'queued' END,failure_code='lease_expired',lease='',lease_until=0,next_at=? WHERE state='running' AND lease_until<=? AND NOT EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations c WHERE c.job_key=portal_monthly_source_preparation.key)`, now, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='failed',failure_code='budget_exhausted' WHERE state='queued' AND (steps>=4096 OR created_at<?) AND NOT EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations c WHERE c.job_key=portal_monthly_source_preparation.key)`, now-int64((24*time.Hour)/time.Millisecond)); err != nil {
			return err
		}
		var raw []byte
		var gid int64
		err := tx.QueryRowContext(ctx, `SELECT q.key,q.group_id,q.payload FROM portal_monthly_source_preparation q WHERE q.state='queued' AND q.next_at<=? AND (? OR NOT EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations c WHERE c.job_key=q.key)) AND NOT EXISTS (SELECT 1 FROM portal_monthly_source_preparation a WHERE a.state='running' AND (? OR NOT EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations c WHERE c.job_key=a.key))) ORDER BY q.next_at,q.key LIMIT 1`, now, p.continuationEnabled, p.continuationEnabled).Scan(&job.key, &gid, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		decodeErr := json.Unmarshal(raw, &job.request)
		canonical, _ := json.Marshal(job.request)
		if len(raw) > 1024 || decodeErr != nil || !bytes.Equal(raw, canonical) || job.request.Key() != job.key || job.request.Owner.GroupID != gid {
			// Quarantine this descriptor without starving unrelated organizations.
			_, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='failed',failure_code='invalid_descriptor' WHERE key=?`, job.key)
			job = sourcePreparationJob{}
			return err
		}
		if err := validateReleasedBoundIntents(ctx, tx, job.request, p.now()); err != nil {
			// A partial restore must not run a queue row that was never released
			// against its durable root intent. Isolate it, not other customers.
			_, writeErr := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='failed',failure_code='invalid_descriptor' WHERE key=?`, job.key)
			job = sourcePreparationJob{}
			return writeErr
		}
		if err := checkTokenWorkModePolicy(ctx, tx, job.key, p.accountTokenWork, p.historySourceWork); err != nil {
			_, writeErr := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='failed',failure_code='invalid_descriptor' WHERE key=?`, job.key)
			job = sourcePreparationJob{}
			return writeErr
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		job.lease = hex.EncodeToString(nonce[:])
		continued, e := p.claimContinuation(ctx, tx, &job, now)
		if e != nil {
			err := stopBadContinuation(ctx, tx, job.key, "invalid_descriptor")
			job = sourcePreparationJob{}
			return err
		}
		if continued {
			return nil
		}
		_, err = tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='running',lease=?,lease_until=?,steps=steps+1 WHERE key=? AND state='queued'`, job.lease, now+60000, job.key)
		return err
	})
	return job, err
}

func (p *onDemandPreparation) runOne(ctx context.Context) error {
	job, err := p.claim(ctx)
	if err != nil || job.key == "" {
		return err
	}
	stepCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	dispatch := &tokenWorkDispatch{p: p, job: job}
	if p.accountTokenWork {
		stepCtx = context.WithValue(stepCtx, tokenDispatchKey{}, dispatch)
	}
	err = p.check(stepCtx, job.request)
	if err == nil && job.continuation != "" {
		_, err = p.checkContinuationLease(stepCtx, p.db, job)
	}
	ready := false
	if err == nil {
		ready, err = p.selected(stepCtx, job.request)
	}
	if err == nil && !ready {
		ready, err = p.step(stepCtx, job.request)
		if err == nil && ready {
			ready, err = p.selected(stepCtx, job.request)
			if err == nil && !ready {
				err = ErrGenerationEvidence
			}
		}
	}
	if err == nil {
		err = p.check(stepCtx, job.request)
	}
	if err == nil && ready {
		if job.continuation != "" {
			_, err = p.checkContinuationLease(stepCtx, p.db, job)
		}
	}
	if err == nil && ready {
		_, err = p.base.SubmitMonthly(stepCtx, job.request.Owner.GroupID, job.request.Owner.AuthVersion, job.request.Month)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	} // Leave lease for bounded restart recovery.
	state, failures, delay := "queued", 0, int64(1000)
	stepAdjustment := 0
	code := ""
	if err == nil && ready {
		state = "submitted"
	}
	var pause *sourcePreparationPause
	if errors.As(err, &pause) && pause != nil && pause.until.UnixMilli() > 0 && pause.until.Sub(p.now()) <= 5*time.Minute {
		// Waiting is not failed work or a new source read. The global 24-hour
		// job age still bounds a producer that keeps asking for deferral.
		delay = max(1000, pause.until.UnixMilli()-p.now().UnixMilli())
		if pause.until.After(p.now()) {
			stepAdjustment = -1
		}
	} else if err != nil {
		failures, delay = 1, 30000
		code = PublicUsageGenerationProblem(err).Code
		var public monitor.MonthlyGenerationFailure
		if errors.As(err, &public) {
			// Keep a bounded allowlist, never persist arbitrary producer error text.
			switch public {
			case "temporarily_busy", "temporarily_unavailable", "state_changed", "invalid_request", "not_prepared", "needs_review", "request_cancelled":
				code = string(public)
			}
		}
		if code != "temporarily_busy" && code != "temporarily_unavailable" && !errors.Is(err, context.DeadlineExceeded) {
			state = "failed"
		}
	}
	return monthlytasks.WriteReceiver(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
		if dispatch.receipt != nil && err == nil && !ready {
			if e := applyTokenWork(ctx, tx, dispatch); e != nil {
				return e
			}
			stepAdjustment = -1 // successful originals separately accounted, not erased
		}
		if job.continuation != "" {
			return p.finishContinuation(ctx, tx, job, state, code, failures, stepAdjustment, delay)
		}
		result, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state=CASE WHEN failures+?>=3 THEN 'failed' ELSE ? END,steps=steps+?,failures=failures+?,failure_code=?,next_at=?,lease='',lease_until=0 WHERE key=? AND state='running' AND lease=?`, failures, state, stepAdjustment, failures, code, p.now().UnixMilli()+delay, job.key, job.lease)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err == nil && n != 1 {
			return ErrGenerationChanged
		}
		return err
	})
}

func (p *onDemandPreparation) serve(ctx context.Context) {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for ctx.Err() == nil {
		_ = p.runOne(ctx) // Durable state is authoritative; errors cannot spin.
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-p.wake:
		}
	}
}
