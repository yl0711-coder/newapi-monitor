package monthlydraft

// A SINGLE explicit continuation is separate from the exhausted legacy budget.
// Its grant retains the exact old failure. It never repairs/rewrites evidence,
// renews adaptive roots or approves money. No customer endpoint calls these APIs.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

const continuationDispatchLimit = 100000
const continuationCoordinatorLimit = 4096
const continuationLifetime = 24 * time.Hour

type SourcePreparationContinuationPlan struct {
	Version          int                      `json:"version"`
	Request          SourcePreparationRequest `json:"request"`
	FailureSHA256    string                   `json:"failure_sha256"`
	Source           SourcePreparationRequest `json:"source"`
	SourceIdentity   string                   `json:"source_identity"`
	PlanSHA256       string                   `json:"plan_sha256"`
	CheckpointSHA256 string                   `json:"checkpoint_sha256"`
	Observed         int                      `json:"observed"`
	SpoolSHA256      string                   `json:"spool_sha256"`
}

type preparationContinuationGrant struct {
	Version   int                               `json:"version"`
	Plan      SourcePreparationContinuationPlan `json:"plan"`
	Original  preparationFailedSnapshot         `json:"original"`
	StartedMS int64                             `json:"started_unix_ms"`
}

type preparationContinuation struct {
	grant                    preparationContinuationGrant
	sha                      string
	claims, exempt, failures int
	lastLease                string
}

func initPreparationContinuations(ctx context.Context, tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_preparation_continuations(job_key TEXT PRIMARY KEY CHECK(length(job_key)=64),sha256 TEXT NOT NULL CHECK(length(sha256)=64),payload BLOB NOT NULL CHECK(length(payload)<=8192))`,
		`CREATE TABLE IF NOT EXISTS portal_monthly_preparation_continuation_counts(job_key TEXT PRIMARY KEY CHECK(length(job_key)=64),claims INTEGER NOT NULL DEFAULT 0 CHECK(claims BETWEEN 0 AND 100000),exempt INTEGER NOT NULL DEFAULT 0 CHECK(exempt BETWEEN 0 AND claims),failures INTEGER NOT NULL DEFAULT 0 CHECK(failures BETWEEN 0 AND 3),last_lease TEXT NOT NULL DEFAULT '')`,
		`CREATE TRIGGER IF NOT EXISTS continuation_grant_no_update BEFORE UPDATE ON portal_monthly_preparation_continuations BEGIN SELECT RAISE(ABORT,'continuation grants immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS continuation_grant_no_delete BEFORE DELETE ON portal_monthly_preparation_continuations BEGIN SELECT RAISE(ABORT,'continuation history retained'); END`,
		`CREATE TRIGGER IF NOT EXISTS continuation_counts_no_delete BEFORE DELETE ON portal_monthly_preparation_continuation_counts BEGIN SELECT RAISE(ABORT,'continuation counters retained'); END`,
		`CREATE TRIGGER IF NOT EXISTS continuation_counts_monotonic BEFORE UPDATE ON portal_monthly_preparation_continuation_counts WHEN NEW.job_key!=OLD.job_key OR NEW.claims<OLD.claims OR NEW.exempt<OLD.exempt OR NEW.failures<OLD.failures BEGIN SELECT RAISE(ABORT,'continuation counters monotonic'); END`,
		`CREATE TRIGGER IF NOT EXISTS continuation_original_no_reset BEFORE UPDATE ON portal_monthly_source_preparation WHEN EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations WHERE job_key=OLD.key) AND (NEW.key!=OLD.key OR NEW.group_id!=OLD.group_id OR NEW.payload!=OLD.payload OR NEW.steps!=OLD.steps OR NEW.failures!=OLD.failures OR NEW.created_at!=OLD.created_at) BEGIN SELECT RAISE(ABORT,'original continuation budget immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS continuation_original_no_delete BEFORE DELETE ON portal_monthly_source_preparation WHEN EXISTS(SELECT 1 FROM portal_monthly_preparation_continuations WHERE job_key=OLD.key) BEGIN SELECT RAISE(ABORT,'original preparation history retained'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func eligibleContinuationFailure(s preparationFailedSnapshot, now time.Time) bool {
	return s.State == "failed" && s.Code == "budget_exhausted" && s.CreatedMS > 0 && s.CreatedMS <= now.UnixMilli() &&
		s.Steps >= 0 && s.Steps <= 4096 && s.Failures >= 0 && s.Failures < 3 && s.NextMS >= s.CreatedMS && s.NextMS <= now.UnixMilli()+30000 &&
		s.Lease == "" && s.LeaseUntilMS == 0 && (s.Steps == 4096 || now.UnixMilli()-s.CreatedMS >= continuationLifetime.Milliseconds())
}

func readPreparationContinuation(ctx context.Context, db preparationResumeReader, key string) (preparationContinuation, error) {
	var c preparationContinuation
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT g.sha256,CASE WHEN length(g.payload)<=8192 THEN g.payload END,c.claims,c.exempt,c.failures,c.last_lease FROM portal_monthly_preparation_continuations g LEFT JOIN portal_monthly_preparation_continuation_counts c ON c.job_key=g.job_key WHERE g.job_key=?`, key).
		Scan(&c.sha, &raw, &c.claims, &c.exempt, &c.failures, &c.lastLease)
	if err != nil {
		return c, err
	}
	if json.Unmarshal(raw, &c.grant) != nil {
		return c, ErrGenerationIntegrity
	}
	canonical, _ := json.Marshal(c.grant)
	g, p := c.grant, c.grant.Plan
	if !bytes.Equal(canonical, raw) || generationHash(raw) != c.sha || g.Version != 1 || p.Version != 1 || p.Request.Key() != key || g.Original.Request != p.Request || preparationSnapshotHash(g.Original) != p.FailureSHA256 || !eligibleContinuationFailure(g.Original, time.UnixMilli(g.StartedMS)) || p.Source.Owner != p.Request.Owner || p.Source.Month > p.Request.Month || p.Observed < 0 || p.Observed > 20336 || c.claims < 0 || c.claims > continuationDispatchLimit || c.exempt < 0 || c.exempt > c.claims || c.failures < 0 || c.failures+g.Original.Failures > 3 || (c.claims == 0 && c.lastLease != "") || (c.claims > 0 && !generationHex(c.lastLease, 16)) {
		return c, ErrGenerationIntegrity
	}
	for _, h := range []string{p.SourceIdentity, p.PlanSHA256, p.CheckpointSHA256, p.SpoolSHA256} {
		if !generationHex(h, 32) {
			return c, ErrGenerationIntegrity
		}
	}
	var guards int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN ('continuation_grant_no_update','continuation_grant_no_delete','continuation_counts_no_delete','continuation_counts_monotonic','continuation_original_no_reset','continuation_original_no_delete')`).Scan(&guards); err != nil {
		return c, err
	}
	if guards != 6 {
		return c, ErrGenerationIntegrity
	}
	s, err := failedPreparationSnapshot(ctx, db, p.Request)
	if err != nil {
		return c, err
	}
	if s.CreatedMS != g.Original.CreatedMS || s.Steps != g.Original.Steps || s.Failures != g.Original.Failures {
		return c, ErrGenerationIntegrity
	}
	return c, nil
}

func (d *Deployment) EnableSourcePreparationContinuations() error {
	if d == nil {
		return ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, err := d.preparationResumer()
	if err != nil {
		return err
	}
	if !p.accountTokenWork || p.historySourceWork != nil || d.capturedPreparation.spool == nil || d.capturedPreparation.funding == nil {
		return ErrPreparationResumeDenied
	}
	p.continuationEnabled = true
	return nil
}

// A grant is not automatic renewal: the owner must be stopped, opt in, inspect
// a real old checkpoint and apply that EXACT plan once. Default remains off.
func (d *Deployment) PreviewSourcePreparationContinuation(parent context.Context, gid, version int64, month string) (SourcePreparationContinuationPlan, error) {
	var empty SourcePreparationContinuationPlan
	if parent == nil || d == nil {
		return empty, ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, err := d.preparationResumer()
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	req, err := p.request(ctx, gid, version, month)
	if err != nil {
		return empty, err
	}
	return d.previewPreparationContinuation(ctx, p, req)
}

func (d *Deployment) previewPreparationContinuation(ctx context.Context, p *onDemandPreparation, req SourcePreparationRequest) (SourcePreparationContinuationPlan, error) {
	var empty SourcePreparationContinuationPlan
	if !p.continuationEnabled || !p.accountTokenWork || d.capturedPreparation.spool == nil || d.capturedPreparation.funding == nil {
		return empty, ErrPreparationResumeDenied
	}
	if err := p.check(ctx, req); err != nil {
		return empty, err
	}
	if _, err := readPreparationContinuation(ctx, p.db, req.Key()); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return empty, err
		}
		return empty, ErrPreparationResumeDenied
	}
	s, err := failedPreparationSnapshot(ctx, p.db, req)
	if err != nil {
		return empty, err
	}
	if !eligibleContinuationFailure(s, p.now()) {
		return empty, ErrPreparationResumeDenied
	}
	var prior int
	if err := p.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM portal_monthly_source_resumptions WHERE job_key=?)+(SELECT count(*) FROM portal_monthly_bound_resume_intents WHERE job_key=?)+(SELECT count(*) FROM portal_monthly_preparation_continuation_counts WHERE job_key=?)`, req.Key(), req.Key(), req.Key()).Scan(&prior); err != nil {
		return empty, err
	}
	if prior != 0 {
		return empty, ErrPreparationResumeDenied
	}
	ready, err := p.selected(ctx, req)
	if err != nil {
		return empty, err
	}
	if ready {
		return empty, ErrPreparationResumeDenied
	}
	source := req
	if p.dependencies != nil {
		attempted, err := p.dependencies.hasPublicationAttempt(ctx, req)
		if err != nil {
			return empty, err
		}
		if attempted {
			return empty, ErrPreparationResumeDenied
		}
		_, missing, err := p.dependencies.missing(ctx, req)
		if err != nil {
			return empty, err
		}
		if missing == "" {
			return empty, ErrPreparationResumeDenied
		}
		source.Month = missing
	}
	f := d.capturedPreparation.funding
	scope, err := f.scope(ctx, source)
	if err != nil {
		return empty, err
	}
	// First policy deliberately excludes any old adaptive-root binding. It
	// must never renew stopped/expired roots by only extending coordinator age.
	var table, bindings int
	if err := f.capture.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='monthly_token_recovery_bindings'`).Scan(&table); err != nil {
		return empty, err
	}
	if table != 0 {
		if err := f.capture.QueryRowContext(ctx, `SELECT count(*) FROM monthly_token_recovery_bindings WHERE job_key=?`, source.Key()).Scan(&bindings); err != nil {
			return empty, err
		}
	}
	if bindings != 0 {
		return empty, ErrPreparationResumeDenied
	}
	cp, err := monthlysource.ReadTokenWorkCheckpoint(ctx, f.capture, source.Key(), scope)
	if err != nil {
		return empty, err
	}
	stamp, err := d.capturedPreparation.spool.continuationCapacityStamp(ctx, source)
	if err != nil {
		return empty, err
	}
	return SourcePreparationContinuationPlan{1, req, preparationSnapshotHash(s), source, f.sourceIdentity, cp.Schedule().PlanSHA256, cp.Fingerprint(), cp.Observed(), stamp}, nil
}

func (d *Deployment) ContinueSourcePreparation(parent context.Context, plan SourcePreparationContinuationPlan) (string, error) {
	if parent == nil || d == nil {
		return "", ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, err := d.preparationResumer()
	if err != nil {
		return "", err
	}
	if !p.continuationEnabled {
		return "", ErrPreparationResumeDenied
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	if err := p.check(ctx, plan.Request); err != nil {
		return "", err
	}
	if prior, err := readPreparationContinuation(ctx, p.db, plan.Request.Key()); err == nil {
		if prior.grant.Plan != plan {
			return "", ErrGenerationSelectionConflict
		}
		return prior.sha, nil // acknowledgment only; never requeue/reset
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	want, err := d.previewPreparationContinuation(ctx, p, plan.Request)
	if err != nil {
		return "", err
	}
	if want != plan {
		return "", ErrGenerationSelectionConflict
	}
	var sha string
	err = monthlytasks.WriteReceiver(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
		s, err := failedPreparationSnapshot(ctx, tx, plan.Request)
		if err != nil {
			return err
		}
		if preparationSnapshotHash(s) != plan.FailureSHA256 || !eligibleContinuationFailure(s, p.now()) {
			return ErrGenerationSelectionConflict
		}
		if err := checkTokenWorkMode(ctx, tx, plan.Request.Key(), true); err != nil {
			return err
		}
		var active, total int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_source_preparation WHERE state IN ('queued','running')`).Scan(&active); err != nil {
			return err
		}
		if active >= 8 {
			return ErrGenerationCapacity
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_source_preparation WHERE group_id=? AND state IN ('queued','running')`, plan.Request.Owner.GroupID).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return ErrGenerationBusy
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_preparation_continuations`).Scan(&total); err != nil {
			return err
		}
		if total >= 256 {
			return ErrGenerationCapacity
		}
		g := preparationContinuationGrant{1, plan, s, p.now().UnixMilli()}
		raw, _ := json.Marshal(g)
		if len(raw) > 8192 {
			return ErrGenerationCapacity
		}
		sha = generationHash(raw)
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_preparation_continuations VALUES(?,?,?)`, plan.Request.Key(), sha, raw); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_preparation_continuation_counts(job_key) VALUES(?)`, plan.Request.Key()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='queued',failure_code='',next_at=?,lease='',lease_until=0 WHERE key=?`, g.StartedMS, plan.Request.Key())
		return err
	})
	if err != nil {
		return "", err
	}
	return sha, nil
}
