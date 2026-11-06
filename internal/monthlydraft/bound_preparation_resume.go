package monthlydraft

// Explicit stopped-owner coordination, NOT a cross-SQLite transaction. Intent
// is durable before source epochs advance. Queue release is the final commit.
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

const boundResumeLimit = 256 << 10
const boundResumePool = 8 << 20

type BoundPreparationResumePlan struct {
	Version    int                                          `json:"version"`
	Queue      SourcePreparationResumePlan                  `json:"queue"`
	Capture    monthlysource.RecoveringTokenMonthResumePlan `json:"capture"`
	EarliestMS int64                                        `json:"earliest_unix_ms"`
}

func initBoundPreparationResumptions(ctx context.Context, tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_bound_resume_intents(job_key TEXT NOT NULL CHECK(length(job_key)=64),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 2),sha256 TEXT NOT NULL CHECK(length(sha256)=64),payload BLOB NOT NULL CHECK(length(payload)<=262144),PRIMARY KEY(job_key,ordinal))`,
		`CREATE TRIGGER IF NOT EXISTS bound_resume_no_update BEFORE UPDATE ON portal_monthly_bound_resume_intents BEGIN SELECT RAISE(ABORT,'bound recovery intents are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS bound_resume_no_delete BEFORE DELETE ON portal_monthly_bound_resume_intents BEGIN SELECT RAISE(ABORT,'bound recovery intents are immutable'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func boundResumeBytes(p BoundPreparationResumePlan) ([]byte, error) {
	if p.Version != 1 || p.Queue.Version != 1 || p.Queue.Ordinal < 1 || p.Queue.Ordinal > 2 || p.Capture.Version != 1 || p.Capture.JobKey != p.Queue.Request.Key() || p.Capture.Scope.GroupID != p.Queue.Request.Owner.GroupID || p.Capture.Scope.AuthVersion != p.Queue.Request.Owner.AuthVersion || p.Capture.Scope.MemberFingerprint != p.Queue.Request.Owner.MemberFingerprint || p.Capture.Scope.MemberManifest != p.Queue.Request.Owner.MemberManifest || p.Capture.Scope.Funding.Month != p.Queue.Request.Month || len(p.Capture.Roots) == 0 || len(p.Capture.Roots) > 128 {
		return nil, ErrGenerationInput
	}
	earliest := p.Queue.EarliestMS
	for _, r := range p.Capture.Roots {
		earliest = max(earliest, r.EarliestMS)
	}
	for _, r := range p.Capture.Roots {
		if earliest >= r.RootCreatedMS+int64((24*time.Hour)/time.Millisecond) {
			return nil, ErrPreparationResumeDenied
		}
	}
	if earliest != p.EarliestMS {
		return nil, ErrGenerationIntegrity
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if len(raw) > boundResumeLimit {
		return nil, ErrGenerationCapacity
	}
	return raw, nil
}

func readBoundResumeIntent(ctx context.Context, db preparationResumeReader, req SourcePreparationRequest, ordinal int) (BoundPreparationResumePlan, string, error) {
	var p BoundPreparationResumePlan
	var raw []byte
	var sha string
	var triggers int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN ('bound_resume_no_update','bound_resume_no_delete')`).Scan(&triggers); err != nil {
		return p, "", err
	}
	if triggers != 2 {
		return p, "", ErrGenerationIntegrity
	}
	if err := db.QueryRowContext(ctx, `SELECT sha256,CASE WHEN length(payload)<=262144 THEN payload END FROM portal_monthly_bound_resume_intents WHERE job_key=? AND ordinal=?`, req.Key(), ordinal).Scan(&sha, &raw); err != nil {
		return p, "", err
	}
	if json.Unmarshal(raw, &p) != nil {
		return p, "", ErrGenerationIntegrity
	}
	canonical, err := boundResumeBytes(p)
	if err != nil || !bytes.Equal(canonical, raw) || generationHash(raw) != sha || p.Queue.Request != req || p.Queue.Ordinal != ordinal {
		return p, "", ErrGenerationIntegrity
	}
	return p, sha, nil
}

func validateReleasedBoundIntents(ctx context.Context, db preparationResumeReader, req SourcePreparationRequest, now time.Time) error {
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_bound_resume_intents WHERE job_key=?`, req.Key()).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		var receipts int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_source_resumptions WHERE job_key=?`, req.Key()).Scan(&receipts); err != nil {
			return err
		}
		if receipts == 0 {
			return nil
		}
	}
	if n > 2 {
		return ErrGenerationIntegrity
	}
	receipts, _, err := loadPreparationResumptions(ctx, db, req, now)
	if err != nil {
		return err
	}
	// Check the reverse direction too: a restored release receipt may refer
	// to an intent that is now missing. An empty intent table is NOT proof
	// that this task used the older, queue-only recovery path.
	for _, receipt := range receipts {
		if receipt.RecoverySHA256 == "" {
			continue
		}
		_, sha, err := readBoundResumeIntent(ctx, db, req, receipt.Ordinal)
		if err != nil || sha != receipt.RecoverySHA256 {
			return ErrGenerationIntegrity
		}
	}
	for i := 1; i <= 2; i++ {
		_, sha, err := readBoundResumeIntent(ctx, db, req, i)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if len(receipts) < i || receipts[i-1].RecoverySHA256 != sha {
			return ErrGenerationIntegrity
		}
	}
	return nil
}

func (d *Deployment) boundPreparationResumer() (*onDemandPreparation, *FundingSourcePreparation, error) {
	p, err := d.preparationResumer()
	if err != nil {
		return nil, nil, err
	}
	f := d.capturedPreparation.funding
	if f == nil || !f.tokenRecovery {
		return nil, nil, ErrPreparationResumeDenied
	}
	return p, f, nil
}

// Preview returns an existing interrupted intent verbatim, rather than
// constructing a new epoch from partly advanced source state. No source I/O.
func (d *Deployment) PreviewBoundSourcePreparationResume(parent context.Context, gid, version int64, month string) (BoundPreparationResumePlan, error) {
	var empty BoundPreparationResumePlan
	if parent == nil || d == nil {
		return empty, ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, f, err := d.boundPreparationResumer()
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	req, err := p.request(ctx, gid, version, month)
	if err != nil {
		return empty, err
	}
	q, err := p.previewResume(ctx, req)
	if err != nil {
		return empty, err
	}
	scope, err := f.scope(ctx, req)
	if err != nil {
		return empty, err
	}
	if old, _, err := readBoundResumeIntent(ctx, p.db, req, q.Ordinal); err == nil {
		if old.Queue != q {
			return empty, ErrGenerationSelectionConflict
		}
		actual, _ := json.Marshal(scope)
		pinned, _ := json.Marshal(old.Capture.Scope)
		if !bytes.Equal(actual, pinned) {
			return empty, ErrGenerationChanged
		}
		if err := monthlysource.ValidateRecoveringTokenMonthResumption(ctx, f.capture, old.Capture); err != nil {
			return empty, err
		}
		return old, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return empty, err
	}
	source, err := monthlysource.PreviewRecoveringTokenMonthResumption(ctx, f.capture, req.Key(), scope)
	if err != nil {
		return empty, err
	}
	plan := BoundPreparationResumePlan{1, q, source, q.EarliestMS}
	for _, r := range source.Roots {
		plan.EarliestMS = max(plan.EarliestMS, r.EarliestMS)
	}
	if _, err := boundResumeBytes(plan); err != nil {
		return empty, err
	}
	return plan, p.check(ctx, req)
}

// Nothing queues until every root is acknowledged against the durable intent.
// Retrying this operation may acknowledge partial commits, never reset them.
func (d *Deployment) ResumeBoundSourcePreparation(parent context.Context, plan BoundPreparationResumePlan) (SourcePreparationResumeResult, error) {
	var empty SourcePreparationResumeResult
	if parent == nil || d == nil {
		return empty, ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, f, err := d.boundPreparationResumer()
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	raw, err := boundResumeBytes(plan)
	if err != nil {
		return empty, err
	}
	sha := generationHash(raw)
	if err := p.check(ctx, plan.Queue.Request); err != nil {
		return empty, err
	}
	scope, err := f.scope(ctx, plan.Queue.Request)
	if err != nil {
		return empty, err
	}
	a, _ := json.Marshal(scope)
	b, _ := json.Marshal(plan.Capture.Scope)
	if !bytes.Equal(a, b) {
		return empty, ErrGenerationChanged
	}
	_, existing, readErr := readBoundResumeIntent(ctx, p.db, plan.Queue.Request, plan.Queue.Ordinal)
	if readErr != nil && !errors.Is(readErr, sql.ErrNoRows) {
		return empty, readErr
	}
	if readErr == nil && existing != sha {
		return empty, ErrGenerationSelectionConflict
	}
	receipts, _, err := loadPreparationResumptions(ctx, p.db, plan.Queue.Request, p.now())
	if err != nil {
		return empty, err
	}
	if plan.Queue.Ordinal <= len(receipts) {
		if readErr != nil || receipts[plan.Queue.Ordinal-1].RecoverySHA256 != sha {
			return empty, ErrGenerationIntegrity
		}
		return p.applyResumeWithRecovery(ctx, plan.Queue, sha)
	}
	expected, err := p.previewResume(ctx, plan.Queue.Request)
	if err != nil {
		return empty, err
	}
	if expected != plan.Queue {
		return empty, ErrGenerationSelectionConflict
	}
	if p.now().UnixMilli() < plan.EarliestMS {
		return empty, ErrPreparationResumeCooldown
	}
	if err := monthlysource.ValidateRecoveringTokenMonthResumption(ctx, f.capture, plan.Capture); err != nil {
		return empty, err
	}
	if readErr != nil {
		if err := monthlytasks.WriteReceiver(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
			s, err := failedPreparationSnapshot(ctx, tx, plan.Queue.Request)
			if err != nil {
				return err
			}
			if preparationSnapshotHash(s) != plan.Queue.FailureSHA256 {
				return ErrGenerationSelectionConflict
			}
			if err := eligiblePreparationSnapshot(s, p.now()); err != nil {
				return err
			}
			var used int64
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(sum(length(payload)),0) FROM portal_monthly_bound_resume_intents`).Scan(&used); err != nil {
				return err
			}
			if used > boundResumePool-int64(len(raw)) {
				return ErrGenerationCapacity
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_bound_resume_intents(job_key,ordinal,sha256,payload) VALUES(?,?,?,?)`, plan.Queue.Request.Key(), plan.Queue.Ordinal, sha, raw)
			return err
		}); err != nil {
			return empty, err
		}
	}
	if err := p.resumeGuard(ctx, plan.Queue.Request); err != nil {
		return empty, err
	}
	if err := monthlysource.ApplyRecoveringTokenMonthResumption(ctx, f.capture, plan.Capture); err != nil {
		return empty, err
	}
	if err := p.resumeGuard(ctx, plan.Queue.Request); err != nil {
		return empty, err
	}
	return p.applyResumeWithRecovery(ctx, plan.Queue, sha)
}
