package monthlydraft

// Private, explicit, pre-Start recovery of UNSELECTED preparation tasks only.
// No HTTP route/customer retry bypass, source reads, raw-proof edits or bill
// publication. Root-level hard stops remain outside this policy.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

var ErrPreparationResumeDenied = errors.New("preparation cannot be resumed under transient-failure policy")
var ErrPreparationResumeCooldown = errors.New("preparation resume cooldown has not elapsed")

const preparationResumeLimit = 2

type preparationFailedSnapshot struct {
	Request      SourcePreparationRequest `json:"request"`
	State        string                   `json:"state"`
	Steps        int                      `json:"steps"`
	Failures     int                      `json:"failures"`
	Code         string                   `json:"code"`
	CreatedMS    int64                    `json:"created_unix_ms"`
	NextMS       int64                    `json:"next_unix_ms"`
	Lease        string                   `json:"lease"`
	LeaseUntilMS int64                    `json:"lease_until_unix_ms"`
}

type SourcePreparationResumePlan struct {
	Version       int                      `json:"version"`
	Request       SourcePreparationRequest `json:"request"`
	FailureSHA256 string                   `json:"failure_sha256"`
	Ordinal       int                      `json:"ordinal"`
	ParentSHA256  string                   `json:"parent_sha256"`
	EarliestMS    int64                    `json:"earliest_unix_ms"`
}

type SourcePreparationResumeReceipt struct {
	Version        int                       `json:"version"`
	Ordinal        int                       `json:"ordinal"`
	Failure        preparationFailedSnapshot `json:"failure"`
	ParentSHA256   string                    `json:"parent_sha256"`
	ResumedMS      int64                     `json:"resumed_unix_ms"`
	RecoverySHA256 string                    `json:"recovery_sha256,omitempty"`
}

type SourcePreparationResumeResult struct {
	Applied        bool   `json:"applied"`
	AlreadyApplied bool   `json:"already_applied"`
	ReceiptSHA256  string `json:"receipt_sha256"`
}

func initSourcePreparationResumptions(ctx context.Context, tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_source_resumptions(job_key TEXT NOT NULL CHECK(length(job_key)=64),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 2),failure_sha TEXT NOT NULL CHECK(length(failure_sha)=64),sha256 TEXT NOT NULL CHECK(length(sha256)=64),payload BLOB NOT NULL CHECK(length(payload)<=4096),PRIMARY KEY(job_key,ordinal),UNIQUE(job_key,failure_sha))`,
		`CREATE TRIGGER IF NOT EXISTS source_resumption_no_update BEFORE UPDATE ON portal_monthly_source_resumptions BEGIN SELECT RAISE(ABORT,'source resumptions are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS source_resumption_no_delete BEFORE DELETE ON portal_monthly_source_resumptions BEGIN SELECT RAISE(ABORT,'source resumptions are immutable'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return initBoundPreparationResumptions(ctx, tx)
}

type preparationResumeReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func failedPreparationSnapshot(ctx context.Context, db preparationResumeReader, req SourcePreparationRequest) (preparationFailedSnapshot, error) {
	s := preparationFailedSnapshot{}
	var raw []byte
	var gid int64
	err := db.QueryRowContext(ctx, `SELECT group_id,payload,state,steps,failures,failure_code,created_at,next_at,lease,lease_until FROM portal_monthly_source_preparation WHERE key=?`, req.Key()).Scan(&gid, &raw, &s.State, &s.Steps, &s.Failures, &s.Code, &s.CreatedMS, &s.NextMS, &s.Lease, &s.LeaseUntilMS)
	if err != nil {
		return s, err
	}
	canonical, _ := json.Marshal(req)
	if len(raw) > 1024 || !bytes.Equal(raw, canonical) || gid != req.Owner.GroupID {
		return s, ErrGenerationIntegrity
	}
	s.Request = req
	return s, nil
}

func preparationSnapshotHash(s preparationFailedSnapshot) string {
	raw, _ := json.Marshal(s)
	return generationHash(raw)
}

func eligiblePreparationSnapshot(s preparationFailedSnapshot, now time.Time) error {
	if s.State != "failed" || s.Failures != 3 || s.Steps < 3 || s.Steps >= 4096 || s.CreatedMS <= 0 || s.CreatedMS > now.UnixMilli() || now.UnixMilli()-s.CreatedMS >= int64((24*time.Hour)/time.Millisecond) || s.NextMS < s.CreatedMS || s.NextMS > now.UnixMilli()+30000 || s.Lease != "" || s.LeaseUntilMS != 0 {
		return ErrPreparationResumeDenied
	}
	switch s.Code {
	case "temporarily_busy", "temporarily_unavailable", "lease_expired":
	default:
		return ErrPreparationResumeDenied
	}
	return nil
}

func loadPreparationResumptions(ctx context.Context, db preparationResumeReader, req SourcePreparationRequest, now time.Time) ([]SourcePreparationResumeReceipt, []string, error) {
	rows, err := db.QueryContext(ctx, `SELECT ordinal,failure_sha,sha256,CASE WHEN length(payload)<=4096 THEN payload END FROM portal_monthly_source_resumptions WHERE job_key=? ORDER BY ordinal LIMIT 3`, req.Key())
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	out := []SourcePreparationResumeReceipt{}
	hashes := []string{}
	parent := ""
	for rows.Next() {
		var ordinal int
		var failure, sha string
		var raw []byte
		var r SourcePreparationResumeReceipt
		if err := rows.Scan(&ordinal, &failure, &sha, &raw); err != nil {
			return nil, nil, err
		}
		if len(out) >= preparationResumeLimit || json.Unmarshal(raw, &r) != nil {
			return nil, nil, ErrGenerationIntegrity
		}
		canonical, _ := json.Marshal(r)
		if r.RecoverySHA256 != "" && !generationHex(r.RecoverySHA256, 32) {
			return nil, nil, ErrGenerationIntegrity
		}
		if !bytes.Equal(raw, canonical) || len(raw) == 0 || generationHash(raw) != sha || ordinal != len(out)+1 || r.Version != 1 || r.Ordinal != ordinal || r.Failure.Request != req || preparationSnapshotHash(r.Failure) != failure || r.ParentSHA256 != parent || r.ResumedMS > now.UnixMilli() || r.ResumedMS < r.Failure.NextMS+60000 || eligiblePreparationSnapshot(r.Failure, time.UnixMilli(r.ResumedMS)) != nil {
			return nil, nil, ErrGenerationIntegrity
		}
		if len(out) > 0 {
			previous := out[len(out)-1]
			if r.Failure.CreatedMS != previous.Failure.CreatedMS || r.Failure.Steps < previous.Failure.Steps+3 || r.ResumedMS < previous.ResumedMS {
				return nil, nil, ErrGenerationIntegrity
			}
		}
		out = append(out, r)
		hashes = append(hashes, sha)
		parent = sha
	}
	return out, hashes, rows.Err()
}

func (p *onDemandPreparation) resumeGuard(ctx context.Context, req SourcePreparationRequest) error {
	if err := p.check(ctx, req); err != nil {
		return err
	}
	selected, err := ReadGenerationSelection(ctx, p.db, req.Owner.GroupID, req.Month)
	if err != nil {
		return err
	}
	if selected.Head != "" {
		return ErrPreparationResumeDenied
	}
	if p.dependencies != nil {
		attempted, err := p.dependencies.hasPublicationAttempt(ctx, req)
		if err != nil {
			return err
		}
		if attempted {
			return ErrPreparationResumeDenied
		}
	}
	view, err := p.base.ReadMonthly(ctx, req.Owner.GroupID, req.Owner.AuthVersion, req.Month)
	if err != nil {
		return err
	}
	if view.State != "not_prepared" && view.State != "not_requested" {
		return ErrPreparationResumeDenied
	}
	return nil
}

func (p *onDemandPreparation) previewResume(ctx context.Context, req SourcePreparationRequest) (SourcePreparationResumePlan, error) {
	var empty SourcePreparationResumePlan
	if err := p.resumeGuard(ctx, req); err != nil {
		return empty, err
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	s, err := failedPreparationSnapshot(ctx, tx, req)
	if err != nil {
		return empty, err
	}
	if err := eligiblePreparationSnapshot(s, p.now()); err != nil {
		return empty, err
	}
	receipts, hashes, err := loadPreparationResumptions(ctx, tx, req, p.now())
	if err != nil {
		return empty, err
	}
	if len(receipts) >= preparationResumeLimit {
		return empty, ErrPreparationResumeDenied
	}
	if len(receipts) > 0 {
		last := receipts[len(receipts)-1]
		if s.CreatedMS != last.Failure.CreatedMS || s.Steps < last.Failure.Steps+3 {
			return empty, ErrGenerationIntegrity
		}
	}
	parent := ""
	if len(hashes) > 0 {
		parent = hashes[len(hashes)-1]
	}
	plan := SourcePreparationResumePlan{Version: 1, Request: req, FailureSHA256: preparationSnapshotHash(s), Ordinal: len(receipts) + 1, ParentSHA256: parent, EarliestMS: s.NextMS + 60000}
	if plan.EarliestMS >= s.CreatedMS+int64((24*time.Hour)/time.Millisecond) {
		return empty, ErrPreparationResumeDenied
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	if err := p.check(ctx, req); err != nil {
		return empty, err
	}
	return plan, nil
}

func (p *onDemandPreparation) applyResume(ctx context.Context, plan SourcePreparationResumePlan) (SourcePreparationResumeResult, error) {
	return p.applyResumeWithRecovery(ctx, plan, "")
}

func (p *onDemandPreparation) applyResumeWithRecovery(ctx context.Context, plan SourcePreparationResumePlan, recovery string) (SourcePreparationResumeResult, error) {
	var result SourcePreparationResumeResult
	if recovery != "" && !generationHex(recovery, 32) {
		return result, ErrGenerationInput
	}
	if plan.Version != 1 || plan.Ordinal < 1 || plan.Ordinal > preparationResumeLimit || !generationHex(plan.FailureSHA256, 32) {
		return result, ErrGenerationInput
	}
	if err := p.check(ctx, plan.Request); err != nil {
		return result, err
	}
	// Idempotent acknowledgment never changes a queued/running/published task.
	receipts, hashes, err := loadPreparationResumptions(ctx, p.db, plan.Request, p.now())
	if err != nil {
		return result, err
	}
	if plan.Ordinal <= len(receipts) {
		r := receipts[plan.Ordinal-1]
		if r.RecoverySHA256 != recovery {
			return result, ErrGenerationSelectionConflict
		}
		if preparationSnapshotHash(r.Failure) != plan.FailureSHA256 || r.ParentSHA256 != plan.ParentSHA256 || r.Failure.NextMS+60000 != plan.EarliestMS {
			return result, ErrGenerationSelectionConflict
		}
		current, err := failedPreparationSnapshot(ctx, p.db, plan.Request)
		latest := receipts[len(receipts)-1]
		if err != nil || current.CreatedMS != latest.Failure.CreatedMS || current.Steps < latest.Failure.Steps {
			return result, ErrGenerationIntegrity
		}
		return SourcePreparationResumeResult{Applied: true, AlreadyApplied: true, ReceiptSHA256: hashes[plan.Ordinal-1]}, nil
	}
	expected, err := p.previewResume(ctx, plan.Request)
	if err != nil {
		return result, err
	}
	if expected != plan {
		return result, ErrGenerationSelectionConflict
	}
	if p.now().UnixMilli() < plan.EarliestMS {
		return result, ErrPreparationResumeCooldown
	}
	err = monthlytasks.WriteReceiver(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
		s, err := failedPreparationSnapshot(ctx, tx, plan.Request)
		if err != nil {
			return err
		}
		if preparationSnapshotHash(s) != plan.FailureSHA256 {
			return ErrGenerationSelectionConflict
		}
		if err := eligiblePreparationSnapshot(s, p.now()); err != nil {
			return err
		}
		prior, hashes, err := loadPreparationResumptions(ctx, tx, plan.Request, p.now())
		if err != nil {
			return err
		}
		parent := ""
		if len(hashes) > 0 {
			parent = hashes[len(hashes)-1]
		}
		if len(prior)+1 != plan.Ordinal || parent != plan.ParentSHA256 {
			return ErrGenerationSelectionConflict
		}
		var active, groupActive int
		if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(group_id=?),0) FROM portal_monthly_source_preparation WHERE state IN ('queued','running')`, plan.Request.Owner.GroupID).Scan(&active, &groupActive); err != nil {
			return err
		}
		if active >= 8 {
			return ErrGenerationCapacity
		}
		if groupActive != 0 {
			return ErrGenerationBusy
		}
		r := SourcePreparationResumeReceipt{Version: 1, Ordinal: plan.Ordinal, Failure: s, ParentSHA256: parent, ResumedMS: p.now().UnixMilli(), RecoverySHA256: recovery}
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if len(raw) > 4096 {
			return ErrGenerationCapacity
		}
		sha := generationHash(raw)
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_source_resumptions(job_key,ordinal,failure_sha,sha256,payload) VALUES(?,?,?,?,?)`, plan.Request.Key(), r.Ordinal, plan.FailureSHA256, sha, raw); err != nil {
			return err
		}
		// Only the per-epoch error counter restarts. Original age and cumulative
		// work budget stay fixed; all prior failures are immutable in the receipt.
		updated, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='queued',failures=0,failure_code='',next_at=?,lease='',lease_until=0 WHERE key=? AND state='failed' AND failures=3 AND steps=? AND created_at=?`, r.ResumedMS, plan.Request.Key(), s.Steps, s.CreatedMS)
		if err != nil {
			return err
		}
		n, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrGenerationSelectionConflict
		}
		result.ReceiptSHA256 = sha
		return nil
	})
	if err != nil {
		return SourcePreparationResumeResult{}, err
	}
	result.Applied = true
	if err := p.check(ctx, plan.Request); err != nil {
		return result, err
	}
	return result, nil
}

// PreviewSourcePreparationResume and ResumeSourcePreparation are server-only
// administrative composition APIs, NOT customer HTTP endpoints. A fresh
// disabled captured-source deployment must own/drain the stores before use.
func (d *Deployment) PreviewSourcePreparationResume(parent context.Context, gid, version int64, month string) (SourcePreparationResumePlan, error) {
	var empty SourcePreparationResumePlan
	if parent == nil || d == nil {
		return empty, ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, err := d.preparationResumer()
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	req, err := p.request(ctx, gid, version, month)
	if err != nil {
		return empty, err
	}
	if err := d.unboundPreparationResume(ctx, req); err != nil {
		return empty, err
	}
	return p.previewResume(ctx, req)
}

func (d *Deployment) ResumeSourcePreparation(parent context.Context, plan SourcePreparationResumePlan) (SourcePreparationResumeResult, error) {
	var empty SourcePreparationResumeResult
	if parent == nil || d == nil {
		return empty, ErrGenerationInput
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p, err := d.preparationResumer()
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := d.unboundPreparationResume(ctx, plan.Request); err != nil {
		return empty, err
	}
	return p.applyResume(ctx, plan)
}

func (d *Deployment) preparationResumer() (*onDemandPreparation, error) {
	if d.closed || d.readOnly || d.preparationInstallFailed || d.capturedPreparation == nil || d.runtime == nil {
		return nil, ErrGenerationInput
	}
	r := d.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != "disabled" || r.active != 0 || r.preparation == nil {
		return nil, ErrGenerationBusy
	}
	return r.preparation, nil
}

func (d *Deployment) unboundPreparationResume(ctx context.Context, req SourcePreparationRequest) error {
	var present int
	db := d.capturedPreparation.capture
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='monthly_token_recovery_bindings'`).Scan(&present); err != nil {
		return err
	}
	if present != 0 {
		var bindings int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM monthly_token_recovery_bindings WHERE job_key=?`, req.Key()).Scan(&bindings); err != nil {
			return err
		}
		if bindings != 0 {
			return ErrPreparationResumeDenied
		}
	}
	return nil
}
