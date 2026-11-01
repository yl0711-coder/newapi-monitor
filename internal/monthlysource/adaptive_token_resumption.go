package monthlysource

// Server-owned resumption of exhausted CONNECTION failures only. This is not
// a customer action or an approval to use changed money. Old faults/originals
// stay immutable; a new bounded epoch is appended, never reset in place.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"
)

var ErrAdaptiveTokenResumeDenied = errors.New("adaptive token stop is not resumable under connection-failure policy")
var ErrAdaptiveTokenResumeCooldown = errors.New("adaptive token resumption cooldown has not elapsed")
var ErrAdaptiveTokenResumeChanged = errors.New("adaptive token resumption snapshot changed")

const adaptiveTokenResumeLimit = 2
const adaptiveTokenResumeBytes = 32768

type adaptiveTokenFault struct {
	Ordinal  int    `json:"ordinal"`
	Code     string `json:"code"`
	AtMS     int64  `json:"at_unix_ms"`
	Terminal bool   `json:"terminal"`
}
type adaptiveTokenEvidenceRef struct {
	From        int64  `json:"from"`
	To          int64  `json:"to"`
	Phase       int    `json:"phase"`
	SHA256      string `json:"sha256"`
	UsageID     string `json:"usage_id"`
	StartedMS   int64  `json:"started_unix_ms"`
	CompletedMS int64  `json:"completed_unix_ms"`
}
type AdaptiveTokenResumePlan struct {
	Version        int                `json:"version"`
	JobKey         string             `json:"job_key"`
	Request        UsageWindowRequest `json:"request"`
	RootID         string             `json:"root_id"`
	RootCreatedMS  int64              `json:"root_created_unix_ms"`
	Ordinal        int                `json:"ordinal"`
	FailureSHA256  string             `json:"failure_sha256"`
	EvidenceSHA256 string             `json:"evidence_sha256"`
	ParentSHA256   string             `json:"parent_sha256"`
	EarliestMS     int64              `json:"earliest_unix_ms"`
}
type adaptiveTokenResumeReceipt struct {
	Plan      AdaptiveTokenResumePlan    `json:"plan"`
	Faults    []adaptiveTokenFault       `json:"faults"`
	Evidence  []adaptiveTokenEvidenceRef `json:"evidence"`
	ResumedMS int64                      `json:"resumed_unix_ms"`
}
type AdaptiveTokenResumeResult struct {
	Applied        bool   `json:"applied"`
	AlreadyApplied bool   `json:"already_applied"`
	ReceiptSHA256  string `json:"receipt_sha256"`
}

// Explicit OFFLINE initialization of an existing separate adaptive capture DB.
// Does not create a missing base store, source connection, runtime or task.
func InitAdaptiveTokenResumptionStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrAdaptiveTokenIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	var version string
	if err := db.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&version); err != nil {
		return err
	}
	var bases int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('monthly_adaptive_token_roots','monthly_adaptive_token_observations','monthly_adaptive_token_splits','monthly_adaptive_token_faults')`).Scan(&bases); err != nil {
		return err
	}
	if bases != 4 {
		return ErrAdaptiveTokenIntegrity
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS monthly_adaptive_token_resumptions(root_id TEXT NOT NULL CHECK(length(root_id)=64),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 2),failure_sha TEXT NOT NULL CHECK(length(failure_sha)=64),sha256 TEXT NOT NULL CHECK(length(sha256)=64),payload BLOB NOT NULL CHECK(length(payload)<=32768),PRIMARY KEY(root_id,ordinal),UNIQUE(root_id,failure_sha))`,
		`CREATE TABLE IF NOT EXISTS monthly_adaptive_token_resume_faults(root_id TEXT NOT NULL CHECK(length(root_id)=64),epoch INTEGER NOT NULL CHECK(epoch BETWEEN 1 AND 2),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 2),code TEXT NOT NULL,at_ms INTEGER NOT NULL,terminal INTEGER NOT NULL CHECK(terminal IN (0,1)),PRIMARY KEY(root_id,epoch,ordinal))`,
		`CREATE TRIGGER IF NOT EXISTS adaptive_resumption_no_update BEFORE UPDATE ON monthly_adaptive_token_resumptions BEGIN SELECT RAISE(ABORT,'adaptive resumptions are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS adaptive_resumption_no_delete BEFORE DELETE ON monthly_adaptive_token_resumptions BEGIN SELECT RAISE(ABORT,'adaptive resumptions are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS adaptive_resume_fault_no_update BEFORE UPDATE ON monthly_adaptive_token_resume_faults BEGIN SELECT RAISE(ABORT,'adaptive epoch faults are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS adaptive_resume_fault_no_delete BEFORE DELETE ON monthly_adaptive_token_resume_faults BEGIN SELECT RAISE(ABORT,'adaptive epoch faults are immutable'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func adaptiveTokenResumeSchema(ctx context.Context, db walletRowReader) (bool, error) {
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('monthly_adaptive_token_resumptions','monthly_adaptive_token_resume_faults')`).Scan(&tables); err != nil {
		return false, err
	}
	if tables == 0 {
		return false, nil
	}
	if tables != 2 {
		return false, ErrAdaptiveTokenIntegrity
	}
	var triggers int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN ('adaptive_resumption_no_update','adaptive_resumption_no_delete','adaptive_resume_fault_no_update','adaptive_resume_fault_no_delete')`).Scan(&triggers); err != nil {
		return false, err
	}
	if triggers != 4 {
		return false, ErrAdaptiveTokenIntegrity
	}
	return true, nil
}

func readAdaptiveTokenEpochFaults(ctx context.Context, db adaptiveTokenReader, id string, epoch int, now time.Time) ([]adaptiveTokenFault, error) {
	q := `SELECT ordinal,code,at_ms,terminal FROM monthly_adaptive_token_faults WHERE root_id=? ORDER BY ordinal LIMIT 4`
	args := []any{id}
	if epoch > 0 {
		q = `SELECT ordinal,code,at_ms,terminal FROM monthly_adaptive_token_resume_faults WHERE root_id=? AND epoch=? ORDER BY ordinal LIMIT 4`
		args = append(args, epoch)
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []adaptiveTokenFault{}
	for rows.Next() {
		var f adaptiveTokenFault
		var terminal int
		if err := rows.Scan(&f.Ordinal, &f.Code, &f.AtMS, &terminal); err != nil {
			return nil, err
		}
		if f.Ordinal != len(out) || len(out) >= 3 || f.AtMS <= 0 || f.AtMS > now.UnixMilli() || (terminal != 0 && terminal != 1) || !adaptiveTokenFaultCode(f.Code) {
			return nil, ErrAdaptiveTokenIntegrity
		}
		f.Terminal = terminal == 1
		out = append(out, f)
	}
	return out, rows.Err()
}

func adaptiveEvidenceRefs(s adaptiveTokenState) []adaptiveTokenEvidenceRef {
	out := make([]adaptiveTokenEvidenceRef, 0, len(s.records))
	for key, r := range s.records {
		out = append(out, adaptiveTokenEvidenceRef{key.from, key.to, key.phase, r.sha, r.usage, r.started, r.completed})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		return a.Phase < b.Phase
	})
	return out
}
func adaptiveEvidenceDigest(refs []adaptiveTokenEvidenceRef) string {
	raw, _ := json.Marshal(refs)
	return digest(raw)
}
func adaptiveResumeFailureHash(root string, epoch int, created int64, faults []adaptiveTokenFault, evidence string) string {
	raw, _ := json.Marshal(struct {
		Root     string               `json:"root_id"`
		Epoch    int                  `json:"epoch"`
		Created  int64                `json:"created_unix_ms"`
		Faults   []adaptiveTokenFault `json:"faults"`
		Evidence string               `json:"evidence_sha256"`
	}{root, epoch, created, faults, evidence})
	return digest(raw)
}

func resumableAdaptiveFaults(faults []adaptiveTokenFault, created int64, now time.Time) (int64, error) {
	if len(faults) != 3 || created <= 0 || created > now.UnixMilli() || now.UnixMilli()-created >= int64((24*time.Hour)/time.Millisecond) {
		return 0, ErrAdaptiveTokenResumeDenied
	}
	last := created
	for i, f := range faults {
		if f.Ordinal != i || f.Terminal || f.AtMS < last || f.AtMS > now.UnixMilli() || (f.Code != "retryable_transport" && f.Code != "retryable_source") {
			return 0, ErrAdaptiveTokenResumeDenied
		}
		last = f.AtMS
	}
	earliest := last + 90000
	if earliest >= created+int64((24*time.Hour)/time.Millisecond) {
		return 0, ErrAdaptiveTokenResumeDenied
	}
	return earliest, nil
}

func adaptiveEvidenceSubset(refs []adaptiveTokenEvidenceRef, s adaptiveTokenState) bool {
	if refs == nil || len(refs) > 62 {
		return false
	}
	last := adaptiveTokenSlot{}
	for i, r := range refs {
		key := adaptiveTokenSlot{adaptiveTokenNode{r.From, r.To}, r.Phase}
		if i > 0 && (key.from < last.from || key.from == last.from && (key.to < last.to || key.to == last.to && key.phase <= last.phase)) {
			return false
		}
		if got, ok := s.records[key]; !ok || got != (adaptiveTokenRecord{r.SHA256, r.UsageID, r.StartedMS, r.CompletedMS}) {
			return false
		}
		last = key
	}
	return true
}

func loadAdaptiveTokenFaultCursor(ctx context.Context, db adaptiveTokenReader, s *adaptiveTokenState, now time.Time) error {
	legacy, err := readAdaptiveTokenEpochFaults(ctx, db, s.id, 0, now)
	if err != nil {
		return err
	}
	s.faults = legacy
	enabled, err := adaptiveTokenResumeSchema(ctx, db)
	if err != nil {
		return err
	}
	s.resumeEnabled = enabled
	if !enabled {
		return nil
	}
	rows, err := db.QueryContext(ctx, `SELECT ordinal,failure_sha,sha256,CASE WHEN length(payload)<=32768 THEN payload END FROM monthly_adaptive_token_resumptions WHERE root_id=? ORDER BY ordinal LIMIT 3`, s.id)
	if err != nil {
		return err
	}
	type saved struct {
		ordinal      int
		failure, sha string
		raw          []byte
	}
	savedRows := []saved{}
	for rows.Next() {
		var r saved
		if err := rows.Scan(&r.ordinal, &r.failure, &r.sha, &r.raw); err != nil {
			_ = rows.Close()
			return err
		}
		savedRows = append(savedRows, r)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, row := range savedRows {
		var receipt adaptiveTokenResumeReceipt
		if row.ordinal != s.epoch+1 || row.ordinal > adaptiveTokenResumeLimit || len(row.raw) == 0 || len(row.raw) > adaptiveTokenResumeBytes || json.Unmarshal(row.raw, &receipt) != nil {
			return ErrAdaptiveTokenIntegrity
		}
		p := receipt.Plan
		canonical, _ := json.Marshal(receipt)
		earliest, err := resumableAdaptiveFaults(s.faults, s.created, time.UnixMilli(receipt.ResumedMS))
		if err != nil || !bytes.Equal(canonical, row.raw) || digest(row.raw) != row.sha || p.Version != 1 || p.Ordinal != row.ordinal || p.RootID != s.id || p.JobKey != s.root.JobKey || p.Request != s.root.Request || p.RootCreatedMS != s.created || p.ParentSHA256 != s.resumeHead || p.FailureSHA256 != row.failure || p.EvidenceSHA256 != adaptiveEvidenceDigest(receipt.Evidence) || p.FailureSHA256 != adaptiveResumeFailureHash(s.id, s.epoch, s.created, s.faults, p.EvidenceSHA256) || p.EarliestMS != earliest || receipt.ResumedMS < earliest || receipt.ResumedMS > now.UnixMilli() || !reflect.DeepEqual(receipt.Faults, s.faults) || !adaptiveEvidenceSubset(receipt.Evidence, *s) {
			return ErrAdaptiveTokenIntegrity
		}
		s.epoch, s.resumeHead = row.ordinal, row.sha
		s.faults, err = readAdaptiveTokenEpochFaults(ctx, db, s.id, s.epoch, now)
		if err != nil {
			return err
		}
		for _, f := range s.faults {
			if f.AtMS < receipt.ResumedMS {
				return ErrAdaptiveTokenIntegrity
			}
		}
	}
	var orphan int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM monthly_adaptive_token_resume_faults WHERE root_id=? AND epoch>?`, s.id, s.epoch).Scan(&orphan); err != nil {
		return err
	}
	if orphan != 0 {
		return ErrAdaptiveTokenIntegrity
	}
	return nil
}

func verifyAdaptiveResumptionOriginals(ctx context.Context, db walletRowReader, s adaptiveTokenState) error {
	for _, ref := range adaptiveEvidenceRefs(s) {
		r := s.root.Request
		r.From, r.To = ref.From, ref.To
		o, err := loadAdaptiveTokenRecord(ctx, db, s, r, ref.Phase)
		if err != nil {
			return err
		}
		if ref.Phase == 1 {
			older, err := loadAdaptiveTokenRecord(ctx, db, s, r, 0)
			if err != nil || !TokenUsageWindowsMatch(older, o) {
				return ErrAdaptiveTokenIntegrity
			}
		}
	}
	return ctx.Err()
}

func previewAdaptiveTokenResume(ctx context.Context, db adaptiveTokenReader, job string, request UsageWindowRequest, now time.Time) (AdaptiveTokenResumePlan, error) {
	var empty AdaptiveTokenResumePlan
	s, err := loadAdaptiveTokenStateMode(ctx, db, job, request, now, true)
	if err != nil {
		return empty, err
	}
	if !s.resumeEnabled {
		return empty, ErrAdaptiveTokenResumeDenied
	}
	if s.epoch >= adaptiveTokenResumeLimit {
		return empty, ErrAdaptiveTokenResumeDenied
	}
	earliest, err := resumableAdaptiveFaults(s.faults, s.created, now)
	if err != nil {
		return empty, err
	}
	p, _, phase := adaptiveTokenProgress(s, now)
	if p.Complete || phase == -2 {
		return empty, ErrAdaptiveTokenResumeDenied
	}
	if err := verifyAdaptiveResumptionOriginals(ctx, db, s); err != nil {
		return empty, err
	}
	evidence := adaptiveEvidenceDigest(adaptiveEvidenceRefs(s))
	return AdaptiveTokenResumePlan{1, job, request, s.id, s.created, s.epoch + 1, adaptiveResumeFailureHash(s.id, s.epoch, s.created, s.faults, evidence), evidence, s.resumeHead, earliest}, nil
}

// Preview is read-only, including full original/projection verification. It
// opens no source handle, changes no fault and does not queue any month.
func PreviewAdaptiveTokenResumption(parent context.Context, db *sql.DB, job string, request UsageWindowRequest) (AdaptiveTokenResumePlan, error) {
	if parent == nil || db == nil {
		return AdaptiveTokenResumePlan{}, ErrAdaptiveTokenIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AdaptiveTokenResumePlan{}, err
	}
	defer func() { _ = tx.Rollback() }()
	p, err := previewAdaptiveTokenResume(ctx, tx, job, request, time.Now())
	if err != nil {
		return AdaptiveTokenResumePlan{}, err
	}
	if err := tx.Commit(); err != nil {
		return AdaptiveTokenResumePlan{}, err
	}
	return p, nil
}

// Apply must be used by a trusted stopped owner. It grants no queue retry or
// bill authority. Queue/source coordination is deliberately a separate stage.
func ApplyAdaptiveTokenResumption(parent context.Context, db *sql.DB, plan AdaptiveTokenResumePlan) (AdaptiveTokenResumeResult, error) {
	return applyAdaptiveTokenResume(parent, db, plan, time.Now())
}

func applyAdaptiveTokenResume(parent context.Context, db *sql.DB, plan AdaptiveTokenResumePlan, now time.Time) (AdaptiveTokenResumeResult, error) {
	var result AdaptiveTokenResumeResult
	if parent == nil || db == nil || plan.Version != 1 || plan.Ordinal < 1 || plan.Ordinal > adaptiveTokenResumeLimit || !validDigest(plan.FailureSHA256) {
		return result, ErrAdaptiveTokenIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	s, err := loadAdaptiveTokenStateMode(ctx, tx, plan.JobKey, plan.Request, now, true)
	if err != nil {
		return result, err
	}
	if plan.Ordinal <= s.epoch {
		var raw []byte
		var sha string
		if err := tx.QueryRowContext(ctx, `SELECT sha256,payload FROM monthly_adaptive_token_resumptions WHERE root_id=? AND ordinal=?`, s.id, plan.Ordinal).Scan(&sha, &raw); err != nil {
			return result, err
		}
		var receipt adaptiveTokenResumeReceipt
		if json.Unmarshal(raw, &receipt) != nil || receipt.Plan != plan {
			return result, ErrAdaptiveTokenResumeChanged
		}
		if err := verifyAdaptiveResumptionOriginals(ctx, tx, s); err != nil {
			return result, err
		}
		if err := tx.Commit(); err != nil {
			return result, err
		}
		return AdaptiveTokenResumeResult{true, true, sha}, nil
	}
	expected, err := previewAdaptiveTokenResume(ctx, tx, plan.JobKey, plan.Request, now)
	if err != nil {
		return result, err
	}
	if expected != plan {
		return result, ErrAdaptiveTokenResumeChanged
	}
	if now.UnixMilli() < plan.EarliestMS {
		return result, ErrAdaptiveTokenResumeCooldown
	}
	r := adaptiveTokenResumeReceipt{plan, s.faults, adaptiveEvidenceRefs(s), now.UnixMilli()}
	raw, err := json.Marshal(r)
	if err != nil {
		return result, err
	}
	if len(raw) > adaptiveTokenResumeBytes {
		return result, ErrAdaptiveTokenBudget
	}
	var monthly int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='monthly_token_recovery_bindings'`).Scan(&monthly); err != nil {
		return result, err
	}
	used, err := adaptiveTokenRetainedBytes(ctx, tx, monthly != 0)
	if err != nil {
		return result, err
	}
	if used > adaptiveTokenBytes-int64(len(raw)) {
		return result, ErrAdaptiveTokenBudget
	}
	sha := digest(raw)
	if _, err := tx.ExecContext(ctx, `INSERT INTO monthly_adaptive_token_resumptions(root_id,ordinal,failure_sha,sha256,payload) VALUES(?,?,?,?,?)`, s.id, plan.Ordinal, plan.FailureSHA256, sha, raw); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return AdaptiveTokenResumeResult{Applied: true, ReceiptSHA256: sha}, nil
}
