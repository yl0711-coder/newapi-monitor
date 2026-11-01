package monthlydraft

// Successful original token observations have a separate, source-plan-derived
// bounded counter. The 4096 coordinator/error budget is NOT reset per segment.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type tokenDispatchKey struct{}
type tokenWorkDispatch struct {
	p       *onDemandPreparation
	job     sourcePreparationJob
	receipt *tokenWorkReceipt
}
type tokenWorkReceipt struct {
	request       SourcePreparationRequest
	before, after monthlysource.TokenWorkCheckpoint
	history       *tokenHistoryWorkScope
}
type tokenWorkGrant struct {
	Version         int                                `json:"version"`
	Parent          SourcePreparationRequest           `json:"parent"`
	Source          SourcePreparationRequest           `json:"source"`
	ParentCreatedMS int64                              `json:"parent_created_ms"`
	Schedule        monthlysource.TokenCaptureSegments `json:"schedule"`
	Baseline        int                                `json:"baseline"`
	Limit           int                                `json:"limit"`
	HistoryScope    *tokenHistoryWorkScope             `json:"history_scope,omitempty"`
}

func initTokenWorkAccounting(ctx context.Context, tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_token_work_grants(parent_key TEXT NOT NULL,source_key TEXT NOT NULL,sha256 TEXT NOT NULL CHECK(length(sha256)=64),payload BLOB NOT NULL CHECK(length(payload)<=4096),PRIMARY KEY(parent_key,source_key))`,
		`CREATE TABLE IF NOT EXISTS portal_monthly_token_work_counts(parent_key TEXT NOT NULL,source_key TEXT NOT NULL,observed INTEGER NOT NULL CHECK(observed>=0 AND observed<=20336),credited INTEGER NOT NULL CHECK(credited>=0 AND credited<=20336),fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),last_lease TEXT NOT NULL,PRIMARY KEY(parent_key,source_key))`,
		`CREATE TRIGGER IF NOT EXISTS token_work_grant_no_update BEFORE UPDATE ON portal_monthly_token_work_grants BEGIN SELECT RAISE(ABORT,'token work grant immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS token_work_grant_no_delete BEFORE DELETE ON portal_monthly_token_work_grants BEGIN SELECT RAISE(ABORT,'token work grant retained'); END`,
		`CREATE TRIGGER IF NOT EXISTS token_work_count_no_delete BEFORE DELETE ON portal_monthly_token_work_counts BEGIN SELECT RAISE(ABORT,'token work counters retained'); END`,
		`CREATE TRIGGER IF NOT EXISTS token_work_count_monotonic BEFORE UPDATE ON portal_monthly_token_work_counts WHEN NEW.parent_key!=OLD.parent_key OR NEW.source_key!=OLD.source_key OR NEW.observed<=OLD.observed OR NEW.credited!=OLD.credited+1 OR NEW.last_lease=OLD.last_lease BEGIN SELECT RAISE(ABORT,'token work counters monotonic'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func recordTokenWork(ctx context.Context, req SourcePreparationRequest, before, after monthlysource.TokenWorkCheckpoint) error {
	d, ok := ctx.Value(tokenDispatchKey{}).(*tokenWorkDispatch)
	if !ok || d == nil {
		return ErrGenerationIntegrity
	}
	history, err := tokenWorkHistoryScope(ctx, d, req)
	if err != nil {
		return err
	}
	a, b := before.Schedule(), after.Schedule()
	ar, _ := json.Marshal(a)
	br, _ := json.Marshal(b)
	if !bytes.Equal(ar, br) || a.JobKey != req.Key() || !generationHex(a.PlanSHA256, 32) || before.Limit() != after.Limit() || after.Observed() != before.Observed()+1 || after.Observed() > after.Limit() || before.Fingerprint() == after.Fingerprint() {
		return ErrGenerationIntegrity
	}
	d.receipt = &tokenWorkReceipt{request: req, before: before, after: after, history: history}
	return nil
}

func checkTokenCheckpoint(ctx context.Context, req SourcePreparationRequest, before monthlysource.TokenWorkCheckpoint) error {
	d, ok := ctx.Value(tokenDispatchKey{}).(*tokenWorkDispatch)
	if !ok || d == nil || !d.p.accountTokenWork {
		return ErrGenerationInput
	}
	history, err := tokenWorkHistoryScope(ctx, d, req)
	if err != nil {
		return err
	}
	var observed int
	var fp, sha string
	var raw []byte
	err = d.p.db.QueryRowContext(ctx, `SELECT c.observed,c.fingerprint,g.sha256,g.payload FROM portal_monthly_token_work_counts c JOIN portal_monthly_token_work_grants g ON g.parent_key=c.parent_key AND g.source_key=c.source_key WHERE c.parent_key=? AND c.source_key=?`, d.job.key, req.Key()).Scan(&observed, &fp, &sha, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		var total, local int
		if err := d.p.db.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(parent_key=?),0) FROM portal_monthly_token_work_grants`, d.job.key).Scan(&total, &local); err != nil {
			return err
		}
		if total >= 256 || local >= 120 {
			return ErrGenerationCapacity
		}
		return nil
	}
	if err != nil {
		return err
	}
	var saved tokenWorkGrant
	if len(raw) > 4096 || json.Unmarshal(raw, &saved) != nil {
		return ErrGenerationIntegrity
	}
	canonical, _ := json.Marshal(saved)
	current := tokenWorkGrant{Version: 1, Parent: d.job.request, Source: req, HistoryScope: history}
	if history != nil {
		current.Version = 2
	}
	if !bytes.Equal(raw, canonical) || generationHash(raw) != sha || saved.Parent != d.job.request || saved.Source != req || !validTokenWorkGrantVersion(saved, d.p.historySourceWork != nil) || !compatibleTokenWorkGrantHistory(saved, current) {
		return ErrGenerationIntegrity
	}
	wantSchedule, _ := json.Marshal(before.Schedule())
	savedSchedule, _ := json.Marshal(saved.Schedule)
	if !bytes.Equal(wantSchedule, savedSchedule) || saved.Limit != before.Limit() {
		return ErrGenerationChanged
	}
	if observed > before.Observed() || observed == before.Observed() && fp != before.Fingerprint() {
		return ErrGenerationIntegrity
	}
	return nil
}

// No source or authorization I/O inside this receiver transaction. The worker
// already verified the original checkpoints, current authority and live lease.
func applyTokenWork(ctx context.Context, tx *sql.Tx, d *tokenWorkDispatch) error {
	if d == nil || d.receipt == nil {
		return nil
	}
	r := d.receipt
	if d.job.continuation != "" {
		if _, err := d.p.checkContinuationLease(ctx, tx, d.job); err != nil {
			return err
		}
	}
	var created int64
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM portal_monthly_source_preparation WHERE key=? AND state='running' AND lease=?`, d.job.key, d.job.lease).Scan(&created); err != nil {
		return err
	}
	grant := tokenWorkGrant{Version: 1, Parent: d.job.request, Source: r.request, ParentCreatedMS: created, Schedule: r.before.Schedule(), Baseline: r.before.Observed(), Limit: r.before.Limit(), HistoryScope: r.history}
	if r.history != nil {
		grant.Version = 2
	}
	if !validTokenWorkGrantVersion(grant, d.p.historySourceWork != nil) {
		return ErrGenerationIntegrity
	}
	var old, sha string
	var observed, credited int
	err := tx.QueryRowContext(ctx, `SELECT CAST(payload AS TEXT),sha256 FROM portal_monthly_token_work_grants WHERE parent_key=? AND source_key=?`, d.job.key, r.request.Key()).Scan(&old, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		var n, orphans, local int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_token_work_grants`).Scan(&n); err != nil {
			return err
		}
		if n >= 256 {
			return ErrGenerationCapacity
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_token_work_grants WHERE parent_key=?`, d.job.key).Scan(&local); err != nil {
			return err
		}
		if local >= 120 {
			return ErrGenerationCapacity
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_token_work_counts WHERE parent_key=? AND source_key=?`, d.job.key, r.request.Key()).Scan(&orphans); err != nil {
			return err
		}
		if orphans != 0 {
			return ErrGenerationIntegrity
		}
		raw, _ := json.Marshal(grant)
		if len(raw) > 4096 {
			return ErrGenerationCapacity
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_token_work_grants VALUES(?,?,?,?)`, d.job.key, r.request.Key(), generationHash(raw), raw); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_token_work_counts VALUES(?,?,?,?,?,?)`, d.job.key, r.request.Key(), r.after.Observed(), 1, r.after.Fingerprint(), d.job.lease)
		return err
	}
	if err != nil {
		return err
	}
	var saved tokenWorkGrant
	if json.Unmarshal([]byte(old), &saved) != nil {
		return ErrGenerationIntegrity
	}
	canonical, _ := json.Marshal(saved)
	if string(canonical) != old || generationHash([]byte(old)) != sha || !validTokenWorkGrantVersion(saved, d.p.historySourceWork != nil) || !compatibleTokenWorkGrantHistory(saved, grant) || saved.Parent != grant.Parent || saved.Source != grant.Source || saved.ParentCreatedMS != created || saved.Limit != grant.Limit {
		return ErrGenerationIntegrity
	}
	a, _ := json.Marshal(saved.Schedule)
	b, _ := json.Marshal(grant.Schedule)
	if !bytes.Equal(a, b) {
		return ErrGenerationChanged
	}
	var fingerprint string
	if err := tx.QueryRowContext(ctx, `SELECT observed,credited,fingerprint FROM portal_monthly_token_work_counts WHERE parent_key=? AND source_key=?`, d.job.key, r.request.Key()).Scan(&observed, &credited, &fingerprint); err != nil {
		return err
	}
	if observed > r.before.Observed() || observed < saved.Baseline || credited < 1 || credited > observed-saved.Baseline || r.after.Observed() > saved.Limit || observed == r.before.Observed() && fingerprint != r.before.Fingerprint() {
		return ErrGenerationIntegrity
	}
	result, err := tx.ExecContext(ctx, `UPDATE portal_monthly_token_work_counts SET observed=?,credited=credited+1,fingerprint=?,last_lease=? WHERE parent_key=? AND source_key=? AND observed=? AND credited=?`, r.after.Observed(), r.after.Fingerprint(), d.job.lease, d.job.key, r.request.Key(), observed, credited)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return ErrGenerationIntegrity
	}
	return err
}

func checkTokenWorkMode(ctx context.Context, tx *sql.Tx, key string, enabled bool) error {
	return checkTokenWorkModePolicy(ctx, tx, key, enabled, nil)
}

func checkTokenWorkModePolicy(ctx context.Context, tx *sql.Tx, key string, enabled bool, history *automaticHistoryPreparation) error {
	allowHistory := history != nil && history.wallet.funding.accountTokenWork
	var grants, counts int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_token_work_grants WHERE parent_key=?`, key).Scan(&grants); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_token_work_counts WHERE parent_key=?`, key).Scan(&counts); err != nil {
		return err
	}
	if grants != counts || grants > 120 || grants > 0 && !enabled {
		return ErrGenerationIntegrity
	}
	if grants == 0 {
		return nil
	}
	var guards int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN ('token_work_grant_no_update','token_work_grant_no_delete','token_work_count_no_delete','token_work_count_monotonic')`).Scan(&guards); err != nil {
		return err
	}
	if guards != 4 {
		return ErrGenerationIntegrity
	}
	var created int64
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM portal_monthly_source_preparation WHERE key=?`, key).Scan(&created); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT g.source_key,g.sha256,g.payload,c.observed,c.credited,c.fingerprint,c.last_lease FROM portal_monthly_token_work_grants g LEFT JOIN portal_monthly_token_work_counts c ON g.parent_key=c.parent_key AND g.source_key=c.source_key WHERE g.parent_key=? LIMIT 121`, key)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var source, sha, fp, lease string
		var raw []byte
		var observed, credited int
		var g tokenWorkGrant
		if err := rows.Scan(&source, &sha, &raw, &observed, &credited, &fp, &lease); err != nil {
			return err
		}
		if len(raw) > 4096 || json.Unmarshal(raw, &g) != nil {
			return ErrGenerationIntegrity
		}
		canonical, _ := json.Marshal(g)
		if !bytes.Equal(canonical, raw) || generationHash(raw) != sha || !validTokenWorkGrantVersion(g, allowHistory) || g.Parent.Key() != key || g.Source.Key() != source || g.ParentCreatedMS != created || g.Baseline < 0 || g.Limit < g.Schedule.TotalSlots || g.Limit > 20336 || g.Baseline >= g.Limit || g.Schedule.JobKey != source || g.Schedule.Version != 1 || !generationHex(g.Schedule.PlanSHA256, 32) || observed <= g.Baseline || observed > g.Limit || credited < 1 || credited > observed-g.Baseline || !generationHex(fp, 32) || !generationHex(lease, 16) {
			return ErrGenerationIntegrity
		}
		if g.HistoryScope != nil && g.HistoryScope.SourceIdentity != history.wallet.funding.sourceIdentity {
			return ErrGenerationChanged
		}
	}
	return rows.Err()
}
