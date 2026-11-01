package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
)

func continuationExhausted(c preparationContinuation, now int64) bool {
	return now < c.grant.StartedMS || now-c.grant.StartedMS >= continuationLifetime.Milliseconds() ||
		c.claims >= continuationDispatchLimit || c.claims-c.exempt >= continuationCoordinatorLimit || c.failures+c.grant.Original.Failures >= 3
}

func stopBadContinuation(ctx context.Context, tx *sql.Tx, key, code string) error {
	_, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='failed',failure_code=?,lease='',lease_until=0 WHERE key=?`, code, key)
	return err
}

// No file/source work under the receiver writer. Every actual source step
// independently verifies scope, unchanged capture plans, raw records and roots.
func (p *onDemandPreparation) expireContinuations(ctx context.Context, tx *sql.Tx, now int64) error {
	if !p.continuationEnabled {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT q.key FROM portal_monthly_source_preparation q JOIN portal_monthly_preparation_continuations c ON c.job_key=q.key WHERE q.state IN ('queued','running') LIMIT 9`)
	if err != nil {
		return err
	}
	keys := []string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if len(keys) > 8 {
		return ErrGenerationCapacity
	}
	for _, key := range keys {
		c, err := readPreparationContinuation(ctx, tx, key)
		if err != nil {
			if e := stopBadContinuation(ctx, tx, key, "invalid_descriptor"); e != nil {
				return e
			}
			continue
		}
		s, err := failedPreparationSnapshot(ctx, tx, c.grant.Plan.Request)
		if err != nil {
			return err
		}
		if s.State == "running" && s.LeaseUntilMS <= now {
			if c.lastLease != s.Lease || c.failures+c.grant.Original.Failures >= 3 {
				if err := stopBadContinuation(ctx, tx, key, "invalid_descriptor"); err != nil {
					return err
				}
				continue
			}
			if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_preparation_continuation_counts SET failures=failures+1 WHERE job_key=?`, key); err != nil {
				return err
			}
			c.failures++
			state := "queued"
			if c.failures+c.grant.Original.Failures >= 3 {
				state = "failed"
			}
			if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state=?,failure_code='lease_expired',lease='',lease_until=0,next_at=? WHERE key=?`, state, now, key); err != nil {
				return err
			}
			s.State = state
		}
		if s.State == "queued" && continuationExhausted(c, now) {
			if err := stopBadContinuation(ctx, tx, key, "budget_exhausted"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *onDemandPreparation) claimContinuation(ctx context.Context, tx *sql.Tx, job *sourcePreparationJob, now int64) (bool, error) {
	c, err := readPreparationContinuation(ctx, tx, job.key)
	if errors.Is(err, sql.ErrNoRows) {
		var orphan int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_preparation_continuation_counts WHERE job_key=?`, job.key).Scan(&orphan); err != nil {
			return false, err
		}
		if orphan != 0 {
			return false, ErrGenerationIntegrity
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !p.continuationEnabled || !p.accountTokenWork || c.grant.Plan.Request != job.request || continuationExhausted(c, now) {
		return false, ErrGenerationIntegrity
	}
	if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_preparation_continuation_counts SET claims=claims+1,last_lease=? WHERE job_key=?`, job.lease, job.key); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state='running',lease=?,lease_until=? WHERE key=? AND state='queued'`, job.lease, now+60000, job.key)
	if err != nil {
		return false, err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return false, errors.Join(ErrGenerationChanged, err)
	}
	job.continuation = c.sha
	return true, nil
}

func (p *onDemandPreparation) checkContinuationLease(ctx context.Context, db preparationResumeReader, job sourcePreparationJob) (preparationContinuation, error) {
	var empty preparationContinuation
	if job.continuation == "" {
		return empty, ErrGenerationInput
	}
	c, err := readPreparationContinuation(ctx, db, job.key)
	if err != nil {
		return empty, err
	}
	if !p.continuationEnabled || !p.accountTokenWork || c.sha != job.continuation || c.lastLease != job.lease || c.grant.Plan.Request != job.request || p.now().UnixMilli() < c.grant.StartedMS || p.now().UnixMilli()-c.grant.StartedMS >= continuationLifetime.Milliseconds() || c.claims > continuationDispatchLimit || c.claims-c.exempt > continuationCoordinatorLimit || c.failures+c.grant.Original.Failures >= 3 {
		return empty, ErrGenerationIntegrity
	}
	s, err := failedPreparationSnapshot(ctx, db, job.request)
	if err != nil {
		return empty, err
	}
	if s.State != "running" || s.Lease != job.lease || s.LeaseUntilMS <= p.now().UnixMilli() {
		return empty, ErrGenerationChanged
	}
	return c, nil
}

func (p *onDemandPreparation) finishContinuation(ctx context.Context, tx *sql.Tx, job sourcePreparationJob, state, code string, failures, adjust int, delay int64) error {
	// The lease fence remains mandatory even when an error/expiry is being
	// recorded. A late writer must not overwrite another claimant's progress.
	c, err := readPreparationContinuation(ctx, tx, job.key)
	if err != nil {
		return err
	}
	s, err := failedPreparationSnapshot(ctx, tx, job.request)
	if err != nil {
		return err
	}
	if c.sha != job.continuation || c.lastLease != job.lease || s.State != "running" || s.Lease != job.lease || s.LeaseUntilMS <= p.now().UnixMilli() {
		return ErrGenerationChanged
	}
	if failures < 0 || failures > 1 || adjust > 0 || adjust < -1 || c.failures+c.grant.Original.Failures+failures > 3 {
		return ErrGenerationIntegrity
	}
	if c.failures+c.grant.Original.Failures+failures >= 3 {
		state = "failed"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_preparation_continuation_counts SET exempt=exempt+?,failures=failures+? WHERE job_key=?`, -adjust, failures, job.key); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE portal_monthly_source_preparation SET state=?,failure_code=?,next_at=?,lease='',lease_until=0 WHERE key=? AND state='running' AND lease=?`, state, code, p.now().UnixMilli()+delay, job.key, job.lease)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errors.Join(ErrGenerationChanged, err)
	}
	return nil
}
