package monthlydraft

// This journal owns execution/recovery metadata ONLY. It never stores money,
// updates source databases, or makes a draft a customer-visible publication.
import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

const (
	publicationRunning   = "running"
	publicationRetry     = "retry"
	publicationPaused    = "paused"
	publicationSaved     = "saved"
	publicationAttempts  = 3
	publicationLeaseTime = 150 * time.Second // longer than the 2-minute finalizer
	publicationRetryTime = 30 * time.Second
)

// InitAutomaticPublicationReceiver is an explicit LOCAL setup/upgrade step on
// the feature's receiver. Constructors and production startup never call it.
// Default draft workers do not require or modify this additive table.
func InitAutomaticPublicationReceiver(ctx context.Context, db *sql.DB) error {
	return generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		var existing int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('portal_monthly_publication_control','portal_monthly_publication_jobs')`).Scan(&existing); err != nil {
			return err
		}
		if existing != 0 {
			if existing != 2 {
				return ErrGenerationIntegrity
			}
			if err := publicationVersion(ctx, tx); err != nil {
				return err
			}
		}
		for _, query := range []string{
			`CREATE TABLE IF NOT EXISTS portal_monthly_publication_control (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL)`,
			`INSERT OR IGNORE INTO portal_monthly_publication_control(id,version) VALUES(1,1)`,
			`CREATE TABLE IF NOT EXISTS portal_monthly_publication_jobs (
task_id TEXT PRIMARY KEY NOT NULL CHECK(length(task_id)=32),
task_key TEXT NOT NULL CHECK(length(task_key)=64), result_hash TEXT NOT NULL CHECK(length(result_hash)=64),
state TEXT NOT NULL CHECK(state IN ('running','retry','paused','saved')),
attempts INTEGER NOT NULL CHECK(attempts BETWEEN 1 AND 3),
updated_at INTEGER NOT NULL, lease_until INTEGER NOT NULL DEFAULT 0,
lease_token TEXT NOT NULL DEFAULT '', retry_at INTEGER NOT NULL DEFAULT 0,
evidence_hash TEXT NOT NULL DEFAULT '' CHECK(length(evidence_hash) IN (0,64)))`,
			`CREATE TRIGGER IF NOT EXISTS monthly_publication_no_delete BEFORE DELETE ON portal_monthly_publication_jobs
BEGIN SELECT RAISE(ABORT,'publication recovery records are retained'); END`,
			`CREATE TRIGGER IF NOT EXISTS monthly_publication_identity BEFORE UPDATE ON portal_monthly_publication_jobs
WHEN NEW.task_id!=OLD.task_id OR NEW.task_key!=OLD.task_key OR NEW.result_hash!=OLD.result_hash
OR NEW.attempts<OLD.attempts OR NEW.updated_at<OLD.updated_at
OR (OLD.evidence_hash!='' AND NEW.evidence_hash!=OLD.evidence_hash) OR OLD.state IN ('paused','saved')
BEGIN SELECT RAISE(ABORT,'publication recovery identity and terminal states are immutable'); END`,
		} {
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
		var version int
		if err := tx.QueryRowContext(ctx, `SELECT version FROM portal_monthly_publication_control WHERE id=1`).Scan(&version); err != nil {
			return err
		}
		if version != 1 {
			return ErrGenerationIntegrity
		}
		return nil
	})
}

type publicationStore struct {
	db  *sql.DB
	now func() time.Time
}

type publicationRecord struct {
	id, key, result, state, token, evidence string
	attempts                                int
	updated, until, retry                   int64
}

// A lease cannot be serialized and resumed after restart. Reclaim instead.
type publicationLease struct {
	id, key, result, token string
	attempt                int
}

func (publicationLease) MarshalJSON() ([]byte, error) { return nil, ErrGenerationInput }
func (l *publicationLease) UnmarshalJSON([]byte) error {
	*l = publicationLease{}
	return ErrGenerationInput
}

func newPublicationStore(db *sql.DB) (*publicationStore, error) {
	if db == nil {
		return nil, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := publicationVersion(ctx, db); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT task_id,task_key,result_hash,state,attempts,updated_at,lease_until,lease_token,retry_at,evidence_hash FROM portal_monthly_publication_jobs LIMIT 0`)
	if err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return &publicationStore{db: db, now: time.Now}, nil
}

type publicationQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func publicationVersion(ctx context.Context, q publicationQuerier) error {
	var version int
	if err := q.QueryRowContext(ctx, `SELECT version FROM portal_monthly_publication_control WHERE id=1`).Scan(&version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrGenerationIntegrity
		}
		return err
	}
	if version != 1 {
		return ErrGenerationIntegrity
	}
	return nil
}

func loadPublication(ctx context.Context, q publicationQuerier, id string) (publicationRecord, error) {
	var r publicationRecord
	if err := publicationVersion(ctx, q); err != nil {
		return r, err
	}
	var key, result, state string
	err := q.QueryRowContext(ctx, `SELECT p.task_id,p.task_key,p.result_hash,p.state,p.attempts,p.updated_at,p.lease_until,p.lease_token,p.retry_at,p.evidence_hash,
t.task_key,t.result_sha256,t.state FROM portal_monthly_publication_jobs p
LEFT JOIN portal_monthly_draft_tasks t ON t.id=p.task_id WHERE p.task_id=?`, id).
		Scan(&r.id, &r.key, &r.result, &r.state, &r.attempts, &r.updated, &r.until, &r.token, &r.retry, &r.evidence, &key, &result, &state)
	if err != nil {
		return r, err
	}
	if key != r.key || result != r.result || state != monthlytasks.DraftReady {
		return r, ErrGenerationIntegrity
	}
	return r, validatePublicationRecord(r)
}

func validatePublicationRecord(r publicationRecord) error {
	if !generationHex(r.id, 16) || !generationHex(r.key, 32) || !generationHex(r.result, 32) ||
		(r.evidence != "" && !generationHex(r.evidence, 32)) ||
		r.attempts < 1 || r.attempts > publicationAttempts || r.updated <= 0 || r.updated > math.MaxInt64-publicationLeaseTime.Milliseconds() {
		return ErrGenerationIntegrity
	}
	switch r.state {
	case publicationRunning:
		if !generationHex(r.token, 16) || r.until <= r.updated || r.until != r.updated+publicationLeaseTime.Milliseconds() || r.retry != 0 {
			return ErrGenerationIntegrity
		}
	case publicationRetry:
		if r.token != "" || r.until != 0 || r.attempts >= publicationAttempts || r.retry != r.updated+publicationRetryTime.Milliseconds() {
			return ErrGenerationIntegrity
		}
	case publicationPaused, publicationSaved:
		if r.token != "" || r.until != 0 || r.retry != 0 || (r.state == publicationSaved && r.evidence == "") {
			return ErrGenerationIntegrity
		}
	default:
		return ErrGenerationIntegrity
	}
	return nil
}

func (s *publicationStore) read(ctx context.Context, id string) (publicationRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r, err := loadPublication(ctx, s.db, id)
	if err == nil && s.now().UnixMilli() < r.updated {
		return r, monthlytasks.ErrClock
	}
	return r, err
}

func publicationEligible(r publicationRecord, now int64) bool {
	return (r.state == publicationRunning && now >= r.until) || (r.state == publicationRetry && now >= r.retry)
}

// claim serializes independent processes using the existing receiver writer
// reservation. A crash consumes an attempt; restart cannot reset the budget.
func (s *publicationStore) claim(ctx context.Context, task monthlytasks.Task) (publicationLease, bool, error) {
	var lease publicationLease
	descriptor, err := json.Marshal(task.Request)
	if err != nil || !generationHex(task.ID, 16) || generationHash(descriptor) != task.Key {
		return lease, false, ErrGenerationInput
	}
	claimed := false
	err = generationWrite(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		now := s.now().UnixMilli()
		if now <= 0 || now > math.MaxInt64-publicationLeaseTime.Milliseconds() {
			return monthlytasks.ErrClock
		}
		var key, result, state string
		if err := tx.QueryRowContext(ctx, `SELECT task_key,result_sha256,state FROM portal_monthly_draft_tasks WHERE id=?`, task.ID).Scan(&key, &result, &state); err != nil {
			return err
		}
		if task.State != monthlytasks.DraftReady || key != task.Key || result != task.ResultSHA256 || state != monthlytasks.DraftReady {
			return ErrGenerationChanged
		}
		// Archive maintenance and publication claim share the writer reservation.
		// Never claim a task whose retained payload was released first.
		var retained int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_generation_inputs WHERE id=? AND artifact_hash=?`, task.Request.InputSHA256, result).Scan(&retained); err != nil {
			return err
		}
		if retained != 1 {
			return ErrGenerationChanged
		}
		r, err := loadPublication(ctx, tx, task.ID)
		fresh := errors.Is(err, sql.ErrNoRows)
		if !fresh && err != nil {
			return err
		}
		if !fresh {
			if now < r.updated {
				return monthlytasks.ErrClock
			}
			if !publicationEligible(r, now) {
				return nil
			}
			if r.attempts >= publicationAttempts {
				_, err = tx.ExecContext(ctx, `UPDATE portal_monthly_publication_jobs SET state='paused',updated_at=?,lease_until=0,lease_token='',retry_at=0 WHERE task_id=?`, now, task.ID)
				return err
			}
		}
		var entropy [16]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return err
		}
		lease = publicationLease{id: task.ID, key: key, result: result, token: hex.EncodeToString(entropy[:]), attempt: r.attempts + 1}
		if fresh {
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_publication_jobs`).Scan(&count); err != nil {
				return err
			}
			if count >= 256 {
				return ErrGenerationCapacity
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_publication_jobs(task_id,task_key,result_hash,state,attempts,updated_at,lease_until,lease_token) VALUES(?,?,?,'running',?,?,?,?)`, task.ID, key, result, lease.attempt, now, now+publicationLeaseTime.Milliseconds(), lease.token)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE portal_monthly_publication_jobs SET state='running',attempts=?,updated_at=?,lease_until=?,lease_token=?,retry_at=0 WHERE task_id=?`, lease.attempt, now, now+publicationLeaseTime.Milliseconds(), lease.token, task.ID)
		}
		claimed = err == nil
		return err
	})
	if err != nil {
		return publicationLease{}, false, err
	}
	return lease, claimed, nil
}

func (s *publicationStore) withLease(ctx context.Context, l publicationLease, fn func(context.Context, *sql.Tx, publicationRecord, int64) error) error {
	return generationWrite(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		r, err := loadPublication(ctx, tx, l.id)
		if err != nil {
			return err
		}
		now := s.now().UnixMilli()
		if now < r.updated {
			return monthlytasks.ErrClock
		}
		if r.state != publicationRunning || r.key != l.key || r.result != l.result || r.token != l.token || r.attempts != l.attempt || now >= r.until {
			return monthlytasks.ErrLeaseLost
		}
		return fn(ctx, tx, r, now)
	})
}

// Freeze the exact evidence before any publication side effect. Different
// metadata on a retry must not silently produce a different customer bill.
func (s *publicationStore) bind(ctx context.Context, l publicationLease, raw []byte) error {
	if len(raw) == 0 || len(raw) > 2<<20 {
		return ErrGenerationEvidence
	}
	digest := generationHash(raw)
	return s.withLease(ctx, l, func(ctx context.Context, tx *sql.Tx, r publicationRecord, _ int64) error {
		if r.evidence != "" && r.evidence != digest {
			return ErrGenerationEvidence
		}
		if r.evidence == digest {
			return nil
		}
		_, err := tx.ExecContext(ctx, `UPDATE portal_monthly_publication_jobs SET evidence_hash=? WHERE task_id=?`, digest, l.id)
		return err
	})
}

func (s *publicationStore) finish(ctx context.Context, l publicationLease, runErr error) error {
	return s.withLease(ctx, l, func(ctx context.Context, tx *sql.Tx, r publicationRecord, now int64) error {
		state, retry := publicationSaved, int64(0)
		if runErr == nil && r.evidence == "" {
			return ErrGenerationIntegrity
		}
		if runErr != nil {
			state = publicationRetry
			retry = now + publicationRetryTime.Milliseconds()
			if errors.Is(runErr, ErrGenerationEvidence) || errors.Is(runErr, ErrGenerationIntegrity) || errors.Is(runErr, ErrGenerationChanged) || r.attempts >= publicationAttempts {
				state, retry = publicationPaused, 0
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE portal_monthly_publication_jobs SET state=?,updated_at=?,lease_until=0,lease_token='',retry_at=? WHERE task_id=?`, state, now, retry, l.id)
		return err
	})
}
