// Package monthlytasks stores bounded, durable INTERNAL draft-generation tasks.
// It is not an authorization layer, a source-evidence validator, an artifact
// store or a publication API. No worker, route or production migration uses it.
// Callers must revalidate current authorization and source versions before work
// and delivery. A draft_ready task MUST NOT be served as a customer bill.
package monthlytasks

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrInvalid   = errors.New("invalid monthly task input")
	ErrNotFound  = errors.New("monthly task not found in scope")
	ErrBusy      = errors.New("monthly task capacity unavailable")
	ErrCapacity  = errors.New("monthly task retention capacity reached")
	ErrLeaseLost = errors.New("monthly task lease lost")
	ErrState     = errors.New("monthly task transition not allowed")
	ErrCooldown  = errors.New("monthly task retry is cooling down")
	ErrExhausted = errors.New("monthly task attempt budget exhausted")
	ErrStale     = errors.New("monthly task inputs invalidated")
	ErrCorrupt   = errors.New("monthly task integrity failure")
	ErrClock     = errors.New("monthly task clock moved backwards")
)

const (
	Queued      = "queued"
	Running     = "running"
	DraftReady  = "draft_ready"
	Failed      = "failed"
	Invalidated = "invalidated"

	maxTasks       = 256 // Fail closed; never silently delete a bill or evidence.
	maxActive      = 8
	maxAttempts    = 3
	leaseDuration  = 30 * time.Second
	attemptLimit   = 10 * time.Minute
	retryDelay     = 30 * time.Second
	operationLimit = 2 * time.Second
)

// Owner is server-derived, not customer-supplied. Manifest binds membership
// control revisions so remove/re-add cannot revive an old scope. Use existing
// Usage conventions: the membership fingerprint is the first 16 SHA-256 bytes;
// the control manifest is the full SHA-256. Neither is a name/email address.
type Owner struct {
	GroupID           int64  `json:"group_id"`
	AuthVersion       int64  `json:"auth_version"`
	MemberFingerprint string `json:"member_fingerprint"`
	MemberManifest    string `json:"member_manifest"`
}

// Request identifies immutable inputs, not amounts or proof of completeness.
// InputSHA256 binds the entire evidence manifest (including selected scan
// heads, exact boundaries and preparation inputs). RulesSHA256 also binds the
// generator/format version. A changed input MUST produce a different digest.
type Request struct {
	Owner          Owner  `json:"owner"`
	Month          string `json:"month"`
	SourceIdentity string `json:"source_identity"`
	InputSHA256    string `json:"input_sha256"`
	RulesSHA256    string `json:"rules_sha256"`
}

// Task is an internal status DTO; it contains no raw logs or financial amounts.
// ResultSHA256 only identifies a separately retained internal draft. This store
// neither verifies that draft nor grants authority to publish it.
type Task struct {
	ID, Key      string
	Request      Request
	State        string
	Attempts     int
	CreatedAtMS  int64
	UpdatedAtMS  int64
	RetryAtMS    int64
	ResultSHA256 string
	FailureCode  string
}

// Lease is a process-local capability, fenced on every transition. After a
// crash, reclaim the stored task; never deserialize a token and resume work.
type Lease struct {
	id, key, token string
	attempt        int
}

func (Lease) MarshalJSON() ([]byte, error) { return nil, ErrInvalid }
func (l *Lease) UnmarshalJSON([]byte) error {
	*l = Lease{}
	return ErrInvalid
}

type record struct {
	Task
	token, descriptor string
	started, until    int64
}

// Store does not own db, start goroutines or initialize tables. Use a dedicated
// local receiver connection; do not point it at the upstream source database.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, ErrInvalid
	}
	return &Store{db: db, now: time.Now}, nil
}

func validHex(s string, size int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == size && hex.EncodeToString(b) == s
}

func (o Owner) valid() bool {
	return o.GroupID > 0 && o.AuthVersion >= 0 && validHex(o.MemberFingerprint, 16) && validHex(o.MemberManifest, 32)
}

var monthZone = time.FixedZone("Usage-UTC+8", 8*60*60)

func requestBytes(r Request) ([]byte, error) {
	start, err := time.ParseInLocation("2006-01", r.Month, monthZone)
	if err != nil || start.Format("2006-01") != r.Month || start.Year() < 2020 || !r.Owner.valid() ||
		!validHex(r.SourceIdentity, 32) || !validHex(r.InputSHA256, 32) || !validHex(r.RulesSHA256, 32) {
		return nil, ErrInvalid
	}
	return json.Marshal(r)
}

func hash(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Submit reuses an exact task, including failure (no implicit retry on refresh).
// At most one queued/running task per organization and eight globally. Source
// or scope changes never rewrite a previous task: invalidate it explicitly and
// submit a new version. Different months do not cancel one another implicitly.
func (s *Store) Submit(ctx context.Context, request Request) (Task, error) {
	return s.submit(ctx, request, nil)
}

// SubmitWithInputs atomically admits a task and its immutable receiver inputs.
// prepare is trusted internal storage code, not a customer callback. It runs
// after admission checks under the same 2-second SQLite write transaction.
// existing=true means validate the retained inputs ONLY: missing inputs for an
// established task are corruption, not permission to silently reconstruct them.
// It must use tx (not a second connection), respect ctx, and must not commit,
// roll back, change task rows, do I/O outside this receiver or calculate money.
// Any failure rolls back both records and returns an empty Task. Invalidated
// tasks and rejected admissions never call prepare.
func (s *Store) SubmitWithInputs(ctx context.Context, request Request, prepare func(context.Context, *sql.Tx, bool) error) (Task, error) {
	if prepare == nil {
		return Task{}, ErrInvalid
	}
	return s.submit(ctx, request, prepare)
}

func (s *Store) submit(ctx context.Context, request Request, prepare func(context.Context, *sql.Tx, bool) error) (Task, error) {
	raw, err := requestBytes(request)
	if err != nil {
		return Task{}, err
	}
	key := hash(raw)
	var result Task
	err = s.write(ctx, func(ctx context.Context, tx *sql.Tx, now int64) error {
		month, _ := time.ParseInLocation("2006-01", request.Month, monthZone)
		if month.AddDate(0, 1, 0).After(time.UnixMilli(now)) {
			return ErrInvalid
		}
		old, err := load(ctx, tx, "task_key", key)
		if err == nil {
			if old.descriptor != string(raw) {
				return ErrCorrupt
			}
			if old.State == Invalidated {
				return ErrStale
			}
			if prepare != nil {
				if err := prepare(ctx, tx, true); err != nil {
					return err
				}
			}
			result = old.Task
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_draft_tasks`).Scan(&count); err != nil {
			return err
		}
		if count >= maxTasks {
			return ErrCapacity
		}
		if err := activeCapacity(ctx, tx, request.Owner.GroupID); err != nil {
			return err
		}
		id, err := randomID()
		if err != nil {
			return err
		}
		r := record{Task: Task{ID: id, Key: key, Request: request, State: Queued, CreatedAtMS: now, UpdatedAtMS: now}, descriptor: string(raw)}
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_draft_tasks
		(id,task_key,group_id,descriptor,state,attempts,created_at,updated_at,started_at,lease_until,lease_token,retry_at,result_sha256,failure_code)
		VALUES(?,?,?,?,?,0,?,?,0,0,'',0,'','')`, id, key, request.Owner.GroupID, raw, Queued, now, now); err != nil {
			return err
		}
		if prepare != nil {
			if err := prepare(ctx, tx, false); err != nil {
				return err
			}
		}
		result = r.Task
		return nil
	})
	if err != nil {
		return Task{}, err
	}
	return result, nil
}

func (s *Store) Get(ctx context.Context, owner Owner, id string) (Task, error) {
	if !owner.valid() || !validHex(id, 16) {
		return Task{}, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, operationLimit)
	defer cancel()
	r, err := scoped(ctx, s.db, owner, id)
	if err != nil {
		return Task{}, err
	}
	return r.Task, nil
}

// Claim begins a bounded attempt, or reclaims an expired lease after a crash.
// The old worker cannot renew/fail/finish once the lease expires, even if no
// replacement has started. Only one unexpired worker can run in this database.
func (s *Store) Claim(ctx context.Context, owner Owner, id string) (Task, Lease, error) {
	var result Task
	var lease Lease
	var exhausted bool
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now int64) error {
		r, err := scoped(ctx, tx, owner, id)
		if err != nil {
			return err
		}
		if now < r.UpdatedAtMS {
			return ErrClock
		}
		if r.State != Queued && r.State != Running {
			return ErrState
		}
		if r.State == Running && r.until > now {
			return ErrBusy
		}
		if r.Attempts >= maxAttempts {
			finishRecord(&r, Failed, "attempts_exhausted", "", now)
			exhausted = true
			return save(ctx, tx, r)
		}
		var running int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_draft_tasks WHERE state='running' AND lease_until>?`, now).Scan(&running); err != nil {
			return err
		}
		if running > 0 {
			return ErrBusy
		}
		token, err := randomID()
		if err != nil {
			return err
		}
		r.State, r.token, r.started, r.until = Running, token, now, now+leaseDuration.Milliseconds()
		r.Attempts++
		r.UpdatedAtMS, r.RetryAtMS, r.FailureCode = now, 0, ""
		if err := save(ctx, tx, r); err != nil {
			return err
		}
		result, lease = r.Task, Lease{r.ID, r.Key, token, r.Attempts}
		return nil
	})
	if err != nil {
		return Task{}, Lease{}, err
	}
	if exhausted {
		return Task{}, Lease{}, ErrExhausted
	}
	return result, lease, nil
}

// Renew never revives an expired lease or extends an attempt past ten minutes.
// Work must stop on any renewal error; don't keep using an unavailable store.
func (s *Store) Renew(ctx context.Context, lease Lease) error {
	return s.withLease(ctx, lease, func(ctx context.Context, tx *sql.Tx, r record, now int64) error {
		r.until = min(now+leaseDuration.Milliseconds(), r.started+attemptLimit.Milliseconds())
		r.UpdatedAtMS = now
		return save(ctx, tx, r)
	})
}

func (s *Store) Complete(ctx context.Context, lease Lease, draftSHA256 string) error {
	if !validHex(draftSHA256, 32) {
		return ErrInvalid
	}
	return s.withLease(ctx, lease, func(ctx context.Context, tx *sql.Tx, r record, now int64) error {
		finishRecord(&r, DraftReady, "", draftSHA256, now)
		return save(ctx, tx, r)
	})
}

func validFailure(code string) bool {
	switch code {
	case "source_unavailable", "evidence_incomplete", "reconciliation_failed", "internal_failure":
		return true
	}
	return false
}

// Fail accepts fixed internal codes only, never raw SQL/upstream errors.
func (s *Store) Fail(ctx context.Context, lease Lease, code string) error {
	if !validFailure(code) {
		return ErrInvalid
	}
	return s.withLease(ctx, lease, func(ctx context.Context, tx *sql.Tx, r record, now int64) error {
		finishRecord(&r, Failed, code, "", now)
		r.RetryAtMS = now + retryDelay.Milliseconds()
		return save(ctx, tx, r)
	})
}

func (s *Store) Retry(ctx context.Context, owner Owner, id string) error {
	return s.retry(ctx, owner, id, false)
}

// RetryTransient rechecks the retry policy inside the write transaction, not
// merely against a scheduler's earlier snapshot. Evidence failures stay paused.
func (s *Store) RetryTransient(ctx context.Context, owner Owner, id string) error {
	return s.retry(ctx, owner, id, true)
}

func (s *Store) retry(ctx context.Context, owner Owner, id string, transientOnly bool) error {
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now int64) error {
		r, err := scoped(ctx, tx, owner, id)
		if err != nil {
			return err
		}
		if now < r.UpdatedAtMS {
			return ErrClock
		}
		if r.State != Failed {
			return ErrState
		}
		if transientOnly && r.FailureCode != "source_unavailable" && r.FailureCode != "internal_failure" {
			return ErrState
		}
		if r.Attempts >= maxAttempts {
			return ErrExhausted
		}
		if now < r.RetryAtMS {
			return ErrCooldown
		}
		if err := activeCapacity(ctx, tx, owner.GroupID); err != nil {
			return err
		}
		finishRecord(&r, Queued, "", "", now)
		return save(ctx, tx, r)
	})
}

// Invalidate stops only this internal task and clears its draft reference.
// It cannot withdraw/delete an existing published bill or alter source facts.
// It is sticky: an identical Submit cannot resurrect invalidated inputs.
func (s *Store) Invalidate(ctx context.Context, owner Owner, id string) error {
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now int64) error {
		r, err := scoped(ctx, tx, owner, id)
		if err != nil {
			return err
		}
		if now < r.UpdatedAtMS {
			return ErrClock
		}
		if r.State == Invalidated {
			return nil
		}
		finishRecord(&r, Invalidated, "inputs_changed", "", now)
		return save(ctx, tx, r)
	})
}

func finishRecord(r *record, state, code, result string, now int64) {
	r.State, r.FailureCode, r.ResultSHA256, r.UpdatedAtMS = state, code, result, now
	r.token, r.started, r.until, r.RetryAtMS = "", 0, 0, 0
}

func (s *Store) withLease(ctx context.Context, lease Lease, fn func(context.Context, *sql.Tx, record, int64) error) error {
	if !validHex(lease.id, 16) || !validHex(lease.key, 32) || !validHex(lease.token, 16) || lease.attempt < 1 {
		return ErrLeaseLost
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now int64) error {
		r, err := load(ctx, tx, "id", lease.id)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if r.State != Running || r.Key != lease.key || r.token != lease.token || r.Attempts != lease.attempt ||
			r.until <= now || now < r.UpdatedAtMS || now >= r.started+attemptLimit.Milliseconds() {
			return ErrLeaseLost
		}
		return fn(ctx, tx, r, now)
	})
}
