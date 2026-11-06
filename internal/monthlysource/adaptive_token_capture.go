package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/go-sql-driver/mysql"
)

var adaptiveTokenAdvanceSlots = make(chan struct{}, 1)

type adaptiveTokenCaptureOps struct {
	now        func() time.Time
	pace       func(context.Context) error
	capture    func(context.Context, *sql.DB, UsageWindowRequest) (TokenUsageWindowObservation, error)
	jointMonth bool // month scheduler shares the old-slot/adaptive byte pool
}

// AdvanceAdaptiveTokenWindow performs at most ONE original read-only capture.
// A successful split is local scheduling progress, not a captured observation.
// Actual source failure budgets survive restarts; waiting does not query source
// or hold a connection. This entry is not wired to production month slots.
func AdvanceAdaptiveTokenWindow(parent context.Context, source, receiver *sql.DB, job string, request UsageWindowRequest) (AdaptiveTokenWindowProgress, error) {
	return advanceAdaptiveTokenWindow(parent, source, receiver, job, request, adaptiveTokenCaptureOps{
		now: time.Now, capture: CaptureTokenUsageWindow,
		pace: func(ctx context.Context) error { return waitFundingInterval(ctx, time.Second) },
	})
}

func advanceAdaptiveTokenWindow(parent context.Context, source, receiver *sql.DB, job string, request UsageWindowRequest, ops adaptiveTokenCaptureOps) (AdaptiveTokenWindowProgress, error) {
	var empty AdaptiveTokenWindowProgress
	if parent == nil || source == nil || receiver == nil || source == receiver || reflect.TypeOf(source.Driver()) == reflect.TypeOf(receiver.Driver()) || ops.now == nil || ops.capture == nil || ops.pace == nil {
		return empty, ErrAdaptiveTokenIntegrity
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	select {
	case adaptiveTokenAdvanceSlots <- struct{}{}:
		defer func() { <-adaptiveTokenAdvanceSlots }()
	default:
		return empty, ErrWalletCaptureBusy
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	// A store explicitly initialized for month recovery has ONE shared pool,
	// even if an internal caller uses the standalone entry on that store.
	if !ops.jointMonth {
		var monthly int
		if err := receiver.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='monthly_token_recovery_bindings'`).Scan(&monthly); err != nil {
			return empty, err
		}
		ops.jointMonth = monthly != 0
	}
	if err := ensureAdaptiveTokenRoot(ctx, receiver, job, request, ops.now()); err != nil {
		return empty, err
	}
	s, err := loadAdaptiveTokenState(ctx, receiver, job, request, ops.now())
	if err != nil {
		return empty, err
	}
	p, next, phase := adaptiveTokenProgress(s, ops.now())
	if phase == -2 {
		return empty, ErrAdaptiveTokenIntegrity
	}
	if p.Complete {
		_, err := loadAdaptiveTokenPairs(ctx, receiver, s)
		if err != nil {
			return empty, err
		}
		return p, nil
	}
	if ops.now().UnixMilli()-s.created > int64((24*time.Hour)/time.Millisecond) {
		return empty, ErrAdaptiveTokenBudget
	}
	if phase < 0 {
		return p, nil
	}
	used, err := adaptiveTokenRetainedBytes(ctx, receiver, ops.jointMonth)
	if err != nil {
		return empty, err
	}
	if used >= adaptiveTokenBytes {
		return empty, errors.Join(ErrAdaptiveTokenBudget, saveAdaptiveTokenFault(parent, receiver, s, "byte_budget", true, ops.now()))
	}
	if phase == 1 {
		if _, err := loadAdaptiveTokenRecord(ctx, receiver, s, next, 0); err != nil {
			return empty, err // reject lost/tampered old proof BEFORE more source I/O
		}
	}
	if err := ops.pace(ctx); err != nil {
		return empty, err
	}
	connectCtx, connectCancel := context.WithTimeout(ctx, 4*time.Second)
	err = source.PingContext(connectCtx)
	connectCancel()
	if err != nil {
		if parent.Err() == nil {
			code := "transport" // Legacy/unknown connection errors are not resumable.
			if adaptiveTokenConnectionError(err) {
				code = "retryable_transport"
			}
			err = errors.Join(err, saveAdaptiveTokenFault(parent, receiver, s, code, false, ops.now()))
		}
		return empty, err
	}
	o, err := ops.capture(ctx, source, next)
	if err != nil {
		if parent.Err() != nil {
			return empty, parent.Err()
		}
		if errors.Is(err, ErrWalletCaptureBusy) {
			return empty, err
		}
		if ctx.Err() == nil && adaptiveTokenCanSplit(err) {
			if _, splitErr := SplitUsageWindow(next); splitErr == nil {
				if err := saveAdaptiveTokenSplit(ctx, receiver, s, next, phase, ops.now()); err != nil {
					return empty, err
				}
				after, err := loadAdaptiveTokenState(ctx, receiver, job, request, ops.now())
				if err != nil {
					return empty, err
				}
				p, _, _ = adaptiveTokenProgress(after, ops.now())
				p.Split = true
				return p, nil // failed parent has NO retained observation
			}
			return empty, errors.Join(ErrAdaptiveTokenBudget, saveAdaptiveTokenFault(parent, receiver, s, "minimum_window", true, ops.now()))
		}
		code := "source"
		if adaptiveTokenRetryableSource(err) {
			code = "retryable_source"
		}
		return empty, errors.Join(err, saveAdaptiveTokenFault(parent, receiver, s, code, false, ops.now()))
	}
	if err := saveAdaptiveTokenRecordBounded(ctx, receiver, s, next, phase, o, ops.now(), ops.jointMonth); err != nil {
		if errors.Is(err, ErrAdaptiveTokenBudget) && parent.Err() == nil {
			err = errors.Join(err, saveAdaptiveTokenFault(parent, receiver, s, "byte_budget", true, ops.now()))
		}
		return empty, err
	}
	after, err := loadAdaptiveTokenState(ctx, receiver, job, request, ops.now())
	if err != nil {
		return empty, err
	}
	p, _, phase = adaptiveTokenProgress(after, ops.now())
	if phase == -2 {
		return empty, ErrAdaptiveTokenIntegrity
	}
	if p.Complete {
		if _, err := loadAdaptiveTokenPairs(ctx, receiver, after); err != nil {
			return empty, err
		}
	}
	p.Captured = true
	return p, nil
}

// ONLY actual aggregation-query execution deadlines qualify. No splitting on
// caller cancellation, auth/transport/schema failure, validation or money drift.
func adaptiveTokenCanSplit(err error) bool {
	stage := TokenUsageFailureStage(err)
	if stage != "user_totals" && stage != "token_groups" {
		return false
	}
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 3024 || errors.Is(err, context.DeadlineExceeded)
}

func adaptiveTokenRetryableSource(err error) bool {
	switch TokenUsageFailureStage(err) {
	case "transaction", "identity", "wallet", "user_totals", "token_groups", "commit":
	default:
		return false
	}
	return adaptiveTokenConnectionError(err)
}

func adaptiveTokenConnectionError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var network net.Error
	return errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.As(err, &network)
}

func saveAdaptiveTokenSplit(ctx context.Context, db *sql.DB, state adaptiveTokenState, req UsageWindowRequest, phase int, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadAdaptiveTokenState(ctx, tx, state.root.JobKey, state.root.Request, now)
	if err != nil || !adaptiveTokenLeaf(current, req) {
		return ErrAdaptiveTokenIntegrity
	}
	if _, err := SplitUsageWindow(req); err != nil {
		return ErrAdaptiveTokenBudget
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO monthly_adaptive_token_splits(root_id,window_from,window_to,phase) SELECT ?,?,?,? WHERE (SELECT count(*) FROM monthly_adaptive_token_splits WHERE root_id=?)<?`, state.id, req.From, req.To, phase, state.id, adaptiveTokenSplits); err != nil {
		return err
	}
	return tx.Commit()
}

func adaptiveTokenLeaf(s adaptiveTokenState, req UsageWindowRequest) bool {
	for _, leaf := range s.leaves {
		if leaf == req {
			return true
		}
	}
	return false
}

func saveAdaptiveTokenRecordBounded(ctx context.Context, db *sql.DB, s adaptiveTokenState, req UsageWindowRequest, phase int, o TokenUsageWindowObservation, now time.Time, jointMonth bool) error {
	if o.Request != req || phase < 0 || phase > 1 || ValidateTokenUsageWindowObservation(o) != nil || o.CompletedAtMS > now.UnixMilli() {
		return ErrAdaptiveTokenIntegrity
	}
	raw, err := json.Marshal(o)
	if err != nil {
		return err
	}
	u, err := UsageObservationFromToken(o)
	if err != nil {
		return err
	}
	projection, err := usageWindowBytes(u)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadAdaptiveTokenState(ctx, tx, s.root.JobKey, s.root.Request, now)
	if err != nil || !adaptiveTokenLeaf(current, req) {
		return ErrAdaptiveTokenIntegrity
	}
	if jointMonth || current.resumeEnabled {
		used, err := adaptiveTokenRetainedBytes(ctx, tx, jointMonth)
		if err != nil {
			return err
		}
		additional := int64(len(raw) + len(projection))
		if _, exists := current.records[adaptiveTokenSlot{adaptiveTokenNode{req.From, req.To}, phase}]; exists {
			additional = 0 // exact duplicate still revalidated below
		}
		if used > adaptiveTokenBytes-additional {
			return ErrAdaptiveTokenBudget
		}
	}
	var older TokenUsageWindowObservation
	if phase == 1 {
		older, err = loadAdaptiveTokenRecord(ctx, tx, current, req, 0)
		if err != nil || o.StartedAtMS-older.CompletedAtMS < 300000 {
			return ErrAdaptiveTokenIntegrity
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO monthly_adaptive_token_observations(root_id,window_from,window_to,phase,sha,usage_id,started_at_ms,completed_at_ms,payload,projection)
SELECT ?,?,?,?,?,?,?,?,?,? WHERE (SELECT COALESCE(sum(length(payload)+length(projection)),0) FROM monthly_adaptive_token_observations)+?<=? ON CONFLICT(root_id,window_from,window_to,phase) DO NOTHING`, s.id, req.From, req.To, phase, digest(raw), digest(projection), o.StartedAtMS, o.CompletedAtMS, raw, projection, len(raw)+len(projection), adaptiveTokenBytes)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_usage_observations(id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, digest(projection), req.SourceIdentity, req.UserID, req.From, req.To, o.StartedAtMS, o.CompletedAtMS, projection)
	if err != nil {
		return err
	}
	key := adaptiveTokenSlot{adaptiveTokenNode{req.From, req.To}, phase}
	current.records[key] = adaptiveTokenRecord{sha: digest(raw), usage: digest(projection), started: o.StartedAtMS, completed: o.CompletedAtMS}
	stored, err := loadAdaptiveTokenRecord(ctx, tx, current, req, phase)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAdaptiveTokenBudget
		}
		return err
	}
	storedRaw, _ := json.Marshal(stored)
	if !bytes.Equal(raw, storedRaw) {
		return ErrAdaptiveTokenIntegrity
	}
	changed := phase == 1 && !TokenUsageWindowsMatch(older, o)
	if changed {
		if err := saveAdaptiveTokenFault(ctx, tx, current, "pair_changed", true, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if changed {
		return ErrAdaptiveTokenIntegrity // BOTH genuine originals remain retained
	}
	return nil
}

func loadAdaptiveTokenRecord(ctx context.Context, db walletRowReader, s adaptiveTokenState, req UsageWindowRequest, phase int) (TokenUsageWindowObservation, error) {
	var o TokenUsageWindowObservation
	var raw, projection []byte
	var record adaptiveTokenRecord
	key := adaptiveTokenSlot{adaptiveTokenNode{req.From, req.To}, phase}
	err := db.QueryRowContext(ctx, `SELECT sha,usage_id,started_at_ms,completed_at_ms,CASE WHEN length(payload)<=2097152 THEN payload END,CASE WHEN length(projection)<=2097152 THEN projection END FROM monthly_adaptive_token_observations WHERE root_id=? AND window_from=? AND window_to=? AND phase=?`, s.id, req.From, req.To, phase).Scan(&record.sha, &record.usage, &record.started, &record.completed, &raw, &projection)
	if err != nil {
		return o, err
	}
	if expected, ok := s.records[key]; !ok || expected != record || digest(raw) != record.sha || digest(projection) != record.usage || json.Unmarshal(raw, &o) != nil || o.Request != req || o.StartedAtMS != record.started || o.CompletedAtMS != record.completed || ValidateTokenUsageWindowObservation(o) != nil {
		return TokenUsageWindowObservation{}, ErrAdaptiveTokenIntegrity
	}
	canonical, _ := json.Marshal(o)
	u, err := UsageObservationFromToken(o)
	if err != nil || !bytes.Equal(canonical, raw) {
		return TokenUsageWindowObservation{}, ErrAdaptiveTokenIntegrity
	}
	want, err := usageWindowBytes(u)
	if err != nil || !bytes.Equal(want, projection) {
		return TokenUsageWindowObservation{}, ErrAdaptiveTokenIntegrity
	}
	stored, err := loadUsageWindow(ctx, db, record.usage)
	if err != nil || !bytes.Equal(stored, want) {
		return TokenUsageWindowObservation{}, errors.Join(ErrAdaptiveTokenIntegrity, err)
	}
	return o, nil
}

func loadAdaptiveTokenPairs(ctx context.Context, db walletRowReader, s adaptiveTokenState) ([]TokenUsageWindowPair, error) {
	pairs := make([]TokenUsageWindowPair, 0, len(s.leaves))
	for _, leaf := range s.leaves {
		a, err := loadAdaptiveTokenRecord(ctx, db, s, leaf, 0)
		if err != nil {
			return nil, err
		}
		b, err := loadAdaptiveTokenRecord(ctx, db, s, leaf, 1)
		if err != nil || !TokenUsageWindowsMatch(a, b) {
			return nil, errors.Join(ErrAdaptiveTokenIntegrity, err)
		}
		pairs = append(pairs, TokenUsageWindowPair{Older: a, Newer: b})
	}
	return pairs, ctx.Err()
}

// LoadAdaptiveTokenWindowPairs returns only original child pairs with exact
// root coverage. It cannot turn them into a synthetic parent or approved month.
func LoadAdaptiveTokenWindowPairs(parent context.Context, db *sql.DB, job string, request UsageWindowRequest) ([]TokenUsageWindowPair, error) {
	if parent == nil || db == nil {
		return nil, ErrAdaptiveTokenIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	s, err := loadAdaptiveTokenState(ctx, tx, job, request, time.Now())
	if err != nil {
		return nil, err
	}
	pairs, err := loadAdaptiveTokenPairs(ctx, tx, s)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return pairs, nil
}
