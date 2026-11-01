package monthlysource

// Work accounting is NOT money/coverage evidence. Only this source-owned
// reader can construct a checkpoint; consumers cannot deserialize one.
import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

type TokenWorkCheckpoint struct {
	schedule        TokenCaptureSegments
	observed, limit int
	stamp           string
}

func (TokenWorkCheckpoint) MarshalJSON() ([]byte, error) { return nil, ErrTokenMonthCaptureIntegrity }
func (c *TokenWorkCheckpoint) UnmarshalJSON([]byte) error {
	*c = TokenWorkCheckpoint{}
	return ErrTokenMonthCaptureIntegrity
}
func (c TokenWorkCheckpoint) Schedule() TokenCaptureSegments {
	s := c.schedule
	s.Segments = append([]TokenCaptureSegment(nil), s.Segments...)
	return s
}
func (c TokenWorkCheckpoint) Observed() int       { return c.observed }
func (c TokenWorkCheckpoint) Limit() int          { return c.limit }
func (c TokenWorkCheckpoint) Fingerprint() string { return c.stamp }

// ReadTokenWorkCheckpoint reads only an already-initialized private capture
// store. Source connection, raw writes, monetary approval and queue are absent.
func ReadTokenWorkCheckpoint(parent context.Context, db *sql.DB, key string, scope OrganizationMonthScope) (TokenWorkCheckpoint, error) {
	var empty TokenWorkCheckpoint
	if parent == nil || db == nil {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now()
	plan, err := loadFundingMonthCapturePlan(ctx, tx, key, scope, now)
	if err != nil {
		return empty, err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return empty, err
	}
	schedule, err := PlanTokenCaptureSegments(ctx, raw, now)
	if err != nil {
		return empty, err
	}
	var recovery int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='monthly_token_recovery_bindings'`).Scan(&recovery); err != nil {
		return empty, err
	}
	limit := schedule.TotalSlots
	var recovered recoveringTokenInventory
	if recovery != 0 {
		recovered, err = loadRecoveringTokenInventoryMode(ctx, tx, plan, now, true)
		if err != nil {
			return empty, err
		}
		limit += adaptiveTokenJobs * 62 // original fixed global roots/observations cap
	} else if _, err := tokenCaptureInventory(ctx, tx, key, schedule.TotalSlots, now); err != nil {
		return empty, err
	}
	h := sha256.New()
	enc := json.NewEncoder(h)
	if err := enc.Encode(schedule.PlanSHA256); err != nil {
		return empty, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms FROM portal_monthly_token_capture_slots WHERE job_key=? ORDER BY ordinal LIMIT 12401`, key)
	if err != nil {
		return empty, err
	}
	n := 0
	for rows.Next() {
		var ordinal int
		var user, started, completed int64
		var sha, usage string
		if err := rows.Scan(&ordinal, &user, &sha, &usage, &started, &completed); err != nil {
			_ = rows.Close()
			return empty, err
		}
		r, err := tokenCaptureRequest(plan, ordinal)
		if err != nil || r.UserID != user || !validDigest(sha) || !validDigest(usage) || started <= 0 || completed < started || completed > now.UnixMilli() {
			_ = rows.Close()
			return empty, ErrTokenMonthCaptureIntegrity
		}
		if err := enc.Encode([]any{ordinal, user, sha, usage, started, completed}); err != nil {
			_ = rows.Close()
			return empty, err
		}
		n++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return empty, err
	}
	// Fixed first-slot order, never map iteration order in the fingerprint.
	for i := 0; i < schedule.TotalSlots/2; i++ {
		if d, ok := recovered.days[i]; ok {
			refs := adaptiveEvidenceRefs(d.state)
			// The recovery inventory above still audits plans/splits even when
			// empty. They are coordinator work, not successful originals. A
			// newly planned root must not invalidate an unchanged saved work
			// checkpoint before its first child observation can be collected.
			if len(refs) == 0 {
				continue
			}
			if err := enc.Encode(struct {
				Root string
				Refs []adaptiveTokenEvidenceRef
			}{d.state.id, refs}); err != nil {
				return empty, err
			}
			n += len(refs)
		}
	}
	if n > limit {
		return empty, ErrTokenMonthCaptureBudget
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return TokenWorkCheckpoint{schedule, n, limit, hex.EncodeToString(h.Sum(nil))}, nil
}
