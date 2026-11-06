package monthlytasks

import (
	"context"
	"database/sql"
	"encoding/json"
)

// TaskObservation is INTERNAL receiver metadata, not current authorization or a
// bill. StorageSHA256 also binds the hidden lease fields without exposing them.
type TaskObservation struct {
	Task          Task   `json:"task"`
	StorageSHA256 string `json:"storage_sha256"`
	Retryable     bool   `json:"retryable"`
}

// InspectReceiverTasks only reads the caller's SQLite snapshot. It never
// initializes, renews, invalidates, retries or deletes tasks. The caller must
// bound ctx and retain the SAME transaction while reading associated inputs.
func InspectReceiverTasks(ctx context.Context, tx *sql.Tx) ([]TaskObservation, error) {
	if tx == nil {
		return nil, ErrInvalid
	}
	var version, count int
	if err := tx.QueryRowContext(ctx, `SELECT version FROM portal_monthly_draft_task_control WHERE id=1`).Scan(&version); err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, ErrCorrupt
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_draft_tasks`).Scan(&count); err != nil {
		return nil, err
	}
	if count > maxTasks {
		return nil, ErrCapacity // Never silently truncate a maintenance inventory.
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM portal_monthly_draft_tasks ORDER BY id LIMIT ?`, maxTasks+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, count)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(ids) != count {
		return nil, ErrCorrupt
	}
	out := make([]TaskObservation, 0, count)
	for _, id := range ids {
		r, err := load(ctx, tx, "id", id)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(struct {
			Task
			Started, Until int64
			Token          string
		}{r.Task, r.started, r.until, r.token})
		if err != nil {
			return nil, err
		}
		out = append(out, TaskObservation{r.Task, hash(raw), r.State == Failed && r.Attempts < maxAttempts})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
