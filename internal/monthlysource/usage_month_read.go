package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// At most 64 pairs/128 bounded records are held at once. The caller retains
// one read transaction and one cumulative byte/deadline budget for all batches.
func loadUsageMonthBatch(ctx context.Context, tx *sql.Tx, versions []UsageWindowVersion, usedBytes *int) (map[string]UsageWindowObservation, error) {
	if len(versions) == 0 || len(versions) > 64 || usedBytes == nil {
		return nil, errors.New("invalid usage month read batch")
	}
	args := make([]any, 0, len(versions)*2)
	wanted := make(map[string]bool, len(versions)*2)
	for _, v := range versions {
		for _, id := range []string{v.OlderID, v.NewerID} {
			if !validDigest(id) || wanted[id] {
				return nil, errors.New("invalid or reused usage observation version")
			}
			wanted[id] = true
			args = append(args, id)
		}
	}
	q := `SELECT id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload
FROM portal_monthly_usage_observations WHERE length(payload)<=32768 AND id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + ")"
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]UsageWindowObservation, len(args))
	for rows.Next() {
		var id, source string
		var user, from, to, started, completed int64
		var raw []byte
		if err := rows.Scan(&id, &source, &user, &from, &to, &started, &completed, &raw); err != nil {
			return nil, err
		}
		*usedBytes += len(raw)
		if *usedBytes > usageMonthBytesLimit {
			return nil, errors.New("usage month payload budget exceeded; split account batches")
		}
		o, err := validateStoredUsageWindow(id, source, user, from, to, started, completed, raw)
		if err != nil {
			return nil, err
		}
		if !wanted[id] {
			return nil, errors.New("foreign usage observation in month batch")
		}
		if _, exists := out[id]; exists {
			return nil, errors.New("duplicate usage observation in month batch")
		}
		out[id] = o
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(out) != len(args) {
		return nil, errors.New("usage month original observation missing")
	}
	return out, ctx.Err()
}
