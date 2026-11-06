package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
)

// Fetch one member's bounded original slots and their atomic projections in
// one read statement. LEFT JOIN is intentional: a missing projection must fail
// validation, not disappear and be mistaken for a complete/zero-cost month.
// No setup, source writes or financial normalization occurs here.
const accountTokenCaptureSlotsQuery = `SELECT s.ordinal,s.user_id,s.sha256,s.usage_id,s.started_at_ms,s.completed_at_ms,
CASE WHEN length(s.payload)<=2097152 THEN s.payload END,
u.source_identity,u.user_id,u.window_from,u.window_to,u.started_at_ms,u.completed_at_ms,
CASE WHEN length(u.payload)<=32768 THEN u.payload END
FROM portal_monthly_token_capture_slots s
LEFT JOIN portal_monthly_usage_observations u ON u.id=s.usage_id
WHERE s.job_key=? AND s.user_id=? LIMIT ?`

func loadAccountTokenCaptureSlots(ctx context.Context, db *sql.DB, plan FundingMonthCapturePlan, userID int64) (map[int]TokenUsageWindowObservation, error) {
	// No SQL ordering is required: the bounded ordinal map below is assembled
	// in the pinned plan's order by the caller. ORDER BY ordinal can make
	// SQLite choose the job/ordinal primary key and scan every other member.
	rows, err := db.QueryContext(ctx, accountTokenCaptureSlotsQuery, plan.Key, userID, len(plan.Scans)+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int]TokenUsageWindowObservation, len(plan.Scans))
	var size int64
	for rows.Next() {
		var ordinal int
		var slot storedTokenCaptureSlot
		// Nullable fields distinguish a missing joined projection without
		// accepting it as an empty valid usage observation.
		var source sql.NullString
		var user, from, to, started, completed sql.NullInt64
		var raw []byte
		if err := rows.Scan(&ordinal, &slot.user, &slot.sha, &slot.usageID, &slot.started, &slot.completed, &slot.raw,
			&source, &user, &from, &to, &started, &completed, &raw); err != nil {
			return nil, err
		}
		size += int64(len(slot.raw))
		if len(out) >= len(plan.Scans) || size > tokenMonthCaptureBytes || slot.user != userID {
			return nil, ErrTokenMonthCaptureIntegrity
		}
		o, projected, err := validateStoredTokenCaptureSlot(plan, ordinal, slot)
		if err != nil {
			return nil, err
		}
		// projected is already canonical, validated and digest-bound to
		// usage_id. Exact equality plus indexed identity checks preserves
		// loadUsageWindow's complete-record checks without a second query.
		if !source.Valid || !user.Valid || !from.Valid || !to.Valid || !started.Valid || !completed.Valid ||
			source.String != o.Request.SourceIdentity || user.Int64 != o.Request.UserID ||
			from.Int64 != o.Request.From || to.Int64 != o.Request.To || started.Int64 != o.StartedAtMS ||
			completed.Int64 != o.CompletedAtMS || !bytes.Equal(raw, projected) {
			return nil, ErrTokenMonthCaptureIntegrity
		}
		if _, exists := out[ordinal]; exists {
			return nil, ErrTokenMonthCaptureIntegrity
		}
		out[ordinal] = o
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(out) != len(plan.Scans) {
		return nil, ErrTokenMonthCaptureIntegrity
	}
	return out, ctx.Err()
}
