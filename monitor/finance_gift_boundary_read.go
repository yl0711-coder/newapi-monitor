package monitor

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// Stream directly into the caller's proof groups instead of materializing and
// copying a second, whole-history slice. This is not a persistent cache: every
// build still reads all required events and verifies every proof. The visitor
// must not query this DB while its single connection is held by the cursor.
func walkFinanceGiftBoundaryEvents(ctx context.Context, db *gorm.DB, from, to int64, users []int64, visit func(FinanceGiftBoundaryEvent) error) error {
	for start := 0; start < len(users); start += financeGiftUserQueryChunk {
		end := min(len(users), start+financeGiftUserQueryChunk)
		if err := walkFinanceGiftBoundaryChunk(ctx, db, from, to, users[start:end], visit); err != nil {
			return fmt.Errorf("read gift boundary events: %w", err)
		}
	}
	return ctx.Err()
}

func walkFinanceGiftBoundaryChunk(ctx context.Context, db *gorm.DB, from, to int64, users []int64, visit func(FinanceGiftBoundaryEvent) error) error {
	// Preserve GORM's NULL-to-zero handling, including legacy rows whose group
	// columns were added later. A missing group remains unknown, never business.
	// Do not add a global ORDER BY: per-hour proof hashes and the gift allocator
	// already sort independently; SQLite need not sort the full history first.
	rows, err := db.WithContext(ctx).Model(&FinanceGiftBoundaryEvent{}).Select(`
		COALESCE(source_epoch,''),COALESCE(source_log_id,0),COALESCE(hour_ts,0),
		COALESCE(user_id,0),COALESCE(event_at,0),COALESCE(kind,''),COALESCE(quota,0),
		COALESCE(grp,''),COALESCE(group_known,0),COALESCE(evidence_hash,'')`).
		Where("hour_ts>=? AND hour_ts<? AND user_id IN ?", from, to, users).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var event FinanceGiftBoundaryEvent
		if err := rows.Scan(&event.SourceEpoch, &event.SourceLogID, &event.HourTs,
			&event.UserID, &event.EventAt, &event.Kind, &event.Quota,
			&event.Group, &event.GroupKnown, &event.EvidenceHash); err != nil {
			return err
		}
		if err := visit(event); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return rows.Close()
}
