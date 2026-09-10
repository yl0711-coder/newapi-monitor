package monitor

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

// Retry only known-ready chains or transient reads. Select again after each
// commit so a long chain can advance within one poll, not one link per S3 sweep.
// Each object is attempted at most once per phase, with a fixed total bound.
func (r *ecsArchiveReplayer) retryReady(ctx context.Context, reader ecsarchive.Reader, now time.Time) error {
	visited := make([]string, 0, ecsArchivePhaseEntries)
	for n := 0; n < ecsArchivePhaseEntries; n++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if archivePhaseYield(ctx, n) {
			return nil
		}
		var receipt ECSLogArchiveReceipt
		query := r.m.storeDB.WithContext(ctx).Model(&ECSLogArchiveReceipt{}).
			Where(`status IN (425,503) AND (status = 503 OR previous_verified = 0 OR previous_verified IS NULL OR previous_hash = '' OR EXISTS (
				SELECT 1 FROM ecs_log_archive_receipts AS predecessor
				WHERE predecessor.node = ecs_log_archive_receipts.node AND predecessor.lane = ecs_log_archive_receipts.lane
				AND predecessor.body_hash = ecs_log_archive_receipts.previous_hash AND predecessor.status = 200))`).
			Order("last_attempt ASC, archive_at ASC, object_key ASC")
		if len(visited) > 0 {
			query = query.Where("object_key NOT IN ?", visited)
		}
		err := query.First(&receipt).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		visited = append(visited, receipt.ObjectKey)
		if err := r.retryReceipt(ctx, reader, receipt, now); err != nil {
			return err
		}
	}
	return nil
}

func (r *ecsArchiveReplayer) retryReceipt(ctx context.Context, reader ecsarchive.Reader, receipt ECSLogArchiveReceipt, now time.Time) error {
	if _, _, _, _, err := ecsarchive.ParseKey(r.prefix, receipt.ObjectKey); err != nil {
		return errors.New("invalid archive retry identity")
	}
	o, err := reader.Get(ctx, receipt.ObjectKey)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		o = ecsarchive.Object{Key: receipt.ObjectKey, ETag: receipt.ETag, Modified: time.Unix(receipt.ArchiveAt, 0)}
		return r.record(ctx, o, ecsArchiveReplayResult{Status: 503})
	}
	if o.Key != receipt.ObjectKey || o.ETag != receipt.ETag || o.Modified.IsZero() || o.Modified.Unix() != receipt.ArchiveAt || len(o.Body) == 0 || len(o.Body) > ecsarchive.MaxObject {
		return errors.New("archive retry immutability violated")
	}
	// Recheck signature/lease/revocation even for previously verified metadata.
	return r.record(ctx, o, r.replay(ctx, o, now))
}
