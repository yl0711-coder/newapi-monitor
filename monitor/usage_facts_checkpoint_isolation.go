package monitor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// usageFactMemberCheckpointIncompleteError reports one published member whose
// current state no longer proves its signed full-history row (for example a
// no-history member that received new activity and went back to discovering).
type usageFactMemberCheckpointIncompleteError struct {
	UserID int64
}

func (e *usageFactMemberCheckpointIncompleteError) Error() string {
	return fmt.Sprintf("full-history verification checkpoint incomplete user_id=%d", e.UserID)
}

// revokeUsageFactPublishedMemberForCheckpoint removes one member from the
// serving publication so the readiness checkpoint can pass for everyone else.
//
// Production 2026-11-17 08:39: one member's state regressed while its signed
// row stayed published; the checkpoint failed and closed the user-usage page
// for every user until a worker happened to re-sign the member. Revoking only
// that member is the same durable operation the history repair hold uses: the
// row is deleted and the publication fingerprint, range and generations are
// recomputed in one transaction. The member returns on the next publish once
// its state proves it again; nothing unverified is served.
func (m *Monitor) revokeUsageFactPublishedMemberForCheckpoint(ctx context.Context, userID int64) (bool, error) {
	var publishedState UsageFactSyncState
	changed := false
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		deleted := tx.Where("user_id = ?", userID).Delete(&UsageFactPublishedMember{})
		if deleted.Error != nil {
			return deleted.Error
		}
		if deleted.RowsAffected == 0 {
			return nil
		}
		changed = true
		var kept []UsageFactPublishedMember
		if err := tx.Order("user_id").Find(&kept).Error; err != nil {
			return err
		}
		if err := tx.First(&publishedState, 1).Error; err != nil {
			return err
		}
		if len(kept) == 0 {
			publishedState.PublishedFingerprint = ""
			publishedState.PublishedRangeStart = 0
			publishedState.PublishedThrough = 0
			publishedState.PublishedWindowDays = 0
			publishedState.PublishedAt = 0
		} else {
			ids := make([]int64, 0, len(kept))
			for _, row := range kept {
				ids = append(ids, row.UserID)
			}
			publishedState.PublishedFingerprint = portalMemberFingerprintFromIDs(ids)
			if err := normalizeUsageFactPublishedRange(&publishedState, kept); err != nil {
				return err
			}
		}
		publishedState.Generation++
		publishedState.ServingGeneration++
		return tx.Save(&publishedState).Error
	})
	if err != nil {
		return false, err
	}
	if changed {
		m.publishUsageFactGenerations(publishedState.Generation, publishedState.ServingGeneration)
		m.publishUsageFactReadBoundsAfterMutation(publishedState)
	}
	return changed, nil
}

// usageFactCheckpointIncompleteMember returns the member named by a
// checkpoint-incomplete error, or 0.
func usageFactCheckpointIncompleteMember(err error) int64 {
	var incomplete *usageFactMemberCheckpointIncompleteError
	if errors.As(err, &incomplete) && incomplete.UserID > 0 {
		return incomplete.UserID
	}
	return 0
}

// usageFactCheckpointRevokeTimeout bounds the revoke transaction; it runs on
// the readiness path after a publish.
const usageFactCheckpointRevokeTimeout = 10 * time.Second
