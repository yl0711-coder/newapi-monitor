package monitor

import (
	"context"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

// The archive recovery worker is sequential. A closure must be the only tail
// of one fully verified chain. This proves batch delivery, not producer EOF.
func (r *ecsArchiveReplayer) checkArchiveClosure(ctx context.Context, e ecsarchive.Envelope) int {
	base := func() *gorm.DB {
		return r.m.storeDB.WithContext(ctx).Model(&ECSLogArchiveReceipt{}).
			Where("node = ? AND lane = ? AND status = 200", e.Node, e.Lane)
	}
	var closed int64
	if err := base().Where("kind = ?", ecsarchive.CollectorClosure).Count(&closed).Error; err != nil {
		return 503
	}
	if closed > 0 {
		var duplicate int64
		if err := base().Where("body_hash = ? AND COALESCE(kind,'') = ?", e.Hash, e.Kind).Count(&duplicate).Error; err != nil {
			return 503
		}
		if duplicate == 1 {
			return 200 // Only already-received bytes may be retried after closure.
		}
		return 409
	}
	if e.Kind != ecsarchive.CollectorClosure {
		return 200
	}
	var count, unknown, afterHead int64
	if err := base().Count(&count).Error; err != nil {
		return 503
	}
	if count == 0 && e.Previous == "" {
		return 200 // Empty batch chain, not proof of zero business requests.
	}
	if err := base().Where("previous_verified IS NULL OR previous_verified = ?", false).Count(&unknown).Error; err != nil {
		return 503
	}
	if unknown > 0 {
		return 422 // Legacy accepted rows lacking chain proof cannot be certified.
	}
	if err := base().Where("previous_hash = ?", e.Previous).Count(&afterHead).Error; err != nil {
		return 503
	}
	if afterHead > 0 || e.Previous == "" {
		return 409
	}
	var branches []struct{ PreviousHash string }
	if err := base().Select("previous_hash").Group("previous_hash").Having("COUNT(*) > 1").Limit(1).Scan(&branches).Error; err != nil {
		return 503
	}
	if len(branches) > 0 {
		return 409
	}
	return 200
}
