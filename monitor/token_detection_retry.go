package monitor

import (
	"context"
	"gorm.io/gorm"
)

func (m *Monitor) deferTokenDetection(ctx context.Context, job TokenMeteringFact, policyHash, lease string, now int64) error {
	return m.tokenStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return m.deferTokenDetectionTx(tx, job, policyHash, lease, now)
	})
}

func (m *Monitor) deferTokenDetectionTx(tx *gorm.DB, job TokenMeteringFact, policyHash, lease string, now int64) error {
	fence := tx.Model(&TokenDetectionState{}).Where("source_epoch = ? AND lease_token = ? AND lease_until > ?", job.SourceEpoch, lease, now).Update("policy_hash", policyHash)
	if fence.Error != nil {
		return fence.Error
	}
	if fence.RowsAffected != 1 {
		return errTokenDetectionLease
	}
	var fact TokenMeteringFact
	if err := tx.First(&fact, "source_epoch = ? AND log_id = ?", job.SourceEpoch, job.LogID).Error; err != nil {
		return err
	}
	var retry TokenDetectionRetry
	if err := tx.Where("source_epoch = ? AND log_id = ?", job.SourceEpoch, job.LogID).Limit(1).Find(&retry).Error; err != nil {
		return err
	}
	if retry.Attempts == 0 {
		var count int64
		if err := tx.Model(&TokenDetectionRetry{}).Count(&count).Error; err != nil {
			return err
		}
		maxRows, _ := m.cfg.tokenDetectionLimits()
		if count >= maxRows {
			return errTokenDetectionCapacity
		}
	}
	retry.SourceEpoch, retry.LogID = job.SourceEpoch, job.LogID
	retry.FactRevision, retry.LinkRevision, retry.PolicyHash = fact.Revision, fact.LinkRevision, policyHash
	retry.Attempts = min(retry.Attempts+1, 7)
	retry.NextAt = now + min(int64(3600), int64(60)<<uint(retry.Attempts-1))
	retry.ExpiresAt = now + m.cfg.tokenDetectionRetentionSeconds()
	retry.Reason = "local_capacity_exhausted"
	switch fact.LinkState {
	case "waiting":
		retry.Reason = "association_pending"
	case "source_missing":
		retry.Reason = "association_source_missing"
	case "evidence_query_failed":
		retry.Reason = "association_query_failed"
	case "ambiguous":
		retry.Reason = "association_ambiguous"
	case "conflict":
		retry.Reason = "association_conflict"
	case "expired":
		retry.Reason = "association_expired"
	}
	return tx.Save(&retry).Error
}
