package monitor

import "gorm.io/gorm"

func recoverTokenNotifications(tx *gorm.DB, now int64) error {
	// A crashed attempt may have reached SMTP; retain identity and mark uncertainty.
	var lost []TokenNotifyOutbox
	if err := tx.Where("state = ? AND lease_until <= ?", "sending", now).Limit(100).Find(&lost).Error; err != nil {
		return err
	}
	for _, row := range lost {
		state, reason := "uncertain", "previous_send_outcome_unknown"
		nextAt := now + 60
		if row.Attempts >= 5 {
			state, reason = "failed", "attempt_limit_after_uncertain_send"
			nextAt = now + tokenNotifyTerminalRetryDelay
		} else if row.ExpiresAt <= now {
			state, reason = "expired", "notification_ttl_expired"
		}
		if err := tx.Model(&TokenNotifyOutbox{}).Where("id = ? AND state = ?", row.ID, "sending").Updates(map[string]any{"state": state, "reason": reason, "next_at": nextAt, "lease_token": "", "lease_until": 0}).Error; err != nil {
			return err
		}
		if err := tokenDeliveryAudit(tx, "worker", "notification_recovered", row.ID, reason, now); err != nil {
			return err
		}
	}
	return nil
}
