package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type tokenNotifySender func(context.Context, AlertConfig, string, string, string) error

var tokenNotifyRetryDelays = [...]int64{60, 180, 600, 1800}

const tokenNotifyTerminalRetryDelay int64 = 1800

func tokenDeliveryNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func (m *Monitor) startTokenDeliveryMaintenance(ctx context.Context) {
	if m.cfg.LocalSnapshotOnly || m.tokenStore() == nil {
		return
	}
	go func() {
		timer := time.NewTicker(time.Minute)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			cleanup, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := m.cleanupTokenDelivery(cleanup, time.Now().Unix()); err != nil {
				slog.Warn("token delivery cleanup incomplete")
			}
			cancel()
			if err := m.runTokenNotificationRound(ctx, time.Now().Unix(), nil); err != nil {
				slog.Warn("token notification round incomplete")
			}
		}
	}()
}
func (m *Monitor) cleanupTokenDelivery(ctx context.Context, now int64) error {
	if m.tokenStore() == nil {
		return nil
	}
	return m.tokenStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Recovery itself writes an audit. Reclaim expired entries first so a
		// full audit table cannot permanently block its own retention cleanup.
		if err := tx.Exec("DELETE FROM token_delivery_audits WHERE id IN (SELECT id FROM token_delivery_audits WHERE at < ? LIMIT 200)", now-tokenDeliveryRetention).Error; err != nil {
			return err
		}
		if err := recoverTokenNotifications(tx, now); err != nil {
			return err
		}
		var exports []TokenEvidenceExport
		if err := tx.Select("id").Where("expires_at <= ? AND length(payload) > 0", now).Limit(100).Find(&exports).Error; err != nil {
			return err
		}
		for _, e := range exports {
			if err := tx.Model(&TokenEvidenceExport{}).Where("id = ?", e.ID).Update("payload", []byte{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Exec("DELETE FROM token_evidence_exports WHERE id IN (SELECT id FROM token_evidence_exports WHERE created_at < ? LIMIT 200)", now-86400).Error; err != nil {
			return err
		}
		if err := tx.Model(&TokenNotifyOutbox{}).Where("expires_at <= ? AND state IN ?", now, []string{"pending", "paused", "retry", "uncertain"}).Updates(map[string]any{"state": "expired", "reason": "notification_ttl_expired", "next_at": now + 60, "payload": ""}).Error; err != nil {
			return err
		}
		if err := tx.Model(&TokenNotifyOutbox{}).Where("expires_at <= ? AND state != ?", now, "sending").Update("payload", "").Error; err != nil {
			return err
		}
		if err := tx.Exec("DELETE FROM token_notify_outboxes WHERE id IN (SELECT id FROM token_notify_outboxes WHERE created_at < ? AND state != 'sending' LIMIT 200)", now-7*86400).Error; err != nil {
			return err
		}
		return tx.Exec("DELETE FROM token_notify_targets WHERE id IN (SELECT id FROM token_notify_targets WHERE updated_at < ? LIMIT 200)", now-tokenDeliveryRetention).Error
	})
}
func (m *Monitor) runTokenNotificationRound(parent context.Context, now int64, send tokenNotifySender) error {
	if !m.cfg.TokenNotificationEvaluationEnabled || m.cfg.LocalSnapshotOnly || m.tokenStore() == nil {
		return nil
	}
	policy, err := parseTokenNotifyPolicy(m.cfg.TokenNotificationPolicy)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	nonce, err := tokenDeliveryNonce()
	if err != nil {
		return err
	}
	db := m.tokenStore().WithContext(ctx)
	if err = db.Clauses(clause.OnConflict{DoNothing: true}).Create(&tokenDeliveryState{ID: "notify"}).Error; err != nil {
		return err
	}
	lease := db.Model(&tokenDeliveryState{}).Where("id = ? AND lease_until <= ?", "notify", now).Updates(map[string]any{"lease_token": nonce, "lease_until": now + 30})
	if lease.Error != nil {
		return lease.Error
	}
	if lease.RowsAffected == 0 {
		return nil
	}
	runErr := m.tokenNotifyEvaluateAndSend(ctx, policy, nonce, now, send)
	finish, done := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer done()
	reason := ""
	if runErr != nil {
		reason = "local_notification_incomplete"
		if errors.Is(runErr, errTokenDeliveryCapacity) {
			reason = "notification_capacity_or_budget_limited"
		}
	}
	fields := map[string]any{"lease_token": "", "lease_until": 0, "last_reason": reason}
	if runErr == nil {
		fields["last_evaluated_at"] = now
	}
	released := m.tokenStore().WithContext(finish).Model(&tokenDeliveryState{}).Where("id = ? AND lease_token = ?", "notify", nonce).Updates(fields)
	if released.Error != nil {
		return errors.Join(runErr, released.Error)
	}
	return runErr
}
func (m *Monitor) tokenNotifyEvaluateAndSend(ctx context.Context, p tokenNotifyPolicy, nonce string, now int64, send tokenNotifySender) (resultErr error) {
	db := m.tokenStore().WithContext(ctx)
	evalErr := db.Transaction(func(tx *gorm.DB) error { return m.evaluateTokenNotifications(tx, p, now) })
	defer func() { resultErr = errors.Join(evalErr, resultErr) }()
	if err := db.Transaction(func(tx *gorm.DB) error {
		return recoverTokenNotifications(tx, now)
	}); err != nil {
		return err
	}
	var cfg AlertConfig
	pause := ""
	if !m.cfg.TokenNotificationsEnabled || m.cfg.AlertsDisabled {
		pause = "notification_delivery_disabled"
	} else if m.storeDB == nil {
		pause = "smtp_disabled_or_unavailable"
	} else if err := m.storeDB.WithContext(ctx).First(&cfg, 1).Error; err != nil || !cfg.Enabled || cfg.SMTPHost == "" {
		pause = "smtp_disabled_or_unavailable"
	}
	cfg.Recipients = m.cfg.TokenNotificationRecipients
	if pause == "" && len(recipientList(cfg.Recipients)) == 0 {
		pause = "notification_recipients_missing"
	}
	if pause != "" {
		return db.Model(&TokenNotifyOutbox{}).Where("state IN ?", []string{"pending", "retry", "uncertain"}).Updates(map[string]any{"state": "paused", "reason": pause}).Error
	}
	var row TokenNotifyOutbox
	err := db.Transaction(func(tx *gorm.DB) error {
		// Expires before resuming and charges attempts to a durable daily budget.
		if err := tx.Model(&TokenNotifyOutbox{}).Where("expires_at <= ? AND state IN ?", now, []string{"pending", "paused", "retry", "uncertain"}).Updates(map[string]any{"state": "expired", "reason": "notification_ttl_expired"}).Error; err != nil {
			return err
		}
		if err := tx.Model(&TokenNotifyOutbox{}).Where("policy_hash != ? AND state IN ?", tokenDetectionHash(p), []string{"pending", "paused", "retry", "uncertain"}).Updates(map[string]any{"state": "superseded", "reason": "notification_policy_changed"}).Error; err != nil {
			return err
		}
		if err := tx.Where("state IN ? AND next_at <= ? AND expires_at > ? AND attempts < ?", []string{"pending", "paused", "retry", "uncertain"}, now, now, 5).Order("next_at,created_at,id").Limit(1).Find(&row).Error; err != nil {
			return err
		}
		if row.ID == "" {
			return nil
		}
		var state tokenDeliveryState
		if err := tx.First(&state, "id = ? AND lease_token = ?", "notify", nonce).Error; err != nil {
			return err
		}
		day := (now + 8*3600) / 86400
		if state.BudgetDay != day {
			state.BudgetDay, state.BudgetUsed = day, 0
		}
		if state.BudgetUsed >= p.DailyBudget {
			return errTokenDeliveryCapacity
		}
		state.BudgetUsed++
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		row.State, row.Reason, row.LeaseToken, row.LeaseUntil = "sending", "", nonce, now+30
		row.Attempts++
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		return tokenDeliveryAudit(tx, "worker", "notification_attempt", row.ID, "smtp_attempt_reserved", now)
	})
	if err != nil {
		return err
	}
	if row.ID == "" {
		return nil
	}
	var payload tokenNotifyPayload
	if err := json.Unmarshal([]byte(row.Payload), &payload); err != nil {
		return m.finishTokenNotification(ctx, row, now, errors.New("notification payload invalid"))
	}
	subject, body := tokenNotificationMail(payload, m.cfg.TokenNotificationBaseURL)
	if send == nil {
		send = sendMailWithContext
	}
	sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
	sendErr := send(sendCtx, cfg, subject, body, row.ID+"@monitor.local")
	sendCancel()
	return m.finishTokenNotification(ctx, row, now, sendErr)
}
func (m *Monitor) finishTokenNotification(parent context.Context, row TokenNotifyOutbox, now int64, sendErr error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer cancel()
	return m.tokenStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		fields := map[string]any{"lease_token": "", "lease_until": 0}
		reason := "smtp_accepted_not_recipient_ack"
		if sendErr == nil {
			fields["state"] = "sent"
			fields["sent_at"] = now
		} else {
			reason = "smtp_outcome_unknown"
			fields["state"] = "uncertain"
			if row.Attempts >= 5 {
				fields["state"] = "failed"
				reason = "notification_attempts_exhausted"
				fields["next_at"] = now + tokenNotifyTerminalRetryDelay
			} else {
				fields["next_at"] = now + tokenNotifyRetryDelays[row.Attempts-1]
			}
		}
		fields["reason"] = reason
		updated := tx.Model(&TokenNotifyOutbox{}).Where("id = ? AND state = ? AND lease_token = ?", row.ID, "sending", row.LeaseToken).Updates(fields)
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected != 1 {
			return errors.New("notification send lease lost")
		}
		if sendErr == nil {
			if err := tx.Model(&tokenNotifyTarget{}).Where("id = ?", row.TargetKey).Updates(map[string]any{"last_sent_at": now, "severity": row.Severity, "evidence": row.Evidence, "sent_count": row.Count, "updated_at": now}).Error; err != nil {
				return err
			}
		}
		return tokenDeliveryAudit(tx, "worker", "notification_result", row.ID, reason, now)
	})
}
func tokenNotificationMail(p tokenNotifyPayload, base string) (string, string) {
	q := url.Values{"token_epoch": {p.SourceEpoch}, "tab": {"anomalies"}, "channel_id": {fmt.Sprint(p.ChannelID)}, "model": {p.Model}, "group": {p.Group}, "token_from": {fmt.Sprint(p.From)}, "token_to": {fmt.Sprint(p.To)}, "token_rule": {p.Rule}}
	link := base + "/#" + q.Encode()
	body := fmt.Sprintf("疑似计量或计费结构异常，需核查；候选不代表确认上游多扣。\n冻结窗口：%s — %s（左闭右开）；渠道 %d / %s；实际服务组 %s\n规则 %s；证据 %s；候选请求 %d；客户数 %d。严重度仅表示通知优先级。\n%s\n涉及消费、日志公式差额、参考估计分别列示，不可相加。缺口：%s\n打开内部已认证页面（查看当前修订和首次冻结证据）：\n%s", time.Unix(p.From, 0).UTC().Format(time.RFC3339), time.Unix(p.To, 0).UTC().Format(time.RFC3339), p.ChannelID, p.Model, p.Group, p.Rule, p.Evidence, p.Count, p.Customers, tokenDetectionJSON(p.Summary), tokenDetectionJSON(p.Gaps), link)
	return "[" + p.Severity + "] Token 计量风险待核查", body
}
