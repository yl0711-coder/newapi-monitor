package monitor

// Bounded recovery primitive used by the existing pricing scheduler.
// Only the independent token checkpoint is written. Legacy pricing checkpoints,
// backfill cursors, bindings, cost facts and report publications are untouched.

import (
	"context"
	"crypto/hmac"
	"errors"

	"gorm.io/gorm"
)

type sub2TokenRecoveryResult struct {
	Status      string
	StableScans int
}

// The caller supplies the existing account request pacer and refreshed
// credential. This function never refreshes credentials or bypasses HTTP guards.
// One invocation can finish at most one independent scan, not two.
func (m *Monitor) recoverSub2TokenEvidenceDay(ctx context.Context, account ChannelUpstreamAccount, credential sub2APICredential,
	day, now int64, pacer *upstreamUsageRequestPacer) (sub2TokenRecoveryResult, error) {
	result := sub2TokenRecoveryResult{Status: "pending"}
	ctx, cancel := context.WithTimeout(ctx, upstreamPricingOperationTimeout(m.cfg))
	defer cancel()
	if m.cfg.LocalSnapshotOnly {
		return result, errUpstreamTokenReadOnly
	}
	if !m.cfg.ChannelCostClosureEnabled || !m.cfg.UpstreamPricingLedgerEnabled ||
		!channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, account.Domain) ||
		!pricingLedgerDomainAllowed(m.cfg.UpstreamPricingLedgerDomains, account.Domain) {
		return sub2TokenRecoveryResult{Status: "disabled"}, nil
	}
	key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return result, err
	}
	if account.UsageAdapter == upstreamUsageAdapterSub2Stats || now < day+86400 || pacer == nil ||
		pacer.maxRequests < 2 || pacer.maxRequests > upstreamPricingMaxRequestsPerRun ||
		credential.AccessToken == "" || credential.ExpiresAt <= now || credential.ExpiresAt-now <= 120 {
		return result, errors.New("令牌补扫窗口、限额或凭据无效")
	}
	// Do not queue behind normal pricing synchronization or overlap its shadow.
	if !m.upstreamPricingMu.TryLock() {
		return sub2TokenRecoveryResult{Status: "busy"}, nil
	}
	defer m.upstreamPricingMu.Unlock()
	var legacy map[int64][]ChannelUpstreamPricingHourEvidence
	var cursor ChannelUpstreamTokenScanCheckpoint
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current ChannelUpstreamAccount
		if err := tx.Select("domain", "provider", "base_url", "user_id", "account", "usage_adapter", "enabled", "usage_sync_enabled").Where("domain=?", account.Domain).Take(&current).Error; err != nil {
			return err
		}
		if !current.Enabled || !current.UsageSyncEnabled || current.UsageAdapter == upstreamUsageAdapterSub2Stats ||
			newAPIUpstreamAccountEpoch(current) != newAPIUpstreamAccountEpoch(account) {
			return errUpstreamTokenScope
		}
		check, err := channelCostKeyCheck(key)
		if err != nil {
			return err
		}
		var registry []ChannelCostKeyRegistry
		if err := tx.Limit(2).Find(&registry).Error; err != nil {
			return err
		}
		if len(registry) != 1 || registry[0].KeyID != keyID || !hmac.Equal([]byte(registry[0].KeyCheck), []byte(check)) {
			return errUpstreamTokenScope
		}
		var eligible int
		// Current intent needs yesterday's evidence for activation. Older days
		// must overlap historical intent; never rescan every pre-config day.
		err = tx.Raw(`SELECT 1 FROM channel_upstream_token_configs c WHERE c.domain=? AND c.account_epoch=? AND c.token_id<>'' AND c.created_at>0 AND c.created_at<=?
		 AND ((?=? AND NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision))
		 OR (c.created_at<? AND (c.created_at>=? OR NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision AND n.created_at<?)))) LIMIT 1`,
			account.Domain, newAPIUpstreamAccountEpoch(account), now, day, cstDayStart(now)-86400, day+23*3600, day, day).Scan(&eligible).Error
		if err != nil {
			return err
		}
		if eligible == 0 {
			result.Status = "not_configured"
			return nil
		}
		var syncState ChannelUpstreamPricingSyncState
		q := tx.Where("domain=? AND account_epoch=? AND semantics_version=?", account.Domain, newAPIUpstreamAccountEpoch(account), upstreamPricingSemanticsVersion).Limit(1).Find(&syncState)
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected != 1 || syncState.BackfillNextHour < day+86400 || upstreamPricingRetryDeferred(syncState, now) {
			result.Status = "waiting_legacy"
			return nil
		}
		legacy, err = readSub2TokenRecoveryLedger(tx, account, day, now)
		if err != nil {
			return err
		}
		cursor, err = readSub2TokenStoredCursor(tx, account, day)
		return err
	})
	if err != nil || result.Status != "pending" {
		return result, err
	}
	resetInvalid := func(cause error) (sub2TokenRecoveryResult, error) {
		// Only reset this token checkpoint, with CAS. Revoke any previous
		// successful observation before clearing contradictory pages.
		if cursor.SuccessRevision > 0 {
			var err error
			cursor, err = m.invalidateSub2TokenObservation(ctx, account, day, cursor, now)
			if err != nil {
				return result, errors.Join(cause, err)
			}
		}
		_, err := m.resetSub2TokenScanCheckpoint(ctx, account, day, cursor.Revision, now, key, keyID)
		return sub2TokenRecoveryResult{Status: "restart_required"}, errors.Join(cause, err)
	}
	if cursor.Envelope != "" {
		cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), account, day, key, keyID)
		if err != nil {
			return resetInvalid(err)
		}
		if cursor.SuccessRevision == cursor.Revision && cursor.SuccessRevision > 0 {
			if cursor.SuccessKeyID != keyID || cursor.SuccessHash != channelCostEvidenceContentHash(cp.Rows) || cursor.SuccessAt > now || reconcileSub2TokenShadow(cp, legacy) != nil {
				return resetInvalid(errors.New("令牌历史观察与当前账本不一致"))
			}
			if cursor.StableScans >= 2 {
				return sub2TokenRecoveryResult{Status: "observed", StableScans: 2}, nil
			}
			if now-cursor.SuccessAt < 60 {
				return sub2TokenRecoveryResult{Status: "waiting_second_scan", StableScans: cursor.StableScans}, nil
			}
			cursor, err = m.resetSub2TokenScanCheckpoint(ctx, account, day, cursor.Revision, now, key, keyID)
			if err != nil {
				return result, err
			}
		}
	}
	fetchSave := func(page int) error {
		p, err := fetchSub2PricingPage(ctx, m.channelUpstreamHTTPClient(), account, credential, day, page, pacer)
		if err != nil {
			return err
		}
		// Pure validation separates contradictory evidence from store failures.
		if _, err = advanceSub2TokenCheckpoint([]byte(cursor.Envelope), account, p, day, now, key, keyID); err != nil {
			_, err = resetInvalid(err)
			return err
		}
		cursor, err = m.saveSub2TokenScanPage(ctx, account, day, p, cursor.Revision, now, key, keyID)
		return err
	}
	yield := func(err error) (sub2TokenRecoveryResult, error) {
		var budget *upstreamUsageRunBudgetExhausted
		if errors.As(err, &budget) {
			return sub2TokenRecoveryResult{Status: "paging"}, nil
		}
		return result, err
	}
	// Always verify the first page on resume using the token-aware digest.
	if err := fetchSave(1); err != nil {
		return yield(err)
	}
	for {
		cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), account, day, key, keyID)
		if err != nil {
			return result, err
		}
		if cp.NextPage > cp.Pages {
			break
		}
		if err := fetchSave(cp.NextPage); err != nil {
			return yield(err)
		}
	}
	// Final probe also applies after a restart; a budget yield cannot become
	// success until a later turn has rechecked the completed page set.
	if err := fetchSave(1); err != nil {
		return yield(err)
	}
	legacy, err = readSub2TokenRecoveryLedger(m.storeDB.WithContext(ctx), account, day, now)
	if err != nil {
		return result, err
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), account, day, key, keyID)
	if err != nil {
		return resetInvalid(err)
	}
	if err := reconcileSub2TokenShadow(cp, legacy); err != nil {
		return resetInvalid(err)
	}
	observed, err := m.recordSub2TokenObservation(ctx, account, day, cursor, legacy, now, key, keyID)
	if err != nil {
		return result, err
	} // storage/CAS failure is not permission to reset
	cursor = observed
	result.Status, result.StableScans = "waiting_second_scan", cursor.StableScans
	if cursor.StableScans >= 2 {
		result.Status = "observed"
	}
	return result, nil
}

func readSub2TokenRecoveryLedger(tx *gorm.DB, account ChannelUpstreamAccount, day, now int64) (map[int64][]ChannelUpstreamPricingHourEvidence, error) {
	epoch := newAPIUpstreamAccountEpoch(account)
	var rows []ChannelUpstreamPricingHourEvidence
	var states []ChannelUpstreamPricingHourState
	q := func(db *gorm.DB) *gorm.DB {
		return db.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", account.Domain, epoch, upstreamPricingSemanticsVersion, day, day+86400)
	}
	if err := q(tx).Limit(sub2TokenMaxScanRows + 1).Find(&rows).Error; err != nil {
		return nil, err
	}
	if err := q(tx).Limit(25).Find(&states).Error; err != nil {
		return nil, err
	}
	if len(rows) > sub2TokenMaxScanRows || len(states) != 24 {
		return nil, errors.New("令牌补扫所需原账本尚不完整")
	}
	byHour := map[int64][]ChannelUpstreamPricingHourEvidence{}
	for hour := day; hour < day+86400; hour += 3600 {
		byHour[hour] = nil
	}
	for _, row := range rows {
		if row.Provider != account.Provider || row.HourTs%3600 != 0 {
			return nil, errUpstreamTokenScope
		}
		byHour[row.HourTs] = append(byHour[row.HourTs], row)
	}
	seen := map[int64]bool{}
	for _, state := range states {
		computed, err := pricingHourStateFromEvidence(account, state.HourTs, byHour[state.HourTs], now)
		if err != nil {
			return nil, err
		}
		if seen[state.HourTs] || state.HourTs%3600 != 0 || state.Status != "verified" || state.VerifiedScans < 2 ||
			state.ContentHash != computed.ContentHash || state.SourceRows != computed.SourceRows || state.FinalQuota != computed.FinalQuota ||
			state.Tokens != computed.Tokens || state.EligibleRequests != computed.EligibleRequests || state.EvidenceRows != computed.EvidenceRows {
			return nil, errors.New("令牌补扫原账本未通过完整性核验")
		}
		seen[state.HourTs] = true
	}
	return byHour, nil
}
