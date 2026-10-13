package monitor

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
)

const sub2TokenRecoveryRequestsPerRun = 4

type sub2TokenRecoveryCandidate struct {
	DayTs         int64
	NextAttemptAt int64
	LastAttemptAt int64
}

// Only a keyed opaque fingerprint is persisted, never credentials themselves.
// Routine encryption resealing must not wake an authentication-paused scan;
// a genuinely changed credential (or repaired decryption setup) can do so.
func (m *Monitor) sub2TokenRecoveryCredentialRevision(account ChannelUpstreamAccount) int64 {
	mac := hmac.New(sha256.New, []byte(m.cfg.ChannelCostHMACKey))
	material := []byte("unreadable\x00" + account.Credential)
	if value, err := m.credentialForAccount(account); err == nil {
		if credential, ok := value.(sub2APICredential); ok {
			if encoded, err := json.Marshal(credential); err == nil {
				material = encoded
			}
		}
	}
	mac.Write([]byte("token-recovery-credential-v1\x00" + account.Domain + "\x00"))
	mac.Write(material)
	return int64(binary.BigEndian.Uint64(mac.Sum(nil)) & 0x7fffffffffffffff)
}

func (m *Monitor) sub2TokenRecoveryEnabled(account ChannelUpstreamAccount) bool {
	return !m.cfg.LocalSnapshotOnly && account.Provider == upstreamProviderSub2API && account.UsageAdapter != upstreamUsageAdapterSub2Stats &&
		account.Enabled && account.UsageSyncEnabled && m.cfg.ChannelCostClosureEnabled && m.cfg.UpstreamPricingLedgerEnabled &&
		channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, account.Domain) && pricingLedgerDomainAllowed(m.cfg.UpstreamPricingLedgerDomains, account.Domain)
}

// Metadata only: no log envelopes or financial report computation while looking
// for work. A complete observation is periodically revalidated locally; the
// executor makes no request if its signed proof still matches the local ledger.
func (m *Monitor) nextSub2TokenRecovery(ctx context.Context, account ChannelUpstreamAccount, state ChannelUpstreamPricingSyncState, now int64) (sub2TokenRecoveryCandidate, error) {
	var candidate sub2TokenRecoveryCandidate
	if !m.sub2TokenRecoveryEnabled(account) || now <= 86400 || upstreamPricingRetryDeferred(state, now) {
		return candidate, nil
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	credentialRevision := m.sub2TokenRecoveryCredentialRevision(account)
	if state.AccountEpoch != epoch || state.Domain != account.Domain || state.SemanticsVersion != upstreamPricingSemanticsVersion {
		return candidate, errUpstreamTokenScope
	}
	var configured int
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT 1 FROM channel_upstream_token_configs WHERE domain=? AND account_epoch=? AND token_id<>'' AND created_at>0 AND created_at<=? LIMIT 1`, account.Domain, epoch, now).Scan(&configured).Error; err != nil || configured == 0 {
		return candidate, err
	}
	err := m.storeDB.WithContext(ctx).Raw(`WITH days AS (
	 SELECT hour_ts-((hour_ts+28800)%86400) AS day_ts FROM channel_upstream_pricing_hour_states
	 WHERE domain=? AND account_epoch=? AND semantics_version=? AND hour_ts<? AND hour_ts<?
	 GROUP BY day_ts HAVING COUNT(*)=24 AND MIN(CASE WHEN status='verified' AND verified_scans>=2 THEN 1 ELSE 0 END)=1)
	 SELECT d.day_ts, COALESCE(w.next_attempt_at,0) AS next_attempt_at,
	 (SELECT COALESCE(MAX(j.updated_at),0) FROM channel_upstream_token_work_states j WHERE j.domain=? AND j.account_epoch=? AND j.kind='scan_day') AS last_attempt_at
	 FROM days d LEFT JOIN channel_upstream_token_work_states w ON w.domain=? AND w.account_epoch=? AND w.kind='scan_day' AND w.channel_id=0 AND w.day_ts=d.day_ts
	 WHERE d.day_ts>0 AND d.day_ts+86400<=? AND COALESCE(w.lease_until,0)<=?
	 AND (COALESCE(w.next_attempt_at,0)<=? OR (w.status='reconnect' AND w.input_revision<>?))
	 AND NOT EXISTS(SELECT 1 FROM channel_upstream_token_work_states a WHERE a.domain=? AND a.account_epoch=? AND a.kind='scan_day' AND a.status='reconnect' AND a.input_revision=? AND a.next_attempt_at>?)
	 AND EXISTS (SELECT 1 FROM channel_upstream_token_configs c WHERE c.domain=? AND c.account_epoch=? AND c.token_id<>'' AND c.created_at>0 AND c.created_at<=?
	 AND ((d.day_ts=? AND NOT EXISTS(SELECT 1 FROM channel_upstream_token_configs n WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision))
	 OR (c.created_at<d.day_ts+82800 AND (c.created_at>=d.day_ts OR NOT EXISTS(SELECT 1 FROM channel_upstream_token_configs n WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision AND n.created_at<d.day_ts)))))
	 ORDER BY COALESCE(w.updated_at,0),CASE WHEN d.day_ts=? THEN 0 ELSE 1 END,d.day_ts LIMIT 1`,
		account.Domain, epoch, upstreamPricingSemanticsVersion, cstDayStart(now), state.BackfillNextHour,
		account.Domain, epoch, account.Domain, epoch, now, now, now, credentialRevision, account.Domain, epoch, credentialRevision, now, account.Domain, epoch, now, cstDayStart(now)-86400, cstDayStart(now)-86400).Scan(&candidate).Error
	return candidate, err
}

func (m *Monitor) syncStoredSub2TokenRecoveryAt(ctx context.Context, domain string, day, now int64) error {
	if m.cfg.LocalSnapshotOnly {
		return errUpstreamTokenReadOnly
	}
	// The 60-second metadata lease is longer than the existing <=45s operation
	// ceiling. Include credential handling in that ceiling, not only log pages.
	ctx, cancel := context.WithTimeout(ctx, upstreamPricingOperationTimeout(m.cfg))
	defer cancel()
	release, err := m.tryAcquireUpstreamAccountBackground(domain)
	if err != nil {
		return err
	}
	defer release()
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).Where("domain=?", domain).Take(&account).Error; err != nil {
		return err
	}
	if !m.sub2TokenRecoveryEnabled(account) {
		return nil
	}
	var state ChannelUpstreamPricingSyncState
	if err := m.storeDB.WithContext(ctx).Where("domain=? AND account_epoch=? AND semantics_version=?", domain, newAPIUpstreamAccountEpoch(account), upstreamPricingSemanticsVersion).Take(&state).Error; err != nil {
		return err
	}
	// A normal job that became due after candidate selection wins this turn.
	if state.TailNextSyncAt <= now || !state.BackfillDone && state.BackfillNextSyncAt <= now {
		return nil
	}
	candidate, err := m.nextSub2TokenRecovery(ctx, account, state, now)
	if err != nil || candidate.DayTs != day || day == 0 {
		return err
	}
	credentialRevision := m.sub2TokenRecoveryCredentialRevision(account)
	claim, ok, err := m.claimUpstreamTokenWork(ctx, ChannelUpstreamTokenWorkState{Domain: domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), Kind: "scan_day", DayTs: day}, credentialRevision, now)
	if err != nil || !ok {
		return err
	}
	result, runErr := m.fetchStoredSub2TokenRecovery(ctx, &account, day, now)
	credentialRevision = m.sub2TokenRecoveryCredentialRevision(account)
	status, retry, next := result.Status, false, int64(0)
	if runErr != nil {
		status, retry = "retry", true
		attempts := min(max(claim.Attempts, 0), 7) + 1
		next = pricingSyncRetryAt(now, attempts)
		if at := upstreamRetryAt(runErr); at > next {
			next = at
		}
		failureStatus := upstreamStatusError
		boundUpstreamFailure(runErr, now, &attempts, &failureStatus, &next)
		if upstreamProvenAuthenticationFailure(runErr) {
			status = "reconnect"
		} else if next == upstreamAccountIsolatedUntil {
			// Optional evidence recovery must not become permanently stranded.
			// Keep at most one low-frequency probe rather than resetting attempts.
			next = now + 6*3600
		}
	}
	finishErr := m.finishUpstreamTokenWorkWithRetryAt(ctx, claim, credentialRevision, now, status, retry, next)
	return errors.Join(runErr, finishErr)
}

func (m *Monitor) fetchStoredSub2TokenRecovery(ctx context.Context, account *ChannelUpstreamAccount, day, now int64) (sub2TokenRecoveryResult, error) {
	var empty sub2TokenRecoveryResult
	value, err := m.credentialForAccount(*account)
	if err != nil {
		return empty, err
	}
	credential, ok := value.(sub2APICredential)
	if !ok {
		return empty, errors.New("Sub2 令牌补扫凭据格式无效")
	}
	if credential.AccessToken == "" || credential.ExpiresAt <= now+120 {
		credential, err = refreshSub2API(ctx, m.channelUpstreamHTTPClient(), *account, credential)
		if err != nil {
			return empty, err
		}
		if err := m.persistSyncedUpstreamAccount(ctx, account, credential); err != nil {
			return empty, err
		}
	}
	return m.recoverSub2TokenEvidenceDay(ctx, *account, credential, day, now, newUpstreamUsageRequestPacer(sub2TokenRecoveryRequestsPerRun, upstreamUsageRequestInterval))
}
