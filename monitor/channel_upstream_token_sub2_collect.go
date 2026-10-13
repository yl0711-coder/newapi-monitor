package monitor

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"time"
)

// Optional shadow observer of pages already fetched by the account ledger.
// It never makes network requests or returns failures to the legacy collector.
// No evidence is published and no configured token is automatically verified.
type sub2TokenShadow struct {
	m       *Monitor
	ctx     context.Context
	account ChannelUpstreamAccount
	day     int64
	now     int64
	key     []byte
	keyID   string
	cursor  ChannelUpstreamTokenScanCheckpoint
	failed  bool
}

const sub2TokenShadowStoreTimeout = 500 * time.Millisecond

func (s *sub2TokenShadow) fail(reason string) {
	if s == nil || s.failed {
		return
	}
	s.failed = true
	// A resumed completed scan may already have an earlier success marker.
	// Contradictory token evidence must revoke that marker, not leave the
	// read-only checker reporting it as ready. CAS cannot revoke a newer scan.
	if s.cursor.SuccessRevision > 0 && !s.m.cfg.LocalSnapshotOnly {
		// Parent cancellation must not suppress the bounded cleanup attempt.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), sub2TokenShadowStoreTimeout)
		_, err := s.m.invalidateSub2TokenObservation(ctx, s.account, s.day, s.cursor, s.now)
		cancel()
		if err != nil {
			slog.Warn("Sub2 令牌核验标记暂未撤销", "domain", s.account.Domain, "day", s.day)
		}
	}
	// Never log the SQL error/envelope, credentials, or a raw upstream body.
	slog.Warn("Sub2 令牌证据暂不可用，原账户账本继续", "domain", s.account.Domain, "day", s.day, "reason", reason)
}

func (m *Monitor) beginSub2TokenShadow(ctx context.Context, account ChannelUpstreamAccount, day, now int64, legacy ChannelUpstreamPricingPageCheckpoint, resumed bool) *sub2TokenShadow {
	if m.cfg.LocalSnapshotOnly || !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, account.Domain) ||
		account.Provider != upstreamProviderSub2API || account.UsageAdapter == upstreamUsageAdapterSub2Stats {
		return nil
	}
	// A current setting authorizes observation for activation. After clearing,
	// retain observation only for a closed day that overlaps a historically
	// nonempty setting. This collects evidence, never grants cost attribution;
	// historical confirmed bindings remain mandatory in the cost writer.
	readCtx, cancel := context.WithTimeout(ctx, sub2TokenShadowStoreTimeout)
	defer cancel()
	epoch := newAPIUpstreamAccountEpoch(account)
	var configured int
	err := m.storeDB.WithContext(readCtx).Raw(`SELECT 1 FROM channel_upstream_token_configs c
		WHERE c.domain=? AND c.account_epoch=? AND c.token_id<>''
		AND NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n
		 WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision) LIMIT 1`, account.Domain, epoch).Scan(&configured).Error
	if err == nil && configured == 0 && day >= 0 && day <= math.MaxInt64-86400 && day == cstDayStart(day) && day+86400 <= now {
		// Configurations take effect no earlier than the next whole hour.
		// Latest pre-day nonempty head or an in-day nonempty revision with a
		// possible whole hour remaining. Do not keep observing future days
		// indefinitely merely because this account once had a token setting.
		err = m.storeDB.WithContext(readCtx).Raw(`SELECT 1 FROM channel_upstream_token_configs c
		 WHERE c.domain=? AND c.account_epoch=? AND c.token_id<>'' AND c.created_at>0 AND c.created_at<?
		 AND (c.created_at>=? OR NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n
		 WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision AND n.created_at<?)) LIMIT 1`,
			account.Domain, epoch, day+23*3600, day, day).Scan(&configured).Error
	}
	if err != nil {
		slog.Warn("Sub2 令牌配置暂不可读，原账户账本继续", "domain", account.Domain, "reason", "configuration_unavailable")
		return nil
	}
	if configured == 0 {
		return nil
	}
	s := &sub2TokenShadow{m: m, ctx: ctx, account: account, day: day, now: now,
		key: []byte(m.cfg.ChannelCostHMACKey), keyID: m.cfg.ChannelCostHMACKeyID}
	if validateSub2TokenContext(account, day, s.key, s.keyID) != nil {
		s.fail("invalid_context")
		return s
	}
	if resumed {
		// Revoke a completed marker BEFORE reading/validating another page.
		// Read raw staging first so even a corrupt envelope is invalidated.
		s.cursor, err = readSub2TokenStoredCursor(m.storeDB.WithContext(readCtx), account, day)
		if err != nil || s.cursor.Envelope == "" {
			s.fail("missing_or_invalid_resume_cursor")
			return s
		}
		if s.cursor.SuccessRevision > 0 && s.cursor.SuccessRevision == s.cursor.Revision {
			var claimed ChannelUpstreamTokenScanCheckpoint
			claimed, err = m.invalidateSub2TokenObservation(readCtx, account, day, s.cursor, now)
			if err != nil {
				s.fail("verification_invalidation_failed")
				return s
			}
			s.cursor = claimed
		}
		cp, decodeErr := decodeSub2TokenCheckpoint([]byte(s.cursor.Envelope), account, day, s.key, s.keyID)
		if decodeErr != nil || cp.NextPage != legacy.NextPage || cp.Total != legacy.Total || int64(len(cp.SeenRows)) != legacy.SourceRows {
			s.fail("resume_cursor_mismatch")
		}
		return s
	}
	// A fresh legacy scan must also read token evidence afresh; never reuse a
	// completed shadow as an independent second scan. Only staging is reset.
	s.cursor, err = readSub2TokenStoredCursor(m.storeDB.WithContext(readCtx), account, day)
	if err == nil && s.cursor.Envelope != "" {
		s.cursor, err = m.resetSub2TokenScanCheckpoint(readCtx, account, day, s.cursor.Revision, now, s.key, s.keyID)
	}
	if err != nil {
		s.fail("cursor_reset_failed")
	}
	return s
}

func (s *sub2TokenShadow) observe(page sub2PricingPage) {
	if s == nil || s.failed {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, sub2TokenShadowStoreTimeout)
	defer cancel()
	row, err := s.m.saveSub2TokenScanPage(ctx, s.account, s.day, page, s.cursor.Revision, s.now, s.key, s.keyID)
	if err != nil {
		s.fail("page_rejected_or_store_failed")
		return
	}
	s.cursor = row
}

// Complete pages must conserve each hour/pricing dimension of the legacy
// ledger, not just the daily grand total (which could hide swapped hours).
// This is an intra-scan check only, not independent upstream reconciliation.
func reconcileSub2TokenShadow(cp sub2TokenCheckpoint, legacy map[int64][]ChannelUpstreamPricingHourEvidence) error {
	if cp.NextPage != cp.Pages+1 || int64(len(cp.SeenRows)) != cp.Total {
		return errors.New("令牌影子扫描尚未完成")
	}
	type totals struct{ charge, requests, prompt, completion int64 }
	want, got := make(map[string]totals), make(map[string]totals)
	keyFor := func(hour int64, dimension string) string { return strconv.FormatInt(hour, 10) + ":" + dimension }
	for hour := cp.Day; hour < cp.Day+86400; hour += 3600 {
		rows, exists := legacy[hour]
		if !exists {
			return errors.New("账户账本缺少小时窗口")
		}
		for _, row := range rows {
			k := keyFor(hour, row.DimensionHash)
			if _, exists := want[k]; exists || row.HourTs != hour || row.Domain != cp.Domain || row.AccountEpoch != cp.AccountEpoch {
				return errors.New("账户账本维度或身份不一致")
			}
			want[k] = totals{row.FinalQuota, row.EligibleRequests, row.PromptTokens, row.CompletionTokens}
		}
	}
	for _, row := range cp.Rows {
		k := keyFor(row.HourTs, row.PricingDimensionHash)
		v := got[k]
		for _, pair := range []struct {
			target *int64
			n      int64
		}{{&v.charge, row.ChargeUnits}, {&v.requests, row.Requests}, {&v.prompt, row.PromptTokens}, {&v.completion, row.CompletionTokens}} {
			if err := addPricingCounter(pair.target, pair.n); err != nil {
				return err
			}
		}
		got[k] = v
	}
	if len(want) != len(got) {
		return errors.New("令牌影子与账户账本维度数量不同")
	}
	for k, v := range want {
		if other, exists := got[k]; !exists || other != v {
			return errors.New("令牌影子与账户账本金额或请求数不一致")
		}
	}
	return nil
}

func (s *sub2TokenShadow) finish(legacy map[int64][]ChannelUpstreamPricingHourEvidence) {
	if s == nil || s.failed {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, sub2TokenShadowStoreTimeout)
	defer cancel()
	row, err := s.m.recordSub2TokenObservation(ctx, s.account, s.day, s.cursor, legacy, s.now, s.key, s.keyID)
	if err != nil {
		s.fail("observation_rejected_or_store_failed")
		return
	}
	s.cursor = row
	// Intentionally no publication, mapping activation or queue writes here.
}
