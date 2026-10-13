package monitor

// Internal commit boundary, not yet wired to a worker or HTTP route. It must
// be called with server-side time and a previously observed config/cursor CAS.
// There are no network calls, bindings, finance versions or legacy bill writes.

import (
	"context"
	"crypto/hmac"
	"errors"
	"math"

	"gorm.io/gorm"
)

type sub2TokenCostCommitInput struct {
	Domain                 string
	ChannelID              int
	AccountEpoch           string
	ConfigRevision         int64
	DayTs                  int64
	CursorRevision         int64
	SuccessHash            string
	NotBeforeHour          int64  // optional orchestration boundary; can only narrow writes
	BindingPlanDigest      string // optional fence for account-wide ownership at preparation
	HistoricalWindowDigest string // alternative: exact historical configuration/binding window
}

type sub2TokenCostCommitResult struct {
	UpdatedHours   int
	UnchangedHours int
	SkippedHours   int
	QueuedHours    int
}

func sub2CostHourScope(tx *gorm.DB, domain, epoch string, hour int64) *gorm.DB {
	return tx.Where("domain=? AND account_epoch=? AND hour_ts=? AND semantics_version=?", domain, epoch, hour, channelCostEvidenceSemanticsVersion)
}

func sameSub2CostState(a, b ChannelUpstreamCostHourState) bool {
	// A repeat scan with unchanged money must not churn timestamps or queues.
	a.CompletedAt, a.UpdatedAt, b.CompletedAt, b.UpdatedAt = 0, 0, 0, 0
	return a == b
}

func (m *Monitor) commitSub2TokenCostCandidates(ctx context.Context, in sub2TokenCostCommitInput, now int64) (sub2TokenCostCommitResult, error) {
	var result sub2TokenCostCommitResult
	if m.cfg.LocalSnapshotOnly {
		return result, errUpstreamTokenReadOnly
	}
	domain, err := normalizeUpstreamTokenScope(in.Domain, in.ChannelID)
	if err != nil {
		return result, err
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, domain) ||
		!validSHA256Hex(in.AccountEpoch) || !validSHA256Hex(in.SuccessHash) || in.ConfigRevision <= 0 || in.CursorRevision <= 0 || now <= 0 || in.NotBeforeHour < 0 || in.NotBeforeHour%3600 != 0 {
		return result, errors.New("Sub2 令牌成本写入未启用或版本参数无效")
	}
	key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
	keyCheck, err := channelCostKeyCheck(key)
	if err != nil {
		return result, err
	}
	if err := m.lockUpstreamTokenWrite(ctx); err != nil {
		return result, err
	}
	defer m.channelCostBindingMu.Unlock()
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		account, err := readUpstreamTokenScope(tx, domain, in.ChannelID)
		if err != nil {
			return err
		}
		if err := validateSub2TokenContext(account, in.DayTs, key, keyID); err != nil {
			return err
		}
		config, err := readUpstreamTokenConfig(tx, account, in.ChannelID)
		if err != nil {
			return err
		}
		historical := in.HistoricalWindowDigest != ""
		if config.AccountEpoch != in.AccountEpoch || config.Revision != in.ConfigRevision || (!historical && config.TokenID == "") ||
			config.State == "unsupported" || config.UpdatedAt <= 0 || config.UpdatedAt > now || config.UpdatedAt > math.MaxInt64-3600 {
			return errUpstreamTokenConflict
		}
		var window sub2TokenHistoricalWindow
		if historical {
			if !validSHA256Hex(in.HistoricalWindowDigest) || in.BindingPlanDigest != "" || in.NotBeforeHour != 0 {
				return errUpstreamTokenConflict
			}
			window, err = readSub2TokenHistoricalWindow(tx, account, in.ChannelID, in.DayTs, now, key, keyID)
			if err != nil {
				return err
			}
			if window.Digest != in.HistoricalWindowDigest || window.EligibleHours == 0 {
				return errUpstreamTokenConflict
			}
		}
		if in.BindingPlanDigest != "" {
			// A read-side compatibility check is not write authority. Rebuild
			// the full bounded account plan in this transaction: another owner,
			// interval, config or key must invalidate the earlier decision.
			snapshot, err := readUpstreamTokenBindingSnapshot(tx, domain, in.ChannelID, key, keyID, now)
			if err != nil {
				return err
			}
			if !validSHA256Hex(in.BindingPlanDigest) || snapshot.Plan.Digest != in.BindingPlanDigest || len(snapshot.Plan.Changes) != 0 {
				return errUpstreamTokenConflict
			}
		}
		var registry []ChannelCostKeyRegistry
		if err := tx.Limit(2).Find(&registry).Error; err != nil {
			return err
		}
		if len(registry) != 1 || registry[0].KeyID != keyID || !hmac.Equal([]byte(registry[0].KeyCheck), []byte(keyCheck)) {
			return errors.New("Sub2 成本来源密钥与登记记录不一致")
		}
		cursor, err := readSub2TokenStoredCursor(tx, account, in.DayTs)
		if err != nil {
			return err
		}
		if cursor.Revision != in.CursorRevision || cursor.SuccessRevision != in.CursorRevision || cursor.SuccessHash != in.SuccessHash {
			return errSub2TokenCursorConflict
		}
		var pricing []ChannelUpstreamPricingHourEvidence
		var states []ChannelUpstreamPricingHourState
		var controls []ChannelUpstreamUsageHour
		if err := tx.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", domain, in.AccountEpoch, upstreamPricingSemanticsVersion, in.DayTs, in.DayTs+86400).Limit(sub2TokenMaxScanRows + 1).Find(&pricing).Error; err != nil {
			return err
		}
		if err := tx.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", domain, in.AccountEpoch, upstreamPricingSemanticsVersion, in.DayTs, in.DayTs+86400).Limit(25).Find(&states).Error; err != nil {
			return err
		}
		if err := tx.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, in.DayTs, in.DayTs+86400).Limit(25).Find(&controls).Error; err != nil {
			return err
		}
		tokenID := config.TokenID
		if historical {
			tokenID = window.TokenID
		}
		candidates, err := buildSub2TokenCostCandidatesChecked(account, tokenID, in.DayTs, now, cursor, pricing, states, controls, key, keyID, historical)
		if err != nil {
			return err
		}
		// Never backdate the optional setting. Mid-hour changes take effect at
		// the next full hour; the unfinished boundary hour keeps old semantics.
		from := (config.UpdatedAt/3600 + 1) * 3600
		if in.NotBeforeHour > from {
			from = in.NotBeforeHour
		}
		for index, candidate := range candidates {
			s := candidate.State
			if historical && !window.Hours[index] || !historical && s.HourTs < from {
				result.SkippedHours++
				continue
			}
			if s.Status != "verified" || s.ReconcileStatus != "matched" {
				return errors.New("Sub2 舍入差异暂只保留核验结果，不发布正式成本")
			}
			var previous ChannelUpstreamCostHourState
			q := sub2CostHourScope(tx, domain, in.AccountEpoch, s.HourTs)
			if err := q.Limit(1).Find(&previous).Error; err != nil {
				return err
			}
			if previous.Domain != "" && previous.Provider != upstreamProviderSub2API {
				return errUpstreamTokenScope
			}
			var oldRows []ChannelUpstreamCostHourEvidence
			if err := sub2CostHourScope(tx, domain, in.AccountEpoch, s.HourTs).Limit(sub2TokenMaxScanRows + 1).Find(&oldRows).Error; err != nil {
				return err
			}
			if len(oldRows) > sub2TokenMaxScanRows {
				return errors.New("Sub2 已有成本明细超出安全边界")
			}
			sameRows := len(oldRows) == len(candidate.Rows)
			for _, row := range oldRows {
				if row.ContentHash != channelCostEvidenceRowHash(row) {
					sameRows = false
					break
				}
			}
			if sameSub2CostState(previous, s) && sameRows && channelCostEvidenceContentHash(oldRows) == s.ContentHash {
				result.UnchangedHours++
				continue
			}
			if err := sub2CostHourScope(tx, domain, in.AccountEpoch, s.HourTs).Delete(&ChannelUpstreamCostHourEvidence{}).Error; err != nil {
				return err
			}
			if len(candidate.Rows) > 0 {
				if err := tx.CreateInBatches(&candidate.Rows, 100).Error; err != nil {
					return err
				}
			}
			if err := tx.Save(&s).Error; err != nil {
				return err
			}
			// No mapping is inferred from config. Only already-confirmed source
			// ownership makes this hour eligible for downstream recomputation.
			var bound int64
			// Include empty hours: a revised zero must supersede any previously
			// published nonzero amount, even after its last evidence row vanished.
			if err := tx.Model(&ChannelCostSourceBinding{}).
				Where("domain=? AND account_epoch=? AND status='confirmed' AND valid_from<=? AND (valid_to=0 OR valid_to>?)", domain, in.AccountEpoch, s.HourTs, s.HourTs).
				Count(&bound).Error; err != nil {
				return err
			}
			if bound > 0 {
				dirty := ChannelEconomicsDirtyHour{Domain: domain, AccountEpoch: in.AccountEpoch, HourTs: s.HourTs, Reason: "cost_verified", Generation: 1, Status: "pending", NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
				if err := upsertChannelEconomicsDirtyTx(tx, dirty); err != nil {
					return err
				}
				result.QueuedHours++
			}
			result.UpdatedHours++
		}
		return nil
	})
	if err != nil {
		return sub2TokenCostCommitResult{}, err
	}
	return result, nil
}
