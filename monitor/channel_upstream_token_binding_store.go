package monitor

// Internal only: no HTTP route or production timer. The local account turn supplies a
// previously read snapshot digest; it cannot submit arbitrary binding rows.
// This writes future ownership, never money or a historical repair queue.

import (
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type upstreamTokenBindingCommitInput struct {
	Domain              string
	ChannelID           int
	AccountEpoch        string
	PlanDigest          string
	ProofDayTs          int64
	ProofCursorRevision int64
	ProofSuccessHash    string
}

type upstreamTokenBindingCommitResult struct {
	ChangedSources int
	EffectiveFrom  int64
}

func verifySub2TokenBindingProof(tx *gorm.DB, snapshot upstreamTokenBindingSnapshot, in upstreamTokenBindingCommitInput, now int64, key []byte, keyID string) error {
	// Empty/unchanged/cleared settings need no upstream call or fresh scan.
	if len(snapshot.Plan.RequiredSources) == 0 {
		return nil
	}
	account := snapshot.Account
	if in.ProofDayTs != cstDayStart(now)-86400 || in.ProofCursorRevision <= 0 || !validSHA256Hex(in.ProofSuccessHash) {
		return errors.New("令牌归属需要最近已闭合自然日的完整证据")
	}
	if err := validateSub2TokenContext(account, in.ProofDayTs, key, keyID); err != nil {
		return err
	}
	cursor, err := readSub2TokenStoredCursor(tx, account, in.ProofDayTs)
	if err != nil {
		return err
	}
	if cursor.Revision != in.ProofCursorRevision || cursor.SuccessRevision != in.ProofCursorRevision || cursor.SuccessHash != in.ProofSuccessHash {
		return errSub2TokenCursorConflict
	}
	var pricing []ChannelUpstreamPricingHourEvidence
	var states []ChannelUpstreamPricingHourState
	var controls []ChannelUpstreamUsageHour
	if err := tx.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", account.Domain, in.AccountEpoch, upstreamPricingSemanticsVersion, in.ProofDayTs, in.ProofDayTs+86400).Limit(sub2TokenMaxScanRows + 1).Find(&pricing).Error; err != nil {
		return err
	}
	if err := tx.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", account.Domain, in.AccountEpoch, upstreamPricingSemanticsVersion, in.ProofDayTs, in.ProofDayTs+86400).Limit(25).Find(&states).Error; err != nil {
		return err
	}
	if err := tx.Where("domain=? AND hour_ts>=? AND hour_ts<?", account.Domain, in.ProofDayTs, in.ProofDayTs+86400).Limit(25).Find(&controls).Error; err != nil {
		return err
	}
	var token string
	required := make(map[string]bool, len(snapshot.Plan.RequiredSources))
	for _, source := range snapshot.Plan.RequiredSources {
		required[source] = true
	}
	for _, c := range snapshot.Configs {
		if c.TokenID != "" {
			source, err := channelCostSourceRef(key, account.Provider, in.AccountEpoch, sub2TokenSourceKind, c.TokenID)
			if err != nil {
				return err
			}
			if required[source] {
				token = c.TokenID
				break
			}
		}
	}
	candidates, err := buildSub2TokenCostCandidates(account, token, in.ProofDayTs, now, cursor, pricing, states, controls, key, keyID)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, hour := range candidates {
		if hour.State.Status != "verified" || hour.State.ReconcileStatus != "matched" {
			return errors.New("令牌归属证据尚未精确对账，不启用切换")
		}
		for _, row := range hour.Rows {
			seen[row.SourceRef] = true
		}
	}
	for _, source := range snapshot.Plan.RequiredSources {
		if !seen[source] {
			return errors.New("配置令牌尚未出现在已核验日志中，不启用切换")
		}
	}
	return nil
}

func (m *Monitor) commitSub2TokenBindingPlan(ctx context.Context, in upstreamTokenBindingCommitInput, actor string, now int64) (upstreamTokenBindingCommitResult, error) {
	return m.commitUpstreamTokenBindingPlan(ctx, in, actor, now, upstreamProviderSub2API)
}

func (m *Monitor) commitNewAPITokenBindingPlan(ctx context.Context, in upstreamTokenBindingCommitInput, actor string, now int64) (upstreamTokenBindingCommitResult, error) {
	return m.commitUpstreamTokenBindingPlan(ctx, in, actor, now, upstreamProviderNewAPI)
}

func (m *Monitor) commitUpstreamTokenBindingPlan(ctx context.Context, in upstreamTokenBindingCommitInput, actor string, now int64, provider string) (upstreamTokenBindingCommitResult, error) {
	var empty upstreamTokenBindingCommitResult
	if m.cfg.LocalSnapshotOnly {
		return empty, errUpstreamTokenReadOnly
	}
	domain, err := normalizeUpstreamTokenScope(in.Domain, in.ChannelID)
	if err != nil {
		return empty, err
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, domain) || !validSHA256Hex(in.AccountEpoch) || !validSHA256Hex(in.PlanDigest) {
		return empty, errors.New("令牌归属写入未启用或快照无效")
	}
	actor = strings.TrimSpace(actor)
	if actor == "" || len([]rune(actor)) > 128 || strings.ContainsAny(actor, "\r\n\x00") {
		return empty, errors.New("令牌归属操作人无效")
	}
	key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
	keyCheck, err := channelCostKeyCheck(key)
	if err != nil {
		return empty, err
	}
	if err := m.lockUpstreamTokenWrite(ctx); err != nil {
		return empty, err
	}
	defer m.channelCostBindingMu.Unlock()
	var result upstreamTokenBindingCommitResult
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		snapshot, err := readUpstreamTokenBindingSnapshot(tx, domain, in.ChannelID, key, keyID, now)
		if err != nil {
			return err
		}
		if snapshot.Account.Provider != provider || (provider != upstreamProviderNewAPI && provider != upstreamProviderSub2API) {
			return errors.New("该上游的令牌写入适配尚未启用")
		}
		if newAPIUpstreamAccountEpoch(snapshot.Account) != in.AccountEpoch || snapshot.Plan.Digest != in.PlanDigest {
			return errUpstreamTokenConflict
		}
		var registry []ChannelCostKeyRegistry
		if err := tx.Limit(2).Find(&registry).Error; err != nil {
			return err
		}
		if len(registry) != 1 || registry[0].KeyID != keyID || !hmac.Equal([]byte(registry[0].KeyCheck), []byte(keyCheck)) {
			return errors.New("令牌来源密钥与登记记录不一致")
		}
		if provider == upstreamProviderNewAPI {
			err = verifyNewAPITokenBindingProof(tx, snapshot, in, now, keyID)
		} else {
			err = verifySub2TokenBindingProof(tx, snapshot, in, now, key, keyID)
		}
		if err != nil {
			return err
		}
		for _, change := range snapshot.Plan.Changes {
			row := change.After
			row.CreatedBy = actor
			ownerHash := upstreamTokenBindingOwnerHash(row)
			row.Reason = "token_config plan=" + snapshot.Plan.Digest
			if row.AllocationMode != "unallocated" {
				if ownerHash == "" {
					return errors.New("令牌归属缺少已核验配置指纹")
				}
				row.Reason += " config=" + ownerHash
			}
			if len(snapshot.Plan.RequiredSources) > 0 {
				row.Reason += fmt.Sprintf(" proof_day=%d revision=%d hash=%s", in.ProofDayTs, in.ProofCursorRevision, in.ProofSuccessHash)
			}
			if err := validateCostSourceBinding(row); err != nil {
				return err
			}
			if row.ValidFrom <= now {
				return errors.New("令牌切换不得回溯历史")
			}
			if change.Before != nil {
				old := *change.Before
				q := tx.Where("domain=? AND account_epoch=? AND source_ref=? AND valid_from=? AND valid_to=0 AND status='confirmed'", old.Domain, old.AccountEpoch, old.SourceRef, old.ValidFrom)
				var changed *gorm.DB
				if change.ReplacePending {
					if old.ValidFrom <= now || old.ValidFrom != row.ValidFrom {
						return errUpstreamTokenConflict
					}
					changed = q.Delete(&ChannelCostSourceBinding{})
				} else {
					if old.ValidFrom >= row.ValidFrom {
						return errUpstreamTokenConflict
					}
					changed = q.Model(&ChannelCostSourceBinding{}).UpdateColumn("valid_to", row.ValidFrom)
				}
				if changed.Error != nil {
					return changed.Error
				}
				if changed.RowsAffected != 1 {
					return errUpstreamTokenConflict
				}
			}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
			result.ChangedSources++
		}
		if result.ChangedSources > 0 {
			result.EffectiveFrom = snapshot.Plan.EffectiveFrom
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	return result, nil
}
