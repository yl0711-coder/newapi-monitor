package monitor

// Local, bounded end-to-end Sub2 cost turn. Deliberately not registered with a
// timer or HTTP route. NewAPI keeps its existing cost/publication workers.
// A closed day is prepared atomically; up to eight pending hours are then
// published independently and idempotently. All inputs are existing local facts.

import (
	"context"
	"errors"
	"math"

	"gorm.io/gorm"
)

type sub2TokenDayResult struct {
	Status            string
	PreparationStatus string // preparing new costs and draining old jobs are independent
	Cost              sub2TokenCostCommitResult
	PublishedHours    int
	PendingHours      int64
	PendingKnown      bool // an interrupted/error turn must not report unknown as zero
}

func (m *Monitor) advanceSub2TokenCostDay(ctx context.Context, domain string, channelID int, day, now int64) (sub2TokenDayResult, error) {
	var result sub2TokenDayResult
	if m == nil || m.storeDB == nil {
		return result, errors.New("令牌成本存储未就绪")
	}
	if m.cfg.LocalSnapshotOnly {
		return result, errUpstreamTokenReadOnly
	}
	domain, err := normalizeUpstreamTokenScope(domain, channelID)
	if err != nil {
		return result, err
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, domain) {
		return sub2TokenDayResult{Status: "disabled"}, nil
	}
	if day < 0 || day > math.MaxInt64-86400 || day != cstDayStart(day) || now < day+86400 || now > math.MaxInt64-3840 {
		return result, errors.New("令牌成本只处理已闭合自然日")
	}
	var account ChannelUpstreamAccount
	var in sub2TokenCostCommitInput
	var from int64
	status := "ready"
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		account, err = readUpstreamTokenScope(tx, domain, channelID)
		if err != nil {
			return err
		}
		if account.Provider != upstreamProviderSub2API || account.UsageAdapter == upstreamUsageAdapterSub2Stats {
			status = "unsupported"
			return nil
		}
		config, err := readUpstreamTokenConfig(tx, account, channelID)
		if err != nil {
			return err
		}
		if config.TokenID == "" {
			status = "unconfigured"
			return nil
		}
		if config.UpdatedAt <= 0 || config.UpdatedAt > now || config.UpdatedAt > math.MaxInt64-3600 {
			return errUpstreamTokenConflict
		}
		// Optional configuration must have completed a future-only activation.
		// Never turn an old proof day into a newly attributed historical bill.
		var first int64
		if err := tx.Model(&ChannelCostSourceBinding{}).Select("COALESCE(MIN(valid_from),0)").
			Where("domain=? AND account_epoch=? AND mapping_source=? AND status='confirmed'", domain, config.AccountEpoch, upstreamTokenBindingMappingSource).Scan(&first).Error; err != nil {
			return err
		}
		if first <= 0 {
			// Matching manual ownership is intentionally not taken over by the
			// optional-setting planner. Accept it only if the entire account's
			// current plan is unchanged; preserve every original interval.
			var digest string
			first, digest, err = m.readSub2TokenManualBoundary(tx, account, config, now)
			if err != nil {
				return err
			}
			if first <= 0 {
				status = "waiting_binding"
				return nil
			}
			in.BindingPlanDigest = digest
		}
		from = (config.UpdatedAt/3600 + 1) * 3600
		if first > from {
			from = first
		}
		if from >= day+86400 {
			status = "before_effective_window"
			return nil
		}
		if in.BindingPlanDigest == "" {
			// Managed and manual sources can coexist in one account. A
			// managed interval alone must not bypass a conflict elsewhere.
			snapshot, err := readUpstreamTokenBindingSnapshot(tx, domain, channelID, []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID, now)
			if err != nil {
				return err
			}
			if len(snapshot.Plan.Changes) != 0 {
				status = "waiting_binding"
				return nil
			}
			in.BindingPlanDigest = snapshot.Plan.Digest
		}
		cursor, err := readSub2TokenStoredCursor(tx, account, day)
		if err != nil {
			return err
		}
		if cursor.Revision <= 0 || cursor.SuccessRevision != cursor.Revision || cursor.StableScans < 2 ||
			cursor.SuccessKeyID != m.cfg.ChannelCostHMACKeyID || !validSHA256Hex(cursor.SuccessHash) || cursor.SuccessAt < day+86400 || cursor.SuccessAt > now {
			status = "waiting_evidence"
			return nil
		}
		in = sub2TokenCostCommitInput{Domain: domain, ChannelID: channelID, AccountEpoch: config.AccountEpoch,
			ConfigRevision: config.Revision, DayTs: day, CursorRevision: cursor.Revision, SuccessHash: cursor.SuccessHash, NotBeforeHour: from, BindingPlanDigest: in.BindingPlanDigest}
		return nil
	})
	if err != nil {
		return result, err
	}
	if status == "unsupported" {
		return sub2TokenDayResult{Status: status}, nil
	}
	result.PreparationStatus = status
	result.Status = status
	if status == "ready" {
		result.Cost, err = m.commitSub2TokenCostCandidates(ctx, in, now)
		if err != nil {
			return result, err
		}
		result.Status = "costs_prepared"
	}
	// The current optional setting only gates NEW cost preparation. Jobs
	// already committed under an earlier setting must not disappear behind
	// a new activation date, an empty setting, or a changed token ID. Their
	// publisher rechecks evidence and resolves ownership AS OF each hour.
	// Never create costs or queue jobs here to fill a historical gap.
	epoch := newAPIUpstreamAccountEpoch(account)
	scope := func(tx *gorm.DB) *gorm.DB {
		return tx.Where("domain=? AND account_epoch=? AND hour_ts>=? AND hour_ts<? AND status='pending'", domain, epoch, day, day+86400)
	}
	for i := 0; i < channelEconomicsPublishPerTick; i++ {
		published, err := m.publishOneDueSub2TokenEconomicsHour(ctx, account, day, now, true)
		if err != nil {
			result.Status = "publication_retry"
			return result, err
		}
		if !published {
			break
		}
		result.PublishedHours++
	}
	if err := scope(m.storeDB.WithContext(ctx).Model(&ChannelEconomicsDirtyHour{})).Count(&result.PendingHours).Error; err != nil {
		return result, err
	}
	result.PendingKnown = true
	if result.PendingHours > 0 {
		result.Status = "pending"
	} else if status == "ready" || result.PublishedHours > 0 {
		result.Status = "processed" // processing finished, not a claim of financial completeness
	}
	return result, nil
}

// No new binding is inferred here. A matching open-ended confirmed interval
// must already exist for the selected token. Finite historical mappings alone
// cannot activate ongoing processing. The config's next-hour floor still
// applies, even if the manual interval is older.
func (m *Monitor) readSub2TokenManualBoundary(tx *gorm.DB, account ChannelUpstreamAccount, config upstreamTokenConfigView, now int64) (int64, string, error) {
	key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
	snapshot, err := readUpstreamTokenBindingSnapshot(tx, account.Domain, config.ChannelID, key, keyID, now)
	if err != nil {
		return 0, "", err
	}
	if len(snapshot.Plan.Changes) != 0 {
		return 0, "", nil
	}
	source, err := channelCostSourceRef(key, account.Provider, config.AccountEpoch, sub2TokenSourceKind, config.TokenID)
	if err != nil {
		return 0, "", err
	}
	var binding ChannelCostSourceBinding
	q := tx.Where("domain=? AND account_epoch=? AND source_ref=? AND status='confirmed' AND valid_to=0", account.Domain, config.AccountEpoch, source).Limit(1).Find(&binding)
	if q.Error != nil {
		return 0, "", q.Error
	}
	if q.RowsAffected == 0 || binding.MappingSource == upstreamTokenBindingMappingSource || binding.ValidFrom <= 0 {
		return 0, "", nil
	}
	return binding.ValidFrom, snapshot.Plan.Digest, nil
}
