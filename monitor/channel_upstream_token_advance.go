package monitor

// Account-scoped local orchestration, called by the bounded economics pipeline
// but never an HTTP route. It consumes existing evidence only and never downloads
// logs, publishes money, rewrites historical bindings or enqueues backfills.

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

type upstreamTokenAdvanceResult struct {
	Status         string
	ChangedSources int
	EffectiveFrom  int64
}

func (m *Monitor) advanceUpstreamTokenBindings(ctx context.Context, domain string, channelID int, now int64) (upstreamTokenAdvanceResult, error) {
	var empty upstreamTokenAdvanceResult
	if m == nil || m.storeDB == nil {
		return empty, errors.New("令牌归属存储未就绪")
	}
	if m.cfg.LocalSnapshotOnly {
		return empty, errUpstreamTokenReadOnly
	}
	domain, err := normalizeUpstreamTokenScope(domain, channelID)
	if err != nil {
		return empty, err
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, domain) {
		return upstreamTokenAdvanceResult{Status: "disabled"}, nil
	}
	if now <= 0 || cstDayStart(now) < 86400 {
		return empty, errors.New("令牌归属核验时间无效")
	}
	key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
	var snapshot upstreamTokenBindingSnapshot
	var input upstreamTokenBindingCommitInput
	status := "ready"
	// Plan and evidence identity are read in one consistent snapshot. The
	// writer must independently recheck both, so config or evidence changes
	// during this handoff cannot be silently accepted.
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		account, err := readUpstreamTokenScope(tx, domain, channelID)
		if err != nil {
			return err
		}
		// Unconfigured providers/empty legacy channels must not require a key
		// registry, checkpoint, or even a supported token adapter to keep working.
		var configured struct{ ChannelID int }
		found := tx.Model(&ChannelUpstreamTokenConfig{}).Select("channel_id").Where("domain=? AND account_epoch=?", domain, newAPIUpstreamAccountEpoch(account)).Limit(1).Find(&configured)
		if found.Error != nil {
			return found.Error
		}
		if found.RowsAffected == 0 {
			status = "unconfigured"
			return nil
		}
		if _, err := upstreamTokenBindingKind(account); err != nil {
			status = "unsupported"
			return nil
		}
		snapshot, err = readUpstreamTokenBindingSnapshot(tx, domain, channelID, key, keyID, now)
		if err != nil {
			return err
		}
		if len(snapshot.Plan.Changes) == 0 {
			status = "unchanged" // no proof rescan, revision churn or writes
			return nil
		}
		input = upstreamTokenBindingCommitInput{Domain: domain, ChannelID: channelID,
			AccountEpoch: newAPIUpstreamAccountEpoch(snapshot.Account), PlanDigest: snapshot.Plan.Digest}
		if len(snapshot.Plan.RequiredSources) == 0 {
			return nil // explicit clearing must work without upstream evidence
		}
		input.ProofDayTs = cstDayStart(now) - 86400
		if snapshot.Account.Provider == upstreamProviderSub2API {
			cursor, err := readSub2TokenStoredCursor(tx, snapshot.Account, input.ProofDayTs)
			if err != nil {
				return err
			}
			if cursor.Revision <= 0 || cursor.SuccessRevision != cursor.Revision || cursor.StableScans < 2 || cursor.SuccessKeyID != keyID ||
				cursor.SuccessAt < input.ProofDayTs+86400 || cursor.SuccessAt > now || !validSHA256Hex(cursor.SuccessHash) {
				status = "waiting_evidence"
				return nil
			}
			input.ProofCursorRevision, input.ProofSuccessHash = cursor.Revision, cursor.SuccessHash
		} else {
			proof, err := readNewAPITokenProof(tx, snapshot.Account, input.ProofDayTs, now, keyID)
			if err != nil {
				return err
			}
			if proof.View.Status != "evidence_ready" || !validSHA256Hex(proof.Hash) {
				status = "waiting_evidence"
				return nil
			}
			for _, source := range snapshot.Plan.RequiredSources {
				if !proof.Sources[source] {
					status = "waiting_evidence"
					return nil
				}
			}
			input.ProofSuccessHash = proof.Hash
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	if status != "ready" {
		return upstreamTokenAdvanceResult{Status: status}, nil
	}
	result, err := m.commitUpstreamTokenBindingPlan(ctx, input, "monitor-token-config", now, snapshot.Account.Provider)
	if err != nil {
		return empty, err // fail closed; next turn rebuilds the entire plan
	}
	status = "unchanged"
	if result.ChangedSources > 0 {
		status = "scheduled" // future binding != published/complete cost report
	}
	return upstreamTokenAdvanceResult{Status: status, ChangedSources: result.ChangedSources, EffectiveFrom: result.EffectiveFrom}, nil
}
