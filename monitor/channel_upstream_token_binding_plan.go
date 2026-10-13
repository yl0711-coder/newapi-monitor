package monitor

// Planning only. A plan is neither proof of a token's identity nor permission
// to publish attribution. The internal writer re-reads this snapshot, compares
// Digest and verifies every RequiredSource. No HTTP or background caller is
// enabled until downstream attribution and lifecycle acceptance are complete.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"

	"gorm.io/gorm"
)

const upstreamTokenBindingMappingSource = "token_config_v1"
const upstreamTokenBindingMaxChannels = 1024
const upstreamTokenBindingMaxIntervals = 4096

type upstreamTokenBindingChange struct {
	Before         *ChannelCostSourceBinding
	After          ChannelCostSourceBinding
	ReplacePending bool
}

type upstreamTokenBindingPlan struct {
	EffectiveFrom   int64
	Digest          string
	Changes         []upstreamTokenBindingChange
	RequiredSources []string
}

type upstreamTokenBindingConfigIdentity struct {
	ChannelID int
	Revision  int64
	CreatedAt int64
	SourceRef string
}

func upstreamTokenBindingKind(account ChannelUpstreamAccount) (string, error) {
	switch account.Provider {
	case upstreamProviderNewAPI:
		return channelCostSourceKindNewAPIToken, nil
	case upstreamProviderSub2API:
		if account.UsageAdapter != upstreamUsageAdapterSub2Stats {
			return sub2TokenSourceKind, nil
		}
	}
	return "", errors.New("上游暂不支持令牌归属，不改变现有统计")
}

func buildUpstreamTokenBindingPlan(account ChannelUpstreamAccount, configs []ChannelUpstreamTokenConfig,
	channels []ChannelSnap, bindings []ChannelCostSourceBinding, key []byte, keyID string, now int64) (upstreamTokenBindingPlan, error) {
	var empty upstreamTokenBindingPlan
	if now <= 0 || now > math.MaxInt64-3600 || len(configs) > upstreamTokenBindingMaxChannels || len(channels) > upstreamTokenBindingMaxChannels || len(bindings) > upstreamTokenBindingMaxIntervals {
		return empty, errors.New("令牌归属切换时间或快照容量无效")
	}
	if _, err := normalizeUpstreamTokenScope(account.Domain, 1); err != nil || account.BaseURL == "" || (account.UserID <= 0 && account.Account == "") {
		return empty, errUpstreamTokenScope
	}
	kind, err := upstreamTokenBindingKind(account)
	if err != nil {
		return empty, err
	}
	if _, err := channelCostKeyCheck(key); err != nil {
		return empty, err
	}
	if keyID == "" || len(keyID) > 64 || strings.TrimSpace(keyID) != keyID {
		return empty, errUpstreamTokenScope
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	plan := upstreamTokenBindingPlan{EffectiveFrom: (now/3600 + 1) * 3600, Changes: []upstreamTokenBindingChange{}, RequiredSources: []string{}}
	directory := map[int]ChannelSnap{}
	for _, c := range channels {
		if _, exists := directory[c.ID]; exists || c.ID <= 0 {
			return empty, errUpstreamTokenScope
		}
		directory[c.ID] = c
	}
	identities := make([]upstreamTokenBindingConfigIdentity, 0, len(configs))
	owners := map[string][]int{}
	seen := map[int]bool{}
	for _, c := range configs {
		if c.Domain != account.Domain || c.AccountEpoch != epoch || c.Provider != account.Provider || c.ChannelID <= 0 || c.Revision <= 0 || c.CreatedAt <= 0 || c.CreatedAt > now || seen[c.ChannelID] {
			return empty, errUpstreamTokenConflict
		}
		seen[c.ChannelID] = true
		id, err := canonicalUpstreamTokenID(c.TokenID)
		if err != nil || id != c.TokenID {
			return empty, errUpstreamTokenScope
		}
		identity := upstreamTokenBindingConfigIdentity{ChannelID: c.ChannelID, Revision: c.Revision, CreatedAt: c.CreatedAt}
		if id != "" {
			channel, ok := directory[c.ChannelID]
			// Disabled channels retain ownership. Missing, deleted or moved
			// channels require review, never an automatic reassignment.
			if !ok || channel.BaseDomain != account.Domain || channel.DeletedAt != 0 {
				return empty, errUpstreamTokenScope
			}
			identity.SourceRef, err = channelCostSourceRef(key, account.Provider, epoch, kind, id)
			if err != nil {
				return empty, err
			}
			owners[identity.SourceRef] = append(owners[identity.SourceRef], c.ChannelID)
		}
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].ChannelID < identities[j].ChannelID })
	history := append([]ChannelCostSourceBinding(nil), bindings...)
	sort.Slice(history, func(i, j int) bool {
		if history[i].SourceRef == history[j].SourceRef {
			return history[i].ValidFrom < history[j].ValidFrom
		}
		return history[i].SourceRef < history[j].SourceRef
	})
	current := map[string]ChannelCostSourceBinding{}
	previous := map[string]ChannelCostSourceBinding{}
	targets := map[string]bool{}
	for source := range owners {
		targets[source] = true
	}
	for _, b := range history {
		if err := validateCostSourceBinding(b); err != nil {
			return empty, err
		}
		if b.Domain != account.Domain || b.AccountEpoch != epoch || b.Provider != account.Provider || b.Status != "confirmed" || !validSHA256Hex(b.SourceRef) {
			return empty, errUpstreamTokenScope
		}
		if old, ok := previous[b.SourceRef]; ok && (old.ValidTo == 0 || old.ValidTo > b.ValidFrom) {
			return empty, errors.New("已有来源归属区间重叠，拒绝自动切换")
		}
		previous[b.SourceRef] = b
		if b.ValidTo == 0 {
			current[b.SourceRef] = b
			if b.MappingSource == upstreamTokenBindingMappingSource {
				if b.LocalChannelID > 0 && !seen[b.LocalChannelID] {
					return empty, errors.New("已有令牌归属缺少配置记录，不能视为主动清空")
				}
				targets[b.SourceRef] = true
			}
		}
	}
	sources := make([]string, 0, len(targets))
	for source := range targets {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	ownerHashes := upstreamTokenOwnerHashes(account.Domain, epoch, keyID, identities)
	for _, source := range sources {
		mode, channel := "unallocated", 0
		if len(owners[source]) == 1 {
			mode, channel = "allocated", owners[source][0]
		} else if len(owners[source]) > 1 {
			mode = "shared"
		}
		old, exists := current[source]
		if last, ok := previous[source]; ok && (!exists && last.ValidTo > plan.EffectiveFrom || last.ValidFrom > plan.EffectiveFrom) {
			return empty, errors.New("已有未来归属计划，拒绝覆盖")
		}
		if exists {
			if old.SourceRefKind != kind || old.HMACKeyID != keyID {
				return empty, errUpstreamTokenScope
			}
			if old.AllocationMode == mode && old.LocalChannelID == channel &&
				(old.MappingSource != upstreamTokenBindingMappingSource || mode == "unallocated" || upstreamTokenBindingOwnerHash(old) == ownerHashes[source].Hash) {
				continue
			}
			if old.MappingSource != upstreamTokenBindingMappingSource {
				return empty, errors.New("令牌配置与既有手工归属冲突，请先核对")
			}
		}
		if !exists && mode == "unallocated" {
			continue
		}
		after := ChannelCostSourceBinding{Domain: account.Domain, AccountEpoch: epoch, Provider: account.Provider,
			SourceRef: source, SourceRefKind: kind, HMACKeyID: keyID, LocalChannelID: channel,
			ValidFrom: plan.EffectiveFrom, Status: "confirmed", AllocationMode: mode,
			MappingSource: upstreamTokenBindingMappingSource, Reason: "optional token configuration", CreatedBy: "monitor", CreatedAt: now}
		if mode != "unallocated" {
			after.Reason += " config=" + ownerHashes[source].Hash
		}
		change := upstreamTokenBindingChange{After: after}
		if exists {
			copy := old
			change.Before = &copy
			change.ReplacePending = old.ValidFrom == plan.EffectiveFrom
		}
		plan.Changes = append(plan.Changes, change)
		if mode != "unallocated" {
			plan.RequiredSources = append(plan.RequiredSources, source)
		}
	}
	// No raw token, credential, mutable name or group is exposed in the plan.
	// Revision changes still invalidate the snapshot even if owners are equal.
	payload, err := json.Marshal(struct {
		Domain, Epoch, Provider, KeyID string
		EffectiveFrom                  int64
		Configs                        []upstreamTokenBindingConfigIdentity
		Bindings                       []ChannelCostSourceBinding
	}{account.Domain, epoch, account.Provider, keyID, plan.EffectiveFrom, identities, history})
	if err != nil {
		return empty, err
	}
	digest := sha256.Sum256(payload)
	plan.Digest = hex.EncodeToString(digest[:])
	return plan, nil
}

// Local read-only preview; no production route and no writes. Load the entire
// account's latest configuration, not just the channel the operator opened.
func (m *Monitor) previewUpstreamTokenBindingPlan(ctx context.Context, domain string, channelID int, now int64) (upstreamTokenBindingPlan, error) {
	domain, err := normalizeUpstreamTokenScope(domain, channelID)
	if err != nil {
		return upstreamTokenBindingPlan{}, err
	}
	var plan upstreamTokenBindingPlan
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		snapshot, err := readUpstreamTokenBindingSnapshot(tx, domain, channelID, []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID, now)
		plan = snapshot.Plan
		return err
	})
	if err != nil {
		return upstreamTokenBindingPlan{}, err
	}
	return plan, nil
}

type upstreamTokenBindingSnapshot struct {
	Account ChannelUpstreamAccount
	Configs []ChannelUpstreamTokenConfig
	Plan    upstreamTokenBindingPlan
}

func readUpstreamTokenBindingSnapshot(tx *gorm.DB, domain string, channelID int, key []byte, keyID string, now int64) (upstreamTokenBindingSnapshot, error) {
	var empty upstreamTokenBindingSnapshot
	account, err := readUpstreamTokenScope(tx, domain, channelID)
	if err != nil {
		return empty, err
	}
	var configs []ChannelUpstreamTokenConfig
	if err := tx.Raw(`SELECT c.* FROM channel_upstream_token_configs c WHERE c.domain=? AND c.account_epoch=?
			AND NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision)
			ORDER BY c.channel_id LIMIT ?`, domain, newAPIUpstreamAccountEpoch(account), upstreamTokenBindingMaxChannels+1).Scan(&configs).Error; err != nil {
		return empty, err
	}
	var channels []ChannelSnap
	if err := tx.Select("id", "base_domain", "deleted_at").Where("base_domain=?", domain).Limit(upstreamTokenBindingMaxChannels + 1).Find(&channels).Error; err != nil {
		return empty, err
	}
	var bindings []ChannelCostSourceBinding
	if err := tx.Where("domain=? AND account_epoch=? AND status='confirmed'", domain, newAPIUpstreamAccountEpoch(account)).Limit(upstreamTokenBindingMaxIntervals + 1).Find(&bindings).Error; err != nil {
		return empty, err
	}
	plan, err := buildUpstreamTokenBindingPlan(account, configs, channels, bindings, key, keyID, now)
	if err != nil {
		return empty, err
	}
	return upstreamTokenBindingSnapshot{Account: account, Configs: configs, Plan: plan}, nil
}
