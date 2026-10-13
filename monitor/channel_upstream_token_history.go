package monitor

// Local-facts-only catch-up authorization. Existing append-only configuration and
// confirmed intervals authorize individual historical hours, not today's IDs.
// This does not infer, backdate or modify any binding/configuration.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"

	"gorm.io/gorm"
)

type sub2TokenHistoricalWindow struct {
	Digest        string
	TokenID       string // internal proof anchor only, never returned by an HTTP route
	Hours         [24]bool
	EligibleHours int
}

func readSub2TokenHistoricalWindow(tx *gorm.DB, account ChannelUpstreamAccount, channelID int, day, now int64, key []byte, keyID string) (sub2TokenHistoricalWindow, error) {
	var empty sub2TokenHistoricalWindow
	if channelID <= 0 || account.UsageAdapter == upstreamUsageAdapterSub2Stats || day < 0 || day > math.MaxInt64-86400 || day != cstDayStart(day) || now < day+86400 {
		return empty, errUpstreamTokenScope
	}
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return empty, err
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	// One latest pre-day head per channel, plus changes within the day.
	// Avoid 24 repeated directory reads or an unbounded account-history scan.
	var configs []ChannelUpstreamTokenConfig
	if err := tx.Raw(`SELECT c.* FROM channel_upstream_token_configs c
	 WHERE c.domain=? AND c.account_epoch=? AND c.created_at<?
	 AND (c.created_at>=? OR NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n
	 WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.created_at<? AND n.revision>c.revision))
	 ORDER BY c.channel_id,c.revision LIMIT ?`, account.Domain, epoch, day+86400, day, day, upstreamTokenBindingMaxIntervals+1).Scan(&configs).Error; err != nil {
		return empty, err
	}
	if len(configs) > upstreamTokenBindingMaxIntervals {
		return empty, errors.New("历史令牌配置超过单日核验容量")
	}
	previous := map[int]ChannelUpstreamTokenConfig{}
	sources := map[int]map[int64]string{}
	for _, c := range configs {
		id, err := canonicalUpstreamTokenID(c.TokenID)
		if err != nil || id != c.TokenID || c.Domain != account.Domain || c.AccountEpoch != epoch || c.Provider != account.Provider || c.ChannelID <= 0 || c.Revision <= 0 || c.CreatedAt <= 0 || c.CreatedAt >= day+86400 {
			return empty, errUpstreamTokenConflict
		}
		if old, ok := previous[c.ChannelID]; ok && (old.Revision >= c.Revision || old.CreatedAt > c.CreatedAt) {
			return empty, errUpstreamTokenConflict
		}
		previous[c.ChannelID] = c
		if len(previous) > upstreamTokenBindingMaxChannels {
			return empty, errors.New("历史令牌渠道超过核验容量")
		}
		if sources[c.ChannelID] == nil {
			sources[c.ChannelID] = map[int64]string{}
		}
		if id != "" {
			ref, err := channelCostSourceRef(key, account.Provider, epoch, sub2TokenSourceKind, id)
			if err != nil {
				return empty, err
			}
			sources[c.ChannelID][c.Revision] = ref
		}
	}
	var bindings []ChannelCostSourceBinding
	if err := tx.Where("domain=? AND account_epoch=? AND status='confirmed' AND valid_from<? AND (valid_to=0 OR valid_to>?)", account.Domain, epoch, day+86400, day).
		Order("source_ref,valid_from").Limit(upstreamTokenBindingMaxIntervals + 1).Find(&bindings).Error; err != nil {
		return empty, err
	}
	if len(bindings) > upstreamTokenBindingMaxIntervals {
		return empty, errors.New("历史令牌绑定超过单日核验容量")
	}
	bySource := map[string][]ChannelCostSourceBinding{}
	for _, b := range bindings {
		if err := validateCostSourceBinding(b); err != nil {
			return empty, err
		}
		if b.Domain != account.Domain || b.AccountEpoch != epoch || b.Provider != account.Provider || b.SourceRefKind != sub2TokenSourceKind || b.HMACKeyID != keyID || !validSHA256Hex(b.SourceRef) {
			return empty, errUpstreamTokenScope
		}
		old := bySource[b.SourceRef]
		if len(old) > 0 && (old[len(old)-1].ValidTo == 0 || old[len(old)-1].ValidTo > b.ValidFrom) {
			return empty, errors.New("历史令牌绑定区间重叠")
		}
		bySource[b.SourceRef] = append(old, b)
	}
	window := empty
	for i := 0; i < 24; i++ {
		hour := day + int64(i)*3600
		latest := map[int]ChannelUpstreamTokenConfig{}
		for _, c := range configs {
			if c.CreatedAt < hour {
				latest[c.ChannelID] = c
			}
		}
		selected, ok := latest[channelID]
		if !ok || selected.TokenID == "" {
			continue
		}
		identities := make([]upstreamTokenBindingConfigIdentity, 0, len(latest))
		for _, c := range latest {
			identities = append(identities, upstreamTokenBindingConfigIdentity{ChannelID: c.ChannelID, Revision: c.Revision, CreatedAt: c.CreatedAt, SourceRef: sources[c.ChannelID][c.Revision]})
		}
		owners := upstreamTokenOwnerHashes(account.Domain, epoch, keyID, identities)
		// An otherwise valid anchor cannot authorize a conflicting manual
		// allocation for another configured token in this account-hour.
		conflict := false
		for source, owner := range owners {
			for _, b := range bySource[source] {
				if b.ValidFrom <= hour && (b.ValidTo == 0 || b.ValidTo > hour) {
					if b.AllocationMode != "unallocated" && !sub2HistoricalBindingMatches(b, owner) {
						conflict = true
					}
					break
				}
			}
		}
		if conflict {
			continue
		}
		ref := sources[channelID][selected.Revision]
		for _, b := range bySource[ref] {
			if b.ValidFrom > hour || (b.ValidTo != 0 && b.ValidTo <= hour) {
				continue
			}
			owner := owners[ref]
			matches := sub2HistoricalBindingMatches(b, owner)
			if matches {
				window.Hours[i] = true
				window.EligibleHours++
				if window.TokenID == "" {
					window.TokenID = selected.TokenID
				}
			}
			break
		}
	}
	// Bind the whole relevant snapshot, including other source owners. Current
	// post-day changes do not affect historical attribution; in-day edits do.
	sort.Slice(configs, func(i, j int) bool {
		if configs[i].ChannelID != configs[j].ChannelID {
			return configs[i].ChannelID < configs[j].ChannelID
		}
		return configs[i].Revision < configs[j].Revision
	})
	payload, err := json.Marshal(struct {
		Domain, Epoch, KeyID string
		ChannelID            int
		Day                  int64
		Configs              []ChannelUpstreamTokenConfig
		Bindings             []ChannelCostSourceBinding
		Hours                [24]bool
	}{account.Domain, epoch, keyID, channelID, day, configs, bindings, window.Hours})
	if err != nil {
		return empty, err
	}
	hash := sha256.Sum256(payload)
	window.Digest = hex.EncodeToString(hash[:])
	return window, nil
}

func sub2HistoricalBindingMatches(b ChannelCostSourceBinding, owner upstreamTokenOwnerState) bool {
	matches := owner.Hash != "" && (b.AllocationMode == "allocated" && owner.ChannelID > 0 && b.LocalChannelID == owner.ChannelID || b.AllocationMode == "shared" && b.LocalChannelID == 0 && owner.ChannelID == 0)
	if b.MappingSource == upstreamTokenBindingMappingSource {
		matches = matches && upstreamTokenBindingOwnerHash(b) == owner.Hash
	}
	return matches
}

// Internal preparation step used by the bounded local pipeline; no HTTP route
// or upstream I/O.
// Publication continues through the existing guarded, idempotent hour writer.
func (m *Monitor) prepareSub2TokenHistoricalCostDay(ctx context.Context, domain string, channelID int, day, now int64) (sub2TokenCostCommitResult, error) {
	var empty sub2TokenCostCommitResult
	if m == nil || m.storeDB == nil {
		return empty, errors.New("令牌成本存储未就绪")
	}
	if m.cfg.LocalSnapshotOnly {
		return empty, errUpstreamTokenReadOnly
	}
	domain, err := normalizeUpstreamTokenScope(domain, channelID)
	if err != nil {
		return empty, err
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, domain) {
		return empty, errors.New("Sub2 令牌历史成本未启用")
	}
	var in sub2TokenCostCommitInput
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		account, err := readUpstreamTokenScope(tx, domain, channelID)
		if err != nil {
			return err
		}
		config, err := readUpstreamTokenConfig(tx, account, channelID)
		if err != nil {
			return err
		}
		window, err := readSub2TokenHistoricalWindow(tx, account, channelID, day, now, []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID)
		if err != nil {
			return err
		}
		if window.EligibleHours == 0 {
			return nil
		}
		cursor, err := readSub2TokenStoredCursor(tx, account, day)
		if err != nil {
			return err
		}
		in = sub2TokenCostCommitInput{Domain: domain, ChannelID: channelID, AccountEpoch: config.AccountEpoch, ConfigRevision: config.Revision, DayTs: day, CursorRevision: cursor.Revision, SuccessHash: cursor.SuccessHash, HistoricalWindowDigest: window.Digest}
		return nil
	})
	if err != nil {
		return empty, err
	}
	if in.HistoricalWindowDigest == "" {
		return sub2TokenCostCommitResult{SkippedHours: 24}, nil
	}
	return m.commitSub2TokenCostCandidates(ctx, in, now)
}
