package monitor

// Bindings carry a fingerprint of the exact source owners that were verified.
// Resolve configuration as of the cost hour, not as of report generation time.
// This protects pending changes without rewriting any historical binding.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"gorm.io/gorm"
)

type upstreamTokenOwnerState struct {
	Hash      string
	ChannelID int // zero when shared
}

func upstreamTokenOwnerHashes(domain, epoch, keyID string, identities []upstreamTokenBindingConfigIdentity) map[string]upstreamTokenOwnerState {
	owners := map[string][]upstreamTokenBindingConfigIdentity{}
	for _, identity := range identities {
		if identity.SourceRef != "" {
			owners[identity.SourceRef] = append(owners[identity.SourceRef], identity)
		}
	}
	out := map[string]upstreamTokenOwnerState{}
	for source, rows := range owners {
		sort.Slice(rows, func(i, j int) bool { return rows[i].ChannelID < rows[j].ChannelID })
		payload, _ := json.Marshal(struct {
			Domain, Epoch, KeyID string
			Owners               []upstreamTokenBindingConfigIdentity
		}{domain, epoch, keyID, rows})
		hash := sha256.Sum256(payload)
		state := upstreamTokenOwnerState{Hash: hex.EncodeToString(hash[:])}
		if len(rows) == 1 {
			state.ChannelID = rows[0].ChannelID
		}
		out[source] = state
	}
	return out
}

func upstreamTokenBindingOwnerHash(binding ChannelCostSourceBinding) string {
	var hash string
	for _, part := range strings.Fields(binding.Reason) {
		if strings.HasPrefix(part, "config=") {
			if hash != "" || !validSHA256Hex(strings.TrimPrefix(part, "config=")) {
				return ""
			}
			hash = strings.TrimPrefix(part, "config=")
		}
	}
	return hash
}

// One bounded account/hour read, reused for every source in a publication.
// CreatedAt < hour implements the next-full-hour boundary, including changes
// made exactly on an hour. Revisions, not mutable names, identify ownership.
func readUpstreamTokenOwnerHashesAt(tx *gorm.DB, account ChannelUpstreamAccount, hour int64, key []byte, keyID string) (map[string]upstreamTokenOwnerState, error) {
	if hour < 0 || hour%3600 != 0 || keyID == "" {
		return nil, errUpstreamTokenScope
	}
	if _, err := channelCostKeyCheck(key); err != nil {
		return nil, err
	}
	kind, err := upstreamTokenBindingKind(account)
	if err != nil {
		return nil, err
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	var configs []ChannelUpstreamTokenConfig
	if err := tx.Raw(`SELECT c.* FROM channel_upstream_token_configs c
		WHERE c.domain=? AND c.account_epoch=? AND c.created_at<?
		AND NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n
		 WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id
		 AND n.created_at<? AND n.revision>c.revision)
		LIMIT ?`, account.Domain, epoch, hour, hour, upstreamTokenBindingMaxChannels+1).Scan(&configs).Error; err != nil {
		return nil, err
	}
	if len(configs) > upstreamTokenBindingMaxChannels {
		return nil, errors.New("令牌配置超过小时核验安全容量")
	}
	identities := make([]upstreamTokenBindingConfigIdentity, 0, len(configs))
	for _, c := range configs {
		id, err := canonicalUpstreamTokenID(c.TokenID)
		if err != nil || id != c.TokenID || c.Provider != account.Provider || c.ChannelID <= 0 || c.Revision <= 0 || c.CreatedAt <= 0 {
			return nil, errUpstreamTokenConflict
		}
		identity := upstreamTokenBindingConfigIdentity{ChannelID: c.ChannelID, Revision: c.Revision, CreatedAt: c.CreatedAt}
		if id != "" {
			identity.SourceRef, err = channelCostSourceRef(key, account.Provider, epoch, kind, id)
			if err != nil {
				return nil, err
			}
		}
		identities = append(identities, identity)
	}
	return upstreamTokenOwnerHashes(account.Domain, epoch, keyID, identities), nil
}

func upstreamTokenBindingMatchesOwners(binding ChannelCostSourceBinding, hashes map[string]upstreamTokenOwnerState, keyID string) bool {
	hash := upstreamTokenBindingOwnerHash(binding)
	owner := hashes[binding.SourceRef]
	return binding.HMACKeyID == keyID && binding.AllocationMode == "allocated" && binding.LocalChannelID > 0 && binding.LocalChannelID == owner.ChannelID && hash != "" && hash == owner.Hash
}
