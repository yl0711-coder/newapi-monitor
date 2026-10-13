package monitor

// Internal Sub2 publication adapter. The existing bounded local economics lane
// may drain already-committed jobs; there is no HTTP publication endpoint.
// Actual cost is already multiplied by the upstream's billing rate; only the
// existing historical recharge correction applies in the common publisher.

import (
	"context"
	"crypto/hmac"
	"errors"

	"gorm.io/gorm"
)

func (m *Monitor) publishSub2TokenEconomicsHour(ctx context.Context, account ChannelUpstreamAccount, hour int64, reason string, now int64) error {
	if m.cfg.LocalSnapshotOnly {
		return errUpstreamTokenReadOnly
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, account.Domain) ||
		account.Provider != upstreamProviderSub2API || account.UsageAdapter == upstreamUsageAdapterSub2Stats {
		return errors.New("Sub2 令牌经济账未启用")
	}
	return m.publishVerifiedChannelEconomicsHour(ctx, account, hour, reason, now)
}

func (m *Monitor) validateSub2TokenEconomicsHour(tx *gorm.DB, account ChannelUpstreamAccount, hour, now int64, rows []ChannelUpstreamCostHourEvidence, state ChannelUpstreamCostHourState) error {
	invalid := errors.New("Sub2 令牌经济账证据、金额或计费单位不一致")
	epoch := newAPIUpstreamAccountEpoch(account)
	key, keyID := []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID
	if err := validateSub2TokenContext(account, cstDayStart(hour), key, keyID); err != nil {
		return err
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
		return invalid
	}
	if state.Domain != account.Domain || state.AccountEpoch != epoch || state.Provider != account.Provider || state.HourTs != hour || state.SemanticsVersion != channelCostEvidenceSemanticsVersion ||
		state.Status != "verified" || state.VerifiedScans < 2 || state.ReconcileStatus != "matched" || state.ReconcileDelta != 0 || state.ReconcileTolerance < 0 || state.LastError != "" ||
		state.ChargeUnitsPerUSD != "1000000" || state.CompletedAt < hour+3600 || state.CompletedAt > now || len(rows) > sub2TokenMaxScanRows {
		return invalid
	}
	var charge, requests int64
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.DimensionHash] || r.Domain != account.Domain || r.AccountEpoch != epoch || r.HourTs != hour || r.Provider != account.Provider || r.SemanticsVersion != channelCostEvidenceSemanticsVersion ||
			r.SourceRefKind != sub2TokenSourceKind || r.HMACKeyID != keyID || !validSHA256Hex(r.SourceRef) || !validSHA256Hex(r.PricingDimensionHash) ||
			r.ChargeUnit != sub2TokenChargeUnit || r.ChargeUnitsPerUSD != "1000000" || r.Requests <= 0 || r.PromptTokens < 0 || r.CompletionTokens < 0 || r.FetchedAt < 0 || r.FetchedAt > now ||
			r.FirstSourceAt < hour || r.LastSourceAt < r.FirstSourceAt || r.LastSourceAt >= hour+3600 ||
			r.DimensionHash != channelCostDimensionHash(r.SourceRef, r.PricingDimensionHash, r.SourceGroup, r.UpstreamModel, r.BillingMode, r.ChargeUnit) || r.ContentHash != channelCostEvidenceRowHash(r) {
			return invalid
		}
		seen[r.DimensionHash] = true
		if err := addPricingCounter(&charge, r.ChargeUnits); err != nil {
			return err
		}
		if err := addPricingCounter(&requests, r.Requests); err != nil {
			return err
		}
	}
	if charge != state.EvidenceChargeUnits || charge != state.ControlChargeUnits || requests != state.Requests || int64(len(rows)) != state.EvidenceRows || channelCostEvidenceContentHash(rows) != state.ContentHash {
		return invalid
	}
	if err := validateSub2TokenCurrentCostProof(tx, account, hour, now, state.ContentHash, key, keyID); err != nil {
		return err
	}
	// Recheck current control facts in the same transaction, without another
	// upstream request or a full historical day scan.
	var price ChannelUpstreamPricingHourState
	if err := tx.Where("domain=? AND account_epoch=? AND hour_ts=? AND semantics_version=?", account.Domain, epoch, hour, upstreamPricingSemanticsVersion).Take(&price).Error; err != nil {
		return err
	}
	var control ChannelUpstreamUsageHour
	if err := tx.Where("domain=? AND hour_ts=?", account.Domain, hour).Take(&control).Error; err != nil {
		return err
	}
	units, exact := pricingLegacyQuotaAsExact(account.Provider, control)
	if price.Status != "verified" || price.VerifiedScans < 2 || price.ReconcileStatus != "matched" || price.LastError != "" || price.CompletedAt < hour+3600 || price.CompletedAt > now ||
		control.Provider != account.Provider || control.BucketSeconds != 3600 || control.Provisional || !exact || units < 0 || units >= 1<<53 || units != charge ||
		pricingLegacySnapshotChanged(account.Provider, price, control) || price.FinalQuota != charge || price.SourceRows != requests || price.EligibleRequests != control.Requests || price.Tokens != control.Tokens {
		return invalid
	}
	return nil
}

// A verified cost ledger is not sufficient once its source scan has failed,
// restarted or changed token attribution. Read the current bounded staging
// proof in the SAME publication transaction; never rely on an earlier UI
// check or in-memory result. Existing published reports are not deleted.
func validateSub2TokenCurrentCostProof(tx *gorm.DB, account ChannelUpstreamAccount, hour, now int64, hourHash string, key []byte, keyID string) error {
	invalid := errors.New("Sub2 令牌来源正在重新核验或已变更，保留原报表，暂不发布新计算")
	day := cstDayStart(hour)
	cursor, err := readSub2TokenStoredCursor(tx, account, day)
	if err != nil {
		return err
	}
	if cursor.Revision <= 0 || cursor.SuccessRevision != cursor.Revision || cursor.StableScans < 2 || cursor.SuccessKeyID != keyID ||
		cursor.SuccessAt < day+86400 || cursor.SuccessAt > now {
		return invalid
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), account, day, key, keyID)
	if err != nil {
		return invalid
	}
	if cp.NextPage != cp.Pages+1 || int64(len(cp.SeenRows)) != cp.Total || cursor.SuccessHash != channelCostEvidenceContentHash(cp.Rows) {
		return invalid
	}
	var currentRows []ChannelUpstreamCostHourEvidence
	for _, row := range cp.Rows {
		if row.HourTs == hour {
			currentRows = append(currentRows, row)
		}
	}
	// Compare exact token/dimension content, not only account totals. A later
	// independently verified identical scan is valid without rewriting costs
	// just to update a revision or timestamp. Empty hours have a hash too.
	if channelCostEvidenceContentHash(currentRows) != hourHash {
		return invalid
	}
	return nil
}
