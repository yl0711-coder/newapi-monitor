package monitor

// NewAPI optional-token proof reads existing local ledgers only. It neither
// downloads logs again nor changes legacy account totals or conversion units.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

type newAPITokenProof struct {
	View    upstreamTokenReconcileView
	Hash    string
	Sources map[string]bool
}

func validateNewAPITokenProofHour(account ChannelUpstreamAccount, hour, now int64, keyID string,
	rows []ChannelUpstreamCostHourEvidence, cost ChannelUpstreamCostHourState,
	pricing []ChannelUpstreamPricingHourEvidence, price ChannelUpstreamPricingHourState, control ChannelUpstreamUsageHour) error {
	epoch := newAPIUpstreamAccountEpoch(account)
	if !validPositiveCanonicalRat(cost.ChargeUnitsPerUSD) {
		return errors.New("历史计费单位无效")
	}
	if cost.Domain != account.Domain || cost.AccountEpoch != epoch || cost.Provider != upstreamProviderNewAPI || cost.HourTs != hour || cost.SemanticsVersion != channelCostEvidenceSemanticsVersion ||
		cost.Status != "verified" || cost.VerifiedScans < 2 || cost.ReconcileStatus != "matched" || cost.ReconcileDelta != 0 || cost.ReconcileTolerance != 0 || cost.LastError != "" || cost.CompletedAt < hour+3600 || cost.CompletedAt > now {
		return errors.New("成本小时未完成核验")
	}
	if price.Domain != account.Domain || price.AccountEpoch != epoch || price.HourTs != hour || price.SemanticsVersion != upstreamPricingSemanticsVersion || price.Status != "verified" || price.VerifiedScans < 2 || price.ReconcileStatus != "matched" || price.LastError != "" || price.CompletedAt < hour+3600 || price.CompletedAt > now {
		return errors.New("计价小时未完成核验")
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if _, err := decodeChannelCostCheckpoint(ChannelCostPageCheckpoint{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, NextPage: 2, Total: cost.Requests, SourceRows: cost.Requests, AggregatesJSON: string(encoded)}, account.Provider, keyID); err != nil {
		return err
	}
	encoded, err = json.Marshal(pricing)
	if err != nil {
		return err
	}
	if _, err := decodePricingCheckpointEvidence(ChannelUpstreamPricingPageCheckpoint{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, Provider: account.Provider, SemanticsVersion: upstreamPricingSemanticsVersion, WindowSeconds: 3600, SourceRows: price.SourceRows, AggregatesJSON: string(encoded)}); err != nil {
		return err
	}
	computed, err := pricingHourStateFromEvidence(account, hour, pricing, now)
	if err != nil {
		return err
	}
	if computed.ContentHash != price.ContentHash || computed.SourceRows != price.SourceRows || computed.EligibleRequests != price.EligibleRequests || computed.Tokens != price.Tokens || computed.FinalQuota != price.FinalQuota || computed.EvidenceRows != price.EvidenceRows {
		return errors.New("计价事实与核验状态不一致")
	}
	units, exact := pricingLegacyQuotaAsExact(account.Provider, control)
	if control.Domain != account.Domain || control.HourTs != hour || control.Provider != account.Provider || control.BucketSeconds != 3600 || control.Provisional || !exact || units < 0 || units >= 1<<53 ||
		pricingLegacySnapshotChanged(account.Provider, price, control) || control.Requests != computed.EligibleRequests || control.Tokens != computed.Tokens || units != computed.FinalQuota {
		return errors.New("小时账单控制总额已变化或不完整")
	}
	type totals struct {
		charge, requests, prompt, completion int64
		group, model, billing                string
	}
	byDimension := map[string]totals{}
	var total int64
	for _, r := range rows {
		if r.ChargeUnitsPerUSD != cost.ChargeUnitsPerUSD || r.FetchedAt > now {
			return errors.New("历史计费单位或采集时间不一致")
		}
		d := byDimension[r.PricingDimensionHash]
		if d.requests > 0 && (d.group != r.SourceGroup || d.model != r.UpstreamModel || d.billing != r.BillingMode) {
			return errors.New("同一计价维度存在不同身份")
		}
		d.group, d.model, d.billing = r.SourceGroup, r.UpstreamModel, r.BillingMode
		for _, v := range []struct {
			dst   *int64
			value int64
		}{{&d.charge, r.ChargeUnits}, {&d.requests, r.Requests}, {&d.prompt, r.PromptTokens}, {&d.completion, r.CompletionTokens}, {&total, r.ChargeUnits}} {
			if err := addPricingCounter(v.dst, v.value); err != nil {
				return err
			}
		}
		byDimension[r.PricingDimensionHash] = d
	}
	if total != cost.EvidenceChargeUnits || total != cost.ControlChargeUnits || total != price.FinalQuota || cost.Requests != price.SourceRows || cost.EvidenceRows != int64(len(rows)) || cost.ContentHash != channelCostEvidenceContentHash(rows) {
		return errors.New("令牌明细与账户总额不守恒")
	}
	for _, p := range pricing {
		d, ok := byDimension[p.DimensionHash]
		if !ok || d.charge != p.FinalQuota || d.requests != p.SourceRows || d.prompt < p.PromptTokens || d.completion < p.CompletionTokens ||
			(p.NonPositiveQuotaRows == 0 && (d.prompt != p.PromptTokens || d.completion != p.CompletionTokens)) {
			return errors.New("令牌明细与计价维度不一致")
		}
		if d.group != p.SourceGroup || d.model != p.ModelName || d.billing != p.BillingMode {
			return errors.New("令牌计价维度身份不一致")
		}
		delete(byDimension, p.DimensionHash)
	}
	if len(byDimension) != 0 {
		return errors.New("存在未对账的令牌维度")
	}
	return nil
}

func readNewAPITokenProof(tx *gorm.DB, account ChannelUpstreamAccount, day, now int64, keyID string) (newAPITokenProof, error) {
	proof := newAPITokenProof{View: upstreamTokenReconcileView{DayTs: day, Status: "reconciliation_blocked", Hours: []upstreamTokenHourCheck{}}, Sources: map[string]bool{}}
	if account.Provider != upstreamProviderNewAPI || day < 0 || day > math.MaxInt64-86400 || day != cstDayStart(day) || keyID == "" || len(keyID) > 64 || strings.TrimSpace(keyID) != keyID {
		return proof, errUpstreamTokenScope
	}
	if err := validateChannelCostAccountIdentity(account); err != nil {
		return proof, err
	}
	if now < day+86400 {
		proof.View.Status = "window_open"
		proof.View.BlockedHours = 24
		return proof, nil
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	var cost []ChannelUpstreamCostHourEvidence
	var states []ChannelUpstreamCostHourState
	var pricing []ChannelUpstreamPricingHourEvidence
	var prices []ChannelUpstreamPricingHourState
	var controls []ChannelUpstreamUsageHour
	for _, q := range []struct {
		model      any
		sem, limit int
	}{{&cost, channelCostEvidenceSemanticsVersion, sub2TokenMaxScanRows + 1}, {&states, channelCostEvidenceSemanticsVersion, 25}, {&pricing, upstreamPricingSemanticsVersion, sub2TokenMaxScanRows + 1}, {&prices, upstreamPricingSemanticsVersion, 25}} {
		if err := tx.Where("domain=? AND account_epoch=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", account.Domain, epoch, q.sem, day, day+86400).Limit(q.limit).Find(q.model).Error; err != nil {
			return proof, err
		}
	}
	if err := tx.Where("domain=? AND hour_ts>=? AND hour_ts<?", account.Domain, day, day+86400).Limit(25).Find(&controls).Error; err != nil {
		return proof, err
	}
	if len(cost) > sub2TokenMaxScanRows || len(pricing) > sub2TokenMaxScanRows || len(states) > 24 || len(prices) > 24 || len(controls) > 24 {
		return proof, errors.New("令牌核验超出单日安全容量")
	}
	busy := map[int64]bool{}
	for _, table := range []string{"channel_cost_dirty_hours", "channel_cost_page_checkpoints", "channel_upstream_pricing_page_checkpoints"} {
		var hours []int64
		if err := tx.Table(table).Distinct("hour_ts").Where("domain=? AND account_epoch=? AND hour_ts>=? AND hour_ts<?", account.Domain, epoch, day, day+86400).Limit(25).Pluck("hour_ts", &hours).Error; err != nil {
			return proof, err
		}
		for _, hour := range hours {
			busy[hour] = true
		}
	}
	cs := map[int64]ChannelUpstreamCostHourState{}
	ps := map[int64]ChannelUpstreamPricingHourState{}
	usage := map[int64]ChannelUpstreamUsageHour{}
	cr := map[int64][]ChannelUpstreamCostHourEvidence{}
	pr := map[int64][]ChannelUpstreamPricingHourEvidence{}
	for _, s := range states {
		cs[s.HourTs] = s
	}
	for _, s := range prices {
		ps[s.HourTs] = s
	}
	for _, s := range controls {
		usage[s.HourTs] = s
	}
	for _, r := range cost {
		if r.HourTs%3600 != 0 {
			return proof, errUpstreamTokenScope
		}
		cr[r.HourTs] = append(cr[r.HourTs], r)
	}
	for _, r := range pricing {
		if r.HourTs%3600 != 0 {
			return proof, errUpstreamTokenScope
		}
		pr[r.HourTs] = append(pr[r.HourTs], r)
	}
	hash := sha256.New()
	for hour := day; hour < day+86400; hour += 3600 {
		s, p, u := cs[hour], ps[hour], usage[hour]
		check := upstreamTokenHourCheck{HourTs: hour, Status: "evidence_invalid"}
		switch {
		case busy[hour]:
			check.Status = "scan_in_progress"
		case s.Domain == "" || p.Domain == "" || u.Domain == "":
			check.Status = "evidence_missing"
		case validateNewAPITokenProofHour(account, hour, now, keyID, cr[hour], s, pr[hour], p, u) == nil:
			check.Status = "matched"
			check.EvidenceUnits = strconv.FormatInt(s.EvidenceChargeUnits, 10)
			check.ControlUnits = strconv.FormatInt(s.ControlChargeUnits, 10)
			check.DeltaUnits = "0"
			check.ToleranceUnits = "0"
			for _, r := range cr[hour] {
				proof.Sources[r.SourceRef] = true
			}
		}
		if check.Status == "matched" {
			proof.View.ReadyHours++
		} else {
			proof.View.BlockedHours++
		}
		proof.View.Hours = append(proof.View.Hours, check)
		// Ledger hashes cover exact evidence; state/control versions fence any
		// source revision between preview and the transaction that binds it.
		if err := json.NewEncoder(hash).Encode(struct {
			Cost    ChannelUpstreamCostHourState
			Price   ChannelUpstreamPricingHourState
			Control ChannelUpstreamUsageHour
		}{s, p, u}); err != nil {
			return proof, err
		}
	}
	if proof.View.ReadyHours == 24 {
		proof.View.Status = "evidence_ready"
		proof.View.StableScans = 2
		proof.Hash = hex.EncodeToString(hash.Sum(nil))
	}
	return proof, nil
}

func verifyNewAPITokenBindingProof(tx *gorm.DB, snapshot upstreamTokenBindingSnapshot, in upstreamTokenBindingCommitInput, now int64, keyID string) error {
	if len(snapshot.Plan.RequiredSources) == 0 {
		return nil
	}
	if in.ProofDayTs != cstDayStart(now)-86400 || in.ProofCursorRevision != 0 || !validSHA256Hex(in.ProofSuccessHash) {
		return errors.New("NewAPI 令牌归属需要最近闭合日的证据摘要")
	}
	proof, err := readNewAPITokenProof(tx, snapshot.Account, in.ProofDayTs, now, keyID)
	if err != nil {
		return err
	}
	if proof.Hash == "" || proof.Hash != in.ProofSuccessHash {
		return errors.New("NewAPI 令牌证据未齐全或已变化，请重新核验")
	}
	for _, source := range snapshot.Plan.RequiredSources {
		if !proof.Sources[source] {
			return errors.New("配置令牌未出现在已核验的 NewAPI 日志中")
		}
	}
	return nil
}
