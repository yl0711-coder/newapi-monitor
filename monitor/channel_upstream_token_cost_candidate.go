package monitor

// Conversion boundary for a verified Sub2 shadow scan. This is deliberately
// pure: building a candidate is not publishing it or activating a binding.
// Keep all account sources (not just the configured token), otherwise the
// account total would silently shrink to the selected channel's consumption.

import (
	"errors"
	"sort"
	"strconv"
)

type sub2TokenCostHourCandidate struct {
	State ChannelUpstreamCostHourState
	Rows  []ChannelUpstreamCostHourEvidence
}

func buildSub2TokenCostCandidates(account ChannelUpstreamAccount, tokenID string, day, now int64,
	cursor ChannelUpstreamTokenScanCheckpoint, pricingRows []ChannelUpstreamPricingHourEvidence,
	pricingStates []ChannelUpstreamPricingHourState, controls []ChannelUpstreamUsageHour,
	key []byte, keyID string) ([]sub2TokenCostHourCandidate, error) {
	return buildSub2TokenCostCandidatesChecked(account, tokenID, day, now, cursor, pricingRows, pricingStates, controls, key, keyID, false)
}

// A confirmed historical binding can establish token identity even on a day
// with zero usage for that token. This never relaxes account/day reconciliation.
func buildSub2TokenCostCandidatesChecked(account ChannelUpstreamAccount, tokenID string, day, now int64,
	cursor ChannelUpstreamTokenScanCheckpoint, pricingRows []ChannelUpstreamPricingHourEvidence,
	pricingStates []ChannelUpstreamPricingHourState, controls []ChannelUpstreamUsageHour,
	key []byte, keyID string, historicalBindingVerified bool) ([]sub2TokenCostHourCandidate, error) {
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return nil, err
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	if account.UsageAdapter == upstreamUsageAdapterSub2Stats || cursor.Domain != account.Domain ||
		cursor.AccountEpoch != epoch || cursor.DayTs != day || cursor.Format != sub2TokenCheckpointFormat ||
		cursor.SuccessAt < day+86400 || cursor.SuccessAt > now {
		return nil, errors.New("Sub2 成本候选账户、窗口或完成时间不匹配")
	}
	check, err := checkSub2TokenReconciliation(account, tokenID, day, now, cursor, pricingRows, pricingStates, controls, key, keyID)
	if err != nil {
		return nil, err
	}
	identityReady := check.Status == "evidence_ready" && check.TokenSeen || historicalBindingVerified && check.Status == "token_not_observed"
	if !identityReady || check.ReadyHours != 24 || check.BlockedHours != 0 || len(check.Hours) != 24 {
		return nil, errors.New("Sub2 令牌证据尚未完成单日对账，不能生成成本候选")
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), account, day, key, keyID)
	if err != nil {
		return nil, err
	}
	byHour := make(map[int64][]ChannelUpstreamCostHourEvidence, 24)
	for _, row := range cp.Rows {
		byHour[row.HourTs] = append(byHour[row.HourTs], row)
	}
	result := make([]sub2TokenCostHourCandidate, 0, 24)
	for i, hour := range check.Hours {
		if hour.HourTs != day+int64(i)*3600 || (hour.Status != "matched" && hour.Status != "rounding_only") {
			return nil, errors.New("Sub2 成本候选小时不连续或对账未通过")
		}
		control, e1 := strconv.ParseInt(hour.ControlUnits, 10, 64)
		evidence, e2 := strconv.ParseInt(hour.EvidenceUnits, 10, 64)
		delta, e3 := strconv.ParseInt(hour.DeltaUnits, 10, 64)
		tolerance, e4 := strconv.ParseInt(hour.ToleranceUnits, 10, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || control < 0 || evidence < 0 || tolerance < 0 || evidence-control != delta {
			return nil, errors.New("Sub2 成本候选控制金额无效")
		}
		rows := byHour[hour.HourTs]
		if rows == nil {
			rows = []ChannelUpstreamCostHourEvidence{}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].DimensionHash < rows[j].DimensionHash })
		state := ChannelUpstreamCostHourState{
			Domain: account.Domain, AccountEpoch: epoch, Provider: account.Provider,
			HourTs: hour.HourTs, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			Status: "verified", VerifiedScans: 2, ReconcileStatus: hour.Status,
			ControlChargeUnits: control, EvidenceChargeUnits: evidence, ChargeUnitsPerUSD: "1000000",
			ReconcileDelta: delta, ReconcileTolerance: tolerance, EvidenceRows: int64(len(rows)),
			ContentHash: channelCostEvidenceContentHash(rows), CompletedAt: cursor.SuccessAt, UpdatedAt: now,
		}
		// Preserve tolerated rounding as such; existing matched-only consumers
		// must not mistake it for exact equality. No balancing/fictional row.
		if hour.Status != "matched" {
			state.Status = "observed"
		}
		var total int64
		for _, row := range rows {
			if err := addPricingCounter(&total, row.ChargeUnits); err != nil {
				return nil, err
			}
			if err := addPricingCounter(&state.Requests, row.Requests); err != nil {
				return nil, err
			}
		}
		if total != evidence {
			return nil, errors.New("Sub2 成本候选拆分金额不守恒")
		}
		result = append(result, sub2TokenCostHourCandidate{State: state, Rows: rows})
	}
	return result, nil
}
