package monitor

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"
)

type FinanceRechargeRehearsalAmounts struct {
	Period           string `json:"period"`
	Buckets          int64  `json:"buckets"`
	NewlyCosted      int64  `json:"newly_costed"`
	Repriced         int64  `json:"repriced"`
	RemainingUnknown int64  `json:"remaining_unknown"`
	RawMicro         int64  `json:"raw_micro_units"`
	KnownBeforeMicro int64  `json:"known_before_micro_units"`
	KnownAfterMicro  int64  `json:"known_after_micro_units"`
}

type FinanceRechargeRehearsalDomain struct {
	Domain       string                            `json:"domain"`
	BillComplete bool                              `json:"bill_complete"`
	Total        FinanceRechargeRehearsalAmounts   `json:"total"`
	Months       []FinanceRechargeRehearsalAmounts `json:"months"`
	Days         []FinanceRechargeRehearsalAmounts `json:"days"`
}

type FinanceRechargeRehearsalResult struct {
	Mode          string                            `json:"mode"`
	Publishable   bool                              `json:"publishable"`
	SourceSHA256  string                            `json:"source_sha256"`
	OutputSHA256  string                            `json:"output_sha256"`
	AddedVersions int                               `json:"added_versions"`
	Domains       []FinanceRechargeRehearsalDomain  `json:"domains"`
	Economics     []FinanceRechargeEconomicsRebuild `json:"economics_rebuild,omitempty"`
}

func projectFinanceRechargeRehearsal(ctx context.Context, bills []ChannelUpstreamUsageHour, scope stabilityScope, now int64,
	accounts map[string]ChannelUpstreamAccountView, old, revised []channelRechargeVersion, correction FinanceRechargeCorrection) (FinanceRechargeRehearsalDomain, error) {
	out := FinanceRechargeRehearsalDomain{Domain: correction.Domain, Total: FinanceRechargeRehearsalAmounts{Period: "total"}}
	sort.Slice(revised, func(i, j int) bool {
		if revised[i].EffectiveAt == revised[j].EffectiveAt {
			return revised[i].Version < revised[j].Version
		}
		return revised[i].EffectiveAt < revised[j].EffectiveAt
	})
	var accepted []ChannelUpstreamUsageHour
	for _, row := range bills {
		s := row.BucketSeconds
		if s == 0 {
			s = 3600
		}
		if row.HourTs < 0 || (s != 3600 && s != 86400) || row.HourTs > math.MaxInt64-s {
			return out, errors.New("invalid bill interval in recharge rehearsal")
		}
	}
	metrics, err := projectUpstreamUsageBuckets(bills, scope, now, accounts, map[string][]channelRechargeVersion{correction.Domain: old}, func(row ChannelUpstreamUsageHour) { accepted = append(accepted, row) })
	if err != nil {
		return out, err
	}
	metric := metrics[correction.Domain]
	if !metric.Available || metric.Provisional || metric.IntegrityStatus != upstreamUsageIntegrityComplete {
		return out, errors.New("recharge rehearsal refuses missing, invalid, provisional or overlapping source bills")
	}
	out.BillComplete = metric.Complete
	months, days := map[string]*FinanceRechargeRehearsalAmounts{}, map[string]*FinanceRechargeRehearsalAmounts{}
	for _, row := range accepted {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		seconds := row.BucketSeconds
		if seconds == 0 {
			seconds = 3600
		}
		value := FinanceRechargeRehearsalAmounts{Buckets: 1}
		var raw, before, after financeBillSum
		raw.addUSD(row.CostUSD)
		p, c, status := rechargeTermsForBucket(old, row.HourTs, row.HourTs+seconds)
		p2, c2, nextStatus := rechargeTermsForBucket(revised, row.HourTs, row.HourTs+seconds)
		if upstreamBillHasZeroFaceValue(row) {
			status, nextStatus = upstreamAdjustedCostComplete, upstreamAdjustedCostComplete
		} else {
			if status == upstreamAdjustedCostComplete {
				before.addCorrectedUSD(row.CostUSD, p, c)
			}
			if nextStatus == upstreamAdjustedCostComplete {
				after.addCorrectedUSD(row.CostUSD, p2, c2)
			}
		}
		if raw.err != nil || before.err != nil || after.err != nil {
			return out, errors.New("invalid or overflowing recharge rehearsal amount")
		}
		value.RawMicro, value.KnownBeforeMicro, value.KnownAfterMicro = raw.micro, before.micro, after.micro
		if status != upstreamAdjustedCostComplete && nextStatus == upstreamAdjustedCostComplete {
			value.NewlyCosted = 1
		}
		if status == upstreamAdjustedCostComplete && nextStatus == upstreamAdjustedCostComplete && before.micro != after.micro {
			value.Repriced = 1
		}
		if nextStatus != upstreamAdjustedCostComplete {
			value.RemainingUnknown = 1
		}
		if status != nextStatus || before.micro != after.micro {
			if row.HourTs >= correction.ToTs || row.HourTs+seconds <= correction.FromTs {
				return out, errors.New("recharge rehearsal affected a bill outside the explicit interval")
			}
		}
		date := time.Unix(row.HourTs, 0).In(time.FixedZone("CST", 8*3600)).Format("2006-01-02")
		if days[date] == nil {
			days[date] = &FinanceRechargeRehearsalAmounts{Period: date}
		}
		if months[date[:7]] == nil {
			months[date[:7]] = &FinanceRechargeRehearsalAmounts{Period: date[:7]}
		}
		for _, total := range []*FinanceRechargeRehearsalAmounts{&out.Total, days[date], months[date[:7]]} {
			if err := addRechargeRehearsalAmounts(total, value); err != nil {
				return out, err
			}
		}
	}
	for _, row := range days {
		out.Days = append(out.Days, *row)
	}
	for _, row := range months {
		out.Months = append(out.Months, *row)
	}
	sort.Slice(out.Days, func(i, j int) bool { return out.Days[i].Period < out.Days[j].Period })
	sort.Slice(out.Months, func(i, j int) bool { return out.Months[i].Period < out.Months[j].Period })
	return out, nil
}

func addRechargeRehearsalAmounts(out *FinanceRechargeRehearsalAmounts, in FinanceRechargeRehearsalAmounts) error {
	for _, pair := range []struct {
		target *int64
		value  int64
	}{
		{&out.Buckets, in.Buckets}, {&out.NewlyCosted, in.NewlyCosted}, {&out.Repriced, in.Repriced},
		{&out.RemainingUnknown, in.RemainingUnknown}, {&out.RawMicro, in.RawMicro},
		{&out.KnownBeforeMicro, in.KnownBeforeMicro}, {&out.KnownAfterMicro, in.KnownAfterMicro},
	} {
		if err := addEconomicsInt64(pair.target, pair.value); err != nil {
			return err
		}
	}
	return nil
}

// Verify the persisted copy against the pre-write projection, including each
// day and month. A successful insert alone is not an accounting acceptance.
func verifyFinanceRechargeRehearsalBalances(expected, reloaded FinanceRechargeRehearsalResult) error {
	if len(expected.Domains) != len(reloaded.Domains) {
		return errors.New("rehearsal changed the accounting domain scope")
	}
	for i, before := range expected.Domains {
		after := reloaded.Domains[i]
		if before.Domain != after.Domain || before.BillComplete != after.BillComplete ||
			len(before.Days) != len(after.Days) || len(before.Months) != len(after.Months) {
			return errors.New("rehearsal changed original bill coverage")
		}
		want := append(append([]FinanceRechargeRehearsalAmounts{before.Total}, before.Months...), before.Days...)
		got := append(append([]FinanceRechargeRehearsalAmounts{after.Total}, after.Months...), after.Days...)
		for j, amount := range want {
			amount.KnownBeforeMicro = amount.KnownAfterMicro
			amount.NewlyCosted, amount.Repriced = 0, 0
			if amount != got[j] {
				return errors.New("persisted rehearsal day/month/total differs from the projected cost")
			}
		}
	}
	return nil
}
