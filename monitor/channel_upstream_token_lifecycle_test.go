package monitor

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestUpstreamTokenLifecycleChangedBackRequiresNewProof(t *testing.T) {
	f := newTokenBindingPlanFixture()
	history := simulateTokenBindingPlan(t, nil, f.plan(t, nil, 7205))
	// A -> B -> A is not the old verified configuration, even when both
	// edits occurred in the same second and the final ID looks unchanged.
	f.configs[0].Revision += 2
	f.configs[0].CreatedAt = 10900
	p := f.plan(t, history, 10900)
	if len(p.Changes) != 1 || len(p.RequiredSources) != 1 || p.Changes[0].After.LocalChannelID != 185 || p.Changes[0].After.ValidFrom != 14400 {
		t.Fatalf("changed-back configuration bypassed proof: %+v", p)
	}
	if upstreamTokenBindingOwnerHash(*p.Changes[0].Before) == upstreamTokenBindingOwnerHash(p.Changes[0].After) {
		t.Fatal("revision not fenced")
	}
}

func TestUpstreamTokenLifecycleHourlyOwnershipAndMoneyConservation(t *testing.T) {
	f, in, now := newAPITokenStoreFixture(t)
	db, ctx := f.m.storeDB, context.Background()
	if err := db.AutoMigrate(&StabilityHourSample{}, &ChannelTestHourSample{}, &StabilityHourIngestState{}, &ChannelDomainCost{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	activation, err := f.m.commitNewAPITokenBindingPlan(ctx, in, "tester", now)
	if err != nil {
		t.Fatal(err)
	}
	hour := activation.EffectiveFrom
	finance, _ := json.Marshal(channelFinanceVersionSnapshot{Domain: f.account.Domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 10})
	if err := db.Create(&ChannelFinanceVersion{Domain: f.account.Domain, Version: 1, EffectiveAt: hour, SnapshotJSON: string(finance)}).Error; err != nil {
		t.Fatal(err)
	}
	var baseRows []ChannelUpstreamCostHourEvidence
	var baseState ChannelUpstreamCostHourState
	if err := db.Where("hour_ts=?", f.day+36000).Find(&baseRows).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("hour_ts=?", f.day+36000).First(&baseState).Error; err != nil {
		t.Fatal(err)
	}
	for h := hour; h <= hour+4*3600; h += 3600 {
		rows := append([]ChannelUpstreamCostHourEvidence(nil), baseRows...)
		for i := range rows {
			rows[i].HourTs, rows[i].FirstSourceAt, rows[i].LastSourceAt, rows[i].FetchedAt = h, h+1, h+1, h+3601
			rows[i].ContentHash = channelCostEvidenceRowHash(rows[i])
		}
		state := baseState
		state.HourTs, state.CompletedAt, state.UpdatedAt = h, h+3601, h+3601
		state.ContentHash = channelCostEvidenceContentHash(rows)
		if err := db.Create(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&state).Error; err != nil {
			t.Fatal(err)
		}
		for _, id := range []int{185, 186} {
			if err := db.Create(&StabilityHourSample{HourTs: h, ChannelID: id, ModelName: "model-a", Grp: "local", TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 1, Quota: 500000}).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Create(&StabilityHourIngestState{HourTs: h, Status: "complete", TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	publish := func(h int64, want map[int]int64) []ChannelEconomicsHourPublication {
		t.Helper()
		if err := f.m.publishChannelEconomicsHour(ctx, f.account, h, "local lifecycle acceptance", h+3605); err != nil {
			t.Fatal(err)
		}
		var rows []ChannelEconomicsHourPublication
		if err := db.Table("channel_economics_hour_publications p").Select("p.*").Joins("JOIN channel_economics_hour_current c ON c.publication_id=p.publication_id").Where("p.domain=? AND p.hour_ts=?", f.account.Domain, h).Order("p.local_channel_id").Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		got := map[int]int64{}
		var units, revenue, corrected int64
		for _, row := range rows {
			got[row.LocalChannelID] = row.UpstreamChargeUnits
			units += row.UpstreamChargeUnits
			revenue += row.RevenueMicroUSD
			corrected += row.CorrectedCostMicroUSD
			if row.LocalChannelID == 0 && (row.ProfitKnown || row.CoverageStatus != "unallocated_cost") {
				t.Fatal("unallocated cost claimed profit", row)
			}
		}
		if !reflect.DeepEqual(got, want) || units != 375000 || revenue != 2000000 || corrected != 75000 {
			t.Fatalf("hour %d: attribution=%v want=%v totals=%d/%d/%d", h, got, want, units, revenue, corrected)
		}
		return rows
	}
	original := publish(hour, map[int]int64{185: 125000, 186: 250000})
	// Pending sharing must stop BOTH old assignments at the next boundary.
	if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 186, "575", 1), "tester", hour+123); err != nil {
		t.Fatal(err)
	}
	if after := publish(hour, map[int]int64{185: 125000, 186: 250000}); !reflect.DeepEqual(after, original) {
		t.Fatal("future edit revised old publication")
	}
	publish(hour+3600, map[int]int64{0: 375000, 185: 0, 186: 0})
	// Verification after this hour starts can only activate the following
	// hour. Confirmed shared ownership remains unallocated, not duplicated.
	p, err := f.m.previewUpstreamTokenBindingPlan(ctx, f.account.Domain, 185, hour+3601)
	if err != nil {
		t.Fatal(err)
	}
	in.PlanDigest = p.Digest
	if _, err := f.m.commitNewAPITokenBindingPlan(ctx, in, "tester", hour+3601); err != nil {
		t.Fatal(err)
	}
	publish(hour+7200, map[int]int64{0: 375000, 185: 0, 186: 0})
	// Separate again; the already-published shared hour stays unchanged.
	if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 186, "574", 2), "tester", hour+7201); err != nil {
		t.Fatal(err)
	}
	p, err = f.m.previewUpstreamTokenBindingPlan(ctx, f.account.Domain, 185, hour+7202)
	if err != nil {
		t.Fatal(err)
	}
	in.PlanDigest = p.Digest
	if _, err := f.m.commitNewAPITokenBindingPlan(ctx, in, "tester", hour+7202); err != nil {
		t.Fatal(err)
	}
	publish(hour+10800, map[int]int64{185: 125000, 186: 250000})
	// An edit exactly on the boundary applies only to the next full hour.
	if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 185, "", 1), "tester", hour+10800); err != nil {
		t.Fatal(err)
	}
	publish(hour+10800, map[int]int64{185: 125000, 186: 250000})
	publish(hour+14400, map[int]int64{0: 125000, 185: 0, 186: 250000})
	publish(hour, map[int]int64{185: 125000, 186: 250000})
	// A failed lifecycle read must not replace an already published report
	// with a partial or incorrectly attributed result. In-memory fixture only.
	var before, after []ChannelEconomicsHourPublication
	if err := db.Order("publication_id").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Migrator().DropTable(&ChannelUpstreamTokenConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := f.m.publishChannelEconomicsHour(ctx, f.account, hour+14400, "synthetic read failure", hour+18001); err == nil {
		t.Fatal("missing ownership evidence silently accepted")
	}
	if err := db.Order("publication_id").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed ownership check modified existing publications")
	}
}

func TestUpstreamTokenLifecycleRejectsForgedOwnerAndMissingFingerprint(t *testing.T) {
	f := newTokenBindingPlanFixture()
	p := f.plan(t, nil, 7205)
	var identities []upstreamTokenBindingConfigIdentity
	for _, c := range f.configs {
		source, err := channelCostSourceRef(f.key, f.account.Provider, newAPIUpstreamAccountEpoch(f.account), sub2TokenSourceKind, c.TokenID)
		if err != nil {
			t.Fatal(err)
		}
		identities = append(identities, upstreamTokenBindingConfigIdentity{ChannelID: c.ChannelID, Revision: c.Revision, CreatedAt: c.CreatedAt, SourceRef: source})
	}
	hashes := upstreamTokenOwnerHashes(f.account.Domain, newAPIUpstreamAccountEpoch(f.account), "v1", identities)
	base := p.Changes[0].After
	if !upstreamTokenBindingMatchesOwners(base, hashes, "v1") {
		t.Fatal("valid owner rejected")
	}
	for _, kind := range []string{"channel", "key", "missing", "duplicate"} {
		row := base
		switch kind {
		case "channel":
			row.LocalChannelID = 999
		case "key":
			row.HMACKeyID = "wrong"
		case "missing":
			row.Reason = ""
		case "duplicate":
			row.Reason += " config=" + upstreamTokenBindingOwnerHash(base)
		}
		if upstreamTokenBindingMatchesOwners(row, hashes, "v1") {
			t.Fatal("invalid ownership accepted", kind)
		}
	}
}
