package monitor

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestChannelEconomicsProfitSubtractionRejectsUnderflow(t *testing.T) {
	if got, err := subtractNonnegativeInt64(-100, 25); err != nil || got != -125 {
		t.Fatalf("normal negative profit got=%d err=%v", got, err)
	}
	if _, err := subtractNonnegativeInt64(math.MinInt64+1, 2); err == nil {
		t.Fatal("profit underflow was accepted")
	}
}

func TestEconomicsFinanceUsesImmutableWholeHourVersionBoundary(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelFinanceVersion{}, &ChannelDomainCost{}); err != nil {
		t.Fatal(err)
	}
	domain, hour := "4sapi.com", int64(3600)
	first, _ := json.Marshal(channelFinanceVersionSnapshot{Domain: domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 10})
	second, _ := json.Marshal(channelFinanceVersionSnapshot{Domain: domain, UpstreamRechargePaid: 2, UpstreamRechargeCredit: 10})
	if err := db.Create(&ChannelFinanceVersion{Domain: domain, Version: 1, SnapshotJSON: string(first), EffectiveAt: hour}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceVersion{Domain: domain, Version: 2, SnapshotJSON: string(second), EffectiveAt: hour + 3600}).Error; err != nil {
		t.Fatal(err)
	}
	// Mutable current state deliberately disagrees with both historical rows.
	if err := db.Create(&ChannelDomainCost{Domain: domain, RechargePaid: 9, RechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	version, paid, credit, known, err := economicsFinanceAt(db, domain, hour+3599)
	if err != nil || !known || version != 1 || paid != 1 || credit != 10 {
		t.Fatalf("H+3599 selected wrong finance version v=%d paid=%v credit=%v known=%v err=%v", version, paid, credit, known, err)
	}
	version, paid, credit, known, err = economicsFinanceAt(db, domain, hour+3600)
	if err != nil || !known || version != 2 || paid != 2 || credit != 10 {
		t.Fatalf("H+1h boundary selected wrong finance version v=%d paid=%v credit=%v known=%v err=%v", version, paid, credit, known, err)
	}
	version, _, _, known, err = economicsFinanceAt(db, domain, hour-1)
	if err != nil || known || version != 0 {
		t.Fatalf("pre-version hour read mutable current: v=%d known=%v err=%v", version, known, err)
	}
}

func TestChannelEconomicsPublishesImmutableRevisionsAndCorrectedProfit(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&StabilityHourSample{}, &ChannelTestHourSample{}, &StabilityHourIngestState{}, &ChannelSnap{}, &ChannelDomainCost{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	cfg := Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}}
	m := &Monitor{storeDB: db, cfg: cfg}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: quotaPerUSD}
	epoch, hour := newAPIUpstreamAccountEpoch(account), int64(3600)
	sourceRef := strings.Repeat("a", 64)
	costRow := ChannelUpstreamCostHourEvidence{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, SourceRef: sourceRef, DimensionHash: strings.Repeat("b", 64), Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1", PricingDimensionHash: strings.Repeat("c", 64), ChargeUnits: 500000, ChargeUnit: channelCostChargeUnitNewAPIQuota, ChargeUnitsPerUSD: "500000", Requests: 10, ContentHash: strings.Repeat("d", 64)}
	if err := db.Create(&costRow).Error; err != nil {
		t.Fatal(err)
	}
	state := ChannelUpstreamCostHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: account.Provider, Status: "verified", ReconcileStatus: "matched", ControlChargeUnits: 500000, EvidenceChargeUnits: 500000, ChargeUnitsPerUSD: "500000", Requests: 10, EvidenceRows: 1, ContentHash: strings.Repeat("e", 64)}
	if err := db.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	binding := ChannelCostSourceBinding{Domain: account.Domain, AccountEpoch: epoch, SourceRef: sourceRef, ValidFrom: hour, Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1", LocalChannelID: 59, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual"}
	if err := m.saveCostSourceBinding(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 59, BaseDomain: account.Domain}).Error; err != nil {
		t.Fatal(err)
	}
	local := StabilityHourSample{HourTs: hour, ChannelID: 59, ModelName: "gpt-5.5", Grp: "codex-1.2x", TrafficClassVersion: userTrafficClassificationVersion, Success: 10, Quota: 1_000_000, RefundRecords: 1, RefundQuota: 100_000}
	if err := db.Create(&local).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: userTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(channelFinanceVersionSnapshot{Domain: account.Domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceVersion{Domain: account.Domain, Version: 1, SnapshotJSON: string(snapshot), EffectiveAt: hour}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "test", hour+3600); err != nil {
		t.Fatal(err)
	}
	var first ChannelEconomicsHourPublication
	if err := db.First(&first).Error; err != nil {
		t.Fatal(err)
	}
	if first.RevenueMicroUSD != 1_800_000 || first.LocalNetQuota != 900_000 || first.UpstreamCostMicroUSD != 1_000_000 || first.CorrectedCostMicroUSD != 100_000 || first.ProfitMicroUSD != 1_700_000 || first.CoverageStatus != "verified_complete" {
		t.Fatalf("unexpected economics publication: %+v", first)
	}
	if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "same", hour+3601); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&ChannelEconomicsHourPublication{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("identical source published duplicate revision: count=%d err=%v", count, err)
	}
	late := local
	late.Quota = 1_500_000
	if err := m.replaceStabilityHourTraffic(hour, []StabilityHourSample{late}, nil, StabilityHourIngestState{HourTs: hour, JobID: "late-replace"}); err != nil {
		t.Fatal(err)
	}
	var dirty ChannelEconomicsDirtyHour
	if err := db.First(&dirty, "domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, epoch, hour).Error; err != nil || dirty.Status != "pending" || dirty.Generation < 1 {
		t.Fatalf("late authoritative replace did not enqueue economics: %+v err=%v", dirty, err)
	}
	workerNow := time.Now().Unix() + 1
	if err := m.publishOneDueChannelEconomicsHour(context.Background(), account, workerNow); err != nil {
		t.Fatal(err)
	}
	var versions []ChannelEconomicsHourPublication
	if err := db.Order("revision").Find(&versions).Error; err != nil || len(versions) != 2 {
		t.Fatalf("late data did not append revision: versions=%+v err=%v", versions, err)
	}
	if versions[0].PublicationID == versions[1].PublicationID || versions[1].Revision != 2 || versions[1].SupersedesPublicationID != versions[0].PublicationID || versions[1].RevenueMicroUSD != 2_800_000 {
		t.Fatalf("immutable revision chain invalid: %+v", versions)
	}
	var current ChannelEconomicsHourCurrent
	if err := db.First(&current).Error; err != nil || current.PublicationID != versions[1].PublicationID || current.Revision != 2 {
		t.Fatalf("current pointer not atomically advanced: %+v err=%v", current, err)
	}
	var dirtyCount int64
	_ = db.Model(&ChannelEconomicsDirtyHour{}).Count(&dirtyCount).Error
	if dirtyCount != 0 {
		t.Fatalf("completed dirty generation not deleted: %d", dirtyCount)
	}
	if err := m.replaceStabilityHourTraffic(hour, []StabilityHourSample{late}, nil, StabilityHourIngestState{HourTs: hour, JobID: "same-replace"}); err != nil {
		t.Fatal(err)
	}
	if err := m.publishOneDueChannelEconomicsHour(context.Background(), account, time.Now().Unix()+1); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelEconomicsHourPublication{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("unchanged authoritative replace created duplicate revision: count=%d err=%v", count, err)
	}
}
