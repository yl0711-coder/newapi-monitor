package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"gorm.io/gorm"
)

type economicsCoverageFixture struct {
	withCost          bool
	allocatedCost     bool
	withLocal         bool
	localVerified     bool
	withFinance       bool
	unallocatedRefund bool
}

func publishEconomicsCoverageFixture(t *testing.T, fixture economicsCoverageFixture) (*gorm.DB, []ChannelEconomicsHourPublication) {
	t.Helper()
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&StabilityHourSample{}, &StabilityHourIngestState{}, &ChannelSnap{}, &ChannelDomainCost{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: quotaPerUSD}
	epoch, hour := newAPIUpstreamAccountEpoch(account), int64(3600)
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{account.Domain}}}
	state := ChannelUpstreamCostHourState{
		Domain: account.Domain, AccountEpoch: epoch, HourTs: hour,
		SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: account.Provider,
		Status: "verified", ReconcileStatus: "matched", ChargeUnitsPerUSD: "500000",
		ContentHash: strings.Repeat("e", 64),
	}
	if fixture.withCost {
		state.ControlChargeUnits, state.EvidenceChargeUnits, state.Requests, state.EvidenceRows = 500000, 500000, 10, 1
		row := ChannelUpstreamCostHourEvidence{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: hour,
			SemanticsVersion: channelCostEvidenceSemanticsVersion,
			SourceRef:        strings.Repeat("a", 64), DimensionHash: strings.Repeat("b", 64),
			Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken,
			HMACKeyID: "key-v1", PricingDimensionHash: strings.Repeat("c", 64),
			ChargeUnits: 500000, ChargeUnit: channelCostChargeUnitNewAPIQuota,
			ChargeUnitsPerUSD: "500000", Requests: 10, ContentHash: strings.Repeat("d", 64),
		}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		if fixture.allocatedCost {
			binding := ChannelCostSourceBinding{
				Domain: account.Domain, AccountEpoch: epoch, SourceRef: row.SourceRef, ValidFrom: hour,
				Provider: account.Provider, SourceRefKind: row.SourceRefKind, HMACKeyID: row.HMACKeyID,
				LocalChannelID: 59, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual",
			}
			if err := m.saveCostSourceBinding(context.Background(), binding); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 59, BaseDomain: account.Domain}).Error; err != nil {
		t.Fatal(err)
	}
	if fixture.withLocal {
		if err := db.Create(&StabilityHourSample{
			HourTs: hour, ChannelID: 59, ModelName: "gpt-5.5", Grp: "codex-1.2x",
			TrafficClassVersion: userTrafficClassificationVersion, Success: 10, Quota: 1_000_000,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if fixture.unallocatedRefund {
		if err := db.Create(&StabilityHourSample{
			HourTs: hour, ChannelID: 0, ModelName: "refund", Grp: "",
			TrafficClassVersion: userTrafficClassificationVersion, RefundRecords: 1, RefundQuota: 100_000,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if fixture.localVerified {
		if err := db.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: userTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if fixture.withFinance {
		snapshot, err := json.Marshal(channelFinanceVersionSnapshot{Domain: account.Domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&ChannelFinanceVersion{Domain: account.Domain, Version: 1, SnapshotJSON: string(snapshot), EffectiveAt: hour}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return m.enqueueEconomicsForLocalFactTx(tx, hour, hour+3599)
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "coverage_test", hour+3600); err != nil {
		t.Fatal(err)
	}
	var publications []ChannelEconomicsHourPublication
	if err := db.Order("local_channel_id").Find(&publications).Error; err != nil {
		t.Fatal(err)
	}
	return db, publications
}

func publicationForChannel(t *testing.T, rows []ChannelEconomicsHourPublication, channelID int) ChannelEconomicsHourPublication {
	t.Helper()
	for _, row := range rows {
		if row.LocalChannelID == channelID {
			return row
		}
	}
	t.Fatalf("channel %d publication missing: %+v", channelID, rows)
	return ChannelEconomicsHourPublication{}
}

func TestChannelEconomicsCoverageFailsProfitClosed(t *testing.T) {
	tests := []struct {
		name          string
		fixture       economicsCoverageFixture
		channelID     int
		coverage      string
		costKnown     bool
		profitKnown   bool
		profitNonzero bool
	}{
		{"verified complete", economicsCoverageFixture{true, true, true, true, true, false}, 59, "verified_complete", true, true, true},
		{"finance missing", economicsCoverageFixture{true, true, true, true, false, false}, 59, "finance_version_missing", false, false, false},
		{"unallocated refund", economicsCoverageFixture{true, true, true, true, true, true}, 59, "refund_unallocated", true, false, false},
		{"unallocated cost", economicsCoverageFixture{true, false, false, true, true, false}, 0, "unallocated_cost", true, false, false},
		{"upstream cost missing", economicsCoverageFixture{false, false, true, true, true, false}, 59, "upstream_cost_missing", false, false, false},
		{"local revenue missing", economicsCoverageFixture{true, true, false, true, true, false}, 59, "local_revenue_missing", true, false, false},
		{"local fact unverified", economicsCoverageFixture{true, true, true, false, true, false}, 59, "local_fact_unverified", true, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, rows := publishEconomicsCoverageFixture(t, tt.fixture)
			row := publicationForChannel(t, rows, tt.channelID)
			if row.CoverageStatus != tt.coverage || row.CorrectedCostKnown != tt.costKnown || row.ProfitKnown != tt.profitKnown {
				t.Fatalf("coverage flags mismatch: %+v", row)
			}
			if tt.profitNonzero != (row.ProfitMicroUSD != 0) {
				t.Fatalf("profit fail-closed mismatch: %+v", row)
			}
		})
	}
}

func TestChannelEconomicsUnallocatedRefundIsStoredOnlyAsGlobalFact(t *testing.T) {
	db, rows := publishEconomicsCoverageFixture(t, economicsCoverageFixture{true, true, true, true, true, true})
	for _, row := range rows {
		if row.UnallocatedRefundRecords != 0 || row.UnallocatedRefundQuota != 0 {
			t.Fatalf("global refund copied into domain/channel publication: %+v", row)
		}
		if row.ProfitKnown || row.ProfitMicroUSD != 0 {
			t.Fatalf("unallocated refund must make every profit unknown: %+v", row)
		}
	}
	var global ChannelEconomicsGlobalHourFact
	if err := db.First(&global, "hour_ts = ? AND semantics_version = ?", 3600, channelEconomicsSemanticsVersion).Error; err != nil {
		t.Fatal(err)
	}
	if global.UnallocatedRefundRecords != 1 || global.UnallocatedRefundQuota != 100_000 {
		t.Fatalf("global refund fact not conserved: %+v", global)
	}
}

func TestChannelEconomicsRemapSupersedesOldAttributionWithoutDoubleCount(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&StabilityHourSample{}, &StabilityHourIngestState{}, &ChannelSnap{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: quotaPerUSD}
	epoch, hour := newAPIUpstreamAccountEpoch(account), int64(3600)
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{account.Domain}}}
	sourceRef := strings.Repeat("a", 64)
	if err := db.Create(&ChannelUpstreamCostHourEvidence{
		Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
		SourceRef: sourceRef, DimensionHash: strings.Repeat("b", 64), Provider: account.Provider,
		SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1", PricingDimensionHash: strings.Repeat("c", 64),
		ChargeUnits: 500000, ChargeUnit: channelCostChargeUnitNewAPIQuota, ChargeUnitsPerUSD: "500000", Requests: 10, ContentHash: strings.Repeat("d", 64),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelUpstreamCostHourState{
		Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
		Provider: account.Provider, Status: "verified", ReconcileStatus: "matched", ControlChargeUnits: 500000,
		EvidenceChargeUnits: 500000, ChargeUnitsPerUSD: "500000", Requests: 10, EvidenceRows: 1, ContentHash: strings.Repeat("e", 64),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 59, BaseDomain: account.Domain}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&StabilityHourSample{HourTs: hour, ChannelID: 59, ModelName: "gpt-5.5", Grp: "codex-1.2x", TrafficClassVersion: userTrafficClassificationVersion, Success: 10, Quota: 1_000_000}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: userTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, _ := json.Marshal(channelFinanceVersionSnapshot{Domain: account.Domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 1})
	if err := db.Create(&ChannelFinanceVersion{Domain: account.Domain, Version: 1, SnapshotJSON: string(snapshot), EffectiveAt: hour}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "before_mapping", hour+3600); err != nil {
		t.Fatal(err)
	}
	if err := m.saveCostSourceBinding(context.Background(), ChannelCostSourceBinding{
		Domain: account.Domain, AccountEpoch: epoch, SourceRef: sourceRef, ValidFrom: hour,
		Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1",
		LocalChannelID: 59, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual",
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "after_mapping", hour+7200); err != nil {
		t.Fatal(err)
	}
	var current []ChannelEconomicsHourCurrent
	if err := db.Order("logical_key").Find(&current).Error; err != nil {
		t.Fatal(err)
	}
	if len(current) != 2 {
		t.Fatalf("current logical keys=%d, want old and new attribution", len(current))
	}
	var currentRows []ChannelEconomicsHourPublication
	for _, pointer := range current {
		var row ChannelEconomicsHourPublication
		if err := db.First(&row, "publication_id = ?", pointer.PublicationID).Error; err != nil {
			t.Fatal(err)
		}
		currentRows = append(currentRows, row)
	}
	old := publicationForChannel(t, currentRows, 0)
	if old.CoverageStatus != "superseded_empty" || old.LocalRequests != 0 || old.UpstreamRequests != 0 || old.RevenueMicroUSD != 0 || old.UpstreamCostMicroUSD != 0 || old.CorrectedCostMicroUSD != 0 || old.ProfitMicroUSD != 0 || old.ProfitKnown {
		t.Fatalf("old attribution was not zero-superseded: %+v", old)
	}
	allocated := publicationForChannel(t, currentRows, 59)
	if allocated.UpstreamCostMicroUSD != 1_000_000 || allocated.CoverageStatus != "verified_complete" {
		t.Fatalf("new attribution missing exact cost: %+v", allocated)
	}
	var currentCost int64
	for _, row := range currentRows {
		currentCost += row.UpstreamCostMicroUSD
	}
	if currentCost != 1_000_000 {
		t.Fatalf("remap double-counted upstream cost: %d", currentCost)
	}
	var beforeRerun int64
	_ = db.Model(&ChannelEconomicsHourPublication{}).Count(&beforeRerun).Error
	if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "idempotent_rerun", hour+10800); err != nil {
		t.Fatal(err)
	}
	var afterRerun int64
	_ = db.Model(&ChannelEconomicsHourPublication{}).Count(&afterRerun).Error
	if afterRerun != beforeRerun {
		t.Fatalf("unchanged remap rerun created revision: before=%d after=%d", beforeRerun, afterRerun)
	}
}

func TestChannelEconomicsGlobalRefundConservedAcrossDomains(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&StabilityHourSample{}, &StabilityHourIngestState{}, &ChannelSnap{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	hour := int64(3600)
	domains := []struct {
		domain    string
		channelID int
		refByte   string
	}{
		{"4sapi.com", 59, "a"},
		{"codeyu.shop", 60, "f"},
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com", "codeyu.shop"}}}
	for _, domain := range domains {
		account := ChannelUpstreamAccount{Domain: domain.domain, Provider: upstreamProviderNewAPI, BaseURL: "https://" + domain.domain, UserID: 1, BalanceUnit: quotaPerUSD}
		epoch := newAPIUpstreamAccountEpoch(account)
		sourceRef := strings.Repeat(domain.refByte, 64)
		if err := db.Create(&ChannelUpstreamCostHourEvidence{
			Domain: domain.domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			SourceRef: sourceRef, DimensionHash: strings.Repeat("b", 64), Provider: account.Provider,
			SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1", PricingDimensionHash: strings.Repeat("c", 64),
			ChargeUnits: 500000, ChargeUnit: channelCostChargeUnitNewAPIQuota, ChargeUnitsPerUSD: "500000",
			Requests: 10, ContentHash: strings.Repeat("d", 64),
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&ChannelUpstreamCostHourState{
			Domain: domain.domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			Provider: account.Provider, Status: "verified", ReconcileStatus: "matched",
			ControlChargeUnits: 500000, EvidenceChargeUnits: 500000, ChargeUnitsPerUSD: "500000",
			Requests: 10, EvidenceRows: 1, ContentHash: strings.Repeat("e", 64),
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&ChannelSnap{ID: domain.channelID, BaseDomain: domain.domain}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&StabilityHourSample{
			HourTs: hour, ChannelID: domain.channelID, ModelName: "gpt-5.5", Grp: "codex",
			TrafficClassVersion: userTrafficClassificationVersion, Success: 10, Quota: 1_000_000,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.saveCostSourceBinding(context.Background(), ChannelCostSourceBinding{
			Domain: domain.domain, AccountEpoch: epoch, SourceRef: sourceRef, ValidFrom: hour,
			Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1",
			LocalChannelID: domain.channelID, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual",
		}); err != nil {
			t.Fatal(err)
		}
		snapshot, _ := json.Marshal(channelFinanceVersionSnapshot{Domain: domain.domain, UpstreamRechargePaid: 1, UpstreamRechargeCredit: 1})
		if err := db.Create(&ChannelFinanceVersion{Domain: domain.domain, Version: 1, SnapshotJSON: string(snapshot), EffectiveAt: hour}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&StabilityHourSample{
		HourTs: hour, ChannelID: 0, ModelName: "refund", TrafficClassVersion: userTrafficClassificationVersion,
		RefundRecords: 1, RefundQuota: 100_000,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: userTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return m.enqueueEconomicsForLocalFactTx(tx, hour, hour+3599)
	}); err != nil {
		t.Fatal(err)
	}
	for _, domain := range domains {
		account := ChannelUpstreamAccount{Domain: domain.domain, Provider: upstreamProviderNewAPI, BaseURL: "https://" + domain.domain, UserID: 1, BalanceUnit: quotaPerUSD}
		if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "cross_domain", hour+3600); err != nil {
			t.Fatal(err)
		}
	}
	var globalCount int64
	if err := db.Model(&ChannelEconomicsGlobalHourFact{}).Count(&globalCount).Error; err != nil || globalCount != 1 {
		t.Fatalf("global fact duplicated by domain: count=%d err=%v", globalCount, err)
	}
	var global ChannelEconomicsGlobalHourFact
	if err := db.First(&global).Error; err != nil || global.UnallocatedRefundRecords != 1 || global.UnallocatedRefundQuota != 100_000 {
		t.Fatalf("global fact invalid: %+v err=%v", global, err)
	}
	var publications []ChannelEconomicsHourPublication
	if err := db.Find(&publications).Error; err != nil {
		t.Fatal(err)
	}
	if len(publications) != 2 {
		t.Fatalf("unexpected publication count: %d %+v", len(publications), publications)
	}
	for _, row := range publications {
		if row.UnallocatedRefundRecords != 0 || row.UnallocatedRefundQuota != 0 || row.ProfitKnown || row.CoverageStatus != "refund_unallocated" {
			t.Fatalf("domain publication duplicated/accepted global refund: %+v", row)
		}
	}
}

func TestGlobalRefundChangeQueuesEveryDomainBeforeWatermarkAdvance(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&StabilityHourSample{}, &ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	hour := int64(3600)
	domains := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		domain := fmt.Sprintf("upstream-%02d.example", i)
		domains = append(domains, domain)
		if err := db.Create(&ChannelUpstreamCostHourState{
			Domain: domain, AccountEpoch: strings.Repeat(fmt.Sprintf("%x", i%16), 64), HourTs: hour,
			SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: upstreamProviderNewAPI,
			Status: "verified", ReconcileStatus: "matched", ChargeUnitsPerUSD: "500000",
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: domains}}
	if err := db.Create(&ChannelEconomicsGlobalHourFact{HourTs: hour, SemanticsVersion: channelEconomicsSemanticsVersion, SourceHash: strings.Repeat("0", 64)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&StabilityHourSample{HourTs: hour, ChannelID: 0, ModelName: "refund", TrafficClassVersion: userTrafficClassificationVersion, RefundRecords: 1, RefundQuota: 100_000}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return m.enqueueChangedEconomicsLocalHoursTx(tx, hour, hour+3600, 1)
	}); err != nil {
		t.Fatal(err)
	}
	var dirtyCount int64
	if err := db.Model(&ChannelEconomicsDirtyHour{}).Where("hour_ts = ? AND reason = ?", hour, "global_refund_changed").Count(&dirtyCount).Error; err != nil || dirtyCount != 20 {
		t.Fatalf("global refund queued %d/20 domains err=%v", dirtyCount, err)
	}
	var global ChannelEconomicsGlobalHourFact
	if err := db.First(&global, "hour_ts = ? AND semantics_version = ?", hour, channelEconomicsSemanticsVersion).Error; err != nil {
		t.Fatal(err)
	}
	if global.UnallocatedRefundRecords != 1 || global.UnallocatedRefundQuota != 100_000 {
		t.Fatalf("global watermark advanced incorrectly: %+v", global)
	}
}
