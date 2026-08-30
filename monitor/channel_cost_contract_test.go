package monitor

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func newChannelCostTestStore(t *testing.T) *gorm.DB {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	db, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}, &ChannelCostPageCheckpoint{}, &ChannelCostSourceBinding{}, &ChannelCostDirtyHour{}, &ChannelCostKeyRegistry{}, &ChannelPricingChangeProposal{}, &ChannelPricingProposalEvent{}, &ChannelFinanceActivation{}, &ChannelFinanceActivationSlot{}, &ChannelFinanceActivationEvent{}, &ChannelEconomicsHourPublication{}, &ChannelEconomicsHourCurrent{}, &ChannelEconomicsHourManifestPublication{}, &ChannelEconomicsHourManifestCurrent{}, &ChannelEconomicsGlobalHourFact{}, &ChannelEconomicsDirtyHour{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

func TestChannelCostSourceIdentityHMACContract(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	epoch := strings.Repeat("a", 64)
	first, err := channelCostSourceRef(key, "newapi", epoch, channelCostSourceKindNewAPIToken, "75")
	if err != nil {
		t.Fatal(err)
	}
	again, err := channelCostSourceRef(key, " NEWAPI ", epoch, channelCostSourceKindNewAPIToken, "75")
	if err != nil || first != again || len(first) != 64 {
		t.Fatalf("source HMAC not stable: %q %q err=%v", first, again, err)
	}
	otherEpoch, _ := channelCostSourceRef(key, "newapi", strings.Repeat("b", 64), channelCostSourceKindNewAPIToken, "75")
	otherKind, _ := channelCostSourceRef(key, "newapi", epoch, "aicodewith_slot", "75")
	if first == otherEpoch || first == otherKind {
		t.Fatal("source HMAC must be isolated by epoch and source kind")
	}
	if strings.Contains(first, "75") {
		t.Fatal("raw source identity leaked into persisted reference")
	}
	if _, err := channelCostSourceRef([]byte("short"), "newapi", epoch, channelCostSourceKindNewAPIToken, "75"); err == nil {
		t.Fatal("short HMAC key must fail closed")
	}
}

func TestChannelCostKeyRegistryRejectsSilentRotation(t *testing.T) {
	db := newChannelCostTestStore(t)
	m := &Monitor{storeDB: db, cfg: Settings{
		ChannelCostClosureEnabled: true, ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "v1",
	}}
	if err := m.validateChannelCostKeyRegistry(); err != nil {
		t.Fatal(err)
	}
	if err := m.validateChannelCostKeyRegistry(); err != nil {
		t.Fatalf("same key must survive restart: %v", err)
	}
	m.cfg.ChannelCostHMACKey = "abcdef0123456789abcdef0123456789"
	if err := m.validateChannelCostKeyRegistry(); err == nil {
		t.Fatal("same key ID with changed key must fail closed")
	}
	m.cfg.ChannelCostHMACKey = "0123456789abcdef0123456789abcdef"
	m.cfg.ChannelCostHMACKeyID = "v2"
	if err := m.validateChannelCostKeyRegistry(); err == nil {
		t.Fatal("silent key ID rotation must fail closed")
	}
}

func TestChannelCostAccountIdentityFailsClosed(t *testing.T) {
	base := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1}
	if err := validateChannelCostAccountIdentity(base); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []ChannelUpstreamAccount{
		{Provider: base.Provider, BaseURL: base.BaseURL, UserID: base.UserID},
		{Domain: base.Domain, BaseURL: base.BaseURL, UserID: base.UserID},
		{Domain: base.Domain, Provider: base.Provider, UserID: base.UserID},
		{Domain: base.Domain, Provider: base.Provider, BaseURL: base.BaseURL},
	} {
		if err := validateChannelCostAccountIdentity(invalid); err == nil {
			t.Fatalf("incomplete account identity accepted: %+v", invalid)
		}
	}
}

func TestBuildNewAPICostHourEvidenceUsesExactIntegersAndConserves(t *testing.T) {
	hour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 147426, Account: "billing@example.com"}
	pricing := newAPIPricingAttributes{GroupName: "Gpt-codex", ModelName: "gpt-5.5", TokenID: 75, BillingMode: "token", GroupRatioState: pricingRatioValid, GroupRatioCanonical: "1", EffectiveRatioSource: "group_ratio", EffectiveRatio: "1"}
	items := []newAPIPricingUsageItem{
		{CreatedAt: hour + 1, QuotaExact: 7, QuotaExactKnown: true, PromptTokens: 11, CompletionTokens: 2, TokensExactKnown: true, Pricing: pricing},
		{CreatedAt: hour + 2, QuotaExact: 9, QuotaExactKnown: true, PromptTokens: 13, CompletionTokens: 3, TokensExactKnown: true, Pricing: pricing},
	}
	rows, state, err := buildNewAPICostHourEvidence(account, items, hour, hour+3600, []byte("0123456789abcdef0123456789abcdef"), "cost-source-v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChargeUnits != 16 || rows[0].Requests != 2 || rows[0].PromptTokens != 24 || rows[0].CompletionTokens != 5 {
		t.Fatalf("unexpected exact aggregate: %+v", rows)
	}
	if state.EvidenceChargeUnits != 16 || state.Requests != 2 || state.EvidenceRows != 1 || state.ContentHash == "" {
		t.Fatalf("control state does not conserve evidence: %+v", state)
	}
	if rows[0].SourceRef == "" || rows[0].HMACKeyID != "cost-source-v1" || rows[0].ChargeUnit != channelCostChargeUnitNewAPIQuota {
		t.Fatalf("source contract lost: %+v", rows[0])
	}

	overflow := items[:1]
	overflow[0].QuotaExact = math.MaxInt64
	overflow = append(overflow, newAPIPricingUsageItem{CreatedAt: hour + 2, QuotaExact: 1, QuotaExactKnown: true, TokensExactKnown: true, Pricing: pricing})
	if _, _, err := buildNewAPICostHourEvidence(account, overflow, hour, hour+3600, []byte("0123456789abcdef0123456789abcdef"), "cost-source-v1"); err == nil {
		t.Fatal("int64 overflow must fail the whole hour")
	}
}

func TestBuildNewAPICostHourEvidenceKeepsFrozenHistoricalUnit(t *testing.T) {
	hour := int64(3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: 1_000_000}
	item := newAPIPricingUsageItem{
		CreatedAt: hour + 1, QuotaExact: 500_000, QuotaExactKnown: true,
		PromptTokens: 1, CompletionTokens: 1, TokensExactKnown: true,
		Pricing: newAPIPricingAttributes{TokenID: 75, GroupName: "Gpt-codex", ModelName: "gpt-5.5", BillingMode: "token"},
	}
	rows, state, err := buildNewAPICostHourEvidenceWithUnit(account, []newAPIPricingUsageItem{item}, hour, hour+3600, []byte("0123456789abcdef0123456789abcdef"), "key-v1", "500000")
	if err != nil {
		t.Fatal(err)
	}
	if state.ChargeUnitsPerUSD != "500000" || len(rows) != 1 || rows[0].ChargeUnitsPerUSD != "500000" {
		t.Fatalf("current account unit rewrote frozen historical unit: state=%+v rows=%+v", state, rows)
	}
	if _, _, err := buildNewAPICostHourEvidenceWithUnit(account, []newAPIPricingUsageItem{item}, hour, hour+3600, []byte("0123456789abcdef0123456789abcdef"), "key-v1", "2/4"); err == nil {
		t.Fatal("non-canonical frozen unit must fail closed")
	}
}

func TestCostSourceMappingIntervalsFailClosed(t *testing.T) {
	db := newChannelCostTestStore(t)
	m := &Monitor{storeDB: db}
	epoch, source := strings.Repeat("a", 64), strings.Repeat("b", 64)
	base := ChannelCostSourceBinding{
		Domain: "4sapi.com", AccountEpoch: epoch, SourceRef: source,
		Provider: upstreamProviderNewAPI, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "v1",
		LocalChannelID: 59, ValidFrom: 3600, ValidTo: 7200,
		Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual", CreatedBy: "root", CreatedAt: 1,
	}
	if err := m.saveCostSourceBinding(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	touching := base
	touching.ValidFrom, touching.ValidTo, touching.LocalChannelID = 7200, 10800, 60
	if err := m.saveCostSourceBinding(context.Background(), touching); err != nil {
		t.Fatalf("touching half-open interval should be accepted: %v", err)
	}
	overlap := base
	overlap.ValidFrom, overlap.ValidTo, overlap.LocalChannelID = 3600, 10800, 61
	if err := m.saveCostSourceBinding(context.Background(), overlap); err == nil {
		t.Fatal("overlapping confirmed mapping must fail closed")
	}
	if _, err := m.costSourceBindingAt(context.Background(), base.Domain, epoch, source, 3599); err == nil {
		t.Fatal("gap must remain unattributable")
	}
	atBoundary, err := m.costSourceBindingAt(context.Background(), base.Domain, epoch, source, 7200)
	if err != nil || atBoundary.LocalChannelID != 60 {
		t.Fatalf("half-open boundary selected wrong version: %+v err=%v", atBoundary, err)
	}
	invalid := base
	invalid.ValidFrom = 3601
	if err := m.saveCostSourceBinding(context.Background(), invalid); err == nil {
		t.Fatal("non-hour-aligned mapping must be rejected")
	}
	shared := base
	shared.SourceRef, shared.ValidFrom, shared.ValidTo = strings.Repeat("c", 64), 3600, 0
	shared.AllocationMode, shared.LocalChannelID = "shared", 0
	if err := m.saveCostSourceBinding(context.Background(), shared); err != nil {
		t.Fatalf("explicit shared source should remain stored but unallocated: %v", err)
	}
}

func TestCostSourceMappingAtomicSwitchAndConcurrentStaleWriter(t *testing.T) {
	db := newChannelCostTestStore(t)
	m := &Monitor{storeDB: db}
	base := ChannelCostSourceBinding{Domain: "4sapi.com", AccountEpoch: strings.Repeat("a", 64), SourceRef: strings.Repeat("b", 64), Provider: upstreamProviderNewAPI, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "v1", LocalChannelID: 59, ValidFrom: 3600, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual"}
	zero := int64(0)
	if err := m.replaceCostSourceBinding(context.Background(), base, &zero, ""); err != nil {
		t.Fatal(err)
	}
	// GORM may populate CreatedAt during Create. Reload the persisted row before
	// calculating the content CAS token so this test exercises concurrent
	// writers rather than an intentionally stale pre-insert value.
	if err := db.Where("domain = ? AND account_epoch = ? AND source_ref = ? AND valid_to = 0", base.Domain, base.AccountEpoch, base.SourceRef).First(&base).Error; err != nil {
		t.Fatal(err)
	}
	expected := base.ValidFrom
	first := base
	first.ValidFrom, first.LocalChannelID = 7200, 60
	second := base
	second.ValidFrom, second.LocalChannelID = 7200, 61
	errs := make(chan error, 2)
	baseSignature := channelCostBindingSignature(base)
	go func() { errs <- m.replaceCostSourceBinding(context.Background(), first, &expected, baseSignature) }()
	go func() { errs <- m.replaceCostSourceBinding(context.Background(), second, &expected, baseSignature) }()
	accepted := 0
	for i := 0; i < 2; i++ {
		if err := <-errs; err == nil {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("concurrent switch accepted=%d, want exactly one", accepted)
	}
	var rows []ChannelCostSourceBinding
	if err := db.Order("valid_from").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ValidTo != 7200 || rows[1].ValidFrom != 7200 || rows[1].ValidTo != 0 {
		t.Fatalf("atomic half-open history invalid: %+v", rows)
	}
	stale := base
	stale.ValidFrom, stale.LocalChannelID = 10800, 62
	if err := m.replaceCostSourceBinding(context.Background(), stale, &expected, baseSignature); err == nil {
		t.Fatal("stale expected version must be rejected")
	}
	var count int64
	if err := db.Model(&ChannelCostSourceBinding{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("stale write changed history: count=%d err=%v", count, err)
	}
}

func TestCostSourceMappingFutureHourCanBeCorrectedWithContentCAS(t *testing.T) {
	db := newChannelCostTestStore(t)
	m := &Monitor{storeDB: db}
	future := nextWholeHour(time.Now().Unix())
	first := ChannelCostSourceBinding{Domain: "4sapi.com", AccountEpoch: strings.Repeat("a", 64), SourceRef: strings.Repeat("b", 64), Provider: upstreamProviderNewAPI, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "v1", LocalChannelID: 59, ValidFrom: future, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual", Reason: "first", CreatedBy: "root", CreatedAt: time.Now().Unix()}
	zero := int64(0)
	if err := m.replaceCostSourceBinding(context.Background(), first, &zero, ""); err != nil {
		t.Fatal(err)
	}
	corrected := first
	corrected.LocalChannelID, corrected.Reason = 60, "corrected before effective hour"
	expected := future
	if err := m.replaceCostSourceBinding(context.Background(), corrected, &expected, channelCostBindingSignature(first)); err != nil {
		t.Fatalf("future mapping correction failed: %v", err)
	}
	var current ChannelCostSourceBinding
	if err := db.First(&current).Error; err != nil {
		t.Fatal(err)
	}
	if current.LocalChannelID != 60 || current.ValidFrom != future {
		t.Fatalf("future mapping was not replaced exactly: %+v", current)
	}
	stale := corrected
	stale.LocalChannelID = 61
	if err := m.replaceCostSourceBinding(context.Background(), stale, &expected, channelCostBindingSignature(first)); err == nil {
		t.Fatal("stale pre-correction signature overwrote the corrected mapping")
	}
}

func TestChannelCostModelsDoNotAlterLegacyUsagePrimaryKey(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamUsageHour{}); err != nil {
		t.Fatal(err)
	}
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(&ChannelUpstreamUsageHour{}); err != nil {
		t.Fatal(err)
	}
	var primary []string
	for _, field := range stmt.Schema.PrimaryFields {
		primary = append(primary, field.DBName)
	}
	if strings.Join(primary, ",") != "domain,hour_ts" {
		t.Fatalf("legacy usage primary key changed: %v", primary)
	}
}

func TestChannelCostFoundationMigrationIsAdditiveAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	storePath, factsPath := filepath.Join(dir, "monitor.db"), filepath.Join(dir, "usage-facts.db")
	legacyDB, err := gorm.Open(sqlite.Open(storePath), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyDB.AutoMigrate(&ChannelUpstreamUsageHour{}); err != nil {
		t.Fatal(err)
	}
	legacy := ChannelUpstreamUsageHour{Domain: "legacy.example", HourTs: 3600, Requests: 7, Tokens: 9, Quota: 11, Provider: upstreamProviderNewAPI}
	if err := legacyDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	if sqlDB, err := legacyDB.DB(); err == nil {
		_ = sqlDB.Close()
	}

	open := func() {
		m := &Monitor{cfg: Settings{StorePath: storePath, UsageFactsStorePath: factsPath, StoreBackupEnabled: false, StoreMigrationBackupRetention: 3}}
		if err := m.openStore(storePath); err != nil {
			t.Fatal(err)
		}
		for _, model := range []any{
			&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}, &ChannelCostPageCheckpoint{},
			&ChannelCostSourceBinding{}, &ChannelCostDirtyHour{}, &ChannelCostKeyRegistry{},
			&ChannelPricingChangeProposal{}, &ChannelPricingProposalEvent{},
			&ChannelFinanceActivation{}, &ChannelFinanceActivationSlot{}, &ChannelFinanceActivationEvent{},
			&ChannelEconomicsHourPublication{}, &ChannelEconomicsHourCurrent{}, &ChannelEconomicsHourManifestPublication{}, &ChannelEconomicsHourManifestCurrent{}, &ChannelEconomicsGlobalHourFact{}, &ChannelEconomicsDirtyHour{},
		} {
			if !m.storeDB.Migrator().HasTable(model) {
				t.Fatalf("missing additive cost table for %T", model)
			}
		}
		var got ChannelUpstreamUsageHour
		if err := m.storeDB.First(&got, "domain = ? AND hour_ts = ?", legacy.Domain, legacy.HourTs).Error; err != nil || got.Requests != legacy.Requests || got.Quota != legacy.Quota {
			t.Fatalf("legacy usage changed after migration: %+v err=%v", got, err)
		}
		m.Close()
	}
	open()
	open()
}

func TestChannelCostCheckpointAndPublicationAreIndependent(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamPricingPageCheckpoint{}, &ChannelUpstreamPricingHourEvidence{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{
		ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"},
		ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "cost-source-v1",
	}}
	hour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 147426, Account: "billing@example.com"}
	item := newAPIPricingUsageItem{
		CreatedAt: hour + 1, QuotaExact: 16, QuotaExactKnown: true, PromptTokens: 24, CompletionTokens: 5, TokensExactKnown: true,
		Pricing: newAPIPricingAttributes{GroupName: "Gpt-codex", ModelName: "gpt-5.5", TokenID: 75, BillingMode: "token", GroupRatioState: pricingRatioValid, GroupRatioCanonical: "1", EffectiveRatioSource: "group_ratio", EffectiveRatio: "1"},
	}
	pricingRows, _, err := buildNewAPIPricingHour(account, []newAPIPricingUsageItem{item}, hour, hour+3600)
	if err != nil {
		t.Fatal(err)
	}
	costRows, _, err := buildNewAPICostHourEvidence(account, []newAPIPricingUsageItem{item}, hour, hour+3600, []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID)
	if err != nil {
		t.Fatal(err)
	}
	pricingMap := map[string]*ChannelUpstreamPricingHourEvidence{pricingRows[0].DimensionHash: &pricingRows[0]}
	costMap := map[string]*ChannelUpstreamCostHourEvidence{costRows[0].DimensionHash: &costRows[0]}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion,
		HourTs: hour, Provider: account.Provider, WindowSeconds: 3600, NextPage: 2, Total: 1, SourceRows: 1,
		FirstPageFingerprint: strings.Repeat("f", 64),
	}
	if err := m.saveNewAPIPricingAndCostCheckpoint(context.Background(), &checkpoint, pricingMap, costMap); err != nil {
		t.Fatal(err)
	}
	pricingState := ChannelUpstreamPricingHourState{
		Domain: account.Domain, AccountEpoch: checkpoint.AccountEpoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion,
		Status: "verified", ReconcileStatus: "matched", FinalQuota: 16,
	}
	if err := m.publishChannelCostHourFromCheckpoint(context.Background(), account, pricingState, hour+3600); err != nil {
		t.Fatal(err)
	}
	var state ChannelUpstreamCostHourState
	if err := db.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "observed" || state.VerifiedScans != 1 {
		t.Fatalf("first cost scan must remain observed: %+v", state)
	}
	// Cost evidence must independently repeat unchanged before it is trusted.
	if err := m.saveNewAPIPricingAndCostCheckpoint(context.Background(), &checkpoint, pricingMap, costMap); err != nil {
		t.Fatal(err)
	}
	if err := m.publishChannelCostHourFromCheckpoint(context.Background(), account, pricingState, hour+3660); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "verified" || state.VerifiedScans != 2 || state.ReconcileStatus != "matched" || state.ControlChargeUnits != 16 || state.EvidenceChargeUnits != 16 {
		t.Fatalf("unexpected published state: %+v", state)
	}
	var published []ChannelUpstreamCostHourEvidence
	if err := db.Find(&published).Error; err != nil || len(published) != 1 {
		t.Fatalf("published evidence=%+v err=%v", published, err)
	}
	var costCheckpoints int64
	if err := db.Model(&ChannelCostPageCheckpoint{}).Count(&costCheckpoints).Error; err != nil || costCheckpoints != 0 {
		t.Fatalf("completed cost checkpoint must be removed: count=%d err=%v", costCheckpoints, err)
	}
	var pricingCheckpoints int64
	if err := db.Model(&ChannelUpstreamPricingPageCheckpoint{}).Count(&pricingCheckpoints).Error; err != nil || pricingCheckpoints != 1 {
		t.Fatalf("cost publication must not delete old pricing checkpoint: count=%d err=%v", pricingCheckpoints, err)
	}
}

func TestChannelCostDisabledWritesNoCheckpoint(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamPricingPageCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{
		Domain: "4sapi.com", AccountEpoch: strings.Repeat("a", 64), SemanticsVersion: upstreamPricingSemanticsVersion,
		HourTs: 3600, Provider: upstreamProviderNewAPI, WindowSeconds: 3600, NextPage: 2,
	}
	if err := m.saveNewAPIPricingAndCostCheckpoint(context.Background(), &checkpoint, map[string]*ChannelUpstreamPricingHourEvidence{}, map[string]*ChannelUpstreamCostHourEvidence{}); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&ChannelCostPageCheckpoint{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("disabled cost closure wrote business checkpoint: count=%d err=%v", count, err)
	}
}

func TestChannelCostCheckpointFailureFallsBackToPricingCheckpoint(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamPricingPageCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}}}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion, HourTs: 3600, Provider: account.Provider, WindowSeconds: 3600, NextPage: 2}
	if err := db.Migrator().DropTable(&ChannelCostPageCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	if err := m.saveNewAPIPricingCheckpointWithCostFallback(context.Background(), account, &checkpoint, map[string]*ChannelUpstreamPricingHourEvidence{}, map[string]*ChannelUpstreamCostHourEvidence{}); err != nil {
		t.Fatalf("cost-only storage failure must not block pricing checkpoint: %v", err)
	}
	var pricingCount, dirtyCount int64
	if err := db.Model(&ChannelUpstreamPricingPageCheckpoint{}).Count(&pricingCount).Error; err != nil || pricingCount != 1 {
		t.Fatalf("pricing fallback checkpoint count=%d err=%v", pricingCount, err)
	}
	if err := db.Model(&ChannelCostDirtyHour{}).Count(&dirtyCount).Error; err != nil || dirtyCount != 1 {
		t.Fatalf("cost failure was not durably queued: count=%d err=%v", dirtyCount, err)
	}
}

func TestDecodeChannelCostCheckpointRejectsTamperingAndWrongKeyID(t *testing.T) {
	hour := int64(3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1}
	item := newAPIPricingUsageItem{CreatedAt: hour + 1, QuotaExact: 10, QuotaExactKnown: true, TokensExactKnown: true, Pricing: newAPIPricingAttributes{TokenID: 7, GroupName: "g", ModelName: "m", BillingMode: "token", GroupRatioState: pricingRatioValid, GroupRatioCanonical: "1", EffectiveRatioSource: "group_ratio", EffectiveRatio: "1"}}
	rows, _, err := buildNewAPICostHourEvidence(account, []newAPIPricingUsageItem{item}, hour, hour+3600, []byte("0123456789abcdef0123456789abcdef"), "key-v1")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := ChannelCostPageCheckpoint{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: channelCostEvidenceSemanticsVersion, HourTs: hour, NextPage: 2, Total: 1, SourceRows: 1, AggregatesJSON: string(encoded)}
	if _, err := decodeChannelCostCheckpoint(checkpoint, upstreamProviderNewAPI, "key-v1"); err != nil {
		t.Fatalf("valid checkpoint rejected: %v", err)
	}
	if _, err := decodeChannelCostCheckpoint(checkpoint, upstreamProviderNewAPI, "key-v2"); err == nil {
		t.Fatal("checkpoint signed under another key ID must be rejected")
	}
	rows[0].ChargeUnits++
	tampered, _ := json.Marshal(rows)
	checkpoint.AggregatesJSON = string(tampered)
	if _, err := decodeChannelCostCheckpoint(checkpoint, upstreamProviderNewAPI, "key-v1"); err == nil {
		t.Fatal("tampered amount with stale content hash must be rejected")
	}
}

func TestChannelCostMissingVerifiedHourQueuesAndRecoversAcrossRestart(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamPricingHourState{}, &ChannelUpstreamPricingPageCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	cfg := Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}, ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "key-v1"}
	m1 := &Monitor{storeDB: db, cfg: cfg}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1}
	hour := int64(3600)
	epoch := newAPIUpstreamAccountEpoch(account)
	pricingState := ChannelUpstreamPricingHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion, Status: "verified", ReconcileStatus: "matched", FinalQuota: 10}
	if err := db.Create(&pricingState).Error; err != nil {
		t.Fatal(err)
	}
	if err := m1.enqueueMissingChannelCostHours(context.Background(), account, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := m1.nextChannelCostDirtyHour(context.Background(), account, time.Now().Unix()); err != nil {
		t.Fatalf("missing cost hour was not queued: %v", err)
	}
	item := newAPIPricingUsageItem{CreatedAt: hour + 1, QuotaExact: 10, QuotaExactKnown: true, TokensExactKnown: true, Pricing: newAPIPricingAttributes{TokenID: 7, GroupName: "g", ModelName: "m", BillingMode: "token", GroupRatioState: pricingRatioValid, GroupRatioCanonical: "1", EffectiveRatioSource: "group_ratio", EffectiveRatio: "1"}}
	costRows, _, err := buildNewAPICostHourEvidence(account, []newAPIPricingUsageItem{item}, hour, hour+3600, []byte(cfg.ChannelCostHMACKey), cfg.ChannelCostHMACKeyID)
	if err != nil {
		t.Fatal(err)
	}
	writeCheckpoint := func() {
		t.Helper()
		encoded, marshalErr := json.Marshal(costRows)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		row := ChannelCostPageCheckpoint{Domain: account.Domain, AccountEpoch: epoch, SemanticsVersion: channelCostEvidenceSemanticsVersion, HourTs: hour, NextPage: 2, Total: 1, SourceRows: 1, AggregatesJSON: string(encoded)}
		if createErr := db.Save(&row).Error; createErr != nil {
			t.Fatal(createErr)
		}
	}
	writeCheckpoint()
	if err := m1.publishChannelCostHourFromCheckpoint(context.Background(), account, pricingState, hour+3600); err != nil {
		t.Fatal(err)
	}
	var first ChannelUpstreamCostHourState
	if err := db.First(&first).Error; err != nil || first.Status != "observed" {
		t.Fatalf("first recovery scan state=%+v err=%v", first, err)
	}
	// A new Monitor instance represents process restart. The dirty task and the
	// first observed scan remain durable; an identical second scan verifies it.
	m2 := &Monitor{storeDB: db, cfg: cfg}
	if _, err := m2.nextChannelCostDirtyHour(context.Background(), account, time.Now().Unix()); err != nil {
		t.Fatalf("dirty task did not survive restart: %v", err)
	}
	writeCheckpoint()
	if err := m2.publishChannelCostHourFromCheckpoint(context.Background(), account, pricingState, hour+7200); err != nil {
		t.Fatal(err)
	}
	var final ChannelUpstreamCostHourState
	if err := db.First(&final).Error; err != nil || final.Status != "verified" || final.VerifiedScans != 2 {
		t.Fatalf("cost hour not verified after restart recovery: %+v err=%v", final, err)
	}
	var evidenceCount, dirtyCount int64
	if err := db.Model(&ChannelUpstreamCostHourEvidence{}).Count(&evidenceCount).Error; err != nil || evidenceCount != 1 {
		t.Fatalf("recovery was not exactly-once: rows=%d err=%v", evidenceCount, err)
	}
	if err := db.Model(&ChannelCostDirtyHour{}).Count(&dirtyCount).Error; err != nil || dirtyCount != 0 {
		t.Fatalf("verified recovery left dirty rows=%d err=%v", dirtyCount, err)
	}
}

func TestChannelCostVerificationResetsWhenSourceAllocationChanges(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamPricingPageCheckpoint{}, &ChannelUpstreamPricingHourEvidence{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{
		ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"},
		ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "cost-source-v1",
	}}
	hour := int64(7200)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 147426, Account: "billing@example.com"}
	pricing := newAPIPricingAttributes{GroupName: "Gpt-codex", ModelName: "gpt-5.5", BillingMode: "token", GroupRatioState: pricingRatioValid, GroupRatioCanonical: "1", EffectiveRatioSource: "group_ratio", EffectiveRatio: "1"}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion, HourTs: hour, Provider: account.Provider, WindowSeconds: 3600, NextPage: 2, Total: 1, SourceRows: 1, FirstPageFingerprint: strings.Repeat("f", 64)}
	pricingItem := newAPIPricingUsageItem{CreatedAt: hour + 1, QuotaExact: 16, QuotaExactKnown: true, TokensExactKnown: true, Pricing: pricing}
	pricingRows, _, err := buildNewAPIPricingHour(account, []newAPIPricingUsageItem{pricingItem}, hour, hour+3600)
	if err != nil {
		t.Fatal(err)
	}
	pricingMap := map[string]*ChannelUpstreamPricingHourEvidence{pricingRows[0].DimensionHash: &pricingRows[0]}
	pricingState := ChannelUpstreamPricingHourState{Domain: account.Domain, AccountEpoch: checkpoint.AccountEpoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion, Status: "verified", ReconcileStatus: "matched", FinalQuota: 16}
	publishToken := func(tokenID int64) ChannelUpstreamCostHourState {
		t.Helper()
		item := pricingItem
		item.Pricing.TokenID = tokenID
		rows, _, buildErr := buildNewAPICostHourEvidence(account, []newAPIPricingUsageItem{item}, hour, hour+3600, []byte(m.cfg.ChannelCostHMACKey), m.cfg.ChannelCostHMACKeyID)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		costMap := map[string]*ChannelUpstreamCostHourEvidence{rows[0].DimensionHash: &rows[0]}
		if saveErr := m.saveNewAPIPricingAndCostCheckpoint(context.Background(), &checkpoint, pricingMap, costMap); saveErr != nil {
			t.Fatal(saveErr)
		}
		if publishErr := m.publishChannelCostHourFromCheckpoint(context.Background(), account, pricingState, hour+3600); publishErr != nil {
			t.Fatal(publishErr)
		}
		var got ChannelUpstreamCostHourState
		if loadErr := db.First(&got).Error; loadErr != nil {
			t.Fatal(loadErr)
		}
		return got
	}
	first := publishToken(75)
	changed := publishToken(76)
	if first.ContentHash == changed.ContentHash || changed.Status != "observed" || changed.VerifiedScans != 1 {
		t.Fatalf("same total with changed source allocation must reset verification: first=%+v changed=%+v", first, changed)
	}
	stable := publishToken(76)
	if stable.Status != "verified" || stable.VerifiedScans != 2 {
		t.Fatalf("unchanged repeated source allocation should verify: %+v", stable)
	}
}

func TestPricingProposalRequiresTwoVerifiedHoursAndNeverAutoApplies(t *testing.T) {
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{}, &ChannelFinanceChannelCost{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 147426, Account: "billing@example.com"}
	epoch := newAPIUpstreamAccountEpoch(account)
	sourceRef, err := channelCostSourceRef([]byte("0123456789abcdef0123456789abcdef"), account.Provider, epoch, channelCostSourceKindNewAPIToken, "75")
	if err != nil {
		t.Fatal(err)
	}
	firstHour := int64(3600)
	for _, hour := range []int64{firstHour, firstHour + 3600} {
		costState := ChannelUpstreamCostHourState{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			Provider: account.Provider, Status: "verified", ReconcileStatus: "matched", Requests: 10,
		}
		if err := db.Create(&costState).Error; err != nil {
			t.Fatal(err)
		}
		costRow := ChannelUpstreamCostHourEvidence{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			SourceRef: sourceRef, DimensionHash: strings.Repeat(string(rune('a'+hour/3600)), 64), Provider: account.Provider,
			SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "v1", SourceGroup: "Gpt-codex", UpstreamModel: "gpt-5.5",
			ChargeUnits: 100, ChargeUnit: channelCostChargeUnitNewAPIQuota, Requests: 10,
		}
		if err := db.Create(&costRow).Error; err != nil {
			t.Fatal(err)
		}
		pricingState := ChannelUpstreamPricingHourState{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion,
			Status: "verified", ReconcileStatus: "matched",
		}
		if err := db.Create(&pricingState).Error; err != nil {
			t.Fatal(err)
		}
		pricingRow := ChannelUpstreamPricingHourEvidence{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion,
			DimensionHash: strings.Repeat(string(rune('c'+hour/3600)), 64), Provider: account.Provider,
			SourceGroup: "Gpt-codex", ModelName: "gpt-5.5", OtherValid: true,
			EvidenceCapability: "full_rate", EffectiveRatioSource: "group_ratio", EffectiveRatio: "1.1", EligibleRequests: 10,
		}
		if err := db.Create(&pricingRow).Error; err != nil {
			t.Fatal(err)
		}
	}
	binding := ChannelCostSourceBinding{
		Domain: account.Domain, AccountEpoch: epoch, SourceRef: sourceRef, ValidFrom: firstHour,
		Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "v1",
		LocalChannelID: 59, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual",
	}
	if err := m.saveCostSourceBinding(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceChannelCost{ChannelID: 59, Grp: "codex-1.2x", UpstreamGroupName: "Gpt-codex", Multiplier: 1, DiscountFactor: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceVersion{Domain: account.Domain, Version: 3, SnapshotJSON: `{}`, EffectiveAt: 1, CreatedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.detectNewAPIChannelPricingProposals(context.Background(), account, firstHour+3600, 9999); err != nil {
		t.Fatal(err)
	}
	var proposal ChannelPricingChangeProposal
	if err := db.First(&proposal).Error; err != nil {
		t.Fatal(err)
	}
	if proposal.Status != "pending" || proposal.OldValue != "1" || proposal.NewValue != "11/10" || proposal.BaseVersion != 3 || proposal.EvidenceRequests != 20 {
		t.Fatalf("unexpected proposal: %+v", proposal)
	}
	var current ChannelFinanceChannelCost
	if err := db.First(&current, "channel_id = ?", 59).Error; err != nil {
		t.Fatal(err)
	}
	if current.Multiplier != 1 || current.DiscountFactor != 1 {
		t.Fatalf("advisory proposal changed manual finance config: %+v", current)
	}
	var events int64
	if err := db.Model(&ChannelPricingProposalEvent{}).Count(&events).Error; err != nil || events != 1 {
		t.Fatalf("proposal audit events=%d err=%v", events, err)
	}
}
