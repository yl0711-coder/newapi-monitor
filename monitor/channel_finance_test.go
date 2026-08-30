package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func financeFloatPtr(v float64) *float64 { return &v }

func TestChannelGroupFinanceFormulaAndIncompleteState(t *testing.T) {
	snapshot := channelFinanceSnapshot{
		settings:        ChannelFinanceSetting{FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1},
		hasSettings:     true,
		siteGroups:      map[string]ChannelSaleGroupRate{"codex-1.3x": {Grp: "codex-1.3x", Multiplier: 1.3}},
		domainCosts:     map[string]ChannelDomainCost{"last-api.ai": {Domain: "last-api.ai", RechargePaid: 1, RechargeCredit: 2}},
		domainGroupCost: map[string]map[string]ChannelDomainGroupCost{"last-api.ai": {"codex-1.3x": {Domain: "last-api.ai", Grp: "codex-1.3x", Multiplier: 2, DiscountFactor: 0.8}}},
	}
	got := snapshot.groupView("last-api.ai", "codex-1.3x")
	if !got.Complete {
		t.Fatalf("finance should be complete: %+v", got)
	}
	if math.Abs(got.SiteDiscount-1.3/7.0) > 1e-12 {
		t.Fatalf("site discount=%v want %v", got.SiteDiscount, 1.3/7.0)
	}
	if math.Abs(got.UpstreamDiscount-0.8/7.0) > 1e-12 {
		t.Fatalf("upstream discount=%v want %v", got.UpstreamDiscount, 0.8/7.0)
	}
	if math.Abs(got.EstimatedMargin-(1-0.8/1.3)) > 1e-12 {
		t.Fatalf("margin=%v want %v", got.EstimatedMargin, 1-0.8/1.3)
	}
	if math.Abs(got.UpstreamEffectiveMultiplier-0.8) > 1e-12 || math.Abs(got.MultiplierGap-0.5) > 1e-12 {
		t.Fatalf("effective multiplier/gap incorrect: %+v", got)
	}

	missing := snapshot.groupView("last-api.ai", "unconfigured")
	if missing.Complete || missing.SiteDiscount != 0 || missing.UpstreamDiscount != 0 || missing.EstimatedMargin != 0 {
		t.Fatalf("incomplete finance must not expose calculated values: %+v", missing)
	}

	multiplierOnly := snapshot
	multiplierOnly.hasSettings = false
	got = multiplierOnly.groupView("last-api.ai", "codex-1.3x")
	if got.Complete || math.Abs(got.MultiplierGap-0.5) > 1e-12 {
		t.Fatalf("multiplier gap must remain available before discount inputs are complete: %+v", got)
	}
}

func TestChannelMultiplierGapUsesExactWebsiteGroup(t *testing.T) {
	const upstreamEffective = 2.0 / 7.0
	snapshot := channelFinanceSnapshot{
		siteGroups: map[string]ChannelSaleGroupRate{
			"codex-0.7x": {Grp: "codex-0.7x", Multiplier: .7},
			"codex-1.2x": {Grp: "codex-1.2x", Multiplier: 1.2},
		},
		domainCosts: map[string]ChannelDomainCost{
			"codeyu.shop": {Domain: "codeyu.shop", RechargePaid: 1, RechargeCredit: 7},
		},
		channelGroupCost: map[int]map[string]ChannelFinanceChannelCost{34: {
			"codex-0.7x": {ChannelID: 34, Grp: "codex-0.7x", Multiplier: 2, DiscountFactor: 1},
			"codex-1.2x": {ChannelID: 34, Grp: "codex-1.2x", Multiplier: 2, DiscountFactor: 1},
		}},
	}
	low := snapshot.groupViewForChannel("codeyu.shop", 34, "codex-0.7x")
	high := snapshot.groupViewForChannel("codeyu.shop", 34, "codex-1.2x")
	if !low.SiteConfigured || !high.SiteConfigured || math.Abs(low.UpstreamEffectiveMultiplier-upstreamEffective) > 1e-12 || math.Abs(high.UpstreamEffectiveMultiplier-upstreamEffective) > 1e-12 {
		t.Fatalf("channel cost comparison incomplete: low=%+v high=%+v", low, high)
	}
	if math.Abs(low.MultiplierGap-(.7-upstreamEffective)) > 1e-12 || math.Abs(high.MultiplierGap-(1.2-upstreamEffective)) > 1e-12 {
		t.Fatalf("multiplier gap must use the exact website group: low=%+v high=%+v", low, high)
	}
}

func TestChannelGroupFinanceAllowsNegativeMargin(t *testing.T) {
	snapshot := channelFinanceSnapshot{
		settings:        ChannelFinanceSetting{FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1},
		hasSettings:     true,
		siteGroups:      map[string]ChannelSaleGroupRate{"g": {Grp: "g", Multiplier: 1}},
		domainCosts:     map[string]ChannelDomainCost{"loss.example": {Domain: "loss.example", RechargePaid: 1, RechargeCredit: 1}},
		domainGroupCost: map[string]map[string]ChannelDomainGroupCost{"loss.example": {"g": {Domain: "loss.example", Grp: "g", Multiplier: 1.2}}},
	}
	got := snapshot.groupView("loss.example", "g")
	if !got.Complete || math.Abs(got.EstimatedMargin-(-0.2)) > 1e-12 {
		t.Fatalf("negative margin must be preserved, got %+v", got)
	}
	if math.Abs(got.MultiplierGap-(-0.2)) > 1e-12 {
		t.Fatalf("negative multiplier gap must be preserved, got %+v", got)
	}
}

func TestChannelFinanceUsesConsistentHistoricalChannelRateForMissingGroup(t *testing.T) {
	first := ChannelFinanceChannelCost{ChannelID: 33, Grp: "codex-1.2x", UpstreamGroupName: "upstream-codex", Multiplier: 1, DiscountFactor: .8}
	snapshot := channelFinanceSnapshot{
		siteGroups: map[string]ChannelSaleGroupRate{
			"codex-1.2x": {Grp: "codex-1.2x", Multiplier: 1.2},
			"internal":   {Grp: "internal", Multiplier: 1},
		},
		domainCosts: map[string]ChannelDomainCost{"last-api.ai": {Domain: "last-api.ai", RechargePaid: 1, RechargeCredit: 1}},
		channelGroupCost: map[int]map[string]ChannelFinanceChannelCost{33: {
			"codex-1.2x": first,
			"codex-1.4x": {ChannelID: 33, Grp: "codex-1.4x", UpstreamGroupName: "upstream-codex", Multiplier: 1, DiscountFactor: .8},
		}},
		channelCanonicalCost: map[int]ChannelFinanceChannelCost{33: first},
		channelCostConflict:  map[int]bool{},
	}
	got := snapshot.groupViewForChannel("last-api.ai", 33, "internal")
	if !got.UpstreamConfigured || got.UpstreamConflict || got.UpstreamGroupName != "upstream-codex" || math.Abs(got.UpstreamEffectiveMultiplier-.8) > 1e-12 || math.Abs(got.MultiplierGap-.2) > 1e-12 {
		t.Fatalf("consistent historical channel rate must safely cover missing group: %+v", got)
	}
}

func TestLoadChannelFinanceSnapshotBuildsHistoricalChannelFallback(t *testing.T) {
	m := newStabilityTestMonitor(t)
	rows := []ChannelFinanceChannelCost{
		{ChannelID: 33, Grp: "codex-1.2x", UpstreamGroupName: "upstream-codex", Multiplier: 1, DiscountFactor: .8},
		{ChannelID: 33, Grp: "codex-1.4x", UpstreamGroupName: "upstream-codex", Multiplier: 1, DiscountFactor: .8},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelDomainCost{Domain: "last-api.ai", RechargePaid: 1, RechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.loadChannelFinanceSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := snapshot.groupViewForChannel("last-api.ai", 33, "internal")
	if !got.UpstreamConfigured || got.UpstreamConflict || got.UpstreamGroupName != "upstream-codex" || math.Abs(got.UpstreamEffectiveMultiplier-.8) > 1e-12 {
		t.Fatalf("loaded snapshot did not construct safe channel fallback: %+v", got)
	}
}

func TestChannelFinanceDoesNotGuessWhenHistoricalChannelRatesConflict(t *testing.T) {
	first := ChannelFinanceChannelCost{ChannelID: 33, Grp: "codex-1.2x", UpstreamGroupName: "upstream-a", Multiplier: 1, DiscountFactor: .8}
	snapshot := channelFinanceSnapshot{
		siteGroups:  map[string]ChannelSaleGroupRate{"internal": {Grp: "internal", Multiplier: 1}},
		domainCosts: map[string]ChannelDomainCost{"last-api.ai": {Domain: "last-api.ai", RechargePaid: 1, RechargeCredit: 1}},
		channelGroupCost: map[int]map[string]ChannelFinanceChannelCost{33: {
			"codex-1.2x": first,
			"codex-1.4x": {ChannelID: 33, Grp: "codex-1.4x", UpstreamGroupName: "upstream-b", Multiplier: 1.1, DiscountFactor: .8},
		}},
		channelCanonicalCost: map[int]ChannelFinanceChannelCost{33: first},
		channelCostConflict:  map[int]bool{33: true},
	}
	got := snapshot.groupViewForChannel("last-api.ai", 33, "internal")
	if got.UpstreamConfigured || !got.UpstreamConflict || got.UpstreamEffectiveMultiplier != 0 {
		t.Fatalf("conflicting historical channel rates must not be guessed: %+v", got)
	}
}

func TestChannelRateConfiguredDoesNotDependOnRechargeRatio(t *testing.T) {
	// 倍率配置完成度与主域名的充值比例是两类独立维护项：前者已填写时，
	// 不能因为后者尚未填写而在渠道管理页误报“倍率待配置”。
	rate := ChannelFinanceChannelCost{ChannelID: 33, Grp: "codex-1.2x", UpstreamGroupName: "gpt-codex", Multiplier: 1, DiscountFactor: .8}
	snapshot := channelFinanceSnapshot{
		channelCanonicalCost: map[int]ChannelFinanceChannelCost{33: rate},
		channelCostConflict:  map[int]bool{},
	}
	if !snapshot.channelRateConfigured("example.com", 33) {
		t.Fatal("a complete physical-channel rate must not require recharge configuration")
	}
	// 旧版同一渠道记录有冲突时，不能把任意一项误标为已完成。
	snapshot.channelCostConflict[33] = true
	if snapshot.channelRateConfigured("example.com", 33) {
		t.Fatal("conflicting physical-channel rate must remain unconfigured")
	}
}

func TestValidateChannelFinanceInputRejectsPartialAndInvalidValues(t *testing.T) {
	valid := channelFinanceSaveInput{
		Domain: "last-api.ai", FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1,
		UpstreamRechargePaid: 1, UpstreamRechargeCredit: 1,
		Groups: []channelFinanceGroupInput{{Group: "codex", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1), UpstreamDiscountFactor: financeFloatPtr(0.8)}},
	}
	if err := validateChannelFinanceInput(&valid); err != nil {
		t.Fatalf("valid input: %v", err)
	}
	partial := valid
	partial.Groups = []channelFinanceGroupInput{{Group: "codex", SiteMultiplier: financeFloatPtr(1.3)}}
	if err := validateChannelFinanceInput(&partial); err == nil {
		t.Fatal("partial group rate must be rejected")
	}
	invalid := valid
	invalid.FXBenchmark = 0
	if err := validateChannelFinanceInput(&invalid); err == nil {
		t.Fatal("zero benchmark must be rejected")
	}
}

func TestMigrateLegacyChannelFinanceSnapshotsToPerDomainVersions(t *testing.T) {
	m := newStabilityTestMonitor(t)
	if err := m.storeDB.AutoMigrate(&ChannelFinanceAudit{}); err != nil {
		t.Fatal(err)
	}
	legacy := []ChannelFinanceAudit{
		{Domain: "a.example", SnapshotJSON: `{"domain":"a.example","groups":[]}`, CreatedAt: 100, UpdatedBy: "one"},
		{Domain: "a.example", SnapshotJSON: `{"domain":"a.example","groups":[]}`, CreatedAt: 200, UpdatedBy: "two"},
		{Domain: "b.example", SnapshotJSON: `{"domain":"b.example","groups":[]}`, CreatedAt: 150, UpdatedBy: "three"},
	}
	if err := m.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyChannelFinanceVersions(m.storeDB); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyChannelFinanceVersions(m.storeDB); err != nil {
		t.Fatal(err)
	}
	var versions []ChannelFinanceVersion
	if err := m.storeDB.Order("domain ASC, version ASC").Find(&versions).Error; err != nil {
		t.Fatal(err)
	}
	if len(versions) != 3 || versions[0].Version != 1 || versions[1].Version != 2 || versions[2].Version != 1 {
		t.Fatalf("unexpected migrated versions: %+v", versions)
	}
}

func TestNormalizeLegacyChannelFinanceVersionDefaultsDiscountFactor(t *testing.T) {
	raw := `{"domain":"legacy.example","fx_benchmark":7,"site_recharge_paid":1,"site_recharge_credit":1,"upstream_recharge_paid":1,"upstream_recharge_credit":1,"groups":[{"group":"codex","site_multiplier":1.3,"upstream_multiplier":0.8}]}`
	normalized, err := normalizeChannelFinanceVersionJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot channelFinanceVersionSnapshot
	if err := json.Unmarshal([]byte(normalized), &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Groups) != 1 || snapshot.Groups[0].UpstreamDiscountFactor != 1 {
		t.Fatalf("legacy factor must default to 1 without changing historical cost: %+v", snapshot.Groups)
	}
}

func TestMigrateLegacyChannelFinanceVersionsDoesNotSkipOtherDomains(t *testing.T) {
	m := newStabilityTestMonitor(t)
	if err := m.storeDB.AutoMigrate(&ChannelFinanceAudit{}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelFinanceVersion{Domain: "a.example", Version: 1, SnapshotJSON: `{"domain":"a.example","groups":[]}`, EffectiveAt: 100, CreatedAt: 100}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&[]ChannelFinanceAudit{
		{Domain: "a.example", SnapshotJSON: `{"domain":"a.example","groups":[]}`, CreatedAt: 100},
		{Domain: "b.example", SnapshotJSON: `{"domain":"b.example","groups":[]}`, CreatedAt: 200},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyChannelFinanceVersions(m.storeDB); err != nil {
		t.Fatal(err)
	}
	var rows []ChannelFinanceVersion
	if err := m.storeDB.Order("domain ASC, version ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Domain != "a.example" || rows[1].Domain != "b.example" || rows[1].Version != 1 {
		t.Fatalf("mixed migration skipped or duplicated a domain: %+v", rows)
	}
}

func TestChannelFinanceRouteCreatesImmutableVersionsAndRequiresConfirmation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.SessionSecret = "test-session-secret"
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Name: "last", BaseDomain: "last-api.ai", Groups: "codex-1.3x", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.POST("/channels/finance", m.requireRole(roleRoot), m.saveChannelFinanceHandler)
	payload := channelFinanceSaveInput{
		Domain: "last-api.ai", FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1,
		UpstreamRechargePaid: 0.8, UpstreamRechargeCredit: 1,
		Groups: []channelFinanceGroupInput{{Group: "codex-1.3x", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1), UpstreamDiscountFactor: financeFloatPtr(1)}},
	}
	request := func(role int, input channelFinanceSaveInput) *httptest.ResponseRecorder {
		body, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/channels/finance", bytes.NewReader(body))
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		token := m.signSession("tester", role, time.Now().Unix())
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	if w := request(roleAdmin, payload); w.Code != http.StatusForbidden {
		t.Fatalf("admin write status=%d body=%s", w.Code, w.Body.String())
	}
	if w := request(roleRoot, payload); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"version":1`) {
		t.Fatalf("root write status=%d body=%s", w.Code, w.Body.String())
	}

	snapshot, err := m.loadChannelFinanceSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := snapshot.groupView("last-api.ai", "codex-1.3x")
	if !got.Complete || got.UpstreamDiscountFactor != 1 || math.Abs(got.UpstreamEffectiveMultiplier-0.8) > 1e-12 || math.Abs(got.EstimatedMargin-(1-0.8/1.3)) > 1e-12 {
		t.Fatalf("stored finance=%+v", got)
	}
	if view := snapshot.domainView("last-api.ai"); view.Version != 1 || view.EffectiveAt <= 0 {
		t.Fatalf("domain version=%+v", view)
	}

	if w := request(roleRoot, payload); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"unchanged":true`) {
		t.Fatalf("unchanged write status=%d body=%s", w.Code, w.Body.String())
	}
	var versions int64
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Count(&versions).Error; err != nil || versions != 1 {
		t.Fatalf("unchanged save must not create a version: count=%d err=%v", versions, err)
	}

	updated := payload
	updated.Groups = []channelFinanceGroupInput{{Group: "codex-1.3x", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1.1), UpstreamDiscountFactor: financeFloatPtr(1)}}
	if w := request(roleRoot, updated); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"confirmation_required":true`) {
		t.Fatalf("update without confirmation status=%d body=%s", w.Code, w.Body.String())
	}
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Count(&versions).Error; err != nil || versions != 1 {
		t.Fatalf("unconfirmed update must not create a version: count=%d err=%v", versions, err)
	}

	updated.ConfirmUpdate, updated.ExpectedVersion = true, 1
	if w := request(roleRoot, updated); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"version":2`) {
		t.Fatalf("confirmed update status=%d body=%s", w.Code, w.Body.String())
	}
	var rows []ChannelFinanceVersion
	if err := m.storeDB.Order("version ASC").Find(&rows).Error; err != nil || len(rows) != 2 {
		t.Fatalf("version rows=%d err=%v", len(rows), err)
	}
	var first, second channelFinanceVersionSnapshot
	if err := json.Unmarshal([]byte(rows[0].SnapshotJSON), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(rows[1].SnapshotJSON), &second); err != nil {
		t.Fatal(err)
	}
	if rows[0].Version != 1 || rows[1].Version != 2 || first.Groups[0].UpstreamMultiplier != 1 || second.Groups[0].UpstreamMultiplier != 1.1 {
		t.Fatalf("versions were not preserved: rows=%+v first=%+v second=%+v", rows, first, second)
	}

	stale := updated
	stale.Groups = []channelFinanceGroupInput{{Group: "codex-1.3x", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1.2), UpstreamDiscountFactor: financeFloatPtr(1)}}
	stale.ExpectedVersion = 1
	if w := request(roleRoot, stale); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"version_conflict":true`) {
		t.Fatalf("stale update status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestGlobalFinanceChangeVersionsEveryAffectedDomain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.SessionSecret = "test-session-secret"
	if err := m.storeDB.Create(&[]ChannelSnap{
		{ID: 1, Name: "a", BaseDomain: "a.example", Groups: "codex", Status: 1},
		{ID: 2, Name: "b", BaseDomain: "b.example", Groups: "codex", Status: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.POST("/channels/finance", m.requireRole(roleRoot), m.saveChannelFinanceHandler)
	request := func(input channelFinanceSaveInput) *httptest.ResponseRecorder {
		body, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/channels/finance", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("tester", roleRoot, time.Now().Unix())})
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	base := func(domain string, upstream float64) channelFinanceSaveInput {
		return channelFinanceSaveInput{
			Domain: domain, FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1,
			UpstreamRechargePaid: upstream, UpstreamRechargeCredit: 1,
			Groups: []channelFinanceGroupInput{{Group: "codex", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1), UpstreamDiscountFactor: financeFloatPtr(1)}},
		}
	}
	if w := request(base("a.example", 0.8)); w.Code != http.StatusOK {
		t.Fatalf("save a: %d %s", w.Code, w.Body.String())
	}
	if w := request(base("b.example", 0.9)); w.Code != http.StatusOK {
		t.Fatalf("save b: %d %s", w.Code, w.Body.String())
	}
	changed := base("a.example", 0.8)
	changed.FXBenchmark = 8
	w := request(changed)
	if w.Code != http.StatusConflict {
		t.Fatalf("global change must require confirmation: %d %s", w.Code, w.Body.String())
	}
	var confirmation struct {
		CurrentVersion        int64  `json:"current_version"`
		CurrentGlobalRevision string `json:"current_global_revision"`
		AffectedDomains       int    `json:"affected_domains"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &confirmation); err != nil {
		t.Fatal(err)
	}
	if confirmation.CurrentVersion != 1 || confirmation.AffectedDomains != 2 || confirmation.CurrentGlobalRevision == "" {
		t.Fatalf("confirmation metadata=%+v body=%s", confirmation, w.Body.String())
	}
	changed.ConfirmUpdate = true
	changed.ExpectedVersion = confirmation.CurrentVersion
	changed.ExpectedGlobalRevision = confirmation.CurrentGlobalRevision
	w = request(changed)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"updated_domains":2`) {
		t.Fatalf("confirmed global update: %d %s", w.Code, w.Body.String())
	}
	var versions []ChannelFinanceVersion
	if err := m.storeDB.Order("domain ASC, version ASC").Find(&versions).Error; err != nil {
		t.Fatal(err)
	}
	if len(versions) != 4 {
		t.Fatalf("both domains must have v1 and v2: %+v", versions)
	}
	latest := map[string]channelFinanceVersionSnapshot{}
	for _, row := range versions {
		var snapshot channelFinanceVersionSnapshot
		if err := json.Unmarshal([]byte(row.SnapshotJSON), &snapshot); err != nil {
			t.Fatal(err)
		}
		if row.Version == 2 {
			latest[row.Domain] = snapshot
		}
	}
	if latest["a.example"].FXBenchmark != 8 || latest["b.example"].FXBenchmark != 8 ||
		latest["b.example"].UpstreamRechargePaid != 0.9 {
		t.Fatalf("fan-out versions did not preserve each upstream: %+v", latest)
	}
}

func TestFirstDomainVersionRejectsStaleGlobalConfirmation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.SessionSecret = "test-session-secret"
	if err := m.storeDB.Create(&[]ChannelSnap{
		{ID: 1, Name: "a", BaseDomain: "a.example", Groups: "codex", Status: 1},
		{ID: 2, Name: "b", BaseDomain: "b.example", Groups: "codex", Status: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.POST("/channels/finance", m.requireRole(roleRoot), m.saveChannelFinanceHandler)
	request := func(input channelFinanceSaveInput) *httptest.ResponseRecorder {
		body, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/channels/finance", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("tester", roleRoot, time.Now().Unix())})
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	input := func(domain string, fx float64) channelFinanceSaveInput {
		return channelFinanceSaveInput{
			Domain: domain, FXBenchmark: fx, SiteRechargePaid: 1, SiteRechargeCredit: 1,
			UpstreamRechargePaid: 1, UpstreamRechargeCredit: 1,
			Groups: []channelFinanceGroupInput{{Group: "codex", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1), UpstreamDiscountFactor: financeFloatPtr(1)}},
		}
	}
	if w := request(input("a.example", 7)); w.Code != http.StatusOK {
		t.Fatalf("save a baseline: %d %s", w.Code, w.Body.String())
	}

	// b 还没有域名版本，因此它的 ExpectedVersion 仍会是 0。
	// 在用户确认前，a 修改了全局口径；此时必须依靠全局修订号拒绝 b 的旧确认。
	b := input("b.example", 8)
	w := request(b)
	if w.Code != http.StatusConflict {
		t.Fatalf("b must require confirmation: %d %s", w.Code, w.Body.String())
	}
	var stale struct {
		CurrentVersion        int64  `json:"current_version"`
		CurrentGlobalRevision string `json:"current_global_revision"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &stale); err != nil {
		t.Fatal(err)
	}
	if stale.CurrentVersion != 0 || stale.CurrentGlobalRevision == "" {
		t.Fatalf("unexpected stale confirmation metadata: %+v", stale)
	}

	a := input("a.example", 7.5)
	w = request(a)
	if w.Code != http.StatusConflict {
		t.Fatalf("a global update must require confirmation: %d %s", w.Code, w.Body.String())
	}
	var fresh struct {
		CurrentVersion        int64  `json:"current_version"`
		CurrentGlobalRevision string `json:"current_global_revision"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &fresh); err != nil {
		t.Fatal(err)
	}
	a.ConfirmUpdate = true
	a.ExpectedVersion = fresh.CurrentVersion
	a.ExpectedGlobalRevision = fresh.CurrentGlobalRevision
	if w = request(a); w.Code != http.StatusOK {
		t.Fatalf("confirmed a update: %d %s", w.Code, w.Body.String())
	}

	b.ConfirmUpdate = true
	b.ExpectedVersion = stale.CurrentVersion
	b.ExpectedGlobalRevision = stale.CurrentGlobalRevision
	if w = request(b); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"version_conflict":true`) {
		t.Fatalf("stale global confirmation status=%d body=%s", w.Code, w.Body.String())
	}
	var bVersions int64
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain = ?", "b.example").Count(&bVersions).Error; err != nil || bVersions != 0 {
		t.Fatalf("stale confirmation must not create b version: count=%d err=%v", bVersions, err)
	}
}

func TestLayeredChannelFinanceRoutesKeepScopesSeparate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.SessionSecret = "test-session-secret"
	if err := m.storeDB.Create(&[]ChannelSnap{
		{ID: 33, Name: "LA-codex", BaseDomain: "last-api.ai", Groups: "codex-1.2x, codex-1.4x", Status: 1},
		{ID: 44, Name: "LA-codex-temp", BaseDomain: "other.example", Groups: "codex-1.2x", Status: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.POST("/site", m.requireRole(roleRoot), m.saveChannelFinanceSiteHandler)
	r.POST("/domain", m.requireRole(roleRoot), m.saveChannelFinanceDomainHandler)
	r.POST("/channel", m.requireRole(roleRoot), m.saveChannelFinanceChannelHandler)
	r.POST("/domain-rates", m.requireRole(roleRoot), m.saveChannelFinanceDomainRatesHandler)
	request := func(path string, input any) *httptest.ResponseRecorder {
		body, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("tester", roleRoot, time.Now().Unix())})
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	site := channelFinanceSiteSaveInput{
		FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1,
		Groups: []channelFinanceSiteGroupInput{{Group: "codex-1.2x", SiteMultiplier: financeFloatPtr(1.2)}},
	}
	if w := request("/site", site); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"updated_domains":2`) {
		t.Fatalf("site config status=%d body=%s", w.Code, w.Body.String())
	}

	domain := channelFinanceDomainSaveInput{Domain: "last-api.ai", UpstreamRechargePaid: 2, UpstreamRechargeCredit: 1}
	if w := request("/domain", domain); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"confirmation_required":true`) {
		t.Fatalf("domain update must require version confirmation: %d %s", w.Code, w.Body.String())
	}
	domain.ConfirmUpdate = true
	domain.ExpectedVersion = 1
	if w := request("/domain", domain); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"version":2`) {
		t.Fatalf("confirmed domain update status=%d body=%s", w.Code, w.Body.String())
	}

	channelRates := channelFinanceDomainRatesSaveInput{Domain: "last-api.ai", UpstreamRechargePaid: 2, UpstreamRechargeCredit: 1, Rates: []channelFinanceChannelRateInput{{ChannelID: 33, UpstreamGroupName: "上游 Codex 主组", UpstreamMultiplier: 1, UpstreamDiscountFactor: .8}}}
	if w := request("/domain-rates", channelRates); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"confirmation_required":true`) {
		t.Fatalf("domain channel rates update must require version confirmation: %d %s", w.Code, w.Body.String())
	}
	channelRates.ConfirmUpdate = true
	channelRates.ExpectedVersion = 2
	if w := request("/domain-rates", channelRates); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"version":3`) {
		t.Fatalf("confirmed domain channel rates update status=%d body=%s", w.Code, w.Body.String())
	}

	snapshot, err := m.loadChannelFinanceSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	view := snapshot.groupViewForChannel("last-api.ai", 33, "codex-1.2x")
	if !view.Complete || view.UpstreamGroupName != "上游 Codex 主组" || math.Abs(view.SiteMultiplier-1.2) > 1e-12 || math.Abs(view.UpstreamEffectiveMultiplier-1.6) > 1e-12 || math.Abs(view.MultiplierGap-(-.4)) > 1e-12 {
		t.Fatalf("layered finance values are mixed or incorrect: %+v", view)
	}
	var channelCost ChannelFinanceChannelCost
	if err := m.storeDB.First(&channelCost, "channel_id = ? AND grp = ?", 33, "codex-1.2x").Error; err != nil {
		t.Fatal(err)
	}
	if channelCost.UpstreamGroupName != "上游 Codex 主组" || channelCost.Multiplier != 1 || channelCost.DiscountFactor != .8 {
		t.Fatalf("channel cost=%+v", channelCost)
	}
	var secondChannelCost ChannelFinanceChannelCost
	if err := m.storeDB.First(&secondChannelCost, "channel_id = ? AND grp = ?", 33, "codex-1.4x").Error; err != nil {
		t.Fatal(err)
	}
	if secondChannelCost.UpstreamGroupName != "上游 Codex 主组" || secondChannelCost.Multiplier != 1 || secondChannelCost.DiscountFactor != .8 {
		t.Fatalf("channel-level cost must apply to every configured group: %+v", secondChannelCost)
	}
	// 旧版可能留下已经不属于渠道当前分组的行。它不能触发当前配置冲突，
	// 但下一次批量确认保存必须将其从当前表清掉。
	staleCost := ChannelFinanceChannelCost{ChannelID: 33, Grp: "internal_test", UpstreamGroupName: "旧上游组", Multiplier: 9, DiscountFactor: 1}
	if err := m.storeDB.Create(&staleCost).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err = m.loadChannelFinanceSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.channelCostConflict[33] {
		t.Fatal("removed historical group must not mark the current channel configuration as conflicting")
	}
	if equal, err := channelFinanceDomainRatesEqual(m.storeDB, "last-api.ai", channelRates.Rates); err != nil || equal {
		t.Fatalf("stale current-table rows must require a cleanup save: equal=%v err=%v", equal, err)
	}
	channelRates.ConfirmUpdate = false
	channelRates.ExpectedVersion = 0
	if w := request("/domain-rates", channelRates); w.Code != http.StatusConflict {
		t.Fatalf("stale row cleanup must require confirmation: %d %s", w.Code, w.Body.String())
	}
	channelRates.ConfirmUpdate = true
	channelRates.ExpectedVersion = 3
	if w := request("/domain-rates", channelRates); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"version":3`) {
		t.Fatalf("confirmed cleanup save status=%d body=%s", w.Code, w.Body.String())
	}
	var remainingCosts []ChannelFinanceChannelCost
	if err := m.storeDB.Where("channel_id = ?", 33).Order("grp").Find(&remainingCosts).Error; err != nil {
		t.Fatal(err)
	}
	if len(remainingCosts) != 2 || remainingCosts[0].Grp != "codex-1.2x" || remainingCosts[1].Grp != "codex-1.4x" {
		t.Fatalf("cleanup must leave exactly the current channel groups: %+v", remainingCosts)
	}
	var legacyGroupCost int64
	if err := m.storeDB.Model(&ChannelDomainGroupCost{}).Where("domain = ?", "last-api.ai").Count(&legacyGroupCost).Error; err != nil {
		t.Fatal(err)
	}
	if legacyGroupCost != 0 {
		t.Fatalf("new layered routes must not write legacy domain group costs: %d", legacyGroupCost)
	}
}
