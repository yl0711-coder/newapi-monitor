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
		domainCosts:     map[string]ChannelDomainCost{"last-api.ai": {Domain: "last-api.ai", RechargePaid: 0.8, RechargeCredit: 1}},
		domainGroupCost: map[string]map[string]ChannelDomainGroupCost{"last-api.ai": {"codex-1.3x": {Domain: "last-api.ai", Grp: "codex-1.3x", Multiplier: 1}}},
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
	if math.Abs(got.MultiplierGap-0.3) > 1e-12 {
		t.Fatalf("multiplier gap=%v want %v", got.MultiplierGap, 0.3)
	}

	missing := snapshot.groupView("last-api.ai", "unconfigured")
	if missing.Complete || missing.SiteDiscount != 0 || missing.UpstreamDiscount != 0 || missing.EstimatedMargin != 0 {
		t.Fatalf("incomplete finance must not expose calculated values: %+v", missing)
	}

	multiplierOnly := snapshot
	multiplierOnly.hasSettings = false
	got = multiplierOnly.groupView("last-api.ai", "codex-1.3x")
	if got.Complete || math.Abs(got.MultiplierGap-0.3) > 1e-12 {
		t.Fatalf("multiplier gap must remain available before discount inputs are complete: %+v", got)
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

func TestValidateChannelFinanceInputRejectsPartialAndInvalidValues(t *testing.T) {
	valid := channelFinanceSaveInput{
		Domain: "last-api.ai", FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1,
		UpstreamRechargePaid: 1, UpstreamRechargeCredit: 1,
		Groups: []channelFinanceGroupInput{{Group: "codex", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1)}},
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
		Groups: []channelFinanceGroupInput{{Group: "codex-1.3x", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1)}},
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
	if !got.Complete || math.Abs(got.EstimatedMargin-(1-0.8/1.3)) > 1e-12 {
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
	updated.Groups = []channelFinanceGroupInput{{Group: "codex-1.3x", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1.1)}}
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
	stale.Groups = []channelFinanceGroupInput{{Group: "codex-1.3x", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1.2)}}
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
			Groups: []channelFinanceGroupInput{{Group: "codex", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1)}},
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
			Groups: []channelFinanceGroupInput{{Group: "codex", SiteMultiplier: financeFloatPtr(1.3), UpstreamMultiplier: financeFloatPtr(1)}},
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
