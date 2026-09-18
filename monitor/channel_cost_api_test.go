package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func postChannelCostBinding(t *testing.T, m *Monitor, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/cost/bindings", m.saveChannelCostBindingHandler)
	req := httptest.NewRequest(http.MethodPost, "/channels/cost/bindings", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	return response
}

func postChannelCostHistoricalBinding(t *testing.T, m *Monitor, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/cost/historical-bindings", m.saveChannelCostHistoricalBindingHandler)
	req := httptest.NewRequest(http.MethodPost, "/channels/cost/historical-bindings", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	return response
}

func previewChannelCostHistoricalBinding(t *testing.T, m *Monitor, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/cost/historical-bindings/preview", m.previewChannelCostHistoricalBindingHandler)
	req := httptest.NewRequest(http.MethodPost, "/channels/cost/historical-bindings/preview", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	return response
}

func TestHistoricalCostBindingIsFiniteAuditedAndQueuesOnlyVerifiedHours(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelSnap{}, &StabilityHourSample{}); err != nil {
		t.Fatal(err)
	}
	domain, epoch, source := "4sapi.com", strings.Repeat("a", 64), strings.Repeat("b", 64)
	for index, hour := range []int64{3600, 10800} {
		if err := db.Create(&ChannelUpstreamCostHourEvidence{
			Domain: domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			SourceRef: source, DimensionHash: strings.Repeat(fmt.Sprintf("%x", index+1), 64), Provider: upstreamProviderNewAPI,
			SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1", ChargeUnits: 500_000,
			ChargeUnitsPerUSD: "500000", Requests: int64(index + 2),
		}).Error; err != nil {
			t.Fatal(err)
		}
		status := "observed"
		if hour == 3600 {
			status = "verified"
		}
		if err := db.Create(&ChannelUpstreamCostHourState{
			Domain: domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			Provider: upstreamProviderNewAPI, Status: status, ReconcileStatus: "matched",
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&ChannelSnap{ID: 59, Name: "4sapi_Gpt-codex", BaseDomain: domain, DeletedAt: time.Now().Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 60, BaseDomain: "other.example"}).Error; err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"domain": domain, "account_epoch": epoch, "source_ref": source, "local_channel_id": 59, "reason": "confirmed from upstream token ownership record"}
	localOnly := &Monitor{storeDB: db, cfg: Settings{LocalSnapshotOnly: true, ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{domain}}}
	if err := db.Create(&StabilityHourSample{HourTs: 3600, ChannelID: 59, ModelName: "gpt-5.6-sol", Grp: "codex-1.2x", Success: 7, Failed: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelTestHourSample{HourTs: 10800, ChannelID: 59, ModelName: "gpt-5.6-sol", Grp: "internal", Origin: "scheduled", TrafficClassVersion: stabilityTrafficClassificationVersion, Requests: 3}).Error; err != nil {
		t.Fatal(err)
	}
	previewResponse := previewChannelCostHistoricalBinding(t, localOnly, body)
	if previewResponse.Code != http.StatusOK {
		t.Fatalf("local snapshot preview rejected: status=%d body=%s", previewResponse.Code, previewResponse.Body.String())
	}
	var preview channelCostHistoricalBindingPlan
	if err := json.Unmarshal(previewResponse.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.ChannelName != "4sapi_Gpt-codex" || preview.EvidenceHours != 2 || preview.WillQueueHours != 1 || preview.LocalActiveHours != 1 || preview.LocalRequests != 8 || !preview.HasLocalActivity {
		t.Fatalf("historical preview mismatch: %+v", preview)
	}
	if preview.ActivityCoverage != 0.5 || preview.TestActivityCoverage != 0.5 || preview.LocalTestActiveHours != 1 || preview.LocalTestRequests != 3 || preview.TemporalOverlapQuality != "review" || len(preview.RiskWarnings) != 3 {
		t.Fatalf("historical preview evidence quality mismatch: %+v", preview)
	}
	if preview.EvidenceBilledCost.MicroUSD != "2000000" || preview.ActiveBilledCost.MicroUSD != "1000000" || preview.InactiveBilledCost.MicroUSD != "1000000" || preview.CostActivityCoverage != 0.5 || preview.ActiveEvidenceRequests != 2 || preview.InactiveEvidenceRequests != 3 {
		t.Fatalf("historical preview economic overlap mismatch: %+v", preview)
	}
	var beforeBindings, beforeDirty int64
	if err := db.Model(&ChannelCostSourceBinding{}).Count(&beforeBindings).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelEconomicsDirtyHour{}).Count(&beforeDirty).Error; err != nil {
		t.Fatal(err)
	}
	if beforeBindings != 0 || beforeDirty != 0 {
		t.Fatalf("preview performed writes: bindings=%d dirty=%d", beforeBindings, beforeDirty)
	}
	if response := postChannelCostHistoricalBinding(t, localOnly, body); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("local snapshot accepted historical write: status=%d body=%s", response.Code, response.Body.String())
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{domain}}}
	wrong := map[string]any{"domain": domain, "account_epoch": epoch, "source_ref": source, "local_channel_id": 60, "reason": "wrong domain"}
	if response := postChannelCostHistoricalBinding(t, m, wrong); response.Code != http.StatusBadRequest {
		t.Fatalf("cross-domain historical mapping accepted: status=%d body=%s", response.Code, response.Body.String())
	}
	response := postChannelCostHistoricalBinding(t, m, body)
	if response.Code != http.StatusOK {
		t.Fatalf("valid historical binding rejected: status=%d body=%s", response.Code, response.Body.String())
	}
	var binding ChannelCostSourceBinding
	if err := db.First(&binding, "domain = ? AND account_epoch = ? AND source_ref = ?", domain, epoch, source).Error; err != nil {
		t.Fatal(err)
	}
	if binding.ValidFrom != 3600 || binding.ValidTo != 14400 || binding.LocalChannelID != 59 || binding.MappingSource != "manual_history" || binding.Status != "confirmed" {
		t.Fatalf("historical binding contract mismatch: %+v", binding)
	}
	var dirty []ChannelEconomicsDirtyHour
	if err := db.Order("hour_ts").Find(&dirty).Error; err != nil {
		t.Fatal(err)
	}
	if len(dirty) != 1 || dirty[0].HourTs != 3600 {
		t.Fatalf("historical binding queued wrong hours: %+v", dirty)
	}
	if duplicate := postChannelCostHistoricalBinding(t, m, body); duplicate.Code != http.StatusConflict {
		t.Fatalf("overlapping historical replay accepted: status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
}

func TestHistoricalBindingEvidenceQuality(t *testing.T) {
	tests := []struct {
		name             string
		evidenceHours    int64
		localActiveHours int64
		wantCoverage     float64
		wantQuality      string
		wantWarnings     int
	}{
		{name: "invalid", evidenceHours: 0, wantQuality: "invalid", wantWarnings: 1},
		{name: "no activity", evidenceHours: 100, wantQuality: "no_activity", wantWarnings: 1},
		{name: "weak", evidenceHours: 100, localActiveHours: 9, wantCoverage: 0.09, wantQuality: "weak", wantWarnings: 1},
		{name: "review", evidenceHours: 100, localActiveHours: 79, wantCoverage: 0.79, wantQuality: "review", wantWarnings: 1},
		{name: "strong", evidenceHours: 100, localActiveHours: 80, wantCoverage: 0.8, wantQuality: "strong"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coverage, quality, warnings := historicalBindingEvidenceQuality(test.evidenceHours, test.localActiveHours)
			if coverage != test.wantCoverage || quality != test.wantQuality || len(warnings) != test.wantWarnings {
				t.Fatalf("quality mismatch: coverage=%v quality=%q warnings=%v", coverage, quality, warnings)
			}
		})
	}
}

func TestHistoricalCostBindingRejectsSparseWideOrMixedEvidence(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	domain, epoch := "4sapi.com", strings.Repeat("a", 64)
	if err := db.Create(&ChannelSnap{ID: 59, BaseDomain: domain}).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{domain}}}
	createEvidence := func(source, dimension, provider, key string, hour int64) {
		t.Helper()
		if err := db.Create(&ChannelUpstreamCostHourEvidence{
			Domain: domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			SourceRef: source, DimensionHash: dimension, Provider: provider,
			SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: key,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	wideSource := strings.Repeat("b", 64)
	createEvidence(wideSource, strings.Repeat("1", 64), upstreamProviderNewAPI, "key-v1", 3600)
	createEvidence(wideSource, strings.Repeat("2", 64), upstreamProviderNewAPI, "key-v1", 3600+int64(channelCostHistoricalBindingMaxHours)*3600)
	body := map[string]any{"domain": domain, "account_epoch": epoch, "source_ref": wideSource, "local_channel_id": 59, "reason": "wide sparse evidence"}
	if response := postChannelCostHistoricalBinding(t, m, body); response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("wide sparse evidence accepted: status=%d body=%s", response.Code, response.Body.String())
	}

	mixedSource := strings.Repeat("c", 64)
	createEvidence(mixedSource, strings.Repeat("3", 64), upstreamProviderNewAPI, "key-v1", 3600)
	createEvidence(mixedSource, strings.Repeat("4", 64), "unexpected-provider", "key-v2", 3600)
	body["source_ref"] = mixedSource
	body["reason"] = "mixed metadata"
	if response := postChannelCostHistoricalBinding(t, m, body); response.Code != http.StatusConflict {
		t.Fatalf("mixed source metadata accepted: status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestChannelCostBindingAPIFailsClosedAndValidatesChannelDomain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamAccount{}, &ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1}
	if err := db.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	sourceRef := strings.Repeat("a", 64)
	evidence := ChannelUpstreamCostHourEvidence{Domain: account.Domain, AccountEpoch: epoch, HourTs: 3600, SemanticsVersion: channelCostEvidenceSemanticsVersion, SourceRef: sourceRef, DimensionHash: strings.Repeat("b", 64), Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "key-v1", PricingDimensionHash: strings.Repeat("c", 64), ChargeUnit: channelCostChargeUnitNewAPIQuota, Requests: 1}
	evidence.SourceGroup, evidence.UpstreamModel = "Gpt-codex", "gpt-5.4"
	if err := db.Create(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	secondDimension := evidence
	secondDimension.HourTs = 7200
	secondDimension.DimensionHash = strings.Repeat("d", 64)
	secondDimension.PricingDimensionHash = strings.Repeat("e", 64)
	secondDimension.SourceGroup, secondDimension.UpstreamModel = "Gpt-codex-pro", "gpt-5.5"
	if err := db.Create(&secondDimension).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 59, BaseDomain: account.Domain}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 61, BaseDomain: account.Domain}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 60, BaseDomain: "other.example"}).Error; err != nil {
		t.Fatal(err)
	}
	future := nextWholeHour(time.Now().Unix())
	body := map[string]any{"domain": account.Domain, "account_epoch": epoch, "source_ref": sourceRef, "source_ref_kind": channelCostSourceKindNewAPIToken, "hmac_key_id": "key-v1", "local_channel_id": 59, "valid_from": future, "allocation_mode": "allocated", "reason": "initial mapping", "expected_current_valid_from": 0}
	disabled := &Monitor{storeDB: db}
	if response := postChannelCostBinding(t, disabled, body); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled API status=%d body=%s", response.Code, response.Body.String())
	}
	snapshotListRouter := gin.New()
	snapshotListRouter.GET("/channels/cost/sources", (&Monitor{storeDB: db, cfg: Settings{LocalSnapshotOnly: true}}).listChannelCostSourcesHandler)
	snapshotListResponse := httptest.NewRecorder()
	snapshotListRouter.ServeHTTP(snapshotListResponse, httptest.NewRequest(http.MethodGet, "/channels/cost/sources?domain="+account.Domain, nil))
	if snapshotListResponse.Code != http.StatusOK {
		t.Fatalf("local snapshot read-only source list rejected: status=%d body=%s", snapshotListResponse.Code, snapshotListResponse.Body.String())
	}
	disabledListRouter := gin.New()
	disabledListRouter.GET("/channels/cost/sources", disabled.listChannelCostSourcesHandler)
	disabledListResponse := httptest.NewRecorder()
	disabledListRouter.ServeHTTP(disabledListResponse, httptest.NewRequest(http.MethodGet, "/channels/cost/sources?domain="+account.Domain, nil))
	if disabledListResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("production source list bypassed rollout gate: status=%d body=%s", disabledListResponse.Code, disabledListResponse.Body.String())
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{account.Domain}}}
	wrong := make(map[string]any, len(body))
	for key, value := range body {
		wrong[key] = value
	}
	wrong["local_channel_id"] = 60
	if response := postChannelCostBinding(t, m, wrong); response.Code != http.StatusBadRequest {
		t.Fatalf("cross-domain channel accepted: status=%d body=%s", response.Code, response.Body.String())
	}
	if response := postChannelCostBinding(t, m, body); response.Code != http.StatusOK {
		t.Fatalf("valid binding rejected: status=%d body=%s", response.Code, response.Body.String())
	}
	listRouter := gin.New()
	listRouter.GET("/channels/cost/sources", m.listChannelCostSourcesHandler)
	listResponse := httptest.NewRecorder()
	listRouter.ServeHTTP(listResponse, httptest.NewRequest(http.MethodGet, "/channels/cost/sources?domain="+account.Domain, nil))
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), `"AllocationMode":"allocated"`) || !strings.Contains(listResponse.Body.String(), `"LocalChannelID":59`) {
		t.Fatalf("next-hour binding was not visible after save: status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	var listed struct {
		Sources []struct {
			SourceGroups            []string                 `json:"source_groups"`
			UpstreamModels          []string                 `json:"upstream_models"`
			DimensionCount          int                      `json:"dimension_count"`
			CurrentBinding          ChannelCostSourceBinding `json:"current_binding"`
			CurrentBindingSignature string                   `json:"current_binding_signature"`
		} `json:"sources"`
	}
	if err := json.Unmarshal(listResponse.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Sources) != 1 || listed.Sources[0].DimensionCount != 2 || strings.Join(listed.Sources[0].SourceGroups, ",") != "Gpt-codex,Gpt-codex-pro" || strings.Join(listed.Sources[0].UpstreamModels, ",") != "gpt-5.4,gpt-5.5" {
		t.Fatalf("one binding identity must aggregate all pricing dimensions: %+v", listed.Sources)
	}
	firstSignature := listed.Sources[0].CurrentBindingSignature
	if !validSHA256Hex(firstSignature) {
		t.Fatalf("source list did not return a valid content CAS signature: %q", firstSignature)
	}
	if response := postChannelCostBinding(t, m, body); response.Code != http.StatusConflict {
		t.Fatalf("stale duplicate binding status=%d body=%s", response.Code, response.Body.String())
	}
	corrected := make(map[string]any, len(body)+2)
	for key, value := range body {
		corrected[key] = value
	}
	corrected["local_channel_id"] = 61
	corrected["reason"] = "correct future mapping"
	corrected["expected_current_valid_from"] = future
	corrected["expected_current_signature"] = firstSignature
	if response := postChannelCostBinding(t, m, corrected); response.Code != http.StatusOK {
		t.Fatalf("same-hour future correction rejected: status=%d body=%s", response.Code, response.Body.String())
	}
	correctedList := httptest.NewRecorder()
	listRouter.ServeHTTP(correctedList, httptest.NewRequest(http.MethodGet, "/channels/cost/sources?domain="+account.Domain, nil))
	var correctedState struct {
		Sources []struct {
			CurrentBinding          ChannelCostSourceBinding `json:"current_binding"`
			CurrentBindingSignature string                   `json:"current_binding_signature"`
		} `json:"sources"`
	}
	if err := json.Unmarshal(correctedList.Body.Bytes(), &correctedState); err != nil {
		t.Fatal(err)
	}
	if len(correctedState.Sources) != 1 || correctedState.Sources[0].CurrentBinding.LocalChannelID != 61 || correctedState.Sources[0].CurrentBindingSignature == firstSignature {
		t.Fatalf("corrected mapping/signature not returned: %+v", correctedState.Sources)
	}
	staleReplay := make(map[string]any, len(corrected))
	for key, value := range corrected {
		staleReplay[key] = value
	}
	staleReplay["local_channel_id"] = 59
	staleReplay["reason"] = "stale replay"
	if response := postChannelCostBinding(t, m, staleReplay); response.Code != http.StatusConflict {
		t.Fatalf("old signature replay status=%d body=%s", response.Code, response.Body.String())
	}
	emptyReason := make(map[string]any, len(corrected))
	for key, value := range corrected {
		emptyReason[key] = value
	}
	emptyReason["reason"] = "  "
	emptyReason["expected_current_signature"] = correctedState.Sources[0].CurrentBindingSignature
	if response := postChannelCostBinding(t, m, emptyReason); response.Code != http.StatusBadRequest {
		t.Fatalf("empty audit reason status=%d body=%s", response.Code, response.Body.String())
	}
	wrongHour := make(map[string]any, len(body))
	for key, value := range body {
		wrongHour[key] = value
	}
	wrongHour["valid_from"] = future + 3600
	if response := postChannelCostBinding(t, m, wrongHour); response.Code != http.StatusBadRequest {
		t.Fatalf("arbitrary mapping effective hour accepted: status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestChannelFinanceVersionAuditViewsShowFieldLevelHistory(t *testing.T) {
	before := channelFinanceVersionSnapshot{
		Domain: "4sapi.com", FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1,
		UpstreamRechargePaid: 1, UpstreamRechargeCredit: 1,
		Groups:       []channelFinanceVersionGroup{{Group: "codex-1.2x", SiteMultiplier: 1.2, UpstreamMultiplier: 1, UpstreamDiscountFactor: 1}},
		ChannelRates: []channelFinanceVersionChannel{{ChannelID: 59, Group: "codex-1.2x", UpstreamGroupName: "Gpt-codex", UpstreamMultiplier: 1, UpstreamDiscountFactor: 1}},
	}
	after := before
	after.ChannelRates = append([]channelFinanceVersionChannel(nil), before.ChannelRates...)
	after.ChannelRates[0].UpstreamMultiplier = 1.1
	after.ChannelRates[0].UpstreamDiscountFactor = 0.9
	encode := func(value channelFinanceVersionSnapshot) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	views, err := channelFinanceVersionAuditViews([]ChannelFinanceVersion{
		{Domain: before.Domain, Version: 2, SnapshotJSON: encode(after), EffectiveAt: 7200, CreatedAt: 7000, UpdatedBy: "root"},
		{Domain: before.Domain, Version: 1, SnapshotJSON: encode(before), EffectiveAt: 3600, CreatedAt: 3500, UpdatedBy: "root"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 2 || views[0].Version != 2 || views[1].Version != 1 {
		t.Fatalf("version audit ordering invalid: %+v", views)
	}
	var multiplier, discount bool
	for _, change := range views[0].Changes {
		if change.Scope == "channel_group" && change.Key == "59/codex-1.2x" && change.Field == "upstream_multiplier" && change.OldValue == "1" && change.NewValue == "1.1" {
			multiplier = true
		}
		if change.Scope == "channel_group" && change.Key == "59/codex-1.2x" && change.Field == "upstream_discount_factor" && change.OldValue == "1" && change.NewValue == "0.9" {
			discount = true
		}
	}
	if !multiplier || !discount || !validSHA256Hex(views[0].SnapshotHash) {
		t.Fatalf("field-level finance audit incomplete: %+v", views[0])
	}
	if _, err := channelFinanceVersionAuditViews([]ChannelFinanceVersion{{Domain: before.Domain, Version: 1, SnapshotJSON: "{"}}); err == nil {
		t.Fatal("corrupt immutable finance version must fail closed")
	}
}

func TestChannelPricingProposalAPIShowsEveryAffectedServiceGroup(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelFinanceChannelCost{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []ChannelFinanceChannelCost{
		{ChannelID: 59, Grp: "codex-1.2x", UpstreamGroupName: "Gpt-codex", Multiplier: 1, DiscountFactor: 0.5},
		{ChannelID: 59, Grp: "codex-1.4x", UpstreamGroupName: "Gpt-codex", Multiplier: 1, DiscountFactor: 0.5},
	} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 23; i++ {
		if err := db.Create(&ChannelFinanceChannelCost{ChannelID: 59, Grp: fmt.Sprintf("extra-%02d", i), UpstreamGroupName: "Gpt-codex", Multiplier: 1, DiscountFactor: 0.5}).Error; err != nil {
			t.Fatal(err)
		}
	}
	proposal := ChannelPricingChangeProposal{
		ProposalKey: strings.Repeat("a", 64), Domain: "4sapi.com", LocalChannelID: 59,
		SourceGroup: "Gpt-codex", OldValue: "1/2", NewValue: "3/5", Status: "pending",
	}
	if err := db.Create(&proposal).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{proposal.Domain}}}
	router := gin.New()
	router.GET("/channels/cost/proposals", m.listChannelPricingProposalsHandler)
	router.GET("/channels/cost/proposals/:proposal_key/impact", m.getChannelPricingProposalImpactHandler)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/channels/cost/proposals?domain="+proposal.Domain, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("proposal list status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Proposals []struct {
			Impact          *channelFinanceActivationPatch `json:"impact"`
			ImpactTotal     int                            `json:"impact_total"`
			ImpactTruncated bool                           `json:"impact_truncated"`
		} `json:"proposals"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Proposals) != 1 || result.Proposals[0].Impact == nil || len(result.Proposals[0].Impact.Rows) != 20 || result.Proposals[0].ImpactTotal != 25 || !result.Proposals[0].ImpactTruncated {
		t.Fatalf("approval impact did not include every affected service group: %+v body=%s", result.Proposals, response.Body.String())
	}
	groups := []string{result.Proposals[0].Impact.Rows[0].Before.Group, result.Proposals[0].Impact.Rows[1].Before.Group}
	if strings.Join(groups, ",") != "codex-1.2x,codex-1.4x" {
		t.Fatalf("approval impact groups=%v", groups)
	}
	detailResponse := httptest.NewRecorder()
	router.ServeHTTP(detailResponse, httptest.NewRequest(http.MethodGet, "/channels/cost/proposals/"+proposal.ProposalKey+"/impact", nil))
	var detail struct {
		Impact channelFinanceActivationPatch `json:"impact"`
	}
	if err := json.Unmarshal(detailResponse.Body.Bytes(), &detail); err != nil || detailResponse.Code != http.StatusOK || len(detail.Impact.Rows) != 25 {
		t.Fatalf("lazy impact detail must return the complete bounded patch: status=%d err=%v body=%s", detailResponse.Code, err, detailResponse.Body.String())
	}
	approvedPatch := detail.Impact
	patchJSON, err := encodeFinanceActivationPatch(approvedPatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelPricingChangeProposal{}).Where("proposal_key = ?", proposal.ProposalKey).Updates(map[string]any{"status": "applied", "applied_version": 2}).Error; err != nil {
		t.Fatal(err)
	}
	versionRaw, err := json.Marshal(channelFinanceVersionSnapshot{Domain: proposal.Domain})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceVersion{Domain: proposal.Domain, Version: 2, SnapshotJSON: string(versionRaw), EffectiveAt: 2, CreatedAt: 2, UpdatedBy: "root"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceActivation{ActivationID: strings.Repeat("b", 64), ProposalKey: proposal.ProposalKey, Domain: proposal.Domain, Action: "approve", Status: "applied", PatchJSON: patchJSON, AppliedVersion: 2, IdempotencyKey: "applied-1"}).Error; err != nil {
		t.Fatal(err)
	}
	rollbackResponse := httptest.NewRecorder()
	router.ServeHTTP(rollbackResponse, httptest.NewRequest(http.MethodGet, "/channels/cost/proposals?domain="+proposal.Domain, nil))
	var rollbackResult struct {
		Proposals []struct {
			Impact          *channelFinanceActivationPatch `json:"impact"`
			RollbackAllowed bool                           `json:"rollback_allowed"`
		} `json:"proposals"`
	}
	if err := json.Unmarshal(rollbackResponse.Body.Bytes(), &rollbackResult); err != nil {
		t.Fatal(err)
	}
	if len(rollbackResult.Proposals) != 1 || !rollbackResult.Proposals[0].RollbackAllowed || rollbackResult.Proposals[0].Impact == nil ||
		rollbackResult.Proposals[0].Impact.Rows[0].Before.Multiplier != approvedPatch.Rows[0].After.Multiplier ||
		rollbackResult.Proposals[0].Impact.Rows[0].After.Multiplier != approvedPatch.Rows[0].Before.Multiplier {
		t.Fatalf("applied proposal must preview the reverse rollback patch: body=%s", rollbackResponse.Body.String())
	}
	if err := db.Create(&ChannelFinanceVersion{Domain: proposal.Domain, Version: 3, SnapshotJSON: string(versionRaw), EffectiveAt: 3, CreatedAt: 3, UpdatedBy: "root"}).Error; err != nil {
		t.Fatal(err)
	}
	staleResponse := httptest.NewRecorder()
	router.ServeHTTP(staleResponse, httptest.NewRequest(http.MethodGet, "/channels/cost/proposals?domain="+proposal.Domain, nil))
	if staleResponse.Code != http.StatusOK {
		t.Fatalf("stale applied list status=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}
	var staleResult struct {
		Proposals []struct {
			ProposalKey     string `json:"ProposalKey"`
			RollbackAllowed bool   `json:"rollback_allowed"`
		} `json:"proposals"`
	}
	if err := json.Unmarshal(staleResponse.Body.Bytes(), &staleResult); err != nil {
		t.Fatal(err)
	}
	for _, row := range staleResult.Proposals {
		if row.ProposalKey == proposal.ProposalKey && row.RollbackAllowed {
			t.Fatalf("stale applied proposal must not remain rollbackable: body=%s", staleResponse.Body.String())
		}
	}
	staleImpact := httptest.NewRecorder()
	router.ServeHTTP(staleImpact, httptest.NewRequest(http.MethodGet, "/channels/cost/proposals/"+proposal.ProposalKey+"/impact", nil))
	if staleImpact.Code != http.StatusConflict {
		t.Fatalf("stale applied proposal impact status=%d body=%s", staleImpact.Code, staleImpact.Body.String())
	}
	if err := db.Model(&ChannelPricingChangeProposal{}).Where("proposal_key = ?", proposal.ProposalKey).Update("status", "pending").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelFinanceChannelCost{}).Where("channel_id = ?", 59).Update("multiplier", 2.0).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceActivation{ActivationID: strings.Repeat("c", 64), ProposalKey: proposal.ProposalKey, Domain: proposal.Domain, Action: "approve", Status: "cancelled", PatchJSON: patchJSON, RequestedAt: 100, IdempotencyKey: "cancelled-1"}).Error; err != nil {
		t.Fatal(err)
	}
	pendingAgain := httptest.NewRecorder()
	router.ServeHTTP(pendingAgain, httptest.NewRequest(http.MethodGet, "/channels/cost/proposals?domain="+proposal.Domain, nil))
	var refreshed struct {
		Proposals []struct {
			Impact *channelFinanceActivationPatch `json:"impact"`
		} `json:"proposals"`
	}
	if err := json.Unmarshal(pendingAgain.Body.Bytes(), &refreshed); err != nil {
		t.Fatal(err)
	}
	if len(refreshed.Proposals) != 1 || refreshed.Proposals[0].Impact == nil || refreshed.Proposals[0].Impact.Rows[0].Before.Multiplier != 2 {
		t.Fatalf("cancelled activation leaked stale impact into pending preview: body=%s", pendingAgain.Body.String())
	}
}

func TestChannelPricingProposalListKeepsOldPendingAheadOfLargeHistory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelFinanceChannelCost{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	domain := "4sapi.com"
	if err := db.Create(&ChannelFinanceChannelCost{ChannelID: 59, Grp: "codex-1.2x", UpstreamGroupName: "Gpt-codex", Multiplier: 1, DiscountFactor: 1}).Error; err != nil {
		t.Fatal(err)
	}
	pending := ChannelPricingChangeProposal{ProposalKey: strings.Repeat("f", 64), Domain: domain, LocalChannelID: 59, SourceGroup: "Gpt-codex", NewValue: "11/10", Status: "pending", CreatedAt: 1}
	if err := db.Create(&pending).Error; err != nil {
		t.Fatal(err)
	}
	history := make([]ChannelPricingChangeProposal, 501)
	for i := range history {
		history[i] = ChannelPricingChangeProposal{ProposalKey: fmt.Sprintf("%064x", i+1), Domain: domain, Status: "conflict", CreatedAt: int64(1000 + i)}
	}
	if err := db.CreateInBatches(&history, 100).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{domain}}}
	router := gin.New()
	router.GET("/channels/cost/proposals", m.listChannelPricingProposalsHandler)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/channels/cost/proposals?domain="+domain, nil))
	var result struct {
		Proposals []struct {
			ProposalKey string `json:"ProposalKey"`
			Status      string `json:"Status"`
		} `json:"proposals"`
		ProposalsTruncated bool `json:"proposals_truncated"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(result.Proposals) == 0 || result.Proposals[0].ProposalKey != pending.ProposalKey || result.Proposals[0].Status != "pending" || !result.ProposalsTruncated {
		t.Fatalf("old actionable proposal was hidden by history: status=%d body=%s", response.Code, response.Body.String())
	}
}
