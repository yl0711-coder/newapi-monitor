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
