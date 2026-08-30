package monitor

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func newFinanceActivationFixture(t *testing.T) (*Monitor, ChannelPricingChangeProposal) {
	t.Helper()
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(
		&ChannelUpstreamAccount{}, &ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{},
		&ChannelFinanceSetting{}, &ChannelDomainCost{}, &ChannelDomainGroupCost{}, &ChannelSaleGroupRate{},
		&ChannelFinanceChannelCost{}, &ChannelFinanceVersion{}, &ChannelSnap{},
	); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db, cfg: Settings{
		ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"},
		ChannelCostHMACKey: "0123456789abcdef0123456789abcdef", ChannelCostHMACKeyID: "v1",
	}}
	account := ChannelUpstreamAccount{
		Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com",
		UserID: 147426, Account: "billing@example.com", Enabled: true, UsageSyncEnabled: true,
	}
	if err := db.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceSetting{ID: 1, FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelDomainCost{Domain: account.Domain, RechargePaid: 1, RechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSaleGroupRate{Grp: "codex-1.2x", Multiplier: 1.2}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelSnap{ID: 59, Name: "4sapi_codex_1x", BaseDomain: account.Domain}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceChannelCost{ChannelID: 59, Grp: "codex-1.2x", UpstreamGroupName: "Gpt-codex", Multiplier: 1, DiscountFactor: 0.5}).Error; err != nil {
		t.Fatal(err)
	}
	raw, err := currentChannelFinanceVersionJSON(db, account.Domain)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFinanceVersion{Domain: account.Domain, Version: 1, SnapshotJSON: raw, EffectiveAt: 1, CreatedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	sourceRef, err := channelCostSourceRef([]byte(m.cfg.ChannelCostHMACKey), account.Provider, epoch, channelCostSourceKindNewAPIToken, "75")
	if err != nil {
		t.Fatal(err)
	}
	for i, hour := range []int64{3600, 7200} {
		if err := db.Create(&ChannelUpstreamCostHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: account.Provider, Status: "verified", ReconcileStatus: "matched", Requests: 10}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&ChannelUpstreamCostHourEvidence{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
			SourceRef: sourceRef, DimensionHash: strings.Repeat(string(rune('a'+i)), 64), Provider: account.Provider,
			SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "v1", SourceGroup: "Gpt-codex", UpstreamModel: "gpt-5.5",
			ChargeUnits: 100, ChargeUnit: channelCostChargeUnitNewAPIQuota, Requests: 10,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&ChannelUpstreamPricingHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion, Status: "verified", ReconcileStatus: "matched"}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&ChannelUpstreamPricingHourEvidence{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion,
			DimensionHash: strings.Repeat(string(rune('c'+i)), 64), Provider: account.Provider, SourceGroup: "Gpt-codex",
			ModelName: "gpt-5.5", OtherValid: true, EvidenceCapability: "full_rate", EffectiveRatioSource: "group_ratio",
			EffectiveRatio: "1.1", EligibleRequests: 10,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.saveCostSourceBinding(context.Background(), ChannelCostSourceBinding{
		Domain: account.Domain, AccountEpoch: epoch, SourceRef: sourceRef, ValidFrom: 3600,
		Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "v1",
		LocalChannelID: 59, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual",
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.detectNewAPIChannelPricingProposals(context.Background(), account, 7200, 9999); err != nil {
		t.Fatal(err)
	}
	var proposal ChannelPricingChangeProposal
	if err := db.First(&proposal).Error; err != nil {
		t.Fatal(err)
	}
	return m, proposal
}

func decisionRequest(t *testing.T, m *Monitor, proposalKey string, body channelPricingProposalDecisionInput) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/p/:proposal_key", func(c *gin.Context) { c.Set("uname", "root"); m.decideChannelPricingProposalHandler(c) })
	req := httptest.NewRequest(http.MethodPost, "/p/"+proposalKey, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	return recorder, response
}

func cancelActivationRequest(t *testing.T, m *Monitor, activationID, reason string) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(channelFinanceActivationCancelInput{Reason: reason})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/a/:activation_id/cancel", func(c *gin.Context) { c.Set("uname", "root"); m.cancelChannelFinanceActivationHandler(c) })
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/a/"+activationID+"/cancel", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)
	return recorder
}

func TestFinanceApprovalSchedulesAppliesIdempotentlyAndRollsBackExactly(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	effective := nextWholeHour(time.Now().Unix())
	approve := channelPricingProposalDecisionInput{
		Action: "approve", ExpectedStatus: "pending", ExpectedBaseVersion: proposal.BaseVersion,
		ExpectedEvidenceDigest: proposal.EvidenceDigest, IdempotencyKey: "approve-1", Reason: "verified evidence", EffectiveFrom: effective,
	}
	recorder, _ := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("approve=%d %s", recorder.Code, recorder.Body.String())
	}
	var before ChannelFinanceChannelCost
	if err := m.storeDB.First(&before, "channel_id = ? AND grp = ?", 59, "codex-1.2x").Error; err != nil {
		t.Fatal(err)
	}
	if before.Multiplier != 1 || before.DiscountFactor != 0.5 {
		t.Fatalf("approval changed current row before boundary: %+v", before)
	}
	if applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), effective-1); err != nil || applied {
		t.Fatalf("activation ran before boundary applied=%v err=%v", applied, err)
	}
	// A user key matching the old internal convention must not block activation.
	if err := m.storeDB.Create(&ChannelPricingProposalEvent{ProposalKey: proposal.ProposalKey, IdempotencyKey: "activate-" + financeActivationID(proposal.ProposalKey, approve.IdempotencyKey, channelFinanceDecisionRequestHash(proposal.ProposalKey, approve))[:16], Event: "user_collision_fixture", CreatedAt: effective - 1}).Error; err != nil {
		t.Fatal(err)
	}
	if applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || !applied {
		t.Fatalf("activation applied=%v err=%v", applied, err)
	}
	var changed ChannelFinanceChannelCost
	if err := m.storeDB.First(&changed, "channel_id = ? AND grp = ?", 59, "codex-1.2x").Error; err != nil {
		t.Fatal(err)
	}
	if changed.Multiplier != 2.2 || changed.DiscountFactor != 0.5 {
		t.Fatalf("activation did not preserve discount decomposition: %+v", changed)
	}
	var versions []ChannelFinanceVersion
	if err := m.storeDB.Where("domain = ?", proposal.Domain).Order("version").Find(&versions).Error; err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 || versions[1].Version != 2 || versions[1].EffectiveAt != effective {
		t.Fatalf("activation version not exactly-once/effective at boundary: %+v", versions)
	}
	var appliedProposal ChannelPricingChangeProposal
	var appliedActivation ChannelFinanceActivation
	if err := m.storeDB.First(&appliedProposal, "proposal_key = ?", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&appliedActivation, "proposal_key = ? AND action = 'approve'", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	var slots int64
	_ = m.storeDB.Model(&ChannelFinanceActivationSlot{}).Count(&slots).Error
	var appliedEvents int64
	_ = m.storeDB.Model(&ChannelFinanceActivationEvent{}).Where("activation_id = ? AND event = 'applied'", appliedActivation.ActivationID).Count(&appliedEvents).Error
	if appliedProposal.Status != "applied" || appliedProposal.AppliedVersion != 2 || appliedActivation.Status != "applied" || appliedActivation.AppliedVersion != 2 || slots != 0 || appliedEvents != 1 {
		t.Fatalf("activation transaction not internally consistent proposal=%+v activation=%+v slots=%d events=%d", appliedProposal, appliedActivation, slots, appliedEvents)
	}
	if applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || applied {
		t.Fatalf("duplicate worker was not idempotent applied=%v err=%v", applied, err)
	}
	// Replay the exact approval after artificial activation remains idempotent.
	recorder, response := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK || response["idempotent"] != true {
		t.Fatalf("approval replay=%d %s", recorder.Code, recorder.Body.String())
	}
	approve.Reason = "different request"
	recorder, _ = decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("same idempotency key with different request=%d %s", recorder.Code, recorder.Body.String())
	}
	rollback := channelPricingProposalDecisionInput{Action: "rollback", ExpectedStatus: "applied", IdempotencyKey: "rollback-1", Reason: "operator rollback", EffectiveFrom: effective}
	recorder, _ = decisionRequest(t, m, proposal.ProposalKey, rollback)
	if recorder.Code != http.StatusOK {
		t.Fatalf("rollback schedule=%d %s", recorder.Code, recorder.Body.String())
	}
	if applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || !applied {
		t.Fatalf("rollback applied=%v err=%v", applied, err)
	}
	var restored ChannelFinanceChannelCost
	if err := m.storeDB.First(&restored, "channel_id = ? AND grp = ?", 59, "codex-1.2x").Error; err != nil {
		t.Fatal(err)
	}
	if restored.Multiplier != 1 || restored.DiscountFactor != 0.5 || restored.UpstreamGroupName != "Gpt-codex" {
		t.Fatalf("rollback did not restore exact row: %+v", restored)
	}
}

func TestFinanceActivationLaneRunsWhenAllUpstreamPollingIsOff(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	effective := nextWholeHour(time.Now().Unix())
	approve := channelPricingProposalDecisionInput{
		Action: "approve", ExpectedStatus: "pending", ExpectedBaseVersion: proposal.BaseVersion,
		ExpectedEvidenceDigest: proposal.EvidenceDigest, IdempotencyKey: "approve-ledger-off",
		Reason: "verified evidence", EffectiveFrom: effective,
	}
	recorder, _ := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("approve=%d %s", recorder.Code, recorder.Body.String())
	}
	var activation ChannelFinanceActivation
	if err := m.storeDB.First(&activation, "proposal_key = ?", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	due := time.Now().Unix() - 1
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&ChannelFinanceActivation{}).Where("activation_id = ?", activation.ActivationID).Update("effective_at", due).Error; err != nil {
			return err
		}
		return tx.Model(&ChannelFinanceActivationSlot{}).Where("activation_id = ?", activation.ActivationID).Update("effective_at", due).Error
	}); err != nil {
		t.Fatal(err)
	}
	m.cfg.UpstreamPricingLedgerEnabled = false
	m.cfg.UpstreamSyncEnabled = false
	m.cfg.UpstreamUsageSyncEnabled = false
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", proposal.Domain).
		Updates(map[string]any{"enabled": false, "usage_sync_enabled": false}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !m.startChannelFinanceActivationLane(ctx, 0) {
		t.Fatal("channel finance activation lane did not start")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var applied ChannelFinanceActivation
		if err := m.storeDB.First(&applied, "activation_id = ?", activation.ActivationID).Error; err != nil {
			t.Fatal(err)
		}
		if applied.Status == "applied" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("local due activation was gated by upstream polling: %+v", applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
}

func TestFinanceActivationLaneDrainsBoundedDueQueuePastOrphans(t *testing.T) {
	m, _ := newFinanceActivationFixture(t)
	now := time.Now().Unix()
	for i, domain := range []string{"orphan-a.example", "orphan-b.example", "orphan-c.example"} {
		activationID := strings.Repeat(string(rune('d'+i)), 64)
		if err := m.storeDB.Create(&ChannelFinanceActivationSlot{
			Domain: domain, ActivationID: activationID, EffectiveAt: now - 1, CreatedAt: now - int64(10-i),
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	m.syncDueChannelFinanceActivations(context.Background())
	var slots, events int64
	if err := m.storeDB.Model(&ChannelFinanceActivationSlot{}).Count(&slots).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelFinanceActivationEvent{}).Where("event = 'orphan_slot_removed'").Count(&events).Error; err != nil {
		t.Fatal(err)
	}
	if slots != 0 || events != 3 {
		t.Fatalf("bounded drain did not advance past orphan heads: slots=%d events=%d", slots, events)
	}
}

func TestFinanceActivationCorruptionIsIsolatedWithoutPartialWrite(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	effective := nextWholeHour(time.Now().Unix())
	approve := channelPricingProposalDecisionInput{Action: "approve", ExpectedStatus: "pending", ExpectedBaseVersion: proposal.BaseVersion, ExpectedEvidenceDigest: proposal.EvidenceDigest, IdempotencyKey: "approve-corrupt", Reason: "test", EffectiveFrom: effective}
	recorder, _ := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("approve=%d %s", recorder.Code, recorder.Body.String())
	}
	if err := m.storeDB.Model(&ChannelFinanceActivation{}).Where("proposal_key = ?", proposal.ProposalKey).Update("target_snapshot_hash", strings.Repeat("f", 64)).Error; err != nil {
		t.Fatal(err)
	}
	if applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || applied {
		t.Fatalf("corrupt task applied=%v err=%v", applied, err)
	}
	var row ChannelFinanceChannelCost
	_ = m.storeDB.First(&row, "channel_id = ? AND grp = ?", 59, "codex-1.2x").Error
	if row.Multiplier != 1 || row.DiscountFactor != 0.5 {
		t.Fatalf("corrupt task partially changed finance row: %+v", row)
	}
	var activation ChannelFinanceActivation
	if err := m.storeDB.First(&activation, "proposal_key = ?", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	if activation.Status != "conflict" || activation.LastError == "" {
		t.Fatalf("corrupt task not made visible: %+v", activation)
	}
	var slots int64
	_ = m.storeDB.Model(&ChannelFinanceActivationSlot{}).Count(&slots).Error
	if slots != 0 {
		t.Fatalf("corrupt task still blocks queue: slots=%d", slots)
	}
}

func TestFinanceActivationOrphanSlotIsRemoved(t *testing.T) {
	m, _ := newFinanceActivationFixture(t)
	now := time.Now().Unix()
	if err := m.storeDB.Create(&ChannelFinanceActivationSlot{Domain: "4sapi.com", ActivationID: strings.Repeat("a", 64), EffectiveAt: now - 1, CreatedAt: now - 2}).Error; err != nil {
		t.Fatal(err)
	}
	if applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), now); err != nil || applied {
		t.Fatalf("orphan cleanup applied=%v err=%v", applied, err)
	}
	var slots int64
	_ = m.storeDB.Model(&ChannelFinanceActivationSlot{}).Count(&slots).Error
	if slots != 0 {
		t.Fatalf("orphan slot remains: %d", slots)
	}
}

func TestFinanceActivationSQLiteBusyRemainsScheduledThenAppliesOnce(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	effective := nextWholeHour(time.Now().Unix())
	approve := channelPricingProposalDecisionInput{Action: "approve", ExpectedStatus: "pending", ExpectedBaseVersion: proposal.BaseVersion, ExpectedEvidenceDigest: proposal.EvidenceDigest, IdempotencyKey: "approve-busy", Reason: "test", EffectiveFrom: effective}
	recorder, _ := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("approve=%d %s", recorder.Code, recorder.Body.String())
	}
	dbPath := t.TempDir() + "/activation-busy.db"
	if err := m.storeDB.Exec("VACUUM INTO ?", dbPath).Error; err != nil {
		t.Fatal(err)
	}
	dsn := dbPath + "?_pragma=busy_timeout(100)&_pragma=journal_mode(WAL)"
	busyStore, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	busySQL, err := busyStore.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer busySQL.Close()
	busySQL.SetMaxOpenConns(1)
	lockDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer lockDB.Close()
	conn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	busyWorker := &Monitor{storeDB: busyStore, cfg: m.cfg}
	start := time.Now()
	if applied, err := busyWorker.applyOneDueChannelFinanceActivation(context.Background(), effective); err == nil || applied {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		t.Fatalf("busy activation applied=%v err=%v", applied, err)
	}
	if time.Since(start) > 3*time.Second {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		t.Fatalf("busy activation exceeded bounded attempt: %s", time.Since(start))
	}
	if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	var current ChannelFinanceChannelCost
	if err := busyStore.First(&current, "channel_id = ? AND grp = ?", 59, "codex-1.2x").Error; err != nil {
		t.Fatal(err)
	}
	var activation ChannelFinanceActivation
	if err := busyStore.First(&activation, "proposal_key = ?", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	var versions, slots int64
	_ = busyStore.Model(&ChannelFinanceVersion{}).Where("domain = ?", proposal.Domain).Count(&versions).Error
	_ = busyStore.Model(&ChannelFinanceActivationSlot{}).Count(&slots).Error
	if current.Multiplier != 1 || activation.Status != "scheduled" || versions != 1 || slots != 1 {
		t.Fatalf("busy attempt changed durable state row=%+v activation=%+v versions=%d slots=%d", current, activation, versions, slots)
	}
	// A newly constructed worker over the same durable store represents process
	// restart; it must finish the pending task exactly once after the lock clears.
	restarted := &Monitor{storeDB: busyStore, cfg: m.cfg}
	if applied, err := restarted.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || !applied {
		t.Fatalf("restart activation applied=%v err=%v", applied, err)
	}
	if applied, err := restarted.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || applied {
		t.Fatalf("restart duplicate applied=%v err=%v", applied, err)
	}
	_ = busyStore.Model(&ChannelFinanceVersion{}).Where("domain = ?", proposal.Domain).Count(&versions).Error
	if versions != 2 {
		t.Fatalf("busy recovery versions=%d, want exactly 2", versions)
	}
}

func TestFinanceActivationVisibleAndCancellableAfterFeatureDisabled(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	effective := nextWholeHour(time.Now().Unix())
	approve := channelPricingProposalDecisionInput{Action: "approve", ExpectedStatus: "pending", ExpectedBaseVersion: proposal.BaseVersion, ExpectedEvidenceDigest: proposal.EvidenceDigest, IdempotencyKey: "approve-cancel", Reason: "test", EffectiveFrom: effective}
	recorder, _ := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("approve=%d %s", recorder.Code, recorder.Body.String())
	}
	var activation ChannelFinanceActivation
	if err := m.storeDB.First(&activation, "proposal_key = ?", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	listRouter := gin.New()
	listRouter.GET("/proposals", m.listChannelPricingProposalsHandler)
	listRecorder := httptest.NewRecorder()
	listRouter.ServeHTTP(listRecorder, httptest.NewRequest(http.MethodGet, "/proposals?domain=4sapi.com", nil))
	if listRecorder.Code != http.StatusOK || !strings.Contains(listRecorder.Body.String(), activation.ActivationID) || !strings.Contains(listRecorder.Body.String(), `"status":"scheduled"`) {
		t.Fatalf("scheduled activation not visible after reload: %d %s", listRecorder.Code, listRecorder.Body.String())
	}
	m.cfg.ChannelCostClosureEnabled = false
	listRecorder = httptest.NewRecorder()
	listRouter.ServeHTTP(listRecorder, httptest.NewRequest(http.MethodGet, "/proposals?domain=4sapi.com", nil))
	if listRecorder.Code != http.StatusOK || !strings.Contains(listRecorder.Body.String(), activation.ActivationID) || !strings.Contains(listRecorder.Body.String(), `"status":"scheduled"`) {
		t.Fatalf("scheduled activation must remain visible through the disabled safety gate: %d %s", listRecorder.Code, listRecorder.Body.String())
	}
	if applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || applied {
		t.Fatalf("disabled safety gate must freeze due activation: applied=%v err=%v", applied, err)
	}
	cancelRecorder := cancelActivationRequest(t, m, activation.ActivationID, "disable rollout")
	if cancelRecorder.Code != http.StatusOK {
		t.Fatalf("cancel while disabled=%d %s", cancelRecorder.Code, cancelRecorder.Body.String())
	}
	var slots int64
	_ = m.storeDB.Model(&ChannelFinanceActivationSlot{}).Count(&slots).Error
	if slots != 0 {
		t.Fatalf("cancel did not release slot: %d", slots)
	}
	listRecorder = httptest.NewRecorder()
	listRouter.ServeHTTP(listRecorder, httptest.NewRequest(http.MethodGet, "/proposals?domain=4sapi.com", nil))
	if listRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled domain without a recovery task must remain closed: %d %s", listRecorder.Code, listRecorder.Body.String())
	}
	raw, err := currentChannelFinanceVersionJSON(m.storeDB, proposal.Domain)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := appendChannelFinanceVersion(m.storeDB, proposal.Domain, raw, time.Now().Unix(), "root"); err != nil {
		t.Fatalf("manual finance remains blocked after cancellation: %v", err)
	}

	// Re-scheduling in the same second must display the active slot, not the
	// cancelled activation selected by a random hash tie-breaker.
	m.cfg.ChannelCostClosureEnabled = true
	approve.IdempotencyKey = "approve-after-cancel"
	recorder, _ = decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("reschedule=%d %s", recorder.Code, recorder.Body.String())
	}
	var activeSlot ChannelFinanceActivationSlot
	if err := m.storeDB.First(&activeSlot, "domain = ?", proposal.Domain).Error; err != nil {
		t.Fatal(err)
	}
	listRecorder = httptest.NewRecorder()
	listRouter.ServeHTTP(listRecorder, httptest.NewRequest(http.MethodGet, "/proposals?domain=4sapi.com", nil))
	if listRecorder.Code != http.StatusOK || !strings.Contains(listRecorder.Body.String(), activeSlot.ActivationID) || !strings.Contains(listRecorder.Body.String(), `"status":"scheduled"`) {
		t.Fatalf("list did not prefer active same-second slot: %d %s", listRecorder.Code, listRecorder.Body.String())
	}
	m.cfg.ChannelCostClosureDomains = nil
	listRecorder = httptest.NewRecorder()
	listRouter.ServeHTTP(listRecorder, httptest.NewRequest(http.MethodGet, "/proposals?domain=4sapi.com", nil))
	if listRecorder.Code != http.StatusOK || !strings.Contains(listRecorder.Body.String(), activeSlot.ActivationID) {
		t.Fatalf("scheduled activation must remain visible after domain rollout removal: %d %s", listRecorder.Code, listRecorder.Body.String())
	}
	cancelRecorder = cancelActivationRequest(t, m, activeSlot.ActivationID, "remove domain rollout")
	if cancelRecorder.Code != http.StatusOK {
		t.Fatalf("cancel after domain rollout removal=%d %s", cancelRecorder.Code, cancelRecorder.Body.String())
	}
}

func TestFinanceActivationFailsClosedAfterDomainRemovedFromRollout(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	effective := nextWholeHour(time.Now().Unix())
	approve := channelPricingProposalDecisionInput{Action: "approve", ExpectedStatus: "pending", ExpectedBaseVersion: proposal.BaseVersion, ExpectedEvidenceDigest: proposal.EvidenceDigest, IdempotencyKey: "approve-remove-domain", Reason: "test", EffectiveFrom: effective}
	recorder, _ := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("approve=%d %s", recorder.Code, recorder.Body.String())
	}
	var before ChannelFinanceChannelCost
	if err := m.storeDB.Where("channel_id = ? AND grp = ?", proposal.LocalChannelID, "codex-1.2x").First(&before).Error; err != nil {
		t.Fatal(err)
	}
	var versionsBefore int64
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain = ?", proposal.Domain).Count(&versionsBefore).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.ChannelCostClosureDomains = nil
	applied, err := m.applyOneDueChannelFinanceActivation(context.Background(), effective)
	if err != nil || applied {
		t.Fatalf("removed rollout domain must fail closed: applied=%v err=%v", applied, err)
	}
	var activation ChannelFinanceActivation
	if err := m.storeDB.First(&activation, "proposal_key = ?", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	if activation.Status != "conflict" || !strings.Contains(activation.LastError, "白名单") {
		t.Fatalf("activation did not record rollout conflict: %+v", activation)
	}
	var currentProposal ChannelPricingChangeProposal
	if err := m.storeDB.First(&currentProposal, "proposal_key = ?", proposal.ProposalKey).Error; err != nil {
		t.Fatal(err)
	}
	if currentProposal.Status != "conflict" {
		t.Fatalf("proposal status=%s, want conflict", currentProposal.Status)
	}
	var after ChannelFinanceChannelCost
	if err := m.storeDB.Where("channel_id = ? AND grp = ?", proposal.LocalChannelID, "codex-1.2x").First(&after).Error; err != nil {
		t.Fatal(err)
	}
	if before.Multiplier != after.Multiplier || before.DiscountFactor != after.DiscountFactor || before.EffectiveAt != after.EffectiveAt {
		t.Fatalf("removed rollout domain mutated finance row: before=%+v after=%+v", before, after)
	}
	var versionsAfter, slots int64
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain = ?", proposal.Domain).Count(&versionsAfter).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelFinanceActivationSlot{}).Where("domain = ?", proposal.Domain).Count(&slots).Error; err != nil {
		t.Fatal(err)
	}
	if versionsAfter != versionsBefore || slots != 0 {
		t.Fatalf("removed rollout conflict versions=%d want=%d slots=%d want=0", versionsAfter, versionsBefore, slots)
	}
}

func TestPendingFinanceActivationSurvivesVerifiedDualStoreBackupRestore(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	effective := nextWholeHour(time.Now().Unix())
	approve := channelPricingProposalDecisionInput{Action: "approve", ExpectedStatus: "pending", ExpectedBaseVersion: proposal.BaseVersion, ExpectedEvidenceDigest: proposal.EvidenceDigest, IdempotencyKey: "approve-backup", Reason: "test", EffectiveFrom: effective}
	recorder, _ := decisionRequest(t, m, proposal.ProposalKey, approve)
	if recorder.Code != http.StatusOK {
		t.Fatalf("approve=%d %s", recorder.Code, recorder.Body.String())
	}
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "monitor.db")
	factsPath := filepath.Join(dir, "usage-facts.db")
	if err := m.storeDB.Exec("VACUUM INTO ?", mainPath).Error; err != nil {
		t.Fatal(err)
	}
	facts, err := sql.Open("sqlite", factsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := facts.Exec("CREATE TABLE backup_guard (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
		_ = facts.Close()
		t.Fatal(err)
	}
	if _, err := facts.Exec("INSERT INTO backup_guard(id,value) VALUES(1,'facts')"); err != nil {
		_ = facts.Close()
		t.Fatal(err)
	}
	if err := facts.Close(); err != nil {
		t.Fatal(err)
	}
	backupMonitor := &Monitor{cfg: Settings{
		StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: filepath.Join(dir, "backups"),
		StoreMigrationBackupRetention: 3,
	}}
	snapshot, err := backupMonitor.createPreMigrationSnapshot(context.Background(), mainPath, factsPath, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	restoreDir := filepath.Join(dir, "restored")
	if err := RestorePreMigrationSnapshot(context.Background(), snapshot.SnapshotDir, restoreDir); err != nil {
		t.Fatal(err)
	}
	restoredMain := filepath.Join(restoreDir, filepath.Base(mainPath))
	restoredStore, err := gorm.Open(sqlite.Open(restoredMain+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	restoredSQL, err := restoredStore.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer restoredSQL.Close()
	restored := &Monitor{storeDB: restoredStore, cfg: m.cfg}
	var pending ChannelFinanceActivation
	if err := restoredStore.First(&pending, "proposal_key = ? AND status = 'scheduled'", proposal.ProposalKey).Error; err != nil {
		t.Fatalf("pending activation missing after restore: %v", err)
	}
	if applied, err := restored.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || !applied {
		t.Fatalf("restored worker applied=%v err=%v", applied, err)
	}
	if applied, err := restored.applyOneDueChannelFinanceActivation(context.Background(), effective); err != nil || applied {
		t.Fatalf("restored duplicate worker applied=%v err=%v", applied, err)
	}
	var versions int64
	_ = restoredStore.Model(&ChannelFinanceVersion{}).Where("domain = ?", proposal.Domain).Count(&versions).Error
	var row ChannelFinanceChannelCost
	_ = restoredStore.First(&row, "channel_id = ? AND grp = ?", 59, "codex-1.2x").Error
	if versions != 2 || row.Multiplier != 2.2 || row.DiscountFactor != 0.5 {
		t.Fatalf("restored activation not exactly once versions=%d row=%+v", versions, row)
	}
	restoredFacts := filepath.Join(restoreDir, filepath.Base(factsPath))
	factsCheck, err := sql.Open("sqlite", sqliteReadOnlyDSN(restoredFacts))
	if err != nil {
		t.Fatal(err)
	}
	defer factsCheck.Close()
	var guard string
	if err := factsCheck.QueryRow("SELECT value FROM backup_guard WHERE id=1").Scan(&guard); err != nil || guard != "facts" {
		t.Fatalf("facts store not restored with main store guard=%q err=%v", guard, err)
	}
}

func TestChannelCostDecisionRoutesRequireRealRootSession(t *testing.T) {
	m, proposal := newFinanceActivationFixture(t)
	authServer := mockNewAPI()
	defer authServer.Close()
	m.cfg.NewAPIBaseURL = authServer.URL
	m.cfg.SessionSecret = "channel-cost-route-secret"
	gin.SetMode(gin.TestMode)
	router := gin.New()
	m.RegisterRoutes(router)
	login := func(user string) *http.Cookie {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"`+user+`","password":"good"}`))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("login %s=%d %s", user, recorder.Code, recorder.Body.String())
		}
		for _, cookie := range recorder.Result().Cookies() {
			if cookie.Name == sessionCookie {
				return cookie
			}
		}
		t.Fatalf("login %s returned no session", user)
		return nil
	}
	adminCookie, rootCookie := login("admin"), login("root")
	request := func(method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Accept", "application/json")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if cookie != nil {
			req.AddCookie(cookie)
		}
		router.ServeHTTP(recorder, req)
		return recorder
	}
	routes := []struct {
		method, path, body string
		rootStatus         int
	}{
		{http.MethodGet, "/channels/cost/sources?domain=" + proposal.Domain, "", http.StatusOK},
		{http.MethodPost, "/channels/cost/bindings", `{}`, http.StatusBadRequest},
		{http.MethodGet, "/channels/cost/proposals?domain=" + proposal.Domain, "", http.StatusOK},
		{http.MethodGet, "/channels/cost/proposals/" + proposal.ProposalKey + "/impact", "", http.StatusOK},
		{http.MethodPost, "/channels/cost/proposals/invalid/decisions", `{}`, http.StatusBadRequest},
		{http.MethodPost, "/channels/cost/activations/invalid/cancel", `{}`, http.StatusBadRequest},
	}
	for _, route := range routes {
		if recorder := request(route.method, route.path, route.body, nil); recorder.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated %s %s=%d %s", route.method, route.path, recorder.Code, recorder.Body.String())
		}
		if recorder := request(route.method, route.path, route.body, adminCookie); recorder.Code != http.StatusForbidden {
			t.Fatalf("admin accessed root route %s %s: %d %s", route.method, route.path, recorder.Code, recorder.Body.String())
		}
		if recorder := request(route.method, route.path, route.body, rootCookie); recorder.Code != route.rootStatus {
			t.Fatalf("root route %s %s=%d want=%d %s", route.method, route.path, recorder.Code, route.rootStatus, recorder.Body.String())
		}
	}
	if recorder := request(http.MethodGet, "/channels/cost/proposals?domain="+proposal.Domain, "", rootCookie); !strings.Contains(recorder.Body.String(), proposal.ProposalKey) {
		t.Fatalf("root proposal route omitted data: %s", recorder.Body.String())
	}
}
