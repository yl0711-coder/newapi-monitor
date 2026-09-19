package monitor

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
)

func TestFetchFinanceInternalAccountFactsKeepsChannelAndGroupDimensions(t *testing.T) {
	db, err := sql.Open("sqlite", "file:finance-internal-source?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE logs (created_at INTEGER,user_id INTEGER,channel_id INTEGER,type INTEGER,quota INTEGER,prompt_tokens INTEGER,completion_tokens INTEGER,`group` TEXT,token_id INTEGER,token_name TEXT,content TEXT,request_id TEXT);" +
		"INSERT INTO logs VALUES (100,7,5,2,500000,10,5,'paid',1,'customer','','r1'),(200,7,6,2,250000,20,6,'test',2,'customer','','r2'),(300,8,5,2,900000,30,7,'paid',3,'customer','','r3')"); err != nil {
		t.Fatal(err)
	}
	rows, err := fetchFinanceInternalAccountHourFacts(context.Background(), db, 0, 3600, []int64{7})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].UserID != 7 || rows[0].ChannelID != 5 || rows[0].Grp != "paid" || rows[0].Tokens != 15 || rows[0].ConsumeQuota != 500000 || rows[1].ChannelID != 6 || rows[1].Grp != "test" || rows[1].Tokens != 26 {
		t.Fatalf("unexpected internal facts: %+v", rows)
	}
}

func TestResolveFinanceInternalAccountsUsesStableUserIDAndRejectsAmbiguousName(t *testing.T) {
	m := newStabilityTestMonitor(t)
	if err := m.storeDB.Create(&[]UserDirectoryEntry{
		{UserID: 7, Username: "internal-a", SyncedAt: 1},
		{UserID: 8, Username: "duplicate", SyncedAt: 1},
		{UserID: 9, Username: "duplicate", SyncedAt: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := m.resolveFinanceInternalAccounts(context.Background(), "internal-a, #7")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].UserID != 7 || rows[0].Username != "internal-a" {
		t.Fatalf("unexpected resolved accounts: %+v", rows)
	}
	if _, err := m.resolveFinanceInternalAccounts(context.Background(), "duplicate"); err == nil || !strings.Contains(err.Error(), "多个用户 ID") {
		t.Fatalf("ambiguous username was accepted: %v", err)
	}
	rows, err = m.resolveFinanceInternalAccounts(context.Background(), "1")
	if err != nil || len(rows) != 1 || rows[0].UserID != 1 || rows[0].Username != "" {
		t.Fatalf("explicit id missing from an offline directory must remain configurable: rows=%+v err=%v", rows, err)
	}
}

func TestFinanceUserFactsExcludeConfiguredInternalAccountAndUncheckedGroup(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "example.test")
	hour := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 5, Name: "channel", BaseDomain: "example.test", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range []StabilityHourSample{
		{HourTs: hour, ChannelID: 5, ModelName: "m", Grp: "paid", Success: 3, Quota: 3_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
		{HourTs: hour, ChannelID: 5, ModelName: "m", Grp: "test-only", Success: 1, Quota: 8_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
	} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 4, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	internal := financeConfiguredInternalEvidence{Complete: true, Requests: 1, NetQuota: 1_000_000, Rows: []FinanceInternalAccountHourFact{{HourTs: hour, UserID: 7, ChannelID: 5, Grp: "paid", Requests: 1, ConsumeQuota: 1_000_000}}}
	statement, coverage, domains, err := m.loadFinanceUserFacts(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, hour+7200, internal, map[string]bool{"test-only": false})
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.Complete || statement.KnownUserConsumption.MicroUSD != "4000000" || statement.InternalTestConsumption.MicroUSD != "2000000" || statement.InternalTestRequests != 1 {
		t.Fatalf("unexpected internal-account exclusion: statement=%+v coverage=%+v", statement, coverage)
	}
	if got := domains["example.test"]; got.ConsumeQuota != 2_000_000 || got.Requests != 2 {
		t.Fatalf("domain total was not conserved: %+v", got)
	}
}

func TestConfiguredInternalCostOnlyExcludesPureChannelHour(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "example.test")
	hour := int64(1_788_195_600)
	publication := insertEconomicsReportHour(t, m, "example.test", strings.Repeat("b", 64), hour, 5, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", publication.PublicationID).Updates(map[string]any{"local_requests": int64(2), "local_consume_quota": int64(1_000_000), "local_net_quota": int64(1_000_000)}).Error; err != nil {
		t.Fatal(err)
	}
	insertEconomicsReportManifest(t, m, "example.test", strings.Repeat("b", 64), hour, publication)
	evidence, err := m.loadFinanceInternalTestCostEvidence(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, financeConfiguredInternalEvidence{Complete: true, Rows: []FinanceInternalAccountHourFact{{HourTs: hour, UserID: 7, ChannelID: 5, Grp: "paid", Requests: 2, ConsumeQuota: 1_000_000}}})
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.Complete || evidence.StrictPairs != 1 || evidence.MixedPairs != 0 || evidence.Total.CorrectedCostMicroUSD != 1_000_000 || evidence.ExcludeByDomain["example.test"].RevenueMicroUSD != 2_000_000 {
		t.Fatalf("pure internal channel hour was not classified exactly: %+v", evidence)
	}
}

func TestChannelManagementFiltersConfiguredInternalUsageAndStrictCost(t *testing.T) {
	evidence := financeConfiguredInternalEvidence{
		Complete: true,
		Accounts: 8,
		Rows: []FinanceInternalAccountHourFact{
			{HourTs: 100, UserID: 1, ChannelID: 5, Grp: "paid", Requests: 2, Tokens: 30, ConsumeQuota: 700_000},
			{HourTs: 100, UserID: 2, ChannelID: 5, Grp: "paid", Requests: 1, Tokens: 20, ConsumeQuota: 300_000},
		},
	}
	usage, err := channelInternalUsageByChannelGroup(evidence)
	if err != nil {
		t.Fatal(err)
	}
	fact := usage[channelInternalUsageKey{ChannelID: 5, Group: "paid"}]
	if fact.Requests != 3 || fact.Tokens != 50 || fact.Quota != 1_000_000 {
		t.Fatalf("unexpected configured-account usage: %+v", fact)
	}
	row := channelManagementUsageRow{ChannelID: 5, Grp: "paid", Requests: 10, Tokens: 500, Quota: 5_000_000}
	if err := subtractChannelInternalUsage(&row, fact); err != nil {
		t.Fatal(err)
	}
	if row.Requests != 7 || row.Tokens != 450 || row.Quota != 4_000_000 {
		t.Fatalf("configured-account usage was not excluded: %+v", row)
	}

	metrics := ChannelUpstreamUsageMetrics{
		Available: true, CostUSD: 10,
		AdjustedCostAvailable: true, AdjustedCostUSD: 5,
	}
	filtered := applyChannelInternalCostFilter(metrics, financeInternalTestCostFact{
		UpstreamCostMicroUSD: 2_000_000, CorrectedCostMicroUSD: 1_000_000,
	}, 8, true)
	if !filtered.BusinessCostAvailable || filtered.BusinessCostUSD != 8 ||
		!filtered.BusinessAdjustedCostAvailable || filtered.BusinessAdjustedCostUSD != 4 ||
		filtered.InternalExcludedCostUSD != 2 || filtered.InternalExcludedAdjustedUSD != 1 ||
		filtered.InternalFilterStatus != "complete" {
		t.Fatalf("unexpected strict upstream cost filter: %+v", filtered)
	}

	backfilling := applyChannelInternalCostFilter(metrics, financeInternalTestCostFact{}, 8, false)
	if backfilling.BusinessCostAvailable || backfilling.BusinessAdjustedCostAvailable || backfilling.InternalFilterStatus != "backfilling" {
		t.Fatalf("incomplete evidence must fail closed: %+v", backfilling)
	}
}

func TestInternalCostEvidenceFailureIsIsolatedByDomain(t *testing.T) {
	evidence := financeInternalTestCostEvidence{
		SourceComplete: true,
		Events: []financeInternalTestCostEvent{
			{Domain: "bad.example", State: "mixed"},
			{Domain: "good.example", State: "strict"},
		},
	}
	if financeInternalCostDomainComplete(evidence, "bad.example") {
		t.Fatal("mixed domain must fail closed")
	}
	if !financeInternalCostDomainComplete(evidence, "good.example") {
		t.Fatal("one mixed domain must not blank an unrelated verified domain")
	}
	evidence.Events = append(evidence.Events, financeInternalTestCostEvent{State: "unverified"})
	if financeInternalCostDomainComplete(evidence, "good.example") {
		t.Fatal("an unowned unverified pair must remain a global blocker")
	}
}

func TestFinanceFactWakeInterruptsIdleWait(t *testing.T) {
	m := &Monitor{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- m.waitFinanceFactsSync(ctx, time.Hour) }()
	m.notifyFinanceFactsSync()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("wake should resume the finance worker")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("finance worker did not wake promptly")
	}
}

func TestFinanceInternalFactConfigChangeKeepsCurrentAccountsUntilAtomicReplacement(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.FinanceStartDate = "2026-05-01"
	m.cfg.UsageFactsHistorySourceEpoch = "epoch-a"
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	oldAccounts := []FinanceInternalAccount{{UserID: 1}, {UserID: 2}}
	if err := m.usageFactsStore().Create(&FinanceInternalAccountFactState{
		ID: financeInternalFactStateID, ConfigHash: financeInternalAccountHash(oldAccounts), SourceEpoch: "epoch-a",
		StartHourTs: start, NextHourTs: start + 86400, Status: "running",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&[]FinanceInternalAccountHourFact{
		{HourTs: start, UserID: 1, ChannelID: 10, Grp: "paid", Requests: 1},
		{HourTs: start, UserID: 2, ChannelID: 20, Grp: "paid", Requests: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	state, err := m.financeInternalFactState(context.Background(), []FinanceInternalAccount{{UserID: 1}}, "epoch-a")
	if err != nil {
		t.Fatal(err)
	}
	if state.NextHourTs != start || state.Status != "pending" {
		t.Fatalf("state was not reset for rebuild: %+v", state)
	}
	var rows []FinanceInternalAccountHourFact
	if err := m.usageFactsStore().Order("user_id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].UserID != 1 {
		t.Fatalf("current-account facts should remain while removed accounts are pruned: %+v", rows)
	}
}

func TestConfiguredAccountCostEvidenceDoesNotDependOnAutomaticProbePairs(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "example.test")
	hour := int64(1_788_195_600)
	publication := insertEconomicsReportHour(t, m, "example.test", strings.Repeat("c", 64), hour, 5, 2_000_000, 1_000_000, 1_000_000, 1_000_000)
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", publication.PublicationID).
		Updates(map[string]any{"local_requests": int64(2), "local_consume_quota": int64(1_000_000), "local_net_quota": int64(1_000_000)}).Error; err != nil {
		t.Fatal(err)
	}
	insertEconomicsReportManifest(t, m, "example.test", strings.Repeat("c", 64), hour, publication)
	if err := m.storeDB.Create(&ChannelTestHourSample{HourTs: hour, ChannelID: 99, Requests: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	evidence, err := m.loadFinanceConfiguredAccountCostEvidence(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, financeConfiguredInternalEvidence{
		Complete: true,
		Rows:     []FinanceInternalAccountHourFact{{HourTs: hour, UserID: 1, ChannelID: 5, Grp: "paid", Requests: 2, ConsumeQuota: 1_000_000}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.Complete || evidence.TestPairs != 1 || evidence.StrictPairs != 1 || evidence.UnverifiedPairs != 0 {
		t.Fatalf("automatic probe pair leaked into configured-account evidence: %+v", evidence)
	}
}

func TestChannelManagementReportExcludesConfiguredInternalAccountUsage(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.FinanceStartDate = "2026-05-01"
	hour := time.Now().Add(-4 * time.Hour).Truncate(time.Hour).Unix()
	if err := m.storeDB.Create(&ChannelSnap{ID: 5, Name: "channel", BaseDomain: "example.test", Status: 1, Groups: "paid"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: hour, ChannelID: 5, ModelName: "m", Grp: "paid", Success: 5,
		Tokens: 500, Quota: 5_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 5, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	accounts := []FinanceInternalAccount{{UserID: 1, CreatedAt: hour, UpdatedAt: hour}}
	if err := m.storeDB.Create(&accounts).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&FinanceInternalAccountHourFact{
		HourTs: hour, UserID: 1, ChannelID: 5, Grp: "paid", Requests: 2, Tokens: 200, ConsumeQuota: 2_000_000,
	}).Error; err != nil {
		t.Fatal(err)
	}
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	finalized := m.usageFactFinalizedHour(time.Now())
	if err := m.usageFactsStore().Create(&FinanceInternalAccountFactState{
		ID: financeInternalFactStateID, ConfigHash: financeInternalAccountHash(accounts), StartHourTs: start,
		NextHourTs: finalized, LastCompletedHour: finalized - 3600, Status: "caught_up", UpdatedAt: time.Now().Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, hour+7200)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Usage.Requests != 3 || report.Summary.Usage.Tokens != 300 || report.Summary.Usage.CostUSD != 6 {
		t.Fatalf("configured internal account remained in channel report: %+v", report.Summary.Usage)
	}
	if report.InternalAccounts.Status != "caught_up" {
		t.Fatalf("unexpected internal-account status: %+v", report.InternalAccounts)
	}
}

func TestExcludeFinanceGiftUsersPreventsDoubleDeduction(t *testing.T) {
	result := financeGiftAllocationResult{Coverage: financeGiftCoverageView{FromTs: 0, ToTs: 3600, Complete: true}, ledger: []financecredit.LedgerEvent{
		{UserID: 7, At: 10, Sequence: 1, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 2_000_000},
		{UserID: 7, At: 20, Sequence: 2, Kind: financecredit.EventNetUsage, AmountMicroUSD: 1_000_000},
		{UserID: 8, At: 10, Sequence: 3, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 2_000_000},
		{UserID: 8, At: 20, Sequence: 4, Kind: financecredit.EventNetUsage, AmountMicroUSD: 500_000},
	}}
	filtered, err := excludeFinanceGiftUsers(result, map[int64]bool{7: true})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Allocation.PeriodGiftConsumptionMicroUSD != 500_000 || filtered.Coverage.GiftUsers != 1 || filtered.Coverage.EligibleGrants != 1 {
		t.Fatalf("internal gift usage was still deducted: %+v", filtered)
	}
}
