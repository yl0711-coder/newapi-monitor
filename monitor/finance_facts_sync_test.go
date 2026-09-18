package monitor

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func financeSyncSource(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/finance-sync-source.db")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE logs(
		id INTEGER PRIMARY KEY,user_id INTEGER,created_at INTEGER,type INTEGER,quota INTEGER,
		token_id INTEGER,token_name TEXT,request_id TEXT,content TEXT,other TEXT);
		CREATE TABLE users(id INTEGER PRIMARY KEY,created_at INTEGER);`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestFinanceSnapshotRecipientIDsKeepsExistingAndEarliestNewGrant(t *testing.T) {
	events := []FinanceCreditEvent{
		{SourceLogID: 12, TargetUserID: 8, EventAt: 3_700, EligibleTrial: true},
		{SourceLogID: 11, TargetUserID: 8, EventAt: 3_650, EligibleTrial: true},
		{SourceLogID: 13, TargetUserID: 9, EventAt: 3_800},
	}
	ids, recipients, err := financeSnapshotRecipientIDs([]int64{7}, financeCreditHourFetch{Events: events})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 7 || ids[1] != 8 {
		t.Fatalf("recipient ids=%v", ids)
	}
	if row := recipients[8]; row.FirstGrantAt != 3_650 || row.FirstGrantHour != 3_600 || row.SourceLogID != 11 {
		t.Fatalf("recipient=%+v", row)
	}
}

func TestCollectAndPublishFinanceHourUsesOneCompleteSnapshot(t *testing.T) {
	hour := int64(1_788_195_600)
	source := financeSyncSource(t)
	if _, err := source.Exec(`INSERT INTO users(id,created_at) VALUES(7,?)`, hour-60); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,token_id,token_name,request_id,content,other) VALUES
		(1,7,?,3,0,0,'','','管理员增加用户额度 ＄100.000000 额度',''),
		(2,7,?,2,500000,1,'customer','req-secret','secret prompt',''),
		(3,7,?,6,100000,1,'customer','req-secret','refund','')`, hour+10, hour+20, hour+30); err != nil {
		t.Fatal(err)
	}
	m := newStabilityTestMonitor(t)
	m.prodDB = source
	m.cfg.UsageFactsQueryTimeoutSec = 20
	snapshot, recipients, err := m.collectFinanceHourSnapshot(context.Background(), hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.UserSourceRows != 2 || len(snapshot.UserFacts) != 1 || len(snapshot.Credit.Events) != 1 || len(snapshot.RecipientIDs) != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if len(snapshot.BoundaryByUserID[7]) != 2 || len(recipients) != 1 {
		t.Fatalf("boundaries=%+v recipients=%+v", snapshot.BoundaryByUserID, recipients)
	}
	if err := m.publishFinanceHourSnapshot(context.Background(), hour, time.Now().Unix(), "source-v1", snapshot, recipients); err != nil {
		t.Fatal(err)
	}
	var userState FinanceUserHourState
	if err := m.usageFactsStore().First(&userState, "hour_ts=?", hour).Error; err != nil {
		t.Fatal(err)
	}
	var creditState FinanceCreditHourState
	if err := m.usageFactsStore().First(&creditState, "hour_ts=?", hour).Error; err != nil {
		t.Fatal(err)
	}
	var boundaryState FinanceGiftBoundaryState
	if err := m.usageFactsStore().First(&boundaryState, "source_epoch=? AND hour_ts=? AND user_id=?", "source-v1", hour, 7).Error; err != nil {
		t.Fatal(err)
	}
	if userState.Status != "complete" || creditState.Status != "complete" || boundaryState.Status != "complete" || boundaryState.Rows != 2 {
		t.Fatalf("user=%+v credit=%+v boundary=%+v", userState, creditState, boundaryState)
	}
	var recipient FinanceGiftRecipient
	if err := m.usageFactsStore().First(&recipient, "source_epoch=? AND user_id=?", "source-v1", 7).Error; err != nil {
		t.Fatal(err)
	}
	if recipient.FirstGrantAt != hour+10 || recipient.FirstGrantHour != hour {
		t.Fatalf("recipient=%+v", recipient)
	}
}

func TestPublishFinanceHourRejectsIncompleteCreditProof(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := int64(3_600)
	event := FinanceCreditEvent{SourceLogID: 1, EventAt: hour + 1, TargetUserID: 7,
		Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000,
		Cohort: "registration_time_unknown"}
	event.EvidenceHash = financeCreditEventHash(event)
	snapshot := financeHourSnapshot{
		Credit: financeCreditHourFetch{Events: []FinanceCreditEvent{event}, SourceRows: 1, UnknownRegistrationRows: 1},
	}
	if err := m.publishFinanceHourSnapshot(context.Background(), hour, 10_000, "source-v1", snapshot, nil); err == nil {
		t.Fatal("an incomplete credit proof must not become a completed finance hour")
	}
	var states int64
	if err := m.usageFactsStore().Model(&FinanceCreditHourState{}).Where("hour_ts=?", hour).Count(&states).Error; err != nil {
		t.Fatal(err)
	}
	if states != 0 {
		t.Fatalf("incomplete hour leaked %d credit proof rows", states)
	}
	var userStates int64
	if err := m.usageFactsStore().Model(&FinanceUserHourState{}).Where("hour_ts=?", hour).Count(&userStates).Error; err != nil {
		t.Fatal(err)
	}
	if userStates != 0 {
		t.Fatalf("outer publication transaction leaked %d user proof rows", userStates)
	}
}

func TestFinanceFactSyncStateResetsCursorWhenSourceEpochChanges(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.FinanceStartDate = "2026-05-01"
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "old", NextHourTs: start + 10*3600, LastCompletedHour: start + 9*3600}).Error; err != nil {
		t.Fatal(err)
	}
	state, err := m.financeFactSyncState(context.Background(), "new")
	if err != nil {
		t.Fatal(err)
	}
	if state.SourceEpoch != "new" || state.StartHourTs != start || state.NextHourTs != start || state.LastCompletedHour != 0 || state.Status != "pending" {
		t.Fatalf("state=%+v", state)
	}
}

func TestFinanceFactSyncStateResetsCursorWhenStartHourChanges(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.FinanceStartDate = "2026-09-01"
	oldStart, err := financeStartHour("2026-09-15")
	if err != nil {
		t.Fatal(err)
	}
	newStart, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "source-v1", StartHourTs: oldStart,
		NextHourTs: oldStart + 24*3600, LastCompletedHour: oldStart + 23*3600, Status: "running"}).Error; err != nil {
		t.Fatal(err)
	}
	state, err := m.financeFactSyncState(context.Background(), "source-v1")
	if err != nil {
		t.Fatal(err)
	}
	if state.StartHourTs != newStart || state.NextHourTs != newStart || state.LastCompletedHour != 0 || state.Status != "pending" {
		t.Fatalf("state=%+v", state)
	}
}

func TestSyncNextFinanceFactHourAdvancesOnlyAfterAtomicPublication(t *testing.T) {
	source := financeSyncSource(t)
	m := newStabilityTestMonitor(t)
	m.prodDB = source
	m.cfg.FinanceEnabled = true
	m.cfg.FinanceFactsSyncEnabled = true
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsHistorySourceEpoch = "source-v1"
	m.cfg.FinanceStartDate = "2026-09-01"
	hour, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`INSERT INTO users(id,created_at) VALUES(7,?);`, hour-60); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,token_id,token_name,request_id,content,other) VALUES
		(1,7,?,3,0,0,'','','管理员增加用户额度 ＄100.000000 额度',''),
		(2,7,?,2,500000,1,'customer','request-redacted-by-fact-layer','prompt-redacted-by-fact-layer','')`, hour+10, hour+20); err != nil {
		t.Fatal(err)
	}
	progressed, err := m.syncNextFinanceFactHour(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !progressed {
		t.Fatal("closed finance hour did not advance")
	}
	var state FinanceFactSyncState
	if err := m.usageFactsStore().First(&state, financeFactSyncStateID).Error; err != nil {
		t.Fatal(err)
	}
	if state.NextHourTs != hour+usageFactHourSeconds || state.LastCompletedHour != hour || state.Status != "running" || state.FailureStreak != 0 {
		t.Fatalf("state=%+v", state)
	}
}

func TestSyncNextFinanceFactHourKeepsCursorOnIncompleteCredit(t *testing.T) {
	source := financeSyncSource(t)
	m := newStabilityTestMonitor(t)
	m.prodDB = source
	m.cfg.FinanceEnabled = true
	m.cfg.FinanceFactsSyncEnabled = true
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsHistorySourceEpoch = "source-v1"
	m.cfg.FinanceStartDate = "2026-09-01"
	hour, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	// The adjustment is recognizable, but the referenced user does not exist;
	// registration-gift classification must fail closed and pin the cursor.
	if _, err := source.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,token_id,token_name,request_id,content,other)
		VALUES(1,404,?,3,0,0,'','','管理员增加用户额度 ＄100.000000 额度','')`, hour+10); err != nil {
		t.Fatal(err)
	}
	progressed, err := m.syncNextFinanceFactHour(context.Background())
	if err == nil || progressed {
		t.Fatalf("incomplete hour advanced: progressed=%v err=%v", progressed, err)
	}
	var state FinanceFactSyncState
	if err := m.usageFactsStore().First(&state, financeFactSyncStateID).Error; err != nil {
		t.Fatal(err)
	}
	if state.NextHourTs != hour || state.Status != "error" || state.FailureStreak != 1 || state.LastError == "" {
		t.Fatalf("state=%+v", state)
	}
	var proofs int64
	if err := m.usageFactsStore().Model(&FinanceUserHourState{}).Where("hour_ts=?", hour).Count(&proofs).Error; err != nil {
		t.Fatal(err)
	}
	if proofs != 0 {
		t.Fatalf("failed hour leaked %d local proofs", proofs)
	}
}

func TestRunFinanceFactBackfillIsBoundedAndAdvancesChronologically(t *testing.T) {
	source := financeSyncSource(t)
	m := newStabilityTestMonitor(t)
	m.prodDB = source
	m.cfg.FinanceEnabled = true
	m.cfg.FinanceFactsSyncEnabled = true
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsHistorySourceEpoch = "source-v1"
	m.cfg.FinanceStartDate = "2026-09-01"
	m.cfg.BackgroundSourceMinStartIntervalMS = -1
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`INSERT INTO users(id,created_at) VALUES(7,?);`, start-60); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,token_id,token_name,request_id,content,other) VALUES
		(1,7,?,3,0,0,'','','管理员增加用户额度 ＄100.000000 额度',''),
		(2,7,?,2,500000,1,'customer','request-redacted-by-fact-layer','prompt-redacted-by-fact-layer','')`, start+10, start+20); err != nil {
		t.Fatal(err)
	}
	result, err := m.RunFinanceFactBackfill(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestedHours != 2 || result.CompletedHours != 2 || result.CaughtUp ||
		result.RunFromHour != start || result.NextHour != start+2*usageFactHourSeconds ||
		result.LastCompletedHour != start+usageFactHourSeconds || result.UserSourceRows != 1 ||
		result.CreditSourceRows != 1 || result.EligibleGrants != 1 || result.GiftRecipients != 1 ||
		result.BoundaryEvents < 1 {
		t.Fatalf("result=%+v", result)
	}
	var userProofs, creditProofs int64
	if err := m.usageFactsStore().Model(&FinanceUserHourState{}).Count(&userProofs).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&FinanceCreditHourState{}).Count(&creditProofs).Error; err != nil {
		t.Fatal(err)
	}
	if userProofs != 2 || creditProofs != 2 {
		t.Fatalf("published user=%d credit=%d", userProofs, creditProofs)
	}
}

func TestRunFinanceFactBackfillRejectsUnsafeInvocation(t *testing.T) {
	m := newStabilityTestMonitor(t)
	if _, err := m.RunFinanceFactBackfill(context.Background(), 1); err == nil {
		t.Fatal("disabled finance sync must be rejected")
	}
	m.cfg.FinanceFactsSyncEnabled = true
	m.cfg.UsageFactsEnabled = true
	m.prodDB = financeSyncSource(t)
	if _, err := m.RunFinanceFactBackfill(context.Background(), 169); err == nil {
		t.Fatal("unbounded finance maintenance run must be rejected")
	}
}

func TestFinanceFactPublishedThroughIsReadOnlyAndFailClosed(t *testing.T) {
	m := newStabilityTestMonitor(t)
	start := int64(3_600)
	through, err := m.financeFactPublishedThrough(context.Background(), start)
	if err != nil || through != start {
		t.Fatalf("missing cursor boundary=%d err=%v", through, err)
	}
	var count int64
	if err := m.usageFactsStore().Model(&FinanceFactSyncState{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("report lookup mutated cursor: count=%d err=%v", count, err)
	}
	state := FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "source-v1", StartHourTs: start, NextHourTs: start + 2*usageFactHourSeconds}
	if err := m.usageFactsStore().Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	through, err = m.financeFactPublishedThrough(context.Background(), start)
	if err != nil || through != state.NextHourTs {
		t.Fatalf("published boundary=%d err=%v", through, err)
	}
	through, err = m.financeFactPublishedThrough(context.Background(), start+usageFactHourSeconds)
	if err != nil || through != start+usageFactHourSeconds {
		t.Fatalf("mismatched start must fail closed: boundary=%d err=%v", through, err)
	}
}

func TestNewFinanceFactBackfillRejectsMainStoreAndMissingFactsStore(t *testing.T) {
	base := Settings{
		StorePath:                          t.TempDir() + "/main.db",
		UsageFactsStorePath:                t.TempDir() + "/facts.db",
		UsageFactsEnabled:                  true,
		UsageFactsHistorySourceMode:        "complete",
		UsageFactsHistorySourceEpoch:       "source-v1",
		FinanceEnabled:                     true,
		FinanceFactsSyncEnabled:            true,
		FinanceStartDate:                   "2026-09-01",
		UsageFactsFullHistoryEnabled:       true,
		UsageFactsReadEnabled:              true,
		UsageFactsQueryTimeoutSec:          20,
		BackgroundSourceMinStartIntervalMS: 2000,
	}
	if _, err := NewFinanceFactBackfill(base); err == nil {
		t.Fatal("missing facts store must not be created by maintenance command")
	}
	base.UsageFactsStorePath = base.StorePath
	if _, err := NewFinanceFactBackfill(base); err == nil {
		t.Fatal("main Monitor store must not be opened by finance maintenance command")
	}
}

func TestFinanceFactSyncStatusIsLocalAndReportsDurableProgress(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.FinanceEnabled = false
	m.cfg.FinanceFactsSyncEnabled = true
	m.cfg.FinanceStartDate = "2026-09-01"
	m.cfg.UsageFactsHistorySourceEpoch = "source-v1"
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	state := FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "source-v1", StartHourTs: start, NextHourTs: start + 24*3600,
		LastCompletedHour: start + 23*3600, Status: "running", LastAttemptAt: start + 24*3600,
		LastSuccessAt: start + 24*3600, UpdatedAt: start + 24*3600}
	if err := m.usageFactsStore().Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&FinanceGiftRecipient{SourceEpoch: "source-v1", UserID: 7, FirstGrantAt: start + 10, FirstGrantHour: start}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(start+48*3600+10*60, 0)
	status, err := m.financeFactSyncStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Enabled || status.ReportEnabled || status.Status != "backfilling" || status.ExpectedHours != 48 || status.CompletedHours != 24 || status.ProgressPercent != 50 || status.GiftRecipients != 1 {
		t.Fatalf("status=%+v", status)
	}
}

func TestFinanceFactSyncStatusDisabledDoesNotInventProgress(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.FinanceStartDate = "2026-09-01"
	start, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	status, err := m.financeFactSyncStatus(context.Background(), time.Unix(start+24*3600, 0))
	if err != nil {
		t.Fatal(err)
	}
	if status.Enabled || status.Status != "disabled" || status.CompletedHours != 0 || status.NextHour != start {
		t.Fatalf("status=%+v", status)
	}
}
