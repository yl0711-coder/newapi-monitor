package monitor

import (
	"context"
	"testing"
	"time"
)

// runShrinkAuditDay prepares a verified day for user 42 holding priorRequests
// requests / priorQuota quota, while the source now has one request of 1000
// quota, then runs the final hourly audit step for that day.
func runShrinkAuditDay(t *testing.T, priorRequests, priorQuota int64, corruptLocal bool) (*Monitor, int64) {
	t.Helper()
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 42, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	through := day + usageFactDaySeconds
	hour23 := through - usageFactHourSeconds
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	now := time.Unix(through+usageFactHourSeconds, 0)
	if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email,created_at) VALUES(42,'u42','u42@x',?)", day); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES(1,42,10,?,2,'m',1000,10,20,'g',5,'t','')`, hour23+60); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 42).Updates(map[string]any{
		"source_epoch": epoch, "source_history_status": "complete_hot", "coverage_status": "ready",
		"classification_version":  userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	old := UsageDailyFact{DateTs: day, UserID: 42, ChannelID: 10, Grp: "g", ModelName: "m",
		TokenID: 5, TokenName: "t", Requests: priorRequests, PromptTokens: 10 * priorRequests, CompletionTokens: 20 * priorRequests, ConsumeQuota: priorQuota}
	oldHash := usageDailyFactContentHash([]UsageDailyFact{old})
	if corruptLocal {
		old.ConsumeQuota++ // local copy no longer matches its own proof
	}
	if err := m.usageFactsStore().Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactMemberDayState{
		UserID: 42, DateTs: day, Status: "complete", Rows: 1, SourceRows: priorRequests, Requests: priorRequests,
		PromptTokens: 10 * priorRequests, CompletionTokens: 20 * priorRequests, Tokens: 30 * priorRequests, ConsumeQuota: priorQuota,
		ContentHash: oldHash, SourceResultHash: "old-source", FactContentHash: oldHash,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceEpoch: epoch, SourceCheckedAt: now.Unix(), CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	emptyHash := usageFactContentHash(nil)
	proofs := make([]UsageFactMemberHourState, 0, 23)
	for hour := day; hour < hour23; hour += usageFactHourSeconds {
		proofs = append(proofs, UsageFactMemberHourState{UserID: 42, HourTs: hour, Status: "complete",
			ContentHash: emptyHash, SourceEpoch: epoch, CompletedAt: now.Unix(), UpdatedAt: now.Unix()})
	}
	if err := m.usageFactsStore().CreateInBatches(proofs, 100).Error; err != nil {
		t.Fatal(err)
	}
	uid := int64(42)
	job := UsageFactJob{
		ID: usageFactSourceAuditJobID(uid, 1), Kind: usageFactHistoryKindAuditHour, Priority: 5,
		UserID: &uid, TrackedRevision: 1, SourceEpoch: epoch, FromTs: day, ThroughTs: through,
		NextHour: hour23, VerifyNextHour: through, TotalHours: 24, CompletedHours: 23,
		Status: usageFactHistoryJobRunning, LeaseOwner: "hour-audit", LeaseUntil: now.Add(time.Minute).Unix(),
		CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner, From: hour23, Through: through}
	if err := m.executeUsageFactHistorySourceAuditHour(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	var got UsageFactJob
	if err := m.usageFactsStore().First(&got, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Status != usageFactHistoryJobComplete || got.NextHour != through {
		t.Fatalf("audit must finish the day either way, got %+v", got)
	}
	return m, day
}

func dailyFor(t *testing.T, m *Monitor, day int64) []UsageDailyFact {
	t.Helper()
	var rows []UsageDailyFact
	if err := m.usageFactsStore().Where("user_id = ? AND date_ts = ?", 42, day).Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestUsageFactAuditKeepsVerifiedDayWhenSourceShrinks(t *testing.T) {
	m, day := runShrinkAuditDay(t, 3, 3000, false)
	if daily := dailyFor(t, m, day); len(daily) != 1 || daily[0].Requests != 3 || daily[0].ConsumeQuota != 3000 {
		t.Fatalf("verified day must be kept when new-api lost logs: %+v", daily)
	}
	var proof UsageFactMemberDayState
	if err := m.usageFactsStore().First(&proof, "user_id = ? AND date_ts = ?", 42, day).Error; err != nil || proof.Requests != 3 || proof.SourceResultHash != "old-source" {
		t.Fatalf("verified proof must be kept: %+v err=%v", proof, err)
	}
	var finding UsageFactSourceShrinkFinding
	if err := m.usageFactsStore().First(&finding, "user_id = ? AND date_ts = ?", 42, day).Error; err != nil {
		t.Fatalf("shrink not recorded: %v", err)
	}
	if finding.KeptRequests != 3 || finding.SourceRequests != 1 || finding.KeptConsumeQuota != 3000 || finding.SourceConsumeQuota != 1000 || finding.Occurrences != 1 {
		t.Fatalf("finding: %+v", finding)
	}
}

func TestUsageFactAuditStillRepairsCorruptedLocalDay(t *testing.T) {
	m, day := runShrinkAuditDay(t, 3, 3000, true)
	if daily := dailyFor(t, m, day); len(daily) != 1 || daily[0].Requests != 1 || daily[0].ConsumeQuota != 1000 {
		t.Fatalf("a local copy that no longer matches its proof must be repaired from source: %+v", daily)
	}
	var findings int64
	m.usageFactsStore().Model(&UsageFactSourceShrinkFinding{}).Count(&findings)
	if findings != 0 {
		t.Fatalf("local corruption is not a source shrink, findings=%d", findings)
	}
}

func TestUsageFactSourceShrankRules(t *testing.T) {
	epoch := "e1"
	prior := UsageFactMemberDayState{Status: "complete", FactContentHash: "h", SourceRows: 5, Requests: 5, ConsumeQuota: 500, RefundRecords: 1, RefundQuota: 50,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion, SourceEpoch: epoch}
	same := usageFactHistoryControl{SourceRows: 5, Requests: 5, ConsumeQuota: 500, RefundRecords: 1, RefundQuota: 50}
	if usageFactSourceShrank(prior, "h", same, epoch) {
		t.Fatal("unchanged source is not a shrink")
	}
	late := same
	late.SourceRows, late.Requests, late.ConsumeQuota = 7, 7, 700
	lateRefund := same
	lateRefund.SourceRows, lateRefund.RefundRecords, lateRefund.RefundQuota = 6, 2, 90
	for name, fresh := range map[string]usageFactHistoryControl{"late logs": late, "late refund": lateRefund} {
		if usageFactSourceShrank(prior, "h", fresh, epoch) {
			t.Fatalf("%s must follow the source", name)
		}
	}
	gone := same
	gone.SourceRows, gone.Requests, gone.ConsumeQuota = 0, 0, 0
	if !usageFactSourceShrank(prior, "h", gone, epoch) {
		t.Fatal("cleared logs must be detected")
	}
	for name, p := range map[string]UsageFactMemberDayState{
		"not verified":          func() UsageFactMemberDayState { x := prior; x.Status = "partial"; return x }(),
		"classification change": func() UsageFactMemberDayState { x := prior; x.ClassificationVersion--; return x }(),
		"epoch change":          func() UsageFactMemberDayState { x := prior; x.SourceEpoch = "e0"; return x }(),
	} {
		if usageFactSourceShrank(p, "h", gone, epoch) {
			t.Fatalf("%s: the source must be followed", name)
		}
	}
	if usageFactSourceShrank(prior, "other-hash", gone, epoch) {
		t.Fatal("a local copy that differs from its proof must be repaired")
	}
}

// The range commit path (whole-day history import/repair) applies the same
// rule: first commit two logs, then new-api loses one; the verified day stays.
func TestCommitUsageFactHistoryRangeKeepsVerifiedDayWhenSourceShrinks(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 1, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	for id, at := range map[int]int64{1: day + 60, 2: day + 120} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES(?,1,10,?,2,'m-a',1000,100,20,'g-a',5,'token-a','')`, id, at); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(job string) {
		t.Helper()
		result, err := m.fetchUsageFactHistoryRange(context.Background(), day, day+usageFactDaySeconds, []int64{1})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.commitUsageFactHistoryRange(context.Background(), result, job, map[int64]int64{1: 1}); err != nil {
			t.Fatal(err)
		}
	}
	commit("history-job-1")
	if _, err := m.prodDB.Exec("DELETE FROM logs WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	commit("history-job-2")
	var facts []UsageDailyFact
	if err := m.usageFactsStore().Where("user_id = ? AND date_ts = ?", 1, day).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].Requests != 2 || facts[0].ConsumeQuota != 2000 {
		t.Fatalf("verified day must survive a shrunken source: %+v", facts)
	}
	var finding UsageFactSourceShrinkFinding
	if err := m.usageFactsStore().First(&finding, "user_id = ? AND date_ts = ?", 1, day).Error; err != nil || finding.KeptRequests != 2 || finding.SourceRequests != 1 {
		t.Fatalf("finding=%+v err=%v", finding, err)
	}
	// A later re-read that recovers the logs follows the source again.
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES(2,1,10,?,2,'m-a',1000,100,20,'g-a',5,'token-a',''),(3,1,10,?,2,'m-a',1000,100,20,'g-a',5,'token-a','')`, day+120, day+180); err != nil {
		t.Fatal(err)
	}
	commit("history-job-3")
	facts = nil
	if err := m.usageFactsStore().Where("user_id = ? AND date_ts = ?", 1, day).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].Requests != 3 {
		t.Fatalf("a larger source must be followed: %+v", facts)
	}
}
