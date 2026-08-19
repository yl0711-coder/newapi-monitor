package monitor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// This is the read-boundary regression test for all customer-visible log
// consumers. It uses only fields emitted by the unmodified NewAPI; internal
// probes must be absent from direct aggregates, facts, detail and export.
func TestChannelTestTrafficExcludedFromDirectFactsDetailAndExport(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)

	hour := time.Date(2026, 8, 12, 9, 0, 0, 0, usageCST).Unix()
	rows := []struct {
		id, typ, tokenID, quota, prompt, completion int
		other, tokenName, content, requestID        string
	}{
		{1, 2, 10, 110, 7, 4, "", "customer-token", "customer consume", "user-consume"},
		{2, 5, 10, 0, 0, 0, "", "customer-token", "status_code=503", "user-error"},
		{3, 2, 0, 900, 20, 10, `{"group_ratio":1}`, "模型测试", "模型测试", ""},
		{4, 5, 0, 0, 0, 0, `{"status_code":503}`, "", "upstream failed", ""},
	}
	for _, row := range rows {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,username,content,other,request_id)
			VALUES (?,1,9,?,?,?,?,?,?,?, ?,?,'root',?,?,?)`,
			row.id, hour+int64(row.id), row.typ, "gpt-test", row.quota, row.prompt, row.completion,
			"customer", row.tokenID, row.tokenName, row.content, row.other, row.requestID); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := m.computeUsageStats(context.Background(), []int64{1}, hour, hour+3600, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Summary.Requests != 1 || stats.Summary.ConsumeQuota != 110 || stats.Summary.Tokens != 11 {
		t.Fatalf("direct stats included internal probes: %+v", stats.Summary)
	}
	matrix, err := m.computeUsageMatrix(context.Background(), []int64{1}, hour, hour+3600)
	if err != nil {
		t.Fatal(err)
	}
	if len(matrix.Cells) != 1 || matrix.Cells[0].Requests != 1 || matrix.Cells[0].ConsumeQuota != 110 {
		t.Fatalf("matrix included internal probes: %+v", matrix.Cells)
	}
	tokens, err := m.computeUserTokenAggregates(context.Background(), 1, hour, hour+3600)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 || tokens[0].TokenID != 10 || tokens[0].Requests != 1 || tokens[0].ConsumeQuota != 110 {
		t.Fatalf("token aggregate included internal probes: %+v", tokens)
	}
	facts, err := m.fetchUsageFactHour(context.Background(), hour, []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].TokenID != 10 || facts[0].Requests != 1 || facts[0].ConsumeQuota != 110 {
		t.Fatalf("facts ingestion included internal probes: %+v", facts)
	}
	detail, err := m.queryGroupLogs(context.Background(), []int64{1}, hour, hour+3600, 0, 0, "", "", "", "", "", 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail) != 2 || detail[0].ID != 2 || detail[1].ID != 1 {
		t.Fatalf("log detail included internal probes: %+v", detail)
	}
	exported, err := m.queryGroupLogsForExport(context.Background(), []int64{1}, hour, hour+3600, 0, 0, "", "", "", "", "", 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(exported) != 2 || exported[0].ID != 2 || exported[1].ID != 1 {
		t.Fatalf("log export included internal probes: %+v", exported)
	}
	count, err := m.countGroupLogs(context.Background(), []int64{1}, hour, hour+3600, 0, 0, "", "", "", "", "")
	if err != nil || count != 2 {
		t.Fatalf("log count included internal probes: count=%d err=%v", count, err)
	}
}

func TestStabilityProblemSamplerExcludesLegacyChannelTestFailure(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	from := time.Date(2026, 8, 12, 10, 0, 0, 0, cstLocation).Unix()
	for _, row := range []struct {
		id        int
		other     string
		requestID string
		content   string
	}{
		{1, "", "customer-request", "status_code=503 customer failure"},
		{2, `{"status_code":503}`, "", "status_code=503 probe failure"},
	} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(id,user_id,channel_id,created_at,type,model_name,`+"`group`"+`,content,other,request_id)
			VALUES (?,1,9,?,5,'gpt-test','customer',?,?,?)`, row.id, from+int64(row.id), row.content, row.other, row.requestID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.sampleStabilityProblems(context.Background(), from, from+60); err != nil {
		t.Fatal(err)
	}
	result, err := m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: from, ToTs: from + 60}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if result.CapturedTotal != 1 || len(result.Problems) != 1 || result.Problems[0].Message != "status_code=503 customer failure" {
		t.Fatalf("problem sampler exposed channel-test failure: %+v", result)
	}
}

func TestResetStaleStabilityProblemClassificationRequiresExplicitNonDestructiveMigration(t *testing.T) {
	m := newStabilityTestMonitor(t)
	base := time.Date(2026, 8, 12, 11, 0, 0, 0, cstLocation).Unix()
	for i := 0; i < 3; i++ {
		bucket := base + int64(i)*60
		suffix := string(rune('a' + i))
		if err := m.storeDB.Create(&StabilityProblemSample{
			BucketTs: bucket, Source: "newapi", SignatureHash: "sample-" + suffix,
			ChannelID: 9, ModelName: "gpt-test", Grp: "g", Count: 1,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityProblemStage{
			BucketTs: bucket, Source: "newapi", SignatureHash: "stage-" + suffix,
			ChannelID: 9, ModelName: "gpt-test", Grp: "g", Count: 1,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityProblemIngestState{BucketTs: bucket, Complete: true}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"stability_problem_samples", "stability_problem_stages", "stability_problem_ingest_states"} {
		if err := m.storeDB.Exec("UPDATE "+table+" SET traffic_class_version=NULL WHERE bucket_ts=?", base).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Exec("UPDATE "+table+" SET traffic_class_version=? WHERE bucket_ts=?", userTrafficClassificationVersion-1, base+60).Error; err != nil {
			t.Fatal(err)
		}
	}

	if err := m.resetStaleStabilityProblemClassification(); !errors.Is(err, errStabilityProblemClassificationMigrationRequired) {
		t.Fatalf("ordinary startup must fail closed before classification migration: %v", err)
	}
	if progress := m.stabilityProblemMigrationProgress(); progress.Status != "paused_disabled" || progress.NextTs <= 0 {
		t.Fatalf("disabled migration must remain durably visible instead of disappearing: %+v", progress)
	}
	m.cfg.StabilityClassificationMigrationEnabled = true
	if err := m.resetStaleStabilityProblemClassification(); err != nil {
		t.Fatal(err)
	}
	for name, model := range map[string]any{
		"samples": &StabilityProblemSample{}, "stages": &StabilityProblemStage{}, "states": &StabilityProblemIngestState{},
	} {
		var rows int64
		if err := m.storeDB.Model(model).Count(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if rows != 3 {
			t.Fatalf("%s must remain intact until bounded replacement, rows=%d", name, rows)
		}
		var current int64
		if err := m.storeDB.Model(model).Where("bucket_ts=? AND traffic_class_version=?", base+120, userTrafficClassificationVersion).Count(&current).Error; err != nil || current != 1 {
			t.Fatalf("%s current row was removed: count=%d err=%v", name, current, err)
		}
	}
	var migration StabilityProblemClassificationMigration
	if err := m.storeDB.First(&migration, 1).Error; err != nil {
		t.Fatal(err)
	}
	finalizedThrough := (time.Now().Unix() - stabilityProblemFinalizeDelaySec) / 60 * 60
	if migration.ThroughTs > finalizedThrough || migration.ThroughTs < finalizedThrough-60 {
		t.Fatalf("cold migration included an unfinalized minute: through=%s finalized=%s",
			time.Unix(migration.ThroughTs, 0), time.Unix(finalizedThrough, 0))
	}
	if migration.TrafficClassVersion != userTrafficClassificationVersion || migration.Status != "queued" || migration.NextTs != migration.FromTs {
		t.Fatalf("unexpected durable problem migration: %+v", migration)
	}
}

// Auto-repair is the durable cost path for recent probes: the minute sampler
// intentionally excludes them, then the first finalized-hour repair query
// writes only channel_test_hour_samples.  Executing this against SQLite also
// prevents MySQL-only JSON functions from slipping into local acceptance.
func TestRecentFinalizedHourRepairPersistsOnlyChannelTestCostOnSQLite(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite // marks this as the local fake-source dialect

	to := finalizedStabilityHourTo(time.Now().Unix())
	hour := to - 3600
	tests := []struct {
		id                        int
		result, other             string
		quota, prompt, completion int
	}{
		{1, "success", `{"group_ratio":2}`, 120, 8, 3},
		{2, "anomaly", `{"billing_mode":"tiered_expr","group_ratio":2}`, 80, 6, 0},
		{3, "failed", `{"status_code":503}`, 0, 0, 0},
	}
	for _, row := range tests {
		typ := 2
		tokenName, content := "模型测试", "模型测试"
		if row.result == "failed" {
			typ = 5
			tokenName, content = "", "status_code=503 probe failure"
		}
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`+"`group`"+`,token_id,token_name,content,other,request_id)
			VALUES (?,1,17,?,?,'tiered-test',?,?,?,2,'internal',0,?,?,?,'')`,
			row.id, hour+int64(row.id), typ, row.quota, row.prompt, row.completion, tokenName, content, row.other); err != nil {
			t.Fatal(err)
		}
	}

	m.repairOneStabilityHour(context.Background())

	var userRows int64
	if err := m.storeDB.Model(&StabilityHourSample{}).Where("hour_ts = ?", hour).Count(&userRows).Error; err != nil {
		t.Fatal(err)
	}
	if userRows != 0 {
		t.Fatalf("internal probes leaked into user stability table: rows=%d", userRows)
	}
	var got []ChannelTestHourSample
	if err := m.storeDB.Where("hour_ts = ?", hour).Order("origin").Find(&got).Error; err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected two legacy cost-basis series, got %+v", got)
	}
	want := map[string]struct {
		success, anomaly, failed, quota int64
	}{
		"legacy_base":   {1, 0, 1, 120},
		"legacy_tiered": {0, 1, 0, 80},
	}
	for _, row := range got {
		expected, ok := want[row.Origin]
		wantCost := "legacy_assumed_base"
		if row.Origin == "legacy_tiered" {
			wantCost = "legacy_after_group"
		}
		if !ok || row.CostBasis != wantCost || row.Scope != "legacy" || row.TrafficClassVersion != userTrafficClassificationVersion ||
			row.Success != expected.success || row.Anomaly != expected.anomaly || row.Failed != expected.failed || row.Quota != expected.quota {
			t.Fatalf("unexpected channel-test hour row: %+v expected=%+v", row, expected)
		}
	}
	var state StabilityHourIngestState
	if err := m.storeDB.First(&state, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.Requests != 0 || state.InternalTestRequests != 3 || state.InternalTestQuota != 200 {
		t.Fatalf("split completeness ledger mismatch: %+v", state)
	}
}

func TestUsageFactsTrafficClassificationV5RequiresExplicitMaintenanceAndPreservesDerivedRows(t *testing.T) {
	m := newTestMonitor(t)
	db := m.usageFactsStore()
	day := usageFactTestDay().Unix()
	oldVersion := userTrafficClassificationVersion - 1
	stateFields := map[string]any{
		"traffic_class_version": oldVersion, "generation": int64(11), "serving_generation": int64(7),
		"member_fingerprint": "candidate-v2", "backfill_window_days": 90, "next_backfill_hour": day + 7200,
		"next_reconcile_hour": day + 3600, "published_fingerprint": portalMemberFingerprintFromIDs([]int64{2}),
		"published_window_days": 90, "published_range_start": day, "published_through": day + 86400, "published_at": int64(12345),
	}
	if err := db.Model(&UsageFactSyncState{}).Where("id = 1").Updates(stateFields).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range []any{
		&UsageHourFact{HourTs: day, DayTs: day, UserID: 2, ChannelID: 9, Grp: "g", ModelName: "m", TokenID: 2, Requests: 1},
		&UsageDailyFact{DateTs: day, UserID: 2, ChannelID: 9, Grp: "g", ModelName: "m", TokenID: 2, Requests: 1},
		&UsageFactMemberDayState{UserID: 2, DateTs: day, Rows: 1, ContentHash: "proof"},
		&UsageHourIngestState{HourTs: day, Status: "complete", ContentHash: "hour-proof"},
		&UsageFactMemberState{UserID: 2, Active: true, NextBackfillHour: day + 7200},
		&UsageFactMemberHourState{UserID: 2, HourTs: day, Status: "complete", ContentHash: "member-hour-proof"},
		&UsageFactPublishedMember{UserID: 2, PublishedAt: 12345},
	} {
		if err := db.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}

	state, rebuilt, err := migrateUsageFactsTrafficClassification(db, false)
	if !errors.Is(err, errUsageFactsClassificationMigrationRequired) {
		t.Fatalf("ordinary startup must require an explicit migration: state=%+v rebuilt=%v err=%v", state, rebuilt, err)
	}
	if rebuilt || state.TrafficClassVersion != oldVersion {
		t.Fatalf("ordinary startup mutated the old snapshot: rebuilt=%v state=%+v", rebuilt, state)
	}
	for name, model := range map[string]any{
		"hour facts": &UsageHourFact{}, "daily facts": &UsageDailyFact{}, "day proof": &UsageFactMemberDayState{},
		"hour ledger": &UsageHourIngestState{}, "member cursor": &UsageFactMemberState{},
		"member-hour proof": &UsageFactMemberHourState{}, "published members": &UsageFactPublishedMember{},
	} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("ordinary startup changed %s: count=%d err=%v", name, count, err)
		}
	}

	state, rebuilt, err = migrateUsageFactsTrafficClassification(db, true)
	if err != nil {
		t.Fatal(err)
	}
	if !rebuilt || state.TrafficClassVersion != userTrafficClassificationVersion {
		t.Fatalf("v5 NULL-safe semantics must rebuild arbitrary-user facts: rebuilt=%v state=%+v", rebuilt, state)
	}
	if state.Generation != 12 || state.ServingGeneration != 8 || state.MemberFingerprint != "" ||
		state.NextBackfillHour != 0 || state.NextReconcileHour != 0 ||
		state.PublishedAt != 0 || state.PublishedRangeStart != 0 || state.PublishedThrough != 0 {
		t.Fatalf("v5 classification rebuild did not fail closed: %+v", state)
	}
	for name, model := range map[string]any{
		"hour facts": &UsageHourFact{}, "daily facts": &UsageDailyFact{}, "day proof": &UsageFactMemberDayState{},
		"hour ledger": &UsageHourIngestState{}, "member cursor": &UsageFactMemberState{},
		"member-hour proof": &UsageFactMemberHourState{},
	} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("explicit migration must preserve large %s for controlled replacement: count=%d err=%v", name, count, err)
		}
	}
	var publishedCount int64
	if err := db.Model(&UsageFactPublishedMember{}).Count(&publishedCount).Error; err != nil || publishedCount != 0 {
		t.Fatalf("old publication authorization survived explicit migration: count=%d err=%v", publishedCount, err)
	}
}

func TestChannelTestSourcePredicateIsNullSafeForUserTraffic(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,created_at,type,token_name,content,request_id,token_id) VALUES
(1,9,1,2,NULL,NULL,'',1),
(2,9,2,2,'模型测试',NULL,'',1),
(3,9,3,2,NULL,'模型测试','',1),
(4,9,4,2,'模型测试','模型测试','',1),
(5,9,5,2,'normal','normal','',1)`); err != nil {
		t.Fatal(err)
	}
	rows, err := m.prodDB.Query("SELECT id FROM logs WHERE type=2 AND NOT (" + m.channelTestSourcePredicateSQL() + ") ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []int64{1, 2, 3, 5}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("NULL user traffic was filtered by three-valued SQL logic: got=%v want=%v", got, want)
	}
}

func TestUsageFactsTrafficClassificationMaintenancePreservesProfilesAndDerivedRows(t *testing.T) {
	m := newTestMonitor(t)
	db := m.usageFactsStore()
	day := usageFactTestDay().Unix()
	oldVersion := userTrafficClassificationVersion - 1
	if err := db.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": oldVersion, "generation": int64(7), "serving_generation": int64(9),
		"member_fingerprint": portalMemberFingerprintFromIDs([]int64{1}), "backfill_window_days": 90,
		"next_backfill_hour": day + 7200, "published_fingerprint": portalMemberFingerprintFromIDs([]int64{1}),
		"published_window_days": 90, "published_range_start": day, "published_through": day + 86400,
		"published_at": int64(12345), "last_profile_sync_at": int64(333), "last_profile_failure_at": int64(222),
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range []any{
		&UsageHourFact{HourTs: day, DayTs: day, UserID: 1, ChannelID: 9, Grp: "g", ModelName: "m", TokenID: 1, Requests: 1},
		&UsageDailyFact{DateTs: day, UserID: 1, ChannelID: 9, Grp: "g", ModelName: "m", TokenID: 1, Requests: 1},
		&UsageFactMemberDayState{UserID: 1, DateTs: day, Rows: 1, ContentHash: "proof"},
		&UsageHourIngestState{HourTs: day, Status: "complete", ContentHash: "hour-proof"},
		&UsageFactMemberState{UserID: 1, Active: true, NextBackfillHour: day + 7200},
		&UsageFactMemberHourState{UserID: 1, HourTs: day, Status: "complete", ContentHash: "member-hour-proof"},
		&UsageFactPublishedMember{UserID: 1, PublishedAt: 12345},
		&UsageUserSnapshot{UserID: 1, Username: "root", CapturedAt: 100},
		&UsageTokenSnapshot{TokenID: 1, UserID: 1, Name: "snapshot", CapturedAt: 100},
	} {
		if err := db.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}

	state, rebuilt, err := migrateUsageFactsTrafficClassification(db, true)
	if err != nil {
		t.Fatal(err)
	}
	if !rebuilt || state.TrafficClassVersion != userTrafficClassificationVersion ||
		state.Generation != 8 || state.ServingGeneration != 10 {
		t.Fatalf("impacted facts were not rebuilt with a new cache generation: rebuilt=%v state=%+v", rebuilt, state)
	}
	if state.PublishedAt != 0 || state.PublishedFingerprint != "" || state.PublishedRangeStart != 0 || state.PublishedThrough != 0 ||
		state.MemberFingerprint != "" || state.NextBackfillHour != 0 {
		t.Fatalf("old serving/candidate authorization survived classification reset: %+v", state)
	}
	if state.LastProfileSyncAt != 333 || state.LastProfileFailureAt != 222 {
		t.Fatalf("non-log profile freshness should survive rebuild: %+v", state)
	}
	for name, model := range map[string]any{
		"hour facts": &UsageHourFact{}, "daily facts": &UsageDailyFact{}, "day proof": &UsageFactMemberDayState{},
		"hour ledger": &UsageHourIngestState{}, "member cursor": &UsageFactMemberState{},
		"member-hour proof": &UsageFactMemberHourState{},
	} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("%s must remain available for bounded replacement/rollback: count=%d err=%v", name, count, err)
		}
	}
	var publishedCount int64
	if err := db.Model(&UsageFactPublishedMember{}).Count(&publishedCount).Error; err != nil || publishedCount != 0 {
		t.Fatalf("old publication authorization must be revoked: count=%d err=%v", publishedCount, err)
	}
	for name, model := range map[string]any{"user profile": &UsageUserSnapshot{}, "token profile": &UsageTokenSnapshot{}} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("%s should be retained: count=%d err=%v", name, count, err)
		}
	}
}
