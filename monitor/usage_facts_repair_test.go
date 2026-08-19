package monitor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestUsageFactsControlledRepairRebuildsWholeHistoricalDay 锁住 8 天以外晚到数据的
// 安全修复语义：请求只回退候选小时证明，已发布旧日在 24 小时完整重建前
// 继续可读；后台严格串行查询 24 个小时，最终日事实/proof 同步更新。
func TestUsageFactsControlledRepairRebuildsWholeHistoricalDay(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 12
	m.cfg.UsageFactsHourRetentionDays = 8

	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	repairDay := time.Date(2026, 8, 4, 0, 0, 0, 0, usageCST).Unix() // 距当日 >8 天
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).
		Updates(map[string]any{"next_backfill_hour": end, "range_start": start}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = ?", 1).Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": m.usageFactBackfillDays(),
		"next_backfill_hour":   end,
	}).Error; err != nil {
		t.Fatal(err)
	}

	emptyHash := usageFactContentHash(nil)
	memberHours := make([]UsageFactMemberHourState, 0, (end-start)/usageFactHourSeconds)
	for hour := start; hour < end; hour += usageFactHourSeconds {
		memberHours = append(memberHours, UsageFactMemberHourState{
			UserID: 1, HourTs: hour, Status: "complete", ContentHash: emptyHash,
			CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
		})
	}
	if err := m.storeDB.CreateInBatches(memberHours, 500).Error; err != nil {
		t.Fatal(err)
	}
	oldRows := []UsageDailyFact{{
		DateTs: repairDay, UserID: 1, ChannelID: 9, Grp: "old", ModelName: "old-model", TokenID: 10,
		TokenName: "old-token", Requests: 1, PromptTokens: 1, CompletionTokens: 1, ConsumeQuota: 100,
	}}
	if err := m.storeDB.Create(&oldRows).Error; err != nil {
		t.Fatal(err)
	}
	oldMetrics := dailyFactsMetrics(oldRows)
	if err := m.storeDB.Create(&UsageFactMemberDayState{
		UserID: 1, DateTs: repairDay, Rows: len(oldRows), Requests: oldMetrics.Requests,
		Tokens: oldMetrics.tokens(), ContentHash: usageDailyFactContentHash(oldRows), UpdatedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{1}, start, end)
	var generationBefore UsageFactSyncState
	if err := m.storeDB.First(&generationBefore, 1).Error; err != nil {
		t.Fatal(err)
	}

	lateHour := repairDay + 13*usageFactHourSeconds
	if _, err := prodDB.Exec(fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,33,%d,2,'model-new',500000,100,20,'g1',10,'token-new')", lateHour+1)); err != nil {
		t.Fatal(err)
	}

	state, err := m.requestUsageFactsRepair(context.Background(), usageFactRepairRequest{
		From: "2026-08-04", To: "2026-08-04", Mode: "manual", Confirm: "REPAIR_LOCAL_FACTS",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !usageFactRepairActive(state) || state.RepairTotalMemberHours != 24 || state.RepairFrom != repairDay || state.RepairThrough != repairDay+usageFactDaySeconds {
		t.Fatalf("补数状态错误: %+v", state)
	}
	if !m.usageFactsReadReady.Load() {
		t.Fatal("受控补数不应在新自然日完整前关闭仍自洽的旧服务版")
	}
	var remainingProofs int64
	if err := m.storeDB.Model(&UsageFactMemberHourState{}).
		Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", 1, repairDay, repairDay+usageFactDaySeconds).
		Count(&remainingProofs).Error; err != nil {
		t.Fatal(err)
	}
	if remainingProofs != 0 {
		t.Fatalf("待修复自然日的 24 小时 proof 应全部重建，仍有 %d", remainingProofs)
	}

	counts.reset()
	for i := 0; i < 24; i++ {
		worked, err := m.syncNextUsageFactHour(context.Background(), now)
		if err != nil || !worked {
			t.Fatalf("受控补数第 %d 小时失败: worked=%v err=%v", i+1, worked, err)
		}
		if i < 23 {
			var during UsageFactSyncState
			if err := m.storeDB.First(&during, 1).Error; err != nil {
				t.Fatal(err)
			}
			if during.ServingGeneration != generationBefore.ServingGeneration {
				t.Fatalf("完整自然日尚未原子重建时不得提前切服务缓存世代: hour=%d before=%d during=%d",
					i+1, generationBefore.ServingGeneration, during.ServingGeneration)
			}
		}
	}
	if got := counts.logs.Load(); got != 24 {
		t.Fatalf("完整自然日补数应严格查询 24 个小时，实际 %d", got)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if usageFactRepairActive(state) || state.RepairCompletedAt == 0 || state.RepairCompletedMemberHours != 24 {
		t.Fatalf("补数完成进度未持久化: %+v", state)
	}
	if state.ServingGeneration != generationBefore.ServingGeneration+1 {
		t.Fatalf("完整日提交后应只切一次服务缓存世代: before=%d after=%d", generationBefore.ServingGeneration, state.ServingGeneration)
	}

	var rebuilt []UsageDailyFact
	if err := m.storeDB.Where("date_ts = ? AND user_id = ?", repairDay, 1).Find(&rebuilt).Error; err != nil {
		t.Fatal(err)
	}
	if metrics := dailyFactsMetrics(rebuilt); len(rebuilt) != 1 || metrics.Requests != 1 || metrics.ConsumeQuota != 500000 {
		t.Fatalf("晚到日志未重建进日事实: rows=%+v metrics=%+v", rebuilt, metrics)
	}
	var proof UsageFactMemberDayState
	if err := m.storeDB.First(&proof, "user_id = ? AND date_ts = ?", 1, repairDay).Error; err != nil {
		t.Fatal(err)
	}
	if proof.ContentHash != usageDailyFactContentHash(rebuilt) || !usageFactMemberDayMetricsMatchState(dailyFactsMetrics(rebuilt), proof) {
		t.Fatalf("重建后日 proof 与事实不一致: proof=%+v rows=%+v", proof, rebuilt)
	}

	counts.reset()
	stats, err := m.computeUsageStatsForRead(context.Background(), []int64{1}, repairDay, repairDay+usageFactDaySeconds, 0)
	if err != nil || stats.Summary.ConsumeQuota != 500000 {
		t.Fatalf("补数后本地读取错误: stats=%+v err=%v", stats, err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("页面事实读取不得因补数回扫来源 logs: %d", got)
	}
}

// 候选快照可能已达到 100% 小时水位，却因一个历史成员日缺 proof 无法发布。
// readiness 必须把首个坏日自动转成持久 candidate-gap job，只重读坏成员，
// 保持旧服务版可读；24 小时完成后候选可原子发布，页面仍不访问来源 logs。
func TestUsageFactsCandidateGapAutoRepairPublishesCandidate(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 3
	m.cfg.UsageFactsHourRetentionDays = 1
	tracked := []TrackedUser{{UserID: 1, Username: "alice"}, {UserID: 2, Username: "bob"}}
	if err := m.storeDB.Create(&tracked).Error; err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 8, 14, 12, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	ids := []int64{1, 2}
	if err := m.ensureUsageFactMembershipAt(ids, start); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("active = ?", true).
		Updates(map[string]any{"next_backfill_hour": end, "range_start": start}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint": portalMemberFingerprintFromIDs(ids), "backfill_window_days": 3, "next_backfill_hour": end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	emptyHourHash := usageFactContentHash(nil)
	memberHours := make([]UsageFactMemberHourState, 0, int((end-start)/usageFactHourSeconds)*len(ids))
	for hour := start; hour < end; hour += usageFactHourSeconds {
		for _, id := range ids {
			memberHours = append(memberHours, UsageFactMemberHourState{
				UserID: id, HourTs: hour, Status: "complete", ContentHash: emptyHourHash,
				CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
			})
		}
	}
	if err := m.storeDB.CreateInBatches(memberHours, 500).Error; err != nil {
		t.Fatal(err)
	}
	firstFullDay, lastFullDay := usageFactSemanticFullDayRange(start, end)
	repairDay := firstFullDay
	for day := firstFullDay; day < lastFullDay; day += usageFactDaySeconds {
		for _, id := range ids {
			if day == repairDay && id == 1 {
				continue // 精确制造一个候选 member-day 缺口。
			}
			if err := m.storeDB.Create(&UsageFactMemberDayState{
				UserID: id, DateTs: day, ContentHash: usageDailyFactContentHash(nil), UpdatedAt: now.Unix(),
			}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	// 旧服务版只覆盖最后一天；候选坏日位于它的左侧，修复期间旧版必须继续可读。
	seedPublishedUsageFactsForTest(t, m, ids, end-usageFactDaySeconds, end)
	lateHour := repairDay + 13*usageFactHourSeconds
	if _, err := prodDB.Exec(fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,33,%d,2,'model-new',700000,100,20,'g1',10,'token-new')", lateHour+1)); err != nil {
		t.Fatal(err)
	}

	counts.reset()
	m.refreshUsageFactsReadiness(context.Background(), now)
	var state UsageFactSyncState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if !usageFactRepairActive(state) || state.RepairMode != "candidate_gap" || state.RepairTargetMembers != 1 ||
		state.RepairFrom != repairDay || state.RepairThrough != repairDay+usageFactDaySeconds || state.RepairTotalMemberHours != 24 {
		t.Fatalf("候选缺口未精确转成持久修复任务: %+v", state)
	}
	if !m.usageFactsReadReady.Load() {
		t.Fatal("候选修复期间上一份已发布快照必须继续可读")
	}
	var targets []UsageFactRepairMember
	if err := m.storeDB.Order("user_id").Find(&targets).Error; err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].UserID != 1 || targets[0].ResumeBackfillHour != end {
		t.Fatalf("candidate-gap 目标或恢复水位错误: %+v", targets)
	}
	var alice, bob UsageFactMemberState
	if err := m.storeDB.First(&alice, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&bob, "user_id = ?", 2).Error; err != nil {
		t.Fatal(err)
	}
	if alice.NextBackfillHour != repairDay || bob.NextBackfillHour != end {
		t.Fatalf("修复只能回退坏成员: alice=%d bob=%d", alice.NextBackfillHour, bob.NextBackfillHour)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("候选审计和建单只能读本地 SQLite，实际访问 logs=%d", got)
	}

	// 重复 readiness 不得覆盖活动任务或重复目标。
	requestedAt := state.RepairRequestedAt
	m.refreshUsageFactsReadiness(context.Background(), now)
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.RepairRequestedAt != requestedAt {
		t.Fatalf("活动 candidate-gap 被重复建单: before=%d after=%d", requestedAt, state.RepairRequestedAt)
	}

	counts.reset()
	for i := 0; i < 24; i++ {
		worked, err := m.syncNextUsageFactHour(context.Background(), now)
		if err != nil || !worked {
			t.Fatalf("candidate-gap 第 %d 小时失败: worked=%v err=%v", i+1, worked, err)
		}
	}
	if got := counts.logs.Load(); got != 24 {
		t.Fatalf("单个坏成员自然日只应执行 24 条来源小时查询，实际 %d", got)
	}
	if err := m.storeDB.First(&alice, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if alice.NextBackfillHour != end {
		t.Fatalf("修复完成后应直接恢复任务前水位: got=%d want=%d", alice.NextBackfillHour, end)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if usageFactRepairActive(state) || state.RepairCompletedMemberHours != 24 || state.RepairCompletedAt == 0 {
		t.Fatalf("candidate-gap 完成状态未持久化: %+v", state)
	}

	m.refreshUsageFactsReadiness(context.Background(), now)
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.PublishedRangeStart != start || state.PublishedThrough != end || state.PublishedWindowDays != 3 {
		t.Fatalf("修复后候选未原子发布: %+v", state)
	}
	var rebuilt []UsageDailyFact
	if err := m.storeDB.Where("date_ts = ? AND user_id = ?", repairDay, 1).Find(&rebuilt).Error; err != nil {
		t.Fatal(err)
	}
	if metrics := dailyFactsMetrics(rebuilt); metrics.Requests != 1 || metrics.ConsumeQuota != 700000 {
		t.Fatalf("候选坏日未按来源重建: rows=%+v metrics=%+v", rebuilt, metrics)
	}
	counts.reset()
	stats, err := m.computeUsageStatsForRead(context.Background(), ids, repairDay, repairDay+usageFactDaySeconds, 0)
	if err != nil || stats.Summary.ConsumeQuota != 700000 {
		t.Fatalf("修复发布后本地读取错误: stats=%+v err=%v", stats, err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("候选发布后页面不得回扫来源 logs: %d", got)
	}
}

func TestUsageFactsControlledRepairGuardsScopeAndConcurrency(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 60
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - int64(m.usageFactBackfillDays())*usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).Update("next_backfill_hour", end).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = ?", 1).Updates(map[string]any{
		"member_fingerprint": portalMemberFingerprintFromIDs([]int64{1}), "backfill_window_days": 60, "next_backfill_hour": end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{1}, start, end)

	if _, err := m.requestUsageFactsRepair(context.Background(), usageFactRepairRequest{From: "2026-08-14", To: "2026-08-14"}, now); err == nil {
		t.Fatal("未闭合当日必须拒绝")
	}
	if _, err := m.requestUsageFactsRepair(context.Background(), usageFactRepairRequest{From: "2026-06-20", To: "2026-07-25"}, now); err == nil {
		t.Fatal("manual 单次超过 31 天必须拒绝")
	}
	first, err := m.requestUsageFactsRepair(context.Background(), usageFactRepairRequest{From: "2026-08-01", To: "2026-08-01"}, now)
	if err != nil || !usageFactRepairActive(first) {
		t.Fatalf("首个补数请求失败: %+v %v", first, err)
	}
	if _, err := m.requestUsageFactsRepair(context.Background(), usageFactRepairRequest{From: "2026-08-02", To: "2026-08-02"}, now); !errors.Is(err, errUsageFactRepairConflict) {
		t.Fatalf("并发补数任务必须冲突拒绝: %v", err)
	}
}

func TestUsageFactsRepairPersistsAcrossSourceFailure(t *testing.T) {
	m := newTestMonitor(t)
	prodDB := newFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 2
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - 2*usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).
		Update("next_backfill_hour", end).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint": portalMemberFingerprintFromIDs([]int64{1}), "backfill_window_days": 2, "next_backfill_hour": end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{1}, start, end)
	repairDay, _ := usageFactSemanticFullDayRange(start, end)
	state, err := m.requestUsageFactsRepair(context.Background(), usageFactRepairRequest{
		From: time.Unix(repairDay, 0).In(usageCST).Format("2006-01-02"),
		To:   time.Unix(repairDay, 0).In(usageCST).Format("2006-01-02"),
	}, now)
	if err != nil || !usageFactRepairActive(state) {
		t.Fatalf("创建受控修复失败: state=%+v err=%v", state, err)
	}
	if _, err := prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err == nil || !worked {
		t.Fatalf("来源失败应保留可重试任务: worked=%v err=%v", worked, err)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if !usageFactRepairActive(state) || state.RepairCompletedMemberHours != 0 ||
		state.RepairLastFailureAt == 0 || state.RepairLastError == "" {
		t.Fatalf("来源失败后修复进度/错误未持久化: %+v", state)
	}
	var targets int64
	if err := m.storeDB.Model(&UsageFactRepairMember{}).Count(&targets).Error; err != nil {
		t.Fatal(err)
	}
	if targets != 1 {
		t.Fatalf("来源失败不得丢失持久目标: %d", targets)
	}
}

func TestUsageFactsRepairCancelsSafelyWhenMembershipChanges(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 2
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - 2*usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).Update("next_backfill_hour", end).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint": portalMemberFingerprintFromIDs([]int64{1}), "backfill_window_days": 2, "next_backfill_hour": end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{1}, start, end)
	repairDay, _ := usageFactSemanticFullDayRange(start, end)
	state, err := m.requestUsageFactsRepair(context.Background(), usageFactRepairRequest{
		From: time.Unix(repairDay, 0).In(usageCST).Format("2006-01-02"),
		To:   time.Unix(repairDay, 0).In(usageCST).Format("2006-01-02"),
	}, now)
	if err != nil || !usageFactRepairActive(state) {
		t.Fatalf("创建受控修复失败: state=%+v err=%v", state, err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 2, Username: "bob"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.ensureUsageFactMembershipAt([]int64{1, 2}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.setUsageFactMemberBackfillCursor([]int64{1}, repairDay+usageFactHourSeconds); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if usageFactRepairActive(state) || state.RepairCompletedAt == 0 || state.RepairLastFailureAt == 0 || state.RepairLastError == "" {
		t.Fatalf("成员变化时旧任务应安全取消并等待新候选审计: %+v", state)
	}
	if !m.usageFactsReadReady.Load() {
		t.Fatal("候选修复取消不得关闭上一份已发布服务版")
	}
}

// 旧版事实库可能已有完整的小时覆盖，但没有新版成员×自然日
// 语义证明。状态接口必须给出精确迁移范围，不能把结构完好误当成
// 业务内容已校验。
func TestUsageFactsStatusReportsLegacyDayProofMigration(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 2
	m.cfg.UsageFactsRetentionDays = 2
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - 2*usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).
		Update("next_backfill_hour", end).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = ?", 1).Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 2,
		"next_backfill_hour":   end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	hours := make([]UsageFactMemberHourState, 0, 48)
	for hour := start; hour < end; hour += usageFactHourSeconds {
		hours = append(hours, UsageFactMemberHourState{
			UserID: 1, HourTs: hour, Status: "complete", ContentHash: usageFactContentHash(nil),
		})
	}
	if err := m.storeDB.Create(&hours).Error; err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{1}, start, end)

	status, err := m.usageFactsStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	dayFrom, dayThrough := usageFactSemanticFullDayRange(start, end)
	if !status.ProofMigrationRequired || status.ProofMigrationFrom != dayFrom ||
		status.ProofMigrationThrough != dayThrough || status.ExpectedMemberDays != 1 || status.CompleteMemberDays != 0 {
		t.Fatalf("旧快照迁移状态错误: %+v", status)
	}
	if err := m.storeDB.Create(&UsageFactMemberDayState{
		UserID: 1, DateTs: dayFrom, ContentHash: usageDailyFactContentHash(nil),
	}).Error; err != nil {
		t.Fatal(err)
	}
	status, err = m.usageFactsStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if status.ProofMigrationRequired || status.CompleteMemberDays != status.ExpectedMemberDays {
		t.Fatalf("证明补齐后仍误报迁移: %+v", status)
	}
}
