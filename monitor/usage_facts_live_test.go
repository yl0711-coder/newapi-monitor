package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func prepareUsageFactRawLiveMember(t *testing.T, m *Monitor, userID int64, floor int64) {
	t.Helper()
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_epoch": m.cfg.UsageFactsHistorySourceEpoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion, "source_history_status": "complete_hot",
		"source_floor_hour": floor, "raw_page_span_hours": 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
}

func TestUsageFactRawLiveCursorAdvancesContinuouslyAndSurvivesNewTarget(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 930
	now := time.Date(2026, 8, 18, 12, 20, 0, 0, usageCST)
	target := m.usageFactFinalizedHour(now)
	prepareUsageFactRawLiveMember(t, m, userID, target-24*usageFactHourSeconds)
	if err := m.prepareUsageFactRawLiveTargets(context.Background(), target, now); err != nil {
		t.Fatal(err)
	}
	var state UsageFactMemberState
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	start := target - usageFactRawLiveInitialLookbackHours*usageFactHourSeconds
	if state.LiveFromHour == nil || state.LiveThroughHour == nil || *state.LiveFromHour != start || *state.LiveThroughHour != start {
		t.Fatalf("initial live cursor=%+v want=%d", state, start)
	}
	event := makeUsageFactRawPageEvents(start, userID, 1)[0]
	if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
		event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota,
		event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
		t.Fatal(err)
	}
	previous := start
	for previous < target {
		advanced := false
		for turn := 0; turn < 5 && !advanced; turn++ {
			if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.syncOneUsageFactRawLiveMember(context.Background(), state, now); err != nil {
				t.Fatal(err)
			}
			if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
				t.Fatal(err)
			}
			advanced = state.LiveThroughHour != nil && *state.LiveThroughHour > previous
		}
		if !advanced || state.LiveThroughHour == nil || *state.LiveThroughHour > target {
			t.Fatalf("live cursor failed to advance continuously from %d: %+v", previous, state)
		}
		previous = *state.LiveThroughHour
	}
	newTarget := target + 2*usageFactHourSeconds
	if err := m.prepareUsageFactRawLiveTargets(context.Background(), newTarget, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if state.LiveThroughHour == nil || *state.LiveThroughHour != target || state.LiveTargetHour == nil || *state.LiveTargetHour != newTarget {
		t.Fatalf("new target reset or skipped durable cursor: %+v", state)
	}
}

func TestUsageFactRawLiveResumesExistingLegacyOneHourCheckpoint(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 939
	now := time.Date(2026, 8, 18, 12, 20, 0, 0, usageCST)
	target := m.usageFactFinalizedHour(now)
	prepareUsageFactRawLiveMember(t, m, userID, target-24*usageFactHourSeconds)
	if err := m.prepareUsageFactRawLiveTargets(context.Background(), target, now); err != nil {
		t.Fatal(err)
	}
	var state UsageFactMemberState
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	start := *state.LiveThroughHour
	// ThroughTs=0 is the upgrade representation of an exact one-hour shard.
	if err := m.usageFactsStore().Create(&UsageFactPageIngestState{
		UserID: userID, HourTs: start, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
		Status: "complete", ContentHash: usageFactContentHash(nil), CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.syncOneUsageFactRawLiveMember(context.Background(), state, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if state.LiveThroughHour == nil || *state.LiveThroughHour != start+usageFactHourSeconds {
		t.Fatalf("live cursor did not resume exact legacy shard: %+v", state)
	}
}

func TestUsageFactRawLiveSchedulerChoosesLeastRecentlyServedMember(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	now := time.Date(2026, 8, 18, 12, 20, 0, 0, usageCST)
	target := m.usageFactFinalizedHour(now)
	for _, id := range []int64{931, 932, 933} {
		prepareUsageFactRawLiveMember(t, m, id, target-24*usageFactHourSeconds)
	}
	if err := m.prepareUsageFactRawLiveTargets(context.Background(), target, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 931).Update("live_last_served_at", now.Unix()).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 932).Update("live_last_served_at", now.Add(-time.Minute).Unix()).Error; err != nil {
		t.Fatal(err)
	}
	state, ok, err := m.nextUsageFactRawLiveMember(context.Background(), now)
	if err != nil || !ok {
		t.Fatalf("next live member ok=%v err=%v", ok, err)
	}
	if state.UserID != 933 {
		t.Fatalf("scheduler chose user=%d; want never-served user 933", state.UserID)
	}
}

func TestUsageFactRawLiveSchedulerFairnessScalesToAllHighVolumeMembers(t *testing.T) {
	for _, members := range []int{50, 200, 500} {
		t.Run(fmt.Sprintf("members_%d", members), func(t *testing.T) {
			m := newUsageHistoryTestMonitor(t)
			m.cfg.UsageFactsRawPageImportEnabled = true
			now := time.Date(2026, 8, 18, 12, 20, 0, 0, usageCST)
			target := m.usageFactFinalizedHour(now)
			start := target - usageFactRawLiveInitialLookbackHours*usageFactHourSeconds
			states := make([]UsageFactMemberState, 0, members)
			for i := 0; i < members; i++ {
				states = append(states, UsageFactMemberState{
					UserID: int64(20_000 + i), Active: true, TrackedRevision: 1,
					SourceEpoch:           m.cfg.UsageFactsHistorySourceEpoch,
					ClassificationVersion: userTrafficClassificationVersion,
					QuerySemanticsVersion: usageFactQuerySemanticsVersion,
					SourceHistoryStatus:   "complete_hot",
					SourceFloorHour:       ptrInt64(start), LiveFromHour: ptrInt64(start),
					LiveThroughHour: ptrInt64(start), LiveTargetHour: ptrInt64(target),
					LiveStatus: "catching_up",
				})
			}
			if err := m.usageFactsStore().CreateInBatches(states, 25).Error; err != nil {
				t.Fatal(err)
			}
			seen := make(map[int64]struct{}, members)
			for turn := 0; turn < members; turn++ {
				state, ok, err := m.nextUsageFactRawLiveMember(context.Background(), now)
				if err != nil || !ok {
					t.Fatalf("turn=%d ok=%v err=%v", turn, ok, err)
				}
				if _, duplicate := seen[state.UserID]; duplicate {
					t.Fatalf("live member %d received a second turn before all %d peers", state.UserID, members)
				}
				if state.LiveLastServedSeq <= 0 {
					t.Fatalf("live member %d did not receive a durable scheduler ticket", state.UserID)
				}
				seen[state.UserID] = struct{}{}
			}
		})
	}
}

func TestUsageFactRecentBridgeReusesColdProofAndExpandsOnlyAfterComplete(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 940
	now := time.Date(2026, 8, 18, 12, 20, 0, 0, usageCST)
	target := m.usageFactFinalizedHour(now)
	liveFrom := target - usageFactRawLiveInitialLookbackHours*usageFactHourSeconds
	desired := usageFactRecentServiceFrom(target)
	prepareUsageFactRawLiveMember(t, m, userID, desired-usageFactDaySeconds)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"live_from_hour": liveFrom, "live_through_hour": target, "live_target_hour": target, "live_status": "ready",
		"coverage_through_hour": desired + usageFactHourSeconds,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.prepareUsageFactRawRecentTargets(context.Background(), target, now); err != nil {
		t.Fatal(err)
	}
	var state UsageFactMemberState
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if state.RecentFromHour == nil || *state.RecentFromHour != desired || state.RecentThroughHour == nil ||
		*state.RecentThroughHour != desired+usageFactHourSeconds || state.RecentTargetHour == nil || *state.RecentTargetHour != liveFrom {
		t.Fatalf("recent bridge did not reuse continuous cold proof: %+v", state)
	}
	if state.LiveFromHour == nil || *state.LiveFromHour != liveFrom {
		t.Fatalf("partial recent bridge moved publication left edge: %+v", state)
	}
	var pageCount int64
	if err := m.usageFactsStore().Model(&UsageFactPageIngestState{}).Where("user_id = ?", userID).Count(&pageCount).Error; err != nil {
		t.Fatal(err)
	}
	if pageCount != 0 {
		t.Fatalf("cold-proof reuse unexpectedly created %d source page checkpoints", pageCount)
	}

	// Keep the test range short. The first empty pass must not publish; the
	// independent second pass completes the exact bridge and then moves the
	// serving floor atomically.
	bridgeTarget := *state.RecentThroughHour + 2*usageFactHourSeconds
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"recent_target_hour": bridgeTarget, "live_from_hour": bridgeTarget, "recent_span_hours": 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	before := *state.LiveFromHour
	if err := m.syncOneUsageFactRawRecentMember(context.Background(), state, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if *state.LiveFromHour != before || *state.RecentThroughHour != desired+usageFactHourSeconds {
		t.Fatalf("first pass falsely published a partial bridge: %+v", state)
	}
	for turns := 0; turns < 8 && *state.RecentThroughHour < bridgeTarget; turns++ {
		if err := m.syncOneUsageFactRawRecentMember(context.Background(), state, now); err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
			t.Fatal(err)
		}
	}
	if *state.RecentThroughHour != bridgeTarget || state.RecentStatus != "ready" || *state.LiveFromHour != desired {
		t.Fatalf("complete recent bridge did not atomically expand publication: %+v", state)
	}
}

func TestUsageFactRecentServiceWindowIncludesSevenCompleteCSTDays(t *testing.T) {
	target := time.Date(2026, 8, 19, 0, 0, 0, 0, usageCST).Unix()
	want := time.Date(2026, 8, 12, 0, 0, 0, 0, usageCST).Unix()
	if got := usageFactRecentServiceFrom(target); got != want {
		t.Fatalf("recent service from=%s, want seven complete CST days from=%s",
			time.Unix(got, 0).In(usageCST), time.Unix(want, 0).In(usageCST))
	}
	// A finalized hour later today must not slide the oldest complete day out
	// of the default report window.
	if got := usageFactRecentServiceFrom(target + 15*usageFactHourSeconds); got != want {
		t.Fatalf("intraday recent service from=%s, want=%s",
			time.Unix(got, 0).In(usageCST), time.Unix(want, 0).In(usageCST))
	}
}

func TestUsageFactRecentBridgeRefinalizesDayOwnedPartlyByLive(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 941
	day := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	prepareUsageFactRawLiveMember(t, m, userID, day)
	for hour := day; hour < day+usageFactDaySeconds; hour += usageFactHourSeconds {
		row := UsageHourFact{HourTs: hour, DayTs: day, UserID: userID, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, TokenName: "t", Requests: 1, PromptTokens: 2, CompletionTokens: 3, ConsumeQuota: 5}
		if err := m.usageFactsStore().Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		metrics, hash := factsMetrics([]UsageHourFact{row}), usageFactContentHash([]UsageHourFact{row})
		if err := m.usageFactsStore().Create(&UsageFactMemberHourState{UserID: userID, HourTs: hour, Status: "complete", Rows: 1, Requests: 1, Tokens: 5, ContentHash: hash, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Create(&UsageFactPageIngestState{UserID: userID, HourTs: hour, ThroughTs: hour + usageFactHourSeconds, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, Status: "complete", SourceRows: metrics.Rows, Requests: metrics.Requests, PromptTokens: metrics.PromptTokens, CompletionTokens: metrics.CompletionTokens, ConsumeQuota: metrics.ConsumeQuota, ContentHash: hash}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.finalizeUsageFactHistoryDayFromHours(context.Background(), day, []int64{userID}, map[int64]int64{userID: 1}, "recent-before"); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactMemberDayState{}).Where("user_id = ? AND date_ts = ?", userID, day).Update("content_hash", "stale").Error; err != nil {
		t.Fatal(err)
	}
	// The recent bridge owns only 00:00-21:00; live already owns 21:00-24:00.
	// The bridge must nevertheless regenerate the complete natural-day proof.
	if err := m.finalizeUsageFactRecentTouchedDays(context.Background(), userID, day, day+21*usageFactHourSeconds); err != nil {
		t.Fatal(err)
	}
	if err := auditUsageFactFullHistoryDayRange(m.usageFactsStore(), UsageFactMemberState{UserID: userID, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch}, day, day+usageFactDaySeconds); err != nil {
		t.Fatalf("recent/live split left a stale daily proof: %v", err)
	}
	// Repair/finalization compacts a closed day by deleting its hourly staging.
	// The recent publication audit must accept the strict daily representation
	// instead of keeping this healed member hidden until deep history completes.
	if err := m.usageFactsStore().Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", userID, day, day+usageFactDaySeconds).Delete(&UsageHourFact{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", userID, day, day+usageFactDaySeconds).Delete(&UsageFactMemberHourState{}).Error; err != nil {
		t.Fatal(err)
	}
	state := UsageFactMemberState{UserID: userID, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, LiveFromHour: ptrInt64(day)}
	if err := auditUsageFactRecentServiceRange(m.usageFactsStore(), state, day+usageFactDaySeconds); err != nil {
		t.Fatalf("compacted repaired day was rejected by recent service audit: %v", err)
	}
}

func TestUsageFactRecentBridgeSchedulerFairnessScalesToHighVolumeMembers(t *testing.T) {
	for _, members := range []int{50, 200, 500} {
		t.Run(fmt.Sprintf("members_%d", members), func(t *testing.T) {
			m := newUsageHistoryTestMonitor(t)
			now := time.Date(2026, 8, 18, 12, 20, 0, 0, usageCST)
			from := usageFactRecentServiceFrom(m.usageFactFinalizedHour(now))
			states := make([]UsageFactMemberState, 0, members)
			for i := 0; i < members; i++ {
				states = append(states, UsageFactMemberState{UserID: int64(30_000 + i), Active: true, TrackedRevision: 1,
					RecentFromHour: ptrInt64(from), RecentThroughHour: ptrInt64(from), RecentTargetHour: ptrInt64(from + usageFactDaySeconds),
					RecentStatus: "catching_up"})
			}
			if err := m.usageFactsStore().CreateInBatches(states, 25).Error; err != nil {
				t.Fatal(err)
			}
			seen := make(map[int64]struct{}, members)
			for turn := 0; turn < members; turn++ {
				state, ok, err := m.nextUsageFactRawRecentMember(context.Background(), now)
				if err != nil || !ok {
					t.Fatalf("turn=%d ok=%v err=%v", turn, ok, err)
				}
				if _, duplicate := seen[state.UserID]; duplicate {
					t.Fatalf("recent member %d received a second turn before all %d peers", state.UserID, members)
				}
				seen[state.UserID] = struct{}{}
			}
		})
	}
}

func TestUsageFactRawLiveHighVolumeYieldsWithoutAdvancingWaterline(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsRawPageImportEnabled = true
	const userID int64 = 934
	now := time.Date(2026, 8, 18, 12, 20, 0, 0, usageCST)
	target := m.usageFactFinalizedHour(now)
	prepareUsageFactRawLiveMember(t, m, userID, target-24*usageFactHourSeconds)
	if err := m.prepareUsageFactRawLiveTargets(context.Background(), target, now); err != nil {
		t.Fatal(err)
	}
	var state UsageFactMemberState
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	start := *state.LiveThroughHour
	for _, event := range makeUsageFactRawPageEvents(start, userID, 3_501) {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
			event.ID, event.UserID, event.ChannelID, event.CreatedAt, event.Type, event.ModelName, event.Quota,
			event.PromptTokens, event.CompletionTokens, event.Grp, event.TokenID, event.TokenName); err != nil {
			t.Fatal(err)
		}
	}
	// A full first page in a wide shard must shrink 24h -> 6h -> 3h -> 1h before
	// committing any staged fact. Only the bounded one-hour shard may page.
	for _, wantSpan := range []int{usageFactRawShardWideHours, usageFactRawShardMediumHours, 1} {
		if err := m.syncOneUsageFactRawLiveMember(context.Background(), state, now); err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
			t.Fatal(err)
		}
		if state.LiveThroughHour == nil || *state.LiveThroughHour != start || state.LiveSpanHours != wantSpan {
			t.Fatalf("dense wide shard did not shrink without advancing: %+v want_span=%d", state, wantSpan)
		}
		var staged int64
		if err := m.usageFactsStore().Model(&UsageHourFact{}).Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", userID, start, start+6*usageFactHourSeconds).Count(&staged).Error; err != nil {
			t.Fatal(err)
		}
		if staged != 0 {
			t.Fatalf("dense wide shard committed %d staged rows before shrinking", staged)
		}
	}
	if err := m.syncOneUsageFactRawLiveMember(context.Background(), state, now); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if state.LiveThroughHour == nil || *state.LiveThroughHour != start || state.LiveStatus != "importing" {
		t.Fatalf("partial hot hour falsely advanced live cursor: %+v", state)
	}
	var page UsageFactPageIngestState
	if err := m.usageFactsStore().First(&page, "user_id = ? AND hour_ts = ?", userID, start).Error; err != nil {
		t.Fatal(err)
	}
	if page.Pages != usageFactRawPagesPerTurn || page.SourceRows != usageFactRawPagesPerTurn*usageFactRawPageSize {
		t.Fatalf("hot live turn exceeded/lost bounded pages: %+v", page)
	}
}
