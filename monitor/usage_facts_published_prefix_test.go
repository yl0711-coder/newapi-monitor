package monitor

import (
	"testing"
	"time"
)

// Production shape (user 159): source floor 10-09, first log 10-13 21:00, no
// facts or day proofs before that. The publication checkpoint must prove that
// prefix as known-empty, as the verify and local-audit jobs do, instead of
// demanding day proofs that by design never exist.
func TestPublishedHistoryAuditProvesEmptyPrefixBeforeFirstLog(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	const userID int64 = 159
	floor := time.Date(2026, 10, 9, 0, 0, 0, 0, usageCST).Unix()
	firstDay := time.Date(2026, 10, 13, 0, 0, 0, 0, usageCST).Unix()
	firstLog := firstDay + 21*usageFactHourSeconds
	through := firstDay + usageFactDaySeconds
	db := m.usageFactsStore()
	for hour := firstLog; hour < through; hour += usageFactHourSeconds {
		row := UsageHourFact{HourTs: hour, DayTs: firstDay, UserID: userID, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, TokenName: "t", Requests: 1, PromptTokens: 2, CompletionTokens: 3, ConsumeQuota: 5}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&UsageFactMemberHourState{
			UserID: userID, HourTs: hour, Status: "complete", Rows: 1, Requests: 1, Tokens: 5,
			ContentHash: usageFactContentHash([]UsageHourFact{row}), SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	first := firstLog
	state := UsageFactMemberState{UserID: userID, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, SourceHistoryStatus: "complete_hot", SourceFirstLogHour: &first}

	// The old checkpoint audited from the floor as proved days and failed.
	if err := auditUsageFactFullHistoryDayRangeWithRefresh(db, state, floor, through, true); err == nil {
		t.Fatal("fixture must reproduce the floor-as-proved-days failure")
	}
	if err := auditUsageFactPublishedHistoryWithRefresh(db, state, floor, through); err != nil {
		t.Fatalf("empty prefix before the first log must pass: %v", err)
	}

	// A fact inside the supposedly empty prefix is still caught.
	stray := UsageDailyFact{DateTs: floor + usageFactDaySeconds, UserID: userID, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, TokenName: "t", Requests: 1}
	if err := db.Create(&stray).Error; err != nil {
		t.Fatal(err)
	}
	if err := auditUsageFactPublishedHistoryWithRefresh(db, state, floor, through); err == nil {
		t.Fatal("a fact inside the empty prefix must fail the audit")
	}

	// Without a first-log boundary the audit cannot decide and must fail.
	state.SourceFirstLogHour = nil
	if err := auditUsageFactPublishedHistoryWithRefresh(db, state, floor, through); err == nil {
		t.Fatal("missing first-log boundary must fail")
	}
}
