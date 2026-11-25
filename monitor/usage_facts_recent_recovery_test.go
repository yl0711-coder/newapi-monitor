package monitor

import (
	"testing"
	"time"
)

func TestRecentServiceAuditKnownEmptyPrefixRecovery(t *testing.T) {
	for _, scenario := range []string{"empty prefix", "no history", "missing boundary", "contradictory prefix", "missing hour", "wrong epoch", "bad hash"} {
		t.Run(scenario, func(t *testing.T) {
			m := newUsageHistoryTestMonitor(t)
			db := m.usageFactsStore()
			day := time.Date(2026, 11, 25, 0, 0, 0, 0, usageCST).Unix()
			const uid int64 = 6901
			state := UsageFactMemberState{UserID: uid, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
				SourceHistoryStatus: "complete_hot", SourceFirstLogHour: int64Ptr(day), LiveFromHour: int64Ptr(day - 3*usageFactDaySeconds)}
			proof := UsageFactMemberHourState{UserID: uid, HourTs: day, Status: "complete", SourceEpoch: state.SourceEpoch, ContentHash: usageFactContentHash(nil)}
			switch scenario {
			case "no history":
				state.SourceHistoryStatus, state.SourceFirstLogHour = "no_history", nil
			case "missing boundary":
				state.SourceFirstLogHour = nil
			case "contradictory prefix":
				if err := db.Create(&UsageHourFact{UserID: uid, HourTs: day - usageFactHourSeconds, DayTs: day - usageFactDaySeconds, Requests: 1}).Error; err != nil {
					t.Fatal(err)
				}
			case "wrong epoch":
				proof.SourceEpoch = "other-source"
			case "bad hash":
				proof.ContentHash = "incorrect"
			}
			if scenario != "missing hour" {
				if err := db.Create(&proof).Error; err != nil {
					t.Fatal(err)
				}
			}
			err := auditUsageFactRecentServiceRange(db, state, day+usageFactHourSeconds)
			wantOK := scenario == "empty prefix" || scenario == "no history"
			if (err == nil) != wantOK {
				t.Fatalf("success=%v, want=%v: %v", err == nil, wantOK, err)
			}
		})
	}
}
