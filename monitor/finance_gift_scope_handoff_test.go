//go:build unix

package monitor

import (
	"context"
	"reflect"
	"testing"
)

func giftHandoffFixture(t *testing.T) (*Monitor, financeGiftScopeTarget, []FinanceGiftBoundaryEvent) {
	t.Helper()
	m, source, hour := giftScopeRepairFixture(t)
	evidence, err := fetchFinanceGiftBoundaryEvents(context.Background(), source, hour, []int64{7})
	if err != nil {
		t.Fatal(err)
	}
	for i := range evidence {
		evidence[i].SourceEpoch = "v1"
	}
	return m, financeGiftScopeTarget{"v1", hour, 7}, evidence
}

func TestFinanceGiftHandoffPreservesNewDataAndReplay(t *testing.T) {
	m, target, evidence := giftHandoffFixture(t)
	db, ctx := m.usageFactsStore(), context.Background()
	// Data arrived after evidence extraction: another hour and a newer cursor.
	next := target.HourTs + 3600
	if _, err := replaceFinanceUserHourFacts(ctx, db, next, "v1", []FinanceUserHourFact{{HourTs: next, UserID: 7, Requests: 1, ConsumeQuota: 123}}, next+7200); err != nil {
		t.Fatal(err)
	}
	e := FinanceGiftBoundaryEvent{SourceLogID: 99, HourTs: next, UserID: 7, EventAt: next + 10, Kind: "usage", Quota: 123}
	e.EvidenceHash = financeGiftBoundaryEventHash(e)
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, next, 7, next+7200, "v1", []FinanceGiftBoundaryEvent{e}); err != nil {
		t.Fatal(err)
	}
	cursor := FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "v1", StartHourTs: target.HourTs, NextHourTs: next + 3600, Status: "backfilling"}
	if err := db.Create(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	newBefore, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", next, 7)
	if err != nil {
		t.Fatal(err)
	}
	otherBefore, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 8)
	if err != nil {
		t.Fatal(err)
	}
	var factsBefore []FinanceUserHourFact
	if err := db.Order("hour_ts,user_id").Find(&factsBefore).Error; err != nil {
		t.Fatal(err)
	}
	fullBefore, err := m.financeReportSourceFingerprint(ctx, next, next+3600)
	if err != nil {
		t.Fatal(err)
	}
	baseBefore, err := m.financeReportPeriodSourceFingerprint(ctx, next, next+3600)
	if err != nil {
		t.Fatal(err)
	}
	result, err := applyFinanceGiftScopeHandoff(ctx, db, target, evidence, financeGiftBoundaryContentHash(evidence), next+8000)
	if err != nil || result.RowsUpdated != 3 {
		t.Fatalf("handoff=%+v %v", result, err)
	}
	completed, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
	if err != nil {
		t.Fatal(err)
	}
	result, err = applyFinanceGiftScopeHandoff(ctx, db, target, evidence, financeGiftBoundaryContentHash(evidence), next+9000)
	if err != nil || result.RowsUpdated != 0 {
		t.Fatalf("repeat=%+v %v", result, err)
	}
	after, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
	if err != nil || !reflect.DeepEqual(completed, after) {
		t.Fatal("replay changed target", err)
	}
	newAfter, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", next, 7)
	if err != nil || !reflect.DeepEqual(newBefore, newAfter) {
		t.Fatal("new hour lost", err)
	}
	otherAfter, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 8)
	if err != nil || !reflect.DeepEqual(otherBefore, otherAfter) {
		t.Fatal("other user changed", err)
	}
	var factsAfter []FinanceUserHourFact
	if err := db.Order("hour_ts,user_id").Find(&factsAfter).Error; err != nil || !reflect.DeepEqual(factsBefore, factsAfter) {
		t.Fatal("amounts changed", err)
	}
	var cursorAfter FinanceFactSyncState
	if err := db.First(&cursorAfter, financeFactSyncStateID).Error; err != nil || cursorAfter != cursor {
		t.Fatal("cursor changed", err)
	}
	fullAfter, err := m.financeReportSourceFingerprint(ctx, next, next+3600)
	if err != nil || fullAfter == fullBefore {
		t.Fatal("full cache not invalidated", err)
	}
	baseAfter, err := m.financeReportPeriodSourceFingerprint(ctx, next, next+3600)
	if err != nil || baseAfter != baseBefore {
		t.Fatal("base cache invalidated", err)
	}
}

func TestFinanceGiftHandoffRejectsChangedEvidence(t *testing.T) {
	for _, scenario := range []string{"epoch", "amount", "identity", "missing", "duplicate", "unknown", "hash", "confirmation", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			m, target, evidence := giftHandoffFixture(t)
			db, ctx := m.usageFactsStore(), context.Background()
			before, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "epoch":
				evidence[0].SourceEpoch = "other"
			case "amount":
				evidence[0].Quota++
				evidence[1].Quota--
			case "identity":
				evidence[0].SourceLogID = 999
			case "missing":
				evidence = evidence[:2]
			case "duplicate":
				evidence[1] = evidence[0]
			case "unknown":
				evidence[0].GroupKnown = false
			}
			for i := range evidence {
				evidence[i].EvidenceHash = financeGiftBoundaryEventHash(evidence[i])
			}
			if scenario == "hash" {
				evidence[0].EvidenceHash = "wrong"
			}
			digest := financeGiftBoundaryContentHash(evidence)
			if scenario == "confirmation" {
				digest = "wrong"
			}
			if scenario == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			result, err := applyFinanceGiftScopeHandoff(ctx, db, target, evidence, digest, target.HourTs+8000)
			if err == nil || result.RowsUpdated != 0 {
				t.Fatalf("unsafe evidence accepted %+v %v", result, err)
			}
			after, err := loadFinanceGiftScopeSnapshot(context.Background(), db, "v1", target.HourTs, 7)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("rejection changed receiver", err)
			}
		})
	}
}

func TestFinanceGiftHandoffVerifiedGroupConflictAndRollback(t *testing.T) {
	for _, scenario := range []string{"known-conflict", "write-failure", "partial-match"} {
		t.Run(scenario, func(t *testing.T) {
			m, target, evidence := giftHandoffFixture(t)
			db, ctx := m.usageFactsStore(), context.Background()
			current, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "write-failure" {
				current.Events[0] = evidence[0]
				if scenario == "known-conflict" {
					current.Events = append([]FinanceGiftBoundaryEvent(nil), evidence...)
					current.Events[0].Group = "changed-by-newer-sync"
					current.Events[0].EvidenceHash = financeGiftBoundaryEventHash(current.Events[0])
				}
				if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, target.HourTs, 7, target.HourTs+7900, "v1", current.Events); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := db.Exec(`CREATE TRIGGER reject_handoff BEFORE INSERT ON finance_gift_boundary_events WHEN NEW.group_known=1 BEGIN SELECT RAISE(ABORT,'test failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			}
			before, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
			if err != nil {
				t.Fatal(err)
			}
			result, err := applyFinanceGiftScopeHandoff(ctx, db, target, evidence, financeGiftBoundaryContentHash(evidence), target.HourTs+8000)
			if scenario == "partial-match" {
				if err != nil || result.RowsUpdated != 2 {
					t.Fatalf("partial %+v %v", result, err)
				}
				return
			}
			if err == nil || result.RowsUpdated != 0 {
				t.Fatalf("conflict/rollback %+v %v", result, err)
			}
			after, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("atomicity failed", err)
			}
		})
	}
}

func TestFinanceGiftHandoffRejectsUnboundedOrOpenTargets(t *testing.T) {
	for _, scenario := range []string{"empty", "oversize", "open-hour", "invalid-user"} {
		t.Run(scenario, func(t *testing.T) {
			m, target, evidence := giftHandoffFixture(t)
			now := target.HourTs + 8000
			switch scenario {
			case "empty":
				evidence = nil
			case "oversize":
				evidence = make([]FinanceGiftBoundaryEvent, financeGiftLocalMaxRows+1)
			case "open-hour":
				now = target.HourTs + 1800
			case "invalid-user":
				target.UserID = 0
			}
			result, err := applyFinanceGiftScopeHandoff(context.Background(), m.usageFactsStore(), target, evidence, financeGiftBoundaryContentHash(evidence), now)
			if err == nil || result.RowsUpdated != 0 {
				t.Fatalf("invalid budget/target accepted: %+v %v", result, err)
			}
		})
	}
}
