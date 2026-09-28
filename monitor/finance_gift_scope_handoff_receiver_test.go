//go:build unix

package monitor

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"
)

func TestFinanceGiftHandoffRejectsChangedReceiver(t *testing.T) {
	for _, scenario := range []string{"late-row", "redistributed-money", "new-epoch", "known-aggregate-changed"} {
		t.Run(scenario, func(t *testing.T) {
			m, target, evidence := giftHandoffFixture(t)
			db, ctx := m.usageFactsStore(), context.Background()
			current, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "late-row":
				var facts []FinanceUserHourFact
				if err := db.Where("hour_ts=?", target.HourTs).Find(&facts).Error; err != nil {
					t.Fatal(err)
				}
				for i := range facts {
					if facts[i].UserID == 7 {
						facts[i].Requests++
						facts[i].ConsumeQuota += 123
					}
				}
				if _, err := replaceFinanceUserHourFacts(ctx, db, target.HourTs, "v1", facts, target.HourTs+7900); err != nil {
					t.Fatal(err)
				}
				e := FinanceGiftBoundaryEvent{SourceLogID: 99, HourTs: target.HourTs, UserID: 7, EventAt: target.HourTs + 60, Kind: "usage", Quota: 123}
				e.EvidenceHash = financeGiftBoundaryEventHash(e)
				current.Events = append(current.Events, e)
			case "redistributed-money":
				current.Events[0].Quota++
				current.Events[1].Quota--
				for i := range current.Events {
					current.Events[i].EvidenceHash = financeGiftBoundaryEventHash(current.Events[i])
				}
			case "new-epoch":
				if err := db.Model(&FinanceUserHourState{}).Where("hour_ts=?", target.HourTs).Update("source_epoch", "v2").Error; err != nil {
					t.Fatal(err)
				}
			case "known-aggregate-changed":
				current.Events = append([]FinanceGiftBoundaryEvent(nil), evidence...)
			}
			if scenario != "new-epoch" {
				if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, target.HourTs, 7, target.HourTs+7900, "v1", current.Events); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "known-aggregate-changed" {
				if err := db.Model(&FinanceUserHourFact{}).Where("hour_ts=? AND user_id=?", target.HourTs, 7).Update("consume_quota", 1).Error; err != nil {
					t.Fatal(err)
				}
			}
			var before, after []FinanceGiftBoundaryEvent
			if err := db.Order("source_epoch,source_log_id").Find(&before).Error; err != nil {
				t.Fatal(err)
			}
			result, err := applyFinanceGiftScopeHandoff(ctx, db, target, evidence, financeGiftBoundaryContentHash(evidence), target.HourTs+8000)
			if err == nil || result.RowsUpdated != 0 {
				t.Fatalf("stale handoff accepted %+v %v", result, err)
			}
			if err := db.Order("source_epoch,source_log_id").Find(&after).Error; err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("changed receiver overwritten", err)
			}
		})
	}
}

func TestFinanceGiftHandoffWriteContentionStopsWithoutMutation(t *testing.T) {
	m, target, evidence := giftHandoffFixture(t)
	db, ctx := m.usageFactsStore(), context.Background()
	before, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
	if err != nil {
		t.Fatal(err)
	}
	var databases []struct {
		Name string
		File string
	}
	if err := db.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		t.Fatal(err)
	}
	var path string
	for _, item := range databases {
		if item.Name == "main" {
			path = item.File
		}
	}
	if path == "" {
		t.Fatal("expected on-disk fixture")
	}
	other, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	conn, err := other.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }() // Also release the lock on test failure.
	bounded, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	result, err := applyFinanceGiftScopeHandoff(bounded, db, target, evidence, financeGiftBoundaryContentHash(evidence), target.HourTs+8000)
	if err == nil || result.RowsUpdated != 0 {
		t.Fatalf("contention should stop: %+v %v", result, err)
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	after, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", target.HourTs, 7)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("contention partially changed data", err)
	}
	result, err = applyFinanceGiftScopeHandoff(ctx, db, target, evidence, financeGiftBoundaryContentHash(evidence), target.HourTs+8000)
	if err != nil || result.RowsUpdated != 3 {
		t.Fatalf("explicit retry failed %+v %v", result, err)
	}
}
