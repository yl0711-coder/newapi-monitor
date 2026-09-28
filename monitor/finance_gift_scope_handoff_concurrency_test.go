//go:build unix

package monitor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gorm.io/gorm"
)

func handoffConcurrentFixture(t *testing.T) (*gorm.DB, *gorm.DB, financeGiftScopeTarget, []FinanceGiftBoundaryEvent) {
	t.Helper()
	m, target, evidence := giftHandoffFixture(t)
	db := m.usageFactsStore()
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&financeGiftHandoffCommit{}); err != nil {
		t.Fatal(err)
	}
	var databases []struct{ Name, File string }
	if err := db.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		t.Fatal(err)
	}
	for _, item := range databases {
		if item.Name != "main" {
			continue
		}
		other, closeDB, err := giftLocalDatabase(item.File)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeDB)
		return db, other, target, evidence
	}
	t.Fatal("missing disk fixture")
	return nil, nil, target, nil
}

func TestFinanceGiftHandoffConcurrentWriterYieldsAndResumes(t *testing.T) {
	db, other, target, evidence := handoffConcurrentFixture(t)
	before, err := loadFinanceGiftScopeSnapshot(context.Background(), db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		t.Fatal(err)
	}
	tx := other.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Exec("UPDATE finance_user_hour_states SET updated_at=updated_at").Error; err != nil {
		t.Fatal(err)
	}
	var audit []financeGiftScopeBatchEntry
	appendEntry := func(e financeGiftScopeBatchEntry) error { audit = append(audit, e); return nil }
	started := time.Now()
	result, err := executeFinanceGiftHandoff(context.Background(), db, "concurrency-test", []financeGiftScopeTarget{target}, [][]FinanceGiftBoundaryEvent{evidence}, handoffNoWait, appendEntry)
	if err == nil || result.Status != "paused" || result.Remaining != 1 || len(audit) != 1 || audit[0].RowsUpdated != 0 || audit[0].ErrorCode != "handoff_store_busy" {
		t.Fatal("contention must yield without retry", result, audit, err)
	}
	if time.Since(started) > financeGiftHandoffWriteBudget+time.Second {
		t.Fatal("contention did not yield promptly")
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	after, err := loadFinanceGiftScopeSnapshot(context.Background(), db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("busy attempt changed facts", err)
	}
	var count int64
	if err := db.Model(&financeGiftHandoffCommit{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("busy attempt left durable commit", count, err)
	}
	result, err = executeFinanceGiftHandoff(context.Background(), db, "concurrency-test", []financeGiftScopeTarget{target}, [][]FinanceGiftBoundaryEvent{evidence}, handoffNoWait, appendEntry)
	if err != nil || result.Status != "complete" || result.Entries[0].RowsUpdated != 3 {
		t.Fatal("explicit resume failed", result, err)
	}
}

func TestFinanceGiftHandoffPoolWaitIsBounded(t *testing.T) {
	for _, phase := range []string{"inspect", "commit"} {
		t.Run(phase, func(t *testing.T) {
			db, other, target, evidence := handoffConcurrentFixture(t)
			tx := db.Begin() // Normal publisher owns the only pooled connection.
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			defer tx.Rollback()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started := time.Now()
			var err error
			if phase == "inspect" {
				_, err = inspectFinanceGiftHandoffWithBudget(ctx, db, target, evidence)
			} else {
				_, err = applyFinanceGiftHandoffWithCommit(ctx, db, "pool-test", 0, target, evidence, time.Now().Unix())
			}
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > financeGiftHandoffWriteBudget+time.Second || ctx.Err() != nil {
				t.Fatal("phase exceeded its own budget", time.Since(started), err)
			}
			// Independent WAL reader still sees valid, unchanged facts.
			proof, err := loadFinanceGiftScopeSnapshot(context.Background(), other, target.SourceEpoch, target.HourTs, target.UserID)
			if err != nil || proof.Events[0].GroupKnown {
				t.Fatal("waiting repair modified facts", err)
			}
		})
	}
}

func handoffPublishTestHour(ctx context.Context, db *gorm.DB, hour int64, quota int64) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := replaceFinanceUserHourFacts(ctx, tx, hour, "v1", []FinanceUserHourFact{{HourTs: hour, UserID: 7, Requests: 1, ConsumeQuota: quota}}, hour+7200); err != nil {
			return err
		}
		e := FinanceGiftBoundaryEvent{SourceLogID: 999, HourTs: hour, UserID: 7, EventAt: hour + 20, Kind: "usage", Quota: quota, Group: "business", GroupKnown: true}
		e.EvidenceHash = financeGiftBoundaryEventHash(e)
		_, err := replaceFinanceGiftBoundaryUserHour(ctx, tx, hour, 7, hour+7200, "v1", []FinanceGiftBoundaryEvent{e})
		return err
	})
}

func TestFinanceGiftHandoffCooldownAndAuditReleaseWriter(t *testing.T) {
	db, other, target, evidence := handoffConcurrentFixture(t)
	ctx := context.Background()
	second := target
	second.HourTs += 3600
	if err := handoffPublishTestHour(ctx, db, second.HourTs, 77); err != nil {
		t.Fatal(err)
	}
	proof, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", second.HourTs, 7)
	if err != nil {
		t.Fatal(err)
	}
	verified := append([]FinanceGiftBoundaryEvent(nil), proof.Events...)
	proof.Events[0].GroupKnown = false
	proof.Events[0].Group = ""
	proof.Events[0].EvidenceHash = financeGiftBoundaryEventHash(proof.Events[0])
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, second.HourTs, 7, second.HourTs+8000, "v1", proof.Events); err != nil {
		t.Fatal(err)
	}
	normalHour := second.HourTs + 3600
	// Use a different log ID so the new hour cannot replace the second target.
	if err := other.Exec("CREATE TABLE handoff_normal_progress (id INTEGER PRIMARY KEY, value INTEGER)").Error; err != nil {
		t.Fatal(err)
	}
	if err := other.Exec("INSERT INTO handoff_normal_progress VALUES (1,0)").Error; err != nil {
		t.Fatal(err)
	}
	waits, audits := 0, 0
	checkFree := func() error {
		bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		// Both same-pool reads and independent writes must proceed outside the repair transaction.
		var count int64
		if err := db.WithContext(bounded).Model(&FinanceUserHourState{}).Count(&count).Error; err != nil {
			return err
		}
		return other.WithContext(bounded).Exec("UPDATE handoff_normal_progress SET value=value+1 WHERE id=1").Error
	}
	result, err := executeFinanceGiftHandoff(ctx, db, "cooldown-test", []financeGiftScopeTarget{target, second}, [][]FinanceGiftBoundaryEvent{evidence, verified}, func(context.Context, time.Duration) error { waits++; return checkFree() }, func(financeGiftScopeBatchEntry) error { audits++; return checkFree() })
	if err != nil || result.Status != "complete" || waits != 1 || audits != 2 {
		t.Fatal("cooldown/audit held writer", result, waits, audits, err)
	}
	// Normal fact publication after the repair still works with its original API.
	if _, err := replaceFinanceUserHourFacts(ctx, db, normalHour, "v1", []FinanceUserHourFact{{HourTs: normalHour, UserID: 7, Requests: 1, ConsumeQuota: 123}}, normalHour+7200); err != nil {
		t.Fatal(err)
	}
}
