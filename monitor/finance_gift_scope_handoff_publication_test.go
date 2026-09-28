//go:build unix

package monitor

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestFinanceGiftHandoffQueuedBehindNewPublication(t *testing.T) {
	db, other, target, evidence := handoffConcurrentFixture(t)
	ctx := context.Background()
	before, err := loadFinanceGiftScopeSnapshot(ctx, other, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	newEvents := append([]FinanceGiftBoundaryEvent(nil), evidence...)
	for i := range newEvents {
		newEvents[i].Group = "newer-source-correction"
		newEvents[i].EvidenceHash = financeGiftBoundaryEventHash(newEvents[i])
	}
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, tx, target.HourTs, target.UserID, time.Now().Unix(), target.SourceEpoch, newEvents); err != nil {
		t.Fatal(err)
	}
	// WAL readers must not see the uncommitted new groups.
	reading, err := loadFinanceGiftScopeSnapshot(ctx, other, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil || !reflect.DeepEqual(before, reading) {
		t.Fatal("reader saw uncommitted publication", err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	initialWaits := pool.Stats().WaitCount
	type answer struct {
		result financeGiftScopeBatchResult
		err    error
	}
	done := make(chan answer, 1)
	workerCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	go func() {
		result, err := executeFinanceGiftHandoff(workerCtx, db, "queued-publication", []financeGiftScopeTarget{target}, [][]FinanceGiftBoundaryEvent{evidence}, handoffNoWait, func(financeGiftScopeBatchEntry) error { return nil })
		done <- answer{result, err}
	}()
	deadline := time.Now().Add(time.Second)
	for pool.Stats().WaitCount == initialWaits && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pool.Stats().WaitCount == initialWaits {
		t.Fatal("repair did not queue behind normal publication")
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	var got answer
	select {
	case got = <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("queued repair stuck")
	}
	if got.err == nil || got.result.Status != "failed" || got.result.Entries[0].RowsUpdated != 0 {
		t.Fatal("old evidence overwrote newer publication", got)
	}
	current, err := loadFinanceGiftScopeSnapshot(ctx, other, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range current.Events {
		if event.Group != "newer-source-correction" || !event.GroupKnown {
			t.Fatal("newer groups lost")
		}
	}
	var commits int64
	if err := db.Model(&financeGiftHandoffCommit{}).Count(&commits).Error; err != nil || commits != 0 {
		t.Fatal("rejected stale work committed", commits, err)
	}
}
