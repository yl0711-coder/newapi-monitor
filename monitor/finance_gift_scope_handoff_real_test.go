//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Opt-in acceptance: all writes go to a NEW testing temp copy. Inputs remain
// readonly. The added later hour simulates post-backup ingestion, not a real
// production event. No production connection or credentials are accepted.
func TestFinanceGiftHandoffRealSnapshot(t *testing.T) {
	job, digest, backup := os.Getenv("MONITOR_GIFT_HANDOFF_JOB"), os.Getenv("MONITOR_GIFT_HANDOFF_CONFIRM"), os.Getenv("MONITOR_GIFT_HANDOFF_RECEIVER")
	if job == "" || digest == "" || backup == "" {
		t.Skip("requires private verified job, confirmation and closed receiver backup")
	}
	ctx := context.Background()
	progress, err := InspectFinanceGiftLocalJob(ctx, job, digest)
	if err != nil || progress.Status != "complete" {
		t.Fatalf("unverified input: %+v %v", progress, err)
	}
	lock, err := giftLocalLock(job)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	plan, evidence, err := giftLocalLoadConfirmedInputs(job, digest)
	if err != nil {
		t.Fatal(err)
	}
	source, closeSource, err := giftLocalReadonlyDatabase(filepath.Join(job, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSource()
	if err := giftLocalCheckEvidence(ctx, source, plan, evidence); err != nil {
		t.Fatal(err)
	}
	originalHash := giftLocalFileHash(t, backup)
	copyPath := filepath.Join(t.TempDir(), "receiver.db")
	if _, err := giftLocalCopyBackup(backup, copyPath); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var latest, maxID int64
	if err := db.Model(&FinanceUserHourState{}).Select("COALESCE(MAX(hour_ts),0)").Scan(&latest).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&FinanceGiftBoundaryEvent{}).Select("COALESCE(MAX(source_log_id),0)").Scan(&maxID).Error; err != nil {
		t.Fatal(err)
	}
	epoch, user, next := plan.Targets[0].SourceEpoch, plan.Targets[0].UserID, latest+3600
	if _, err := replaceFinanceUserHourFacts(ctx, db, next, epoch, []FinanceUserHourFact{{HourTs: next, UserID: user, Requests: 1, ConsumeQuota: 123}}, next+7200); err != nil {
		t.Fatal(err)
	}
	newEvent := FinanceGiftBoundaryEvent{SourceLogID: maxID + 1, HourTs: next, UserID: user, EventAt: next + 10, Kind: "usage", Quota: 123}
	newEvent.EvidenceHash = financeGiftBoundaryEventHash(newEvent)
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, next, user, next+7200, epoch, []FinanceGiftBoundaryEvent{newEvent}); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&FinanceFactSyncState{}).Where("id=?", financeFactSyncStateID).Updates(map[string]any{"next_hour_ts": next + 3600, "updated_at": next + 7200}).Error; err != nil {
		t.Fatal(err)
	}
	newBefore, err := loadFinanceGiftScopeSnapshot(ctx, db, epoch, next, user)
	if err != nil {
		t.Fatal(err)
	}
	moneyBefore := giftMixedMonetarySnapshot(t, db)
	var unknownBefore int64
	if err := db.Model(&FinanceGiftBoundaryEvent{}).Where("COALESCE(group_known,0)=0").Count(&unknownBefore).Error; err != nil {
		t.Fatal(err)
	}
	updated := 0
	for attempt := 0; attempt < 2; attempt++ {
		for _, target := range plan.Targets {
			verified, err := loadFinanceGiftScopeSnapshot(ctx, source, target.SourceEpoch, target.HourTs, target.UserID)
			if err != nil {
				t.Fatal(err)
			}
			prior, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := applyFinanceGiftScopeHandoff(ctx, db, target, verified.Events, verified.State.ContentHash, next+8000)
			if err != nil {
				t.Fatal(err)
			}
			if attempt == 0 {
				updated += result.RowsUpdated
			} else if result.RowsUpdated != 0 {
				t.Fatal("replay wrote rows")
			}
			after, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
			if err != nil || !reflect.DeepEqual(after.Events, verified.Events) {
				t.Fatal("handoff differs from verified evidence", err)
			}
			if attempt == 1 && !reflect.DeepEqual(prior, after) {
				t.Fatal("replay changed state")
			}
		}
	}
	newAfter, err := loadFinanceGiftScopeSnapshot(ctx, db, epoch, next, user)
	if err != nil || !reflect.DeepEqual(newBefore, newAfter) {
		t.Fatal("new data overwritten", err)
	}
	if giftMixedMonetarySnapshot(t, db) != moneyBefore {
		t.Fatal("monetary data/cursors changed")
	}
	var unknownAfter int64
	if err := db.Model(&FinanceGiftBoundaryEvent{}).Where("COALESCE(group_known,0)=0").Count(&unknownAfter).Error; err != nil {
		t.Fatal(err)
	}
	if int64(updated) != unknownBefore-unknownAfter || updated != progress.Rows {
		t.Fatal("scope improvement mismatch", updated, unknownBefore, unknownAfter)
	}
	if giftLocalFileHash(t, backup) != originalHash {
		t.Fatal("receiver backup changed")
	}
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("integrity", err)
	}
	t.Logf("verified handoff: %d rows / %d targets, unknown %d -> %d (includes one synthetic later event); replay zero, new hour and cursor retained, original receiver unchanged", updated, len(plan.Targets), unknownBefore, unknownAfter)
}
