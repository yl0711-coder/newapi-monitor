//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
	"gorm.io/gorm"
)

func giftRelevantScopeFixture(t *testing.T, refund, grant bool) (*Monitor, int64) {
	t.Helper()
	m, hour := giftScopeHorizonFixture(t, refund, grant)
	if err := m.storeDB.Create(&ChannelBusinessGroupPolicy{Grp: "test", Included: false}).Error; err != nil {
		t.Fatal(err)
	}
	return m, hour
}

func TestFinanceGiftRelevantScopePlanSkipsOnlyProvenIrrelevantHours(t *testing.T) {
	m, hour := giftRelevantScopeFixture(t, false, false)
	db, ctx := m.usageFactsStore(), context.Background()
	var events []FinanceGiftBoundaryEvent
	if err := db.Where("hour_ts=? AND user_id=?", hour+3600, 7).Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	for i := range events {
		events[i].GroupKnown = false
		events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
	}
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, hour+3600, 7, 30001, "epoch", events); err != nil {
		t.Fatal(err)
	}
	before := giftLocalFileHash(t, m.cfg.StorePath)
	result, err := inspectFinanceGiftRelevantScope(ctx, m, "epoch", hour, hour+14400)
	if err != nil || result.Coverage.Complete || result.Coverage.ScopeUnknownEvents != 1 || result.Coverage.ScopeIndependentUserHours != 1 {
		t.Fatalf("gift relevance proof changed: %+v %v", result, err)
	}
	if result.Candidates.Status != "ready" || result.Candidates.Rows != 1 || len(result.Candidates.Entries) != 1 || result.Candidates.Entries[0].HourTs != hour+3600 || result.ReadPlan == nil {
		t.Fatalf("plan selected a gift-neutral hour or lost the relevant hour: %+v", result)
	}
	digest, err := financegiftexport.BatchConfirmation(*result.ReadPlan)
	if err != nil || digest != result.Confirmation {
		t.Fatal("invalid existing exporter handoff", err)
	}
	repeated, err := inspectFinanceGiftRelevantScope(ctx, m, "epoch", hour, hour+14400)
	if err != nil || !reflect.DeepEqual(result, repeated) || giftLocalFileHash(t, m.cfg.StorePath) != before {
		t.Fatal("read-only scope planning changed data or is nondeterministic", err)
	}
}

func TestFinanceGiftRelevantScopePlanRetainsRefundAndLaterGrant(t *testing.T) {
	for _, tc := range []struct {
		name          string
		refund, grant bool
	}{{"refund", true, false}, {"later_grant", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			m, hour := giftRelevantScopeFixture(t, tc.refund, tc.grant)
			result, err := inspectFinanceGiftRelevantScope(context.Background(), m, "epoch", hour, hour+14400)
			if err != nil || result.ReadPlan == nil || len(result.ReadPlan.Targets) != 1 || result.ReadPlan.Targets[0].HourTs != hour+10800 || result.Coverage.ScopeIndependentUserHours != 0 {
				t.Fatalf("refundable/reopened wallet lost its repair target: %+v %v", result, err)
			}
		})
	}
}

func TestFinanceGiftRelevantScopePlanMixedCacheHitsKeepEarliestTargets(t *testing.T) {
	m, hour := giftRelevantScopeFixture(t, true, false)
	db, ctx := m.usageFactsStore(), context.Background()
	var events []FinanceGiftBoundaryEvent
	if err := db.Where("hour_ts=? AND user_id=?", hour+3600, 7).Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	for i := range events {
		events[i].GroupKnown = false
		events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
	}
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, hour+3600, 7, 30001, "epoch", events); err != nil {
		t.Fatal(err)
	}
	cold, err := inspectFinanceGiftRelevantScope(ctx, m, "epoch", hour, hour+14400)
	if err != nil || cold.ReadPlan == nil || len(cold.ReadPlan.Targets) != 2 {
		t.Fatal("fixture lacks two relevant gaps", cold, err)
	}
	var state FinanceGiftBoundaryState
	if err := db.First(&state, "hour_ts=? AND user_id=?", hour+3600, 7).Error; err != nil {
		t.Fatal(err)
	}
	// A real mixed hit/miss, without a DB publication changing all cache hints.
	m.getFinanceGiftEvidenceCache().Delete(financeGiftEvidenceKey(state, cold.ScopeSHA256))
	mixed, err := inspectFinanceGiftRelevantScope(ctx, m, "epoch", hour, hour+14400)
	if err != nil || !reflect.DeepEqual(cold, mixed) {
		t.Fatalf("cache hit ordering changed bounded targets or their confirmation: cold=%+v mixed=%+v err=%v", cold.ReadPlan, mixed.ReadPlan, err)
	}
}

func TestFinanceGiftRelevantScopePlanEmptyIsNotRawHistoryComplete(t *testing.T) {
	m, hour := giftRelevantScopeFixture(t, false, false)
	result, err := inspectFinanceGiftRelevantScope(context.Background(), m, "epoch", hour, hour+14400)
	if err != nil || !result.Coverage.Complete || result.Candidates.Status != "empty" || result.ReadPlan != nil || result.Confirmation != "" || result.Coverage.ScopeIndependentUserHours != 1 {
		t.Fatalf("gift-neutral gaps became a raw repair claim: %+v %v", result, err)
	}
	var unknown int64
	if err := m.usageFactsStore().Model(&FinanceGiftBoundaryEvent{}).Where("group_known=?", false).Count(&unknown).Error; err != nil || unknown != 1 {
		t.Fatal("scope inspection rewrote raw history", unknown, err)
	}
}

func TestFinanceGiftRelevantScopePlanRejectsIncompleteAndUnsafeInputs(t *testing.T) {
	for _, scenario := range []string{"failed_hour", "wrong_epoch", "cancel", "invalid_range", "corrupt_content", "later_origin", "open_range", "overlong_range"} {
		t.Run(scenario, func(t *testing.T) {
			m, hour := giftRelevantScopeFixture(t, true, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			epoch, from, to := "epoch", hour, hour+14400
			switch scenario {
			case "failed_hour":
				if err := m.usageFactsStore().Model(&FinanceGiftBoundaryState{}).Where("hour_ts=?", hour+10800).Update("status", "failed").Error; err != nil {
					t.Fatal(err)
				}
			case "wrong_epoch":
				epoch = "another-epoch"
			case "cancel":
				cancel()
			case "invalid_range":
				to++
			case "later_origin":
				from += 3600
			case "open_range":
				to = time.Now().Unix()/3600*3600 + 3600
			case "overlong_range":
				from = to - (financeReportMaxDays*24+1)*3600
			case "corrupt_content":
				if err := m.usageFactsStore().Model(&FinanceGiftBoundaryEvent{}).Where("hour_ts=?", hour+10800).Update("quota", 1).Error; err != nil {
					t.Fatal(err)
				}
			}
			result, err := inspectFinanceGiftRelevantScope(ctx, m, epoch, from, to)
			if result.ReadPlan != nil || result.Confirmation != "" {
				t.Fatalf("unsafe/incomplete evidence issued a read plan: %+v %v", result, err)
			}
			if scenario == "failed_hour" {
				if err != nil || result.Candidates.Status != "blocked" {
					t.Fatal("missing proof was called exhausted", result, err)
				}
			} else if err == nil {
				t.Fatal("unsafe scope inspection accepted", scenario)
			}
		})
	}
}

func TestFinanceGiftRelevantCandidatesRetainWholeHourBudgetsAndEpochBoundaries(t *testing.T) {
	backup, _ := giftLargeLocalFixture(t, 11, 1500, 1501, 3001)
	db, closeDB, err := giftLocalReadonlyDatabase(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var states []FinanceGiftBoundaryState
	if err := db.Order("hour_ts,user_id").Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	ctx, now := context.Background(), time.Now().Unix()
	result, err := giftRelevantLocalCandidates(ctx, db, states, "v1", now)
	if err != nil || result.Rows != 1511 || len(result.Entries) != 2 || result.StopReason != "row_budget" {
		t.Fatalf("whole-hour row budget changed: %+v %v", result, err)
	}
	for _, tc := range []struct{ epoch, reason string }{{"v1", "whole_hour_exceeds_row_limit"}, {"another", "source_epoch_boundary"}} {
		result, err := giftRelevantLocalCandidates(ctx, db, states[3:], tc.epoch, now)
		if err != nil || result.Status != "blocked" || result.StopReason != tc.reason || len(result.Entries) != 0 {
			t.Fatalf("blocked target was split or silently skipped: %+v %v", result, err)
		}
	}
	changed := states[0]
	changed.ContentHash = "changed-between-proof-reads"
	if _, err := giftRelevantLocalCandidates(ctx, db, []FinanceGiftBoundaryState{changed}, "v1", now); err == nil {
		t.Fatal("changed selected proof was accepted")
	}
}

func TestFinanceGiftRelevantCandidatesTargetLimit(t *testing.T) {
	backup, _ := giftLargeLocalFixture(t, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	db, closeDB, err := giftLocalReadonlyDatabase(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var states []FinanceGiftBoundaryState
	if err := db.Order("hour_ts,user_id").Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	result, err := giftRelevantLocalCandidates(context.Background(), db, states, "v1", time.Now().Unix())
	if err != nil || len(result.Entries) != financeGiftScopeBatchLimit || result.Rows != financeGiftScopeBatchLimit || result.StopReason != "target_limit" {
		t.Fatal("existing finite target limit changed", result, err)
	}
}

func TestFinanceGiftRelevantGapRetentionIsBoundedAndChronological(t *testing.T) {
	var kept []FinanceGiftBoundaryState
	for hour := int64(20); hour >= 1; hour-- {
		for user := int64(2); user >= 1; user-- {
			kept = retainEarliestFinanceGiftScopeGap(kept, FinanceGiftBoundaryState{HourTs: hour * 3600, UserID: user})
			if len(kept) > financeGiftScopeBatchLimit+1 {
				t.Fatal("unbounded gap metadata retained")
			}
		}
	}
	for index, state := range kept {
		if state.HourTs != int64(index/2+1)*3600 || state.UserID != int64(index%2+1) {
			t.Fatalf("earlier blocking target was dropped: index=%d state=%+v", index, state)
		}
	}
}

func TestFinanceGiftRelevantBackupsAreReadOnlyAndClosed(t *testing.T) {
	m, hour := giftRelevantScopeFixture(t, false, false)
	db := m.usageFactsStore()
	for i := int64(4); i < 24; i++ {
		h := hour + i*3600
		if _, err := replaceFinanceUserHourFacts(context.Background(), db, h, "epoch", nil, 30000); err != nil {
			t.Fatal(err)
		}
		if _, err := replaceFinanceCreditHour(context.Background(), db, h, 30000, "epoch", financeCreditHourFetch{}); err != nil {
			t.Fatal(err)
		}
	}
	mainPath, factsPath := filepath.Join(t.TempDir(), "main.db"), filepath.Join(t.TempDir(), "facts.db")
	for _, target := range []struct {
		db   *gorm.DB
		path string
	}{{m.storeDB, mainPath}, {db, factsPath}} {
		if err := target.db.Exec("VACUUM INTO ?", target.path).Error; err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(target.path, 0600); err != nil {
			t.Fatal(err)
		}
	}
	beforeMain, beforeFacts := giftLocalFileHash(t, mainPath), giftLocalFileHash(t, factsPath)
	result, err := InspectFinanceGiftRelevantScopeBackups(context.Background(), mainPath, factsPath, "epoch", "2026-05-01", "2026-05-02")
	if err != nil || !result.Coverage.Complete || result.Candidates.Status != "empty" || result.ScopeSHA256 == "" || result.ReadPlan != nil {
		t.Fatalf("read-only complete range inspection failed: %+v %v", result, err)
	}
	if giftLocalFileHash(t, mainPath) != beforeMain || giftLocalFileHash(t, factsPath) != beforeFacts {
		t.Fatal("inspection changed original backups")
	}
	for _, path := range []string{mainPath, factsPath} {
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
				t.Fatal("inspection created a SQLite sidecar", err)
			}
		}
	}
	link := filepath.Join(t.TempDir(), "link.db")
	if err := os.Symlink(factsPath, link); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectFinanceGiftRelevantScopeBackups(context.Background(), mainPath, link, "epoch", "2026-05-01", "2026-05-02"); err == nil {
		t.Fatal("symlink input accepted")
	}
	if err := os.WriteFile(factsPath+"-wal", []byte("not-closed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectFinanceGiftRelevantScopeBackups(context.Background(), mainPath, factsPath, "epoch", "2026-05-01", "2026-05-02"); err == nil {
		t.Fatal("unclosed backup accepted")
	}
}
