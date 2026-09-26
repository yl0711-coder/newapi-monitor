//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Synthetic large-hour evidence; never presented as a production sample.
func giftLargeLocalFixture(t *testing.T, counts ...int) (string, []string) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "large-gift.example")
	db, ctx := m.usageFactsStore(), context.Background()
	hour, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var paths []string
	for userIndex, count := range counts {
		user, start := int64(userIndex+7), hour+int64(userIndex)*3600
		fact := FinanceUserHourFact{HourTs: start, UserID: user}
		var events []FinanceGiftBoundaryEvent
		var rows []map[string]any
		for i := 0; i < count; i++ {
			e := FinanceGiftBoundaryEvent{SourceLogID: int64(userIndex*10_000 + i + 1), HourTs: start, UserID: user, EventAt: start + int64(i/3), Kind: "usage", Quota: int64(i + 1)}
			kind, group := 2, "business"
			if i%11 == 0 {
				group = "test"
			}
			if i%17 == 0 {
				kind, e.Kind = 6, "refund"
				fact.RefundRecords++
				fact.RefundQuota += e.Quota
			} else {
				fact.Requests++
				fact.ConsumeQuota += e.Quota
			}
			e.EvidenceHash = financeGiftBoundaryEventHash(e)
			events = append(events, e)
			rows = append(rows, map[string]any{"id": e.SourceLogID, "user_id": user, "created_at": e.EventAt, "type": kind, "quota": e.Quota, "group": group})
		}
		if _, err := replaceFinanceUserHourFacts(ctx, db, start, "v1", []FinanceUserHourFact{fact}, start+7200); err != nil {
			t.Fatal(err)
		}
		if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, start, user, start+7200, "v1", events); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(map[string]any{"user_id": user, "hour_ts": start, "rows": rows})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, fmt.Sprintf("synthetic-%d.json", user))
		if err = giftLocalWriteNew(path, data); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	backup := filepath.Join(dir, "backup.db")
	if err = db.Exec("VACUUM INTO ?", backup).Error; err != nil {
		t.Fatal(err)
	}
	return backup, paths
}

func TestFinanceGiftScopeLocalLargeCompleteAndRetry(t *testing.T) {
	for _, count := range []int{101, 2498, financeGiftLocalMaxRows} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backup, evidence := giftLargeLocalFixture(t, count)
			original := giftLocalFileHash(t, backup)
			dir := filepath.Join(t.TempDir(), "job")
			_, hash, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, evidence)
			if err != nil {
				t.Fatal(err)
			}
			db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			before := giftMixedMonetarySnapshot(t, db)
			for attempt := 0; attempt < 2; attempt++ {
				result, err := runFinanceGiftLocalJob(context.Background(), dir, hash, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
				want := count
				if attempt > 0 {
					want = 0
				}
				if err != nil || result.Status != "complete" || len(result.Entries) != 1 || result.Entries[0].RowsUpdated != want {
					t.Fatalf("result=%+v error=%v", result, err)
				}
				if before != giftMixedMonetarySnapshot(t, db) || original != giftLocalFileHash(t, backup) {
					t.Fatal("money or original backup changed")
				}
			}
			var known int64
			if err = db.Model(&FinanceGiftBoundaryEvent{}).Where("group_known=?", true).Count(&known).Error; err != nil || known != int64(count) {
				t.Fatalf("known=%d error=%v", known, err)
			}
			var originalEvidence financeGiftLocalEvidence
			if _, err = giftLocalReadJSON(evidence[0], &originalEvidence); err != nil {
				t.Fatal(err)
			}
			after, err := loadFinanceGiftScopeSnapshot(context.Background(), db, "v1", originalEvidence.Hour, originalEvidence.UserID)
			if err != nil {
				t.Fatal(err)
			}
			for i, event := range after.Events {
				if event.SourceLogID != originalEvidence.Rows[i].ID || event.Group != *originalEvidence.Rows[i].Group || !event.GroupKnown {
					t.Fatal("large-hour historical group changed during import")
				}
			}
		})
	}
}

func TestFinanceGiftScopeLocalLargeRejectsIncompleteOrChangedEvidence(t *testing.T) {
	for _, scenario := range []string{"missing_page", "duplicate", "amount", "over_file_limit", "over_plan_limit"} {
		t.Run(scenario, func(t *testing.T) {
			counts := []int{201}
			if scenario == "over_file_limit" {
				counts = []int{financeGiftLocalMaxRows + 1}
			} else if scenario == "over_plan_limit" {
				counts = []int{1500, 1501}
			}
			backup, paths := giftLargeLocalFixture(t, counts...)
			original := giftLocalFileHash(t, backup)
			var e financeGiftLocalEvidence
			if _, err := giftLocalReadJSON(paths[0], &e); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "missing_page":
				e.Rows = append(e.Rows[:100], e.Rows[200:]...)
			case "duplicate":
				e.Rows[100] = e.Rows[99]
			case "amount":
				*e.Rows[100].Quota++
			}
			data, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(paths[0], data, 0600); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "job")
			if _, _, err = PrepareFinanceGiftLocalJob(context.Background(), backup, dir, paths); err == nil {
				t.Fatal("invalid evidence produced an executable plan")
			}
			if _, err = os.Stat(filepath.Join(dir, "plan.json")); !os.IsNotExist(err) {
				t.Fatal("rejected preparation published a plan")
			}
			if original != giftLocalFileHash(t, backup) {
				t.Fatal("original backup changed")
			}
		})
	}
}

func TestFinanceGiftScopeLocalRowBudget(t *testing.T) {
	for _, rows := range [][]int{nil, {0}, {-1}, {3001}, {1500, 1501}, {3000, 1}, {int(^uint(0) >> 1)}} {
		if validateFinanceGiftLocalRowBudget(rows) == nil {
			t.Fatalf("invalid budget accepted: %v", rows)
		}
	}
	for _, rows := range [][]int{{1}, {3000}, {1500, 1500}, {11, 1, 17, 3}} {
		if err := validateFinanceGiftLocalRowBudget(rows); err != nil {
			t.Fatalf("valid budget rejected: %v %v", rows, err)
		}
	}
}

func TestFinanceGiftScopeLocalRunRechecksRowBudget(t *testing.T) {
	for _, count := range []int{2, financeGiftLocalMaxRows + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backup, evidence, dir := giftLocalFixture(t)
			plan, _, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, []string{evidence})
			if err != nil {
				t.Fatal(err)
			}
			// Even an explicitly reconfirmed edited plan cannot bypass runtime
			// bounds or claim fewer rows than its evidence actually contains.
			plan.Rows[0] = count
			data, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, "plan.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			result, err := runFinanceGiftLocalJob(context.Background(), dir, giftLocalDigest(data), func(context.Context, time.Duration) error {
				t.Fatal("invalid row budget reached execution wait")
				return nil
			})
			if err == nil || result.Status != "rejected" || len(result.Entries) != 0 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}
