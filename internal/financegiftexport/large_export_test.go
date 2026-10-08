package financegiftexport

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func largeExportFixture(count int) (LargeHourPlan, [][]driver.Value) {
	data := giftExportData(count)
	plan := LargeHourPlan{Version: 1, SourceEpoch: "v1", UserID: 7, HourTs: 3600, LocalContentHash: strings.Repeat("a", 64)}
	for _, row := range data {
		plan.Rows = append(plan.Rows, LargeHourRecord{ID: row[0].(int64), CreatedAt: row[2].(int64), Type: int(row[3].(int64)), Quota: row[4].(int64)})
	}
	return plan, data
}

func TestLargeHourExportBoundedAndWholeEvidence(t *testing.T) {
	for _, count := range []int{3001, 4833, LargeHourMaxRows} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			plan, data := largeExportFixture(count)
			// Same-second records and noncontiguous IDs must not require OFFSET.
			for i := range data {
				data[i][0], data[i][2] = int64(2*i+1), int64(3601)
				plan.Rows[i].ID, plan.Rows[i].CreatedAt = int64(2*i+1), 3601
			}
			db, state := giftExportTestDB(t, data)
			digest, err := LargeHourConfirmation(plan)
			if err != nil {
				t.Fatal(err)
			}
			path, waits := filepath.Join(t.TempDir(), "large.json"), 0
			wait := func(ctx context.Context, d time.Duration) error {
				if state.activeTx || d != 10*time.Second {
					t.Fatal("cooldown changed or retained source transaction")
				}
				waits++
				return ctx.Err()
			}
			if err := exportLargeHour(context.Background(), db, plan, digest, path, wait); err != nil {
				t.Fatal(err)
			}
			pages := (count + largeHourPageRows - 1) / largeHourPageRows
			if waits != pages || state.transactions != pages || len(state.queries) != 2*pages || !state.options.ReadOnly {
				t.Fatal("query or transaction budget changed")
			}
			for i := 0; i < len(state.queries); i += 2 {
				query := state.queries[i+1]
				if state.queries[i] != "EXPLAIN "+query || strings.Contains(query, "OFFSET") || !strings.Contains(query, "ORDER BY id LIMIT ") {
					t.Fatal("unsafe pagination")
				}
			}
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var result LargeHourEvidence
			if err := json.Unmarshal(encoded, &result); err != nil || result.PlanSHA256 != digest || len(result.Rows) != count {
				t.Fatal("incomplete published proof", err)
			}
			for i, row := range result.Rows {
				if row.ID != plan.Rows[i].ID || row.Quota != plan.Rows[i].Quota || row.Group != "business" {
					t.Fatal("row changed")
				}
			}
			info, _ := os.Stat(path)
			if info.Mode().Perm() != 0600 {
				t.Fatal("evidence is not private")
			}
			if err := exportLargeHour(context.Background(), db, plan, digest, path, wait); err == nil || waits != pages {
				t.Fatal("existing evidence was overwritten or requeried")
			}
		})
	}
}

func TestLargeHourExportFailsWithoutPublishingPartialEvidence(t *testing.T) {
	for _, scenario := range []string{"missing", "duplicate", "quota", "time", "type", "user", "known_group", "null_group", "bytes", "cancel", "unsafe_plan", "commit", "read"} {
		t.Run(scenario, func(t *testing.T) {
			plan, data := largeExportFixture(3001)
			if scenario == "known_group" {
				group := "expected"
				plan.Rows[500].Group = &group
			}
			digest, err := LargeHourConfirmation(plan)
			if err != nil {
				t.Fatal(err)
			}
			db, state := giftExportTestDB(t, data)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waits := 0
			wait := func(context.Context, time.Duration) error {
				waits++
				if waits != 2 {
					return nil
				}
				switch scenario {
				case "missing":
					state.data = append(state.data[:500], state.data[501:]...)
				case "duplicate":
					state.data[501] = state.data[500]
				case "quota":
					state.data[500][4] = int64(9999)
				case "time":
					state.data[500][2] = int64(3600)
				case "type":
					state.data[500][3] = int64(6)
				case "user":
					state.data[500][1] = int64(8)
				case "null_group":
					state.data[500][5] = nil
				case "bytes":
					state.data[500][5] = strings.Repeat("x", giftScopeExportBytes+1)
				case "cancel":
					cancel()
				case "unsafe_plan":
					state.planType = "ALL"
				case "commit":
					state.commitErr = errors.New("failed commit")
				case "read":
					state.readErr = errors.New("failed page read")
				}
				return nil
			}
			path := filepath.Join(t.TempDir(), "large.json")
			if err := exportLargeHour(ctx, db, plan, digest, path, wait); err == nil {
				t.Fatal("bad evidence accepted")
			}
			if waits != 2 || len(state.queries) > 4 {
				t.Fatal("continued after failed page")
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("partial evidence published")
			}
		})
	}
}

func TestLargeHourPlanRejectsInvalidOrChangedConfirmation(t *testing.T) {
	for _, scenario := range []string{"small", "large", "duplicate", "open", "epoch", "hash", "quota", "confirmation"} {
		t.Run(scenario, func(t *testing.T) {
			plan, data := largeExportFixture(3001)
			digest, _ := LargeHourConfirmation(plan)
			switch scenario {
			case "small":
				plan.Rows = plan.Rows[:3000]
			case "large":
				plan, _ = largeExportFixture(5001)
			case "duplicate":
				plan.Rows[1].ID = plan.Rows[0].ID
			case "open":
				plan.HourTs = time.Now().Unix() / 3600 * 3600
			case "epoch":
				plan.SourceEpoch = " "
			case "hash":
				plan.LocalContentHash = "invalid"
			case "quota":
				plan.Rows[0].Quota = -1
			case "confirmation":
				plan.Rows[0].Quota++
			}
			db, state := giftExportTestDB(t, data)
			if err := exportLargeHour(context.Background(), db, plan, digest, filepath.Join(t.TempDir(), "no.json"), func(context.Context, time.Duration) error { t.Fatal("invalid plan reached wait"); return nil }); err == nil || state.transactions != 0 {
				t.Fatal("invalid plan reached source")
			}
		})
	}
}

func TestLargeHourRechecksSourceIdentityForEveryPage(t *testing.T) {
	plan, data := largeExportFixture(3001)
	digest, err := LargeHourConfirmation(plan)
	if err != nil {
		t.Fatal(err)
	}
	db, state := giftExportTestDB(t, data)
	identity := SourceIdentity{Database: "fixture", ServerUUID: "synthetic-uuid"}
	waits := 0
	wait := func(context.Context, time.Duration) error {
		waits++
		if waits == 2 {
			state.sourceUUID = "different-server"
		}
		return nil
	}
	path := filepath.Join(t.TempDir(), "not-complete.json")
	if err := exportLargeHourWithIdentity(context.Background(), db, plan, digest, path, wait, &identity); err == nil {
		t.Fatal("source changed but export continued")
	}
	if waits != 2 || len(state.queries) != 4 || state.queries[3] != "SELECT @@server_uuid, DATABASE(), CURRENT_USER()" {
		t.Fatal("unexpected read after identity change", state.queries)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("partial export published")
	}
}
