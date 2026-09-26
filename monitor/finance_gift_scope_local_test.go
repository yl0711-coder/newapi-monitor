//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func giftLocalFixture(t *testing.T) (string, string, string) {
	t.Helper()
	m, _, hour := giftScopeRepairFixture(t)
	dir := t.TempDir()
	backup := filepath.Join(dir, "backup.db")
	if err := m.usageFactsStore().Exec("VACUUM INTO ?", backup).Error; err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{}
	for i, quota := range []int64{30_000_000, 50_000_000, 5_000_000} {
		kind, group := 2, "business"
		if i == 0 {
			group = "test"
		}
		if i == 2 {
			kind = 6
		}
		rows = append(rows, map[string]any{"id": i + 2, "user_id": 7, "created_at": hour + int64(20+i*10), "type": kind, "quota": quota, "group": group})
	}
	data, err := json.Marshal(map[string]any{"user_id": 7, "hour_ts": hour, "rows": rows})
	if err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(dir, "original.json")
	if err = giftLocalWriteNew(evidence, data); err != nil {
		t.Fatal(err)
	}
	return backup, evidence, filepath.Join(dir, "job")
}

func giftLocalFileHash(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return giftLocalDigest(data)
}

func TestFinanceGiftScopeLocalEvidenceRequiresExplicitScopeAndAmount(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		valid        bool
	}{
		{"missing_group", `,"quota":0`, false},
		{"null_group", `,"quota":0,"group":null`, false},
		{"missing_quota", `,"group":"business"`, false},
		{"null_quota", `,"quota":null,"group":"business"`, false},
		{"explicit_empty_group_and_zero_quota", `,"quota":0,"group":""`, true},
		{"explicit_business", `,"quota":1,"group":"business"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(`{"user_id":7,"hour_ts":3600,"rows":[{"id":1,"user_id":7,"created_at":3610,"type":2` + tc.fields + `}]}`)
			var e financeGiftLocalEvidence
			if err := giftLocalDecodeJSON(data, &e); err != nil {
				t.Fatal(err)
			}
			if err := e.validate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%v validation=%v", tc.valid, err)
			}
			source, err := giftLocalSource(context.Background(), []financeGiftLocalEvidence{e})
			if !tc.valid {
				if err == nil {
					source.Close()
					t.Fatal("incomplete evidence entered source projection")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			events, err := fetchFinanceGiftBoundaryEvents(context.Background(), source, 3600, []int64{7})
			if err != nil || len(events) != 1 || !events[0].GroupKnown || events[0].Group != *e.Rows[0].Group || events[0].Quota != *e.Rows[0].Quota {
				t.Fatalf("explicit evidence changed in projection: %+v %v", events, err)
			}
		})
	}
}

func TestFinanceGiftScopeLocalPlanRunResume(t *testing.T) {
	backup, evidence, dir := giftLocalFixture(t)
	before := giftLocalFileHash(t, backup)
	ctx := context.Background()
	plan, hash, err := PrepareFinanceGiftLocalJob(ctx, backup, dir, []string{evidence})
	if err != nil || len(plan.Targets) != 1 || plan.Rows[0] != 3 || hash == "" {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	target := plan.Targets[0]
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	prior, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
	closeDB()
	if err != nil || prior.Events[0].GroupKnown {
		t.Fatalf("preview repaired facts: %+v %v", prior, err)
	}
	var waits []time.Duration
	wait := func(ctx context.Context, d time.Duration) error { waits = append(waits, d); return ctx.Err() }
	for i := 0; i < 2; i++ {
		result, err := runFinanceGiftLocalJob(ctx, dir, hash, wait)
		if err != nil || result.Status != "complete" || len(result.Entries) != 1 || result.Remaining != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		want := 3
		if i == 1 {
			want = 0
		}
		if result.Entries[0].RowsUpdated != want {
			t.Fatalf("non-idempotent result: %+v", result)
		}
	}
	if !reflect.DeepEqual(waits, []time.Duration{10 * time.Second, 10 * time.Second}) {
		t.Fatal("restart bypassed cooldown", waits)
	}
	db, closeDB, err = giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	after, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil || !reflect.DeepEqual(prior.UserState, after.UserState) || prior.State.ConsumeQuota != after.State.ConsumeQuota || prior.State.RefundQuota != after.State.RefundQuota {
		t.Fatalf("monetary values changed: %v", err)
	}
	if giftLocalFileHash(t, backup) != before {
		t.Fatal("original backup changed")
	}
	if err = giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err = PrepareFinanceGiftLocalJob(ctx, backup, dir, []string{evidence}); err == nil {
		t.Fatal("existing job overwritten")
	}
}

func TestFinanceGiftScopeLocalRejectsChangedOrUnsafeJob(t *testing.T) {
	for _, scenario := range []string{"confirmation", "evidence", "partial_audit", "unrelated_audit", "db_symlink", "db_hardlink", "lock", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			backup, evidence, dir := giftLocalFixture(t)
			_, hash, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, []string{evidence})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch scenario {
			case "confirmation":
				hash = "wrong"
			case "evidence":
				if err = os.WriteFile(filepath.Join(dir, giftLocalEvidenceName(0)), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "partial_audit", "unrelated_audit":
				data := []byte("{\"status\":")
				if scenario == "unrelated_audit" {
					data = []byte("{\"status\":\"unchanged\",\"user_id\":999}\n")
				}
				if err = os.WriteFile(filepath.Join(dir, "audit.jsonl"), data, 0600); err != nil {
					t.Fatal(err)
				}
			case "db_symlink", "db_hardlink":
				path := filepath.Join(dir, "usage-facts.db")
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if scenario == "db_symlink" {
					err = os.Symlink(backup, path)
				} else {
					err = os.Link(backup, path)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "lock":
				lock, err := giftLocalLock(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before := giftLocalFileHash(t, backup)
			result, err := runFinanceGiftLocalJob(ctx, dir, hash, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
			if err == nil || len(result.Entries) != 0 {
				t.Fatalf("unsafe job accepted: %+v %v", result, err)
			}
			if giftLocalFileHash(t, backup) != before {
				t.Fatal("original modified")
			}
		})
	}
}

func TestFinanceGiftScopeLocalPreparationFailsClosed(t *testing.T) {
	for _, scenario := range []string{"wal", "monetary_mismatch", "extra_json", "duplicate", "empty"} {
		t.Run(scenario, func(t *testing.T) {
			backup, evidence, dir := giftLocalFixture(t)
			paths := []string{evidence}
			switch scenario {
			case "wal":
				if err := os.WriteFile(backup+"-wal", []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			case "monetary_mismatch":
				var e financeGiftLocalEvidence
				if _, err := giftLocalReadJSON(evidence, &e); err != nil {
					t.Fatal(err)
				}
				(*e.Rows[0].Quota)++
				data, err := json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(evidence, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "extra_json":
				f, err := os.OpenFile(evidence, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString("{}")
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "duplicate":
				paths = append(paths, evidence)
			case "empty":
				paths = nil
			}
			_, _, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, paths)
			if err == nil {
				t.Fatal("unsafe preparation accepted")
			}
			if _, err = os.Stat(filepath.Join(dir, "plan.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed preparation published plan")
			}
		})
	}
}

func TestFinanceGiftScopeLocalLockReleasedOnClose(t *testing.T) {
	_, evidence, dir := giftLocalFixture(t)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := giftLocalWriteNew(filepath.Join(dir, "job.lock"), nil); err != nil {
		t.Fatal(err)
	}
	lock, err := giftLocalLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := giftLocalLock(dir); err == nil {
		other.Close()
		t.Fatal("concurrent lock accepted")
	}
	lock.Close()
	lock, err = giftLocalLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
	if _, err = os.Stat(evidence); err != nil {
		t.Fatal(err)
	}
}
