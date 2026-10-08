//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func rechargeClosedFixture(t *testing.T) string {
	t.Helper()
	m, scope, _ := dailyBillFixture(t)
	if err := m.storeDB.Create(&ChannelUpstreamAccount{Domain: "day.example", Provider: upstreamProviderAICodeWith, UsageSyncEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	createChannelRechargeVersion(t, m, "hour.example", 1, scope.FromTs+86400, 1, 1)
	createChannelRechargeVersion(t, m, "day.example", 1, scope.FromTs+3600, 1, 1)
	path := filepath.Join(t.TempDir(), "closed.db")
	if err := m.storeDB.Exec("VACUUM INTO ?", path).Error; err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFinanceRechargeBackupInspectorIsReadOnlyAndRepeatable(t *testing.T) {
	path := rechargeClosedFixture(t)
	before := giftLocalFileHash(t, path)
	first, err := InspectFinanceRechargeBackup(context.Background(), path, "2026-05-01", "2026-05-03", FinanceRechargeInspectionOptions{})
	if err != nil || len(first.Domains) != 2 || first.SourceSnapshotSHA256 != before {
		t.Fatal("closed fixture did not produce an identity-pinned plan", first, err)
	}
	second, err := InspectFinanceRechargeBackup(context.Background(), path, "2026-05-01", "2026-05-03", FinanceRechargeInspectionOptions{})
	if err != nil || !reflect.DeepEqual(first, second) || giftLocalFileHash(t, path) != before {
		t.Fatal("inspection updated data/schema or changed the plan", err)
	}
	for _, suffix := range []string{"-wal", "-journal", "-shm"} {
		if _, err := os.Lstat(path + suffix); !os.IsNotExist(err) {
			t.Fatal("readonly inspector created a SQLite sidecar", suffix, err)
		}
	}
}

func TestFinanceRechargeBackupInspectorRejectsUnsafeFiles(t *testing.T) {
	for _, mode := range []string{"wal", "journal", "symlink", "empty", "bad_date", "future", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			path := rechargeClosedFixture(t)
			ctx := context.Background()
			start, end := "2026-05-01", "2026-05-03"
			switch mode {
			case "wal", "journal":
				if err := os.WriteFile(path+"-"+mode, []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := filepath.Join(filepath.Dir(path), "alias.db")
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			case "empty":
				end = start
			case "bad_date":
				start = "not-a-date"
			case "future":
				end = "2099-01-01"
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			if result, err := InspectFinanceRechargeBackup(ctx, path, start, end, FinanceRechargeInspectionOptions{}); err == nil || result.Mode != "" {
				t.Fatal("unsafe source returned a usable plan", result, err)
			}
		})
	}
}

func TestFinanceRechargeBackupPlanningExclusionsDoNotChangeSourceScope(t *testing.T) {
	path := rechargeClosedFixture(t)
	before := giftLocalFileHash(t, path)
	options := FinanceRechargeInspectionOptions{ExcludedDomains: []string{"hour.example", "hour.example", "ignored.example"}}
	plan, err := InspectFinanceRechargeBackup(context.Background(), path, "2026-05-01", "2026-05-03", options)
	if err != nil || len(plan.Domains) != 1 || plan.Domains[0].Domain != "day.example" ||
		!reflect.DeepEqual(plan.ExcludedDomains, []string{"hour.example", "ignored.example"}) || giftLocalFileHash(t, path) != before {
		t.Fatal("planning exclusions changed original data or were not explicit", plan, err)
	}
	options.ExcludedDomains = []string{" Hour.Example "}
	if result, err := InspectFinanceRechargeBackup(context.Background(), path, "2026-05-01", "2026-05-03", options); err == nil || result.Mode != "" {
		t.Fatal("ambiguous exclusion names returned a plan", err)
	}
}
