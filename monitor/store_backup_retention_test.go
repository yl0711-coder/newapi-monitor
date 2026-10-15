package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func retentionFixture(t *testing.T, dir string, stamp time.Time) string {
	t.Helper()
	id := stamp.UTC().Format(backupSetStampLayout)
	path := filepath.Join(dir, storeBackupPrefix+id+storeBackupSuffix)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE sample (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO sample VALUES(1,12345)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := storeBackupFile(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := writeStoreBackupSetManifest(dir, storeBackupSetManifest{Version: storeBackupSetVersion, MigrationPlan: "test", SetID: id, CreatedAt: stamp.Unix(), Main: file})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestBackupRetentionCalendarDaysAndLatestTwo(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 10, 1, 10, 0, 0, 0, cstLocation)
	for day := 0; day < 10; day++ {
		for hour := 0; hour < 3; hour++ {
			retentionFixture(t, dir, start.AddDate(0, 0, day).Add(time.Duration(hour)*time.Hour))
		}
	}
	now := start.AddDate(0, 0, 30) // downtime never ages out every recovery point
	for repeat := 0; repeat < 2; repeat++ {
		if err := pruneStoreBackupSetsWithPolicy(context.Background(), dir, 7, 7, now, nil); err != nil {
			t.Fatal(err)
		}
		entries, err := scanRuntimeBackupSets(dir, now, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 8 {
			t.Fatalf("want 7 days + extra newest set, got %d", len(entries))
		}
		days := map[string]int{}
		for _, entry := range entries {
			days[time.Unix(entry.manifest.CreatedAt, 0).In(cstLocation).Format("2006-01-02")]++
			if _, err := verifyStoreBackupSetManifest(context.Background(), entry.path); err != nil {
				t.Fatal(err)
			}
		}
		if len(days) != 7 || days["2026-10-10"] != 2 || days["2026-10-04"] != 1 {
			t.Fatalf("wrong calendar coverage: %v", days)
		}
	}
}

func TestBackupRetentionBeijingMidnightAndLegacyCount(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 10, 1, 15, 59, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		retentionFixture(t, dir, start.Add(time.Duration(i)*time.Minute))
	}
	entries, err := scanRuntimeBackupSets(dir, start.Add(time.Hour), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runtimeBackupKeepSet(entries, 7, 2)); got != 3 {
		t.Fatalf("midnight keep=%d want3", got)
	}
	if err := pruneStoreBackupSetsWithPolicy(context.Background(), dir, 2, 0, start.Add(time.Hour), nil); err != nil {
		t.Fatal(err)
	}
	entries, err = scanRuntimeBackupSets(dir, start.Add(time.Hour), nil)
	if err != nil || len(entries) != 2 {
		t.Fatalf("legacy count: %d, %v", len(entries), err)
	}
}

func TestBackupRetentionFailsClosed(t *testing.T) {
	for _, problem := range []string{"json", "path", "cross_set", "future", "missing", "symlink", "hardlink", "wal", "newest_corrupt", "live", "cancelled", "zero_count"} {
		t.Run(problem, func(t *testing.T) {
			dir := t.TempDir()
			start := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
			var paths []string
			for i := 0; i < 4; i++ {
				paths = append(paths, retentionFixture(t, dir, start.AddDate(0, 0, i)))
			}
			data, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			var m storeBackupSetManifest
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatal(err)
			}
			memberPath := filepath.Join(dir, m.Main.File)
			ctx := context.Background()
			var live []string
			count := 2
			switch problem {
			case "json":
				if err := os.WriteFile(paths[0], []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "path", "cross_set":
				if problem == "path" {
					m.Main.File = "../live.db"
				} else {
					m.Main.File = storeBackupPrefix + start.AddDate(0, 0, 1).Format(backupSetStampLayout) + storeBackupSuffix
				}
				if _, err := writeStoreBackupSetManifest(dir, m); err != nil {
					t.Fatal(err)
				}
			case "future":
				retentionFixture(t, dir, start.AddDate(1, 0, 0))
			case "missing":
				if err := os.Remove(memberPath); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(memberPath, memberPath+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(memberPath+".original", memberPath); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(memberPath, memberPath+".linked"); err != nil {
					t.Fatal(err)
				}
			case "wal":
				if err := os.WriteFile(memberPath+"-wal", []byte("active"), 0600); err != nil {
					t.Fatal(err)
				}
			case "newest_corrupt":
				path := filepath.Join(dir, storeBackupPrefix+start.AddDate(0, 0, 3).Format(backupSetStampLayout)+storeBackupSuffix)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data[len(data)-1] ^= 1
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "live":
				live = []string{memberPath}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "zero_count":
				count = 0
			}
			if err := pruneStoreBackupSetsWithPolicy(ctx, dir, count, 2, start.AddDate(0, 1, 0), live); err == nil {
				t.Fatal("unsafe sweep unexpectedly succeeded")
			}
			for _, path := range paths {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("sweep removed manifest: %s: %v", path, err)
				}
			}
		})
	}
}

func TestBackupRetentionPreservesForeignFilesAndPairedMembers(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var manifests []string
	for i := 0; i < 4; i++ {
		manifests = append(manifests, retentionFixture(t, dir, start.AddDate(0, 0, i)))
	}
	// An older pair must be deleted as a unit, not counted twice.
	data, err := os.ReadFile(manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	var m storeBackupSetManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, m.Main.File))
	if err != nil {
		t.Fatal(err)
	}
	factsPath := filepath.Join(dir, usageFactsBackupPrefix+m.SetID+storeBackupSuffix)
	if err := os.WriteFile(factsPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	m.Facts, err = storeBackupFile(factsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeStoreBackupSetManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	foreign := []string{"manual.db", ".monitor-unfinished.tmp", "finance-gift-proof.db", "monitor-loose.db"}
	for _, name := range foreign {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "pre-migrate-protected"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := pruneSQLiteBackups(dir, storeBackupPrefix, 1); err != nil {
		t.Fatal(err)
	}
	for _, path := range manifests {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	if err := pruneStoreBackupSetsWithPolicy(context.Background(), dir, 2, 2, start.AddDate(0, 1, 0), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(factsPath); !os.IsNotExist(err) {
		t.Fatalf("old pair facts remained: %v", err)
	}
	for _, name := range append(foreign, "pre-migrate-protected") {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("foreign file removed: %s", name)
		}
	}
}

func TestBackupRetentionConfigurationBounds(t *testing.T) {
	for _, tc := range []struct{ in, want int }{{-1, 0}, {0, 0}, {7, 7}, {366, 365}} {
		m := &Monitor{cfg: Settings{StoreBackupRetentionDays: tc.in}}
		if got := m.storeBackupRetentionDays(); got != tc.want {
			t.Fatalf("%d -> %d", tc.in, got)
		}
		if got := m.storeReliabilityStatus().BackupRetentionDays; got != tc.want {
			t.Fatal(got)
		}
	}
}

func TestBackupRetentionRestartResumeAndSourceReplacement(t *testing.T) {
	dir := t.TempDir()
	backupDir := filepath.Join(dir, "backups")
	if err := os.Mkdir(backupDir, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "source.db")
	if err := os.WriteFile(source, []byte("source identity only"), 0600); err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)
	manifestPath := retentionFixture(t, backupDir, stamp)
	m := &Monitor{cfg: Settings{StorePath: source, StoreBackupDir: backupDir, StoreBackupRetentionDays: 7}}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest storeBackupSetManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.MigrationPlan = m.currentMigrationPlanID()
	manifest.SourceIdentity, err = runtimeBackupSourceIdentity([]string{source})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeStoreBackupSetManifest(backupDir, manifest); err != nil {
		t.Fatal(err)
	}
	if delay, err := m.storeBackupResumeDelay(context.Background(), stamp.Add(time.Hour), true, false); err != nil || delay != 23*time.Hour {
		t.Fatalf("resume %s %v", delay, err)
	}
	if !m.storeBackupSetVerified.Load() || m.storeBackupLastSuccess.Load() != stamp.Unix() {
		t.Fatal("verified original timestamp not restored")
	}
	if delay, err := m.storeBackupResumeDelay(context.Background(), stamp.Add(24*time.Hour), true, false); err != nil || delay != 0 {
		t.Fatalf("due %s %v", delay, err)
	}
	if delay, err := m.storeBackupResumeDelay(context.Background(), stamp.Add(time.Hour), true, true); err != nil || delay != 0 {
		t.Fatalf("source roles changed %s %v", delay, err)
	}
	// A restore/replacement with identical contents must still create a fresh backup.
	if err := os.WriteFile(source+".new", []byte("source identity only"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(source+".new", source); err != nil {
		t.Fatal(err)
	}
	if delay, err := m.storeBackupResumeDelay(context.Background(), stamp.Add(time.Hour), true, false); err != nil || delay != 0 {
		t.Fatalf("replaced source %s %v", delay, err)
	}
	manifest.SourceIdentity = "" // old releases remain restorable, but never suppress a fresh snapshot
	if _, err := writeStoreBackupSetManifest(backupDir, manifest); err != nil {
		t.Fatal(err)
	}
	if delay, err := m.storeBackupResumeDelay(context.Background(), stamp.Add(time.Hour), true, false); err != nil || delay != 0 {
		t.Fatalf("legacy identity %s %v", delay, err)
	}
	if _, err := verifyStoreBackupSetManifest(context.Background(), manifestPath); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRetentionRealPairRestore(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{StorePath: filepath.Join(dir, "main.db"), UsageFactsStorePath: filepath.Join(dir, "facts.db"), StoreBackupDir: filepath.Join(dir, "backups"), StoreBackupRetentionDays: 2}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	stamp := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	var latest string
	for _, offset := range []time.Duration{0, 24 * time.Hour, 48 * time.Hour, 49 * time.Hour, 50 * time.Hour} {
		path, err := m.createStoreBackupSet(context.Background(), stamp.Add(offset), true, true)
		if err != nil {
			t.Fatal(err)
		}
		latest = path
	}
	entries, err := scanRuntimeBackupSets(m.storeBackupDir(), stamp.Add(72*time.Hour), nil)
	if err != nil || len(entries) != 3 {
		t.Fatalf("paired retention: %d %v", len(entries), err)
	}
	if err := RestoreStoreBackupSet(context.Background(), latest, filepath.Join(dir, "restored"), "main.db", "facts.db"); err != nil {
		t.Fatal(err)
	}
	if delay, err := m.storeBackupResumeDelay(context.Background(), stamp.Add(51*time.Hour), true, true); err != nil || delay != 23*time.Hour {
		t.Fatalf("real pair resume: %s %v", delay, err)
	}
}
