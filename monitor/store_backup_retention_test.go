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

func TestStoreBackupDailySlot(t *testing.T) {
	m, ok := storeBackupDailyAt("05:40")
	if !ok || m != 5*60+40 {
		t.Fatalf("parse: %d %v", m, ok)
	}
	for _, bad := range []string{"", "5:4x", "24:00", "05:60", "0540"} {
		if _, ok := storeBackupDailyAt(bad); ok {
			t.Fatalf("accepted %q", bad)
		}
	}
	cases := map[string]string{
		"2026-11-09T05:39:59Z": "2026-11-08T05:40:00Z", // before today's slot
		"2026-11-09T05:40:00Z": "2026-11-09T05:40:00Z",
		"2026-11-09T12:40:00Z": "2026-11-09T05:40:00Z", // restart in the afternoon keeps 05:40
	}
	for now, want := range cases {
		n, _ := time.Parse(time.RFC3339, now)
		if got := storeBackupLastSlot(n, m).Format(time.RFC3339); got != want {
			t.Fatalf("%s: got %s want %s", now, got, want)
		}
	}
}

func countBackupSets(t *testing.T, dir string) int {
	t.Helper()
	entries, err := scanRuntimeBackupSets(dir, time.Now(), nil)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return len(entries)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A restart in fixed-time mode must adopt the backup already taken since the
// latest slot (health restored, no duplicate backup), even with the legacy
// count-based retention that production uses.
func TestDailyBackupRestartAdoptsVerifiedBackup(t *testing.T) {
	oldStart := storeBackupDailyStartDelay
	storeBackupDailyStartDelay = time.Millisecond
	t.Cleanup(func() { storeBackupDailyStartDelay = oldStart })
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{StorePath: filepath.Join(dir, "main.db"), UsageFactsStorePath: filepath.Join(dir, "facts.db"), StoreBackupDir: filepath.Join(dir, "backups")}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	now := time.Now().UTC()
	minute := now.Hour()*60 + now.Minute() - 1 // slot one minute ago
	if minute < 0 {
		minute += 24 * 60
	}
	created := now.Add(-10 * time.Second)
	if _, err := m.createStoreBackupSet(context.Background(), created, true, true); err != nil {
		t.Fatal(err)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, usageFactsDB: m.usageFactsDB}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { restarted.runDailyStoreBackups(ctx, minute, true, true); close(done) }()
	waitFor(t, "adoption", func() bool { return restarted.storeBackupSetVerified.Load() })
	cancel()
	<-done
	if got := restarted.storeBackupSetLastSuccess.Load(); got != created.Unix() {
		t.Fatalf("adopted set time %d, want %d", got, created.Unix())
	}
	if restarted.storeBackupLastSuccess.Load() == 0 || restarted.usageFactsBackupLastSuccess.Load() == 0 {
		t.Fatal("per-store backup health not restored")
	}
	if n := countBackupSets(t, m.storeBackupDir()); n != 1 {
		t.Fatalf("restart created a duplicate backup: %d sets", n)
	}
}

// A failed fixed-time backup is retried soon, not a day later.
func TestDailyBackupRetriesAfterFailure(t *testing.T) {
	oldStart, oldRetry := storeBackupDailyStartDelay, storeBackupDailyRetryDelay
	storeBackupDailyStartDelay, storeBackupDailyRetryDelay = time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { storeBackupDailyStartDelay, storeBackupDailyRetryDelay = oldStart, oldRetry })
	dir := t.TempDir()
	backupDir := filepath.Join(dir, "backups")
	m := &Monitor{cfg: Settings{StorePath: filepath.Join(dir, "main.db"), StoreBackupDir: backupDir}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	// A regular file where the backup directory should be makes backups fail.
	if err := os.WriteFile(backupDir, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.runDailyStoreBackups(ctx, now.Hour()*60+now.Minute(), true, false); close(done) }()
	waitFor(t, "first failure", func() bool { return m.storeBackupSetLastFailure.Load() > 0 })
	if err := os.Remove(backupDir); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retry success", func() bool { return m.storeBackupSetLastSuccess.Load() > 0 })
	cancel()
	<-done
	if n := countBackupSets(t, backupDir); n != 1 {
		t.Fatalf("want 1 backup set after retry, got %d", n)
	}
}

// Fixed daily backups are stale after a day plus slack, not two days.
func TestStoreBackupStaleAfter(t *testing.T) {
	daily := &Monitor{cfg: Settings{StoreBackupDailyAt: "05:40"}}
	if got := daily.storeBackupStaleAfter(); got != 25*time.Hour {
		t.Fatalf("daily stale after %s", got)
	}
	interval := &Monitor{cfg: Settings{StoreBackupIntervalHours: 6}}
	if got := interval.storeBackupStaleAfter(); got != 12*time.Hour+10*time.Minute {
		t.Fatalf("interval stale after %s", got)
	}
}
