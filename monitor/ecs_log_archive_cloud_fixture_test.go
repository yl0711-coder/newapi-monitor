package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in LOCAL ONLY: reads private artifacts from a completed synthetic cloud
// run. Never starts Monitor workers, opens a network client or rewrites inputs.
// Original signed bytes, AWS LastModified, public keys and leases stay intact.
func TestECSLogArchiveRealCloudExportOffline(t *testing.T) {
	root := os.Getenv("MONITOR_TEST_ECS_ARCHIVE_EXPORT")
	if root == "" {
		t.Skip("private synthetic cloud export not supplied")
	}
	st, err := os.Lstat(root)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 || filepath.Dir(root) != "/private/tmp" || !strings.HasPrefix(filepath.Base(root), "ecs-cloud-acceptance-") {
		t.Fatal("dedicated private cloud acceptance directory required")
	}
	var cfg ECSAcceptanceConfig
	readCloudFixtureJSON(t, filepath.Join(root, "receiver.json"), &cfg)
	if !strings.HasPrefix(cfg.Audience, "monitor-ecs-acceptance-") {
		t.Fatal("not a synthetic acceptance run")
	}
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing_store_%t", existing), func(t *testing.T) { replayCloudExport(t, root, cfg, existing) })
	}
}

func replayCloudExport(t *testing.T, root string, cfg ECSAcceptanceConfig, existing bool) {
	t.Helper()
	reader, total := readCloudArchiveObjects(t, root, cfg.Prefix)
	// Artificial per-GET latency makes the formerly instantaneous disk fixture
	// exercise bounded progress, while retaining original protocol payloads.
	reader.delay = 2 * time.Millisecond
	dir := t.TempDir()
	if existing {
		copyStoppedCloudStore(t, root, dir)
	}
	newReceiver := func() *Monitor {
		m, _, err := NewECSAcceptance(cfg, dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		return m // Deliberately no m.Start: no AWS or business database access.
	}
	m := newReceiver()
	if !existing {
		seedCloudArchiveSources(t, root, m)
	}
	binding := cfg.Account + "/" + cfg.Region + "/" + cfg.Bucket + "/" + cfg.Prefix
	if existing {
		var scan ECSLogArchiveScan
		if err := m.storeDB.First(&scan, 1).Error; err != nil {
			t.Fatal(err)
		}
		// The private export contains no live S3 continuation-token service.
		// Map its saved opaque token to a conservative full local rescan. Facts,
		// receipt hashes and source leases remain the original migrated values.
		if scan.NextToken != "" {
			reader.pages[scan.NextToken] = reader.pages[""]
		}
	}
	var sources []ECSLogSource
	if err := m.storeDB.Find(&sources).Error; err != nil {
		t.Fatal(err)
	}
	want := readCloudOracleCounts(t, root, sources)
	for turn := 0; turn < 80; turn++ {
		if turn == 3 {
			m.Close()
			m = newReceiver()
		}
		before := reader.gets
		if err := newECSArchiveReplayer(m, cfg.Prefix).scan(context.Background(), reader, binding, time.Now()); err != nil {
			t.Fatal(err)
		}
		if reader.gets-before > 2*ecsArchivePhaseEntries {
			t.Fatal("unbounded cloud replay reads")
		}
		var accepted int64
		if err := m.storeDB.Model(&ECSLogArchiveReceipt{}).Where("status = 200").Count(&accepted).Error; err != nil {
			t.Fatal(err)
		}
		if accepted == int64(total) {
			t.Logf("original cloud objects=%d accepted=%d scans=%d GETs=%d; receiver reopened during replay", total, accepted, turn+1, reader.gets)
			assertCloudLaneCounts(t, m, want)
			// Cover two full listing sweeps even when a newer cloud run contains
			// more than the original 535 objects. No faster runtime poll setting.
			pages := (total + ecsarchive.PageSize - 1) / ecsarchive.PageSize
			turnsPerPage := (ecsarchive.PageSize + ecsArchivePhaseEntries - 1) / ecsArchivePhaseEntries
			for range 2 * pages * turnsPerPage {
				if err := newECSArchiveReplayer(m, cfg.Prefix).scan(context.Background(), reader, binding, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			assertCloudLaneCounts(t, m, want)
			return
		}
	}
	var statuses []struct {
		Status int
		Count  int64
	}
	_ = m.storeDB.Model(&ECSLogArchiveReceipt{}).Select("status,COUNT(*) AS count").Group("status").Scan(&statuses).Error
	t.Fatalf("real export did not fully recover: %+v", statuses)
}

func copyStoppedCloudStore(t *testing.T, root, dir string) {
	t.Helper()
	for _, name := range []string{"monitor.db", "evidence.db", "usage-facts.db"} {
		source := filepath.Join(root, "state", name)
		if st, err := os.Stat(source + "-wal"); err == nil && st.Size() != 0 {
			t.Fatal("fixture must be stopped and checkpointed")
		}
		st, err := os.Lstat(source)
		if err != nil || !st.Mode().IsRegular() || st.Size() > 32<<20 {
			t.Fatal("invalid bounded fixture database")
		}
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func readCloudFixtureJSON(t *testing.T, path string, out any) {
	t.Helper()
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Size() > 16<<20 {
		t.Fatal("invalid bounded local fixture file")
	}
	b, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(b, out) != nil {
		t.Fatal("cannot decode local fixture")
	}
}

func readCloudArchiveObjects(t *testing.T, root, prefix string) (*archiveReaderFixture, int) {
	t.Helper()
	var listing struct {
		Contents []struct {
			Key, ETag    string
			LastModified time.Time
			Size         int64
		}
	}
	readCloudFixtureJSON(t, filepath.Join(root, "archive-objects.json"), &listing)
	if len(listing.Contents) == 0 || len(listing.Contents) > 2000 {
		t.Fatal("cloud fixture size outside test budget")
	}
	objects := make([]ecsarchive.Object, 0, len(listing.Contents))
	var bytes int64
	for _, entry := range listing.Contents {
		if _, _, _, _, err := ecsarchive.ParseKey(prefix, entry.Key); err != nil {
			t.Fatal(err)
		}
		bytes += entry.Size
		if entry.Size < 1 || entry.Size > ecsarchive.MaxObject || bytes > 64<<20 {
			t.Fatal("cloud fixture bytes outside test budget")
		}
		path := filepath.Join(root, "archive", entry.Key)
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Size() != entry.Size {
			t.Fatal("cloud fixture object mismatch")
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, ecsarchive.Object{Key: entry.Key, ETag: entry.ETag, Modified: entry.LastModified, Body: body})
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return pagedArchiveFixture(objects), len(objects)
}

func seedCloudArchiveSources(t *testing.T, root string, m *Monitor) {
	t.Helper()
	// Read-only connection, no migrations and no writes to the preserved store.
	db, err := gorm.Open(sqlite.Open("file:"+filepath.Join(root, "state/monitor.db")+"?mode=ro&immutable=1"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	var sources []ECSLogSource
	var leases []ECSLogLeaseWindow
	if err := db.Find(&sources).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Find(&leases).Error; err != nil {
		t.Fatal(err)
	}
	if len(sources) == 0 || len(sources) > 32 || len(leases) > 1024 {
		t.Fatal("unexpected source fixture")
	}
	if err := m.storeDB.Create(&sources).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&leases).Error; err != nil {
		t.Fatal(err)
	}
}

func readCloudOracleCounts(t *testing.T, root string, sources []ECSLogSource) map[string]int64 {
	t.Helper()
	want := map[string]int64{}
	for _, s := range sources {
		if _, ok := want[s.Node]; ok {
			continue
		}
		task := s.TaskARN[strings.LastIndex(s.TaskARN, "/")+1:]
		if !ecsTaskIDPattern.MatchString(task) {
			t.Fatal("invalid fixture task")
		}
		var events []struct{ Message string }
		readCloudFixtureJSON(t, filepath.Join(root, "logs-test-synthetic-"+task+".json"), &events)
		finals := 0
		for _, e := range events {
			if !strings.HasPrefix(e.Message, "SYNTHETIC_ORACLE ") {
				continue
			}
			var oracle struct {
				Count int64
				Final bool
			}
			if json.Unmarshal([]byte(strings.TrimPrefix(e.Message, "SYNTHETIC_ORACLE ")), &oracle) != nil {
				t.Fatal("invalid oracle")
			}
			if oracle.Final {
				finals++
				want[s.Node] = oracle.Count
			}
		}
		if finals != 1 || want[s.Node] <= 0 {
			t.Fatal("missing independent final count")
		}
	}
	return want
}

func assertCloudLaneCounts(t *testing.T, m *Monitor, want map[string]int64) {
	t.Helper()
	for lane, spec := range map[string]struct {
		DB          *gorm.DB
		Table, Expr string
	}{
		"access":   {m.storeDB, "nginx_minute_samples", "SUM(count)"},
		"error":    {m.storeDB, "nginx_error_minute_samples", "SUM(count)"},
		"reject":   {m.storeDB, "rejection_samples", "SUM(count)"},
		"evidence": {m.nginxEvidenceDB, "nginx_request_evidences", "COUNT(*)"},
	} {
		var rows []struct {
			Node string
			N    int64
		}
		if err := spec.DB.Table(spec.Table).Select(fmt.Sprintf("node, %s AS n", spec.Expr)).Group("node").Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if len(rows) != len(want) {
			t.Fatalf("%s missing node", lane)
		}
		for _, row := range rows {
			if row.N != want[row.Node] {
				t.Fatalf("%s count=%d want=%d", lane, row.N, want[row.Node])
			}
		}
	}
}
