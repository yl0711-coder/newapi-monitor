package monitor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in read-only projection of actual AWS-observed exits. No workers, AWS
// clients, migrations or writes to the original synthetic acceptance store.
func TestECSLogRealCloudFaultProjectionOffline(t *testing.T) {
	root := os.Getenv("MONITOR_TEST_ECS_FAULT_EXPORT")
	if root == "" {
		t.Skip("private synthetic fault export not supplied")
	}
	st, err := os.Lstat(root)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 || filepath.Dir(root) != "/private/tmp" || !strings.HasPrefix(filepath.Base(root), "ecs-cloud-acceptance-") {
		t.Fatal("dedicated private acceptance directory required")
	}
	path := filepath.Join(root, "state/monitor.db")
	if wal, err := os.Stat(path + "-wal"); err == nil && wal.Size() > 0 {
		t.Fatal("fixture must be stopped and checkpointed")
	}
	db, err := gorm.Open(sqlite.Open("file:"+path+"?mode=ro&immutable=1"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	var sources []ECSLogSource
	if err := db.Find(&sources).Error; err != nil {
		t.Fatal(err)
	}
	if err := projectArchiveDelivery(db, sources); err != nil {
		t.Fatal(err)
	}
	failed, normal := 0, 0
	for _, source := range sources {
		if source.StoppedAt == 0 || source.ProducerExitCode == nil {
			t.Fatal("independent AWS exit evidence missing")
		}
		status := ecsLogSourceStatus(source, time.Now().Unix())
		if *source.ProducerExitCode != 0 {
			failed++
			if status != "delivery_gap_unresolved" || source.FinalBoundaryStatus == "retained_files_verified" {
				t.Fatalf("abnormal source incorrectly healthy: %s %s %s", source.Node, status, source.FinalBoundaryStatus)
			}
		} else {
			normal++
			if status != "stopped_archive_drained" || source.FinalBoundaryStatus != "retained_files_verified" {
				t.Fatalf("normal proof regressed: %s %s %s", source.Node, status, source.FinalBoundaryStatus)
			}
		}
	}
	if failed != 4 || normal != 12 {
		t.Fatalf("unexpected fixture lanes: abnormal=%d normal=%d", failed, normal)
	}
	t.Log("AWS nonzero exit: 4 lanes retain delivery_gap_unresolved; 12 normal final boundaries unchanged")
	verifyCloudFaultArchiveRecovery(t, root)
}

// No final producer oracle exists for the killed process. Verify delivery of
// retained archive objects, never infer complete business/file coverage.
func verifyCloudFaultArchiveRecovery(t *testing.T, root string) {
	t.Helper()
	var cfg ECSAcceptanceConfig
	readCloudFixtureJSON(t, filepath.Join(root, "receiver.json"), &cfg)
	if !strings.HasPrefix(cfg.Audience, "monitor-ecs-acceptance-") {
		t.Fatal("synthetic fixture required")
	}
	dir := t.TempDir()
	copyStoppedCloudStore(t, root, dir)
	m, _, err := NewECSAcceptance(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close() // Deliberately no Start: local copies and original bytes only.
	reader, total := readCloudArchiveObjects(t, root, cfg.Prefix)
	var scan ECSLogArchiveScan
	if err := m.storeDB.First(&scan, 1).Error; err != nil {
		t.Fatal(err)
	}
	if scan.NextToken != "" {
		reader.pages[scan.NextToken] = reader.pages[""]
	}
	binding := cfg.Account + "/" + cfg.Region + "/" + cfg.Bucket + "/" + cfg.Prefix
	for turn := 0; turn < 80; turn++ {
		if err := newECSArchiveReplayer(m, cfg.Prefix).scan(context.Background(), reader, binding, time.Now()); err != nil {
			t.Fatal(err)
		}
		var accepted int64
		if err := m.storeDB.Model(&ECSLogArchiveReceipt{}).Where("status = 200").Count(&accepted).Error; err != nil {
			t.Fatal(err)
		}
		if accepted != int64(total) {
			continue
		}
		var sources []ECSLogSource
		if err := m.storeDB.Find(&sources).Error; err != nil {
			t.Fatal(err)
		}
		if err := projectArchiveDelivery(m.storeDB, sources); err != nil {
			t.Fatal(err)
		}
		for _, source := range sources {
			if source.ProducerExitCode != nil && *source.ProducerExitCode != 0 && (ecsLogSourceStatus(source, time.Now().Unix()) != "delivery_gap_unresolved" || source.FinalBoundaryStatus == "retained_files_verified") {
				t.Fatal("archive recovery concealed abnormal exit")
			}
		}
		t.Logf("all %d archived objects recovered; abnormal sources still unresolved", total)
		return
	}
	t.Fatal("retained fault archives did not finish replay")
}
