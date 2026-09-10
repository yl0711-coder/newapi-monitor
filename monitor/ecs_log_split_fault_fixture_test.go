package monitor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in, local-only verification of split producer exits against saved AWS
// task responses. The database is a checkpointed private snapshot, not live
// Monitor state. A collector closure must never conceal a producer SIGKILL.
func TestECSLogSplitCloudFaultProjectionOffline(t *testing.T) {
	root := os.Getenv("MONITOR_TEST_ECS_SPLIT_FAULT_EXPORT")
	if root == "" {
		t.Skip("private split fault export not supplied")
	}
	st, err := os.Lstat(root)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 || filepath.Dir(root) != "/private/tmp" || !strings.HasPrefix(filepath.Base(root), "ecs-split-fault-proof-") {
		t.Fatal("dedicated private split fault snapshot required")
	}
	var expected struct {
		Tasks []struct {
			TaskARN    string `json:"taskArn"`
			LastStatus string `json:"lastStatus"`
			Containers []struct {
				Name     string `json:"name"`
				ExitCode *int32 `json:"exitCode"`
			} `json:"containers"`
		} `json:"tasks"`
	}
	readCloudFixtureJSON(t, filepath.Join(root, "aws-tasks.json"), &expected)
	if len(expected.Tasks) != 6 {
		t.Fatal("this fixture requires five normal tasks and one split fault task")
	}
	exits := map[string]int32{}
	for _, task := range expected.Tasks {
		if task.LastStatus != "STOPPED" || !strings.Contains(task.TaskARN, ":task/monitor-ecs-acceptance-split-") {
			t.Fatal("independent isolated AWS stop evidence missing")
		}
		for _, c := range task.Containers {
			if c.Name == "nginx" || c.Name == "new-api" {
				if c.ExitCode == nil {
					t.Fatal("AWS producer exit code missing")
				}
				exits[task.TaskARN+"/"+c.Name] = *c.ExitCode
			}
		}
	}
	path := filepath.Join(root, "state/monitor.db")
	if st, err := os.Stat(path + "-wal"); err == nil && st.Size() > 0 {
		t.Fatal("snapshot must be checkpointed")
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
	if len(sources) != 24 {
		t.Fatal("six tasks must retain all four source lanes")
	}
	if err := projectArchiveDelivery(db, sources); err != nil {
		t.Fatal(err)
	}
	failed, normal := 0, 0
	for _, source := range sources {
		exit, ok := exits[source.TaskARN+"/"+source.Container]
		if !ok || source.StoppedAt == 0 || source.ProducerExitCode == nil || *source.ProducerExitCode != exit {
			t.Fatal("Monitor exit evidence differs from independent AWS response")
		}
		status := ecsLogSourceStatus(source, time.Now().Unix())
		if exit == 137 {
			failed++
			if source.Container != "nginx" || status != "delivery_gap_unresolved" || source.FinalBoundaryStatus == "retained_files_verified" {
				t.Fatalf("forced exit falsely complete: %s %s", status, source.FinalBoundaryStatus)
			}
		} else {
			normal++
			if exit != 0 || status != "stopped_archive_drained" || source.FinalBoundaryStatus != "retained_files_verified" {
				t.Fatalf("normal producer regressed: exit=%d status=%s boundary=%s", exit, status, source.FinalBoundaryStatus)
			}
		}
	}
	if failed != 3 || normal != 21 {
		t.Fatalf("split producer isolation failed: abnormal=%d normal=%d", failed, normal)
	}
	t.Log("AWS SIGKILL: nginx's three lanes retain gaps; independent new-api and five normal tasks retain 21 verified boundaries")
	verifyCloudFaultArchiveRecovery(t, root)
}
