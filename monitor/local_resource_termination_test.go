package monitor

// Opt-in process actors. The runner must use a private disk volume, no network,
// bounded memory and no restart policy. Ordinary package tests skip these.
import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func terminationFixtureDir(t *testing.T) string {
	t.Helper()
	mode := os.Getenv("MONITOR_TEST_TERMINATION_MODE")
	if mode != "sigkill" && mode != "oom" {
		t.Skip("explicit isolated termination mode required")
	}
	if os.Getenv("MONITOR_TEST_TERMINATION_ROOT") != "/data" {
		t.Fatal("refusing unknown persistent root")
	}
	return "/data/termination-" + mode
}

func TestLocalResourceExternalTerminationWriter(t *testing.T) {
	if os.Getenv("MONITOR_TEST_TERMINATION_PHASE") != "write" {
		t.Skip("isolated termination writer only")
	}
	dir := terminationFixtureDir(t)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal("writer requires a new private directory", err)
	}
	m := &Monitor{cfg: Settings{LocalSnapshotOnly: true, AlertsDisabled: true}, chNames: map[string]string{}}
	if err := m.openStore(filepath.Join(dir, "monitor.db")); err != nil {
		t.Fatal(err)
	}
	db, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;
CREATE TABLE termination_probe(id INTEGER PRIMARY KEY,amount INTEGER);
INSERT INTO termination_probe VALUES(1,123456)`); err != nil {
		t.Fatal(err)
	}
	job := UsageFactJob{ID: "termination-job", Kind: usageFactHistoryKindTail, Status: usageFactHistoryJobQueued,
		NextHour: 123, VerifyNextHour: 120, Attempts: 4, TrackedRevision: 1, SourceEpoch: "synthetic-termination"}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO termination_probe VALUES(2,999999)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE usage_fact_jobs SET status='complete',next_hour=999 WHERE id='termination-job'`); err != nil {
		t.Fatal(err)
	}
	ready, err := os.OpenFile(filepath.Join(dir, "ready"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ready.WriteString("committed amount and cursor retained; transaction open\n"); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(ready.Sync(), ready.Close()); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("MONITOR_TEST_TERMINATION_MODE") == "sigkill" {
		timer := time.NewTimer(40 * time.Second)
		defer timer.Stop()
		<-timer.C
		t.Fatal("runner did not externally SIGKILL the ready writer")
	}
	// Touch/retain at most 256 MiB. The runner gives this sole process a
	// 192 MiB cgroup with no container swap. No host memory exhaustion.
	var retained [][]byte
	for i := 0; i < 256; i++ {
		block := make([]byte, 1<<20)
		for page := 0; page < len(block); page += 4096 {
			block[page] = byte(i + 1)
		}
		retained = append(retained, block)
	}
	runtime.KeepAlive(retained)
	t.Fatal("bounded allocation did not produce the required cgroup OOM")
}

func TestLocalResourceExternalTerminationVerify(t *testing.T) {
	if os.Getenv("MONITOR_TEST_TERMINATION_PHASE") != "verify" {
		t.Skip("isolated termination verifier only")
	}
	dir := terminationFixtureDir(t)
	if _, err := os.Stat(filepath.Join(dir, "ready")); err != nil {
		t.Fatal("writer did not reach the intended critical point", err)
	}
	m := &Monitor{cfg: Settings{LocalSnapshotOnly: true, AlertsDisabled: true}, chNames: map[string]string{}}
	if err := m.openStore(filepath.Join(dir, "monitor.db")); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var job UsageFactJob
	if err := m.storeDB.First(&job, "id = ?", "termination-job").Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != usageFactHistoryJobQueued || job.Attempts != 4 || job.VerifyNextHour != 120 || (job.NextHour != 123 && job.NextHour != 124) {
		t.Fatalf("uncommitted cursor escaped or committed cursor lost: %+v", job)
	}
	db, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	var uncommitted int
	if err := db.QueryRow(`SELECT count(*) FROM termination_probe WHERE id=2`).Scan(&uncommitted); err != nil || uncommitted != 0 {
		t.Fatal("uncommitted money became visible", uncommitted, err)
	}
	// Replayable, bounded synthetic continuation; not a financial publication.
	if _, err := db.Exec(`BEGIN; INSERT OR IGNORE INTO termination_probe VALUES(3,42);
UPDATE usage_fact_jobs SET next_hour=124 WHERE id='termination-job' AND next_hour=123; COMMIT`); err != nil {
		t.Fatal(err)
	}
	var count, amount int64
	if err := db.QueryRow(`SELECT count(*),sum(amount) FROM termination_probe`).Scan(&count, &amount); err != nil || count != 2 || amount != 123498 {
		t.Fatal("continuation lost or repeated money", count, amount, err)
	}
	var check string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&check); err != nil || check != "ok" {
		t.Fatal(check, err)
	}
}

func TestLocalResourceReadonlyVolumeRejectsWrites(t *testing.T) {
	if os.Getenv("MONITOR_TEST_TERMINATION_PHASE") != "readonly" {
		t.Skip("isolated read-only disk volume only")
	}
	dir := terminationFixtureDir(t)
	_, err := os.OpenFile(filepath.Join(dir, "must-not-exist"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if !errors.Is(err, syscall.EROFS) {
		t.Fatal("runner did not mount a genuinely read-only filesystem", err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "monitor.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`UPDATE termination_probe SET amount=0`)
	if err == nil || localResourceTemporarilyUnavailable(err) {
		t.Fatal("read-only storage incorrectly accepted or treated as capacity wait", err)
	}
	var amount int64
	if err := db.QueryRow(`SELECT sum(amount) FROM termination_probe`).Scan(&amount); err != nil || amount != 123498 {
		t.Fatal("read-only inspection changed retained money", amount, err)
	}
	// No source, external calls, automatic repair or file removal is permitted.
	t.Log(fmt.Sprintf("read-only recovery data retained: %s", strings.TrimPrefix(dir, "/data/")))
}
