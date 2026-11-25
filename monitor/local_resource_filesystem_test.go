package monitor

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// The runner supplies a dedicated bounded Linux filesystem. Never fill the
// host filesystem or silently turn this into a persistence qualification.
func TestLocalResourceFilesystemFullStopsAndRecovers(t *testing.T) {
	dir := os.Getenv("MONITOR_TEST_FULL_FS")
	if dir == "" {
		t.Skip("requires a dedicated bounded filesystem from the isolated runner")
	}
	if dir != "/fault" {
		t.Fatal("refusing an unrecognized fill target")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	if size := uint64(fs.Blocks) * uint64(fs.Bsize); size > 32<<20 || size < 4<<20 {
		t.Fatalf("unexpected filesystem size: %d", size)
	}
	// The test disk is deliberately tiny; keep only this fixture's reserve
	// proportional. Production thresholds/defaults are not changed.
	m := &Monitor{cfg: Settings{StorePath: filepath.Join(dir, "monitor.db"), LocalSnapshotOnly: true, UsageFactsHistoryMinFreeBytes: 1 << 20}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	if err := m.storeDB.Exec("CREATE TABLE fs_full_probe(id INTEGER PRIMARY KEY, data BLOB)").Error; err != nil {
		t.Fatal(err)
	}
	fill, err := os.OpenFile(filepath.Join(dir, "filler"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer fill.Close()
	for cycle := 0; cycle < 3; cycle++ {
		t.Logf("filesystem pressure cycle %d", cycle+1)
		testFilesystemPressureCycle(t, m, fill)
	}
}

func testFilesystemPressureCycle(t *testing.T, m *Monitor, fill *os.File) {
	t.Helper()
	if _, err := fill.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	var previous int64
	if err := m.storeDB.Raw("SELECT count(*) FROM fs_full_probe").Scan(&previous).Error; err != nil {
		t.Fatal(err)
	}
	block := make([]byte, 64<<10)
	var full error
	for written := 0; written < 32<<20; written += len(block) {
		if _, err := fill.Write(block); err != nil {
			full = err
			break
		}
	}
	if !errors.Is(full, syscall.ENOSPC) {
		t.Fatalf("dedicated filesystem did not fill: %v", full)
	}
	if ok, err := m.usageFactHistoryCapacityOK(); ok || err == nil {
		t.Fatal("full filesystem did not close capacity gate")
	}
	// SQLite can reuse free pages/preallocated WAL space even after the
	// filesystem is full. Exhaust that bounded reserve before asserting a
	// write failure; successful earlier commits must remain accounted for.
	committed := int64(0)
	var writeErr error
	for i := 0; i < 128; i++ {
		writeErr = m.storeDB.Exec("INSERT INTO fs_full_probe(data) VALUES(zeroblob(262144))").Error
		if writeErr != nil {
			break
		}
		committed++
	}
	if writeErr == nil || !localResourceTemporarilyUnavailable(writeErr) {
		t.Fatalf("expected bounded SQLite capacity failure: commits=%d error=%v", committed, writeErr)
	}
	info, err := fill.Stat()
	if err != nil || info.Size() < 64<<10 {
		t.Fatal("missing filler extent", err)
	}
	// Releasing a little space must not falsely reopen the capacity gate.
	if err := fill.Truncate(info.Size() - 64<<10); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.usageFactHistoryCapacityOK(); ok || err == nil {
		t.Fatal("insufficient recovery margin reopened capacity gate")
	}
	if err := fill.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.usageFactHistoryCapacityOK(); !ok || err != nil {
		entries, _ := os.ReadDir(filepath.Dir(fill.Name()))
		for _, entry := range entries {
			if info, statErr := entry.Info(); statErr == nil {
				t.Logf("fault fixture file %s bytes=%d", entry.Name(), info.Size())
			}
		}
		t.Fatalf("released filesystem stayed blocked: %v", err)
	}
	if err := m.storeDB.Exec("INSERT INTO fs_full_probe(data) VALUES(zeroblob(262144))").Error; err != nil {
		t.Fatalf("same connection did not recover without restart: %v", err)
	}
	var count int64
	if err := m.storeDB.Raw("SELECT count(*) FROM fs_full_probe").Scan(&count).Error; err != nil || count != previous+committed+1 {
		t.Fatalf("failed transaction leaked/retry duplicated: %d %v", count, err)
	}
}
