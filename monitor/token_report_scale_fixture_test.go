package monitor

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func tokenTestCopyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// Bulk synthetic result alignment is fixture preparation, not a measurement of
// production cleanup or worker throughput. An explicitly bounded RAM directory
// avoids minutes of random Windows-bind checkpoint writes during this setup.
// All API/SQL timing still runs on the D-drive copy with the original settings,
// after the prepared DB has been checkpointed, copied back and reopened.
func tokenReportScaleFixtureChange(t *testing.T, m *Monitor, change func(*gorm.DB) error) {
	t.Helper()
	ramRoot := os.Getenv("TOKEN_SCALE_MUTATION_TMPDIR")
	if ramRoot == "" {
		if err := change(m.tokenStore()); err != nil {
			t.Fatal(err)
		}
		return
	}
	started := time.Now()
	destination := filepath.Clean(m.tokenStorePath())
	testRoot := filepath.Clean(os.Getenv("TEST_DATABASE_TMPDIR"))
	if testRoot == "." || !strings.HasPrefix(destination, testRoot+string(filepath.Separator)) || destination == os.Getenv("TOKEN_BACKLOG_SNAPSHOT") {
		t.Fatal("synthetic alignment requires a disposable test DB")
	}
	readLogger := m.tokenReadStore().Config.Logger
	for _, db := range []*gorm.DB{m.tokenReadDB, m.tokenDB} {
		pool, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		if err := pool.Close(); err != nil {
			t.Fatal(err)
		}
	}
	m.tokenReadDB, m.tokenDB = nil, nil
	if info, err := os.Stat(destination + "-wal"); err == nil && info.Size() != 0 {
		t.Fatal("closed fixture has uncheckpointed WAL")
	}
	dir, err := os.MkdirTemp(ramRoot, "token-synthetic-alignment-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "token.db")
	if err := tokenTestCopyFile(destination, file); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(sqlite.Open(file+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	defer pool.Close()
	if err := change(db); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(file + "-wal"); err == nil && info.Size() != 0 {
		t.Fatal("synthetic fixture has uncheckpointed WAL")
	}
	if err := tokenTestCopyFile(file, destination); err != nil {
		t.Fatal(err)
	}
	if err := m.openTokenStore(); err != nil {
		t.Fatal(err)
	}
	m.tokenReadStore().Config.Logger = readLogger
	if !m.tokenMeteringDiskOK() {
		t.Fatal("prepared D-drive fixture no longer meets the original disk reserve")
	}
	t.Logf("synthetic fixture prepared in bounded RAM, copied back to %s; original API/disk settings retained; setup=%s", destination, time.Since(started))
}
