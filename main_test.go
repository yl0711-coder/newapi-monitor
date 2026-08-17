package main

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestMonitoredHTTPServerTimeouts(t *testing.T) {
	s := monitoredHTTPServer(":0", http.NewServeMux())
	if s.ReadHeaderTimeout != 10*time.Second || s.ReadTimeout != 30*time.Second || s.WriteTimeout != 2*time.Minute || s.IdleTimeout != 60*time.Second {
		t.Fatalf("HTTP 超时配置错误: %#v", s)
	}
}

func TestRunRestorePreMigrationCommandRequiresConfirmationAndRestoresPair(t *testing.T) {
	dir := t.TempDir()
	backupDir := filepath.Join(dir, "backups")
	settings := monitor.Settings{
		StorePath:                     filepath.Join(dir, "monitor.db"),
		UsageFactsStorePath:           filepath.Join(dir, "usage-facts.db"),
		StoreBackupDir:                backupDir,
		StoreBackupEnabled:            false,
		StoreMigrationBackupRetention: 3,
		LocalSnapshotOnly:             true,
	}
	first, err := monitor.New(settings)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	second, err := monitor.New(settings)
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
	snapshots, err := filepath.Glob(filepath.Join(backupDir, "pre-migrate-*"))
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("snapshot discovery: paths=%v err=%v", snapshots, err)
	}

	// The restore subcommand has no settings/DSN path. A deliberately invalid
	// production DSN must therefore be irrelevant to a successful local restore.
	t.Setenv("NEWAPI_LOG_DSN", "must-not-be-opened.invalid:1")
	target := filepath.Join(dir, "new-empty-volume")
	var output bytes.Buffer
	baseArgs := []string{"--snapshot", snapshots[0], "--target-dir", target}
	if err := runRestorePreMigrationCommand(baseArgs, &output); err == nil || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("missing confirmation should fail: %v", err)
	}
	args := append([]string(nil), baseArgs...)
	args = append(args, "--confirm", restorePreMigrationConfirmation)
	if err := runRestorePreMigrationCommand(args, &output); err != nil {
		t.Fatalf("restore CLI failed: %v", err)
	}
	if !strings.Contains(output.String(), "只能将旧镜像指向该新卷") {
		t.Fatalf("restore CLI omitted rollback warning: %q", output.String())
	}
	for _, name := range []string{"monitor.db", "usage-facts.db", "PRE_MIGRATION_RESTORE_READY"} {
		if info, err := os.Lstat(filepath.Join(target, name)); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("restored artifact %s missing/not regular: info=%v err=%v", name, info, err)
		}
	}
}

func TestRunRestoreStoreBackupSetCommandRequiresExplicitConfirmation(t *testing.T) {
	dir := t.TempDir()
	var output bytes.Buffer
	args := []string{
		"--manifest", filepath.Join(dir, "backup-set.json"),
		"--target-dir", filepath.Join(dir, "new-volume"),
	}
	if err := runRestoreStoreBackupSetCommand(args, &output); err == nil || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("missing runtime restore confirmation should fail before touching storage: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new-volume")); !os.IsNotExist(err) {
		t.Fatalf("unconfirmed restore touched target volume: %v", err)
	}
	if err := runRestoreStoreBackupSetCommand(append(args, "unexpected"), &output); err == nil ||
		!strings.Contains(err.Error(), "不接受位置参数") {
		t.Fatalf("unexpected positional argument was accepted: %v", err)
	}
}
