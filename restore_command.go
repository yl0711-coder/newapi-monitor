package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const restorePreMigrationConfirmation = "RESTORE_PRE_MIGRATION_SNAPSHOT"
const restoreStoreBackupSetConfirmation = "RESTORE_RUNTIME_BACKUP_SET"

// runRestorePreMigrationCommand is part of the regular monitor binary so the
// exact candidate image that created a snapshot can verify and restore it into
// a fresh volume. It never starts the HTTP service or opens a production DSN.
func runRestorePreMigrationCommand(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("restore-pre-migration", flag.ContinueOnError)
	flags.SetOutput(output)
	snapshotDir := flags.String("snapshot", "", "pre-migrate-* snapshot directory")
	targetDir := flags.String("target-dir", "", "new empty data directory/volume")
	confirmation := flags.String("confirm", "", "required restore confirmation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("restore-pre-migration 不接受位置参数")
	}
	if strings.TrimSpace(*snapshotDir) == "" || strings.TrimSpace(*targetDir) == "" {
		return errors.New("必须同时提供 --snapshot 和 --target-dir")
	}
	if *confirmation != restorePreMigrationConfirmation {
		return fmt.Errorf("必须显式提供 --confirm=%s", restorePreMigrationConfirmation)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := monitor.RestorePreMigrationSnapshot(ctx, *snapshotDir, *targetDir); err != nil {
		return fmt.Errorf("恢复迁移前 SQLite 快照失败: %w", err)
	}
	_, _ = fmt.Fprintf(output, "迁移前主库/facts 快照已恢复并复核到 %s；只能将旧镜像指向该新卷，禁止复用已迁移卷。\n", *targetDir)
	return nil
}

// runRestoreStoreBackupSetCommand restores an ordinary verified runtime
// backup set into a fresh volume.  It deliberately has no settings/DSN path.
func runRestoreStoreBackupSetCommand(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("restore-backup-set", flag.ContinueOnError)
	flags.SetOutput(output)
	manifest := flags.String("manifest", "", "backup-set-*.json manifest")
	targetDir := flags.String("target-dir", "", "new empty data directory/volume")
	mainName := flags.String("main-name", "nexus_monitor.db", "restored main SQLite file name")
	factsName := flags.String("facts-name", "usage-facts.db", "restored facts SQLite file name")
	confirmation := flags.String("confirm", "", "required restore confirmation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("restore-backup-set 不接受位置参数")
	}
	if strings.TrimSpace(*manifest) == "" || strings.TrimSpace(*targetDir) == "" {
		return errors.New("必须同时提供 --manifest 和 --target-dir")
	}
	if *confirmation != restoreStoreBackupSetConfirmation {
		return fmt.Errorf("必须显式提供 --confirm=%s", restoreStoreBackupSetConfirmation)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := monitor.RestoreStoreBackupSet(ctx, *manifest, *targetDir, *mainName, *factsName); err != nil {
		return fmt.Errorf("恢复运行期 SQLite 备份集失败: %w", err)
	}
	_, _ = fmt.Fprintf(output, "运行期 main/facts 备份集已恢复并复核到 %s；READY 已最后发布，下一步只能做无来源只读验收。\n", *targetDir)
	return nil
}
