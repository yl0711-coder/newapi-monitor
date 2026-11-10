package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"time"
)

// The evidence database is independent of the main/facts backup set. Reuse its
// verified, atomic pre-migration snapshot protocol under a separate directory.
func backupNginxLengthMigration(path string) error {
	exists, err := existingRegularStore(path)
	if err != nil || !exists {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), storeBackupTimeout)
	defer cancel()
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return err
	}
	var columns int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('nginx_request_evidences') WHERE name IN ('request_length','request_length_state','request_length_version')").Scan(&columns)
	var tables int
	if err == nil {
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='nginx_request_evidences'").Scan(&tables)
	}
	closeErr := db.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if tables == 0 || columns == 3 {
		return nil
	}
	backup := &Monitor{cfg: Settings{StorePath: path, StoreBackupDir: filepath.Join(filepath.Dir(path), "nginx-evidence-migration-backups")}}
	if _, err := backup.createPreMigrationSnapshot(ctx, path, "", time.Now()); err != nil {
		return fmt.Errorf("backup evidence before request_length migration: %w", err)
	}
	return nil
}
