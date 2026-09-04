package monitor

// store_migration_backup.go protects both Monitor-owned SQLite files as one
// pre-migration recovery unit. It deliberately runs before either database is
// opened by GORM: an incomplete/corrupt snapshot must never be followed by an
// AutoMigrate.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	preMigrationSnapshotVersion = 1
	// Bump this ID whenever either AutoMigrate model set or a post-migration
	// schema/data transform changes. Restarts of the same plan reuse its pinned
	// original snapshot, so they cannot prune away the old-image rollback point.
	// v31 在 v30 上增加模型监控迟到日志定稿水位和上游用量尾部同步模式。
	// 新表/字段会改变 AutoMigrate 模型集，必须先生成当前时点的新快照，不得复用 v30。
	preMigrationPlanID               = "main-facts-schema-20260904-v31-upstream-errorlog-identity-archive-metric-finalize-cursor-upstream-usage-tail-mode"
	preMigrationCombinedPlanID       = "main-facts-schema-20260904-v31-nginx-source-v2-upstream-errorlog-identity-archive-metric-finalize-cursor-upstream-usage-tail-mode"
	preMigrationSnapshotPrefix       = "pre-migrate-"
	preMigrationReferencePrefix      = ".pre-migration-plan-"
	preMigrationReferenceSuffix      = ".json"
	preMigrationManifestName         = "manifest.json"
	preMigrationReadyName            = "READY"
	preMigrationRestoreReadyName     = "PRE_MIGRATION_RESTORE_READY"
	preMigrationMainSnapshotName     = "monitor.db"
	preMigrationFactsSnapshotName    = "usage-facts.db"
	preMigrationSnapshotFileMaxBytes = 1 << 20
)

type sqliteQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type sqliteLogicalInventory struct {
	SchemaSHA256 string           `json:"schema_sha256"`
	TableRows    map[string]int64 `json:"table_rows"`
}

type preMigrationSnapshotStore struct {
	Role         string           `json:"role"`
	SourceFile   string           `json:"source_file"`
	Present      bool             `json:"present"`
	SnapshotFile string           `json:"snapshot_file,omitempty"`
	SizeBytes    int64            `json:"size_bytes,omitempty"`
	FileSHA256   string           `json:"file_sha256,omitempty"`
	SchemaSHA256 string           `json:"schema_sha256,omitempty"`
	TableRows    map[string]int64 `json:"table_rows,omitempty"`
}

type preMigrationSnapshotManifest struct {
	FormatVersion int                         `json:"format_version"`
	MigrationPlan string                      `json:"migration_plan"`
	CreatedAt     string                      `json:"created_at"`
	Stores        []preMigrationSnapshotStore `json:"stores"`
}

type preMigrationPlanReference struct {
	FormatVersion  int    `json:"format_version"`
	MigrationPlan  string `json:"migration_plan"`
	SnapshotDir    string `json:"snapshot_dir"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type preMigrationSnapshotResult struct {
	SnapshotDir string
	Reused      bool
	checked     map[string]bool
}

func (r preMigrationSnapshotResult) checkedPath(path string) bool {
	return r.checked[normalizedStorePath(path)]
}

type preMigrationSource struct {
	role         string
	path         string
	sourceFile   string
	snapshotFile string
	present      bool
	db           *sql.DB
	conn         *sql.Conn
	inventory    sqliteLogicalInventory
}

func preMigrationSources(mainPath, factsPath string) []*preMigrationSource {
	sources := make([]*preMigrationSource, 0, 2)
	if storeUsesFile(mainPath) {
		sources = append(sources, &preMigrationSource{
			role: "main", path: mainPath, sourceFile: filepath.Base(mainPath), snapshotFile: preMigrationMainSnapshotName,
		})
	}
	if storeUsesFile(factsPath) && !sameStorePath(mainPath, factsPath) {
		sources = append(sources, &preMigrationSource{
			role: "facts", path: factsPath, sourceFile: filepath.Base(factsPath), snapshotFile: preMigrationFactsSnapshotName,
		})
	}
	return sources
}

func normalizedStorePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return filepath.Clean(path)
	}
	return abs
}

func sameStorePath(a, b string) bool {
	a, b = normalizedStorePath(a), normalizedStorePath(b)
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	aInfo, aErr := os.Stat(a)
	bInfo, bErr := os.Stat(b)
	return aErr == nil && bErr == nil && os.SameFile(aInfo, bInfo)
}

func sqliteReadWriteDSN(path string) string {
	// Keep this separate from the runtime WAL DSN. The migration gate must read
	// the journal mode already on disk rather than changing it before backup.
	dsn := sqliteReadOnlyDSN(path)
	return strings.Replace(dsn, "mode=ro", "mode=rw", 1)
}

func sqliteQuickCheckQuery(ctx context.Context, q sqliteQueryer) error {
	rows, err := q.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return err
		}
		count++
		if strings.TrimSpace(strings.ToLower(result)) != "ok" {
			return fmt.Errorf("quick_check: %s", result)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("quick_check 返回 %d 行，期望 1 行 ok", count)
	}
	return nil
}

func quoteSQLiteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func writeInventorySchemaField(h hash.Hash, values ...string) {
	for _, value := range values {
		_, _ = io.WriteString(h, value)
		_, _ = h.Write([]byte{0})
	}
}

// inspectSQLiteLogicalInventory records every application table row count and
// a deterministic hash of all application schema objects. Counts are explicit
// (rather than inferred from file/page size) so VACUUM compaction cannot hide a
// missing table or row set.
func inspectSQLiteLogicalInventory(ctx context.Context, q sqliteQueryer) (sqliteLogicalInventory, error) {
	rows, err := q.QueryContext(ctx, `
SELECT type, name, tbl_name, COALESCE(sql, '')
FROM sqlite_master
WHERE name NOT LIKE 'sqlite_%'
ORDER BY type, name`)
	if err != nil {
		return sqliteLogicalInventory{}, err
	}
	type schemaObject struct {
		typ, name, table, sql string
	}
	objects := make([]schemaObject, 0)
	tableNames := make([]string, 0)
	for rows.Next() {
		var object schemaObject
		if err := rows.Scan(&object.typ, &object.name, &object.table, &object.sql); err != nil {
			_ = rows.Close()
			return sqliteLogicalInventory{}, err
		}
		objects = append(objects, object)
		if object.typ == "table" {
			tableNames = append(tableNames, object.name)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return sqliteLogicalInventory{}, err
	}
	if err := rows.Close(); err != nil {
		return sqliteLogicalInventory{}, err
	}

	schemaHash := sha256.New()
	for _, object := range objects {
		writeInventorySchemaField(schemaHash, object.typ, object.name, object.table, object.sql)
	}
	tableRows := make(map[string]int64, len(tableNames))
	for _, table := range tableNames {
		var count int64
		query := "SELECT COUNT(*) FROM " + quoteSQLiteIdentifier(table)
		if err := q.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return sqliteLogicalInventory{}, fmt.Errorf("统计表 %q 行数失败: %w", table, err)
		}
		tableRows[table] = count
	}
	return sqliteLogicalInventory{
		SchemaSHA256: hex.EncodeToString(schemaHash.Sum(nil)),
		TableRows:    tableRows,
	}, nil
}

func compareSQLiteInventories(want, got sqliteLogicalInventory) error {
	if want.SchemaSHA256 != got.SchemaSHA256 {
		return fmt.Errorf("schema hash 不一致: source=%s snapshot=%s", want.SchemaSHA256, got.SchemaSHA256)
	}
	if len(want.TableRows) != len(got.TableRows) {
		return fmt.Errorf("表数量不一致: source=%d snapshot=%d", len(want.TableRows), len(got.TableRows))
	}
	names := make([]string, 0, len(want.TableRows))
	for name := range want.TableRows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		gotCount, ok := got.TableRows[name]
		if !ok {
			return fmt.Errorf("快照缺少表 %q", name)
		}
		if want.TableRows[name] != gotCount {
			return fmt.Errorf("表 %q 行数不一致: source=%d snapshot=%d", name, want.TableRows[name], gotCount)
		}
	}
	return nil
}

func fileSHA256(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func syncFile(path string) error {
	// Use a writable descriptor for portability: some filesystems reject fsync
	// on an O_RDONLY descriptor even when the file itself is writable.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func existingRegularStore(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("SQLite 路径不是普通文件: %s", path)
	}
	return true, nil
}

func lockPreMigrationSource(ctx context.Context, source *preMigrationSource) error {
	present, err := existingRegularStore(source.path)
	if err != nil {
		return err
	}
	source.present = present
	if !present {
		return nil
	}
	db, err := sql.Open("sqlite", sqliteReadWriteDSN(source.path))
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return fmt.Errorf("获取迁移前写入闸门失败（请先停止旧 Monitor）: %w", err)
	}
	source.db = db
	source.conn = conn
	if err := sqliteQuickCheckQuery(ctx, conn); err != nil {
		return fmt.Errorf("源库 quick_check 失败: %w", err)
	}
	inventory, err := inspectSQLiteLogicalInventory(ctx, conn)
	if err != nil {
		return fmt.Errorf("源库计数清单失败: %w", err)
	}
	source.inventory = inventory
	return nil
}

func releasePreMigrationSource(source *preMigrationSource) {
	if source.conn != nil {
		_, _ = source.conn.ExecContext(context.Background(), "ROLLBACK")
		_ = source.conn.Close()
		source.conn = nil
	}
	if source.db != nil {
		_ = source.db.Close()
		source.db = nil
	}
}

func vacuumSourceInto(ctx context.Context, sourcePath, targetPath string) error {
	sourceDB, err := sql.Open("sqlite", sqliteReadOnlyDSN(sourcePath))
	if err != nil {
		return err
	}
	sourceDB.SetMaxOpenConns(1)
	escaped := strings.ReplaceAll(targetPath, "'", "''")
	_, vacuumErr := sourceDB.ExecContext(ctx, "VACUUM INTO '"+escaped+"'")
	closeErr := sourceDB.Close()
	if vacuumErr != nil {
		return vacuumErr
	}
	return closeErr
}

func verifySQLiteSnapshotFile(ctx context.Context, path string, want sqliteLogicalInventory) (string, int64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", 0, err
	}
	if !info.Mode().IsRegular() {
		return "", 0, fmt.Errorf("快照不是普通文件: %s", filepath.Base(path))
	}
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return "", 0, err
	}
	db.SetMaxOpenConns(1)
	if err := sqliteQuickCheckQuery(ctx, db); err != nil {
		_ = db.Close()
		return "", 0, fmt.Errorf("快照 quick_check 失败: %w", err)
	}
	got, err := inspectSQLiteLogicalInventory(ctx, db)
	closeErr := db.Close()
	if err != nil {
		return "", 0, fmt.Errorf("快照计数清单失败: %w", err)
	}
	if closeErr != nil {
		return "", 0, closeErr
	}
	if err := compareSQLiteInventories(want, got); err != nil {
		return "", 0, err
	}
	return fileSHA256(path)
}

func validateSnapshotLeafName(name, field string) error {
	if name == "" || filepath.Base(name) != name || name == "." || name == ".." {
		return fmt.Errorf("%s 不是安全文件名", field)
	}
	return nil
}

func marshalAndSyncSnapshotManifest(dir string, manifest preMigrationSnapshotManifest) ([]byte, error) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	manifestPath := filepath.Join(dir, preMigrationManifestName)
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		return nil, err
	}
	if err := syncFile(manifestPath); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	readyData := []byte("sha256:" + hex.EncodeToString(digest[:]) + "\n")
	readyPath := filepath.Join(dir, preMigrationReadyName)
	if err := os.WriteFile(readyPath, readyData, 0o600); err != nil {
		return nil, err
	}
	if err := syncFile(readyPath); err != nil {
		return nil, err
	}
	return data, nil
}

func preMigrationReferenceName(mainPath, factsPath string) string {
	// The path key is stable across plan bumps. A new plan reads the prior
	// reference, publishes its own snapshot, then atomically replaces this one;
	// retention can subsequently retire the no-longer-pinned older plan.
	digest := sha256.Sum256([]byte("main-facts\x00" + normalizedStorePath(mainPath) + "\x00" + normalizedStorePath(factsPath)))
	return preMigrationReferencePrefix + hex.EncodeToString(digest[:16]) + preMigrationReferenceSuffix
}

func writePreMigrationPlanReference(dir, mainPath, factsPath, snapshotName, manifestHash, planID string) error {
	reference := preMigrationPlanReference{
		FormatVersion:  preMigrationSnapshotVersion,
		MigrationPlan:  planID,
		SnapshotDir:    snapshotName,
		ManifestSHA256: manifestHash,
	}
	data, err := json.MarshalIndent(reference, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	finalPath := filepath.Join(dir, preMigrationReferenceName(mainPath, factsPath))
	tmpPath := finalPath + fmt.Sprintf(".tmp-%d", time.Now().UnixNano())
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	cleanup = false
	return syncDirectory(dir)
}

func readPreMigrationPlanReference(dir, mainPath, factsPath string) (preMigrationPlanReference, bool, error) {
	path := filepath.Join(dir, preMigrationReferenceName(mainPath, factsPath))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return preMigrationPlanReference{}, false, nil
	}
	if err != nil {
		return preMigrationPlanReference{}, false, err
	}
	if !info.Mode().IsRegular() {
		return preMigrationPlanReference{}, false, errors.New("迁移计划引用不是普通文件")
	}
	data, err := readSnapshotControlFile(path)
	if err != nil {
		return preMigrationPlanReference{}, false, err
	}
	var reference preMigrationPlanReference
	if err := json.Unmarshal(data, &reference); err != nil {
		return preMigrationPlanReference{}, false, fmt.Errorf("解析迁移计划引用失败: %w", err)
	}
	if reference.FormatVersion != preMigrationSnapshotVersion || reference.MigrationPlan == "" {
		return preMigrationPlanReference{}, false, errors.New("迁移计划引用版本/plan 无效")
	}
	if err := validateSnapshotLeafName(reference.SnapshotDir, "snapshot_dir"); err != nil {
		return preMigrationPlanReference{}, false, err
	}
	if len(reference.ManifestSHA256) != sha256.Size*2 {
		return preMigrationPlanReference{}, false, errors.New("迁移计划引用 manifest hash 无效")
	}
	return reference, true, nil
}

func reusePreMigrationPlanSnapshot(ctx context.Context, dir, mainPath, factsPath, planID string) (preMigrationSnapshotResult, bool, error) {
	result := preMigrationSnapshotResult{checked: make(map[string]bool)}
	reference, found, err := readPreMigrationPlanReference(dir, mainPath, factsPath)
	if err != nil || !found {
		return result, found, err
	}
	if reference.MigrationPlan != planID {
		// A later schema plan supersedes the pointer only after its own snapshot is
		// durable. The prior plan is not an error and remains recoverable meanwhile.
		return result, false, nil
	}
	snapshotDir := filepath.Join(dir, reference.SnapshotDir)
	manifest, manifestHash, err := loadAndVerifyPreMigrationSnapshot(ctx, snapshotDir)
	if err != nil {
		return result, true, fmt.Errorf("已固定的迁移前快照不可用: %w", err)
	}
	if manifest.MigrationPlan != planID || manifestHash != reference.ManifestSHA256 {
		return result, true, errors.New("迁移计划引用与快照 manifest 不一致")
	}
	manifestStores := make(map[string]preMigrationSnapshotStore, len(manifest.Stores))
	for _, store := range manifest.Stores {
		manifestStores[store.Role] = store
	}
	sources := preMigrationSources(mainPath, factsPath)
	if len(sources) != len(manifest.Stores) {
		return result, true, errors.New("迁移计划引用的数据库集合与当前配置不一致")
	}
	for _, source := range sources {
		store, ok := manifestStores[source.role]
		if !ok || store.SourceFile != source.sourceFile {
			return result, true, fmt.Errorf("迁移计划引用缺少当前 %s 数据库", source.role)
		}
		checked, err := preflightStoreIntegrity(source.path)
		if err != nil {
			return result, true, fmt.Errorf("当前 %s SQLite 完整性检查失败: %w", source.role, err)
		}
		if store.Present && !checked {
			return result, true, fmt.Errorf("当前 %s SQLite 丢失；拒绝用空库越过已固定快照", source.role)
		}
		if checked {
			result.checked[normalizedStorePath(source.path)] = true
		}
	}
	result.SnapshotDir = snapshotDir
	result.Reused = true
	return result, true, nil
}

// createPreMigrationSnapshot creates one atomically-published manifest for the
// main and facts stores. Both BEGIN IMMEDIATE locks are acquired before either
// VACUUM INTO starts, so the pair cannot change while it is copied. This is not
// process mutual exclusion: deployment must stop and confirm the old Monitor is
// gone before starting the candidate, because an idle old process could resume
// writing after these locks are released.
func (m *Monitor) createPreMigrationSnapshot(parent context.Context, mainPath, factsPath string, now time.Time) (preMigrationSnapshotResult, error) {
	result := preMigrationSnapshotResult{checked: make(map[string]bool)}
	sources := preMigrationSources(mainPath, factsPath)
	if len(sources) == 0 {
		return result, nil
	}

	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	dir := m.storeBackupDir()
	planID, err := m.preMigrationPlanForStore(ctx, mainPath)
	if err != nil {
		return result, err
	}
	if reused, found, err := reusePreMigrationPlanSnapshot(ctx, dir, mainPath, factsPath, planID); err != nil {
		return result, fmt.Errorf("复核已固定迁移前快照失败: %w", err)
	} else if found {
		return reused, nil
	}
	// VACUUM INTO temporarily needs another full copy of both stores. Refuse
	// migration before taking SQLite locks when the shared filesystem cannot
	// hold source+WAL plus the established 20%/2GiB safety reserve.
	if sourceBytes, err := storeBackupSourceBytes(mainPath, factsPath); err != nil {
		return result, fmt.Errorf("迁移前备份空间预检失败: %w", err)
	} else if sourceBytes > 0 {
		if err := preflightStoreBackupSpace(dir, mainPath, factsPath); err != nil {
			return result, fmt.Errorf("迁移前备份空间不足: %w", err)
		}
	}
	for _, source := range sources {
		if err := lockPreMigrationSource(ctx, source); err != nil {
			for i := len(sources) - 1; i >= 0; i-- {
				releasePreMigrationSource(sources[i])
			}
			return result, fmt.Errorf("%s SQLite 迁移前检查失败: %w", source.role, err)
		}
		if source.present {
			result.checked[normalizedStorePath(source.path)] = true
		}
	}
	defer func() {
		for i := len(sources) - 1; i >= 0; i-- {
			releasePreMigrationSource(sources[i])
		}
	}()

	present := 0
	for _, source := range sources {
		if source.present {
			present++
		}
	}
	if present == 0 {
		return result, nil
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return result, fmt.Errorf("创建迁移前备份目录失败: %w", err)
	}
	stamp := now.UTC().Format("20060102-150405.000000000")
	finalName := preMigrationSnapshotPrefix + stamp
	finalDir := filepath.Join(dir, finalName)
	tmpDir := filepath.Join(dir, "."+finalName+".tmp")
	if err := os.Mkdir(tmpDir, 0o700); err != nil {
		return result, fmt.Errorf("创建迁移前临时快照目录失败: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	manifest := preMigrationSnapshotManifest{
		FormatVersion: preMigrationSnapshotVersion,
		MigrationPlan: planID,
		CreatedAt:     now.UTC().Format(time.RFC3339Nano),
		Stores:        make([]preMigrationSnapshotStore, 0, len(sources)),
	}
	for _, source := range sources {
		store := preMigrationSnapshotStore{
			Role:       source.role,
			SourceFile: source.sourceFile,
			Present:    source.present,
		}
		if source.present {
			targetPath := filepath.Join(tmpDir, source.snapshotFile)
			if err := vacuumSourceInto(ctx, source.path, targetPath); err != nil {
				return result, fmt.Errorf("%s SQLite 一致性快照失败: %w", source.role, err)
			}
			if err := os.Chmod(targetPath, 0o600); err != nil {
				return result, err
			}
			fileHash, size, err := verifySQLiteSnapshotFile(ctx, targetPath, source.inventory)
			if err != nil {
				return result, fmt.Errorf("%s SQLite 快照校验失败: %w", source.role, err)
			}
			if err := syncFile(targetPath); err != nil {
				return result, fmt.Errorf("%s SQLite 快照落盘失败: %w", source.role, err)
			}
			store.SnapshotFile = source.snapshotFile
			store.SizeBytes = size
			store.FileSHA256 = fileHash
			store.SchemaSHA256 = source.inventory.SchemaSHA256
			store.TableRows = source.inventory.TableRows
		}
		manifest.Stores = append(manifest.Stores, store)
	}
	manifestData, err := marshalAndSyncSnapshotManifest(tmpDir, manifest)
	if err != nil {
		return result, fmt.Errorf("写入迁移前 manifest 失败: %w", err)
	}
	manifestDigest := sha256.Sum256(manifestData)
	manifestHash := hex.EncodeToString(manifestDigest[:])
	if err := syncDirectory(tmpDir); err != nil {
		return result, fmt.Errorf("迁移前快照目录落盘失败: %w", err)
	}
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return result, fmt.Errorf("发布迁移前快照失败: %w", err)
	}
	published = true
	result.SnapshotDir = finalDir
	if err := syncDirectory(dir); err != nil {
		return result, fmt.Errorf("迁移前快照发布目录落盘失败: %w", err)
	}
	if err := writePreMigrationPlanReference(dir, mainPath, factsPath, finalName, manifestHash, planID); err != nil {
		return result, fmt.Errorf("固定迁移前回滚点失败: %w", err)
	}
	if err := prunePreMigrationSnapshots(dir, m.storeMigrationBackupRetention()); err != nil {
		// The new set is already durable and atomically published. Do not discard
		// a valid rollback point merely because retention cleanup needs attention.
		return result, fmt.Errorf("迁移前快照已完成，但清理旧快照失败: %w", err)
	}
	return result, nil
}

func (m *Monitor) preMigrationPlanForStore(ctx context.Context, mainPath string) (string, error) {
	return InspectPreMigrationPlan(ctx, mainPath, m.cfg.NginxSourceV2Enabled)
}

// InspectPreMigrationPlan 以只读方式判定候选镜像对现有主库将选择的
// 迁移方案。发布预检在停旧 Monitor 之前调用它，因此不允许创建、
// 迁移或改写 SQLite；source-v2 部分存在时也必须是完整 schema。
func InspectPreMigrationPlan(ctx context.Context, mainPath string, sourceV2Enabled bool) (string, error) {
	present, err := existingRegularStore(mainPath)
	if err != nil {
		return "", err
	}
	if !present {
		if sourceV2Enabled {
			return preMigrationCombinedPlanID, nil
		}
		return preMigrationPlanID, nil
	}
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(mainPath))
	if err != nil {
		return "", err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ready, err := inspectNginxSourceV2Schema(ctx, db)
	if err != nil {
		return "", err
	}
	if !ready {
		if sourceV2Enabled {
			return preMigrationCombinedPlanID, nil
		}
		return preMigrationPlanID, nil
	}
	return preMigrationCombinedPlanID, nil
}

// currentMigrationPlanID labels periodic backup sets with the schema family
// they actually contain. A store that already owns the complete isolated
// source-v2 ledger remains on the combined v28 plan even when the feature flag
// is later disabled, so operators cannot mistake that backup for a pre-v2
// rollback point.
func (m *Monitor) currentMigrationPlanID() string {
	if m.cfg.NginxSourceV2Enabled || m.nginxSourceV2SchemaReady.Load() {
		return preMigrationCombinedPlanID
	}
	return preMigrationPlanID
}

func (m *Monitor) storeMigrationBackupRetention() int {
	retention := m.cfg.StoreMigrationBackupRetention
	if retention <= 0 {
		retention = 3
	}
	if retention > 30 {
		retention = 30
	}
	return retention
}

func prunePreMigrationSnapshots(dir string, retention int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	protected := make(map[string]bool)
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || !strings.HasPrefix(name, preMigrationReferencePrefix) || !strings.HasSuffix(name, preMigrationReferenceSuffix) {
			continue
		}
		data, err := readSnapshotControlFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		var reference preMigrationPlanReference
		if json.Unmarshal(data, &reference) == nil && validateSnapshotLeafName(reference.SnapshotDir, "snapshot_dir") == nil {
			protected[reference.SnapshotDir] = true
		}
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), preMigrationSnapshotPrefix) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if info, err := os.Lstat(filepath.Join(path, preMigrationReadyName)); err == nil && info.Mode().IsRegular() {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	for len(paths) > retention {
		removeAt := -1
		for i, path := range paths {
			if !protected[filepath.Base(path)] {
				removeAt = i
				break
			}
		}
		if removeAt < 0 {
			break
		}
		if err := os.RemoveAll(paths[removeAt]); err != nil {
			return fmt.Errorf("清理旧迁移前快照 %s 失败: %w", filepath.Base(paths[removeAt]), err)
		}
		paths = append(paths[:removeAt], paths[removeAt+1:]...)
	}
	return nil
}

func readSnapshotControlFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, preMigrationSnapshotFileMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > preMigrationSnapshotFileMaxBytes {
		return nil, fmt.Errorf("控制文件超过 %d 字节", preMigrationSnapshotFileMaxBytes)
	}
	return data, nil
}

func loadAndVerifyPreMigrationSnapshot(ctx context.Context, snapshotDir string) (preMigrationSnapshotManifest, string, error) {
	info, err := os.Lstat(snapshotDir)
	if err != nil {
		return preMigrationSnapshotManifest{}, "", err
	}
	if !info.IsDir() {
		return preMigrationSnapshotManifest{}, "", errors.New("迁移前快照路径不是目录")
	}
	manifestData, err := readSnapshotControlFile(filepath.Join(snapshotDir, preMigrationManifestName))
	if err != nil {
		return preMigrationSnapshotManifest{}, "", fmt.Errorf("读取 manifest 失败: %w", err)
	}
	readyData, err := readSnapshotControlFile(filepath.Join(snapshotDir, preMigrationReadyName))
	if err != nil {
		return preMigrationSnapshotManifest{}, "", fmt.Errorf("快照未原子发布（缺少 READY）: %w", err)
	}
	digest := sha256.Sum256(manifestData)
	manifestHash := hex.EncodeToString(digest[:])
	if strings.TrimSpace(string(readyData)) != "sha256:"+manifestHash {
		return preMigrationSnapshotManifest{}, "", errors.New("READY 与 manifest 校验和不一致")
	}
	var manifest preMigrationSnapshotManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return preMigrationSnapshotManifest{}, "", fmt.Errorf("解析 manifest 失败: %w", err)
	}
	if manifest.FormatVersion != preMigrationSnapshotVersion {
		return preMigrationSnapshotManifest{}, "", fmt.Errorf("不支持的快照格式版本 %d", manifest.FormatVersion)
	}
	if strings.TrimSpace(manifest.MigrationPlan) == "" {
		return preMigrationSnapshotManifest{}, "", errors.New("manifest 缺少 migration_plan")
	}
	if len(manifest.Stores) == 0 || len(manifest.Stores) > 2 {
		return preMigrationSnapshotManifest{}, "", fmt.Errorf("manifest stores 数量无效: %d", len(manifest.Stores))
	}
	roles := make(map[string]bool, len(manifest.Stores))
	present := 0
	for _, store := range manifest.Stores {
		if store.Role != "main" && store.Role != "facts" {
			return preMigrationSnapshotManifest{}, "", fmt.Errorf("未知 store role %q", store.Role)
		}
		if roles[store.Role] {
			return preMigrationSnapshotManifest{}, "", fmt.Errorf("重复 store role %q", store.Role)
		}
		roles[store.Role] = true
		if err := validateSnapshotLeafName(store.SourceFile, "source_file"); err != nil {
			return preMigrationSnapshotManifest{}, "", err
		}
		if !store.Present {
			if store.SnapshotFile != "" || store.SizeBytes != 0 || store.FileSHA256 != "" || store.SchemaSHA256 != "" || len(store.TableRows) != 0 {
				return preMigrationSnapshotManifest{}, "", fmt.Errorf("缺失的 %s store 带有快照数据", store.Role)
			}
			continue
		}
		present++
		if err := validateSnapshotLeafName(store.SnapshotFile, "snapshot_file"); err != nil {
			return preMigrationSnapshotManifest{}, "", err
		}
		if store.SizeBytes <= 0 || len(store.FileSHA256) != sha256.Size*2 || len(store.SchemaSHA256) != sha256.Size*2 || store.TableRows == nil {
			return preMigrationSnapshotManifest{}, "", fmt.Errorf("%s store manifest 校验字段不完整", store.Role)
		}
		path := filepath.Join(snapshotDir, store.SnapshotFile)
		inventory := sqliteLogicalInventory{SchemaSHA256: store.SchemaSHA256, TableRows: store.TableRows}
		fileHash, size, err := verifySQLiteSnapshotFile(ctx, path, inventory)
		if err != nil {
			return preMigrationSnapshotManifest{}, "", fmt.Errorf("%s 快照验证失败: %w", store.Role, err)
		}
		if size != store.SizeBytes || fileHash != store.FileSHA256 {
			return preMigrationSnapshotManifest{}, "", fmt.Errorf("%s 快照文件哈希/大小与 manifest 不一致", store.Role)
		}
	}
	if present == 0 {
		return preMigrationSnapshotManifest{}, "", errors.New("manifest 不包含任何现有 SQLite")
	}
	return manifest, manifestHash, nil
}

// RestorePreMigrationSnapshot restores a verified set into an empty target
// directory. It never overwrites an existing file. Database files are copied to
// hidden staging names and verified again; the READY marker is written last, so
// an interrupted restore is visibly incomplete and must never be started.
func RestorePreMigrationSnapshot(parent context.Context, snapshotDir, targetDir string) error {
	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	manifest, manifestHash, err := loadAndVerifyPreMigrationSnapshot(ctx, snapshotDir)
	if err != nil {
		return err
	}
	createdTarget := false
	info, err := os.Lstat(targetDir)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(targetDir, 0o700); err != nil {
			return fmt.Errorf("创建恢复目录失败: %w", err)
		}
		createdTarget = true
	} else if err != nil {
		return err
	} else if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("恢复目标不得是符号链接")
	} else if !info.IsDir() {
		return errors.New("恢复目标不是目录")
	}
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("恢复目标必须是全新的空目录/空数据卷；拒绝覆盖现有文件")
	}

	type stagedStore struct {
		store preMigrationSnapshotStore
		tmp   string
		final string
	}
	staged := make([]stagedStore, 0, len(manifest.Stores))
	cleanup := true
	defer func() {
		if !cleanup {
			return
		}
		for _, item := range staged {
			_ = os.Remove(item.tmp)
			_ = os.Remove(item.final)
		}
		_ = os.Remove(filepath.Join(targetDir, preMigrationRestoreReadyName))
		if createdTarget {
			_ = os.Remove(targetDir)
		}
	}()
	targetNames := make(map[string]bool)
	for _, store := range manifest.Stores {
		if !store.Present {
			continue
		}
		if targetNames[store.SourceFile] {
			return fmt.Errorf("两个 store 的恢复文件名冲突: %s", store.SourceFile)
		}
		targetNames[store.SourceFile] = true
		item := stagedStore{
			store: store,
			tmp:   filepath.Join(targetDir, "."+store.SourceFile+".restore-tmp"),
			final: filepath.Join(targetDir, store.SourceFile),
		}
		src, err := os.Open(filepath.Join(snapshotDir, store.SnapshotFile))
		if err != nil {
			return err
		}
		dst, err := os.OpenFile(item.tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			_ = src.Close()
			return err
		}
		// Register the staging path immediately so copy/fsync/verification errors
		// cannot leave a plausible-looking partial database behind.
		staged = append(staged, item)
		_, copyErr := io.Copy(dst, src)
		syncErr := dst.Sync()
		dstCloseErr := dst.Close()
		srcCloseErr := src.Close()
		if copyErr != nil {
			return fmt.Errorf("复制 %s 快照失败: %w", store.Role, copyErr)
		}
		if syncErr != nil {
			return fmt.Errorf("恢复 %s 快照落盘失败: %w", store.Role, syncErr)
		}
		if dstCloseErr != nil {
			return dstCloseErr
		}
		if srcCloseErr != nil {
			return srcCloseErr
		}
		inventory := sqliteLogicalInventory{SchemaSHA256: store.SchemaSHA256, TableRows: store.TableRows}
		fileHash, size, err := verifySQLiteSnapshotFile(ctx, item.tmp, inventory)
		if err != nil || fileHash != store.FileSHA256 || size != store.SizeBytes {
			if err == nil {
				err = errors.New("恢复文件哈希/大小不一致")
			}
			return fmt.Errorf("恢复后 %s 校验失败: %w", store.Role, err)
		}
	}
	if err := syncDirectory(targetDir); err != nil {
		return err
	}
	for _, item := range staged {
		// A hard-link publication is same-filesystem and fails if the final name
		// appeared concurrently; unlike os.Rename on Unix it cannot overwrite.
		if err := os.Link(item.tmp, item.final); err != nil {
			return fmt.Errorf("发布恢复文件 %s 失败: %w", item.store.SourceFile, err)
		}
		if err := os.Remove(item.tmp); err != nil {
			return fmt.Errorf("清理恢复暂存文件 %s 失败: %w", filepath.Base(item.tmp), err)
		}
	}
	readyPath := filepath.Join(targetDir, preMigrationRestoreReadyName)
	readyData := []byte("snapshot_manifest_sha256:" + manifestHash + "\n")
	readyFile, err := os.OpenFile(readyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := readyFile.Write(readyData)
	syncErr := readyFile.Sync()
	closeErr := readyFile.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := syncDirectory(targetDir); err != nil {
		return err
	}
	cleanup = false
	return nil
}
