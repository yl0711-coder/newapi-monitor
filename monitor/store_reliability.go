package monitor

// store_reliability.go 只保护 Monitor 自己的 SQLite：启动前完整性闸门、
// 在线一致性备份、备份校验和保留策略。它不连接、更不会写 NewAPI 数据库。

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"gorm.io/gorm"
)

const (
	storeIntegrityTimeout        = 60 * time.Second
	storeBackupTimeout           = 10 * time.Minute
	storeBackupPrefix            = "monitor-"
	usageFactsBackupPrefix       = "usage-facts-"
	storeBackupSetPrefix         = "backup-set-"
	storeBackupSuffix            = ".db"
	storeBackupSetSuffix         = ".json"
	storeBackupSetVersion        = 1
	storeBackupRestoreVersion    = 1
	storeBackupRestoreActiveName = "STORE_BACKUP_RESTORE_ACTIVATED"
	storeBackupRestoreBusyName   = "STORE_BACKUP_RESTORE_IN_PROGRESS"
	storeBackupRestoreReadyName  = "STORE_BACKUP_RESTORE_READY"
)

type storeBackupFileManifest struct {
	File   string `json:"file"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// storeBackupSetManifest is published last. Restore tooling must ignore loose
// .db files and select only a set whose manifest, hashes, quick_checks and
// cross-store member/publication signature all verify.
type storeBackupSetManifest struct {
	Version                 int                      `json:"version"`
	MigrationPlan           string                   `json:"migration_plan"`
	SetID                   string                   `json:"set_id"`
	CreatedAt               int64                    `json:"created_at"`
	Main                    *storeBackupFileManifest `json:"main,omitempty"`
	Facts                   *storeBackupFileManifest `json:"facts,omitempty"`
	ActiveMemberManifestSHA string                   `json:"active_member_manifest_sha256,omitempty"`
	PublishedSignatureSHA   string                   `json:"published_signature_sha256,omitempty"`
	PublishedFingerprint    string                   `json:"published_fingerprint,omitempty"`
	PublishedRangeStart     int64                    `json:"published_range_start,omitempty"`
	PublishedThrough        int64                    `json:"published_through,omitempty"`
	ServingGeneration       int64                    `json:"serving_generation,omitempty"`
}

// storeBackupRestoreReady is the durable activation record written after both
// restored databases and their cross-store authorization signature verify.
// Normal startup validates this record before any backup/migration/open.  The
// preceding IN_PROGRESS marker makes a crash between the two file publications
// fail closed instead of treating the missing facts database as a clean install.
type storeBackupRestoreReady struct {
	Version                 int                     `json:"version"`
	MigrationPlan           string                  `json:"migration_plan"`
	SetID                   string                  `json:"set_id"`
	ManifestSHA256          string                  `json:"manifest_sha256"`
	Main                    storeBackupFileManifest `json:"main"`
	Facts                   storeBackupFileManifest `json:"facts"`
	ActiveMemberManifestSHA string                  `json:"active_member_manifest_sha256"`
	PublishedSignatureSHA   string                  `json:"published_signature_sha256"`
	PublishedFingerprint    string                  `json:"published_fingerprint"`
	PublishedRangeStart     int64                   `json:"published_range_start"`
	PublishedThrough        int64                   `json:"published_through"`
	ServingGeneration       int64                   `json:"serving_generation"`
}

// StoreReliabilityStatus 是 Monitor 自有 SQLite 的只读运维状态。它不返回
// 数据库或备份文件路径，避免管理接口泄露宿主机目录；所有字段均来自配置或
// 内存中的原子状态，读取它不会执行 quick_check、创建备份或争用 SQLite。
type StoreReliabilityStatus struct {
	IntegrityCheckedAt  int64 `json:"integrity_checked_at"`
	IntegrityOK         bool  `json:"integrity_ok"`
	BackupEnabled       bool  `json:"backup_enabled"`
	BackupIntervalHours int   `json:"backup_interval_hours"`
	BackupRetention     int   `json:"backup_retention"`
	LastBackupSuccessAt int64 `json:"last_backup_success_at"`
	LastBackupFailureAt int64 `json:"last_backup_failure_at"`
	LastBackupBytes     int64 `json:"last_backup_bytes"`
	BackupRunning       bool  `json:"backup_running"`
	BackupSetVerified   bool  `json:"backup_set_verified"`
	BackupSetSuccessAt  int64 `json:"backup_set_success_at"`
	BackupSetFailureAt  int64 `json:"backup_set_failure_at"`
}

func (m *Monitor) storeReliabilityStatus() StoreReliabilityStatus {
	return StoreReliabilityStatus{
		IntegrityCheckedAt:  m.storeIntegrityCheckedAt.Load(),
		IntegrityOK:         m.storeIntegrityOK.Load(),
		BackupEnabled:       m.cfg.StoreBackupEnabled && m.storeDB != nil && storeUsesFile(m.cfg.StorePath),
		BackupIntervalHours: int(m.storeBackupInterval() / time.Hour),
		BackupRetention:     m.storeBackupRetention(),
		LastBackupSuccessAt: m.storeBackupLastSuccess.Load(),
		LastBackupFailureAt: m.storeBackupLastFailure.Load(),
		LastBackupBytes:     m.storeBackupBytes.Load(),
		BackupRunning:       m.storeManualBackupRunning.Load(),
		BackupSetVerified:   m.storeBackupSetVerified.Load(),
		BackupSetSuccessAt:  m.storeBackupSetLastSuccess.Load(),
		BackupSetFailureAt:  m.storeBackupSetLastFailure.Load(),
	}
}

func (m *Monitor) usageFactsStoreReliabilityStatus() StoreReliabilityStatus {
	if m.usageFactsDB == m.storeDB || (m.usageFactsDB == nil && strings.TrimSpace(m.cfg.UsageFactsStorePath) == "") {
		return m.storeReliabilityStatus()
	}
	return StoreReliabilityStatus{
		IntegrityCheckedAt:  m.usageFactsIntegrityCheckedAt.Load(),
		IntegrityOK:         m.usageFactsIntegrityOK.Load(),
		BackupEnabled:       m.cfg.StoreBackupEnabled && m.usageFactsDB != nil && storeUsesFile(m.cfg.UsageFactsStorePath),
		BackupIntervalHours: int(m.storeBackupInterval() / time.Hour),
		BackupRetention:     m.storeBackupRetention(),
		LastBackupSuccessAt: m.usageFactsBackupLastSuccess.Load(),
		LastBackupFailureAt: m.usageFactsBackupLastFailure.Load(),
		LastBackupBytes:     m.usageFactsBackupBytes.Load(),
		BackupRunning:       m.storeManualBackupRunning.Load(),
		BackupSetVerified:   m.storeBackupSetVerified.Load(),
		BackupSetSuccessAt:  m.storeBackupSetLastSuccess.Load(),
		BackupSetFailureAt:  m.storeBackupSetLastFailure.Load(),
	}
}

// triggerManualStoreBackup 为受鉴权的本地运维/验收提供一次异步一致性备份。
// 它复用自动维护的同一把锁、VACUUM INTO、quick_check 和原子改名；
// 重复点击不会排队堆积，也不会读取或写入 NewAPI 数据库。
func (m *Monitor) triggerManualStoreBackup() bool {
	mainEnabled := m.cfg.StoreBackupEnabled && m.storeDB != nil && storeUsesFile(m.cfg.StorePath)
	factsEnabled := m.cfg.StoreBackupEnabled && m.usageFactsDB != nil && m.usageFactsDB != m.storeDB && storeUsesFile(m.cfg.UsageFactsStorePath)
	if (!mainEnabled && !factsEnabled) || !m.storeManualBackupRunning.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		defer m.storeManualBackupRunning.Store(false)
		ctx := m.taskContext()
		now := time.Now()
		if _, err := m.createStoreBackupSet(ctx, now, mainEnabled, factsEnabled); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("手动 Monitor 双库成套备份失败", "err", err)
		}
	}()
	return true
}

func storeUsesFile(path string) bool {
	path = strings.TrimSpace(path)
	return path != "" && path != ":memory:" && !strings.Contains(path, "mode=memory") && path != "file::memory:"
}

func sqliteReadOnlyDSN(path string) string {
	u := &url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_pragma", "busy_timeout(5000)")
	u.RawQuery = q.Encode()
	return u.String()
}

// sqliteQuickCheck 必须读完 PRAGMA 返回的所有行；只有唯一的 ok 才算通过。
func sqliteQuickCheck(ctx context.Context, db *sql.DB) error {
	return sqliteQuickCheckQuery(ctx, db)
}

// preflightStoreIntegrity 对已存在文件使用只读连接，确保任何迁移前先挡住损坏库。
// 返回 false 表示文件尚不存在，由 openStore 在创建连接后再检查。
func preflightStoreIntegrity(path string) (bool, error) {
	if !storeUsesFile(path) {
		return false, nil
	}
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
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return false, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), storeIntegrityTimeout)
	defer cancel()
	return true, sqliteQuickCheck(ctx, db)
}

func checkGORMStoreIntegrity(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), storeIntegrityTimeout)
	defer cancel()
	return sqliteQuickCheck(ctx, sqlDB)
}

func (m *Monitor) storeBackupInterval() time.Duration {
	hours := m.cfg.StoreBackupIntervalHours
	if hours <= 0 {
		hours = 24
	}
	if hours > 24*31 {
		hours = 24 * 31
	}
	return time.Duration(hours) * time.Hour
}

func (m *Monitor) storeBackupRetention() int {
	retention := m.cfg.StoreBackupRetention
	if retention <= 0 {
		retention = 7
	}
	if retention > 365 {
		retention = 365
	}
	return retention
}

func (m *Monitor) storeBackupDir() string {
	if dir := strings.TrimSpace(m.cfg.StoreBackupDir); dir != "" {
		return dir
	}
	return filepath.Join(filepath.Dir(m.cfg.StorePath), "backups")
}

// startStoreMaintenance 延迟首次备份，避开启动迁移和第一轮同步；之后每次只在
// 上一次任务结束后计时，不会因慢盘产生重叠备份。
func (m *Monitor) startStoreMaintenance(ctx context.Context) {
	mainEnabled := m.storeDB != nil && storeUsesFile(m.cfg.StorePath)
	factsEnabled := m.usageFactsDB != nil && m.usageFactsDB != m.storeDB && storeUsesFile(m.cfg.UsageFactsStorePath)
	if !m.cfg.StoreBackupEnabled || (!mainEnabled && !factsEnabled) {
		return
	}
	m.storeMaintenanceOnce.Do(func() {
		go func() {
			initial := time.NewTimer(5 * time.Minute)
			defer initial.Stop()
			select {
			case <-ctx.Done():
				return
			case <-initial.C:
			}
			for {
				now := time.Now()
				if _, err := m.createStoreBackupSet(ctx, now, mainEnabled, factsEnabled); err != nil {
					if !errors.Is(err, context.Canceled) {
						slog.Error("Monitor 双库成套在线备份失败", "err", err)
					}
				} else {
					slog.Info("Monitor 双库成套在线备份完成")
				}
				timer := time.NewTimer(m.storeBackupInterval())
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	})
}

func storeBackupFile(path string) (*storeBackupFileManifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	bytes, copyErr := io.Copy(h, file)
	closeErr := file.Close()
	if copyErr != nil {
		return nil, copyErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return &storeBackupFileManifest{File: filepath.Base(path), Bytes: bytes, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func storeBackupSourceBytes(paths ...string) (int64, error) {
	var total int64
	seen := make(map[string]bool)
	for _, raw := range paths {
		path := normalizedStorePath(raw)
		if path == "" || seen[path] || !storeUsesFile(path) {
			continue
		}
		seen[path] = true
		for _, candidate := range []string{path, path + "-wal"} {
			info, err := os.Stat(candidate)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return 0, err
			}
			if !info.Mode().IsRegular() {
				return 0, fmt.Errorf("备份源不是普通文件: %s", candidate)
			}
			total += info.Size()
		}
	}
	return total, nil
}

func preflightStoreBackupSpace(dir string, paths ...string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	sourceBytes, err := storeBackupSourceBytes(paths...)
	if err != nil {
		return err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return err
	}
	free := int64(fs.Bavail * uint64(fs.Bsize))
	reserve := int64(2 * 1024 * 1024 * 1024)
	if proportional := sourceBytes / 5; proportional > reserve {
		reserve = proportional
	}
	required := sourceBytes + reserve
	if sourceBytes <= 0 || free < required {
		return fmt.Errorf("备份目标空间不足: source_and_wal=%d free=%d required=%d", sourceBytes, free, required)
	}
	return nil
}

func hashStoreMemberManifest(ctx context.Context, path string) (string, map[int64]int64, error) {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return "", nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT user_id,active,tracked_revision,current_group_id
 FROM usage_member_controls ORDER BY user_id`)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	h := sha256.New()
	active := make(map[int64]int64)
	for rows.Next() {
		var id, revision, groupID int64
		var enabled bool
		if err := rows.Scan(&id, &enabled, &revision, &groupID); err != nil {
			return "", nil, err
		}
		_, _ = fmt.Fprintf(h, "%d|%t|%d|%d\n", id, enabled, revision, groupID)
		if enabled {
			active[id] = revision
		}
	}
	if err := rows.Err(); err != nil {
		return "", nil, err
	}
	return hex.EncodeToString(h.Sum(nil)), active, nil
}

func verifyStoreBackupPublication(
	ctx context.Context,
	path string,
	active map[int64]int64,
	manifest *storeBackupSetManifest,
) error {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT user_id,tracked_revision,source_epoch,classification_version,
 query_semantics_version,source_floor_hour,verified_through_hour
 FROM usage_fact_published_members ORDER BY user_id`)
	if err != nil {
		return err
	}
	h := sha256.New()
	ids := make([]int64, 0)
	for rows.Next() {
		var id, revision, classification, semantics, floor, verified int64
		var epoch string
		if err := rows.Scan(&id, &revision, &epoch, &classification, &semantics, &floor, &verified); err != nil {
			_ = rows.Close()
			return err
		}
		mainRevision, activeMember := active[id]
		if !activeMember || !storeBackupPublicationRevisionCompatible(mainRevision, revision) {
			_ = rows.Close()
			return fmt.Errorf("备份发布成员与主库权限版本不一致: user_id=%d facts_revision=%d main_revision=%d",
				id, revision, mainRevision)
		}
		ids = append(ids, id)
		_, _ = fmt.Fprintf(h, "%d|%d|%s|%d|%d|%d|%d\n", id, revision, epoch, classification, semantics, floor, verified)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	manifest.PublishedSignatureSHA = hex.EncodeToString(h.Sum(nil))
	var stateFingerprint string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(published_fingerprint,''),published_range_start,
 published_through,serving_generation FROM usage_fact_sync_states WHERE id=1`).Scan(
		&stateFingerprint, &manifest.PublishedRangeStart, &manifest.PublishedThrough, &manifest.ServingGeneration,
	); err != nil {
		return err
	}
	wantFingerprint := ""
	if len(ids) > 0 {
		wantFingerprint = portalMemberFingerprintFromIDs(ids)
	}
	if stateFingerprint != wantFingerprint {
		return fmt.Errorf("备份 facts 发布指纹不一致: state=%q rows=%q", stateFingerprint, wantFingerprint)
	}
	manifest.PublishedFingerprint = stateFingerprint
	return nil
}

// storeBackupPublicationRevisionCompatible mirrors the serving layer's one
// explicit legacy transition. Before tracked revisions were persisted in the
// facts store, an active member was published as revision 0; the main-store
// migration initializes that same member at revision 1. That pair represents
// the same first lifecycle and is safe to back up together. Every later
// revision mismatch still means a remove/rejoin hand-off is incomplete and
// must fail closed.
func storeBackupPublicationRevisionCompatible(mainRevision, factsRevision int64) bool {
	return mainRevision == factsRevision || (mainRevision == 1 && factsRevision == 0)
}

func writeStoreBackupSetManifest(dir string, manifest storeBackupSetManifest) (string, error) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	finalPath := filepath.Join(dir, storeBackupSetPrefix+manifest.SetID+storeBackupSetSuffix)
	tmpPath := finalPath + fmt.Sprintf(".tmp-%d", time.Now().UnixNano())
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
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
		return "", writeErr
	}
	if syncErr != nil {
		return "", syncErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return "", err
	}
	cleanup = false
	if err := syncDirectory(dir); err != nil {
		return "", err
	}
	return finalPath, nil
}

func verifyStoreBackupSetFile(ctx context.Context, dir string, expected *storeBackupFileManifest) (string, error) {
	if expected == nil || expected.File == "" || filepath.Base(expected.File) != expected.File {
		return "", errors.New("备份集文件名无效")
	}
	path := filepath.Join(dir, expected.File)
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() != expected.Bytes || expected.Bytes <= 0 {
		return "", fmt.Errorf("备份集文件类型/大小不一致: %s", expected.File)
	}
	actual, err := storeBackupFile(path)
	if err != nil {
		return "", err
	}
	if actual.SHA256 != expected.SHA256 {
		return "", fmt.Errorf("备份集文件 hash 不一致: %s", expected.File)
	}
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return "", err
	}
	checkErr := sqliteQuickCheck(ctx, db)
	closeErr := db.Close()
	if checkErr != nil {
		return "", checkErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return path, nil
}

// verifyStoreBackupSetManifest is the restore-side gate. It performs no
// writes: a new-volume restore may call it before copying either database.
func verifyStoreBackupSetManifest(ctx context.Context, manifestPath string) (storeBackupSetManifest, error) {
	var manifest storeBackupSetManifest
	info, err := os.Lstat(manifestPath)
	if err != nil {
		return manifest, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 {
		return manifest, errors.New("备份集 manifest 不是可接受的普通文件")
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, err
	}
	// Version is the restore-format compatibility contract. MigrationPlan is
	// provenance: a newer image must be able to restore a verified backup from
	// the immediately preceding schema plan and then run its normal adjacent
	// pre-migration snapshot/AutoMigrate gate. Treating the current plan ID as
	// the format version would make all existing disaster-recovery sets unusable
	// at the exact moment an upgrade is deployed.
	if manifest.Version != storeBackupSetVersion || strings.TrimSpace(manifest.MigrationPlan) == "" || manifest.SetID == "" ||
		filepath.Base(manifestPath) != storeBackupSetPrefix+manifest.SetID+storeBackupSetSuffix ||
		(manifest.Main == nil && manifest.Facts == nil) {
		return manifest, errors.New("备份集 manifest 版本/标识无效")
	}
	dir := filepath.Dir(manifestPath)
	var mainPath, factsPath string
	if manifest.Main != nil {
		mainPath, err = verifyStoreBackupSetFile(ctx, dir, manifest.Main)
		if err != nil {
			return manifest, err
		}
	}
	if manifest.Facts != nil {
		factsPath, err = verifyStoreBackupSetFile(ctx, dir, manifest.Facts)
		if err != nil {
			return manifest, err
		}
	}
	if mainPath != "" && factsPath != "" {
		memberHash, active, err := hashStoreMemberManifest(ctx, mainPath)
		if err != nil {
			return manifest, err
		}
		observed := storeBackupSetManifest{}
		if err := verifyStoreBackupPublication(ctx, factsPath, active, &observed); err != nil {
			return manifest, err
		}
		if memberHash != manifest.ActiveMemberManifestSHA || observed.PublishedSignatureSHA != manifest.PublishedSignatureSHA ||
			observed.PublishedFingerprint != manifest.PublishedFingerprint ||
			observed.PublishedRangeStart != manifest.PublishedRangeStart || observed.PublishedThrough != manifest.PublishedThrough ||
			observed.ServingGeneration != manifest.ServingGeneration {
			return manifest, errors.New("备份集权限/发布签名与 manifest 不一致")
		}
	}
	return manifest, nil
}

func validateStoreRestoreLeafName(name, field string) error {
	if name == "" || name == "." || name == ".." || filepath.IsAbs(name) || filepath.Base(name) != name ||
		strings.ContainsAny(name, `/\\`) {
		return fmt.Errorf("%s 必须是安全的单层文件名", field)
	}
	for _, reserved := range []string{storeBackupRestoreBusyName, storeBackupRestoreReadyName, storeBackupRestoreActiveName} {
		if name == reserved || "."+name+".restore-tmp" == reserved {
			return fmt.Errorf("%s 不得使用恢复协议保留名", field)
		}
	}
	return nil
}

func loadStoreRestoreReadyRecord(readyPath string) (storeBackupRestoreReady, error) {
	var ready storeBackupRestoreReady
	info, err := os.Lstat(readyPath)
	if err != nil {
		return ready, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 {
		return ready, errors.New("恢复 READY 不是可接受的普通文件")
	}
	data, err := os.ReadFile(readyPath)
	if err != nil {
		return ready, err
	}
	if err := json.Unmarshal(data, &ready); err != nil {
		return ready, err
	}
	if ready.Version != storeBackupRestoreVersion || ready.MigrationPlan == "" || ready.SetID == "" || ready.ManifestSHA256 == "" {
		return ready, errors.New("恢复 READY 版本/计划/标识无效")
	}
	return ready, nil
}

func loadAndVerifyStoreRestoreReady(ctx context.Context, readyPath, mainPath, factsPath string) error {
	ready, err := loadStoreRestoreReadyRecord(readyPath)
	if err != nil {
		return err
	}
	if ready.Main.File != filepath.Base(mainPath) || ready.Facts.File != filepath.Base(factsPath) {
		return errors.New("恢复 READY 文件名与启动配置不匹配")
	}
	dir := filepath.Dir(mainPath)
	verifiedMain, err := verifyStoreBackupSetFile(ctx, dir, &ready.Main)
	if err != nil {
		return fmt.Errorf("恢复主库启动校验失败: %w", err)
	}
	verifiedFacts, err := verifyStoreBackupSetFile(ctx, dir, &ready.Facts)
	if err != nil {
		return fmt.Errorf("恢复 facts 启动校验失败: %w", err)
	}
	memberHash, active, err := hashStoreMemberManifest(ctx, verifiedMain)
	if err != nil {
		return err
	}
	observed := storeBackupSetManifest{}
	if err := verifyStoreBackupPublication(ctx, verifiedFacts, active, &observed); err != nil {
		return err
	}
	if memberHash != ready.ActiveMemberManifestSHA || observed.PublishedSignatureSHA != ready.PublishedSignatureSHA ||
		observed.PublishedFingerprint != ready.PublishedFingerprint || observed.PublishedRangeStart != ready.PublishedRangeStart ||
		observed.PublishedThrough != ready.PublishedThrough || observed.ServingGeneration != ready.ServingGeneration {
		return errors.New("恢复后权限/发布签名与 READY 不一致")
	}
	return nil
}

// preflightStoreRestoreActivation runs before createPreMigrationSnapshot and
// every AutoMigrate. A complete restore is re-verified, then READY is atomically
// renamed to the permanent ACTIVATED audit record before ordinary DB writes.
func (m *Monitor) preflightStoreRestoreActivation(parent context.Context, mainPath, factsPath string) error {
	mainFile, factsFile := storeUsesFile(mainPath), storeUsesFile(factsPath)
	dirs := make([]string, 0, 2)
	if mainFile {
		dirs = append(dirs, filepath.Clean(filepath.Dir(mainPath)))
	}
	if factsFile {
		factsDir := filepath.Clean(filepath.Dir(factsPath))
		if len(dirs) == 0 || factsDir != dirs[0] {
			dirs = append(dirs, factsDir)
		}
	}
	markerDir := ""
	for _, dir := range dirs {
		for _, name := range []string{storeBackupRestoreBusyName, storeBackupRestoreReadyName, storeBackupRestoreActiveName} {
			if _, err := os.Lstat(filepath.Join(dir, name)); err == nil {
				if markerDir != "" && markerDir != dir {
					return errors.New("多个目录同时存在恢复协议标记，拒绝选择数据库")
				}
				markerDir = dir
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if markerDir == "" {
		return nil
	}
	if !mainFile || !factsFile || sameStorePath(mainPath, factsPath) ||
		filepath.Clean(filepath.Dir(mainPath)) != filepath.Clean(filepath.Dir(factsPath)) ||
		markerDir != filepath.Clean(filepath.Dir(mainPath)) {
		return errors.New("检测到恢复协议标记，但 main/facts 启动路径不是同目录的两个独立文件")
	}
	mainDir := markerDir
	busyPath := filepath.Join(mainDir, storeBackupRestoreBusyName)
	readyPath := filepath.Join(mainDir, storeBackupRestoreReadyName)
	activePath := filepath.Join(mainDir, storeBackupRestoreActiveName)
	if activeInfo, activeErr := os.Lstat(activePath); activeErr == nil {
		if !activeInfo.Mode().IsRegular() {
			return errors.New("恢复 ACTIVATED 不是普通文件")
		}
		// ACTIVATED is a permanent origin audit record, not a schema-version
		// lock. Future images legitimately advance preMigrationPlanID and run
		// their own adjacent snapshot/migration gate after this preflight.
		activeRecord, err := loadStoreRestoreReadyRecord(activePath)
		if err != nil {
			return fmt.Errorf("恢复 ACTIVATED 审计记录无效: %w", err)
		}
		if activeRecord.Main.File != filepath.Base(mainPath) || activeRecord.Facts.File != filepath.Base(factsPath) {
			return errors.New("恢复 ACTIVATED 记录与 main/facts 启动文件名不匹配")
		}
		for _, path := range []string{mainPath, factsPath} {
			info, err := os.Lstat(path)
			if err != nil {
				return fmt.Errorf("已激活恢复卷缺少成套数据库 %s: %w", filepath.Base(path), err)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("已激活恢复卷数据库 %s 不是普通文件", filepath.Base(path))
			}
		}
		// ACTIVATED is the atomic hand-off point. A crash after that rename may
		// leave the older markers, but must not turn an already verified pair
		// back into an incomplete restore. Writable startup removes only those
		// stale markers; ACTIVATED remains as the durable audit trail.
		for _, stale := range []string{readyPath, busyPath} {
			if err := os.Remove(stale); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		return syncDirectory(mainDir)
	} else if !errors.Is(activeErr, os.ErrNotExist) {
		return activeErr
	}
	_, busyErr := os.Lstat(busyPath)
	_, readyErr := os.Lstat(readyPath)
	busyExists := busyErr == nil
	readyExists := readyErr == nil
	if busyErr != nil && !errors.Is(busyErr, os.ErrNotExist) {
		return busyErr
	}
	if readyErr != nil && !errors.Is(readyErr, os.ErrNotExist) {
		return readyErr
	}
	if !busyExists && !readyExists {
		return nil
	}
	if !readyExists {
		return errors.New("检测到未完成的 main/facts 新卷恢复；缺少 READY，拒绝启动或创建空 facts 库")
	}
	// First activation hashes and quick-checks both complete databases. A
	// multi-GiB facts store cannot safely share the ordinary 60-second probe;
	// use the same bounded maintenance budget as backup/restore.
	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	if err := loadAndVerifyStoreRestoreReady(ctx, readyPath, mainPath, factsPath); err != nil {
		return fmt.Errorf("恢复卷启动门禁失败: %w", err)
	}
	// Rename is the single atomic activation point. The JSON content is kept as
	// an audit record, while later legitimate application writes no longer need
	// to match the immutable restore hashes on every restart.
	if err := os.Rename(readyPath, activePath); err != nil {
		return fmt.Errorf("激活恢复 READY 失败: %w", err)
	}
	if busyExists {
		if err := os.Remove(busyPath); err != nil {
			return fmt.Errorf("消费恢复 IN_PROGRESS 失败: %w", err)
		}
	}
	return syncDirectory(mainDir)
}

// RestoreStoreBackupSet restores one verified runtime main+facts backup set
// into a brand-new empty directory/volume.  It never opens a production DSN,
// never overwrites an existing file, stages both databases under hidden names,
// re-runs hash/quick_check and the cross-store member/publication signature,
// then writes READY last.  A crash before READY is therefore not startable.
func RestoreStoreBackupSet(parent context.Context, manifestPath, targetDir, mainName, factsName string) error {
	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	if err := validateStoreRestoreLeafName(mainName, "main-name"); err != nil {
		return err
	}
	if err := validateStoreRestoreLeafName(factsName, "facts-name"); err != nil {
		return err
	}
	if mainName == factsName {
		return errors.New("main-name 与 facts-name 不得相同")
	}
	manifest, err := verifyStoreBackupSetManifest(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("备份集校验失败: %w", err)
	}
	if manifest.Main == nil || manifest.Facts == nil {
		return errors.New("运行期灾备恢复要求 main+facts 成套 manifest")
	}
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	manifestDigest := sha256.Sum256(manifestData)
	manifestHash := hex.EncodeToString(manifestDigest[:])

	info, err := os.Lstat(targetDir)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(targetDir, 0o700); err != nil {
			return fmt.Errorf("创建恢复目录失败: %w", err)
		}
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
	type restoreStore struct {
		role       string
		expected   *storeBackupFileManifest
		tmp        string
		final      string
		tmpOwned   bool
		finalOwned bool
		tmpInfo    os.FileInfo
		finalInfo  os.FileInfo
	}
	stores := []restoreStore{
		{role: "main", expected: manifest.Main, tmp: filepath.Join(targetDir, "."+mainName+".restore-tmp"), final: filepath.Join(targetDir, mainName)},
		{role: "facts", expected: manifest.Facts, tmp: filepath.Join(targetDir, "."+factsName+".restore-tmp"), final: filepath.Join(targetDir, factsName)},
	}
	busyPath := filepath.Join(targetDir, storeBackupRestoreBusyName)
	readyPath := filepath.Join(targetDir, storeBackupRestoreReadyName)
	cleanup := true
	busy, err := os.OpenFile(busyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	readyOwned := false
	var readyInfo os.FileInfo
	removeOwned := func(path string, owned bool, expected os.FileInfo) bool {
		if !owned {
			return true
		}
		actual, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return true
		}
		if err != nil || expected == nil || !os.SameFile(actual, expected) {
			return false
		}
		return os.Remove(path) == nil
	}
	// Cleanup is registered only after this call owns the O_EXCL marker. A
	// losing concurrent restore must never remove the winner's safety fence.
	defer func() {
		if !cleanup {
			return
		}
		artifactsGone := true
		for i := range stores {
			store := &stores[i]
			if !removeOwned(store.tmp, store.tmpOwned, store.tmpInfo) {
				artifactsGone = false
			}
			if !removeOwned(store.final, store.finalOwned, store.finalInfo) {
				artifactsGone = false
			}
		}
		if !removeOwned(readyPath, readyOwned, readyInfo) {
			artifactsGone = false
		}
		// A failed restore deliberately keeps its owned IN_PROGRESS fence even
		// after best-effort artifact cleanup. This is stricter than trying to
		// infer that a sequence of removes and directory fsyncs was fully durable:
		// the operator must discard/recreate the failed target volume, and normal
		// startup can never mistake it for an empty store that is safe to migrate.
		_ = artifactsGone
		_ = syncDirectory(targetDir)
	}()
	_, writeErr := fmt.Fprintf(busy, "set_id:%s\nmanifest_sha256:%s\n", manifest.SetID, manifestHash)
	syncErr := busy.Sync()
	closeErr := busy.Close()
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
	lockedEntries, err := os.ReadDir(targetDir)
	if err != nil {
		return err
	}
	if len(lockedEntries) != 1 || lockedEntries[0].Name() != filepath.Base(busyPath) {
		return errors.New("恢复目标在取得 IN_PROGRESS 锁时已被其他进程修改")
	}

	sourceDir := filepath.Dir(manifestPath)
	for i := range stores {
		store := &stores[i]
		src, err := os.Open(filepath.Join(sourceDir, store.expected.File))
		if err != nil {
			return err
		}
		dst, err := os.OpenFile(store.tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			_ = src.Close()
			return err
		}
		store.tmpOwned = true
		store.tmpInfo, err = dst.Stat()
		if err != nil {
			_ = dst.Close()
			_ = src.Close()
			return err
		}
		_, copyErr := io.Copy(dst, src)
		syncErr := dst.Sync()
		dstCloseErr := dst.Close()
		srcCloseErr := src.Close()
		if copyErr != nil {
			return fmt.Errorf("复制 %s 备份失败: %w", store.role, copyErr)
		}
		if syncErr != nil {
			return fmt.Errorf("恢复 %s 备份落盘失败: %w", store.role, syncErr)
		}
		if dstCloseErr != nil {
			return dstCloseErr
		}
		if srcCloseErr != nil {
			return srcCloseErr
		}
		tmpExpected := *store.expected
		tmpExpected.File = filepath.Base(store.tmp)
		if _, err := verifyStoreBackupSetFile(ctx, targetDir, &tmpExpected); err != nil {
			return fmt.Errorf("恢复后 %s 校验失败: %w", store.role, err)
		}
	}
	if err := syncDirectory(targetDir); err != nil {
		return err
	}
	for i := range stores {
		store := &stores[i]
		if err := os.Link(store.tmp, store.final); err != nil {
			return fmt.Errorf("发布恢复文件 %s 失败: %w", filepath.Base(store.final), err)
		}
		store.finalOwned = true
		store.finalInfo = store.tmpInfo // hard link: identical inode
		if err := os.Remove(store.tmp); err != nil {
			return err
		}
		store.tmpOwned = false
	}

	memberHash, active, err := hashStoreMemberManifest(ctx, stores[0].final)
	if err != nil {
		return err
	}
	observed := storeBackupSetManifest{}
	if err := verifyStoreBackupPublication(ctx, stores[1].final, active, &observed); err != nil {
		return err
	}
	if memberHash != manifest.ActiveMemberManifestSHA || observed.PublishedSignatureSHA != manifest.PublishedSignatureSHA ||
		observed.PublishedFingerprint != manifest.PublishedFingerprint || observed.PublishedRangeStart != manifest.PublishedRangeStart ||
		observed.PublishedThrough != manifest.PublishedThrough || observed.ServingGeneration != manifest.ServingGeneration {
		return errors.New("恢复后权限/发布签名与备份集 manifest 不一致")
	}

	restoredMain := storeBackupFileManifest{File: filepath.Base(stores[0].final), Bytes: manifest.Main.Bytes, SHA256: manifest.Main.SHA256}
	restoredFacts := storeBackupFileManifest{File: filepath.Base(stores[1].final), Bytes: manifest.Facts.Bytes, SHA256: manifest.Facts.SHA256}
	readyRecord := storeBackupRestoreReady{
		Version: storeBackupRestoreVersion, MigrationPlan: manifest.MigrationPlan, SetID: manifest.SetID,
		ManifestSHA256: manifestHash, Main: restoredMain, Facts: restoredFacts,
		ActiveMemberManifestSHA: manifest.ActiveMemberManifestSHA, PublishedSignatureSHA: manifest.PublishedSignatureSHA,
		PublishedFingerprint: manifest.PublishedFingerprint, PublishedRangeStart: manifest.PublishedRangeStart,
		PublishedThrough: manifest.PublishedThrough, ServingGeneration: manifest.ServingGeneration,
	}
	readyData, err := json.MarshalIndent(readyRecord, "", "  ")
	if err != nil {
		return err
	}
	readyData = append(readyData, '\n')
	ready, err := os.OpenFile(readyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	readyOwned = true
	readyInfo, err = ready.Stat()
	if err != nil {
		_ = ready.Close()
		return err
	}
	_, writeErr = ready.Write(readyData)
	syncErr = ready.Sync()
	closeErr = ready.Close()
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
	// READY plus both verified databases is the durable commit point. From here
	// onward a cleanup error must leave that complete set intact for startup
	// preflight to verify/activate; never roll it back to a marker-less half set.
	cleanup = false
	if err := os.Remove(busyPath); err != nil {
		return err
	}
	if err := syncDirectory(targetDir); err != nil {
		return err
	}
	return nil
}

func pruneStoreBackupSets(dir string, retention int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	manifests := make([]string, 0)
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), storeBackupSetPrefix) && strings.HasSuffix(entry.Name(), storeBackupSetSuffix) {
			manifests = append(manifests, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(manifests)
	for len(manifests) > retention {
		path := manifests[0]
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var manifest storeBackupSetManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return fmt.Errorf("旧备份集 manifest 损坏 %s: %w", filepath.Base(path), err)
		}
		for _, file := range []*storeBackupFileManifest{manifest.Main, manifest.Facts} {
			if file == nil || filepath.Base(file.File) != file.File {
				continue
			}
			if err := os.Remove(filepath.Join(dir, file.File)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		manifests = manifests[1:]
	}
	return nil
}

// createStoreBackupSet holds the member-lifecycle and facts-publication
// barriers across both VACUUM INTO operations. This is intentionally a single
// low-frequency operation: it trades Tail progress during the snapshot for a
// restorable main+facts pair, while all read endpoints continue using WAL.
func (m *Monitor) createStoreBackupSet(parent context.Context, now time.Time, mainEnabled, factsEnabled bool) (string, error) {
	if !mainEnabled && !factsEnabled {
		return "", errors.New("没有可备份的 SQLite 文件")
	}
	m.storeBackupMu.Lock()
	defer m.storeBackupMu.Unlock()
	dir := m.storeBackupDir()
	paths := make([]string, 0, 2)
	if mainEnabled {
		paths = append(paths, m.cfg.StorePath)
	}
	if factsEnabled {
		paths = append(paths, m.cfg.UsageFactsStorePath)
	}
	if err := preflightStoreBackupSpace(dir, paths...); err != nil {
		failedAt := time.Now().Unix()
		m.storeBackupSetLastFailure.Store(failedAt)
		m.storeBackupSetVerified.Store(false)
		return "", err
	}
	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	setID := now.UTC().Format("20060102-150405.000000000")
	manifest := storeBackupSetManifest{
		Version: storeBackupSetVersion, MigrationPlan: m.currentMigrationPlanID(), SetID: setID, CreatedAt: now.Unix(),
	}
	var mainSuccess, mainFailure, mainBytes atomic.Int64
	var factsSuccess, factsFailure, factsBytes atomic.Int64
	var created []string
	cleanup := func() {
		for _, path := range created {
			_ = os.Remove(path)
		}
	}
	fail := func(err error) (string, error) {
		cleanup()
		failedAt := time.Now().Unix()
		m.storeBackupSetLastFailure.Store(failedAt)
		m.storeBackupSetVerified.Store(false)
		if mainEnabled {
			m.storeBackupLastFailure.Store(failedAt)
		}
		if factsEnabled {
			m.usageFactsBackupLastFailure.Store(failedAt)
		}
		return "", err
	}

	usageMemberMutationMu.Lock()
	m.usageFactsSyncMu.Lock()
	defer usageMemberMutationMu.Unlock()
	defer m.usageFactsSyncMu.Unlock()
	var mainPath, factsPath string
	var err error
	if mainEnabled {
		mainPath, err = m.createSQLiteBackup(ctx, now, m.storeDB, storeBackupPrefix,
			&mainSuccess, &mainFailure, &mainBytes, false)
		if err != nil {
			return fail(err)
		}
		created = append(created, mainPath)
		manifest.Main, err = storeBackupFile(mainPath)
		if err != nil {
			return fail(err)
		}
	}
	if factsEnabled {
		factsPath, err = m.createSQLiteBackup(ctx, now, m.usageFactsDB, usageFactsBackupPrefix,
			&factsSuccess, &factsFailure, &factsBytes, false)
		if err != nil {
			return fail(err)
		}
		created = append(created, factsPath)
		manifest.Facts, err = storeBackupFile(factsPath)
		if err != nil {
			return fail(err)
		}
	}
	if mainEnabled && factsEnabled {
		var active map[int64]int64
		manifest.ActiveMemberManifestSHA, active, err = hashStoreMemberManifest(ctx, mainPath)
		if err != nil {
			return fail(err)
		}
		if err := verifyStoreBackupPublication(ctx, factsPath, active, &manifest); err != nil {
			return fail(err)
		}
	}
	manifestPath, err := writeStoreBackupSetManifest(dir, manifest)
	if err != nil {
		return fail(err)
	}
	created = append(created, manifestPath)
	if _, err := verifyStoreBackupSetManifest(ctx, manifestPath); err != nil {
		return fail(fmt.Errorf("备份集发布后校验失败: %w", err))
	}
	created = nil // manifest now owns the pair
	successAt := time.Now().Unix()
	if mainEnabled {
		m.storeBackupLastSuccess.Store(successAt)
		m.storeBackupBytes.Store(mainBytes.Load())
	}
	if factsEnabled {
		m.usageFactsBackupLastSuccess.Store(successAt)
		m.usageFactsBackupBytes.Store(factsBytes.Load())
	}
	m.storeBackupSetLastSuccess.Store(successAt)
	m.storeBackupSetVerified.Store(true)
	if err := pruneStoreBackupSets(dir, m.storeBackupRetention()); err != nil {
		m.storeBackupSetLastFailure.Store(time.Now().Unix())
		return manifestPath, err
	}
	return manifestPath, nil
}

// createStoreBackup 使用 VACUUM INTO 生成 SQLite 自洽快照。临时文件通过
// quick_check 后才原子改名为正式备份；任一步失败都不会触碰运行库或旧备份。
func (m *Monitor) createStoreBackup(parent context.Context, now time.Time) (string, error) {
	m.storeBackupMu.Lock()
	defer m.storeBackupMu.Unlock()
	return m.createSQLiteBackup(parent, now, m.storeDB, storeBackupPrefix,
		&m.storeBackupLastSuccess, &m.storeBackupLastFailure, &m.storeBackupBytes, true)
}

func (m *Monitor) createUsageFactsBackup(parent context.Context, now time.Time) (string, error) {
	if m.usageFactsDB == nil || m.usageFactsDB == m.storeDB {
		return m.createStoreBackup(parent, now)
	}
	m.storeBackupMu.Lock()
	defer m.storeBackupMu.Unlock()
	return m.createSQLiteBackup(parent, now, m.usageFactsDB, usageFactsBackupPrefix,
		&m.usageFactsBackupLastSuccess, &m.usageFactsBackupLastFailure, &m.usageFactsBackupBytes, true)
}

func (m *Monitor) createSQLiteBackup(
	parent context.Context,
	now time.Time,
	db *gorm.DB,
	prefix string,
	lastSuccess, lastFailure, lastBytes *atomic.Int64,
	prune bool,
) (string, error) {
	if db == nil {
		return "", errors.New("SQLite 未初始化")
	}
	if m.usageFactsFullHistoryMode() {
		if ok, err := m.usageFactHistoryCapacityOK(); !ok {
			lastFailure.Store(time.Now().Unix())
			return "", fmt.Errorf("磁盘水位禁止创建全量 SQLite 备份: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	dir := m.storeBackupDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		lastFailure.Store(time.Now().Unix())
		return "", fmt.Errorf("创建备份目录失败: %w", err)
	}
	stamp := now.UTC().Format("20060102-150405.000000000")
	finalPath := filepath.Join(dir, prefix+stamp+storeBackupSuffix)
	tmpPath := filepath.Join(dir, "."+prefix+stamp+".tmp")
	_ = os.Remove(tmpPath)
	defer os.Remove(tmpPath)
	sqlDB, err := db.DB()
	if err != nil {
		lastFailure.Store(time.Now().Unix())
		return "", err
	}
	escaped := strings.ReplaceAll(tmpPath, "'", "''")
	if _, err := sqlDB.ExecContext(ctx, "VACUUM INTO '"+escaped+"'"); err != nil {
		lastFailure.Store(time.Now().Unix())
		return "", fmt.Errorf("创建 SQLite 一致性快照失败: %w", err)
	}
	backupDB, err := sql.Open("sqlite", sqliteReadOnlyDSN(tmpPath))
	if err != nil {
		lastFailure.Store(time.Now().Unix())
		return "", err
	}
	verifyErr := sqliteQuickCheck(ctx, backupDB)
	closeErr := backupDB.Close()
	if verifyErr != nil {
		lastFailure.Store(time.Now().Unix())
		return "", fmt.Errorf("备份完整性校验失败: %w", verifyErr)
	}
	if closeErr != nil {
		lastFailure.Store(time.Now().Unix())
		return "", fmt.Errorf("关闭备份校验连接失败: %w", closeErr)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		lastFailure.Store(time.Now().Unix())
		return "", err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		lastFailure.Store(time.Now().Unix())
		return "", fmt.Errorf("发布备份失败: %w", err)
	}
	info, err := os.Stat(finalPath)
	if err != nil {
		lastFailure.Store(time.Now().Unix())
		return "", err
	}
	// 快照已经原子发布且通过完整性校验，即使后续清理旧文件失败，本次
	// 备份仍然有效。先记录成功，再把清理失败单独标为运维异常。
	lastBytes.Store(info.Size())
	lastSuccess.Store(time.Now().Unix())
	if prune {
		if err := pruneSQLiteBackups(dir, prefix, m.storeBackupRetention()); err != nil {
			lastFailure.Store(time.Now().Unix())
			return finalPath, err
		}
	}
	return finalPath, nil
}

func pruneStoreBackups(dir string, retention int) error {
	return pruneSQLiteBackups(dir, storeBackupPrefix, retention)
}

func pruneSQLiteBackups(dir, prefix string, retention int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() && strings.HasPrefix(name, prefix) && strings.HasSuffix(name, storeBackupSuffix) {
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	sort.Strings(paths)
	for len(paths) > retention {
		if err := os.Remove(paths[0]); err != nil {
			return fmt.Errorf("清理旧备份 %s 失败: %w", filepath.Base(paths[0]), err)
		}
		paths = paths[1:]
	}
	return nil
}
