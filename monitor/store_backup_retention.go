package monitor

// Only runtime backup-set manifests participate. Release cold backups,
// migration snapshots, repair evidence and loose legacy files are NOT ours.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const backupSetStampLayout = "20060102-150405.000000000"

// Bind reuse to the same mounted source files, not merely similar names or
// schema versions. Restores/replaced volumes and pre-feature manifests force
// a fresh snapshot. Only a digest is persisted; no credentials are involved.
func runtimeBackupSourceIdentity(paths []string) (string, error) {
	h := sha256.New()
	for _, path := range paths {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return "", err
		}
		resolved, err = filepath.Abs(resolved)
		if err != nil {
			return "", err
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return "", err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() {
			return "", fmt.Errorf("无法识别备份源文件")
		}
		fmt.Fprintf(h, "%q:%d:%d\n", resolved, stat.Dev, stat.Ino)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (m *Monitor) storeBackupResumeDelay(parent context.Context, now time.Time, mainEnabled, factsEnabled bool) (time.Duration, error) {
	if m.storeBackupRetentionDays() == 0 {
		return 0, nil
	}
	m.storeBackupMu.Lock()
	defer m.storeBackupMu.Unlock()
	if err := parent.Err(); err != nil {
		return 0, err
	}
	var paths []string
	if mainEnabled {
		paths = append(paths, m.cfg.StorePath)
	}
	if factsEnabled {
		paths = append(paths, m.cfg.UsageFactsStorePath)
	}
	entries, err := scanRuntimeBackupSets(m.storeBackupDir(), now, paths)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		return 0, nil
	}
	entry := entries[0]
	manifest := entry.manifest
	delay := time.Unix(manifest.CreatedAt, 0).Add(m.storeBackupInterval()).Sub(now)
	if delay <= 0 || manifest.SourceIdentity == "" || manifest.MigrationPlan != m.currentMigrationPlanID() ||
		(manifest.Main != nil) != mainEnabled || (manifest.Facts != nil) != factsEnabled {
		return 0, nil
	}
	identity, err := runtimeBackupSourceIdentity(paths)
	if err != nil {
		return 0, err
	}
	if manifest.SourceIdentity != identity {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	if _, err := verifyStoreBackupSetManifest(ctx, entry.path); err != nil {
		return 0, err
	}
	if manifest.Main != nil {
		m.storeBackupLastSuccess.Store(manifest.CreatedAt)
		m.storeBackupBytes.Store(manifest.Main.Bytes)
	}
	if manifest.Facts != nil {
		m.usageFactsBackupLastSuccess.Store(manifest.CreatedAt)
		m.usageFactsBackupBytes.Store(manifest.Facts.Bytes)
	}
	m.storeBackupSetLastSuccess.Store(manifest.CreatedAt)
	m.storeBackupSetVerified.Store(true)
	return delay, nil
}

type runtimeBackupRetentionEntry struct {
	path     string
	manifest storeBackupSetManifest
	files    map[string]os.FileInfo
}

// Control files are small and bound to exact generated names. A malformed
// record stops the entire sweep, rather than being treated as disposable.
func scanRuntimeBackupSets(dir string, now time.Time, livePaths []string) ([]runtimeBackupRetentionEntry, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	// macOS /var -> /private/var is a normal system alias; the backup
	// directory itself must not be a symlink.
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("备份目录不是普通目录")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var result []runtimeBackupRetentionEntry
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, storeBackupSetPrefix) || !strings.HasSuffix(name, storeBackupSetSuffix) {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := retentionRegularFile(path)
		if err != nil {
			return nil, err
		}
		if info.Size() <= 0 || info.Size() > 1<<20 {
			return nil, fmt.Errorf("备份 manifest 大小异常: %s", name)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var m storeBackupSetManifest
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("备份 manifest 损坏: %s: %w", name, err)
		}
		stamp, err := time.Parse(backupSetStampLayout, m.SetID)
		if err != nil || stamp.Unix() != m.CreatedAt || stamp.After(now) || m.Version != storeBackupSetVersion ||
			strings.TrimSpace(m.MigrationPlan) == "" || name != storeBackupSetPrefix+m.SetID+storeBackupSetSuffix || (m.Main == nil && m.Facts == nil) {
			return nil, fmt.Errorf("备份 manifest 标识/时间异常: %s", name)
		}
		item := runtimeBackupRetentionEntry{path: path, manifest: m, files: map[string]os.FileInfo{path: info}}
		for i, member := range []*storeBackupFileManifest{m.Main, m.Facts} {
			if member == nil {
				continue
			}
			prefix := storeBackupPrefix
			if i == 1 {
				prefix = usageFactsBackupPrefix
			}
			hash, hashErr := hex.DecodeString(member.SHA256)
			if member.File != prefix+m.SetID+storeBackupSuffix || member.Bytes <= 0 || hashErr != nil || len(hash) != 32 {
				return nil, fmt.Errorf("备份成员不属于本备份集: %s", name)
			}
			memberPath := filepath.Join(dir, member.File)
			memberInfo, err := retentionRegularFile(memberPath)
			if err != nil {
				return nil, err
			}
			if memberInfo.Size() != member.Bytes {
				return nil, fmt.Errorf("备份成员大小异常: %s", member.File)
			}
			for _, live := range livePaths {
				liveInfo, err := os.Stat(live)
				if err != nil && !os.IsNotExist(err) {
					return nil, err
				}
				if err == nil && os.SameFile(liveInfo, memberInfo) {
					return nil, fmt.Errorf("备份成员指向运行库，停止清理")
				}
			}
			// A real read/write SQLite user leaves sidecars. Never sweep such a
			// file, even if someone named a live database like a backup.
			for _, suffix := range []string{"-wal", "-shm", "-journal"} {
				if _, err := os.Lstat(memberPath + suffix); err == nil {
					return nil, fmt.Errorf("备份存在 SQLite 活动边文件: %s", member.File)
				} else if !os.IsNotExist(err) {
					return nil, err
				}
			}
			item.files[memberPath] = memberInfo
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].manifest.SetID > result[j].manifest.SetID })
	return result, nil
}

func retentionRegularFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("清理对象不是普通文件: %s", filepath.Base(path))
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		return nil, fmt.Errorf("清理对象具有硬链接或无法确认: %s", filepath.Base(path))
	}
	return info, nil
}

// days counts distinct available Beijing calendar dates, not a rolling TTL:
// downtime cannot age out the last recovery point. Keep the newest two sets
// as well, even if both occurred on the same date. Maximum: days+1 sets.
func runtimeBackupKeepSet(entries []runtimeBackupRetentionEntry, count, days int) map[string]bool {
	keep := make(map[string]bool)
	seenDays := make(map[string]bool)
	for i, entry := range entries {
		if days <= 0 {
			if i < count {
				keep[entry.path] = true
			}
			continue
		}
		day := time.Unix(entry.manifest.CreatedAt, 0).In(cstLocation).Format("2006-01-02")
		if !seenDays[day] && len(seenDays) < days {
			keep[entry.path] = true
			seenDays[day] = true
		}
		if i < 2 {
			keep[entry.path] = true
		}
	}
	return keep
}

func pruneStoreBackupSetsWithPolicy(ctx context.Context, dir string, count, days int, now time.Time, livePaths []string) error {
	if count < 1 || count > 365 || days < 0 || days > 365 {
		return fmt.Errorf("备份保留配置超出范围")
	}
	entries, err := scanRuntimeBackupSets(dir, now, livePaths)
	if err != nil {
		return err
	}
	keep := runtimeBackupKeepSet(entries, count, days)
	if len(keep) == len(entries) {
		return nil
	}
	// Re-verify the newest retained recovery points before any deletion. This
	// is background-only; never invoked from readiness or an HTTP read path.
	for i, entry := range entries {
		if i >= 2 {
			break
		}
		if !keep[entry.path] {
			continue
		}
		if _, err := verifyStoreBackupSetManifest(ctx, entry.path); err != nil {
			return fmt.Errorf("保留恢复点核验失败，取消淘汰: %w", err)
		}
	}
	for _, entry := range entries {
		if keep[entry.path] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Validate all files before touching any member of this pair.
		for path, before := range entry.files {
			after, err := retentionRegularFile(path)
			if err != nil {
				return err
			}
			if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
				return fmt.Errorf("备份文件在清理期间改变，停止淘汰: %s", filepath.Base(path))
			}
		}
		// Unpublish first: a crash may leave loose DBs, never a manifest that
		// falsely advertises a complete restorable pair. Loose files are left
		// for explicit review, not broad wildcard deletion on the next run.
		if err := os.Remove(entry.path); err != nil {
			return err
		}
		if err := syncDirectory(dir); err != nil {
			return err
		}
		for path := range entry.files {
			if path == entry.path {
				continue
			}
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		if err := syncDirectory(dir); err != nil {
			return err
		}
		slog.Info("淘汰过期在线备份集", "set_id", entry.manifest.SetID, "retention_days", days, "retention_count", count)
	}
	return nil
}
