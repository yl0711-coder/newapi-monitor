package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	financeReportSnapshotFormatVersion = 1
	financeReportSnapshotDirName       = "finance-report-cache"
	financeReportSnapshotFileSuffix    = ".json"
)

type financeReportSnapshotEnvelope struct {
	FormatVersion     int             `json:"format_version"`
	CacheKey          string          `json:"cache_key"`
	SourceFingerprint string          `json:"source_fingerprint"`
	StoredAt          int64           `json:"stored_at"`
	PayloadSHA256     string          `json:"payload_sha256"`
	Payload           json.RawMessage `json:"payload"`
}

func financeReportSnapshotDir(storePath string) (string, error) {
	storePath = strings.TrimSpace(storePath)
	if storePath == "" || !storeUsesFile(storePath) {
		return "", errors.New("经营核算持久快照需要文件型本地库路径")
	}
	return filepath.Join(filepath.Dir(storePath), financeReportSnapshotDirName), nil
}

func financeReportSnapshotPath(storePath, key string) (string, error) {
	dir, err := financeReportSnapshotDir(storePath)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(dir, hex.EncodeToString(digest[:])+financeReportSnapshotFileSuffix), nil
}

func (m *Monitor) loadFinanceReportSnapshot(request financeReportRequest, now time.Time) ([]byte, time.Time, string, bool, error) {
	if !m.cfg.FinanceReportSnapshotReadEnabled {
		return nil, time.Time{}, "", false, nil
	}
	key := request.logicalKey()
	path, err := financeReportSnapshotPath(m.cfg.StorePath, key)
	if err != nil {
		return nil, time.Time{}, "", false, err
	}
	m.financeSnapshotWriteMu.RLock()
	defer m.financeSnapshotWriteMu.RUnlock()
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, time.Time{}, "", false, nil
	}
	if err != nil {
		return nil, time.Time{}, "", false, fmt.Errorf("读取经营核算持久快照信息: %w", err)
	}
	if info.Size() <= 0 || info.Size() > int64(financeReportCacheMaxBytes+64*1024) {
		return nil, time.Time{}, "", false, errors.New("经营核算持久快照文件大小异常")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, "", false, fmt.Errorf("读取经营核算持久快照: %w", err)
	}
	var envelope financeReportSnapshotEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, time.Time{}, "", false, fmt.Errorf("解析经营核算持久快照: %w", err)
	}
	if envelope.FormatVersion != financeReportSnapshotFormatVersion || envelope.CacheKey != key ||
		envelope.StoredAt <= 0 || len(envelope.Payload) == 0 || len(envelope.Payload) > financeReportCacheMaxBytes || !json.Valid(envelope.Payload) {
		return nil, time.Time{}, "", false, errors.New("经营核算持久快照元数据无效")
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.PayloadSHA256 != hex.EncodeToString(digest[:]) {
		return nil, time.Time{}, "", false, errors.New("经营核算持久快照载荷哈希不一致")
	}
	storedAt := time.Unix(envelope.StoredAt, 0)
	if storedAt.After(now.Add(time.Minute)) {
		return nil, time.Time{}, "", false, errors.New("经营核算持久快照时间来自未来")
	}
	age := now.Sub(storedAt)
	if age >= financeReportPersistentStale {
		return nil, time.Time{}, "", false, nil
	}
	state := "stale"
	// A verified matching local-source fingerprint is stronger than wall-clock
	// age: unchanged closed history remains reusable without recomputation.
	// When probing is unavailable, retain the conservative five-minute TTL.
	if request.sourceFingerprint != "" && envelope.SourceFingerprint == request.sourceFingerprint {
		state = "fresh"
	} else if request.sourceFingerprint == "" && envelope.SourceFingerprint == "" && age < financeReportCacheTTL {
		state = "fresh"
	}
	return append([]byte(nil), envelope.Payload...), storedAt, state, true, nil
}

// persistFinanceReportSnapshotShadow stores only a rebuildable, bounded cache
// artifact beside Monitor's local database. Reading remains behind a separate
// rollout gate, so shadow writes can be validated before serving them.
func (m *Monitor) persistFinanceReportSnapshotShadow(key, sourceFingerprint string, payload []byte, now time.Time) error {
	if !m.cfg.FinanceReportSnapshotShadowEnabled {
		return nil
	}
	if strings.TrimSpace(key) == "" {
		return errors.New("经营核算持久快照缺少缓存键")
	}
	if len(payload) == 0 || len(payload) > financeReportCacheMaxBytes || !json.Valid(payload) {
		return errors.New("经营核算持久快照载荷无效或超过上限")
	}
	path, err := financeReportSnapshotPath(m.cfg.StorePath, key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("创建经营核算持久快照目录: %w", err)
	}
	payloadDigest := sha256.Sum256(payload)
	envelope, err := json.Marshal(financeReportSnapshotEnvelope{
		FormatVersion:     financeReportSnapshotFormatVersion,
		CacheKey:          key,
		SourceFingerprint: sourceFingerprint,
		StoredAt:          now.Unix(),
		PayloadSHA256:     hex.EncodeToString(payloadDigest[:]),
		Payload:           append(json.RawMessage(nil), payload...),
	})
	if err != nil {
		return fmt.Errorf("序列化经营核算持久快照: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".finance-report-*.tmp")
	if err != nil {
		return fmt.Errorf("创建经营核算持久快照临时文件: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("限制经营核算持久快照权限: %w", err)
	}
	if _, err := tmp.Write(envelope); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("写入经营核算持久快照: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("同步经营核算持久快照: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("关闭经营核算持久快照: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("发布经营核算持久快照: %w", err)
	}
	if err := os.Chtimes(path, now, now); err != nil {
		return fmt.Errorf("标记经营核算持久快照时间: %w", err)
	}
	return pruneFinanceReportSnapshots(dir, financeReportCacheMaxEntries)
}

func pruneFinanceReportSnapshots(dir string, limit int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("读取经营核算持久快照目录: %w", err)
	}
	type snapshotFile struct {
		path    string
		modTime time.Time
	}
	files := make([]snapshotFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), financeReportSnapshotFileSuffix) {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return fmt.Errorf("读取经营核算持久快照信息: %w", infoErr)
		}
		files = append(files, snapshotFile{path: filepath.Join(dir, entry.Name()), modTime: info.ModTime()})
	}
	if len(files) <= limit {
		return nil
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime.Equal(files[j].modTime) {
			return files[i].path < files[j].path
		}
		return files[i].modTime.Before(files[j].modTime)
	})
	for _, file := range files[:len(files)-limit] {
		if err := os.Remove(file.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("清理经营核算旧持久快照: %w", err)
		}
	}
	return nil
}
