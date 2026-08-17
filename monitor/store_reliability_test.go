package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestPreflightStoreIntegrityAcceptsValidAndRejectsCorrupt(t *testing.T) {
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid store.db")
	db, err := sql.Open("sqlite", valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE facts(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO facts(value) VALUES ('ok')"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	checked, err := preflightStoreIntegrity(valid)
	if err != nil || !checked {
		t.Fatalf("valid store rejected: checked=%v err=%v", checked, err)
	}

	corrupt := filepath.Join(dir, "corrupt.db")
	if err := os.WriteFile(corrupt, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}
	checked, err = preflightStoreIntegrity(corrupt)
	if err == nil || !checked {
		t.Fatalf("corrupt store must be rejected before migration: checked=%v err=%v", checked, err)
	}
}

func TestNewDerivesIndependentUsageFactsStorePath(t *testing.T) {
	dir := t.TempDir()
	m, err := New(Settings{
		StorePath:          filepath.Join(dir, "monitor.db"),
		LocalSnapshotOnly:  true,
		StoreBackupEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	want := filepath.Join(dir, "usage-facts.db")
	if m.cfg.UsageFactsStorePath != want || m.usageFactsDB == nil || m.usageFactsDB == m.storeDB {
		t.Fatalf("默认事实库路径/连接未隔离: got=%q want=%q", m.cfg.UsageFactsStorePath, want)
	}
}

func TestCreateStoreBackupIsReadableAndPreservesData(t *testing.T) {
	m := newTestMonitor(t)
	backupDir := filepath.Join(t.TempDir(), "verified backups")
	m.cfg.StoreBackupEnabled = true
	m.cfg.StoreBackupDir = backupDir
	m.cfg.StoreBackupRetention = 3
	if err := m.storeDB.Create(&TrackedUser{UserID: 99001, Username: "backup-check"}).Error; err != nil {
		t.Fatal(err)
	}

	backup, err := m.createStoreBackup(context.Background(), time.Date(2026, 8, 13, 9, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(backup)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || info.Size() == 0 {
		t.Fatalf("unexpected backup mode/size: mode=%o size=%d", info.Mode().Perm(), info.Size())
	}
	if m.storeBackupLastSuccess.Load() == 0 || m.storeBackupBytes.Load() != info.Size() {
		t.Fatalf("backup health state not updated: success=%d bytes=%d", m.storeBackupLastSuccess.Load(), m.storeBackupBytes.Load())
	}
	status := m.storeReliabilityStatus()
	if !status.BackupEnabled || !status.IntegrityOK || status.IntegrityCheckedAt == 0 ||
		status.LastBackupSuccessAt == 0 || status.LastBackupBytes != info.Size() ||
		status.BackupRetention != 3 {
		t.Fatalf("unexpected store reliability status: %+v", status)
	}

	backupDB, err := sql.Open("sqlite", sqliteReadOnlyDSN(backup))
	if err != nil {
		t.Fatal(err)
	}
	defer backupDB.Close()
	if err := sqliteQuickCheck(context.Background(), backupDB); err != nil {
		t.Fatalf("backup quick_check: %v", err)
	}
	var username string
	if err := backupDB.QueryRow("SELECT username FROM tracked_users WHERE user_id = 99001").Scan(&username); err != nil {
		t.Fatal(err)
	}
	if username != "backup-check" {
		t.Fatalf("backup data mismatch: %q", username)
	}
}

func TestUsageFactsStoreIsIndependentBackedUpAndFailureIsolated(t *testing.T) {
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "monitor.db")
	factsPath := filepath.Join(dir, "usage-facts.db")
	backupDir := filepath.Join(dir, "backups")
	m := &Monitor{cfg: Settings{
		UsageFactsStorePath:  factsPath,
		StoreBackupEnabled:   true,
		StoreBackupDir:       backupDir,
		StoreBackupRetention: 3,
	}}
	if err := m.openStore(mainPath); err != nil {
		t.Fatal(err)
	}
	if m.usageFactsDB == nil || m.usageFactsDB == m.storeDB {
		t.Fatal("生产形态必须使用独立的用量事实 SQLite")
	}
	if m.storeDB.Migrator().HasTable(&UsageHourFact{}) || !m.usageFactsDB.Migrator().HasTable(&UsageHourFact{}) {
		t.Fatal("用量事实表不得迁移到 Monitor 主库")
	}
	if m.usageFactsDB.Migrator().HasTable(&TrackedUser{}) || !m.storeDB.Migrator().HasTable(&TrackedUser{}) {
		t.Fatal("权限/控制表不得迁移到用量事实库")
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 99101, Username: "main-store"}).Error; err != nil {
		t.Fatal(err)
	}
	fact := UsageHourFact{
		HourTs: usageFactTestDay().Unix(), DayTs: usageFactTestDay().Unix(), UserID: 99101,
		ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 1, ConsumeQuota: 123,
	}
	if err := m.usageFactsDB.Create(&fact).Error; err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 8, 13, 9, 30, 0, 0, time.UTC)
	mainBackup, err := m.createStoreBackup(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	factsBackup, err := m.createUsageFactsBackup(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(filepath.Base(mainBackup), storeBackupPrefix) ||
		!strings.HasPrefix(filepath.Base(factsBackup), usageFactsBackupPrefix) || mainBackup == factsBackup {
		t.Fatalf("主库和事实库备份命名/目标未隔离: main=%q facts=%q", mainBackup, factsBackup)
	}
	mainCopy, err := sql.Open("sqlite", sqliteReadOnlyDSN(mainBackup))
	if err != nil {
		t.Fatal(err)
	}
	defer mainCopy.Close()
	var username string
	if err := mainCopy.QueryRow("SELECT username FROM tracked_users WHERE user_id = 99101").Scan(&username); err != nil || username != "main-store" {
		t.Fatalf("主库备份内容错误: username=%q err=%v", username, err)
	}
	if err := mainCopy.QueryRow("SELECT COUNT(*) FROM usage_hour_facts").Scan(new(int64)); err == nil {
		t.Fatal("主库备份不应包含用量事实表")
	}
	factsCopy, err := sql.Open("sqlite", sqliteReadOnlyDSN(factsBackup))
	if err != nil {
		t.Fatal(err)
	}
	defer factsCopy.Close()
	var quota int64
	if err := factsCopy.QueryRow("SELECT consume_quota FROM usage_hour_facts WHERE user_id = 99101").Scan(&quota); err != nil || quota != 123 {
		t.Fatalf("事实库备份内容错误: quota=%d err=%v", quota, err)
	}
	if err := factsCopy.QueryRow("SELECT COUNT(*) FROM tracked_users").Scan(new(int64)); err == nil {
		t.Fatal("事实库备份不应包含权限/控制表")
	}
	if mainStatus, factsStatus := m.storeReliabilityStatus(), m.usageFactsStoreReliabilityStatus(); !mainStatus.IntegrityOK || !factsStatus.IntegrityOK || mainStatus.LastBackupSuccessAt == 0 || factsStatus.LastBackupSuccessAt == 0 {
		t.Fatalf("两库可靠性状态应独立成功: main=%+v facts=%+v", mainStatus, factsStatus)
	}

	// 迁移前快照是双库共同闸门：即使仍处于 write-only/shadow，只要现有
	// facts 库损坏，就必须在主库任何 AutoMigrate 前拒绝整个新进程启动。
	m.Close()
	if err := os.WriteFile(factsPath, []byte("corrupt usage facts"), 0o600); err != nil {
		t.Fatal(err)
	}
	degraded := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath}}
	if err := degraded.openStore(mainPath); err == nil {
		degraded.Close()
		t.Fatal("write-only 阶段也不得越过损坏 facts 库执行主库迁移")
	}

	strict := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath, UsageFactsReadEnabled: true}}
	if err := strict.openStore(mainPath); err == nil {
		strict.Close()
		t.Fatal("已切读时事实库损坏必须阻止启动，不能静默回退来源 logs")
	}
}

func TestStoreReliabilityStatusDoesNotClaimBackupForMemoryStore(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.StorePath = ":memory:"
	m.cfg.StoreBackupEnabled = true
	status := m.storeReliabilityStatus()
	if status.BackupEnabled {
		t.Fatalf("memory store must not report backup enabled: %+v", status)
	}
}

func TestManualStoreBackupIsAsyncSingleFlightAndKeepsStoresReadable(t *testing.T) {
	const (
		restoreLegacySessionSecret = "production-session-secret-used-before-credential-split"
		restoreCredentialSecret    = "production-dedicated-upstream-credential-secret"
		restoreUpstreamDomain      = "restored-upstream.example"
	)
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                    filepath.Join(dir, "main.db"),
		UsageFactsStorePath:          filepath.Join(dir, "facts.db"),
		StoreBackupEnabled:           true,
		StoreBackupDir:               filepath.Join(dir, "backups"),
		StoreBackupRetention:         3,
		UsageFactsEnabled:            true,
		UsageFactsReadEnabled:        true,
		UsageFactsFullHistoryEnabled: true,
		UsageFactsHistorySourceMode:  "complete",
		UsageFactsHistorySourceEpoch: "restore-source-epoch-20260817",
		LocalSnapshotOnly:            true,
		UsageFactsLocalReadOnly:      true,
		SessionSecret:                restoreLegacySessionSecret,
	}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	if err := m.storeDB.Create(&TrackedUser{UserID: 7, Username: "backup-user"}).Error; err != nil {
		t.Fatal(err)
	}
	// The runtime backup must carry a genuinely publishable full-history
	// checkpoint, not merely two syntactically valid SQLite files. Model a
	// signed no-history member so the restored zero-network process can prove
	// its read authority without contacting the source database.
	publishedNow := time.Now()
	publishedThrough := m.usageFactFinalizedHour(publishedNow)
	publishedFloor := usageFactDayStart(publishedThrough - 2*usageFactDaySeconds)
	verifiedThrough := usageFactDayStart(publishedThrough)
	if err := m.usageFactsStore().Create(&UsageFactMemberState{
		UserID: 7, Active: true, TrackedRevision: 1,
		SourceFloorHour: &publishedFloor, CoverageThroughHour: &verifiedThrough,
		TailThroughHour: &publishedThrough, VerifyNextHour: &verifiedThrough,
		VerifiedThroughHour: &verifiedThrough, VerificationStatus: "complete",
		VerifiedAt: publishedNow.Unix(), SourceFloorCheckedAt: publishedNow.Unix(),
		SourceHistoryStatus: "no_history", CoverageStatus: "ready",
		ClassificationVersion: userTrafficClassificationVersion,
		QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceEpoch:           "restore-source-epoch-20260817", UpdatedAt: publishedNow.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
		UserID: 7, TrackedRevision: 1, SourceEpoch: "restore-source-epoch-20260817",
		ClassificationVersion: userTrafficClassificationVersion,
		QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour:       publishedFloor, VerifiedThroughHour: verifiedThrough,
		PublishedAt: publishedNow.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = ?", 1).Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"generation":            1, "serving_generation": 1,
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{7}),
		"published_range_start": publishedFloor, "published_through": publishedThrough,
		"published_at": publishedNow.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageDailyFact{DateTs: 1, UserID: 7, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 1}).Error; err != nil {
		t.Fatal(err)
	}
	sealedUpstream, err := m.sealUpstreamCredential(restoreUpstreamDomain, upstreamProviderNewAPI, newAPICredential{AccessToken: "restored-access-token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamAccount{
		Domain: restoreUpstreamDomain, Provider: upstreamProviderNewAPI,
		Credential: sealedUpstream, CredentialVersion: upstreamCredentialVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if !m.triggerManualStoreBackup() {
		t.Fatal("首次手动备份应启动")
	}
	if m.triggerManualStoreBackup() {
		t.Fatal("备份进行中不应排队第二份")
	}
	deadline := time.Now().Add(5 * time.Second)
	for m.storeManualBackupRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if m.storeManualBackupRunning.Load() {
		t.Fatal("手动备份超时")
	}
	mainStatus, factsStatus := m.storeReliabilityStatus(), m.usageFactsStoreReliabilityStatus()
	if mainStatus.LastBackupSuccessAt == 0 || factsStatus.LastBackupSuccessAt == 0 || mainStatus.BackupRunning || factsStatus.BackupRunning ||
		!mainStatus.BackupSetVerified || !factsStatus.BackupSetVerified || mainStatus.BackupSetSuccessAt == 0 {
		t.Fatalf("两份 SQLite 备份状态错误: main=%+v facts=%+v", mainStatus, factsStatus)
	}
	entries, err := os.ReadDir(m.cfg.StoreBackupDir)
	if err != nil {
		t.Fatal(err)
	}
	var mainCopies, factsCopies, setCopies int
	var manifestPath string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), storeBackupPrefix) {
			mainCopies++
		}
		if strings.HasPrefix(entry.Name(), usageFactsBackupPrefix) {
			factsCopies++
		}
		if strings.HasPrefix(entry.Name(), storeBackupSetPrefix) && strings.HasSuffix(entry.Name(), storeBackupSetSuffix) {
			setCopies++
			manifestPath = filepath.Join(m.cfg.StoreBackupDir, entry.Name())
		}
	}
	if mainCopies != 1 || factsCopies != 1 || setCopies != 1 {
		t.Fatalf("应产生一个已校验双库备份集: main=%d facts=%d sets=%d", mainCopies, factsCopies, setCopies)
	}
	manifest, err := verifyStoreBackupSetManifest(context.Background(), manifestPath)
	if err != nil || manifest.Main == nil || manifest.Facts == nil || manifest.MigrationPlan != preMigrationPlanID ||
		manifest.ActiveMemberManifestSHA == "" || manifest.PublishedSignatureSHA == "" {
		t.Fatalf("双库备份集不可恢复校验: manifest=%+v err=%v", manifest, err)
	}
	concurrentDir := filepath.Join(dir, "concurrent-restore-volume")
	if err := os.Mkdir(concurrentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	const contenders = 4
	start := make(chan struct{})
	results := make(chan error, contenders)
	var restores sync.WaitGroup
	for i := 0; i < contenders; i++ {
		restores.Add(1)
		go func() {
			defer restores.Done()
			<-start
			results <- RestoreStoreBackupSet(context.Background(), manifestPath, concurrentDir, "main.db", "facts.db")
		}()
	}
	close(start)
	restores.Wait()
	close(results)
	var successes int
	for restoreErr := range results {
		if restoreErr == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("并发恢复必须只有一个O_EXCL所有者成功，实际成功=%d", successes)
	}
	for _, name := range []string{"main.db", "facts.db", storeBackupRestoreReadyName} {
		if info, err := os.Lstat(filepath.Join(concurrentDir, name)); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("并发loser破坏winner产物 %s: info=%v err=%v", name, info, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(concurrentDir, storeBackupRestoreBusyName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("并发成功后仍留有IN_PROGRESS: %v", err)
	}
	restoreDir := filepath.Join(dir, "restored-runtime-volume")
	if err := RestoreStoreBackupSet(context.Background(), manifestPath, restoreDir, "nexus_monitor.db", "usage-facts.db"); err != nil {
		t.Fatalf("运行期备份集恢复失败: %v", err)
	}
	for _, name := range []string{"nexus_monitor.db", "usage-facts.db", storeBackupRestoreReadyName} {
		info, err := os.Lstat(filepath.Join(restoreDir, name))
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("恢复产物 %s 缺失或不是普通文件: info=%v err=%v", name, info, err)
		}
	}
	restoredMainPath := filepath.Join(restoreDir, "nexus_monitor.db")
	restoredFactsPath := filepath.Join(restoreDir, "usage-facts.db")
	restoredMain, err := sql.Open("sqlite", restoredMainPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var restoredUsers int64
	if err := restoredMain.QueryRow("SELECT COUNT(*) FROM tracked_users WHERE user_id=7").Scan(&restoredUsers); err != nil || restoredUsers != 1 {
		t.Fatalf("恢复主库成员数据错误: users=%d err=%v", restoredUsers, err)
	}
	if err := restoredMain.Close(); err != nil {
		t.Fatal(err)
	}
	restoredFacts, err := sql.Open("sqlite", restoredFactsPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var restoredRows int64
	if err := restoredFacts.QueryRow("SELECT COUNT(*) FROM usage_daily_facts WHERE user_id=7").Scan(&restoredRows); err != nil || restoredRows != 1 {
		t.Fatalf("恢复 facts 数据错误: rows=%d err=%v", restoredRows, err)
	}
	if err := restoredFacts.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, storeBackupRestoreBusyName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("成功恢复仍留有 IN_PROGRESS: %v", err)
	}
	// Exercise the exact standalone, zero-network restore-audit environment from
	// the runbook. In particular, the file names and signed full-history source
	// contract must be explicit; Dockerfile defaults would otherwise point at a
	// different main database or reject full-history mode before READY activation.
	t.Setenv("MONITOR_STORE_PATH", restoredMainPath)
	t.Setenv("MONITOR_USAGE_FACTS_STORE_PATH", restoredFactsPath)
	t.Setenv("MONITOR_ADDR", ":8090")
	t.Setenv("MONITOR_PORTAL_ADDR", ":8091")
	t.Setenv("MONITOR_STORE_BACKUP_ENABLED", "false")
	auditBackupDir := filepath.Join(dir, "restore-audit-backups")
	t.Setenv("MONITOR_STORE_BACKUP_DIR", auditBackupDir)
	t.Setenv("MONITOR_LOCAL_SNAPSHOT_ONLY", "true")
	t.Setenv("MONITOR_SOURCE_WORKER_ENABLED", "false")
	t.Setenv("MONITOR_SOURCE_LEASE_REQUIRED", "false")
	t.Setenv("MONITOR_SESSION_SECRET", restoreLegacySessionSecret)
	t.Setenv("MONITOR_UPSTREAM_CREDENTIAL_SECRET", restoreCredentialSecret)
	t.Setenv("MONITOR_USAGE_FACTS_ENABLED", "true")
	t.Setenv("MONITOR_USAGE_FACTS_READ_ENABLED", "true")
	t.Setenv("MONITOR_USAGE_FACTS_FULL_HISTORY_ENABLED", "true")
	t.Setenv("MONITOR_USAGE_FACTS_LOCAL_READ_ONLY", "true")
	t.Setenv("MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE", "complete")
	t.Setenv("MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH", "restore-source-epoch-20260817")
	t.Setenv("MONITOR_UPSTREAM_SYNC_ENABLED", "false")
	t.Setenv("MONITOR_STABILITY_ENABLED", "false")
	t.Setenv("MONITOR_INFRA_ENABLED", "false")
	restoreSettings := LoadSettings()
	activated, err := New(restoreSettings)
	if err != nil {
		t.Fatalf("完整 READY 备份集无法通过启动前激活: %v", err)
	}
	var restoredUpstream ChannelUpstreamAccount
	if err := activated.storeDB.First(&restoredUpstream, "domain = ?", restoreUpstreamDomain).Error; err != nil {
		t.Fatal(err)
	}
	if restoredUpstream.Credential == sealedUpstream {
		t.Fatal("offline activation did not rotate the restored legacy upstream credential")
	}
	var restoredCredential newAPICredential
	if err := activated.openUpstreamCredential(restoredUpstream, &restoredCredential); err != nil || restoredCredential.AccessToken != "restored-access-token" {
		t.Fatalf("offline activation cannot decrypt restored upstream credential: token=%q err=%v", restoredCredential.AccessToken, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	activated.Start(ctx)
	t.Cleanup(cancel)
	lifecycleEventually(t, time.Second, func() bool {
		return activated.localStoreProbeOK.Load() && activated.localFactsProbeOK.Load() && activated.usageFactsReadReady.Load()
	}, "restored offline snapshot did not become locally ready")
	gin.SetMode(gin.TestMode)
	router := gin.New()
	activated.RegisterRoutes(router)
	for _, path := range []string{"/live", "/ready"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("restored zero-network route %s unavailable: code=%d body=%s", path, w.Code, w.Body.String())
		}
	}
	if activated.prodDB != nil || activated.cfg.sourceWorkerIsEnabled() || activated.sourceWorkerRunning.Load() {
		t.Fatal("restored offline audit unexpectedly enabled a source connection or worker")
	}
	cancel()
	activated.Close()
	if _, err := os.Stat(filepath.Join(restoreDir, storeBackupRestoreActiveName)); err != nil {
		t.Fatalf("完整恢复未发布持久 ACTIVATED 审计标记: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, storeBackupRestoreReadyName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("READY 未原子激活: %v", err)
	}
	if snapshots := preMigrationSnapshotDirs(t, auditBackupDir); len(snapshots) != 1 {
		t.Fatalf("恢复审计的迁移前快照未写入独立backup目标: %v", snapshots)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "backups")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("恢复审计意外在data卷内创建备份目录: %v", err)
	}
	reopened, err := New(restoreSettings)
	if err != nil {
		t.Fatalf("已激活恢复卷重启失败: %v", err)
	}
	reopened.Close()
	activePath := filepath.Join(restoreDir, storeBackupRestoreActiveName)
	activeData, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	var historicalActive storeBackupRestoreReady
	if err := json.Unmarshal(activeData, &historicalActive); err != nil {
		t.Fatal(err)
	}
	historicalActive.MigrationPlan = "main-facts-schema-historical-origin"
	activeData, err = json.MarshalIndent(historicalActive, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activePath, append(activeData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	upgradedAfterRestore, err := New(restoreSettings)
	if err != nil {
		t.Fatalf("历史ACTIVATED来源计划不得阻止未来镜像的普通迁移门禁: %v", err)
	}
	upgradedAfterRestore.Close()
	activeWrongFactsPath := filepath.Join(dir, "active-wrong-facts", "usage-facts.db")
	misconfiguredActive, err := New(Settings{
		StorePath: restoredMainPath, UsageFactsStorePath: activeWrongFactsPath,
		StoreBackupEnabled: false, LocalSnapshotOnly: true, UsageFactsLocalReadOnly: true,
	})
	if err == nil {
		misconfiguredActive.Close()
		t.Fatal("ACTIVATED 恢复卷配到另一 facts 目录仍被启动")
	}
	if _, err := os.Stat(activeWrongFactsPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ACTIVATED 错配置启动创建了新 facts: %v", err)
	}

	wrongConfigDir := filepath.Join(dir, "ready-wrong-config")
	if err := RestoreStoreBackupSet(context.Background(), manifestPath, wrongConfigDir, "nexus_monitor.db", "usage-facts.db"); err != nil {
		t.Fatal(err)
	}
	wrongFactsPath := filepath.Join(dir, "wrong-facts-location", "usage-facts.db")
	misconfigured, err := New(Settings{
		StorePath: filepath.Join(wrongConfigDir, "nexus_monitor.db"), UsageFactsStorePath: wrongFactsPath,
		StoreBackupEnabled: false, LocalSnapshotOnly: true, UsageFactsLocalReadOnly: true,
	})
	if err == nil {
		misconfigured.Close()
		t.Fatal("READY 恢复卷配到另一 facts 目录仍被启动")
	}
	if !strings.Contains(err.Error(), "路径") {
		t.Fatalf("READY 错路径错误不明确: %v", err)
	}
	if _, err := os.Stat(wrongFactsPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("错配置启动创建了新 facts: %v", err)
	}

	crashDir := filepath.Join(dir, "crash-after-main-publication")
	if err := os.Mkdir(crashDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(crashDir, storeBackupRestoreBusyName), []byte("interrupted\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mainData, err := os.ReadFile(restoredMainPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(crashDir, "nexus_monitor.db"), mainData, 0o600); err != nil {
		t.Fatal(err)
	}
	partial, err := New(Settings{
		StorePath: filepath.Join(crashDir, "nexus_monitor.db"), UsageFactsStorePath: filepath.Join(crashDir, "usage-facts.db"),
		StoreBackupEnabled: false, LocalSnapshotOnly: true, UsageFactsLocalReadOnly: true,
	})
	if err == nil {
		partial.Close()
		t.Fatal("恢复在仅发布 main 后崩溃，启动却创建了空 facts 库")
	}
	if !strings.Contains(err.Error(), "未完成") {
		t.Fatalf("恢复中间态错误不明确: %v", err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "usage-facts.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("启动门禁在拒绝前创建了 facts: %v", err)
	}
	if err := RestoreStoreBackupSet(context.Background(), manifestPath, restoreDir, "nexus_monitor.db", "usage-facts.db"); err == nil ||
		!strings.Contains(err.Error(), "全新的空目录") {
		t.Fatalf("恢复工具不得覆盖已有卷: %v", err)
	}
	if err := RestoreStoreBackupSet(context.Background(), manifestPath, filepath.Join(dir, "unsafe-name"), "/", "facts.db"); err == nil {
		t.Fatal("恢复工具接受了绝对主库路径")
	}
	var users int64
	if err := m.storeDB.Model(&TrackedUser{}).Count(&users).Error; err != nil || users != 1 {
		t.Fatalf("备份期间运行库应继续可读: users=%d err=%v", users, err)
	}
}

func TestRuntimeBackupSetRejectsCrossStoreRevisionMismatchAndTampering(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath: filepath.Join(dir, "main.db"), UsageFactsStorePath: filepath.Join(dir, "facts.db"),
		StoreBackupEnabled: true, StoreBackupDir: filepath.Join(dir, "backups"), StoreBackupRetention: 2,
	}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	if err := m.storeDB.Create(&TrackedUser{UserID: 77, Username: "pair-user"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	through := time.Date(2026, 8, 17, 0, 0, 0, 0, usageCST).Unix()
	floor := through - usageFactDaySeconds
	published := UsageFactPublishedMember{
		UserID: 77, TrackedRevision: 2, SourceEpoch: "pair-epoch",
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour: floor, VerifiedThroughHour: through, PublishedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&published).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id=1").Updates(map[string]any{
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{77}),
		"published_range_start": floor, "published_through": through, "published_at": now.Unix(),
		"serving_generation": 2,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.createStoreBackupSet(context.Background(), now, true, true); err == nil ||
		!strings.Contains(err.Error(), "权限版本不一致") {
		t.Fatalf("跨库 revision 不一致仍发布备份集: %v", err)
	}
	entries, err := os.ReadDir(m.cfg.StoreBackupDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), storeBackupSetPrefix) || strings.HasPrefix(entry.Name(), storeBackupPrefix) ||
			strings.HasPrefix(entry.Name(), usageFactsBackupPrefix) {
			t.Fatalf("失败的备份集留下可误恢复文件: %s", entry.Name())
		}
	}

	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id=77").Update("tracked_revision", 1).Error; err != nil {
		t.Fatal(err)
	}
	manifestPath, err := m.createStoreBackupSet(context.Background(), now.Add(time.Second), true, true)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := verifyStoreBackupSetManifest(context.Background(), manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	factsPath := filepath.Join(m.cfg.StoreBackupDir, manifest.Facts.File)
	f, err := os.OpenFile(factsPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("tamper")); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyStoreBackupSetManifest(context.Background(), manifestPath); err == nil {
		t.Fatal("被篡改的备份集仍通过 hash/quick_check 恢复门禁")
	}
	restoreDir := filepath.Join(dir, "tampered-restore")
	if err := RestoreStoreBackupSet(context.Background(), manifestPath, restoreDir, "main.db", "facts.db"); err == nil {
		t.Fatal("被篡改的备份集仍可恢复")
	}
	if _, err := os.Stat(filepath.Join(restoreDir, storeBackupRestoreReadyName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("失败恢复不得发布 READY: %v", err)
	}
}

func TestReadyStatusRequiresVerifiedRuntimeBackupSet(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath: filepath.Join(dir, "main.db"), UsageFactsStorePath: filepath.Join(dir, "facts.db"),
		StoreBackupEnabled: true, StoreBackupIntervalHours: 24,
	}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	nowUnix := now.Unix()
	m.localStoreProbeOK.Store(true)
	m.localStoreProbeAt.Store(nowUnix)
	m.storeIntegrityOK.Store(true)
	m.storeIntegrityCheckedAt.Store(nowUnix)
	m.processStartedAt.Store(nowUnix - int64((20 * time.Minute).Seconds()))
	m.storeBackupLastSuccess.Store(nowUnix - 60)
	m.usageFactsBackupLastSuccess.Store(nowUnix - 60)
	m.storeBackupSetLastSuccess.Store(nowUnix - 60)
	m.storeBackupSetLastFailure.Store(nowUnix)
	m.storeBackupSetVerified.Store(true)

	status, code := m.readyStatus(now)
	if code != http.StatusOK || status.Status != "degraded" ||
		!slices.Contains(status.DegradedReasons, "backup_set_failed") {
		t.Fatalf("failed paired manifest was hidden: code=%d status=%+v", code, status)
	}

	m.storeBackupSetLastFailure.Store(0)
	m.storeBackupSetVerified.Store(false)
	status, _ = m.readyStatus(now)
	if !slices.Contains(status.DegradedReasons, "backup_set_unverified") {
		t.Fatalf("unverified paired manifest was hidden: %+v", status)
	}

	m.storeBackupSetVerified.Store(true)
	status, _ = m.readyStatus(now)
	if slices.Contains(status.DegradedReasons, "backup_set_failed") ||
		slices.Contains(status.DegradedReasons, "backup_set_unverified") ||
		slices.Contains(status.DegradedReasons, "backup_set_missing") {
		t.Fatalf("verified paired manifest remained degraded: %+v", status)
	}
}

func TestPruneStoreBackupsKeepsNewestVerifiedFilesOnly(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"monitor-20260810-000000.000000000.db",
		"monitor-20260811-000000.000000000.db",
		"monitor-20260812-000000.000000000.db",
		"monitor-20260813-000000.000000000.db",
		"unrelated.db",
		".monitor-incomplete.tmp",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := pruneStoreBackups(dir, 2); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), storeBackupPrefix) && strings.HasSuffix(entry.Name(), storeBackupSuffix) {
			kept = append(kept, entry.Name())
		}
	}
	want := []string{"monitor-20260812-000000.000000000.db", "monitor-20260813-000000.000000000.db"}
	if strings.Join(kept, ",") != strings.Join(want, ",") {
		t.Fatalf("kept=%v want=%v", kept, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated.db")); err != nil {
		t.Fatalf("unrelated file removed: %v", err)
	}
}
