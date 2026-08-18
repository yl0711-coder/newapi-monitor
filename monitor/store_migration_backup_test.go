package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const legacyMainSchema = `
CREATE TABLE metric_samples (
  bucket_ts INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  model_name TEXT NOT NULL,
  grp TEXT NOT NULL,
  success INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_ts, channel_id, model_name, grp)
);
CREATE TABLE legacy_guard (
  id INTEGER PRIMARY KEY,
  value TEXT NOT NULL
);`

const legacyFactsSchema = `
CREATE TABLE usage_hour_facts (
  hour_ts INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  grp TEXT NOT NULL,
  model_name TEXT NOT NULL,
  token_id INTEGER NOT NULL,
  day_ts INTEGER NOT NULL,
  token_name TEXT,
  requests INTEGER NOT NULL DEFAULT 0,
  consume_quota INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (hour_ts, user_id, channel_id, grp, model_name, token_id)
);
CREATE TABLE legacy_facts_guard (
  id INTEGER PRIMARY KEY,
  value TEXT NOT NULL
);`

func createLegacyWALStore(t *testing.T, path, schema, insert string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	// Put the schema in the main file, then make the sentinel transaction live
	// only in WAL. The migration snapshot must still contain it.
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(insert); err != nil {
		t.Fatal(err)
	}
	walInfo, err := os.Stat(path + "-wal")
	if err != nil || walInfo.Size() == 0 {
		t.Fatalf("test fixture did not retain committed WAL: info=%v err=%v", walInfo, err)
	}
	return db
}

func sqliteHasColumn(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	rows, err := db.Query("PRAGMA table_info(" + quoteSQLiteIdentifier(table) + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return false
}

func sqliteHasTable(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count == 1
}

func preMigrationSnapshotDirs(t *testing.T, backupDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), preMigrationSnapshotPrefix) {
			paths = append(paths, filepath.Join(backupDir, entry.Name()))
		}
	}
	sort.Strings(paths)
	return paths
}

func openReadOnlyTestStore(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func rewritePreMigrationSnapshotPlanForTest(t *testing.T, backupDir, mainPath, factsPath, snapshotDir, plan string) {
	t.Helper()
	manifestPath := filepath.Join(snapshotDir, preMigrationManifestName)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest preMigrationSnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.MigrationPlan = plan
	data, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	hash := hex.EncodeToString(digest[:])
	if err := os.WriteFile(filepath.Join(snapshotDir, preMigrationReadyName), []byte("sha256:"+hash+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reference := preMigrationPlanReference{
		FormatVersion: preMigrationSnapshotVersion, MigrationPlan: plan,
		SnapshotDir: filepath.Base(snapshotDir), ManifestSHA256: hash,
	}
	referenceData, err := json.MarshalIndent(reference, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	referenceData = append(referenceData, '\n')
	if err := os.WriteFile(filepath.Join(backupDir, preMigrationReferenceName(mainPath, factsPath)), referenceData, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialKeyRotationPlanBumpCreatesFreshRollbackSnapshot(t *testing.T) {
	const (
		priorPlan = "main-facts-schema-20260817-v11"
		oldSecret = "legacy-session-secret-before-v12-key-rotation"
		newSecret = "dedicated-upstream-secret-after-v12-key-rotation"
		domain    = "plan-bump-credential.example"
	)
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "nexus_monitor.db")
	factsPath := filepath.Join(dir, "usage-facts.db")
	backupDir := filepath.Join(dir, "backups")
	legacy := &Monitor{cfg: Settings{
		StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: backupDir,
		StoreMigrationBackupRetention: 3, SessionSecret: oldSecret,
	}}
	if err := legacy.openStore(mainPath); err != nil {
		t.Fatal(err)
	}
	prior, err := legacy.createPreMigrationSnapshot(context.Background(), mainPath, factsPath, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	rewritePreMigrationSnapshotPlanForTest(t, backupDir, mainPath, factsPath, prior.SnapshotDir, priorPlan)
	sealed, err := legacy.sealUpstreamCredential(domain, upstreamProviderNewAPI, newAPICredential{AccessToken: "rollback-token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.storeDB.Create(&ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, Credential: sealed,
		CredentialVersion: upstreamCredentialVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	legacy.Close()

	candidate := &Monitor{cfg: Settings{
		StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: backupDir,
		StoreMigrationBackupRetention: 3, SessionSecret: oldSecret,
		UpstreamCredentialSecret: newSecret,
	}}
	if err := candidate.openStore(mainPath); err != nil {
		t.Fatalf("v12 startup failed: %v", err)
	}
	defer candidate.Close()

	snapshots := preMigrationSnapshotDirs(t, backupDir)
	if len(snapshots) != 2 {
		t.Fatalf("plan bump must retain prior and create a fresh snapshot: %v", snapshots)
	}
	var currentSnapshot string
	for _, snapshot := range snapshots {
		manifest, _, err := loadAndVerifyPreMigrationSnapshot(context.Background(), snapshot)
		if err != nil {
			t.Fatalf("snapshot %s invalid: %v", snapshot, err)
		}
		if manifest.MigrationPlan == preMigrationPlanID {
			currentSnapshot = snapshot
		}
	}
	if currentSnapshot == "" || currentSnapshot == prior.SnapshotDir {
		t.Fatalf("v12 did not publish a distinct rollback point: prior=%s current=%s", prior.SnapshotDir, currentSnapshot)
	}

	snapshotDB := openReadOnlyTestStore(t, filepath.Join(currentSnapshot, preMigrationMainSnapshotName))
	var rollbackRow ChannelUpstreamAccount
	if err := snapshotDB.QueryRow(`SELECT domain, provider, credential, credential_version
		FROM channel_upstream_accounts WHERE domain = ?`, domain).
		Scan(&rollbackRow.Domain, &rollbackRow.Provider, &rollbackRow.Credential, &rollbackRow.CredentialVersion); err != nil {
		t.Fatalf("fresh rollback snapshot omitted the newly configured upstream: %v", err)
	}
	legacyReader := &Monitor{cfg: Settings{SessionSecret: oldSecret}}
	var rollbackCredential newAPICredential
	if err := legacyReader.openUpstreamCredential(rollbackRow, &rollbackCredential); err != nil || rollbackCredential.AccessToken != "rollback-token" {
		t.Fatalf("v12 rollback snapshot was not captured before key rotation: token=%q err=%v", rollbackCredential.AccessToken, err)
	}

	var liveRow ChannelUpstreamAccount
	if err := candidate.storeDB.First(&liveRow, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if liveRow.Credential == rollbackRow.Credential {
		t.Fatal("live credential was not rotated after the v12 snapshot committed")
	}
	var liveCredential newAPICredential
	if err := candidate.openUpstreamCredential(liveRow, &liveCredential); err != nil || liveCredential.AccessToken != "rollback-token" {
		t.Fatalf("rotated live credential is unavailable: token=%q err=%v", liveCredential.AccessToken, err)
	}
}

func TestPreMigrationSnapshotMigratesRealLegacySchemasIncludesWALRestoresAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "monitor.db")
	factsPath := filepath.Join(dir, "usage-facts.db")
	backupDir := filepath.Join(dir, "backups")
	mainWriter := createLegacyWALStore(t, mainPath, legacyMainSchema, `
INSERT INTO metric_samples(bucket_ts, channel_id, model_name, grp, success)
VALUES (1700000000, 7, 'legacy-model', 'legacy-group', 3);
INSERT INTO legacy_guard(id, value) VALUES (1, 'main-in-wal');`)
	factsWriter := createLegacyWALStore(t, factsPath, legacyFactsSchema, `
INSERT INTO usage_hour_facts(hour_ts, user_id, channel_id, grp, model_name, token_id, day_ts, token_name, requests, consume_quota)
VALUES (1700000000, 91, 7, 'legacy-group', 'legacy-model', 11, 1699920000, 'masked', 3, 12345);
INSERT INTO legacy_facts_guard(id, value) VALUES (1, 'facts-in-wal');`)

	m := &Monitor{cfg: Settings{
		UsageFactsStorePath:           factsPath,
		StoreBackupDir:                backupDir,
		StoreBackupEnabled:            false, // migration snapshots are mandatory regardless of periodic backups
		StoreMigrationBackupRetention: 3,
	}}
	if err := m.openStore(mainPath); err != nil {
		t.Fatalf("legacy migration with WAL snapshot failed: %v", err)
	}
	if !m.storeDB.Migrator().HasColumn(&MetricSample{}, "traffic_class_version") ||
		!m.storeDB.Migrator().HasTable(&ChannelTestHourSample{}) {
		t.Fatal("current main schema was not migrated after the snapshot gate")
	}
	if !m.usageFactsDB.Migrator().HasColumn(&UsageHourFact{}, "refund_records") {
		t.Fatal("current facts schema was not migrated after the snapshot gate")
	}

	snapshots := preMigrationSnapshotDirs(t, backupDir)
	if len(snapshots) != 1 {
		t.Fatalf("expected one atomic pre-migration set, got %v", snapshots)
	}
	manifest, _, err := loadAndVerifyPreMigrationSnapshot(context.Background(), snapshots[0])
	if err != nil {
		t.Fatalf("published snapshot set did not verify: %v", err)
	}
	if len(manifest.Stores) != 2 || !manifest.Stores[0].Present || !manifest.Stores[1].Present {
		t.Fatalf("manifest must contain both existing stores: %+v", manifest.Stores)
	}

	mainSnapshot := openReadOnlyTestStore(t, filepath.Join(snapshots[0], preMigrationMainSnapshotName))
	var mainSentinel string
	if err := mainSnapshot.QueryRow("SELECT value FROM legacy_guard WHERE id=1").Scan(&mainSentinel); err != nil || mainSentinel != "main-in-wal" {
		t.Fatalf("main WAL row missing from snapshot: value=%q err=%v", mainSentinel, err)
	}
	if sqliteHasColumn(t, mainSnapshot, "metric_samples", "traffic_class_version") || sqliteHasTable(t, mainSnapshot, "channel_test_hour_samples") {
		t.Fatal("pre-migration main snapshot unexpectedly contains current schema")
	}
	factsSnapshot := openReadOnlyTestStore(t, filepath.Join(snapshots[0], preMigrationFactsSnapshotName))
	var quota int64
	var factsSentinel string
	if err := factsSnapshot.QueryRow("SELECT consume_quota FROM usage_hour_facts WHERE user_id=91").Scan(&quota); err != nil || quota != 12345 {
		t.Fatalf("facts WAL row missing from snapshot: quota=%d err=%v", quota, err)
	}
	if err := factsSnapshot.QueryRow("SELECT value FROM legacy_facts_guard WHERE id=1").Scan(&factsSentinel); err != nil || factsSentinel != "facts-in-wal" {
		t.Fatalf("facts guard missing from snapshot: value=%q err=%v", factsSentinel, err)
	}
	if sqliteHasColumn(t, factsSnapshot, "usage_hour_facts", "refund_records") {
		t.Fatal("pre-migration facts snapshot unexpectedly contains current schema")
	}

	restoreDir := filepath.Join(dir, "restored-old-volume")
	if err := RestorePreMigrationSnapshot(context.Background(), snapshots[0], restoreDir); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, preMigrationRestoreReadyName)); err != nil {
		t.Fatalf("restore READY marker missing: %v", err)
	}
	restoredMain := openReadOnlyTestStore(t, filepath.Join(restoreDir, filepath.Base(mainPath)))
	restoredFacts := openReadOnlyTestStore(t, filepath.Join(restoreDir, filepath.Base(factsPath)))
	if sqliteHasColumn(t, restoredMain, "metric_samples", "traffic_class_version") ||
		sqliteHasColumn(t, restoredFacts, "usage_hour_facts", "refund_records") {
		t.Fatal("restored volume is not the pre-migration schema")
	}
	if err := RestorePreMigrationSnapshot(context.Background(), snapshots[0], restoreDir); err == nil {
		t.Fatal("restore must never overwrite a non-empty target")
	}

	// Repeating startup/AutoMigrate is safe and reuses the verified original
	// rollback point without duplicating or dropping the legacy rows.
	m.Close()
	_ = mainWriter.Close()
	_ = factsWriter.Close()
	m2 := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath, StoreBackupDir: backupDir, StoreMigrationBackupRetention: 3}}
	if err := m2.openStore(mainPath); err != nil {
		t.Fatalf("second startup must be idempotent: %v", err)
	}
	defer m2.Close()
	var mainRows, factsRows int64
	if err := m2.storeDB.Table("legacy_guard").Count(&mainRows).Error; err != nil || mainRows != 1 {
		t.Fatalf("main rows changed after repeated migration: rows=%d err=%v", mainRows, err)
	}
	if err := m2.usageFactsDB.Table("legacy_facts_guard").Count(&factsRows).Error; err != nil || factsRows != 1 {
		t.Fatalf("facts rows changed after repeated migration: rows=%d err=%v", factsRows, err)
	}
	finalSnapshots := preMigrationSnapshotDirs(t, backupDir)
	if len(finalSnapshots) != 1 || finalSnapshots[0] != snapshots[0] {
		t.Fatalf("same migration plan must keep its original rollback point pinned: before=%v after=%v", snapshots, finalSnapshots)
	}
	for _, snapshot := range finalSnapshots {
		if _, _, err := loadAndVerifyPreMigrationSnapshot(context.Background(), snapshot); err != nil {
			t.Fatalf("idempotent startup produced invalid snapshot %s: %v", filepath.Base(snapshot), err)
		}
	}
	// Reusing a pinned rollback point must not skip validation of the current
	// live pair. Corrupting facts after migration still blocks before main opens.
	m2.Close()
	if err := os.WriteFile(factsPath, []byte("corrupt current facts"), 0o600); err != nil {
		t.Fatal(err)
	}
	m3 := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath, StoreBackupDir: backupDir}}
	if err := m3.openStore(mainPath); err == nil {
		m3.Close()
		t.Fatal("pinned snapshot reuse skipped current facts quick_check")
	}
}

func TestStoreMigrationBackupRetentionHasIndependentBounds(t *testing.T) {
	m := &Monitor{}
	if got := m.storeMigrationBackupRetention(); got != 3 {
		t.Fatalf("default migration retention=%d want=3", got)
	}
	m.cfg.StoreMigrationBackupRetention = 2
	if got := m.storeMigrationBackupRetention(); got != 2 {
		t.Fatalf("explicit migration retention=%d want=2", got)
	}
	m.cfg.StoreMigrationBackupRetention = 100
	if got := m.storeMigrationBackupRetention(); got != 30 {
		t.Fatalf("bounded migration retention=%d want=30", got)
	}
}

func TestPrunePreMigrationSnapshotsKeepsNewestThreeAcrossPlanBumps(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"pre-migrate-20260814T010000.000000000Z-plan1",
		"pre-migrate-20260814T020000.000000000Z-plan2",
		"pre-migrate-20260814T030000.000000000Z-plan3",
		"pre-migrate-20260814T040000.000000000Z-plan4",
	}
	for _, name := range names {
		path := filepath.Join(dir, name)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, preMigrationReadyName), []byte("ready"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// The stable store-pair reference is atomically replaced on every plan bump,
	// so only the newest rollback point is pinned when retention runs.
	reference, err := json.Marshal(preMigrationPlanReference{SnapshotDir: names[3]})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, preMigrationReferencePrefix+"pair"+preMigrationReferenceSuffix), reference, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prunePreMigrationSnapshots(dir, 3); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		_, statErr := os.Stat(filepath.Join(dir, name))
		if i == 0 && !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("oldest snapshot was not pruned: %v", statErr)
		}
		if i > 0 && statErr != nil {
			t.Fatalf("retained snapshot %s missing: %v", name, statErr)
		}
	}
}

func TestPreMigrationSnapshotFailureBlocksBothMigrationsAndCorruptRestore(t *testing.T) {
	t.Run("corrupt pinned reference blocks migration", func(t *testing.T) {
		dir := t.TempDir()
		mainPath := filepath.Join(dir, "monitor.db")
		factsPath := filepath.Join(dir, "usage-facts.db")
		backupDir := filepath.Join(dir, "backups")
		mainWriter := createLegacyWALStore(t, mainPath, legacyMainSchema, "INSERT INTO legacy_guard(id,value) VALUES(1,'safe')")
		factsWriter := createLegacyWALStore(t, factsPath, legacyFactsSchema, "INSERT INTO legacy_facts_guard(id,value) VALUES(1,'safe')")
		defer mainWriter.Close()
		defer factsWriter.Close()
		m := &Monitor{cfg: Settings{StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: backupDir}}
		if _, err := m.createPreMigrationSnapshot(context.Background(), mainPath, factsPath, time.Now()); err != nil {
			t.Fatal(err)
		}
		referencePath := filepath.Join(backupDir, preMigrationReferenceName(mainPath, factsPath))
		if err := os.WriteFile(referencePath, []byte("broken reference"), 0o600); err != nil {
			t.Fatal(err)
		}
		candidate := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath, StoreBackupDir: backupDir}}
		if err := candidate.openStore(mainPath); err == nil {
			candidate.Close()
			t.Fatal("corrupt pinned reference must fail before AutoMigrate")
		}
		mainDB := openReadOnlyTestStore(t, mainPath)
		if sqliteHasColumn(t, mainDB, "metric_samples", "traffic_class_version") {
			t.Fatal("main migrated despite corrupt pinned reference")
		}
	})

	t.Run("unwritable snapshot destination", func(t *testing.T) {
		dir := t.TempDir()
		mainPath := filepath.Join(dir, "monitor.db")
		factsPath := filepath.Join(dir, "usage-facts.db")
		mainWriter := createLegacyWALStore(t, mainPath, legacyMainSchema, "INSERT INTO legacy_guard(id,value) VALUES(1,'safe')")
		factsWriter := createLegacyWALStore(t, factsPath, legacyFactsSchema, "INSERT INTO legacy_facts_guard(id,value) VALUES(1,'safe')")
		defer mainWriter.Close()
		defer factsWriter.Close()
		blockedPath := filepath.Join(dir, "not-a-directory")
		if err := os.WriteFile(blockedPath, []byte("block"), 0o600); err != nil {
			t.Fatal(err)
		}
		m := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath, StoreBackupDir: blockedPath}}
		if err := m.openStore(mainPath); err == nil {
			m.Close()
			t.Fatal("snapshot creation failure must block startup")
		}
		mainDB := openReadOnlyTestStore(t, mainPath)
		factsDB := openReadOnlyTestStore(t, factsPath)
		if sqliteHasColumn(t, mainDB, "metric_samples", "traffic_class_version") ||
			sqliteHasTable(t, mainDB, "channel_test_hour_samples") ||
			sqliteHasColumn(t, factsDB, "usage_hour_facts", "refund_records") {
			t.Fatal("a database migrated even though the paired snapshot gate failed")
		}
	})

	t.Run("corrupt facts source blocks main migration", func(t *testing.T) {
		dir := t.TempDir()
		mainPath := filepath.Join(dir, "monitor.db")
		factsPath := filepath.Join(dir, "usage-facts.db")
		writer := createLegacyWALStore(t, mainPath, legacyMainSchema, "INSERT INTO legacy_guard(id,value) VALUES(1,'safe')")
		defer writer.Close()
		if err := os.WriteFile(factsPath, []byte("not sqlite"), 0o600); err != nil {
			t.Fatal(err)
		}
		m := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath, StoreBackupDir: filepath.Join(dir, "backups")}}
		if err := m.openStore(mainPath); err == nil {
			m.Close()
			t.Fatal("corrupt facts source must block the pair")
		}
		mainDB := openReadOnlyTestStore(t, mainPath)
		if sqliteHasColumn(t, mainDB, "metric_samples", "traffic_class_version") {
			t.Fatal("main migrated despite corrupt facts source")
		}
	})

	t.Run("corrupt published backup is rejected before restore writes", func(t *testing.T) {
		dir := t.TempDir()
		mainPath := filepath.Join(dir, "monitor.db")
		factsPath := filepath.Join(dir, "usage-facts.db")
		mainWriter := createLegacyWALStore(t, mainPath, legacyMainSchema, "INSERT INTO legacy_guard(id,value) VALUES(1,'safe')")
		factsWriter := createLegacyWALStore(t, factsPath, legacyFactsSchema, "INSERT INTO legacy_facts_guard(id,value) VALUES(1,'safe')")
		defer mainWriter.Close()
		defer factsWriter.Close()
		m := &Monitor{cfg: Settings{StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: filepath.Join(dir, "backups")}}
		result, err := m.createPreMigrationSnapshot(context.Background(), mainPath, factsPath, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(result.SnapshotDir, preMigrationFactsSnapshotName), []byte("corrupt backup"), 0o600); err != nil {
			t.Fatal(err)
		}
		candidate := &Monitor{cfg: Settings{UsageFactsStorePath: factsPath, StoreBackupDir: m.cfg.StoreBackupDir}}
		if err := candidate.openStore(mainPath); err == nil {
			candidate.Close()
			t.Fatal("corrupt pinned snapshot must block startup before AutoMigrate")
		}
		mainDB := openReadOnlyTestStore(t, mainPath)
		if sqliteHasColumn(t, mainDB, "metric_samples", "traffic_class_version") {
			t.Fatal("main migrated despite corrupt pinned snapshot")
		}
		target := filepath.Join(dir, "empty-restore")
		if err := RestorePreMigrationSnapshot(context.Background(), result.SnapshotDir, target); err == nil {
			t.Fatal("corrupt backup must be rejected")
		}
		entries, readErr := os.ReadDir(target)
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			t.Fatal(readErr)
		}
		if len(entries) != 0 {
			t.Fatalf("failed restore left published files: %v", entries)
		}
	})
}

func TestRestorePreMigrationSnapshotRejectsSymlinkTarget(t *testing.T) {
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "monitor.db")
	writer := createLegacyWALStore(t, mainPath, legacyMainSchema, "INSERT INTO legacy_guard(id,value) VALUES(1,'safe')")
	defer writer.Close()
	m := &Monitor{cfg: Settings{StorePath: mainPath, StoreBackupDir: filepath.Join(dir, "backups")}}
	result, err := m.createPreMigrationSnapshot(context.Background(), mainPath, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	realTarget := filepath.Join(dir, "real-empty")
	if err := os.Mkdir(realTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	symlinkTarget := filepath.Join(dir, "target-link")
	if err := os.Symlink(realTarget, symlinkTarget); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := RestorePreMigrationSnapshot(context.Background(), result.SnapshotDir, symlinkTarget); err == nil {
		t.Fatal("restore target symlink must be rejected")
	}
}
