package monitor

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Keep the retained query/index working set in the SQLite connection cache.
// The default roughly two-MiB cache repeatedly rereads the same pages through
// the disk bind during concurrent collection, detection and report queries.
// One writer plus at most two readers budgets 384 MiB of database pages, filled
// lazily; WAL snapshot isolation and the existing query/write limits are intact.
// This stays inside the 640 MiB Go heap target and the 1536 MiB scale container.
const tokenStoreCacheKiB = 128 * 1024

func tokenStoreModels() []any {
	return []any{&TokenMeteringFact{}, &TokenMeteringRef{}, &TokenMeteringState{}, &TokenMeteringWindow{}, &TokenMeteringLaneState{},
		&TokenDetectionState{}, &TokenDetectionRetry{}, &TokenDetectionBucket{}, &TokenDetectionCurrent{}, &TokenDetectionRevision{}, &TokenDetectionReference{},
		&TokenEvidenceExport{}, &TokenDeliveryAudit{}, &TokenNotifyOutbox{}, &tokenNotifyTarget{}, &tokenDeliveryState{}}
}

// An explicit store never falls back to the main database on failure. This
// keeps high-volume Token writes out of Monitor/Usage and preserves old data.
func (m *Monitor) tokenStore() *gorm.DB {
	if strings.TrimSpace(m.cfg.TokenStorePath) == "" {
		return m.storeDB
	}
	return m.tokenDB
}

// Keep API snapshots and queue selection off the single writer connection.
// Legacy shared/in-memory stores retain their existing connection. Explicit
// isolated stores are opened with their reader together, before workers start.
func (m *Monitor) tokenReadStore() *gorm.DB {
	if m.tokenStore() == nil {
		return nil
	}
	if m.tokenReadDB != nil {
		return m.tokenReadDB
	}
	return m.tokenStore()
}

func (m *Monitor) tokenStorePath() string {
	if path := strings.TrimSpace(m.cfg.TokenStorePath); path != "" {
		return path
	}
	return m.cfg.StorePath
}

func (m *Monitor) openTokenStore() error {
	path := strings.TrimSpace(m.cfg.TokenStorePath)
	if path == "" {
		return nil
	}
	if !storeUsesFile(path) || strings.ContainsAny(path, "?\x00") || sameStorePath(path, m.cfg.StorePath) ||
		sameStorePath(path, m.cfg.UsageFactsStorePath) || sameStorePath(path, m.cfg.NginxEvidenceStorePath) {
		return errors.New("token store requires a separate file path")
	}
	// Migration of existing evidence needs its own reviewed backup/copy plan.
	// Never silently start an empty history beside populated legacy tables.
	for _, model := range tokenStoreModels() {
		if !m.storeDB.Migrator().HasTable(model) {
			continue
		}
		var count int64
		if err := m.storeDB.Model(model).Select("1").Limit(1).Scan(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return errors.New("token main-store history exists; explicit migration is required")
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// Long JSON fact migrations and detection revisions are bounded, but a
	// reader can still hold a WAL snapshot while the single writer commits.
	// Five seconds lets the durable write lane wait for that snapshot instead of
	// returning SQLITE_BUSY, which used to be surfaced as local_detection_incomplete.
	cachePragma := fmt.Sprintf("cache_size(-%d)", tokenStoreCacheKiB)
	// Reuse bounded writer statements: compiling the same trigger-heavy fact
	// and head updates for every row otherwise spends the local write budget
	// repeatedly. Values remain bound parameters and the reader pool retains
	// its independent read-only snapshots.
	db, err := gorm.Open(sqlite.Open(path+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=temp_store(2)&_pragma="+cachePragma), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent), PrepareStmt: true, PrepareStmtMaxSize: 128, PrepareStmtTTL: 10 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("open token store: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	ready := false
	defer func() {
		if !ready {
			if statements, ok := db.ConnPool.(*gorm.PreparedStmtDB); ok {
				statements.Close()
			}
			_ = sqlDB.Close()
		}
	}()
	if err := db.AutoMigrate(tokenStoreModels()...); err != nil {
		return fmt.Errorf("migrate token store: %w", err)
	}
	if err := initTokenMeteringLinkIndex(db); err != nil {
		return fmt.Errorf("initialize token association index: %w", err)
	}
	// Counter backfill parses every retained fact and can take minutes on the
	// production-sized store. Create the schema/triggers here so writes remain
	// safe, then rebuild counters from Start in a bounded background lane.
	if err := ensureTokenMeteringHealthCountsSchema(db); err != nil {
		return fmt.Errorf("initialize token health counters: %w", err)
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		return fmt.Errorf("initialize token report index: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	u := &url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	q := u.Query()
	q.Set("mode", "ro")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "query_only(1)")
	q.Add("_pragma", "temp_store(2)")
	q.Add("_pragma", cachePragma)
	u.RawQuery = q.Encode()
	reader, err := gorm.Open(sqlite.Open(u.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return fmt.Errorf("open token reader: %w", err)
	}
	readPool, err := reader.DB()
	if err != nil {
		return err
	}
	// One reader remains available to foreground health/detail requests.
	readPool.SetMaxOpenConns(m.cfg.tokenDetectionWorkers() + 1)
	readPool.SetMaxIdleConns(m.cfg.tokenDetectionWorkers() + 1)
	// Reopening a stopped store (including recovery tests) closes its old reader.
	if m.tokenReadDB != nil {
		if old, err := m.tokenReadDB.DB(); err == nil {
			_ = old.Close()
		}
	}
	m.tokenDB = db
	m.tokenReadDB = reader
	ready = true
	return nil
}
