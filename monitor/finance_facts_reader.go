package monitor

import (
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// financeFactsReadStore keeps long finance scans off the facts writer's
// single-connection pool. A second SQLite handle is safe for concurrent reads
// only when the already-opened facts database is a separate WAL file. It is
// strictly read-only, capped at one connection, and opt-in for gray acceptance.
// In-memory tests and shared-main legacy stores retain the original path.
func (m *Monitor) financeFactsReadStore() *gorm.DB {
	if m == nil {
		return nil
	}
	writer := m.usageFactsStore()
	if !m.cfg.FinanceFactsReadIsolationEnabled || writer == nil || writer == m.storeDB {
		return writer
	}
	path := strings.TrimSpace(m.cfg.UsageFactsStorePath)
	if !storeUsesFile(path) || sameStorePath(path, m.cfg.StorePath) {
		return writer
	}
	m.financeFactsReadOnce.Do(func() {
		abs, err := filepath.Abs(path)
		if err != nil {
			slog.Warn("经营核算事实只读连接未启用，沿用原连接", "err", err)
			return
		}
		readDB, err := gorm.Open(sqlite.Open(sqliteReadOnlyDSN(abs)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			slog.Warn("经营核算事实只读连接未启用，沿用原连接", "err", err)
			return
		}
		pool, err := readDB.DB()
		if err != nil {
			slog.Warn("经营核算事实只读连接未启用，沿用原连接", "err", err)
			return
		}
		pool.SetMaxOpenConns(1)
		pool.SetMaxIdleConns(1)
		var mode string
		if err := pool.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
			_ = pool.Close()
			slog.Warn("经营核算事实只读连接要求 WAL，沿用原连接", "mode", mode, "err", err)
			return
		}
		m.financeFactsReadDB.Store(readDB)
	})
	if readDB := m.financeFactsReadDB.Load(); readDB != nil {
		return readDB
	}
	return writer
}
