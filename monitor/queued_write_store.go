package monitor

import (
	"log/slog"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// queuedWriteBusyTimeout is how long a queued write waits for SQLite's
// single writer slot (busy_timeout, milliseconds).
const queuedWriteBusyTimeout = "15000"

// queuedWriteStore returns the handle used by background write transactions
// that must not fail on a busy store (upstream usage sync and the monthly bill
// job queue). It is the same
// SQLite file as storeDB, but:
//
//   - transactions BEGIN IMMEDIATE (_txlock=immediate), so they take the
//     writer slot up front and wait for it under busy_timeout. A deferred
//     transaction that reads first and then writes is instead refused at once
//     with SQLITE_BUSY whenever another connection committed meanwhile; that
//     was the most common upstream-sync failure;
//   - one pooled connection, so upstream syncs queue among themselves instead
//     of competing inside SQLite.
//
// Fetched upstream data therefore reaches the store instead of being dropped
// and fetched again next round. In-memory stores (tests) keep storeDB.
func (m *Monitor) queuedWriteStore() *gorm.DB {
	if m.storeDB == nil || !storeUsesFile(m.cfg.StorePath) {
		return m.storeDB
	}
	m.queuedWriteMu.Lock()
	defer m.queuedWriteMu.Unlock()
	if m.queuedWriteDB != nil {
		return m.queuedWriteDB
	}
	if m.queuedWriteFailed {
		return m.storeDB
	}
	dsn := m.cfg.StorePath + "?_pragma=busy_timeout(" + queuedWriteBusyTimeout + ")&_pragma=journal_mode(WAL)&_pragma=temp_store(2)&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err == nil {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			sqlDB.SetMaxOpenConns(1)
			sqlDB.SetMaxIdleConns(1)
		} else {
			err = dbErr
		}
	}
	if err != nil {
		m.queuedWriteFailed = true
		slog.Warn("后台排队写连接打开失败，回退到主连接", "err", err)
		return m.storeDB
	}
	m.queuedWriteDB = db
	return db
}

func (m *Monitor) closeQueuedWriteStore() {
	m.queuedWriteMu.Lock()
	defer m.queuedWriteMu.Unlock()
	if m.queuedWriteDB != nil {
		if sqlDB, err := m.queuedWriteDB.DB(); err == nil {
			_ = sqlDB.Close()
		}
		m.queuedWriteDB = nil
	}
}
