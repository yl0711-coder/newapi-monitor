package monitor

import (
	"database/sql"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// usageFactsInteractiveReadStore serves the user-usage page aggregates (matrix,
// stats, token rollups) and local profile snapshots from a second, strictly
// read-only SQLite handle. The facts writer pool is capped at one connection so
// background imports and audits keep commits ordered; without a separate handle
// an interactive aggregate queues behind those writers and their long scans.
// WAL lets this handle read the latest committed data concurrently. Publication
// fences, readiness and membership reads stay on the writer handle. Same opt-in
// and fallback rules as financeFactsReadStore.
func (m *Monitor) usageFactsInteractiveReadStore() *gorm.DB {
	if m == nil {
		return nil
	}
	writer := m.usageFactsStore()
	if !m.cfg.UsageFactsReadIsolationEnabled || writer == nil || writer == m.storeDB {
		return writer
	}
	path := strings.TrimSpace(m.cfg.UsageFactsStorePath)
	if !storeUsesFile(path) || sameStorePath(path, m.cfg.StorePath) {
		return writer
	}
	m.usageFactsReadOnce.Do(func() {
		abs, err := filepath.Abs(path)
		if err != nil {
			slog.Warn("用户用量只读连接未启用，沿用原连接", "err", err)
			return
		}
		readDB, err := gorm.Open(sqlite.Open(sqliteReadOnlyDSN(abs)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			slog.Warn("用户用量只读连接未启用，沿用原连接", "err", err)
			return
		}
		pool, err := readDB.DB()
		if err != nil {
			slog.Warn("用户用量只读连接未启用，沿用原连接", "err", err)
			return
		}
		// Interactive aggregates are already bounded by usageFactsReadCapacity.
		pool.SetMaxOpenConns(usageFactsReadCapacity)
		pool.SetMaxIdleConns(usageFactsReadCapacity)
		var mode string
		if err := pool.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
			_ = pool.Close()
			slog.Warn("用户用量只读连接要求 WAL，沿用原连接", "mode", mode, "err", err)
			return
		}
		m.usageFactsReadDB.Store(readDB)
	})
	if readDB := m.usageFactsReadDB.Load(); readDB != nil {
		return readDB
	}
	return writer
}

const usageReadDiagnosticThreshold = 2 * time.Second

// usageReadTimingDiagnostic logs slow user-usage page requests with aggregate
// pool counters only (no SQL parameters, user IDs or amounts), so a slow page
// can be attributed to waiting for the facts writer connection, the read-only
// handle, the interactive read slot, or the query itself.
func (m *Monitor) usageReadTimingDiagnostic(route string, next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		writerBefore := financeDBPoolStats(m.usageFactsDB)
		readerBefore := financeDBPoolStats(m.usageFactsReadDB.Load())
		budgetBefore := m.usageFactsReadBudgetStats()
		next(c)
		elapsed := time.Since(started)
		if elapsed < usageReadDiagnosticThreshold {
			return
		}
		writerAfter := financeDBPoolStats(m.usageFactsDB)
		readerAfter := financeDBPoolStats(m.usageFactsReadDB.Load())
		budgetAfter := m.usageFactsReadBudgetStats()
		slog.Warn("用户用量读取耗时诊断",
			"route", route, "status", c.Writer.Status(), "elapsed_ms", elapsed.Milliseconds(),
			"facts_pool_wait_count", poolWaitCount(writerBefore, writerAfter),
			"facts_pool_wait_ms", poolWaitMS(writerBefore, writerAfter),
			"usage_read_pool_wait_count", poolWaitCount(readerBefore, readerAfter),
			"usage_read_pool_wait_ms", poolWaitMS(readerBefore, readerAfter),
			"usage_read_pool_enabled", m.usageFactsReadDB.Load() != nil,
			"read_slot_wait_ms", budgetAfter.TotalWaitMS-budgetBefore.TotalWaitMS,
			"read_slot_run_ms", budgetAfter.TotalRunMS-budgetBefore.TotalRunMS)
	}
}

func poolWaitCount(before, after sql.DBStats) int64 { return after.WaitCount - before.WaitCount }

func poolWaitMS(before, after sql.DBStats) int64 {
	return (after.WaitDuration - before.WaitDuration).Milliseconds()
}
