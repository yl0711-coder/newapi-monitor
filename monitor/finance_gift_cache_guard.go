package monitor

import (
	"context"
	"database/sql"
	"strconv"
	"sync"

	"gorm.io/gorm"
)

// data_version is connection-local. Keep ONE dedicated read-only connection
// which never writes, so all commits (including repairs without a proof update)
// change its version. This is an idle local SQLite handle, not a long-running
// read transaction and not another production connection. On any error or an
// in-memory store, disable reuse rather than weaken monetary verification.
type financeGiftCacheGuard struct {
	mu                sync.Mutex
	pool              *sql.DB
	conn              *sql.Conn
	source            *gorm.DB
	attempted, closed bool
}

func (g *financeGiftCacheGuard) version(ctx context.Context, source *gorm.DB) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || source == nil || ctx.Err() != nil {
		return "", false
	}
	if !g.attempted {
		g.attempted = true
		var databases []struct{ Name, File string }
		if err := source.WithContext(ctx).Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
			return "", false
		}
		path := ""
		for _, db := range databases {
			if db.Name == "main" {
				path = db.File
			}
		}
		if path == "" {
			return "", false
		}
		pool, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
		if err != nil {
			return "", false
		}
		pool.SetMaxOpenConns(1)
		conn, err := pool.Conn(ctx)
		if err != nil {
			_ = pool.Close()
			return "", false
		}
		g.pool, g.conn, g.source = pool, conn, source
	}
	if g.conn == nil || g.source != source {
		return "", false
	}
	var version int64
	if err := g.conn.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version); err != nil {
		return "", false
	}
	return strconv.FormatInt(version, 10), true
}

func (g *financeGiftCacheGuard) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	if g.conn != nil {
		_ = g.conn.Close()
		g.conn = nil
	}
	if g.pool != nil {
		_ = g.pool.Close()
		g.pool = nil
	}
}
