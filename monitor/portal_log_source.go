package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

type portalLogColumnSet struct {
	ip, upstreamID bool
	unavailable    bool
}

type portalLogColumnCache struct {
	sync.Mutex
	db      *sql.DB
	expires time.Time
	columns portalLogColumnSet
}

// Metadata only, never table contents or COUNT(*). Discovery is cached so old
// tables need no migration and an in-place NewAPI upgrade is detected later.
func (m *Monitor) portalLogOptionalColumns(ctx context.Context) portalLogColumnSet {
	c := &m.portalLogColumns
	c.Lock()
	defer c.Unlock()
	if c.db == m.prodDB && time.Now().Before(c.expires) {
		return c.columns
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	columns, err := readPortalLogColumns(ctx, m.prodDB)
	ttl := 5 * time.Minute
	if err != nil {
		columns = portalLogColumnSet{unavailable: true}
		ttl = 30 * time.Second
	}
	c.db, c.columns, c.expires = m.prodDB, columns, time.Now().Add(ttl)
	return columns
}

func readPortalLogColumns(ctx context.Context, db *sql.DB) (portalLogColumnSet, error) {
	var result portalLogColumnSet
	if db == nil {
		return result, fmt.Errorf("source not connected")
	}
	mysql := strings.Contains(strings.ToLower(fmt.Sprintf("%T", db.Driver())), "mysql")
	query := "PRAGMA table_info(logs)"
	if mysql {
		query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'logs' AND COLUMN_NAME IN ('ip','upstream_request_id')"
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if mysql {
			err = rows.Scan(&name)
		} else {
			var cid, notNull, primary int
			var typ string
			var defaultValue any
			err = rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primary)
		}
		if err != nil {
			return result, err
		}
		switch name {
		case "ip":
			result.ip = true
		case "upstream_request_id":
			result.upstreamID = true
		}
	}
	return result, rows.Err()
}

func (s portalLogColumnSet) selectSQL() string {
	ip, upstream := "''", "''"
	if s.ip {
		ip = "COALESCE(ip,'')"
	}
	if s.upstreamID {
		upstream = "COALESCE(upstream_request_id,'')"
	}
	return ", " + ip + ", " + upstream
}
