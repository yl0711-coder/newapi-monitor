// Package financegiftexport performs bounded, explicit, read-only evidence
// exports. It has no scheduler, credentials, connection setup or HTTP endpoint.
package financegiftexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/trafficclass"
)

const giftScopeExportLimit = 100
const giftScopeExplicitExportLimit = 3000
const giftScopeMaxEstimatedRows = 10000
const giftScopeExportBytes = 2 << 20

type giftScopeExportRow struct {
	ID        int64  `json:"id"`
	UserID    int64  `json:"user_id"`
	CreatedAt int64  `json:"created_at"`
	Type      int    `json:"type"`
	Quota     int64  `json:"quota"`
	Group     string `json:"group"`
}

func giftScopeExportSQL() string {
	return giftScopeExportSQLWithLimit(giftScopeExportLimit)
}

func giftScopeExportSQLWithLimit(limit int) string {
	return fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,user_id,created_at,type,quota,COALESCE(`group`,'') FROM logs WHERE user_id=? AND created_at>=? AND created_at<? AND type IN (2,6) AND NOT (%s) ORDER BY created_at,id LIMIT %d", trafficclass.SourceExclusionPredicateSQL, limit+1)
}

// Reject scans before executing the data SELECT. EXPLAIN is not EXPLAIN ANALYZE.
func validateGiftScopePlan(rows *sql.Rows) error {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		plan := make(map[string]string, len(columns))
		for i, name := range columns {
			plan[name] = values[i].String
		}
		var estimate int64
		if _, err := fmt.Sscan(plan["rows"], &estimate); err != nil {
			return errors.New("missing query row estimate")
		}
		if plan["table"] != "logs" || plan["key"] == "" || (plan["type"] != "ref" && plan["type"] != "range" && plan["type"] != "const") || estimate < 0 || estimate > giftScopeMaxEstimatedRows {
			return errors.New("gift scope query requires a selective indexed plan; no data query executed")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 1 {
		return errors.New("unexpected gift scope query plan")
	}
	return nil
}

// Export writes complete evidence to a new private file. A positive expected
// count is an explicit bounded request, not a new default.
// It must come from the existing complete local user-hour ledger. There is no
// automatic retry or fallback to a wider query if the proof does not match.
func Export(parent context.Context, db *sql.DB, user, hour int64, path string, expected int) error {
	limit := giftScopeExportLimit
	if expected < 0 || expected > giftScopeExplicitExportLimit {
		return errors.New("explicit gift scope expected rows must be 1 to 3000; omitted/zero keeps the 100-row default")
	}
	if expected > 0 {
		limit = expected
	}
	if user <= 0 || hour <= 0 || hour%3600 != 0 || hour > time.Now().Unix()-3600 || !filepath.IsAbs(path) {
		return errors.New("require one user, one closed hour and an absolute new output path")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return errors.New("output already exists or cannot be checked")
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := giftScopeExportSQLWithLimit(limit)
	args := []any{user, hour, hour + 3600}
	plan, err := tx.QueryContext(ctx, "EXPLAIN "+query, args...)
	if err != nil {
		return err
	}
	if err := validateGiftScopePlan(plan); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := make([]giftScopeExportRow, 0)
	seen := make(map[int64]bool)
	groupBytes := 0
	for rows.Next() {
		var item giftScopeExportRow
		if err := rows.Scan(&item.ID, &item.UserID, &item.CreatedAt, &item.Type, &item.Quota, &item.Group); err != nil {
			return err
		}
		if item.ID <= 0 || item.UserID != user || item.CreatedAt < hour || item.CreatedAt >= hour+3600 || (item.Type != 2 && item.Type != 6) || item.Quota < 0 {
			return errors.New("invalid source row")
		}
		groupBytes += len(item.Group)
		if groupBytes > giftScopeExportBytes {
			return errors.New("source exceeds evidence byte budget")
		}
		if seen[item.ID] || (len(items) > 0 && (item.CreatedAt < items[len(items)-1].CreatedAt ||
			(item.CreatedAt == items[len(items)-1].CreatedAt && item.ID <= items[len(items)-1].ID))) {
			return errors.New("source contains duplicate or unordered evidence")
		}
		seen[item.ID] = true
		items = append(items, item)
		if len(items) > limit {
			return errors.New("source exceeds explicit read bound; no partial export")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(items) == 0 || (expected > 0 && len(items) != expected) {
		return errors.New("source row count differs from local proof; no partial export")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(struct {
		UserID int64                `json:"user_id"`
		Hour   int64                `json:"hour_ts"`
		Rows   []giftScopeExportRow `json:"rows"`
	}{user, hour, items}, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > giftScopeExportBytes {
		return errors.New("evidence exceeds offline file byte budget; no export")
	}
	if err := publishGiftScopeEvidence(path, data); err != nil {
		return err
	}
	fmt.Printf("gift scope evidence: indexed read-only query; %d rows; saved to private local file\n", len(items))
	return nil
}

// Publish a complete private file without ever replacing an existing target.
// The temporary file is in the same directory, so a hard link is an atomic
// no-clobber publication. Only our own temporary path is removed on failure.
func publishGiftScopeEvidence(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".gift-scope-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Link(f.Name(), path); err != nil {
		return err
	}
	if err := os.Remove(f.Name()); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
