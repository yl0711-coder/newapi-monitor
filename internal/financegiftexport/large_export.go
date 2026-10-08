package financegiftexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/trafficclass"
)

// LargeHourEvidence is deliberately distinct from ordinary batch evidence.
// It proves the pinned ledger's records, NOT a new whole-source snapshot or
// the absence of late source inserts. The importer must recheck that ledger.
type LargeHourEvidence struct {
	Version          int                  `json:"version"`
	PlanSHA256       string               `json:"plan_sha256"`
	SourceEpoch      string               `json:"source_epoch"`
	LocalContentHash string               `json:"local_content_hash"`
	UserID           int64                `json:"user_id"`
	HourTs           int64                `json:"hour_ts"`
	Rows             []giftScopeExportRow `json:"rows"`
}

// ExportLargeHour has no runtime/CLI wiring, connection setup or retry loop.
// It releases each readonly transaction before the next cooldown; incomplete
// evidence is never published. Original 3000-row batch APIs remain unchanged.
func ExportLargeHour(ctx context.Context, db *sql.DB, plan LargeHourPlan, confirmation, path string) error {
	return exportLargeHour(ctx, db, plan, confirmation, path, waitBatch)
}

// The operator-facing source command requires identity verification for every
// page, including after a connection loss. The connection is still readonly.
func ExportVerifiedLargeHour(ctx context.Context, db *sql.DB, plan LargeHourPlan, confirmation, path string, identity SourceIdentity) error {
	if identity.Database == "" || identity.ServerUUID == "" {
		return errors.New("explicit source identity required")
	}
	return exportLargeHourWithIdentity(ctx, db, plan, confirmation, path, waitBatch, &identity)
}

func exportLargeHour(parent context.Context, db *sql.DB, plan LargeHourPlan, confirmation, path string, wait func(context.Context, time.Duration) error) error {
	return exportLargeHourWithIdentity(parent, db, plan, confirmation, path, wait, nil)
}

func exportLargeHourWithIdentity(parent context.Context, db *sql.DB, plan LargeHourPlan, confirmation, path string, wait func(context.Context, time.Duration) error, identity *SourceIdentity) error {
	// Reject oversized input before cloning caller-owned memory.
	if len(plan.Rows) > LargeHourMaxRows {
		return errors.New("large-hour row budget exceeded")
	}
	plan = cloneLargeHourPlan(plan)
	digest, err := LargeHourConfirmation(plan)
	if err != nil {
		return err
	}
	if db == nil || digest != confirmation || !filepath.IsAbs(path) {
		return errors.New("confirmed large-hour plan, source connection and absolute new output required")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return errors.New("output already exists or cannot be checked")
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	result := LargeHourEvidence{Version: 1, PlanSHA256: digest, SourceEpoch: plan.SourceEpoch, LocalContentHash: plan.LocalContentHash, UserID: plan.UserID, HourTs: plan.HourTs}
	groupBytes := 0
	for start := 0; start < len(plan.Rows); start += largeHourPageRows {
		if err := wait(ctx, batchInterval); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := readLargeHourPage(ctx, db, plan, plan.Rows[start:min(start+largeHourPageRows, len(plan.Rows))], identity)
		if err != nil {
			return fmt.Errorf("large-hour page %d rejected: %w", start/largeHourPageRows+1, err)
		}
		for _, row := range page {
			groupBytes += len(row.Group)
		}
		if groupBytes > giftScopeExportBytes {
			return errors.New("large-hour evidence exceeds total byte budget")
		}
		result.Rows = append(result.Rows, page...)
	}
	data, err := json.Marshal(result)
	if err != nil || len(data) > giftScopeExportBytes {
		return errors.New("large-hour evidence exceeds encoded byte budget")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return publishGiftScopeEvidence(path, data)
}

func readLargeHourPage(parent context.Context, db *sql.DB, plan LargeHourPlan, expected []LargeHourRecord, identity *SourceIdentity) ([]giftScopeExportRow, error) {
	if len(expected) == 0 || len(expected) > largeHourPageRows {
		return nil, errors.New("invalid large-hour page size")
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }() // Best-effort cleanup; a committed transaction is already closed.
	if identity != nil {
		if err := VerifySourceIdentity(ctx, tx, *identity); err != nil {
			return nil, err
		}
	}
	args := []any{plan.UserID, plan.HourTs, plan.HourTs + 3600}
	for _, row := range expected {
		args = append(args, row.ID)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(expected)), ",")
	query := fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,user_id,created_at,type,quota,`group` FROM logs WHERE user_id=? AND created_at>=? AND created_at<? AND id IN (%s) AND type IN (2,6) AND NOT (%s) ORDER BY id LIMIT %d", placeholders, trafficclass.SourceExclusionPredicateSQL, len(expected)+1)
	explain, err := tx.QueryContext(ctx, "EXPLAIN "+query, args...)
	if err != nil {
		return nil, err
	}
	if err := validateGiftScopePlan(explain); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []giftScopeExportRow
	bytes := 0
	for rows.Next() {
		var item giftScopeExportRow
		if err := rows.Scan(&item.ID, &item.UserID, &item.CreatedAt, &item.Type, &item.Quota, &item.Group); err != nil {
			return nil, err // NULL group is not verified evidence.
		}
		if len(items) >= len(expected) {
			return nil, errors.New("source exceeded exact page identity set")
		}
		want := expected[len(items)]
		if item.ID != want.ID || item.UserID != plan.UserID || item.CreatedAt != want.CreatedAt || item.Type != want.Type || item.Quota != want.Quota || (want.Group != nil && item.Group != *want.Group) {
			return nil, errors.New("source differs from pinned monetary identity or known group")
		}
		bytes += len(item.Group)
		if bytes > giftScopeExportBytes {
			return nil, errors.New("source page exceeds byte budget")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(items) != len(expected) {
		return nil, errors.New("source page missing pinned records")
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return items, nil
}
