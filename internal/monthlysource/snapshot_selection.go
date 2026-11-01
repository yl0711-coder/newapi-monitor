package monthlysource

// Local cold-backup preparation only. Extract original snapshot rows without
// opening a source writable, inventing observations or asserting a month cutoff.
import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const RetainedSnapshotSelectionMode = "original_snapshot_table_projection_not_new_capture_or_month_boundary"
const retainedSnapshotRowsLimit = 10000
const retainedSnapshotBytesLimit int64 = 1 << 30

var ErrSnapshotSelection = errors.New("invalid, changing or unsealed retained snapshot source")

// Null stays null, including unknown existence/capture time. There are no
// editable opening/closing balances, approval flags or inferred user names.
type RetainedSnapshotRow struct {
	UserID       int64  `json:"user_id"`
	BalanceQuota *int64 `json:"balance_quota"`
	UsedQuota    *int64 `json:"used_quota"`
	Exists       *int64 `json:"exists"`
	CapturedAt   *int64 `json:"captured_at"`
}

type RetainedSnapshotSelection struct {
	Version      int                   `json:"version"`
	Mode         string                `json:"mode"`
	OriginSHA256 string                `json:"origin_sha256"`
	OriginBytes  int64                 `json:"origin_bytes"`
	Table        string                `json:"table"`
	Columns      []string              `json:"columns"`
	Rows         []RetainedSnapshotRow `json:"rows"`
}

// ReadRetainedSnapshotSelection projects ALL rows of the fixed original table.
// The caller owns an explicit query-only SQLite handle and keeps the cold
// backup. Only private derived output may be written by the caller. This
// function performs fixed SELECT/PRAGMA reads, no initialization or repair.
// The before/after whole-file digests and seal checks make this a traceable
// preparation artifact, NOT cryptographic attestation of financial semantics.
func ReadRetainedSnapshotSelection(parent context.Context, db *sql.DB, expectedSHA256 string) (out RetainedSnapshotSelection, err error) {
	if parent == nil || db == nil || !validDigest(expectedSHA256) {
		return out, ErrSnapshotSelection
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			err = errors.Join(err, e)
		}
		if err != nil {
			out = RetainedSnapshotSelection{}
		}
	}()
	var queryOnly int
	if tx.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly) != nil || queryOnly != 1 {
		return out, ErrSnapshotSelection
	}
	var file string
	if tx.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&file) != nil || !filepath.IsAbs(file) {
		return out, ErrSnapshotSelection
	}
	before, err := retainedSnapshotSeal(file)
	if err != nil {
		return out, err
	}
	if err := retainedSnapshotDigest(ctx, file, expectedSHA256); err != nil {
		return out, err
	}
	var kind, schema string
	if tx.QueryRowContext(ctx, "SELECT type,sql FROM main.sqlite_master WHERE name='usage_user_snapshots'").Scan(&kind, &schema) != nil ||
		kind != "table" || !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(schema)), "CREATE TABLE") {
		return out, ErrSnapshotSelection
	}
	rows, err := tx.QueryContext(ctx, `SELECT user_id,balance_quota,used_quota,"exists",captured_at,
typeof(user_id),typeof(balance_quota),typeof(used_quota),typeof("exists"),typeof(captured_at)
FROM main.usage_user_snapshots ORDER BY user_id LIMIT 10001`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	selected := []RetainedSnapshotRow{}
	var previous int64
	for rows.Next() {
		if len(selected) >= retainedSnapshotRowsLimit {
			return out, ErrSnapshotSelection
		}
		var id int64
		var balance, used, exists, captured sql.NullInt64
		var types [5]string
		if err := rows.Scan(&id, &balance, &used, &exists, &captured, &types[0], &types[1], &types[2], &types[3], &types[4]); err != nil {
			return out, err
		}
		if id <= previous || types[0] != "integer" {
			return out, ErrSnapshotSelection
		}
		for _, typ := range types[1:] {
			if typ != "integer" && typ != "null" {
				return out, ErrSnapshotSelection // Never round/coerce source money.
			}
		}
		if exists.Valid && exists.Int64 != 0 && exists.Int64 != 1 {
			return out, ErrSnapshotSelection
		}
		pointer := func(n sql.NullInt64) *int64 {
			if !n.Valid {
				return nil
			}
			v := n.Int64
			return &v
		}
		selected = append(selected, RetainedSnapshotRow{UserID: id, BalanceQuota: pointer(balance), UsedQuota: pointer(used), Exists: pointer(exists), CapturedAt: pointer(captured)})
		previous = id
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if err := rows.Close(); err != nil {
		return out, err
	}
	if err := retainedSnapshotDigest(ctx, file, expectedSHA256); err != nil {
		return out, err
	}
	after, err := retainedSnapshotSeal(file)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return out, ErrSnapshotSelection
	}
	return RetainedSnapshotSelection{Version: 1, Mode: RetainedSnapshotSelectionMode, OriginSHA256: expectedSHA256, OriginBytes: before.Size(),
		Table: "usage_user_snapshots", Columns: []string{"user_id", "balance_quota", "used_quota", "exists", "captured_at"}, Rows: selected}, ctx.Err()
}

func retainedSnapshotSeal(file string) (os.FileInfo, error) {
	actual, err := filepath.EvalSymlinks(file)
	if err != nil || actual != file || !filepath.IsAbs(file) || filepath.Clean(file) != file {
		return nil, ErrSnapshotSelection
	}
	info, err := os.Lstat(file)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > retainedSnapshotBytesLimit {
		return nil, ErrSnapshotSelection
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(file + suffix); !os.IsNotExist(err) {
			return nil, ErrSnapshotSelection
		}
	}
	return info, nil
}

func retainedSnapshotDigest(ctx context.Context, file, expected string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	buffer := make([]byte, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := f.Read(buffer)
		if n > 0 {
			_, _ = h.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return ErrSnapshotSelection
	}
	return nil
}
