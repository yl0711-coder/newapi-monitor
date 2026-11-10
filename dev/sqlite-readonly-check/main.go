// sqlite-readonly-check verifies stopped SQLite stores without migrations,
// checkpoints, repairs, VACUUM or an application/source connection.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	budget := flag.Duration("budget", 5*time.Minute, "maximum read time per database")
	immutableStopped := flag.Bool("immutable-stopped", false, "read a stopped store with no WAL or journal, without creating shared-memory files")
	flag.Parse()
	if flag.NArg() == 0 || *budget <= 0 || *budget > 10*time.Minute {
		fmt.Fprintln(os.Stderr, "provide database paths and a budget up to 10 minutes")
		os.Exit(2)
	}
	for _, name := range flag.Args() {
		if err := check(name, *budget, *immutableStopped); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func check(name string, budget time.Duration, immutableStopped bool) error {
	start := time.Now()
	if !filepath.IsAbs(name) {
		return fmt.Errorf("absolute database path required")
	}
	u := &url.URL{Scheme: "file", Path: filepath.ToSlash(name)}
	q := u.Query()
	q.Set("mode", "ro")
	if immutableStopped {
		// The caller must have stopped all writers. Immutable mode ignores WAL
		// recovery, so refuse any existing journal, even an empty one.
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Stat(name + suffix); !os.IsNotExist(err) {
				return fmt.Errorf("immutable check requires absent %s sidecar", suffix)
			}
		}
		q.Set("immutable", "1")
	}
	q.Add("_pragma", "query_only(1)")
	q.Add("_pragma", "busy_timeout(1000)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check(1)").Scan(&result); err != nil {
		return fmt.Errorf("%s quick_check: %w", filepath.Base(name), err)
	}
	if result != "ok" {
		return fmt.Errorf("%s quick_check: %s", filepath.Base(name), result)
	}
	pragmas := map[string]int64{}
	for _, key := range []string{"page_count", "page_size", "freelist_count"} {
		var value int64
		if err := db.QueryRowContext(ctx, "PRAGMA "+key).Scan(&value); err != nil {
			return err
		}
		pragmas[key] = value
	}
	record := map[string]any{"database": name, "read_only": true, "immutable_stopped": immutableStopped, "quick_check": result, "elapsed_ms": time.Since(start).Milliseconds(), "pragmas": pragmas}
	if filepath.Base(name) == "token.db" {
		rows, err := db.QueryContext(ctx, "SELECT source_epoch,start_at,scan_through,complete_through,fact_count,fact_bytes,migration_cursor FROM token_metering_states ORDER BY source_epoch LIMIT 100")
		if err != nil {
			return err
		}
		defer rows.Close()
		states := []map[string]any{}
		for rows.Next() {
			var epoch string
			var from, scan, complete, count, bytes, migration int64
			if err := rows.Scan(&epoch, &from, &scan, &complete, &count, &bytes, &migration); err != nil {
				return err
			}
			states = append(states, map[string]any{"source_epoch": epoch, "start_at": from, "scan_through": scan, "complete_through": complete, "fact_count": count, "fact_bytes": bytes, "migration_cursor": migration})
		}
		if err := rows.Err(); err != nil {
			return err
		}
		record["states"] = states
	}
	return json.NewEncoder(os.Stdout).Encode(record)
}
