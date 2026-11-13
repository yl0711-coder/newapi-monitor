// Command frt-source-reconcile reads production logs through an allow-listed
// read-only connection and writes independent FRT leaf aggregates.  It is
// deliberately separate from the Monitor HTTP/fact-store path: the result is
// used to prove that FRT recomputation read the production source correctly.
//
// Required environment:
//
//	NEWAPI_LOG_DSN=monitor_ro|nexus_ro:...@tcp(...)/nexusapi?...
//
// Usage:
//
//	go run ./dev/frt-source-reconcile --to 2026-10-21T09:26:00Z --out ./reconcile
//
// The program never prints the DSN or row contents to stdout.  JSON files only
// contain aggregate dimensions and numeric measurements needed by the parity
// script; callers should store them in an access-controlled evidence folder.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/trafficclass"
)

type sourceRow struct {
	BucketTs  int64  `json:"bucket_ts"`
	UserID    int64  `json:"user_id"`
	ChannelID int64  `json:"channel_id"`
	ModelName string `json:"model_name"`
	Group     string `json:"grp"`
	SumMs     int64  `json:"sum_ms"`
	Observed  int64  `json:"observed"`
}

type sourceWindow struct {
	Source string      `json:"source"`
	Window string      `json:"window"`
	FromTs int64       `json:"from_ts"`
	ToTs   int64       `json:"to_ts"`
	Rows   []sourceRow `json:"rows"`
}

var windows = []struct {
	label string
	hours int64
}{
	{label: "24h", hours: 24},
	{label: "3d", hours: 72},
	{label: "7d", hours: 168},
}

// A full-day GROUP BY exceeds the production statement budget. Keep each
// source read bounded, then merge the minute/dimension rows locally. The
// half-open slices are aligned to minutes and therefore cannot double count a
// log at a boundary. Six hours stayed below the 8-second MySQL hint during
// acceptance; the seven-day audit takes 28 sequential reads.
const sourceQuerySliceSeconds int64 = 6 * 60 * 60

type sourceRowKey struct {
	BucketTs  int64
	UserID    int64
	ChannelID int64
	ModelName string
	Group     string
}

func main() {
	var rawTo, outDir string
	flag.StringVar(&rawTo, "to", "", "fixed UTC minute right edge (RFC3339 or Unix seconds)")
	flag.StringVar(&outDir, "out", "", "evidence output directory")
	flag.Parse()
	if strings.TrimSpace(rawTo) == "" || strings.TrimSpace(outDir) == "" {
		fail("--to and --out are required")
	}
	to, err := parseMinute(rawTo)
	if err != nil {
		fail(err.Error())
	}
	dsn := strings.TrimSpace(os.Getenv("NEWAPI_LOG_DSN"))
	readOnlyUser, err := validateReadOnlyDSN(dsn)
	if err != nil {
		fail(err.Error())
	}
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		fail("cannot create output directory")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fail("cannot open read-only source")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fail("read-only source probe failed")
	}
	for _, w := range windows {
		from := to - w.hours*3600
		result, err := queryWindow(ctx, db, w.label, from, to)
		if err != nil {
			fail("source aggregation failed for " + w.label)
		}
		payload, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			fail("cannot encode source aggregate")
		}
		if err := os.WriteFile(filepath.Join(outDir, "source-"+w.label+".json"), append(payload, '\n'), 0o600); err != nil {
			fail("cannot write source aggregate")
		}
	}
	meta := map[string]any{"generated_at": time.Now().UTC().Format(time.RFC3339Nano), "to_ts": to, "source": "production.logs", "read_only_user": readOnlyUser}
	b, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "source-meta.json"), append(b, '\n'), 0o600); err != nil {
		fail("cannot write source metadata")
	}
	fmt.Printf("wrote independent production source FRT aggregates to %s (to=%d)\n", filepath.Clean(outDir), to)
}

func parseMinute(raw string) (int64, error) {
	if n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
		if n > 0 && n%60 == 0 {
			return n, nil
		}
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		return 0, errors.New("--to must be a UTC RFC3339 minute or Unix minute")
	}
	if t.Location() != time.UTC || t.Second() != 0 || t.Nanosecond() != 0 {
		return 0, errors.New("--to must use UTC and an exact minute")
	}
	return t.Unix(), nil
}

func validateReadOnlyDSN(dsn string) (string, error) {
	if dsn == "" {
		return "", errors.New("NEWAPI_LOG_DSN is required")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", errors.New("NEWAPI_LOG_DSN is not a valid MySQL DSN")
	}
	if cfg.User != "monitor_ro" && cfg.User != "nexus_ro" {
		return "", errors.New("NEWAPI_LOG_DSN must use an allow-listed read-only user")
	}
	if cfg.Net != "tcp" || cfg.DBName != "nexusapi" {
		return "", errors.New("NEWAPI_LOG_DSN must be a tcp MySQL DSN for nexusapi")
	}
	return cfg.User, nil
}

func queryWindow(ctx context.Context, db *sql.DB, label string, from, to int64) (sourceWindow, error) {
	// Keep the independent audit logically equivalent to frtRecomputeWindowSQL:
	// it uses production logs, the same FRT JSON field, traffic-class exclusion,
	// five grouping dimensions and a bounded read.  It deliberately omits the
	// recompute query's positive-sample HAVING so zero-sample nodes remain
	// visible to the verifier.
	query := `
SELECT /*+ MAX_EXECUTION_TIME(8000) */ (created_at DIV 60)*60 AS bucket,
       user_id, channel_id, model_name, ` + "`group`" + ` AS grp,
       CAST(COALESCE(SUM(CASE WHEN type=2 AND COALESCE(is_stream,0)=1 AND
           (CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)>0
           THEN (CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END) END),0) AS SIGNED) AS sum_ms,
       CAST(COALESCE(SUM(type=2 AND COALESCE(is_stream,0)=1 AND
           (CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)>0),0) AS SIGNED) AS observed
FROM logs
WHERE created_at >= ? AND created_at < ? AND type IN (2,5,6)
  AND NOT (` + trafficclass.SourceExclusionPredicateSQL + `)
GROUP BY bucket, user_id, channel_id, model_name, grp
ORDER BY bucket, user_id, channel_id, grp, model_name`
	aggregated := make(map[sourceRowKey]sourceRow)
	for cursor := from; cursor < to; {
		next := cursor + sourceQuerySliceSeconds
		if next > to {
			next = to
		}
		rows, err := db.QueryContext(ctx, query, cursor, next)
		if err != nil {
			return sourceWindow{}, err
		}
		for rows.Next() {
			var r sourceRow
			var group sql.NullString
			if err := rows.Scan(&r.BucketTs, &r.UserID, &r.ChannelID, &r.ModelName, &group, &r.SumMs, &r.Observed); err != nil {
				_ = rows.Close()
				return sourceWindow{}, err
			}
			r.Group = group.String
			key := sourceRowKey{BucketTs: r.BucketTs, UserID: r.UserID, ChannelID: r.ChannelID, ModelName: r.ModelName, Group: r.Group}
			if previous, ok := aggregated[key]; ok {
				if previous.SumMs > math.MaxInt64-r.SumMs || previous.Observed > math.MaxInt64-r.Observed {
					_ = rows.Close()
					return sourceWindow{}, errors.New("source aggregate overflow")
				}
				r.SumMs += previous.SumMs
				r.Observed += previous.Observed
			}
			aggregated[key] = r
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return sourceWindow{}, err
		}
		if err := rows.Close(); err != nil {
			return sourceWindow{}, err
		}
		cursor = next
	}
	result := sourceWindow{Source: "production.logs", Window: label, FromTs: from, ToTs: to, Rows: make([]sourceRow, 0, len(aggregated))}
	for _, row := range aggregated {
		result.Rows = append(result.Rows, row)
	}
	sort.Slice(result.Rows, func(i, j int) bool {
		a, b := result.Rows[i], result.Rows[j]
		if a.BucketTs != b.BucketTs {
			return a.BucketTs < b.BucketTs
		}
		if a.UserID != b.UserID {
			return a.UserID < b.UserID
		}
		if a.ChannelID != b.ChannelID {
			return a.ChannelID < b.ChannelID
		}
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		return a.ModelName < b.ModelName
	})
	return result, nil
}

func fail(message string) {
	// Never include driver errors here: MySQL errors can echo the host, schema
	// or connection details. The evidence directory is the only data output.
	fmt.Fprintln(os.Stderr, "frt-source-reconcile: "+message)
	os.Exit(1)
}

// Keep the shared traffic-class package linked so a future predicate change
// cannot be missed during review of this independent source tool.
var _ = trafficclass.SourceExclusionPredicateSQL
