package monitor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// The shape is the SQL result, not source log volume. The large fixture makes
// the cost of materializing the grouped rows observable without disk writes.
const modelStatisticsTestRows = `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < ?)
	SELECT CASE WHEN i%11 = 0 THEN NULL ELSE ' group ' END AS grp, 'model' AS model,
	 i%500 AS user_id, CASE WHEN i%13 = 0 THEN NULL ELSE i%60 END AS channel_id,
	 CASE WHEN i%7 = 0 THEN NULL ELSE 'user-' || i%500 END AS username,
	 3 AS routed_requests, i%2 AS unavailable_routes,
	 0 AS ttft_500, 1 AS ttft_1k, 2 AS ttft_2k, 0 AS ttft_5k, 0 AS ttft_10k, 0 AS ttft_inf,
	 1800 AS ttft_max_ms, 3 AS ttft_observed, 0 AS ttft_over_3s FROM n`

func legacyModelStatisticsRows(query *gorm.DB) (map[string]map[string]*modelStatisticsAggregate, map[int64]string, error) {
	var rows []modelStatisticsRow
	if err := query.Scan(&rows).Error; err != nil {
		return nil, nil, err
	}
	groups := make(map[string]map[string]*modelStatisticsAggregate)
	usernames := make(map[int64]string)
	for _, row := range rows {
		addModelStatisticsRow(groups, usernames, row)
	}
	return groups, usernames, nil
}

func TestModelStatisticsStreamingMatchesHistoricalScan(t *testing.T) {
	db := openModelStatisticsFixture(t)
	legacyGroups, legacyNames, err := legacyModelStatisticsRows(db.Raw(modelStatisticsTestRows, 600))
	if err != nil {
		t.Fatal(err)
	}
	groups, names, err := readModelStatisticsRows(context.Background(), db.Raw(modelStatisticsTestRows, 600))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(groups, legacyGroups) || !reflect.DeepEqual(names, legacyNames) {
		t.Fatal("streaming scan changed grouped counts, histograms, or NULL dimension handling")
	}
}

func TestModelStatisticsStreamingStopsSQLiteOnDeadline(t *testing.T) {
	db := openModelStatisticsFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	const slow = `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 8000000) SELECT SUM(i) FROM n`
	// A blocked aggregation yields no rows; cancellation must interrupt SQLite,
	// not just the Go loop that would otherwise collect its results.
	watchdog := make(chan error, 1)
	go func() {
		_, _, err := readModelStatisticsRows(ctx, db.Raw(slow))
		watchdog <- err
	}()
	select {
	case err := <-watchdog:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected SQLite read to stop on deadline, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SQLite kept running after the request deadline")
	}
	var one int
	if err := db.Raw("SELECT 1").Scan(&one).Error; err != nil || one != 1 {
		t.Fatalf("connection did not recover after cancel: %d %v", one, err)
	}
}

func BenchmarkModelStatisticsGroupedRead(b *testing.B) {
	for _, kind := range []string{"legacy", "streaming"} {
		b.Run(kind, func(b *testing.B) {
			db := openModelStatisticsFixture(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := db.Raw(modelStatisticsTestRows, 60000)
				var err error
				if kind == "legacy" {
					_, _, err = legacyModelStatisticsRows(query)
				} else {
					_, _, err = readModelStatisticsRows(context.Background(), query)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func openModelStatisticsFixture(t testing.TB) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = pool.Close() })
	return db
}
