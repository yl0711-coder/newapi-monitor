package monitor

import (
	"strings"
	"testing"
)

// The sample tables carry single-column indexes on traffic_class_version, and
// almost every row has the current version. Without sqlite_stat1 (production
// has none) SQLite prefers that equality index over the bucket_ts range index
// and walks the whole table: production 2026-11-17, /data took 1.5–3s even for
// a 15-minute window and the hourly rollup scanned all minute samples. The read
// paths write the version test as `+traffic_class_version = ?` so it is not
// used to choose an index. These plans must keep using the bucket index.
func TestSampleReadsUseBucketIndexNotVersionIndex(t *testing.T) {
	m := newTestMonitor(t)
	cases := []struct {
		name, sql string
		args      []any
		want      string
	}{
		{"dashboard summary", `SELECT COUNT(*) FROM metric_samples WHERE bucket_ts >= ? AND bucket_ts < ?` + currentMetricTrafficFilter, []any{1, 2}, "idx_metric_bucket"},
		{"freshness", `SELECT COALESCE(MAX(bucket_ts),0) AS m FROM metric_samples WHERE +traffic_class_version = ?`, []any{stabilityTrafficClassificationVersion}, "idx_metric_bucket"},
		{"token window", `SELECT COUNT(*) FROM token_samples WHERE bucket_ts >= ? AND bucket_ts < ?` + currentTokenTrafficFilter, []any{1, 2}, "bucket"},
		{"capacity window", `SELECT COUNT(*) FROM capacity_user_minute_samples WHERE bucket_ts >= ? AND bucket_ts < ? AND +traffic_class_version = ? AND user_id >= 0`, []any{1, 2, stabilityTrafficClassificationVersion}, "bucket"},
	}
	for _, tc := range cases {
		var plan []struct {
			ID, Parent, Notused int
			Detail              string
		}
		if err := m.storeDB.Raw("EXPLAIN QUERY PLAN "+tc.sql, tc.args...).Scan(&plan).Error; err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		var details []string
		for _, p := range plan {
			details = append(details, p.Detail)
		}
		joined := strings.Join(details, " | ")
		if strings.Contains(joined, "traffic_class_version") || !strings.Contains(joined, tc.want) {
			t.Fatalf("%s: plan %q, want %s and no version index", tc.name, joined, tc.want)
		}
	}
	// The unary plus must stay in the shared filters used by the dashboard.
	for _, f := range []string{currentMetricTrafficFilter, currentTokenTrafficFilter, currentHourTrafficFilter} {
		if !strings.Contains(f, "+") {
			t.Fatalf("shared traffic filter lost its unary plus: %q", f)
		}
	}
}
