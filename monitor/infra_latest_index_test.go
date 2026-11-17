package monitor

import (
	"context"
	"strings"
	"testing"
)

// The latest-per-key infra read must stay index-only (no full scan + temp
// sort of the 7-day table) and still return the newest row of every key.
func TestInfraLatestUsesKeyIndexAndReturnsNewestPerKey(t *testing.T) {
	m := newTestMonitor(t)
	var rows []InfraSample
	for b := int64(0); b < 50; b++ {
		for _, k := range [][3]string{{"db", "aws", "cpu"}, {"db", "host", "cpu"}, {"web", "aws", "mem"}} {
			rows = append(rows, InfraSample{BucketTs: 1000 + b*300, Resource: k[0], RType: k[1], Metric: k[2], Value: float64(b)})
		}
	}
	// A key whose newest sample is old must still be returned (shown stale).
	rows = append(rows, InfraSample{BucketTs: 500, Resource: "old", RType: "aws", Metric: "cpu", Value: 7})
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	got, err := m.storeInfraLatestContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("latest rows = %d, want 4: %+v", len(got), got)
	}
	for _, r := range got {
		want := int64(1000 + 49*300)
		if r.Resource == "old" {
			want = 500
		}
		if r.BucketTs != want {
			t.Fatalf("%s/%s/%s newest = %d, want %d", r.Resource, r.RType, r.Metric, r.BucketTs, want)
		}
	}
	var plan []struct {
		ID, Parent, Notused int
		Detail              string
	}
	if err := m.storeDB.Raw(`EXPLAIN QUERY PLAN SELECT resource, rtype, metric, MAX(bucket_ts) AS mx FROM infra_samples GROUP BY resource, rtype, metric`).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	var details []string
	for _, p := range plan {
		details = append(details, p.Detail)
	}
	joined := strings.Join(details, " | ")
	if !strings.Contains(joined, "idx_infra_key_bucket") || strings.Contains(joined, "TEMP B-TREE") {
		t.Fatalf("latest-per-key aggregate is not served by idx_infra_key_bucket: %s", joined)
	}
}
