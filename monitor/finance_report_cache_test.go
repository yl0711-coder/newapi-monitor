package monitor

import (
	"context"
	"testing"
	"time"
)

func TestFinanceReportCacheKeySeparatesRangesAndSnapshots(t *testing.T) {
	base := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0)}
	cases := []financeReportRequest{
		base,
		{from: base.from, to: time.Unix(10800, 0)},
		{from: base.from, to: base.to, snapshotAsOf: 7200},
		{from: base.from, to: base.to, snapshotAsOf: 7200, snapshotClamped: true},
	}
	seen := make(map[string]struct{}, len(cases))
	for _, request := range cases {
		key := request.cacheKey()
		if _, exists := seen[key]; exists {
			t.Fatalf("finance report cache key collision: %q", key)
		}
		seen[key] = struct{}{}
	}
}

func TestFinanceReportPayloadUsesBoundedCachedJSON(t *testing.T) {
	m := &Monitor{}
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0)}
	want := []byte(`{"enabled":true,"generated_at":123}`)
	m.getFinanceReportCache().PutWithStale(request.cacheKey(), want, time.Minute, time.Minute, time.Now())

	got, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil {
		t.Fatal(err)
	}
	if status != "hit" || string(got) != string(want) {
		t.Fatalf("cached finance response mismatch: status=%q payload=%s", status, got)
	}
	entries, bytes := m.getFinanceReportCache().size()
	if entries != 1 || bytes != len(want) {
		t.Fatalf("unexpected bounded cache size: entries=%d bytes=%d", entries, bytes)
	}
}

func TestFinanceReportPayloadMissHitAndForcedRefresh(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "cache.example")
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, loc)
	request := financeReportRequest{from: from, to: from.Add(time.Hour)}

	first, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil || status != "miss" || len(first) == 0 {
		t.Fatalf("first finance report build failed: status=%q bytes=%d err=%v", status, len(first), err)
	}
	second, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil || status != "hit" || string(second) != string(first) {
		t.Fatalf("finance report cache hit mismatch: status=%q bytes=%d err=%v", status, len(second), err)
	}
	refreshed, status, err := m.financeReportPayload(context.Background(), request, true)
	if err != nil || status != "refresh" || len(refreshed) == 0 {
		t.Fatalf("forced finance report refresh failed: status=%q bytes=%d err=%v", status, len(refreshed), err)
	}
}
