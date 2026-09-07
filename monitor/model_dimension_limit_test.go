package monitor

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestModelDimensionLimitIsExplicitAndKeepsFullTotals(t *testing.T) {
	for _, count := range []int{199, 200, 201, 205} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			m := newTestMonitor(t)
			defer m.Close()
			const now = int64(1_800_000_000)
			end := metricFinalizeTarget(now)
			rows := make([]MetricSample, count)
			for i := range rows {
				rows[i] = MetricSample{BucketTs: end - 60, ChannelID: i + 1,
					Grp: fmt.Sprintf("g%03d", i), ModelName: fmt.Sprintf("m%03d", i),
					Success: 1, Tokens: 100, Quota: int64(i + 1)}
			}
			if err := m.upsertSamples(rows); err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: end - 7200, NextTs: end, SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
				t.Fatal(err)
			}
			for _, observed := range []bool{false, true} {
				s, err := m.getSnapshotView(120, now, observed)
				if err != nil {
					t.Fatal(err)
				}
				// JSON metadata is the frontend contract, including truncated=false
				// at exactly 200 rows (not a guess based on len == limit).
				encoded, err := json.Marshal(s)
				if err != nil {
					t.Fatal(err)
				}
				var decoded Snapshot
				if err := json.Unmarshal(encoded, &decoded); err != nil {
					t.Fatal(err)
				}
				for dim, got := range map[string][]Row{"group": s.ByGroup, "model": s.ByModel, "channel": s.ByChannel} {
					meta, ok := decoded.DimensionLimits[dim]
					if !ok || meta.Limit != modelDimensionRowLimit || meta.Truncated != (count > modelDimensionRowLimit) || len(got) != min(count, modelDimensionRowLimit) {
						t.Fatalf("%s rows=%d metadata=%+v", dim, len(got), meta)
					}
					if got[0].CostUSD <= got[len(got)-1].CostUSD {
						t.Fatalf("%s is no longer cost ordered", dim)
					}
				}
				var trendTotal int64
				for _, p := range s.Trend {
					trendTotal += p.Success + p.Anomaly + p.Failed
				}
				if s.Summary.Total != int64(count) || s.Summary.Tokens != int64(count)*100 || trendTotal != int64(count) {
					t.Fatalf("full totals were truncated: summary=%+v trend=%d", s.Summary, trendTotal)
				}
			}
			operational, err := m.storeDimRange(channelDim, end-3600, end, 3600)
			if err != nil || len(operational) != min(count, modelDimensionRowLimit) {
				t.Fatalf("operational row budget changed: rows=%d err=%v", len(operational), err)
			}
		})
	}
}
