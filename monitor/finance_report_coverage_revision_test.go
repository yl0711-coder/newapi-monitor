package monitor

import (
	"context"
	"testing"
)

func TestFinanceReportFingerprintTracksHourlyProofCorrections(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "revision.example")
	defer m.Close()
	ctx := context.Background()
	from := int64(1_777_516_800)
	to := from + 2*3600
	rows := []StabilityHourIngestState{
		{HourTs: from, Status: "complete", Rows: 1, Requests: 1, Tokens: 10, Quota: 100, UpdatedAt: to, TrafficClassVersion: stabilityTrafficClassificationVersion},
		{HourTs: from + 3600, Status: "complete", Rows: 2, Requests: 2, Tokens: 20, Quota: 200, UpdatedAt: to, TrafficClassVersion: stabilityTrafficClassificationVersion},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	for _, period := range []bool{false, true} {
		t.Run(map[bool]string{false: "report", true: "month"}[period], func(t *testing.T) {
			fingerprint := func() string {
				t.Helper()
				value, err := m.financeReportSourceFingerprintForScope(ctx, from, to, !period)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			update := func(hour int64, values map[string]any) {
				t.Helper()
				// GORM otherwise auto-updates UpdatedAt, hiding a same-second collision.
				if _, ok := values["updated_at"]; !ok {
					values["updated_at"] = to
				}
				if err := m.storeDB.Model(&StabilityHourIngestState{}).Where("hour_ts = ?", hour).Updates(values).Error; err != nil {
					t.Fatal(err)
				}
			}
			for _, change := range []struct {
				name   string
				values map[string]any
			}{
				{"coverage", map[string]any{"status": "failed"}},
				{"classification", map[string]any{"traffic_class_version": stabilityTrafficClassificationVersion - 1}},
				{"internal tokens", map[string]any{"internal_test_tokens": 5}},
				{"publication timestamp", map[string]any{"updated_at": to + 1}},
			} {
				before := fingerprint()
				update(from, change.values)
				if fingerprint() == before {
					t.Errorf("%s correction within same second did not invalidate cache", change.name)
				}
				update(from, map[string]any{"status": "complete", "traffic_class_version": stabilityTrafficClassificationVersion, "internal_test_tokens": 0, "updated_at": to})
			}
			before := fingerprint()
			// The interval total stays identical; the per-day/hour attribution changes.
			update(from, map[string]any{"quota": 200})
			update(from+3600, map[string]any{"quota": 100})
			if fingerprint() == before {
				t.Error("same-total redistribution did not invalidate cache")
			}
			update(from, map[string]any{"quota": 100})
			update(from+3600, map[string]any{"quota": 200})
			if fingerprint() != before {
				t.Error("restoring identical publication changed fingerprint")
			}
		})
	}
}

func TestFinanceFingerprintTracksZeroHourContradiction(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "zero.example")
	defer m.Close()
	ctx := context.Background()
	from := int64(1_777_516_800)
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: from, Status: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	before, err := m.financeReportPeriodSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&MetricSample{BucketTs: from, ChannelID: 1, ModelName: "test", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportPeriodSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("new positive minute invalidates zero-hour coverage but not its cache")
	}
}

func TestFinanceFingerprintToleratesLegacyNullProofColumns(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "legacy-null.example")
	defer m.Close()
	from := int64(1_777_516_800)
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: from, Status: "complete", Requests: 1}).Error; err != nil {
		t.Fatal(err)
	}
	before, err := m.financeReportSourceFingerprint(context.Background(), from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`UPDATE stability_hour_ingest_states SET status=NULL,traffic_class_version=NULL,
		internal_test_rows=NULL,internal_test_requests=NULL,internal_test_tokens=NULL,internal_test_quota=NULL WHERE hour_ts=?`, from).Error; err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportSourceFingerprint(context.Background(), from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("unknown legacy coverage reused valid proof")
	}
}
