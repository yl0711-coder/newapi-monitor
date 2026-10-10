package monitor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCustomerHealthWatermarkUsableAcrossMinuteRefreshAndRecovery(t *testing.T) {
	for _, independent := range []bool{false, true} {
		name := "source_worker"
		if independent {
			name = "logchain_only"
		}
		t.Run(name, func(t *testing.T) {
			m := newTestMonitor(t)
			defer m.Close()
			now := chTestNow()
			from, through := customerHealthSourceRange(now)
			chSeedCompleteTodaySource(t, m, now)
			m.cfg.CustomerHealthSourceEnabled = independent
			m.customerHealthSourceRunning.Store(true)
			chSeedCompany(t, m, "连续公司", 101)
			if err := m.storeDB.Create(&[]CapacityUserMinuteSample{
				{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "known", Grp: "g", Success: 9, Failed: 1, CustomerHealthFailed: 1},
				// A real row is already present after the certificate. It must not
				// affect counts, stability, primary model or attribution yet.
				{BucketTs: through, UserID: 101, ChannelID: 7, ModelName: "tail", Grp: "g", Failed: 90, CustomerHealthFailed: 90},
			}).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Create(&StabilityProblemSample{
				BucketTs: through, Source: "newapi", ChannelID: 7, ModelName: "known", Grp: "g",
				Code: "500", Message: "upstream unavailable", Count: 90,
			}).Error; err != nil {
				t.Fatal(err)
			}
			for _, offset := range []time.Duration{0, time.Minute, time.Minute, 2 * time.Minute} {
				// Zero the volatile watermark: refresh/reentry/restart must rely
				// on the same durable proof, not a browser or in-memory latch.
				m.customerHealthSourceFrom.Store(0)
				m.customerHealthSourceThrough.Store(0)
				report, err := m.buildCustomerHealthReport(context.Background(), now.Add(offset))
				if err != nil {
					t.Fatal(err)
				}
				row := report.Rows[0]
				if !row.MetricsReady || !report.Collection.MetricsAvailable || !report.Collection.CoverageComplete ||
					report.ToTs != through || row.Total != 10 || row.StabilityPct == nil || *row.StabilityPct != 90 ||
					len(row.PrimaryModels) != 1 || row.PrimaryModels[0].Name != "known" || row.Fault != faultUnknown {
					t.Fatalf("usable prefix or its common window changed on refresh: offset=%v report=%+v row=%+v", offset, report, row)
				}
				if report.Collection.Ready != (offset == 0) || !strings.Contains(row.MetricsNote, "尾部同步中") {
					t.Fatalf("caught-up and available were conflated: %+v", report.Collection)
				}
			}
			if err := m.saveCustomerHealthSourceCursor(from, through+120); err != nil {
				t.Fatal(err)
			}
			report, err := m.buildCustomerHealthReport(context.Background(), now.Add(2*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if !report.Collection.Ready || !report.Rows[0].MetricsReady || report.Rows[0].Total != 100 || report.ToTs != through+120 {
				t.Fatalf("recovery did not advance the common report window: %+v", report)
			}
		})
	}
}

func TestCustomerHealthWatermarkUnusableReasons(t *testing.T) {
	from, target := customerHealthSourceRange(chTestNow())
	for _, tc := range []struct {
		name   string
		cursor *CustomerHealthSourceCursor
		want   string
	}{
		{"first_run", nil, "initial_backfill"},
		{"no_complete_minute", &CustomerHealthSourceCursor{ID: 1, DayTs: from, ThroughTs: from, SemanticsVersion: customerHealthStabilityPolicyVersion}, "initial_backfill"},
		{"prefix_does_not_start_at_midnight", &CustomerHealthSourceCursor{ID: 1, DayTs: from + 60, ThroughTs: target, SemanticsVersion: customerHealthStabilityPolicyVersion}, "coverage_gap"},
		{"invalid_right_edge", &CustomerHealthSourceCursor{ID: 1, DayTs: from, ThroughTs: from - 60, SemanticsVersion: customerHealthStabilityPolicyVersion}, "coverage_gap"},
		{"old_semantics", &CustomerHealthSourceCursor{ID: 1, DayTs: from, ThroughTs: target, SemanticsVersion: customerHealthStabilityPolicyVersion - 1}, "policy_backfill"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestMonitor(t)
			defer m.Close()
			m.cfg.CapacityEnabled = true
			m.sourceWorkerRunning.Store(true)
			m.lastRun.Store(chTestNow().Unix())
			chSeedCompany(t, m, "不可用公司", 101)
			if tc.cursor != nil {
				if err := m.storeDB.Save(tc.cursor).Error; err != nil {
					t.Fatal(err)
				}
			}
			report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
			if err != nil {
				t.Fatal(err)
			}
			if report.Collection.State != tc.want || report.Collection.MetricsAvailable || report.Collection.Ready || report.Collection.CoverageComplete ||
				report.Rows[0].MetricsReady || report.Rows[0].StabilityPct != nil || report.ToTs != from {
				t.Fatalf("missing proof must not be interpreted as zero or usable data: %+v", report)
			}
		})
	}
}

func TestCustomerHealthWatermarkRejectsOldClassificationInProvenWindow(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	chSeedCompleteTodaySource(t, m, chTestNow())
	chSeedCompany(t, m, "旧分类公司", 101)
	from, _ := customerHealthSourceRange(chTestNow())
	row := CapacityUserMinuteSample{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "old", Grp: "g", Success: 8}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ?", row.BucketTs).Update("traffic_class_version", 0).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if report.Collection.State != "policy_backfill" || report.Collection.CoverageComplete || report.Rows[0].MetricsReady {
		t.Fatalf("filtered old rows must not fabricate zero current facts: %+v", report)
	}
}

func TestCustomerHealthWatermarkCapsClockRollbackAndShowsSourceFailure(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	chSeedCompleteTodaySource(t, m, chTestNow())
	// GORM's update callbacks use wall time for UpdatedAt. Keep this fixture
	// on the same fixed clock as its simulated failure and report target.
	if err := m.storeDB.Model(&CustomerHealthSourceCursor{}).Where("id = ?", 1).
		UpdateColumn("updated_at", chTestNow().Unix()).Error; err != nil {
		t.Fatal(err)
	}
	from, target := customerHealthSourceRange(chTestNow())
	if got := m.customerHealthCollectionStatus(from, target-60); !got.MetricsAvailable || got.ThroughTs != target-60 {
		t.Fatalf("time rollback escaped this report's target: %+v", got)
	}
	m.sourceLastFailureAt.Store(chTestNow().Unix() + 1)
	got := m.customerHealthCollectionStatus(from, target)
	if !got.MetricsAvailable || got.State != "source_failed" || !strings.Contains(got.Note, "采集失败") || strings.Contains(got.Note, "尾部同步中") {
		t.Fatalf("source failure must remain distinct from ordinary tail sync: %+v", got)
	}
	m.sourceLastFailureAt.Store(0)
	m.sourceWorkerRunning.Store(false)
	got = m.customerHealthCollectionStatus(from, target)
	if !got.MetricsAvailable || got.State != "source_paused" || !strings.Contains(got.Note, "暂停") {
		t.Fatalf("paused: %+v", got)
	}
	store := m.storeDB
	m.storeDB = nil
	got = m.customerHealthCollectionStatus(from, target)
	m.storeDB = store
	if got.MetricsAvailable || got.State != "source_unavailable" {
		t.Fatalf("unreadable proof: %+v", got)
	}
	if err := m.storeDB.Model(&CustomerHealthSourceCursor{}).Where("id = ?", 1).
		UpdateColumns(map[string]any{"through_ts": from, "updated_at": chTestNow().Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	m.sourceLastFailureAt.Store(chTestNow().Unix() + 1)
	got = m.customerHealthCollectionStatus(from, target)
	if got.MetricsAvailable || got.State != "source_unavailable" || !strings.Contains(got.Note, "采集失败") {
		t.Fatalf("failed collection with no usable prefix is unavailable: %+v", got)
	}
}
