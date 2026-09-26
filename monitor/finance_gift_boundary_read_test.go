package monitor

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestFinanceGiftBoundaryStreamMatchesMaterializedRead(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "gift-stream.example")
	db := m.usageFactsStore()
	var events []FinanceGiftBoundaryEvent
	users := make([]int64, financeGiftUserQueryChunk+1)
	for i := range users {
		users[i] = int64(i + 1)
		for j := 0; j < 2; j++ {
			e := FinanceGiftBoundaryEvent{SourceEpoch: "old", SourceLogID: int64(i*2 + j + 1),
				HourTs: 3600, UserID: users[i], EventAt: int64(3700 - j), Kind: "usage",
				Quota: int64(i + 1), Group: "customer", GroupKnown: true}
			if j == 1 {
				e.SourceEpoch, e.Kind, e.Group, e.GroupKnown = "current", "refund", "", false
			}
			e.EvidenceHash = financeGiftBoundaryEventHash(e)
			events = append(events, e)
		}
	}
	// A reader must preserve the exact half-open interval and requested users.
	events = append(events, []FinanceGiftBoundaryEvent{
		{SourceEpoch: "extra", SourceLogID: 10001, HourTs: 0, UserID: 1},
		{SourceEpoch: "extra", SourceLogID: 10002, HourTs: 7200, UserID: 1},
		{SourceEpoch: "extra", SourceLogID: 10003, HourTs: 3600, UserID: 9999},
	}...)
	if err := db.CreateInBatches(events, 50).Error; err != nil {
		t.Fatal(err)
	}
	// Legacy nullable columns decode to their Go zero values, as with GORM Find.
	if err := db.Model(&FinanceGiftBoundaryEvent{}).Where("source_log_id=1").Updates(map[string]any{
		"event_at": nil, "kind": nil, "quota": nil, "grp": nil, "group_known": nil, "evidence_hash": nil,
	}).Error; err != nil {
		t.Fatal(err)
	}
	var want, got []FinanceGiftBoundaryEvent
	for start := 0; start < len(users); start += financeGiftUserQueryChunk {
		var chunk []FinanceGiftBoundaryEvent
		if err := db.Where("hour_ts>=? AND hour_ts<? AND user_id IN ?", 3600, 7200, users[start:min(len(users), start+financeGiftUserQueryChunk)]).Find(&chunk).Error; err != nil {
			t.Fatal(err)
		}
		want = append(want, chunk...)
	}
	counter := &financeAcceptanceSQLCounter{Interface: logger.Default.LogMode(logger.Silent)}
	err := walkFinanceGiftBoundaryEvents(context.Background(), db.Session(&gorm.Session{Logger: counter}), 3600, 7200, users, func(event FinanceGiftBoundaryEvent) error {
		got = append(got, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sortEvents := func(events []FinanceGiftBoundaryEvent) {
		sort.Slice(events, func(i, j int) bool { return events[i].SourceLogID < events[j].SourceLogID })
	}
	sortEvents(want)
	sortEvents(got)
	if len(got) != len(users)*2 || !reflect.DeepEqual(got, want) || counter.queries.Load() != 2 {
		t.Fatalf("stream changed fields, boundaries or chunk count: rows=%d queries=%d", len(got), counter.queries.Load())
	}
}

func TestFinanceGiftBoundaryStreamReleasesSingleConnectionOnFailure(t *testing.T) {
	for _, scenario := range []string{"visitor", "cancel", "scan"} {
		t.Run(scenario, func(t *testing.T) {
			m := newFinanceReportTestMonitor(t, "gift-cursor.example")
			db := m.usageFactsStore()
			pool, err := db.DB()
			if err != nil {
				t.Fatal(err)
			}
			pool.SetMaxOpenConns(1)
			for id := int64(1); id <= 3; id++ {
				if err := db.Create(&FinanceGiftBoundaryEvent{SourceEpoch: "v1", SourceLogID: id, HourTs: 3600, UserID: 7, Quota: 1}).Error; err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "scan" {
				if err := db.Model(&FinanceGiftBoundaryEvent{}).Where("source_log_id=2").Update("quota", "invalid").Error; err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stop := errors.New("visitor stopped")
			seen := 0
			err = walkFinanceGiftBoundaryEvents(ctx, db, 3600, 7200, []int64{7}, func(FinanceGiftBoundaryEvent) error {
				seen++
				if scenario == "visitor" {
					return stop
				}
				if scenario == "cancel" {
					cancel()
				}
				return nil
			})
			if err == nil || (scenario == "visitor" && !errors.Is(err, stop)) || (scenario == "cancel" && !errors.Is(err, context.Canceled)) {
				t.Fatalf("failure was swallowed: scenario=%s err=%v", scenario, err)
			}
			if (scenario == "visitor" || scenario == "cancel") && seen != 1 {
				t.Fatalf("read continued after interruption: %d", seen)
			}
			checkCtx, checkCancel := context.WithTimeout(context.Background(), time.Second)
			defer checkCancel()
			if err := db.WithContext(checkCtx).Exec("UPDATE finance_gift_boundary_events SET quota=quota WHERE source_log_id=1").Error; err != nil {
				t.Fatalf("reader leaked the only connection: %v", err)
			}
		})
	}
}

func TestFinanceGiftBoundaryStreamDoesNotPublishChangedEvidence(t *testing.T) {
	for _, scenario := range []string{"missing", "quota", "hash", "state"} {
		t.Run(scenario, func(t *testing.T) {
			m := newFinanceReportTestMonitor(t, "gift-integrity.example")
			db, ctx := m.usageFactsStore(), context.Background()
			publishFinanceGiftTestHour(t, m, 3600, 7, "v1", 500_000, 2)
			grant := FinanceCreditEvent{SourceLogID: 1, EventAt: 3650, TargetUserID: 7, UserCreatedAt: 1,
				Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000,
				Cohort: "trial_candidate_within_24h", EligibleTrial: true}
			grant.EvidenceHash = financeCreditEventHash(grant)
			if _, err := replaceFinanceCreditHour(ctx, db, 3600, 20_000, "v1", financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}); err != nil {
				t.Fatal(err)
			}
			before, err := m.loadFinanceGiftAllocation(ctx, 3600, 3600, 7200)
			if err != nil || !before.Coverage.Complete || before.Allocation.PeriodGiftConsumptionMicroUSD != 1_000_000 {
				t.Fatal("fixture must first publish verified gift consumption")
			}
			var mutation *gorm.DB
			switch scenario {
			case "missing":
				mutation = db.Where("source_epoch=? AND source_log_id=?", "v1", 2).Delete(&FinanceGiftBoundaryEvent{})
			case "quota":
				mutation = db.Model(&FinanceGiftBoundaryEvent{}).Where("source_log_id=2").Update("quota", 1)
			case "hash":
				mutation = db.Model(&FinanceGiftBoundaryEvent{}).Where("source_log_id=2").Update("evidence_hash", "bad")
			case "state":
				mutation = db.Model(&FinanceGiftBoundaryState{}).Where("user_id=7").Update("content_hash", "bad")
			}
			if mutation.Error != nil {
				t.Fatal(mutation.Error)
			}
			after, err := m.loadFinanceGiftAllocation(ctx, 3600, 3600, 7200)
			if err == nil || after.Coverage.Complete || after.verifiedPrefix != nil {
				t.Fatal("streaming accepted altered or missing monetary evidence")
			}
		})
	}
}
