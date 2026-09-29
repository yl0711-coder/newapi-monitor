package monitor

import (
	"context"
	"errors"
	"testing"
)

func TestFinanceGiftCachedReadAllowsUnrelatedPublication(t *testing.T) {
	for _, target := range []string{"unrelated-table", "outside-window"} {
		t.Run(target, func(t *testing.T) {
			m, _ := giftEvidenceCacheFixture(t)
			ctx := context.Background()
			if _, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 7200); err != nil {
				t.Fatal(err)
			}
			before, _ := m.getFinanceGiftEvidenceCache().size()
			var rows []FinanceGiftBoundaryState
			if err := m.usageFactsStore().Where("hour_ts < ?", 7200).Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			states := make(map[financeGiftUserHourKey]FinanceGiftBoundaryState)
			for _, row := range rows {
				states[financeGiftUserHourKey{HourTs: row.HourTs, UserID: row.UserID}] = row
			}
			changed := false
			err := m.walkFinanceGiftHourEvidence(ctx, m.financeFactsReadStore(), states, nil, nil, func(_ financeGiftUserHourKey, _ financeGiftHourEvidence) {
				if changed {
					return
				}
				changed = true
				if target == "outside-window" {
					publishFinanceGiftTestHour(t, m, 7200, 7, "epoch", 1_000_000, 7300)
				} else if err := m.usageFactsStore().Exec("CREATE TABLE unrelated_writer_test (id INTEGER)").Error; err != nil {
					t.Fatal(err)
				}
			})
			if err != nil || !changed {
				t.Fatalf("unrelated commit rejected: %v", err)
			}
			r, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 7200)
			if err != nil || r.Allocation.PeriodGiftConsumptionMicroUSD != 2_000_000 {
				t.Fatalf("money changed: %+v %v", r, err)
			}
			after, _ := m.getFinanceGiftEvidenceCache().size()
			if after != before {
				t.Fatalf("DB version duplicated cache entries: %d -> %d", before, after)
			}
		})
	}
}

func TestFinanceGiftCacheDetectsUnpublishedEditBetweenReads(t *testing.T) {
	m, _ := giftEvidenceCacheFixture(t)
	ctx := context.Background()
	if _, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 10800); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Exec("UPDATE finance_gift_boundary_events SET quota=1 WHERE source_log_id=100").Error; err != nil {
		t.Fatal(err)
	}
	_, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 10800)
	if !errors.Is(err, errFinanceFactsChanged) {
		t.Fatalf("unpublished mutation hidden: %v", err)
	}
	var cause *financeFactChangeError
	if !errors.As(err, &cause) || cause.Stage != "gift-evidence" {
		t.Fatalf("missing source attribution: %v", err)
	}
}
