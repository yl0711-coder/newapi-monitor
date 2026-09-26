//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"gorm.io/gorm"
)

// Synthetic, hand-calculated test cases, never production evidence. User 7
// mixes excluded test usage with business usage and next-hour refunds. User 8
// has an independent gift wallet and may be excluded as an internal account.
func giftMixedLocalFixture(t *testing.T) (backup string, paths []string, hour int64) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "mixed-gift.example")
	db, ctx := m.usageFactsStore(), context.Background()
	hour, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		id, user, offset, usd int64
		kind                  int
		group                 string
	}
	rows := []row{
		{11, 7, 20, 60, 2, "test"}, {12, 7, 30, 100, 2, "business"}, {13, 7, 40, 10, 6, "business"},
		{14, 8, 20, 20, 2, "business"},
		{21, 7, 3610, 30, 6, "test"}, {22, 7, 3620, 50, 2, "business"},
		{23, 8, 3610, 5, 6, "business"}, {24, 8, 3620, 10, 2, "business"},
	}
	dir := t.TempDir()
	for h := hour; h < hour+7200; h += 3600 {
		var facts []FinanceUserHourFact
		byUser := map[int64][]FinanceGiftBoundaryEvent{}
		for _, user := range []int64{7, 8} {
			fact := FinanceUserHourFact{HourTs: h, UserID: user}
			var exported []map[string]any
			for _, r := range rows {
				at := hour + r.offset
				if r.user != user || at < h || at >= h+3600 {
					continue
				}
				quota := r.usd * int64(quotaPerUSD)
				event := FinanceGiftBoundaryEvent{SourceLogID: r.id, HourTs: h, UserID: user, EventAt: at, Kind: "usage", Quota: quota}
				if r.kind == 6 {
					event.Kind = "refund"
					fact.RefundRecords++
					fact.RefundQuota += quota
				} else {
					fact.Requests++
					fact.ConsumeQuota += quota
				}
				event.EvidenceHash = financeGiftBoundaryEventHash(event)
				byUser[user] = append(byUser[user], event)
				exported = append(exported, map[string]any{"id": r.id, "user_id": user, "created_at": at, "type": r.kind, "quota": quota, "group": r.group})
			}
			facts = append(facts, fact)
			data, err := json.Marshal(map[string]any{"user_id": user, "hour_ts": h, "rows": exported})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, fmt.Sprintf("synthetic-%d-%d.json", h, user))
			if err = giftLocalWriteNew(path, data); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, path)
		}
		if _, err = replaceFinanceUserHourFacts(ctx, db, h, "v1", facts, hour+10800); err != nil {
			t.Fatal(err)
		}
		var grants []FinanceCreditEvent
		if h == hour {
			for _, user := range []int64{7, 8} {
				g := FinanceCreditEvent{SourceLogID: user, EventAt: hour + 10, TargetUserID: user, Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000, EligibleTrial: true}
				g.EvidenceHash = financeCreditEventHash(g)
				grants = append(grants, g)
			}
		}
		if _, err = replaceFinanceCreditHour(ctx, db, h, hour+10800, "v1", financeCreditHourFetch{Events: grants, SourceRows: int64(len(grants))}); err != nil {
			t.Fatal(err)
		}
		for _, user := range []int64{7, 8} {
			if _, err = replaceFinanceGiftBoundaryUserHour(ctx, db, h, user, hour+10800, "v1", byUser[user]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = db.Create(&FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "v1", StartHourTs: hour, NextHourTs: hour + 7200, Status: "backfilling"}).Error; err != nil {
		t.Fatal(err)
	}
	backup = filepath.Join(dir, "backup.db")
	if err = db.Exec("VACUUM INTO ?", backup).Error; err != nil {
		t.Fatal(err)
	}
	return backup, paths, hour
}

// Comparing all monetary facts and proofs catches changes outside the target,
// including a cursor accidentally advanced by a manual scope-only repair.
func giftMixedMonetarySnapshot(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var facts []FinanceUserHourFact
	var states []FinanceUserHourState
	var credits []FinanceCreditEvent
	var creditStates []FinanceCreditHourState
	var cursors []FinanceFactSyncState
	var events []struct {
		SourceEpoch                                 string
		SourceLogID, HourTs, UserID, EventAt, Quota int64
		Kind                                        string
	}
	var boundaries []struct {
		SourceEpoch, Status                                                      string
		HourTs, UserID, Rows, Requests, RefundRecords, ConsumeQuota, RefundQuota int64
	}
	checks := []struct {
		table, order string
		out          any
	}{
		{"finance_user_hour_facts", "hour_ts,user_id", &facts},
		{"finance_user_hour_states", "hour_ts", &states},
		{"finance_credit_events", "source_epoch,source_log_id", &credits},
		{"finance_credit_hour_states", "hour_ts", &creditStates},
		{"finance_fact_sync_states", "id", &cursors},
		{"finance_gift_boundary_events", "source_epoch,source_log_id", &events},
		{"finance_gift_boundary_states", "source_epoch,hour_ts,user_id", &boundaries},
	}
	var all []any
	for _, check := range checks {
		if err := db.Table(check.table).Order(check.order).Find(check.out).Error; err != nil {
			t.Fatal(err)
		}
		all = append(all, check.out)
	}
	data, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
