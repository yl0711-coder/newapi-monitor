package monitor

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSub2CloseoutCompletedHistory(t *testing.T) {
	for _, fail := range []bool{false, true} {
		stats := map[string]any{"total_requests": 4, "total_tokens": 80, "total_actual_cost": 1.5}
		if fail {
			stats["total_actual_cost"] = 9
		}
		server, paths, mu := sub2BillFixture(t, stats, 0)
		m := newChannelUpstreamTestMonitor(t)
		now := time.Now().Unix()
		today := cstDayStart(now)
		row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL,
			Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600,
			UsageBackfillDone: true, UsageBackfillCursor: today, UsageDataUntil: now}
		if err := m.persistSyncedUpstreamAccount(context.Background(), &row, sub2APICredential{AccessToken: "bill-test-access", ExpiresAt: now + 3600}); err != nil {
			t.Fatal(err)
		}
		old := ChannelUpstreamUsageHour{Domain: row.Domain, Provider: row.Provider, HourTs: today - 3600, BucketSeconds: 1800, CostUSD: 7, Quota: 7, Requests: 7, FetchedAt: today - 1800, Provisional: true}
		if err := m.storeDB.Create(&old).Error; err != nil {
			t.Fatal(err)
		}
		due, err := m.loadDueUpstreamUsageAccountsForLane(context.Background(), now, 1, upstreamUsageLaneHistory)
		if err != nil || len(due) != 1 {
			t.Fatalf("unverified yesterday not scheduled: rows=%d err=%v", len(due), err)
		}
		got, err := m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneHistory)
		if (err != nil) != fail {
			t.Fatalf("fail=%v err=%v", fail, err)
		}
		if got.UsageBackfillCursor != row.UsageBackfillCursor || !got.UsageBackfillDone || got.UsageDataUntil != row.UsageDataUntil || got.UsageNextSyncAt != row.UsageNextSyncAt {
			t.Fatalf("closeout changed independent cursors: %+v", got)
		}
		var rows []ChannelUpstreamUsageHour
		if err := m.storeDB.Where("domain=?", row.Domain).Order("hour_ts").Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if fail {
			if !hasSub2PendingCloseout(got) {
				t.Fatal("failed reconciliation discarded the pinned date")
			}
			if len(rows) != 1 || !reflect.DeepEqual(rows[0], old) || got.UsageBackfillConsecutiveFails != 1 || got.UsageBackfillNextSyncAt <= now {
				t.Fatalf("failed closeout lost bills/backoff: %+v %+v", rows, got)
			}
		} else {
			if got.UsageBackfillProgress != "" {
				t.Fatal("successful reconciliation retained pending date")
			}
			if len(rows) != 24 || rows[0].CostUSD != 1.5 {
				t.Fatalf("missing complete day: %+v", rows)
			}
			for _, h := range rows {
				if h.Provisional || h.SourceKind != sub2VerifiedDaySource {
					t.Fatalf("unverified: %+v", h)
				}
			}
		}
		// Reload from durable state, as after restart. Neither success nor a
		// failure under backoff may immediately call the upstream again.
		due, err = m.loadDueUpstreamUsageAccountsForLane(context.Background(), now+1, 1, upstreamUsageLaneHistory)
		if err != nil || len(due) != 0 {
			t.Fatalf("closeout was immediately requeued: %+v %v", due, err)
		}
		_, _ = m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneHistory)
		mu.Lock()
		calls := len(*paths)
		mu.Unlock()
		if calls != 2 {
			t.Fatalf("want one trend + one stats, got %d", calls)
		}
	}
}

func TestSub2CloseoutSelectionBoundaries(t *testing.T) {
	now := cstDayStart(time.Now().Unix()) + 12*3600
	today := cstDayStart(now)
	for _, kind := range []string{"missing", "unverified", "partial", "hourly_verified", "daily_verified", "verified_gap", "stale_proof", "off_grid", "overlap", "future_backoff", "auth_paused", "tail_error", "disabled", "sync_disabled", "other_provider"} {
		t.Run(kind, func(t *testing.T) {
			m := newChannelUpstreamTestMonitor(t)
			a := ChannelUpstreamAccount{Domain: "closeout.example", Provider: upstreamProviderSub2API, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageBackfillDone: true, UsageBackfillCursor: today}
			switch kind {
			case "future_backoff":
				a.UsageBackfillNextSyncAt = now + 3600
			case "auth_paused":
				a.UsageStatus = upstreamStatusReconnect
			case "tail_error":
				a.UsageStatus = upstreamStatusError
			case "disabled":
				a.Enabled = false
			case "sync_disabled":
				a.UsageSyncEnabled = false
			case "other_provider":
				a.Provider = upstreamProviderNewAPI
			}
			if err := m.storeDB.Create(&a).Error; err != nil {
				t.Fatal(err)
			}
			if kind != "missing" {
				rows := []ChannelUpstreamUsageHour{}
				for h := today - 86400; h < today; h += 3600 {
					rows = append(rows, ChannelUpstreamUsageHour{Domain: a.Domain, Provider: a.Provider, HourTs: h, BucketSeconds: 3600, SourceKind: sub2VerifiedDaySource, FetchedAt: today + 10})
				}
				switch kind {
				case "hourly_verified":
				case "daily_verified":
					rows = rows[:1]
					rows[0].BucketSeconds = 86400
				case "verified_gap":
					rows = rows[:23]
				case "stale_proof":
					rows[0].FetchedAt = today - 1
				case "off_grid":
					rows[0].HourTs++
				case "overlap":
					rows[0].BucketSeconds = 86400
				default:
					rows[0].SourceKind = ""
				}
				if kind == "partial" {
					rows[23].BucketSeconds = 1800
					rows[23].Provisional = true
				}
				if err := m.storeDB.Create(&rows).Error; err != nil {
					t.Fatal(err)
				}
			}
			want := kind == "unverified" || kind == "partial" || kind == "verified_gap" || kind == "stale_proof" || kind == "off_grid" || kind == "overlap"
			due, err := m.loadDueUpstreamUsageAccountsForLane(context.Background(), now, 1, upstreamUsageLaneHistory)
			if err != nil || (len(due) == 1) != want {
				t.Fatalf("want due=%v rows=%d err=%v", want, len(due), err)
			}
			if a.Enabled && a.UsageSyncEnabled && a.Provider == upstreamProviderSub2API {
				plan := planUpstreamUsageSyncForLane(a, now, 7, upstreamUsageLaneHistory)
				plan, err = m.prioritizeSub2RecentUsage(context.Background(), a, now, plan)
				if err != nil || (plan.backfillTo > plan.backfillFrom) != want {
					t.Fatalf("selector/planner disagree: %+v %v", plan, err)
				}
			}
			plan := planUpstreamUsageSyncForLane(a, now, 7, upstreamUsageLaneTail)
			plan, err = m.prioritizeSub2RecentUsage(context.Background(), a, now, plan)
			if err != nil || plan.backfillTo > plan.backfillFrom {
				t.Fatalf("closeout invaded tail lane: %+v %v", plan, err)
			}
		})
	}
}

func TestSub2CloseoutQueryUsesBoundedHourIndex(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	day := cstDayStart(time.Now().Unix())
	var plan []struct{ Detail string }
	err := m.storeDB.Raw("EXPLAIN QUERY PLAN SELECT domain FROM channel_upstream_accounts WHERE provider='sub2api' AND "+sub2UnverifiedYesterdaySQL,
		day-86400, day, day-86400, day, day, day-86400).Scan(&plan).Error
	if err != nil {
		t.Fatal(err)
	}
	searches := 0
	for _, step := range plan {
		if strings.Contains(step.Detail, "SEARCH u ") {
			searches++
			if !strings.Contains(step.Detail, "hour_ts>?") || !strings.Contains(step.Detail, "hour_ts<?") {
				t.Fatalf("unbounded account history scan: %s", step.Detail)
			}
		}
		if strings.Contains(step.Detail, "SCAN u") {
			t.Fatalf("full bill scan: %s", step.Detail)
		}
	}
	if searches < 2 {
		t.Fatalf("missing two bounded evidence probes: %+v", plan)
	}
}

func TestSub2CloseoutNextDayAndFailureResume(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	today := cstDayStart(time.Now().Unix())
	now := today + 12*3600
	a := ChannelUpstreamAccount{Domain: "next-day.example", Provider: upstreamProviderSub2API, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageBackfillDone: true, UsageBackfillCursor: today}
	if err := m.storeDB.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	verified := ChannelUpstreamUsageHour{Domain: a.Domain, Provider: a.Provider, HourTs: today - 86400, BucketSeconds: 86400, SourceKind: sub2VerifiedDaySource, FetchedAt: today + 1}
	open := ChannelUpstreamUsageHour{Domain: a.Domain, Provider: a.Provider, HourTs: today, BucketSeconds: 1800, Provisional: true, FetchedAt: today + 1800}
	if err := m.storeDB.Create(&[]ChannelUpstreamUsageHour{verified, open}).Error; err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int64{0, 86400} {
		due, err := m.loadDueUpstreamUsageAccountsForLane(context.Background(), now+delta, 1, upstreamUsageLaneHistory)
		if err != nil || (len(due) == 1) != (delta > 0) {
			t.Fatalf("day rollover not detected: delta=%d rows=%d err=%v", delta, len(due), err)
		}
		if delta > 0 {
			plan := planUpstreamUsageSyncForLane(a, now+delta, 7, upstreamUsageLaneHistory)
			plan, err = m.prioritizeSub2RecentUsage(context.Background(), a, now+delta, plan)
			if err != nil || plan.backfillFrom != today || plan.backfillTo != today+86400 || !plan.recentOnly {
				t.Fatalf("wrong new closed day: %+v %v", plan, err)
			}
		}
	}
	// The persisted retry deadline, not process memory, gates activation.
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", a.Domain).Updates(map[string]any{"usage_backfill_next_sync_at": now + 86400 + 60, "usage_backfill_consecutive_fails": 2}).Error; err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, 61} {
		due, err := m.loadDueUpstreamUsageAccountsForLane(context.Background(), now+86400+offset, 1, upstreamUsageLaneHistory)
		if err != nil || (len(due) == 1) != (offset > 0) {
			t.Fatalf("retry deadline ignored: offset=%d rows=%d err=%v", offset, len(due), err)
		}
	}
}
