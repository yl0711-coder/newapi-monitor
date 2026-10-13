package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// A successful probe is not a repaired bill. Exercise the real worker after
// reactivation, including a previously completed history lane and a restart.
func TestUpstreamRecoveryCatchupReopensCompletedHistory(t *testing.T) {
	for _, provider := range []string{upstreamProviderNewAPI, upstreamProviderSub2API} {
		t.Run(provider, func(t *testing.T) {
			m, row, calls := newUpstreamRecoveryFixture(t)
			now := time.Now().Unix()
			today := cstDayStart(now)
			row.Provider = provider
			row.UsageBackfillCursor = today - 3*86400
			row.UsageDataUntil = row.UsageBackfillCursor + 900
			row.UsageBackfillDone = true // It was complete before the outage.
			row.UsageBackfillNextSyncAt = 0
			row.UsageBackfillLastError = ""
			row.UsageBackfillConsecutiveFails = 0
			var credential any = newAPICredential{AccessToken: "fixture-access"}
			if provider == upstreamProviderSub2API {
				row.Account, row.UsageAdapter = "fixture@example.com", upstreamUsageAdapterSub2Trend
				row.BalanceUnit = 1 // Sub2API bills expose face-value cost, not NewAPI quota.
				credential = sub2APICredential{AccessToken: "fixture-access", RefreshToken: "fixture-refresh", ExpiresAt: now + 3600}
				setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					if r.Method != http.MethodGet || (r.URL.Path != "/api/v1/usage/dashboard/trend" && r.URL.Path != "/api/v1/usage/stats") {
						t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
					}
					day := r.URL.Query().Get("start_date")
					if day == "" || day != r.URL.Query().Get("end_date") {
						t.Fatal("history request expanded beyond one calendar day")
					}
					if r.URL.Path == "/api/v1/usage/stats" {
						return recoveryHTTPResponse(http.StatusOK, `{"code":0,"data":{"total_requests":2,"total_tokens":30,"total_actual_cost":"1.5"}}`), nil
					}
					body, err := json.Marshal(map[string]any{"code": 0, "data": map[string]any{
						"start_date": day, "end_date": day, "granularity": "hour",
						"trend": []any{map[string]any{"date": day + " 00:00", "requests": 2, "total_tokens": 30, "actual_cost": "1.5"}},
					}})
					if err != nil {
						t.Fatal(err)
					}
					return recoveryHTTPResponse(http.StatusOK, string(body)), nil
				}))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := m.persistSyncedUpstreamAccount(ctx, &row, credential); err != nil {
				t.Fatal(err)
			}
			// Keep a partial pre-outage observation. Catch-up must replace it
			// with a whole-hour bucket, not retain it as a completed hour.
			partial := ChannelUpstreamUsageHour{Domain: row.Domain, Provider: provider, HourTs: row.UsageBackfillCursor,
				BucketSeconds: 900, UnitPerUSD: row.BalanceUnit}
			if err := m.storeDB.Create(&partial).Error; err != nil {
				t.Fatal(err)
			}
			result, err := m.recoverUpstreamTask(ctx, row.Domain, "usage")
			if err != nil || result.Status != "queued" {
				t.Fatalf("recovery: %+v %v", result, err)
			}
			var count int64
			if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Count(&count).Error; err != nil || count != 1 {
				t.Fatal("probe published bill data", count, err)
			}
			tail, err := m.syncStoredUpstreamUsageLaneBackground(ctx, row.Domain, upstreamUsageLaneTail)
			if err != nil || tail.UsageStatus != upstreamStatusOK || tail.UsageBackfillDone || tail.UsageBackfillCursor != row.UsageBackfillCursor {
				t.Fatalf("tail did not reopen historical catch-up without skipping the cursor: err=%v done=%t cursor=%d", err, tail.UsageBackfillDone, tail.UsageBackfillCursor)
			}
			// New in-memory scheduler state; the durable cursor must be enough.
			restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, upstreamClient: m.upstreamClient}
			plan := planUpstreamUsageSyncForLane(tail, time.Now().Unix(), 7, upstreamUsageLaneHistory)
			if plan.backfillFrom != row.UsageBackfillCursor || plan.backfillTo <= plan.backfillFrom || plan.tailTo != 0 {
				t.Fatalf("restart lost backlog: %+v", plan)
			}
			before := calls.Load()
			history, err := restarted.syncStoredUpstreamUsageLaneBackground(ctx, row.Domain, upstreamUsageLaneHistory)
			if err != nil {
				t.Fatal(err)
			}
			if used := calls.Load() - before; used <= 0 || int(used) > upstreamUsageRequestBudget(provider, upstreamUsageLaneHistory) {
				t.Fatal("catch-up exceeded request budget or made no requests", used)
			}
			if history.UsageBackfillCursor <= row.UsageBackfillCursor || history.UsageBackfillCursor > today || history.UsageBackfillDone {
				t.Fatal("bounded history skipped or prematurely completed the backlog")
			}
			var buckets []ChannelUpstreamUsageHour
			if err := m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", row.Domain, row.UsageBackfillCursor, history.UsageBackfillCursor).
				Order("hour_ts").Find(&buckets).Error; err != nil {
				t.Fatal(err)
			}
			if int64(len(buckets))*3600 != history.UsageBackfillCursor-row.UsageBackfillCursor {
				t.Fatal("advanced cursor contains missing hours")
			}
			for i, bucket := range buckets {
				if bucket.HourTs != row.UsageBackfillCursor+int64(i)*3600 || bucket.BucketSeconds != 3600 || bucket.Provisional {
					t.Fatal("partial or missing hour behind advanced cursor")
				}
			}
			createChannelRechargeVersion(t, m, row.Domain, 1, row.UsageBackfillCursor, 2.5, 8)
			accounts := map[string]ChannelUpstreamAccountView{row.Domain: {
				Configured: true, UsageSyncEnabled: true, Provider: provider, UsageGranularity: "hour",
			}}
			cost := 0.0
			if provider == upstreamProviderSub2API {
				cost = 1.5
			}
			assertRecoveryAccountingAmounts(t, restarted,
				stabilityScope{FromTs: row.UsageBackfillCursor, ToTs: history.UsageBackfillCursor}, now,
				accounts, row.Domain, cost, cost*2.5/8)
		})
	}
}
