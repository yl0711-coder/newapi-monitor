package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

// These fixtures use loopback-only upstreams and disposable SQLite databases.
func TestCostIntegritySub2TrendPresence(t *testing.T) {
	for _, tc := range []struct {
		name, trend string
		valid       bool
	}{
		{"missing", "", false}, {"null", "null", false},
		{"object", "{}", false}, {"empty_array", "[]", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statsCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data := map[string]any{"start_date": r.URL.Query().Get("start_date"), "end_date": r.URL.Query().Get("end_date"), "granularity": "hour"}
				if r.URL.Path == "/api/v1/usage/stats" {
					statsCalls++
					data = map[string]any{"total_requests": 0, "total_tokens": 0, "total_actual_cost": 0}
				} else if tc.trend != "" {
					data["trend"] = json.RawMessage(tc.trend)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": data})
			}))
			defer server.Close()
			m := newChannelUpstreamTestMonitor(t)
			now := time.Now().Unix()
			from := cstDayStart(now) - 86400
			row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageDataUntil: from + 86400, UsageBackfillCursor: from}
			if err := m.persistSyncedUpstreamAccount(context.Background(), &row, sub2APICredential{AccessToken: "fixture", ExpiresAt: now + 3600}); err != nil {
				t.Fatal(err)
			}
			old := ChannelUpstreamUsageHour{Domain: row.Domain, Provider: row.Provider, HourTs: from, BucketSeconds: 3600, CostUSD: 9, Quota: 9, Requests: 9, Tokens: 900, FetchedAt: from + 3600}
			if err := m.storeDB.Create(&old).Error; err != nil {
				t.Fatal(err)
			}
			var before bytes.Buffer
			if err := m.hashFinanceUpstreamBillBuckets(context.Background(), &before, from, from+86400); err != nil {
				t.Fatal(err)
			}
			synced, err := m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneHistory)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			var got []ChannelUpstreamUsageHour
			if err := m.storeDB.Where("domain = ?", row.Domain).Order("hour_ts").Find(&got).Error; err != nil {
				t.Fatal(err)
			}
			if tc.valid {
				if statsCalls != 1 || len(got) != 24 {
					t.Fatalf("valid zero day not reconciled: calls=%d rows=%d", statsCalls, len(got))
				}
				for _, h := range got {
					if h.Provisional || h.CostUSD != 0 || h.SourceKind != sub2VerifiedDaySource {
						t.Fatalf("unverified zero: %+v", h)
					}
				}
			} else {
				if statsCalls != 0 || len(got) != 1 || !reflect.DeepEqual(got[0], old) {
					t.Fatalf("malformed trend replaced facts: calls=%d rows=%+v", statsCalls, got)
				}
				if synced.UsageBackfillCursor != from || synced.UsageBackfillDone || synced.UsageBackfillConsecutiveFails != 1 || synced.UsageBackfillNextSyncAt <= now || synced.UsageNextSyncAt != row.UsageNextSyncAt {
					t.Fatalf("unsafe failure recovery: %+v", synced)
				}
				var after bytes.Buffer
				if err := m.hashFinanceUpstreamBillBuckets(context.Background(), &after, from, from+86400); err != nil {
					t.Fatal(err)
				}
				if before.String() != after.String() {
					t.Fatal("failed collection changed published bill revision")
				}
			}
		})
	}
}

func TestCostIntegrityUnitMarkerIsNotBill(t *testing.T) {
	for _, quota := range []float64{0, 72} {
		m := newChannelUpstreamTestMonitor(t)
		previous := ChannelUpstreamAccount{Domain: "unit.example", Provider: upstreamProviderNewAPI, BalanceUnit: 7.2}
		next := previous
		next.BalanceUnit, next.UpdatedAt = 8, 7217
		if err := reconcileUpstreamEconomicUnitTx(m.storeDB, previous, &next); err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&next).Error; err != nil {
			t.Fatal(err)
		}
		var rows []ChannelUpstreamUsageHour
		if err := m.storeDB.Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || !rows[0].Provisional || rows[0].SourceKind != "newapi_unit_boundary" {
			t.Fatalf("boundary lacks non-fact identity: %+v", rows)
		}
		accounts := map[string]ChannelUpstreamAccountView{previous.Domain: {Configured: true, UsageSyncEnabled: true, Provider: previous.Provider}}
		scope := stabilityScope{FromTs: 7200, ToTs: 10800}
		metrics, money, err := projectFinanceBillWindow(rows, scope, 14400, accounts, nil)
		if err != nil {
			t.Fatal(err)
		}
		got := metrics[previous.Domain]
		if got.Available || got.Complete || got.AdjustedCostAvailable || got.CompletedHours != 0 || len(money) != 0 {
			t.Fatalf("marker advertised as consumption: %+v money=%+v", got, money)
		}
		var before bytes.Buffer
		if err := m.hashFinanceUpstreamBillBuckets(context.Background(), &before, 7200, 10800); err != nil {
			t.Fatal(err)
		}
		real := ChannelUpstreamUsageHour{Domain: previous.Domain, Provider: previous.Provider, HourTs: 7200, BucketSeconds: 3600, Quota: quota}
		// Even a real zero with the same timestamp must change the cache identity.
		if err := m.persistUpstreamUsageWindow(context.Background(), previous.Domain, 7200, 10800, []ChannelUpstreamUsageHour{real}, 7217); err != nil {
			t.Fatal(err)
		}
		rows = nil
		if err := m.storeDB.Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Provisional || rows[0].SourceKind != "" || rows[0].UnitPerUSD != 7.2 || rows[0].CostUSD != quota/7.2 {
			t.Fatalf("real facts lost boundary unit: %+v", rows)
		}
		metrics, money, err = projectFinanceBillWindow(rows, scope, 14400, accounts, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !metrics[previous.Domain].Complete || len(money) != 1 {
			t.Fatalf("real facts unavailable: %+v", metrics)
		}
		var after bytes.Buffer
		if err := m.hashFinanceUpstreamBillBuckets(context.Background(), &after, 7200, 10800); err != nil {
			t.Fatal(err)
		}
		if before.String() == after.String() {
			t.Fatal("real fact replacement did not invalidate bill revision")
		}
	}
}

func TestCostIntegrityMixedMarkerCoverage(t *testing.T) {
	const domain = "mixed.example"
	rows := []ChannelUpstreamUsageHour{
		{Domain: domain, Provider: upstreamProviderNewAPI, HourTs: 3600, BucketSeconds: 3600, CostUSD: 10, Quota: 72, UnitPerUSD: 7.2, Requests: 4, FetchedAt: 7200},
		{Domain: domain, Provider: upstreamProviderNewAPI, HourTs: 7200, BucketSeconds: 3600, SourceKind: "newapi_unit_boundary", Provisional: true, UnitPerUSD: 7.2, FetchedAt: 9000},
	}
	scope := stabilityScope{FromTs: 3600, ToTs: 10800}
	accounts := map[string]ChannelUpstreamAccountView{domain: {Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderNewAPI}}
	channel, err := projectChannelUpstreamUsageWindow(rows, scope, 14400, accounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	finance, _, err := projectFinanceBillWindow(rows, scope, 14400, accounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(channel, finance) {
		t.Fatal("channel and finance diverged")
	}
	m := channel[domain]
	if m.Complete || !m.Available || m.CompletedHours != 1 || m.ExpectedHours != 2 || m.CostUSD != 10 || m.DataUntil != 7200 {
		t.Fatalf("marker inflated coverage or freshness: %+v", m)
	}
}
