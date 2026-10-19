package monitor

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func financeRangeContext(query string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/api/finance/operating-report"+query, nil)
	return c
}

func TestFinanceRangeUsesAccountingFinalizedHour(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	for _, tc := range []struct {
		name, now, query, want string
	}{
		{"before_internal_catchup", "2026-10-16 21:08:00", "", "2026-10-16 20:00:00"},
		{"after_internal_catchup", "2026-10-16 21:28:00", "", "2026-10-16 20:00:00"},
		{"hour_end", "2026-10-16 21:59:59", "?to=2026-10-16", "2026-10-16 20:00:00"},
		{"next_hour", "2026-10-16 22:00:00", "", "2026-10-16 21:00:00"},
		{"future_end", "2026-10-16 21:08:00", "?to=2026-12-31", "2026-10-16 20:00:00"},
		{"historical_day", "2026-10-16 21:08:00", "?to=2026-10-14", "2026-10-15 00:00:00"},
		{"month_boundary", "2026-11-01 00:08:00", "?to=2026-10-31", "2026-10-31 23:00:00"},
		{"month_closed", "2026-11-01 01:00:00", "?to=2026-10-31", "2026-11-01 00:00:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, _ := time.ParseInLocation(time.DateTime, tc.now, loc)
			// UTC host/container must produce the same Shanghai report bounds.
			from, to, err := parseFinanceRange(financeRangeContext(tc.query), now.UTC(), "2026-05-01")
			if err != nil || to.In(loc).Format(time.DateTime) != tc.want || from.In(loc).Format("2006-01-02") != "2026-05-01" {
				t.Fatalf("range: from=%v to=%v err=%v; want end=%s", from, to, err, tc.want)
			}
			if to.Unix() > finalizedStabilityHourTo(now.Unix()) {
				t.Fatal("report includes hours outside its coverage proof")
			}
			for _, lag := range []int{3, 10, 60} {
				m := &Monitor{cfg: Settings{UsageFactsLagMinutes: lag}}
				if to.Unix() > m.usageFactFinalizedHour(now) {
					t.Fatalf("report exceeds internal-account finalization at lag %d", lag)
				}
			}
		})
	}
	for _, query := range []string{"?from=2026-11-01", "?from=invalid", "?to=invalid", "?from=2026-11-02&to=2026-10-31"} {
		now := time.Date(2026, 11, 1, 0, 8, 0, 0, loc)
		if _, _, err := parseFinanceRange(financeRangeContext(query), now, "2026-05-01"); err == nil {
			t.Fatalf("invalid/empty finalized range accepted: %s", query)
		}
	}
}

func TestFinanceFinalizedRangePreservesAmountsWithoutHidingRealGaps(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "cutoff.example")
	ctx := context.Background()
	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 10, 16, 21, 8, 0, 0, loc)
	end := time.Date(2026, 10, 16, 20, 0, 0, 0, loc).Unix()
	start := end - 3600
	if err := m.storeDB.Create(&ChannelSnap{ID: 41, BaseDomain: "cutoff.example"}).Error; err != nil {
		t.Fatal(err)
	}
	// The tail may already have positive facts, but it is not yet covered by
	// accounting's finalization proof or the internal-account evidence.
	for _, hour := range []int64{start, end} {
		if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 41, Grp: "paid", Success: 2,
			Quota: 500000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 2,
			TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	evidence := financeConfiguredInternalEvidence{Accounts: 1, VerifiedScope: stabilityScope{FromTs: start, ToTs: end},
		Rows: []FinanceInternalAccountHourFact{{HourTs: start, UserID: 1, ChannelID: 41, Grp: "paid", Requests: 1, ConsumeQuota: 100000}}}
	oldScope := stabilityScope{FromTs: start, ToTs: now.Truncate(time.Hour).Unix()}
	oldInternal, err := financeConfiguredInternalSubrange(evidence, oldScope)
	if err != nil {
		t.Fatal(err)
	}
	old, coverage, _, err := m.loadFinanceUserFacts(ctx, oldScope, now.Unix(), oldInternal, nil)
	if err != nil || coverage.Percent != 100 || old.UserConsumption != nil || old.KnownUserConsumption.MicroUSD != "" {
		t.Fatalf("old boundary no longer reproduces 100%% coverage with unpublished amount: %+v %+v %v", old, coverage, err)
	}
	_, to, err := parseFinanceRange(financeRangeContext(""), now, "2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: start, ToTs: to.Unix()}
	internal, err := financeConfiguredInternalSubrange(evidence, scope)
	if err != nil {
		t.Fatal(err)
	}
	got, coverage, _, err := m.loadFinanceUserFacts(ctx, scope, now.Unix(), internal, nil)
	if err != nil || !coverage.Complete || coverage.LatestHourPending || got.UserConsumption == nil || got.UserConsumption.MicroUSD != "800000" {
		t.Fatalf("closed consumption must exclude internal account and unfinalized tail: %+v %+v %v", got, coverage, err)
	}
	if err := m.storeDB.Delete(&StabilityHourIngestState{}, "hour_ts=?", start).Error; err != nil {
		t.Fatal(err)
	}
	got, coverage, _, err = m.loadFinanceUserFacts(ctx, scope, now.Unix(), internal, nil)
	if err != nil || coverage.Complete || got.UserConsumption != nil {
		t.Fatalf("real missing receipt must still block exact amount: %+v %+v %v", got, coverage, err)
	}
}
