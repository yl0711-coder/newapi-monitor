package monitor

import (
	"encoding/json"
	"math/rand"
	"os"
	"strings"
	"testing"
)

func TestMonthlyMoneyCentsSharedBrowserAndExportCases(t *testing.T) {
	raw, err := os.ReadFile("testdata/monthly_money_cents.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Value string
		Want  *string
	}
	if err := json.Unmarshal(raw, &cases); err != nil || len(cases) < 30 {
		t.Fatal("missing money boundary coverage", err)
	}
	for _, tc := range cases {
		t.Run(tc.Value, func(t *testing.T) {
			_, ok := monthlyRoundedDisplayUnits(tc.Value, 2)
			if ok != (tc.Want != nil) {
				t.Fatal("strict decimal validation differs", ok)
			}
			if tc.Want == nil {
				return
			}
			if got := monthlySummaryMoneyTextAt(tc.Value, 2); got != *tc.Want || got != monthlyAuditCents(tc.Value) {
				t.Fatal("decimal rounding differs", got, *tc.Want)
			}
			cell := monthlyMoneyCell("A1", 4, tc.Value)
			if !strings.Contains(cell, "<v>"+*tc.Want+"</v>") && !strings.Contains(cell, "¥"+*tc.Want+"</t>") {
				t.Fatal("export differs from shared display", cell)
			}
		})
	}
}

func TestMonthlyMoneyCentsFullQuotaRangeAndAggregateBeforeRounding(t *testing.T) {
	rng := rand.New(rand.NewSource(20261027))
	quotas := []int64{0, 1, -1, 1<<63 - 1, -1 << 63, 502500, -502500}
	for range 4000 {
		quotas = append(quotas, int64(rng.Uint64()))
	}
	for _, q := range quotas {
		value := monthlyQuotaMoney(q)
		if got, want := monthlySummaryMoneyTextAt(value, 2), monthlyAuditCents(value); got != want {
			t.Fatal("quota rounding differs from independent rational oracle", q, got, want)
		}
	}
	// Ten thousand tiny requests must not disappear by rounding each to zero.
	var total int64
	for range 10000 {
		if err := addEconomicsInt64(&total, 1); err != nil {
			t.Fatal(err)
		}
	}
	if monthlySummaryMoneyTextAt(monthlyQuotaMoney(1), 2) != "0.00" || monthlySummaryMoneyTextAt(monthlyQuotaMoney(total), 2) != "0.02" {
		t.Fatal("aggregate must preserve sub-cent source charges")
	}
}
