package monitor

import (
	"testing"
	"time"
)

func TestMonthlyWindow(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, usageCST)
	for _, tc := range []struct {
		month, end string
		days       int
	}{{"2024-02", "2024-03-01", 29}, {"2026-02", "2026-03-01", 28}, {"2026-09", "2026-10-01", 30}, {"2026-08", "2026-09-01", 31}} {
		a, b, err := monthlyWindow(tc.month, now)
		if err != nil || b.Format("2006-01-02") != tc.end || int(b.Sub(a).Hours()/24) != tc.days || a.Hour() != 0 {
			t.Fatalf("window %s: %v %v %v", tc.month, a, b, err)
		}
		if a.UTC().Hour() != 16 {
			t.Fatal("not Beijing midnight")
		}
	}
	for _, bad := range []string{"2026-10", "2026-13", "2026-1", "2026-11", "2019-12", "2026-10-01", "../../"} {
		if _, _, err := monthlyWindow(bad, now); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	a, _, err := monthlyWindow("", now)
	if err != nil || a.Format("2006-01") != "2026-09" {
		t.Fatal("default must be last closed month")
	}
	a, _, err = monthlyWindow("", time.Date(2026, 1, 1, 0, 0, 0, 0, usageCST))
	if err != nil || a.Format("2006-01") != "2025-12" {
		t.Fatal("January boundary")
	}
}

func TestMonthlyQuotaExactAndFailClosed(t *testing.T) {
	for text, want := range map[string]int64{"＄20000.000000 额度": 10000000000, "$0.000002": 1, "123 点额度": 123, "$1.00": 500000} {
		got, ok := parseMonthlyQuota(text)
		if !ok || got != want {
			t.Fatalf("%q => %d/%v", text, got, ok)
		}
	}
	for _, bad := range []string{"¥7.30 额度", "1", "$0.000001", "$NaN", "$1e9", "$18446744073709551616", "$1.2345678", "$1<script>", ""} {
		if _, ok := parseMonthlyQuota(bad); ok {
			t.Fatalf("accepted %q", bad)
		}
	}
	if got := monthlyQuotaMoney(1); got != "0.000002" {
		t.Fatal(got)
	}
}
