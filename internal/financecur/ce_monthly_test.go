package financecur

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func ceTestMetric(s string) map[string]ceMetric {
	return map[string]ceMetric{"NetUnblendedCost": {s, "USD"}}
}
func ceTestInputs() [3]ceResponse {
	f := false
	s := ceResult{TimePeriod: cePeriod{"2026-05-01", "2026-06-01"}, Estimated: &f, Groups: []ceGroup{{[]string{"A"}, ceTestMetric("4.1")}, {[]string{"B"}, ceTestMetric("4")}}}
	r := ceResult{TimePeriod: s.TimePeriod, Estimated: &f, Groups: []ceGroup{{[]string{"Usage"}, ceTestMetric("10.1")}, {[]string{"Credit"}, ceTestMetric("-2")}, {[]string{"Tax"}, ceTestMetric("0")}}}
	inputs := [3]ceResponse{{ResultsByTime: []ceResult{s}}, {ResultsByTime: []ceResult{r}}, {}}
	inputs[0].GroupDefinitions = append(inputs[0].GroupDefinitions, struct{ Type, Key string }{"DIMENSION", "SERVICE"})
	inputs[1].GroupDefinitions = append(inputs[1].GroupDefinitions, struct{ Type, Key string }{"DIMENSION", "RECORD_TYPE"})
	for day := 1; day <= 31; day++ {
		start := time.Date(2026, 5, day, 0, 0, 0, 0, time.UTC)
		amount := "0"
		if day == 1 {
			amount = "8.1"
		}
		inputs[2].ResultsByTime = append(inputs[2].ResultsByTime, ceResult{TimePeriod: cePeriod{start.Format("2006-01-02"), start.AddDate(0, 0, 1).Format("2006-01-02")}, Total: ceTestMetric(amount), Estimated: &f})
	}
	return inputs
}
func ceTestBytes(t *testing.T, r ceResponse) []byte {
	t.Helper()
	b, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func ceTestReconcile(t *testing.T, in [3]ceResponse) (CEMonthlyEvidence, error) {
	t.Helper()
	return ReconcileCEMonths(bytes.NewReader(ceTestBytes(t, in[0])), bytes.NewReader(ceTestBytes(t, in[1])), bytes.NewReader(ceTestBytes(t, in[2])), "2026-05", "2026-06")
}

func TestCEMonthlySignedAmountsAndExplicitUTCBasis(t *testing.T) {
	in := ceTestInputs()
	out, err := ceTestReconcile(t, in)
	if err != nil {
		t.Fatal(err)
	}
	if out.TimeZone != "UTC" || out.Kind != "aws_ce_monthly_evidence" || out.Currency != "USD" || out.Metric != "NetUnblendedCost" || len(out.SourceSHA256) != 3 || len(out.Months) != 1 {
		t.Fatalf("unexpected evidence: %+v", out)
	}
	m := out.Months[0]
	if m.NetUSD != "8.1" || m.VerifiedDays != 31 || m.Estimated || m.FromUTC != "2026-05-01" || m.ToUTC != "2026-06-01" {
		t.Fatalf("incorrect month: %+v", m)
	}
	if m.RecordTypes[0].Name != "Credit" || m.RecordTypes[0].USD != "-2" {
		t.Fatal("signed credit lost")
	}
	// A source sum is already net. Do not subtract this Credit a second time.
	in[0].ResultsByTime[0].Groups[0], in[0].ResultsByTime[0].Groups[1] = in[0].ResultsByTime[0].Groups[1], in[0].ResultsByTime[0].Groups[0]
	for i, j := 0, len(in[2].ResultsByTime)-1; i < j; i, j = i+1, j-1 {
		in[2].ResultsByTime[i], in[2].ResultsByTime[j] = in[2].ResultsByTime[j], in[2].ResultsByTime[i]
	}
	reordered, err := ceTestReconcile(t, in)
	if err != nil || !reflect.DeepEqual(reordered.Months, out.Months) {
		t.Fatal("source order changed money", err)
	}
	if reordered.SourceSHA256["services"] == out.SourceSHA256["services"] {
		t.Fatal("changed raw source hash not reflected")
	}
	// Returned data cannot mutate the original inputs or later runs.
	out.Months[0].RecordTypes[0].USD = "999"
	again, err := ceTestReconcile(t, in)
	if err != nil || again.Months[0].NetUSD != "8.1" || again.Months[0].RecordTypes[0].USD != "-2" {
		t.Fatal("result aliasing")
	}
}

func TestCEMonthlyRejectsIncompleteOrConflictingEvidence(t *testing.T) {
	cases := map[string]func(*[3]ceResponse){
		"missing_day":       func(x *[3]ceResponse) { x[2].ResultsByTime = x[2].ResultsByTime[:30] },
		"duplicate_day":     func(x *[3]ceResponse) { x[2].ResultsByTime = append(x[2].ResultsByTime, x[2].ResultsByTime[0]) },
		"duplicate_month":   func(x *[3]ceResponse) { x[0].ResultsByTime = append(x[0].ResultsByTime, x[0].ResultsByTime[0]) },
		"wrong_day_end":     func(x *[3]ceResponse) { x[2].ResultsByTime[0].TimePeriod.End = "2026-05-03" },
		"partial_month":     func(x *[3]ceResponse) { x[0].ResultsByTime[0].TimePeriod.Start = "2026-05-02" },
		"wrong_month_end":   func(x *[3]ceResponse) { x[0].ResultsByTime[0].TimePeriod.End = "2026-07-01" },
		"wrong_grouping":    func(x *[3]ceResponse) { x[0].GroupDefinitions[0].Key = "RECORD_TYPE" },
		"missing_estimated": func(x *[3]ceResponse) { x[2].ResultsByTime[0].Estimated = nil },
		"currency": func(x *[3]ceResponse) {
			x[0].ResultsByTime[0].Groups[0].Metrics["NetUnblendedCost"] = ceMetric{"4.1", "CNY"}
		},
		"missing_metric_no_fallback": func(x *[3]ceResponse) {
			x[0].ResultsByTime[0].Groups[0].Metrics = map[string]ceMetric{"UnblendedCost": {"4.1", "USD"}}
		},
		"duplicate_group": func(x *[3]ceResponse) {
			x[0].ResultsByTime[0].Groups = append(x[0].ResultsByTime[0].Groups, x[0].ResultsByTime[0].Groups[0])
		},
		"empty_group":           func(x *[3]ceResponse) { x[0].ResultsByTime[0].Groups[0].Keys = []string{" "} },
		"pagination":            func(x *[3]ceResponse) { x[1].NextPageToken = "more" },
		"different_month_total": func(x *[3]ceResponse) { x[1].ResultsByTime[0].Groups[0].Metrics = ceTestMetric("10.2") },
		"different_daily_total": func(x *[3]ceResponse) { x[2].ResultsByTime[0].Total = ceTestMetric("6.1") },
		"unknown_amount":        func(x *[3]ceResponse) { x[2].ResultsByTime[0].Total = ceTestMetric("NaN") },
		"exponent":              func(x *[3]ceResponse) { x[2].ResultsByTime[0].Total = ceTestMetric("8.1e0") },
		"too_precise":           func(x *[3]ceResponse) { x[2].ResultsByTime[0].Total = ceTestMetric("8.1000000000000000001") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := ceTestInputs()
			mutate(&in)
			out, err := ceTestReconcile(t, in)
			if err == nil || !reflect.DeepEqual(out, CEMonthlyEvidence{}) {
				t.Fatalf("bad input accepted or partial result leaked: %v %+v", err, out)
			}
		})
	}
}

func TestCEMonthlyEstimatedAndDecimalPrecision(t *testing.T) {
	in := ceTestInputs()
	yes := true
	in[2].ResultsByTime[15].Estimated = &yes
	out, err := ceTestReconcile(t, in)
	if err != nil || !out.Months[0].Estimated {
		t.Fatal("provisional day promoted to verified month", err)
	}
	for _, s := range []string{"0", "-0", "0.000000000000000001", "-0.0000000452", "197.1008700236", "999999999999.123456789012345678"} {
		n, e := ceNetAmount(ceTestMetric(s))
		if e != nil {
			t.Fatal(e)
		}
		want := s
		if s == "-0" {
			want = "0"
		}
		if ceDecimalString(n) != want {
			t.Fatal("decimal precision loss", s, ceDecimalString(n))
		}
	}
}

func TestCEMonthlyReadBoundsAndInvalidShape(t *testing.T) {
	base := ceTestBytes(t, ceTestInputs()[0])
	for _, b := range [][]byte{
		append(append([]byte{}, base...), []byte(" {}")...),
		[]byte(`{"ResultsByTime":[],"resultsbytime":[]}`),
		[]byte(`{"ResultsByTime":[],"Unknown":true}`),
		[]byte(`null`),
		[]byte(strings.Repeat(" ", maxCEEvidenceBytes+1)),
	} {
		r, _, e := readCEEvidence(bytes.NewReader(b))
		if e == nil {
			_, e = validateCEResponse(r, "SERVICE")
		}
		if e == nil {
			t.Fatal("invalid shape accepted")
		}
	}
	if _, _, err := readCEEvidence(nil); err == nil {
		t.Fatal("nil source accepted")
	}
	for _, v := range [][2]string{{"2026-05", "2026-05"}, {"2026-5", "2026-06"}, {"2026-05", "2030-06"}, {"2026-05", "2026-04"}} {
		if _, e := ReconcileCEMonths(nil, nil, nil, v[0], v[1]); e == nil {
			t.Fatal("invalid range accepted")
		}
	}
}

// Opt-in frozen real response check. No AWS, database, network or file writes.
func TestCEMonthlyRetainedHistoricalEvidence(t *testing.T) {
	dir := os.Getenv("MONITOR_CUR_LOCAL_EVIDENCE_DIR")
	if dir == "" {
		t.Skip("requires retained local CE evidence")
	}
	names := []string{"ce-monthly-202605-202609.json", "ce-record-types-202605-202609.json", "ce-daily-202605-202608.json"}
	data := [][]byte{}
	for _, n := range names {
		b, e := os.ReadFile(filepath.Join(dir, n))
		if e != nil {
			t.Fatal(e)
		}
		data = append(data, b)
	}
	out, e := ReconcileCEMonths(bytes.NewReader(data[0]), bytes.NewReader(data[1]), bytes.NewReader(data[2]), "2026-05", "2026-09")
	if e != nil {
		t.Fatal(e)
	}
	want := []string{"9.9999999968", "4.9999999995", "79.7693883741", "197.1008700236"}
	if len(out.Months) != len(want) {
		t.Fatal("month lost")
	}
	days := 0
	for i, m := range out.Months {
		if m.NetUSD != want[i] || m.Estimated {
			t.Fatalf("unexpected month %+v", m)
		}
		days += m.VerifiedDays
	}
	if days != 123 {
		t.Fatal("historical day missing")
	}
	for i, n := range names {
		b, e := os.ReadFile(filepath.Join(dir, n))
		if e != nil || sha256.Sum256(b) != sha256.Sum256(data[i]) {
			t.Fatal("original evidence changed")
		}
	}
	t.Logf("PASS: 4 UTC months, %d daily totals, signed credit reconciliation and original hashes unchanged", days)
}
