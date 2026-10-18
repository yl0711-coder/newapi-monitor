package monitor

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

func awsBundleRaw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func awsBundleFixture(t *testing.T) financeAWSEvidenceBundle {
	t.Helper()
	set, e := financecur.NewSourceSet([]financecur.Artifact{financeCURMonthFixture(t, 9, 720_000_000, true), financeCURMonthFixture(t, 10, 744_000_000, true)})
	if e != nil {
		t.Fatal(e)
	}
	metric := func(s string) any {
		return map[string]any{"NetUnblendedCost": map[string]string{"Amount": s, "Unit": "USD"}}
	}
	group := func(name, amount string) any {
		return map[string]any{"Keys": []string{name}, "Metrics": metric(amount)}
	}
	monthly := func(dimension string, groups []any) json.RawMessage {
		return awsBundleRaw(t, map[string]any{"GroupDefinitions": []any{map[string]string{"Type": "DIMENSION", "Key": dimension}}, "ResultsByTime": []any{map[string]any{"TimePeriod": map[string]string{"Start": "2026-08-01", "End": "2026-09-01"}, "Estimated": false, "Groups": groups}}})
	}
	days := []any{}
	for d := 1; d <= 31; d++ {
		start := time.Date(2026, 8, d, 0, 0, 0, 0, time.UTC)
		amount := "0"
		if d == 1 {
			amount = "8"
		}
		days = append(days, map[string]any{"TimePeriod": map[string]string{"Start": start.Format("2006-01-02"), "End": start.AddDate(0, 0, 1).Format("2006-01-02")}, "Estimated": false, "Total": metric(amount)})
	}
	return financeAWSEvidenceBundle{SchemaVersion: 1, Kind: "aws_cost_evidence_bundle", CUR: set, HistoricalCE: financeCEEvidenceInput{FromMonth: "2026-08", ToMonthExclusive: "2026-09", Services: monthly("SERVICE", []any{group("Amazon Lightsail", "8")}), RecordTypes: monthly("RECORD_TYPE", []any{group("Usage", "10"), group("Credit", "-2")}), Daily: awsBundleRaw(t, map[string]any{"ResultsByTime": days})}}
}

func TestFinanceAWSMonthlyBundleIsolationAndBoundaries(t *testing.T) {
	b := awsBundleFixture(t)
	raw := awsBundleRaw(t, b)
	s, e := readFinanceCURSource(bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	aug := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).Unix()
	sep := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	rows := s.monthlyBills(aug, sep)
	if len(rows) != 1 || rows[0].RangeRelation != "whole_utc_month" || rows[0].NetUSD != "8" || rows[0].Status != "reconciled" {
		t.Fatal("monthly evidence lost", rows)
	}
	for _, bounds := range [][2]int64{{aug - 8*3600, sep - 8*3600}, {aug + 3600, sep}, {sep - 3600, sep + 3600}} {
		r := s.monthlyBills(bounds[0], bounds[1])
		if len(r) != 1 || r[0].RangeRelation != "partial_overlap" || r[0].NetUSD != "8" {
			t.Fatal("partial month prorated or marked whole", r)
		}
	}
	if len(s.monthlyBills(sep, sep+86400)) != 0 || len(s.monthlyBills(aug-86400, aug)) != 0 {
		t.Fatal("nonoverlapping UTC month leaked")
	}
	rows[0].Services[0].USD = "999"
	if s.monthlyBills(aug, sep)[0].Services[0].USD != "8" {
		t.Fatal("mutable evidence alias")
	}
	path := filepath.Join(t.TempDir(), "source.json")
	if e := os.WriteFile(path, awsBundleRaw(t, b.CUR), 0600); e != nil {
		t.Fatal(e)
	}
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	// Real report days are Shanghai calendar days; UTC/partial selection is
	// covered above without inventing shifted daily report buckets.
	base := financeCURRangeReport(t, aug-8*3600, sep+86400*2-8*3600)
	m.applyFinanceCURArtifactToReport(base)
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	withMonthly := financeCURRangeReport(t, aug-8*3600, sep+86400*2-8*3600)
	m.applyFinanceCURArtifactToReport(withMonthly)
	if len(withMonthly.AWSMonthlyBills) != 1 {
		t.Fatalf("monthly view not attached: %+v", withMonthly.CURCost)
	}
	if !reflect.DeepEqual(base.Statement, withMonthly.Statement) || !reflect.DeepEqual(base.Days, withMonthly.Days) || !reflect.DeepEqual(base.Periods, withMonthly.Periods) || !reflect.DeepEqual(base.CURProducts, withMonthly.CURProducts) {
		t.Fatal("CE supplement changed daily cost, business money or profit")
	}
	if withMonthly.Statement.OperatingProfit != nil {
		t.Fatal("missing same-window cost published as profit")
	}
	encoded := awsBundleRaw(t, withMonthly)
	var cached financeOperatingReport
	if err := json.Unmarshal(encoded, &cached); err != nil || !reflect.DeepEqual(cached.AWSMonthlyBills, withMonthly.AWSMonthlyBills) {
		t.Fatal("monthly details lost in report cache round trip", err)
	}
	// A corrupt replacement must clear both old CUR costs and monthly evidence,
	// while retaining revenue and upstream facts for diagnosis.
	if err := os.WriteFile(path, []byte(`{"kind":"aws_cost_evidence_bundle","schema_version":99}`), 0600); err != nil {
		t.Fatal(err)
	}
	m.applyFinanceCURArtifactToReport(&cached)
	if len(cached.AWSMonthlyBills) != 0 || cached.CURCost.Loaded || cached.CURCost.Status != "error" || cached.Statement.AWSInfrastructureCost != nil || cached.Statement.OperatingProfit != nil || !reflect.DeepEqual(cached.Statement.OperatingRevenue, withMonthly.Statement.OperatingRevenue) || !reflect.DeepEqual(cached.Statement.RawCorrectedUpstreamCost, withMonthly.Statement.RawCorrectedUpstreamCost) {
		t.Fatal("invalid replacement retained obsolete AWS money or changed business facts")
	}
	if e := os.WriteFile(path, awsBundleRaw(t, b.CUR), 0600); e != nil {
		t.Fatal(e)
	}
	m.applyFinanceCURArtifactToReport(withMonthly)
	if len(withMonthly.AWSMonthlyBills) != 0 || !reflect.DeepEqual(base.Statement, withMonthly.Statement) {
		t.Fatal("rollback left stale monthly bills")
	}
}

func TestFinanceAWSMonthlyBundleRejectsOverlapAndInvalidEvidence(t *testing.T) {
	for _, name := range []string{"overlap", "missing_day", "unknown_field", "version", "trailing"} {
		t.Run(name, func(t *testing.T) {
			b := awsBundleFixture(t)
			switch name {
			case "overlap":
				s, e := financecur.NewSourceSet([]financecur.Artifact{financeCURMonthFixture(t, 8, 744000000, true)})
				if e != nil {
					t.Fatal(e)
				}
				b.CUR = s
			case "missing_day":
				b.HistoricalCE.Daily = json.RawMessage(`{"ResultsByTime":[]}`)
			case "version":
				b.SchemaVersion = 2
			}
			raw := awsBundleRaw(t, b)
			if name == "unknown_field" {
				raw = append([]byte(`{"unreviewed":true,`), raw[1:]...)
			}
			if name == "trailing" {
				raw = append(raw, []byte(" {}")...)
			}
			if s, e := readFinanceCURSource(bytes.NewReader(raw)); e == nil || s != nil {
				t.Fatal("invalid or overlapping costs accepted")
			}
		})
	}
}

func TestFinanceAWSMonthlyBundleVersionChangesOnEvidence(t *testing.T) {
	b := awsBundleFixture(t)
	a, e := readFinanceCURSource(bytes.NewReader(awsBundleRaw(t, b)))
	if e != nil {
		t.Fatal(e)
	}
	// Equal financial facts but different source content must still be revalidated.
	var daily map[string]any
	if e := json.Unmarshal(b.HistoricalCE.Daily, &daily); e != nil {
		t.Fatal(e)
	}
	daily["DimensionValueAttributes"] = []any{}
	b.HistoricalCE.Daily = awsBundleRaw(t, daily)
	c, e := readFinanceCURSource(bytes.NewReader(awsBundleRaw(t, b)))
	if e != nil {
		t.Fatal(e)
	}
	if a.version() == c.version() || !reflect.DeepEqual(a.monthly.Months, c.monthly.Months) {
		t.Fatal("monthly content identity or amounts drift")
	}
}

// Uses already retained files only. It is shared by the opt-in evidence replay
// and the local read-only preview; never fetches or edits original evidence.
func financeAWSLocalBundle(t *testing.T, dir string, artifacts []financecur.Artifact) []byte {
	t.Helper()
	set, err := financecur.NewSourceSet(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	read := func(name string) json.RawMessage {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	return awsBundleRaw(t, financeAWSEvidenceBundle{SchemaVersion: 1, Kind: "aws_cost_evidence_bundle", CUR: set, HistoricalCE: financeCEEvidenceInput{
		FromMonth: "2026-05", ToMonthExclusive: "2026-09",
		Services: read("ce-monthly-202605-202609.json"), RecordTypes: read("ce-record-types-202605-202609.json"), Daily: read("ce-daily-202605-202608.json"),
	}})
}

func financeAWSLocalAccountingBundle(t *testing.T, dir string, artifacts []financecur.Artifact) []byte {
	t.Helper()
	var bundle financeAWSEvidenceBundle
	if err := json.Unmarshal(financeAWSLocalBundle(t, dir, artifacts), &bundle); err != nil {
		t.Fatal(err)
	}
	bundle.AccountingBasis = financeAWSBillingMonthBasis
	return awsBundleRaw(t, bundle)
}
