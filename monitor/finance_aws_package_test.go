package monitor

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// Optional retained-evidence acceptance: the external CLI output must produce
// exactly the already accepted combined report, not merely parse successfully.
func TestFinanceAWSPackageRetainedReport(t *testing.T) {
	path, business, expected := os.Getenv("MONITOR_AWS_PACKAGE_FILE"), os.Getenv("MONITOR_FINANCE_COMBINED_INPUT"), os.Getenv("MONITOR_AWS_PACKAGE_EXPECTED_REPORT")
	if path == "" || business == "" || expected == "" {
		t.Skip("requires local package and accepted retained reports")
	}
	load := func(path string) *financeOperatingReport {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var saved struct {
			Report *financeOperatingReport `json:"after_rebuild"`
		}
		if err := json.Unmarshal(raw, &saved); err != nil || saved.Report == nil {
			t.Fatal("invalid retained report", err)
		}
		return saved.Report
	}
	r, want := load(business), load(expected)
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	m.applyFinanceCURArtifactToReport(r)
	// Compare the entire API representation: optional empty CUR slices are
	// omitted on disk and decode to nil, unlike freshly projected empty slices.
	encode := func(v *financeOperatingReport) []byte {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	wantJSON := encode(want)
	if !r.CURCost.Loaded || !bytes.Equal(encode(r), wantJSON) {
		t.Fatal("CLI package changed accepted report")
	}
	m.applyFinanceCURArtifactToReport(r)
	if !bytes.Equal(encode(r), wantJSON) {
		t.Fatal("CLI package reapplication changed report")
	}
}

func packageFixture(t *testing.T) FinanceAWSPackageInput {
	t.Helper()
	b := awsBundleFixture(t)
	in := FinanceAWSPackageInput{Services: b.HistoricalCE.Services, RecordTypes: b.HistoricalCE.RecordTypes, Daily: b.HistoricalCE.Daily,
		FromMonth: b.HistoricalCE.FromMonth, ToMonthExclusive: b.HistoricalCE.ToMonthExclusive, AccountingBasis: financeAWSBillingMonthBasis}
	for _, a := range b.CUR.Artifacts {
		in.CURArtifacts = append(in.CURArtifacts, awsBundleRaw(t, a))
	}
	return in
}

func TestFinanceAWSPackageRoundTrip(t *testing.T) {
	in := packageFixture(t)
	before, _ := json.Marshal(in)
	raw, err := BuildFinanceAWSPackage(in)
	if err != nil {
		t.Fatal(err)
	}
	source, err := readFinanceCURSource(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !source.billingMonth || len(source.monthly.Months) != 1 || source.monthly.Months[0].NetUSD != "8" || len(source.review.Statements) != 2 {
		t.Fatal("packaging changed evidence")
	}
	b := awsBundleFixture(t)
	b.AccountingBasis = financeAWSBillingMonthBasis
	want, err := readFinanceCURSource(bytes.NewReader(awsBundleRaw(t, b)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source.review, want.review) || !reflect.DeepEqual(source.monthly, want.monthly) {
		t.Fatal("packager and prior reader disagree")
	}
	after, _ := json.Marshal(in)
	if !bytes.Equal(before, after) {
		t.Fatal("packager changed input")
	}
	in.CURArtifacts[0], in.CURArtifacts[1] = in.CURArtifacts[1], in.CURArtifacts[0]
	reordered, err := BuildFinanceAWSPackage(in)
	if err != nil || !bytes.Equal(raw, reordered) {
		t.Fatal("input order changed canonical output", err)
	}
}

func TestFinanceAWSPackageRejectsInvalidInputs(t *testing.T) {
	for _, name := range []string{"basis", "none", "duplicate", "overlap", "malformed", "missing_day", "pagination", "amount_disagreement", "nested_bundle", "oversize"} {
		t.Run(name, func(t *testing.T) {
			in := packageFixture(t)
			switch name {
			case "basis":
				in.AccountingBasis = ""
			case "none":
				in.CURArtifacts = nil
			case "duplicate":
				in.CURArtifacts = append(in.CURArtifacts, in.CURArtifacts[0])
			case "overlap":
				in.CURArtifacts = append(in.CURArtifacts, awsBundleRaw(t, financeCURMonthFixture(t, 8, 1, true)))
			case "malformed":
				in.CURArtifacts[0] = []byte("broken")
			case "missing_day":
				in.Daily = []byte(`{"ResultsByTime":[]}`)
			case "pagination":
				in.Daily = bytes.Replace(in.Daily, []byte(`{"ResultsByTime"`), []byte(`{"NextPageToken":"pending","ResultsByTime"`), 1)
			case "amount_disagreement":
				in.Services = bytes.Replace(in.Services, []byte(`"Amount":"8"`), []byte(`"Amount":"9"`), 1)
			case "nested_bundle":
				in.CURArtifacts[0] = awsBundleRaw(t, awsBundleFixture(t))
			case "oversize":
				in.Daily = bytes.Repeat([]byte(" "), maxFinanceCURSourceBytes+1)
			}
			if raw, err := BuildFinanceAWSPackage(in); err == nil || raw != nil {
				t.Fatal("invalid package accepted", name)
			}
		})
	}
}
