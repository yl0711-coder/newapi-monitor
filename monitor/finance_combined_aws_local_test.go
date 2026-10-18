//go:build unix

package monitor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

// Offline acceptance only: use retained account-wide AWS evidence and an
// already verified combined business report. No network or live DB is opened.
func TestFinanceCombinedRetainedAWSReport(t *testing.T) {
	dir, reportPath := os.Getenv("MONITOR_CUR_LOCAL_EVIDENCE_DIR"), os.Getenv("MONITOR_FINANCE_COMBINED_INPUT")
	if dir == "" || reportPath == "" {
		t.Skip("requires retained AWS evidence and combined local report")
	}
	policyPath := filepath.Join(dir, "confirmed-account-wide-policy.json")
	inputs := map[string]string{}
	for _, path := range []string{policyPath, reportPath, filepath.Join(dir, "2026-09.parquet"), filepath.Join(dir, "2026-10.parquet"), filepath.Join(dir, "ce-monthly-202605-202609.json"), filepath.Join(dir, "ce-record-types-202605-202609.json"), filepath.Join(dir, "ce-daily-202605-202608.json")} {
		inputs[path] = giftLocalFileHash(t, path)
	}
	if inputs[policyPath] != "db01e86220446644ebbb4b69d70a3fd333c957b579f7e03d19a2a6ed9324b039" {
		t.Fatal("account-wide policy differs from reviewed owner-confirmed evidence")
	}
	t.Cleanup(func() {
		for path, hash := range inputs {
			if giftLocalFileHash(t, path) != hash {
				t.Error("retained input changed", filepath.Base(path))
			}
		}
	})
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	var rules []financecur.AllocationRule
	if err := json.Unmarshal(raw, &rules); err != nil {
		t.Fatal(err)
	}
	policyHash, err := financecur.AllocationPolicyHash(rules)
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []financecur.Artifact
	for _, name := range []string{"2026-09.parquet", "2026-10.parquet"} {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			t.Fatal(err)
		}
		if _, err := f.Seek(0, 0); err != nil {
			f.Close()
			t.Fatal(err)
		}
		audit, err := financecur.ReadParquet(f, financecur.Limits{})
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		allocated, err := financecur.Allocate(audit.CostEntries, rules)
		if err != nil {
			t.Fatal(err)
		}
		statement, timeline, err := financecur.BuildStatement(audit, allocated, hex.EncodeToString(h.Sum(nil)), policyHash, "Asia/Shanghai", false)
		if err != nil {
			t.Fatal(err)
		}
		a, err := financecur.NewArtifact(statement, timeline)
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, a)
	}
	raw, err = os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Report *financeOperatingReport `json:"after_rebuild"`
	}
	if err := json.Unmarshal(raw, &saved); err != nil || saved.Report == nil {
		t.Fatal("invalid combined report", err)
	}
	r := saved.Report
	if !r.GiftCoverage.Complete || r.From != 1777564800 || r.To != 1790784000 || len(r.Periods) != 5 {
		t.Fatal("requires verified May through September report with completed gift evidence")
	}
	before, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "aws-bundle.json")
	if err := giftLocalWriteNew(path, financeAWSLocalAccountingBundle(t, dir, artifacts)); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	m.applyFinanceCURArtifactToReport(r)
	if !r.CURCost.Loaded || r.BillingResult == nil || len(r.AWSMonthlyBills) != 4 {
		t.Fatal("AWS evidence failed to attach", r.CURCost)
	}
	var original financeOperatingReport
	if err := json.Unmarshal(before, &original); err != nil {
		t.Fatal(err)
	}
	checkBusiness := func(a, b financeStatementView) {
		t.Helper()
		if !reflect.DeepEqual(a.UserConsumption, b.UserConsumption) || !reflect.DeepEqual(a.OperatingRevenue, b.OperatingRevenue) || !reflect.DeepEqual(a.RegistrationGiftConsumption, b.RegistrationGiftConsumption) || !reflect.DeepEqual(a.RawCorrectedUpstreamCost, b.RawCorrectedUpstreamCost) || !reflect.DeepEqual(a.KnownRawCorrectedUpstreamCost, b.KnownRawCorrectedUpstreamCost) {
			t.Fatal("AWS changed business amounts")
		}
	}
	checkBusiness(original.Statement, r.Statement)
	if !reflect.DeepEqual(original.GiftCoverage, r.GiftCoverage) || !reflect.DeepEqual(original.UpstreamCoverage, r.UpstreamCoverage) {
		t.Fatal("AWS changed business coverage")
	}
	for i := range r.Days {
		checkBusiness(original.Days[i].Statement, r.Days[i].Statement)
	}
	complete := 0
	for i, p := range r.Periods {
		checkBusiness(original.Periods[i].Statement, p.Statement)
		b := p.BillingResult
		if b == nil || b.AWSCost == nil {
			t.Fatal("monthly AWS amount missing", p.Period)
		}
		if p.Statement.RawCorrectedUpstreamCost == nil || p.Statement.OperatingRevenue == nil {
			if b.Profit != nil || b.MarginPercent != nil {
				t.Fatal("unknown cost or revenue became profit", p.Period)
			}
			continue
		}
		if b.Profit == nil {
			t.Fatal("complete month failed to calculate", p.Period, b.Reason)
		}
		revenue, ok1 := financeMoneyInt64(*b.Revenue)
		cost, ok2 := financeMoneyInt64(*b.UpstreamCost)
		aws, ok3 := financeMoneyInt64(*b.AWSCost)
		profit, ok4 := financeMoneyInt64(*b.Profit)
		if !ok1 || !ok2 || !ok3 || !ok4 || revenue-cost-aws != profit {
			t.Fatal("monthly equation mismatch", p.Period)
		}
		complete++
		t.Logf("month=%s profit=%s AWS=%s status=%s", p.Period, b.Profit.MicroUSD, b.AWSCost.MicroUSD, b.Status)
	}
	if complete == 0 || r.BillingResult.Profit != nil {
		t.Fatal("independent complete month lost or incomplete total invented")
	}
	once, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	m.applyFinanceCURArtifactToReport(r)
	twice, err := json.Marshal(r)
	if err != nil || string(once) != string(twice) {
		t.Fatal("AWS applied twice changed report", err)
	}
	// Rehearse loss/disable/recovery on the same populated report, not an empty
	// fixture. Only the disposable evidence file is changed; retained inputs and
	// user/upstream amounts must survive every failure and recovery unchanged.
	bundleBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"corrupt", "missing", "disabled"} {
		t.Run("evidence_recovery_"+failure, func(t *testing.T) {
			switch failure {
			case "corrupt":
				if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				m.cfg.FinanceCURArtifactEnabled = false
			}
			m.applyFinanceCURArtifactToReport(r)
			wantStatus := "error"
			if failure == "disabled" {
				wantStatus = "disabled"
			}
			if r.CURCost.Status != wantStatus || r.CURCost.Loaded || r.BillingResult != nil || len(r.AWSMonthlyBills) != 0 || len(r.CURProducts) != 0 {
				t.Fatal("unavailable evidence retained AWS publication", failure, r.CURCost)
			}
			checkCleared := func(s financeStatementView) {
				t.Helper()
				if s.AWSInfrastructureCost != nil || s.OperatingProfit != nil || s.KnownAWSInfrastructureCost != (channelEconomicsMoneyView{}) || s.KnownOperatingProfit != (channelEconomicsMoneyView{}) {
					t.Fatal("unavailable evidence retained stale money", failure)
				}
			}
			checkCleared(r.Statement)
			checkBusiness(original.Statement, r.Statement)
			for i, p := range r.Periods {
				if p.BillingResult != nil {
					t.Fatal("unavailable evidence retained monthly profit", p.Period)
				}
				checkCleared(p.Statement)
				checkBusiness(original.Periods[i].Statement, p.Statement)
			}
			for i, d := range r.Days {
				checkCleared(d.Statement)
				checkBusiness(original.Days[i].Statement, d.Statement)
			}
			if !reflect.DeepEqual(original.GiftCoverage, r.GiftCoverage) || !reflect.DeepEqual(original.UpstreamCoverage, r.UpstreamCoverage) {
				t.Fatal("AWS failure changed business coverage", failure)
			}
			if err := os.WriteFile(path, bundleBytes, 0600); err != nil {
				t.Fatal(err)
			}
			m.cfg.FinanceCURArtifactEnabled = true
			m.applyFinanceCURArtifactToReport(r)
			recovered, err := json.Marshal(r)
			if err != nil || !bytes.Equal(once, recovered) {
				t.Fatal("restored evidence did not reproduce accepted report", failure, err)
			}
		})
	}
	if output := os.Getenv("MONITOR_FINANCE_COMBINED_AWS_RESULT"); output != "" {
		data, err := json.MarshalIndent(struct {
			Mode        string                  `json:"mode"`
			InputHashes map[string]string       `json:"input_hashes"`
			Report      *financeOperatingReport `json:"after_rebuild"`
		}{"local_combined_report_not_publishable", inputs, r}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("combined AWS PASS: independently usable months=%d; business facts unchanged; repeat application identical; incomplete total not published", complete)
}
