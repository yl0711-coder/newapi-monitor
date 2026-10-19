package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

func TestCLIRetainedRoundTrip(t *testing.T) {
	artifacts, evidence := os.Getenv("MONITOR_AWS_PACKAGE_ARTIFACT_DIR"), os.Getenv("MONITOR_CUR_LOCAL_EVIDENCE_DIR")
	if artifacts == "" || evidence == "" {
		t.Skip("requires retained local artifacts and CE evidence")
	}
	testCLIRoundTrip(t, artifacts, evidence)
}

func testCLIRoundTrip(t *testing.T, artifacts, evidence string) {
	t.Helper()
	output := filepath.Join(t.TempDir(), "bundle.json")
	args := []string{"-cur", filepath.Join(artifacts, "2026-09.json"), "-cur", filepath.Join(artifacts, "2026-10.json"),
		"-ce-services", filepath.Join(evidence, "ce-monthly-202605-202609.json"), "-ce-record-types", filepath.Join(evidence, "ce-record-types-202605-202609.json"),
		"-ce-daily", filepath.Join(evidence, "ce-daily-202605-202608.json"), "-ce-from", "2026-05", "-ce-to-exclusive", "2026-09",
		"-accounting-basis", "aws_billing_month_v1", "-out", output}
	var out bytes.Buffer
	if err := run(args, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var r receipt
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if r.Mode != "offline_validated_not_published" || r.Output != output || r.SHA256 != hex.EncodeToString(h[:]) || len(r.InputSHA256) != 5 {
		t.Fatal("invalid receipt")
	}
	for path, want := range r.InputSHA256 {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(b)
		if hex.EncodeToString(h[:]) != want {
			t.Fatal("input changed")
		}
	}
	if err := run(args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("CLI overwrote output")
	}
	after, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(b, after) {
		t.Fatal("failed repeat changed output", err)
	}
}

// Always-on positive CLI coverage; CI does not require private billing files.
func TestCLISyntheticRoundTrip(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, v any) {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, month := range []time.Month{9, 10} {
		start := time.Date(2026, month, 1, 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 1, 0)
		allocated, err := financecur.Allocate([]financecur.CostEntry{{FromUnix: start.Unix(), ToUnix: end.Unix(), ProductCode: "AmazonECS", LineItemType: "Usage", Rows: 1, NanoUSD: 100}}, []financecur.AllocationRule{{ID: "synthetic", ProductCode: "AmazonECS", ResourceMatch: financecur.ResourceAny, NexusAPIPPM: 1_000_000, Reason: "test"}})
		if err != nil {
			t.Fatal(err)
		}
		audit := financecur.Audit{Currency: "USD", Rows: 1, BillingPeriodStart: start.Unix(), BillingPeriodEnd: end.Unix(), FirstUsageUnix: start.Unix(), LastUsageThroughUnix: end.Unix(), TotalNanoUSD: 100}
		s, tl, err := financecur.BuildStatement(audit, allocated, strings.Repeat("a", 64), strings.Repeat("b", 64), "Asia/Shanghai", false)
		if err != nil {
			t.Fatal(err)
		}
		a, err := financecur.NewArtifact(s, tl)
		if err != nil {
			t.Fatal(err)
		}
		write(start.Format("2006-01")+".json", a)
	}
	metric := func(amount string) any {
		return map[string]any{"NetUnblendedCost": map[string]string{"Amount": amount, "Unit": "USD"}}
	}
	var services, records, days []any
	for month := time.May; month < time.September; month++ {
		start := time.Date(2026, month, 1, 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 1, 0)
		period := map[string]string{"Start": start.Format("2006-01-02"), "End": end.Format("2006-01-02")}
		row := func(name string) any {
			return map[string]any{"TimePeriod": period, "Estimated": false, "Groups": []any{map[string]any{"Keys": []string{name}, "Metrics": metric("1")}}}
		}
		services = append(services, row("AmazonECS"))
		records = append(records, row("Usage"))
		for day := start; day.Before(end); day = day.AddDate(0, 0, 1) {
			amount := "0"
			if day == start {
				amount = "1"
			}
			days = append(days, map[string]any{"TimePeriod": map[string]string{"Start": day.Format("2006-01-02"), "End": day.AddDate(0, 0, 1).Format("2006-01-02")}, "Estimated": false, "Total": metric(amount)})
		}
	}
	grouped := func(dimension string, rows []any) any {
		return map[string]any{"GroupDefinitions": []any{map[string]string{"Type": "DIMENSION", "Key": dimension}}, "ResultsByTime": rows}
	}
	write("ce-monthly-202605-202609.json", grouped("SERVICE", services))
	write("ce-record-types-202605-202609.json", grouped("RECORD_TYPE", records))
	write("ce-daily-202605-202608.json", map[string]any{"ResultsByTime": days})
	testCLIRoundTrip(t, dir, dir)
}

func TestAtomicOutputNeverReplacesFilesOrLinks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "new.json")
	if err := writeNewAtomic(path, []byte("accepted")); err != nil {
		t.Fatal(err)
	}
	if err := writeNewAtomic(path, []byte("replacement")); err == nil {
		t.Fatal("overwrote output")
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "accepted" {
		t.Fatal("output changed", err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("unsafe permissions", err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if err := writeNewAtomic(link, []byte("replacement")); err == nil {
		t.Fatal("overwrote symlink")
	}
	dangling := filepath.Join(dir, "dangling.json")
	if err := os.Symlink(filepath.Join(dir, "absent"), dangling); err != nil {
		t.Fatal(err)
	}
	if err := writeNewAtomic(dangling, []byte("replacement")); err == nil {
		t.Fatal("overwrote dangling symlink")
	}
	left, err := filepath.Glob(filepath.Join(dir, ".aws-package-*"))
	if err != nil || len(left) != 0 {
		t.Fatal("temporary output left behind", left, err)
	}
}

func TestReadRegularBoundaries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.json")
	if err := os.WriteFile(path, []byte("1234"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(path, 3); err == nil {
		t.Fatal("oversize accepted")
	}
	if b, err := readRegular(path, 4); err != nil || string(b) != "1234" {
		t.Fatal("exact limit failed", err)
	}
	if _, err := readRegular(dir, 10); err == nil {
		t.Fatal("directory accepted")
	}
	link := filepath.Join(dir, "alias")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(link, 10); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestCLIInvalidInputsLeaveNoOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.json")
	output := filepath.Join(dir, "output.json")
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"-out", output},
		{"-out", output, "-cur", path, "-cur", path},
		{"-out", output, "-cur", path, "-ce-services", path},
	} {
		if err := run(args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatal("bad CLI accepted")
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatal("failed validation created output", err)
		}
	}
}
