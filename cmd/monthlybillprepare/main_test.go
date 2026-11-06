package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func preparationCLIInput(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	source, input, output := filepath.Join(dir, "source.json"), filepath.Join(dir, "input.json"), filepath.Join(dir, "output.json")
	if err := os.WriteFile(source, []byte("retained synthetic source"), 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("retained synthetic source"))
	zero := int64(0)
	b := monitor.MonthlyPreparationBalance{UserID: 1, At: 1780243200, Source: "fixture#before"}
	a := monitor.MonthlyPreparationAccount{UserID: 1, GroupID: 1, Username: "synthetic", Months: []string{"2026-06"}, CounterBasis: "net_consumption", BasisSource: "fixture#basis", Baseline: b,
		Spans:     []monitor.MonthlyPreparationSpan{{UserID: 1, From: b.At, To: 1782835200, Requests: &zero, Tokens: &zero, Source: "fixture#logs"}},
		Movements: []monitor.MonthlyPreparationMovement{}, MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: 1, From: b.At, To: 1782835200, Source: "fixture#funds"}},
		Snapshots: []monitor.MonthlyPreparationBalance{{UserID: 1, At: 1782835200, Source: "fixture#after"}},
	}
	raw, err := json.Marshal(monitor.MonthlyPreparationInput{Version: 1, Sources: []monitor.MonthlyPreparationSource{{ID: "fixture", Path: source, SHA256: hex.EncodeToString(h[:])}}, Accounts: []monitor.MonthlyPreparationAccount{a}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return source, input, output
}

func TestMonthlyPreparationCLILocalOnlyAndNoOverwrite(t *testing.T) {
	source, input, output := preparationCLIInput(t)
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepare(input, output, time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var r monitor.MonthlyPreparationReport
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if r.Accounts[0].Months[0].Status != "reconciled_draft" || !strings.Contains(r.SourceIntegrity, "sha256_verified") {
		t.Fatal(r)
	}
	st, err := os.Stat(output)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("report permission", err)
	}
	if err := prepare(input, output, time.Now()); err == nil {
		t.Fatal("overwrote report")
	}
	after, err := os.ReadFile(source)
	if err != nil || string(after) != string(before) {
		t.Fatal("source changed", err)
	}
	unchanged, err := os.ReadFile(output)
	if err != nil || string(unchanged) != string(raw) {
		t.Fatal("output changed", err)
	}
}

func TestMonthlyPreparationCLISourceDriftDoesNotWriteReport(t *testing.T) {
	source, input, output := preparationCLIInput(t)
	if err := os.WriteFile(source, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepare(input, output, time.Now()); err == nil {
		t.Fatal("source mismatch accepted")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("report created on mismatch")
	}
}

func TestMonthlyPreparationCLIRejectsSymlinkAndRelativePath(t *testing.T) {
	source, input, output := preparationCLIInput(t)
	link := filepath.Join(filepath.Dir(input), "link.json")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{link, filepath.Dir(input), "relative.json"} {
		if f, err := openLocalRegular(path); err == nil {
			_ = f.Close()
			t.Fatal("non-regular/relative source accepted", path)
		}
	}
	if err := prepare(input, "relative.json", time.Now()); err == nil {
		t.Fatal("relative output accepted")
	}
	if err := os.Symlink(source, output); err != nil {
		t.Fatal(err)
	}
	if err := prepare(input, output, time.Now()); err == nil {
		t.Fatal("existing symlink output accepted")
	}
}
