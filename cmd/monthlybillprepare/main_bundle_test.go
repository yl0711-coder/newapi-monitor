package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func preparationBundleCLIInput(t *testing.T) (string, string, string, string) {
	t.Helper()
	source, input, output := preparationCLIInput(t)
	raw, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(raw)
	c := monitor.MonthlyBundleContext{Version: 1, InputSHA256: hex.EncodeToString(h[:]),
		Groups:   []monitor.MonthlyBundleGroup{{GroupID: 1, Name: "synthetic", Source: "fixture#scope", Months: []string{"2026-06"}, Members: []monitor.MonthlyBundleMember{{UserID: 1, Username: "synthetic"}}}},
		Accounts: []monitor.MonthlyBundleAccountContext{{UserID: 1, WalletOnly: true, WalletSource: "fixture#wallet", Events: []monitor.MonthlyBundleEvent{}}},
	}
	contextPath := filepath.Join(filepath.Dir(input), "context.json")
	raw, err = json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(contextPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return source, input, contextPath, output
}

func TestMonthlyBundleCLISuccessPreservesInputsAndLegacyReport(t *testing.T) {
	source, input, contextPath, output := preparationBundleCLIInput(t)
	before := map[string][]byte{}
	for _, path := range []string{source, input, contextPath} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = raw
	}
	now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	if err := prepareBundle(input, contextPath, output, now); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var b monitor.MonthlyPreparationBundle
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	if b.ReadyForReview != 1 || b.Blocked != 0 || !strings.Contains(b.SourceIntegrity, "sha256_verified") || b.Entries[0].Candidate == nil {
		t.Fatal("incomplete candidate", b)
	}
	st, err := os.Stat(output)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("candidate permissions", err)
	}
	if err := prepareBundle(input, contextPath, output, now); err == nil {
		t.Fatal("existing bundle overwritten")
	}
	after, err := os.ReadFile(output)
	if err != nil || string(raw) != string(after) {
		t.Fatal("output changed", err)
	}
	legacy := filepath.Join(filepath.Dir(output), "legacy.json")
	if err := prepare(input, legacy, now); err != nil {
		t.Fatal(err)
	}
	legacyRaw, err := os.ReadFile(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var old monitor.MonthlyPreparationReport
	if err := json.Unmarshal(legacyRaw, &old); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old, b.Reconciliation) {
		t.Fatal("new bundle changed the existing reconciliation result")
	}
	for path, want := range before {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != string(want) {
			t.Fatal("source mutated", path, err)
		}
	}
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(output), ".monthly-candidate-*"))
	if err != nil || len(temps) != 0 {
		t.Fatal("temporary bundle leaked", temps, err)
	}
}

func TestMonthlyBundleCLIFailuresLeaveNoOutput(t *testing.T) {
	for _, scenario := range []string{"source drift", "input drift", "invalid context", "context symlink", "relative output"} {
		t.Run(scenario, func(t *testing.T) {
			source, input, contextPath, output := preparationBundleCLIInput(t)
			switch scenario {
			case "source drift":
				if err := os.WriteFile(source, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "input drift":
				f, err := os.OpenFile(input, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := f.WriteString("\n")
				closeErr := f.Close()
				if writeErr != nil || closeErr != nil {
					t.Fatal(writeErr, closeErr)
				}
			case "invalid context":
				if err := os.WriteFile(contextPath, []byte(`{"version":1}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "context symlink":
				link := filepath.Join(filepath.Dir(input), "linked-context.json")
				if err := os.Symlink(contextPath, link); err != nil {
					t.Fatal(err)
				}
				contextPath = link
			case "relative output":
				output = "relative-candidate.json"
			}
			if err := prepareBundle(input, contextPath, output, time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)); err == nil {
				t.Fatal("invalid source accepted")
			}
			if _, err := os.Lstat(output); !os.IsNotExist(err) {
				t.Fatal("output created", err)
			}
			temps, err := filepath.Glob(filepath.Join(filepath.Dir(input), ".monthly-candidate-*"))
			if err != nil || len(temps) != 0 {
				t.Fatal("temporary file leaked", temps, err)
			}
		})
	}
}

func TestMonthlyBundleCLIAtomicExclusiveOutput(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "candidate.json")
	results := make(chan error, 2)
	for _, text := range []string{"first complete result", "second complete result"} {
		go func(text string) { results <- writeBundleExclusive(output, []byte(text)) }(text)
	}
	success := 0
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			success++
		} else if !os.IsExist(err) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatal("expected exactly one writer", success)
	}
	raw, err := os.ReadFile(output)
	if err != nil || (string(raw) != "first complete result" && string(raw) != "second complete result") {
		t.Fatal("partial or merged output", string(raw), err)
	}
	link := filepath.Join(dir, "linked.json")
	if err := os.Symlink(output, link); err != nil {
		t.Fatal(err)
	}
	if err := writeBundleExclusive(link, []byte("overwrite")); !os.IsExist(err) {
		t.Fatal("symlink overwritten", err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 2 {
		t.Fatal("temporary output leaked", err)
	}
}
