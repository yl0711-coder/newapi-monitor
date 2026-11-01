package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func tokenWalletReuseCommandFixture(t *testing.T) (string, []byte) {
	t.Helper()
	root, err := filepath.EvalSymlinks(auditPrivateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)).Unix()
	a := monthlysource.TokenUsageMonthArchive{Version: 1, Basis: monthlysource.TokenUsageArchiveBasis, SelectionSHA256: strings.Repeat("c", 64), SourceIdentity: strings.Repeat("a", 64), Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}, UserID: 301901, Month: "2026-09", Pairs: []monthlysource.TokenUsageWindowPair{}}
	stamp := time.Now().Add(-time.Hour).UnixMilli()
	for day := range 30 {
		balance, used := int64(-1), int64(9007199254740993)
		x := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary", Request: monthlysource.UsageWindowRequest{SourceIdentity: a.SourceIdentity, Rules: a.Rules, UserID: a.UserID, From: start + int64(day)*86400, To: start + int64(day+1)*86400},
			StartedAtMS: stamp + int64(day)*1000, CompletedAtMS: stamp + int64(day)*1000 + 1, Wallet: monthlysource.WalletUser{UserID: a.UserID, Exists: true, BalanceQuota: &balance, UsedQuota: &used}, Totals: []monthlysource.UsageWindowRow{}, Rows: []monthlysource.TokenUsageWindowRow{}}
		y := x
		y.StartedAtMS = x.CompletedAtMS + 300000
		y.CompletedAtMS = y.StartedAtMS + 1
		a.Pairs = append(a.Pairs, monthlysource.TokenUsageWindowPair{Older: x, Newer: y})
	}
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "token-evidence.json")
	if err := exclusiveFile(path, raw); err != nil {
		t.Fatal(err)
	}
	return path, raw
}

func TestTokenWalletReuseCommandOnlyInspectsPinnedOriginal(t *testing.T) {
	path, raw := tokenWalletReuseCommandFixture(t)
	args := []string{"inspect-token-wallet", "-archive", path, "-expected-archive", hash(raw)}
	var out bytes.Buffer
	if err := run(t.Context(), args, &out); err != nil {
		t.Fatal(err)
	}
	var report monthlysource.TokenWalletReuseReport
	if json.Unmarshal(out.Bytes(), &report) != nil || report.Mode != monthlysource.TokenWalletReuseMode || len(report.Selected) != 2 || report.Eligible != 60 {
		t.Fatal("unexpected read-only report")
	}
	before := bytes.Clone(out.Bytes())
	out.Reset()
	if err := run(t.Context(), args, &out); err != nil || !bytes.Equal(before, out.Bytes()) {
		t.Fatal("inspection is not stable", err)
	}
	current, err := privateRead(path, monthlysource.TokenUsageArchiveLimit)
	if err != nil || !bytes.Equal(current, raw) {
		t.Fatal("original archive changed", err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatal("inspection wrote files", err)
	}
}

func TestTokenWalletReuseCommandRejectsWriteCaptureAndUnpinnedInputs(t *testing.T) {
	path, raw := tokenWalletReuseCommandFixture(t)
	base := []string{"inspect-token-wallet", "-archive", path, "-expected-archive", hash(raw)}
	for _, extra := range [][]string{{"-capture"}, {"-create-output"}, {"-write"}, {"-apply"}, {"-connection-file=/not-read"}, {"-expected-archive="}, {"-expected-archive=" + strings.Repeat("f", 64)}, {"extra"}} {
		var out bytes.Buffer
		if err := run(t.Context(), append(slices.Clone(base), extra...), &out); err == nil || out.Len() != 0 {
			t.Fatal("write/invalid inspection accepted", extra, err)
		}
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(context.Background(), base, &out); err == nil || out.Len() != 0 {
		t.Fatal("public original accepted")
	}
}

// Called from the existing actual-binary CLI gate, not a newly optional gate.
func testTokenWalletReuseActualCLI(t *testing.T, binary string) {
	t.Helper()
	path, raw := tokenWalletReuseCommandFixture(t)
	args := []string{"inspect-token-wallet", "-archive", path, "-expected-archive", hash(raw)}
	result, err := exec.CommandContext(t.Context(), binary, args...).CombinedOutput()
	var report monthlysource.TokenWalletReuseReport
	if err != nil || json.Unmarshal(result, &report) != nil || len(report.Selected) != 2 || report.Eligible != 60 {
		t.Fatal("actual wallet reuse inspection failed", err)
	}
	for _, extra := range [][]string{{"-capture"}, {"-write"}, {"-expected-archive=" + strings.Repeat("f", 64)}} {
		result, err := exec.CommandContext(t.Context(), binary, append(slices.Clone(args), extra...)...).CombinedOutput()
		if err == nil || bytes.Contains(result, []byte(path)) {
			t.Fatal("invalid actual command accepted or exposed private path", err)
		}
	}
	if fileHash(t, path) != hash(raw) {
		t.Fatal("actual command changed original")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatal("actual command wrote files", err)
	}
}
