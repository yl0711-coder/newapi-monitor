package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Explicit private originals only. No network, credentials or original writes.
func TestDailyFundingRetainedRealSinglePassReadOnly(t *testing.T) {
	root, archive := os.Getenv("MONTHLY_FUNDING_RETAINED_DAY"), os.Getenv("MONTHLY_CAPTURE_RETAINED_FUNDING")
	if root == "" || archive == "" {
		t.Skip("explicit retained real day and independent archive required")
	}
	dir := filepath.Join(root, "sep-23-funding-day")
	raw, err := privateRead(filepath.Join(dir, "manifest.json"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := readManifest(dir, hash(raw))
	if err != nil || m.GroupID != 27 || m.Month != "2026-09" || m.Older.Version != 2 || m.Older.From != 1790092800 || m.Older.To != 1790179200 ||
		m.Older.SourceIdentity != "f3cd458aac20374cf8b52dcdf0c4baf8d3e90c966ae42229365cc30399968937" || !slices.Equal(m.Older.UserIDs, []int64{148}) {
		t.Fatal("real retained day scope differs", err)
	}
	path := filepath.Join(dir, "funding.sqlite")
	before := fileHash(t, path)
	t.Cleanup(func() {
		if fileHash(t, path) != before {
			t.Error("original day database changed")
		}
	})
	ctx := context.Background()
	db, err := openReceiver(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p, err := monthlysource.LoadLatestFundingPage(ctx, db, m.Older.ID)
	if err != nil || !p.EndReached || p.PreviousID != "" || len(p.Rows) != 68 {
		t.Fatal("unexpected first day capture", err)
	}
	projection, err := monthlysource.ProjectFundingPage(p)
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[string]int{}
	for _, r := range projection.Review {
		reasons[r.Reason]++
	}
	t.Logf("local review reason counts (no source text): %v", reasons)
	if len(projection.Review) != 1 || projection.Review[0].TargetUserID != 158 || projection.Review[0].Reason != "created_user_wallet_requires_evidence" || len(projection.Separate) != 0 || len(projection.Movements) != 1 {
		t.Fatalf("day classification needs review: uncertain=%d separate=%d selected-movements=%d", len(projection.Review), len(projection.Separate), len(projection.Movements))
	}
	move := projection.Movements[0]
	if move.LogID != 3715551 || move.TargetUserID != 148 || move.DeltaQuota == nil || *move.DeltaQuota != 500000000 {
		t.Fatal("selected original movement changed")
	}
	oldRaw, err := privateRead(archive, 1<<20)
	if err != nil || hash(oldRaw) != "de27c057994b1a379f091dd59f05a5ad849580ffe9bea232acc7423e01e70525" {
		t.Fatal("independent old archive differs", err)
	}
	var old map[string]json.RawMessage
	if json.Unmarshal(oldRaw, &old) != nil {
		t.Fatal("old source JSON")
	}
	matches := 0
	for _, block := range old {
		var rows []monthlysource.FundingLog
		if json.Unmarshal(block, &rows) != nil {
			continue
		}
		for _, row := range rows {
			if row.ID != move.LogID {
				continue
			}
			fresh := slices.IndexFunc(p.Rows, func(r monthlysource.FundingLog) bool { return r.ID == row.ID })
			if fresh < 0 || !reflect.DeepEqual(p.Rows[fresh], row) {
				t.Fatal("fresh day row differs from independent original")
			}
			matches++
		}
	}
	if matches != 1 {
		t.Fatal("funding source missing or duplicated")
	}
	t.Logf("real Sep 23 first pass: 68 original rows; one selected account movement = 500000000 quota = 1000 CNY; transaction %dms; source bytes unchanged; NOT independent dual-pass or complete-month proof", p.CompletedAtMS-p.StartedAtMS)
}

func TestDailyFundingRetainedRealPairReadOnly(t *testing.T) {
	root := os.Getenv("MONTHLY_FUNDING_RETAINED_DAY")
	if root == "" {
		t.Skip("explicit private real day originals required")
	}
	dir := filepath.Join(root, "sep-23-funding-day")
	raw, err := privateRead(filepath.Join(dir, "manifest.json"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := readManifest(dir, hash(raw))
	if err != nil || m.GroupID != 27 || m.Month != "2026-09" || m.Older.Version != 2 || m.Older.From != 1790092800 || m.Older.To != 1790179200 ||
		m.Older.SourceIdentity != "f3cd458aac20374cf8b52dcdf0c4baf8d3e90c966ae42229365cc30399968937" || !slices.Equal(m.Older.UserIDs, []int64{148}) {
		t.Fatal("real day scope", err)
	}
	path := filepath.Join(dir, "funding.sqlite")
	before := fileHash(t, path)
	t.Cleanup(func() {
		if fileHash(t, path) != before {
			t.Error("independent original day scans changed during offline review")
		}
	})
	ctx := context.Background()
	db, err := openReceiver(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	window, err := monthlysource.ReviewFundingWindow(ctx, db, m.Older.ID, m.Newer.ID)
	if err != nil {
		t.Fatal("real independent pair", err)
	}
	c := window.Comparison()
	if !c.ObservationsMatch || c.OlderRows != 68 || c.NewerRows != 68 || len(c.Accounts) != 1 || c.Accounts[0].UserID != 148 ||
		len(c.Accounts[0].ReviewLogIDs) != 0 || len(c.UnscopedReviewLogIDs) != 0 || len(c.UnscopedChangedLogIDs) != 0 {
		t.Fatal("day changed or unrelated resource operations still block selected account")
	}
	request := monthlysource.FundingMonthRequest{Month: "2026-09", SourceIdentity: m.Older.SourceIdentity, Rules: m.Older.Rules, UserIDs: m.Older.UserIDs}
	partial, err := monthlysource.ReviewFundingMoney(ctx, request, []monthlysource.FundingWindowObservation{window})
	if err != nil || len(partial.Accounts) != 1 || partial.Accounts[0].Totals != nil || len(partial.Windows.MissingWindows) == 0 || partial.Windows.Accounts[0].SelectedSeconds != 86400 {
		t.Fatal("one real day promoted to a complete month", err)
	}
	newer, err := monthlysource.LoadLatestFundingPage(ctx, db, m.Newer.ID)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := monthlysource.ProjectFundingPage(newer)
	if err != nil || len(projection.Movements) != 1 || projection.Movements[0].DeltaQuota == nil || *projection.Movements[0].DeltaQuota != 500000000 {
		t.Fatal("independent selected amount differs", err)
	}
	t.Logf("real independent day scans match: 68 rows each, selected movement 1000 CNY; no selected/unscoped review blockers; only 86400 observed seconds, month still incomplete; latest transaction %dms", newer.CompletedAtMS-newer.StartedAtMS)
}
