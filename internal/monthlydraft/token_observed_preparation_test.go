package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Construct token observations for this synthetic source's actual independent
// usage rows, not just coincidentally equal per-month money.
func addTokenArchivesFromObserved(t *testing.T, f *generationFixture) {
	t.Helper()
	for _, user := range f.plan.Funding.UserIDs {
		archive := monthlysource.TokenUsageMonthArchive{Version: 1, Basis: monthlysource.TokenUsageArchiveBasis, SelectionSHA256: strings.Repeat("d", 64), SourceIdentity: f.plan.Funding.SourceIdentity, Rules: f.plan.Funding.Rules, UserID: user, Month: f.plan.Funding.Month, Pairs: []monthlysource.TokenUsageWindowPair{}}
		for _, selected := range f.plan.UsageWindows {
			a, err := monthlysource.LoadUsageWindowObservation(context.Background(), f.funding, selected.OlderID)
			if err != nil {
				t.Fatal(err)
			}
			if a.Request.UserID != user {
				continue
			}
			b, err := monthlysource.LoadUsageWindowObservation(context.Background(), f.funding, selected.NewerID)
			if err != nil {
				t.Fatal(err)
			}
			var pair monthlysource.TokenUsageWindowPair
			for i, o := range []monthlysource.UsageWindowObservation{a, b} {
				target := &pair.Older
				if i == 1 {
					target = &pair.Newer
				}
				*target = monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary", Request: o.Request, StartedAtMS: o.StartedAtMS, CompletedAtMS: o.CompletedAtMS, Wallet: o.Wallet, Totals: o.Rows, Rows: []monthlysource.TokenUsageWindowRow{}}
				for _, r := range o.Rows {
					target.Rows = append(target.Rows, monthlysource.TokenUsageWindowRow{UsageWindowRow: r, TokenID: user*100 + int64(r.Type), Name: "same retained name"})
				}
			}
			archive.Pairs = append(archive.Pairs, pair)
		}
		body, err := json.Marshal(archive)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := monthlysource.DecodeTokenUsageMonthArchive(context.Background(), body, time.Now()); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("observed-tokens-%d", user)
		path := filepath.Join(f.root, id+".json")
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: id, Path: path, SHA256: generationHash(body)})
		f.plan.TokenUsageSources = append(f.plan.TokenUsageSources, TokenUsageSourceSelection{UserID: user, SourceID: id})
	}
	usageInputUpdate(t, f)
}

func TestTokenGenerationCombinedObservedPreparationAndOriginalMetadata(t *testing.T) {
	f := observedPreparationFixture(t, "")
	addTokenArchivesFromObserved(t, &f)
	result, err := f.g.PrepareObservedInput(context.Background(), f.plan, f.raw, "observed_wallet")
	if err != nil || !generationHex(result.TokenSHA256, 32) {
		t.Fatal("combined token normalization failed", err)
	}
	var normalized monitor.MonthlyPreparationInput
	if json.Unmarshal(result.Preparation, &normalized) != nil || len(normalized.Accounts[0].TokenMonths) != 1 || len(normalized.Accounts[0].TokenMonths[0].Rows) != 2 {
		t.Fatal("combined preparation omitted tokens")
	}
	prepared := f
	prepared.input = monitor.MonthlyPreparationInput{}
	prepared.plan = GenerationPlan{}
	useWalletPreparation(t, &prepared, WalletPreparation{Plan: result.Plan, Preparation: result.Preparation})
	job := generationSubmit(t, prepared)
	artifact, err := prepared.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil || artifact.TokenSHA256 != result.TokenSHA256 {
		t.Fatal("combined token task rejected", err)
	}
	assertGenerationAmounts(t, artifact, 1)
	// Alter only retained log identities, preserving exact money and record
	// count. Independent captures must still disagree with this source.
	for i, s := range f.input.Sources {
		if s.ID != f.plan.TokenUsageSources[0].SourceID {
			continue
		}
		raw, err := os.ReadFile(s.Path)
		if err != nil {
			t.Fatal(err)
		}
		var archive monthlysource.TokenUsageMonthArchive
		if json.Unmarshal(raw, &archive) != nil {
			t.Fatal("invalid archive fixture")
		}
		for j := range archive.Pairs {
			for _, o := range []*monthlysource.TokenUsageWindowObservation{&archive.Pairs[j].Older, &archive.Pairs[j].Newer} {
				if len(o.Rows) == 0 {
					continue
				}
				o.Rows[0].MinID += 1000
				o.Rows[0].MaxID += 1000
				o.Totals[0] = o.Rows[0].UsageWindowRow
			}
		}
		raw, _ = json.Marshal(archive)
		if _, _, err := monthlysource.DecodeTokenUsageMonthArchive(context.Background(), raw, time.Now()); err != nil {
			t.Fatal("counterexample invalid before baseline comparison", err)
		}
		if err := os.WriteFile(s.Path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		f.input.Sources[i].SHA256 = generationHash(raw)
	}
	usageInputUpdate(t, &f)
	if _, err := f.g.PrepareObservedInput(context.Background(), f.plan, f.raw, "observed_wallet"); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("same amount/count but different source IDs accepted", err)
	}
}

func TestTokenGenerationAutomaticPreparationFilesToFormalInput(t *testing.T) {
	f, cfg, request := automaticPreparationFileFixture(t)
	addTokenArchivesFromObserved(t, &f)
	request.Plan, request.Template = mustGenerationPlan(t, f), f.raw
	preview, err := PrepareAutomaticGenerationFiles(context.Background(), cfg, request)
	if err != nil || preview.Complete {
		t.Fatal("token file preflight failed", err)
	}
	preparationCounts(t, f, 0, 0)
	request.Write = true
	receipt, err := PrepareAutomaticGenerationFiles(context.Background(), cfg, request)
	if err != nil || !receipt.Complete || preview.PlanSHA256 != receipt.PlanSHA256 {
		t.Fatal("token file preparation failed", err)
	}
	read := func(name string) []byte {
		raw, err := os.ReadFile(filepath.Join(request.OutputRoot, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	var manifest automaticPreparationManifest
	if json.Unmarshal(read("manifest.json"), &manifest) != nil || !generationHex(manifest.TokenSHA256, 32) {
		t.Fatal("token sources missing from formal file manifest")
	}
	result, err := PrepareGenerationInput(context.Background(), cfg, read("plan.json"), read("input.json"), "", false)
	if err != nil || result.Applied {
		t.Fatal("formal token input cannot be inspected", err)
	}
	preparationCounts(t, f, 0, 0)
}
