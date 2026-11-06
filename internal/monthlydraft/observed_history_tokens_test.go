package monthlydraft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func capturedHistoryWithRetainedTokens(t *testing.T, f *capturedHistoryFixture, files CapturedSourceFiles) monitor.MonthlyPreparationInput {
	t.Helper()
	d := f.open(t)
	defer closeDeploymentTest(t, d)
	raw, err := os.ReadFile(files.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	var material monthlysource.MonthlyObservationMaterial
	if err := json.Unmarshal(raw, &material); err != nil {
		t.Fatal(err)
	}
	input := draftCopy(t, f.f.input)
	p := GenerationPlan{Funding: material.Funding}
	for _, token := range material.Tokens {
		raw, err := os.ReadFile(token.File.Path)
		if err != nil {
			t.Fatal(err)
		}
		id := "historical-token-" + strconv.FormatInt(token.UserID, 10)
		path := filepath.Join(f.cfg.EvidenceRoot, id+".json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		source := monitor.MonthlyPreparationSource{ID: id, Path: path, SHA256: generationHash(raw)}
		_, report, err := d.g.loadTokenArchive(t.Context(), source, p, token.UserID)
		if err != nil {
			t.Fatal(err)
		}
		input.Sources = append(input.Sources, source)
		for i := range input.Accounts {
			if input.Accounts[i].UserID == token.UserID {
				input.Accounts[i].TokenMonths = []monitor.MonthlyBillTokenUsage{projectTokenReport(report, id)}
			}
		}
	}
	raw = draftJSON(t, input)
	if err := os.WriteFile(f.sources.History.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.sources.History.SHA256 = generationHash(raw)
	return input
}

func TestObservedHistoryExistingTokenMonthKeepsEveryAccountAndTokenAmount(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	original := capturedHistoryWithRetainedTokens(t, &f, files)
	d := f.open(t)
	prepared, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, f.sources, "captured-scope")
	if err != nil {
		t.Fatal("existing original token totals block preparation", err)
	}
	got, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range original.Accounts {
		b := got.Accounts[i]
		if !reflect.DeepEqual(a.TokenMonths[0].Rows, b.TokenMonths[0].Rows) || a.Baseline != b.Baseline {
			t.Fatal("changed original token money or account", a.UserID)
		}
	}
	raw, err := os.ReadFile(f.sources.History.Path)
	if err != nil || generationHash(raw) != f.sources.History.SHA256 {
		t.Fatal("rewrote historical input", err)
	}
	preparationCounts(t, f.f, 0, 0)
}

func TestObservedHistoryExistingTokensCannotHideDifferencesOrMissingProof(t *testing.T) {
	for _, scenario := range []string{"gross", "net_offset", "token_id", "token_name", "wrong_user", "wrong_month", "forged_reference", "different_original_archive"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			files, _, err := f.spool.Advance(t.Context(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			input := capturedHistoryWithRetainedTokens(t, &f, files)
			a := &input.Accounts[0]
			switch scenario {
			case "gross":
				a.TokenMonths[0].Rows[0].ConsumeQuota++
			case "net_offset":
				a.TokenMonths[0].Rows[0].ConsumeQuota++
				a.TokenMonths[0].Rows[0].RefundQuota++
			case "token_id":
				a.TokenMonths[0].Rows[0].TokenID += 9000000
			case "token_name":
				a.TokenMonths[0].Rows[0].Name += " changed"
			case "wrong_user":
				a.TokenMonths[0].UserID++
			case "wrong_month":
				a.TokenMonths[0].Month = "2026-08"
			case "forged_reference":
				a.TokenMonths[0].Source += "changed"
			case "different_original_archive":
				id := tokenReferenceSource(a.TokenMonths[0].Source)
				for i := range input.Sources {
					s := &input.Sources[i]
					if s.ID != id {
						continue
					}
					raw, err := os.ReadFile(s.Path)
					if err != nil {
						t.Fatal(err)
					}
					var archive monthlysource.TokenUsageMonthArchive
					if err := json.Unmarshal(raw, &archive); err != nil {
						t.Fatal(err)
					}
					for j := range archive.Pairs {
						for _, observation := range []*monthlysource.TokenUsageWindowObservation{&archive.Pairs[j].Older, &archive.Pairs[j].Newer} {
							for k := range observation.Rows {
								observation.Rows[k].Name += " different original"
							}
						}
					}
					raw = materialPreparationJSON(t, archive)
					_, report, err := monthlysource.DecodeTokenUsageMonthArchive(t.Context(), raw, time.Now())
					if err != nil {
						t.Fatal("conflict fixture is not a valid original archive", err)
					}
					if err := os.WriteFile(s.Path, raw, 0600); err != nil {
						t.Fatal(err)
					}
					s.SHA256 = generationHash(raw)
					a.TokenMonths[0] = projectTokenReport(report, id)
				}
			}
			raw := draftJSON(t, input)
			if err := os.WriteFile(f.sources.History.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			f.sources.History.SHA256 = generationHash(raw)
			d := f.open(t)
			if _, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, f.sources, "captured-scope"); err == nil {
				t.Fatal("conflicting retained tokens were silently replaced")
			}
			preparationCounts(t, f.f, 0, 0)
		})
	}
}
