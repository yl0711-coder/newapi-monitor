package monthlydraft

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestPartitionedCapturedGenerationCustomerJourney(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	syntheticCapturedRecovery(t, &f)
	addCapturedHistoryReconstruction(t, &f)
	f.wallet.funding.partitionUsage = true
	testCapturedHistoryPublicationAdapter(t, f, true, true, true, true)
}

func TestPartitionedCapturedGenerationMaterialAndPlanBindings(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	f.wallet.funding.partitionUsage = true
	d := f.open(t)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, f.sources, "captured-scope")
	if err != nil {
		t.Fatal(err)
	}
	var plan GenerationPlan
	if json.Unmarshal(prepared.Plan, &plan) != nil || plan.CapturedUsageSourceID == "" || len(plan.UsageWindows) != 0 {
		t.Fatal("new mode lost explicit pinned index")
	}
	input := prepared.Preparation
	if _, _, _, err := canonicalGeneration(plan, input, time.Now()); err != nil {
		t.Fatal(err)
	}
	rawMaterial, err := os.ReadFile(files.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	var material monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(rawMaterial, &material) != nil || material.Version != 2 || material.Mode != monthlysource.PartitionedObservationMaterialMode || material.UsageSelection == nil || *material.UsageSelection != files.Selection || len(material.UsageWindows) != 0 {
		t.Fatal("unversioned or unbound material")
	}
	// Customer-visible money must still be rechecked, not accepted from a
	// successful partition identity. Mutations are synthetic TEST copies only.
	for _, kind := range []string{"mixed_modes", "no_token_sources", "missing_index", "index_replaced", "one_quota", "token_parent_binding"} {
		t.Run(kind, func(t *testing.T) {
			p := plan
			switch kind {
			case "mixed_modes":
				p.UsageWindows = []monthlysource.UsageWindowVersion{{OlderID: generationHash([]byte("old")), NewerID: generationHash([]byte("new"))}}
			case "no_token_sources":
				p.TokenUsageSources = nil
			case "missing_index":
				p.CapturedUsageSourceID = "not-retained"
			case "index_replaced":
				old, err := os.ReadFile(files.Selection.Path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(files.Selection.Path, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(files.Selection.Path, old, 0600); err != nil {
						t.Fatal(err)
					}
				}()
			}
			var err error
			if kind == "index_replaced" || kind == "one_quota" || kind == "token_parent_binding" {
				decoded, decodeErr := monitor.DecodeMonthlyPreparationInput(input)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if kind == "one_quota" {
					decoded.Accounts[0].Spans[0].ConsumeQuota++
				}
				if kind == "token_parent_binding" {
					for i := range decoded.Sources {
						if decoded.Sources[i].ID == p.TokenUsageSources[0].SourceID {
							file := decoded.Sources[i].Path
							old, err := os.ReadFile(file)
							if err != nil {
								t.Fatal(err)
							}
							var archive monthlysource.TokenUsageMonthArchive
							if json.Unmarshal(old, &archive) != nil {
								t.Fatal("invalid test token archive")
							}
							archive.SelectionSHA256 = generationHash([]byte("unrelated selection"))
							raw := materialPreparationJSON(t, archive)
							if err := os.WriteFile(file, raw, 0600); err != nil {
								t.Fatal(err)
							}
							decoded.Sources[i].SHA256 = generationHash(raw)
							defer func() {
								if err := os.WriteFile(file, old, 0600); err != nil {
									t.Fatal(err)
								}
							}()
						}
					}
				}
				p.PreparationSHA256 = generationHash(draftJSON(t, decoded))
				bound, release, bindErr := d.g.bindObservationStore(t.Context(), p, decoded)
				if bindErr != nil {
					if kind == "index_replaced" && errors.Is(bindErr, ErrGenerationIntegrity) {
						return
					}
					t.Fatal(bindErr)
				}
				defer release()
				if kind == "token_parent_binding" {
					_, err = bound.auditTokenInput(t.Context(), p, decoded)
				} else {
					_, err = bound.auditUsageInput(t.Context(), p, decoded)
				}
			} else {
				_, _, _, err = canonicalGeneration(p, input, time.Now())
			}
			if err == nil {
				t.Fatal("invalid retained source/plan admitted", kind)
			}
		})
	}
	// Turning the mode off must not reinterpret or overwrite a completed seal.
	f.wallet.funding.partitionUsage = false
	if _, _, err := f.spool.Advance(t.Context(), f.req); err == nil {
		t.Fatal("legacy seal silently accepted new material")
	}
	after, err := os.ReadFile(files.Material.Path)
	if err != nil || string(after) != string(rawMaterial) {
		t.Fatal("mode change rewrote frozen material", err)
	}
}
