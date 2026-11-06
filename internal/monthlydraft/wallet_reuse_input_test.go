package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func useWalletReusePreparationFixture(t *testing.T, f generationFixture, r *ObservationMaterialPreparationRequest) monthlysource.WalletReuseMaterial {
	t.Helper()
	originalRaw, err := os.ReadFile(r.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	var m monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(originalRaw, &m) != nil {
		t.Fatal("fixture material invalid")
	}
	archives := map[int64][]byte{}
	for _, a := range m.Tokens {
		raw, err := os.ReadFile(a.File.Path)
		if err != nil {
			t.Fatal(err)
		}
		archives[a.UserID] = raw
	}
	meta, err := monthlysource.BuildWalletReuseMaterial(t.Context(), originalRaw, r.Material, archives, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	metaRaw := materialPreparationJSON(t, meta)
	path := filepath.Join(f.root, "wallet-reuse.json")
	if err := os.WriteFile(path, metaRaw, 0600); err != nil {
		t.Fatal(err)
	}
	m.WalletIDs = []string{}
	m.WalletReuse = &monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(metaRaw)}
	raw := materialPreparationJSON(t, m)
	newPath := filepath.Join(f.root, "reused-materials.json")
	if err := os.WriteFile(newPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	r.Material = monthlysource.MonthlyObservationFile{Path: newPath, SHA256: generationHash(raw)}
	return meta
}

func TestWalletReuseMaterialPreparationExactMoneyAndIndependentAccountViews(t *testing.T) {
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	legacy := r
	meta := useWalletReusePreparationFixture(t, f, &r)
	before := map[string]string{}
	for _, p := range []string{meta.OriginalMaterial.Path, r.Material.Path, cfg.ReceiverPath, cfg.AuthorityPath, f.path, meta.Accounts[0].Archive.Path, meta.Accounts[1].Archive.Path} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		before[p] = generationHash(b)
	}
	preview, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || preview.Complete {
		t.Fatal("reuse inspect failed", err)
	}
	preparationCounts(t, f, 0, 0)
	legacy.OutputRoot = filepath.Join(f.root, "legacy-still-readable")
	if err := os.Mkdir(legacy.OutputRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if old, err := PrepareObservationMaterialFiles(t.Context(), cfg, legacy); err != nil || old.Complete {
		t.Fatal("new local metadata made legacy source unusable", err)
	}
	r.Write = true
	receipt, err := PrepareObservationMaterialFiles(t.Context(), cfg, r)
	if err != nil || !receipt.Complete || receipt.ManifestSHA256 != preview.ManifestSHA256 {
		t.Fatal("reuse preparation failed", err)
	}
	planRaw, err := os.ReadFile(filepath.Join(r.OutputRoot, "plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(r.OutputRoot, "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p GenerationPlan
	if json.Unmarshal(planRaw, &p) != nil || len(p.WalletObservations.IDs) != 0 || p.WalletObservations.ReuseSourceID != walletReuseSourceID {
		t.Fatal("reuse selection not fixed in plan")
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range input.Accounts {
		if a.UserID != meta.Accounts[i].UserID || len(a.Snapshots) != 3 || a.Snapshots[1].TimeBasis != "collection_observation" || a.Snapshots[2].TimeBasis != "collection_observation" {
			t.Fatal("per-account observations missing or relabeled as cutoffs")
		}
		for j, s := range a.Snapshots[1:] {
			if s.At != meta.Accounts[i].Selected[j].CompletedAtMS/1000 || s.CutoffSource != "" {
				t.Fatal("original per-user time changed")
			}
		}
	}
	if _, err := PrepareGenerationInput(t.Context(), cfg, planRaw, raw, "", false); err != nil {
		t.Fatal("existing inspector rejected reused wallet sources", err)
	}
	preparationCounts(t, f, 0, 0)
	if _, err := PrepareGenerationInput(t.Context(), cfg, planRaw, raw, "", true); err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: planRaw, Preparation: raw})
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact, 1)
	again, err := f.g.Run(t.Context(), job.Request.Owner, job.ID)
	if err != nil || !reflect.DeepEqual(artifact, again) {
		t.Fatal("reuse changed saved draft", err)
	}
	for path, sha := range before {
		b, err := os.ReadFile(path)
		if path == cfg.ReceiverPath {
			continue
		}
		if err != nil || generationHash(b) != sha {
			t.Fatal("immutable source changed", path, err)
		}
	}
	// A consumer cannot keep using a saved selection after its original source
	// changes, even when a previous preparation and draft succeeded.
	if err := os.WriteFile(meta.Accounts[0].Archive.Path, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Run(t.Context(), job.Request.Owner, job.ID); err == nil {
		t.Fatal("changed parent reused through cached draft")
	}
}

func TestWalletReuseSelectionPreservesLegacyBytesAndFailsClosedForOldReader(t *testing.T) {
	id := generationHash([]byte("legacy wallet view"))
	sources := []monitor.MonthlyPreparationSource{{ID: "db"}, {ID: "reuse"}}
	s, err := canonicalWalletSelection(&WalletObservationSelection{SourceID: "db", IDs: []string{id}}, sources)
	if err != nil {
		t.Fatal(err)
	}
	raw := materialPreparationJSON(t, s)
	want := []byte(`{"source_id":"db","ids":["` + id + `"]}`)
	if !bytes.Equal(raw, want) {
		t.Fatal("legacy selection bytes changed")
	}
	next, err := canonicalWalletSelection(&WalletObservationSelection{SourceID: "db", IDs: []string{}, ReuseSourceID: "reuse"}, sources)
	if err != nil {
		t.Fatal(err)
	}
	var old struct {
		SourceID string   `json:"source_id"`
		IDs      []string `json:"ids"`
	}
	newRaw := materialPreparationJSON(t, next)
	if json.Unmarshal(newRaw, &old) != nil {
		t.Fatal("bad fixture")
	}
	if bytes.Equal(newRaw, materialPreparationJSON(t, old)) {
		t.Fatal("old canonical reader could silently ignore reuse provenance")
	}
	for _, s := range []*WalletObservationSelection{{SourceID: "db", IDs: []string{id}, ReuseSourceID: "reuse"}, {SourceID: "db", ReuseSourceID: "missing"}, {SourceID: "db", ReuseSourceID: "db"}} {
		if _, err := canonicalWalletSelection(s, sources); err == nil {
			t.Fatal("mixed or missing source accepted")
		}
	}
}

func TestWalletReuseMaterialRejectsLostProvenanceScopeAndTampering(t *testing.T) {
	for _, kind := range []string{"parent", "wallet_digest", "time", "group", "month", "member_missing", "wrong_archive", "origin_hash", "mode", "unknown", "duplicate", "recursive_origin", "overlay_database", "mixed_ids", "reuse_hash", "reuse_as_baseline", "reuse_as_cutoff", "reuse_as_refund"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r, _ := observationMaterialPreparationFixture(t)
			meta := useWalletReusePreparationFixture(t, f, &r)
			var m monthlysource.MonthlyObservationMaterial
			body, err := os.ReadFile(r.Material.Path)
			if err != nil || json.Unmarshal(body, &m) != nil {
				t.Fatal(err)
			}
			switch kind {
			case "parent":
				meta.Accounts[0].Selected[0].ParentSHA256 = meta.Accounts[1].Selected[0].ParentSHA256
			case "wallet_digest":
				meta.Accounts[0].Selected[0].WalletSHA256 = meta.Accounts[1].Selected[0].WalletSHA256
			case "time":
				meta.Accounts[0].Selected[0].CompletedAtMS++
			case "group":
				meta.GroupID++
			case "month":
				meta.Month = "2026-08"
			case "member_missing":
				meta.Accounts = meta.Accounts[:1]
			case "wrong_archive":
				meta.Accounts[0].Archive = meta.Accounts[1].Archive
			case "origin_hash":
				meta.OriginalMaterial.SHA256 = meta.Accounts[0].Archive.SHA256
			case "mode":
				meta.Mode = "approved"
			case "recursive_origin":
				meta.OriginalMaterial = r.Material
			case "overlay_database":
				m.Database = m.Tokens[0].File
			case "mixed_ids":
				m.WalletIDs = []string{meta.Accounts[0].Selected[0].WalletSHA256}
			}
			metaRaw := materialPreparationJSON(t, meta)
			if kind == "unknown" {
				metaRaw = append(bytes.TrimSuffix(metaRaw, []byte("}")), []byte(`,"approved":true}`)...)
			}
			if kind == "duplicate" {
				metaRaw = bytes.Replace(metaRaw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			}
			if err := os.WriteFile(m.WalletReuse.Path, metaRaw, 0600); err != nil {
				t.Fatal(err)
			}
			m.WalletReuse.SHA256 = generationHash(metaRaw)
			if kind == "reuse_hash" {
				m.WalletReuse.SHA256 = meta.OriginalMaterial.SHA256
			}
			if kind == "reuse_as_baseline" || kind == "reuse_as_cutoff" || kind == "reuse_as_refund" {
				input, err := monitor.DecodeMonthlyPreparationInput(r.Template)
				if err != nil {
					t.Fatal(err)
				}
				ref := walletReuseSourceID + "#token-wallet:fake"
				switch kind {
				case "reuse_as_baseline":
					input.Accounts[0].Baseline.Source = ref
				case "reuse_as_cutoff":
					input.Accounts[0].Snapshots[0].CutoffSource = ref
				case "reuse_as_refund":
					input.Accounts[0].WalletMonths[0].Source = ref
				}
				r.Template = draftJSON(t, input)
			}
			raw := materialPreparationJSON(t, m)
			if err := os.WriteFile(r.Material.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			r.Material.SHA256 = generationHash(raw)
			r.Write = true
			receipt, err := PrepareObservationMaterialFiles(context.Background(), cfg, r)
			if err == nil || receipt.Complete || receipt.Directory != "" {
				t.Fatal("invalid reuse metadata produced files", kind, err)
			}
			entries, err := os.ReadDir(r.OutputRoot)
			if err != nil || len(entries) != 0 {
				t.Fatal("invalid reuse wrote output", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}
