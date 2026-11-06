package monthlydraft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func TestCapturedHistoryIndexSelectsOnlyExactMonthAndOrganization(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	index := CapturedHistoryIndex{Version: 1, Entries: []CapturedHistoryIndexEntry{
		{GroupID: f.req.Owner.GroupID, Month: f.req.Month, History: f.sources.History, Scope: f.sources.Scope},
		{GroupID: 919191, Month: "2026-08", History: monthlysource.MonthlyObservationFile{Path: filepath.Join(f.cfg.EvidenceRoot, "unrelated-absent-history.json"), SHA256: generationHash([]byte("unrelated history"))}, Scope: monthlysource.MonthlyObservationFile{Path: filepath.Join(f.cfg.EvidenceRoot, "unrelated-absent-scope.json"), SHA256: generationHash([]byte("unrelated scope"))}},
	}}
	path := filepath.Join(f.cfg.EvidenceRoot, "pinned-history-index.json")
	raw := materialPreparationJSON(t, index)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	load, err := NewCapturedHistoryFileLoader(d.g, monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := load(t.Context(), f.req)
	if err != nil || got != f.sources {
		t.Fatal("selected another account or read unrelated missing files", got, err)
	}
	missing := f.req
	missing.Month = "2026-08"
	if got, err := load(t.Context(), missing); !errors.Is(err, ErrGenerationSelectionMissing) || !reflect.DeepEqual(got, CapturedHistorySources{}) {
		t.Fatal("unconfigured month inferred", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := load(ctx, f.req); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	preparationCounts(t, f.f, 0, 0)
}

func TestCapturedHistoryIndexRejectsChangesAliasesAndAmbiguity(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	for _, scenario := range []string{"duplicate", "unknown_field", "stale_digest", "public", "symlink", "foreign_path"} {
		t.Run(scenario, func(t *testing.T) {
			index := CapturedHistoryIndex{Version: 1, Entries: []CapturedHistoryIndexEntry{{GroupID: f.req.Owner.GroupID, Month: f.req.Month, History: f.sources.History, Scope: f.sources.Scope}}}
			if scenario == "duplicate" {
				index.Entries = append(index.Entries, index.Entries[0])
			}
			if scenario == "foreign_path" {
				index.Entries[0].History.Path = "/outside-evidence/history.json"
			}
			raw := materialPreparationJSON(t, index)
			if scenario == "unknown_field" {
				raw = append(append([]byte{}, raw[:len(raw)-1]...), []byte(`,"approved":true}`)...)
			}
			path := filepath.Join(f.cfg.EvidenceRoot, "history-index-"+scenario+".json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			ref := monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)}
			load, err := NewCapturedHistoryFileLoader(d.g, ref)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "stale_digest":
				if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(path, path+".retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".retained", path); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := load(t.Context(), f.req); err == nil || !reflect.DeepEqual(got, CapturedHistorySources{}) {
				t.Fatal("invalid source routing accepted", err)
			}
		})
	}
}
