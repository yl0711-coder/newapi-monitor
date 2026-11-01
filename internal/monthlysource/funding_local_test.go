package monthlysource

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Explicit opt-in, read-only retained evidence. No DB, network, customer data
// in git, generation, import or publication. Error messages omit raw records.
func TestFundingRetainedMovements(t *testing.T) {
	inputPath, contextPath := os.Getenv("MONTHLY_FUNDING_RETAINED_INPUT"), os.Getenv("MONTHLY_FUNDING_RETAINED_CONTEXT")
	if inputPath == "" || contextPath == "" {
		t.Skip("explicit retained input and context paths required")
	}
	read := func(path string) []byte {
		t.Helper()
		if !filepath.IsAbs(path) {
			t.Fatal("absolute local evidence path required")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal("cannot read retained evidence")
		}
		return raw
	}
	type event struct {
		ID     int64  `json:"id"`
		At     int64  `json:"at"`
		Delta  int64  `json:"delta_quota"`
		Source string `json:"source"`
		Kind   string `json:"kind"`
		Method string `json:"method"`
	}
	var input struct {
		Sources  []struct{ ID, Path, SHA256 string }
		Accounts []struct {
			UserID    int64   `json:"user_id"`
			Movements []event `json:"movements"`
		}
	}
	var evidenceContext struct {
		InputSHA256 string `json:"input_sha256"`
		Accounts    []struct {
			UserID int64   `json:"user_id"`
			Events []event `json:"events"`
		}
	}
	inputRaw, contextRaw := read(inputPath), read(contextPath)
	if json.Unmarshal(inputRaw, &input) != nil || json.Unmarshal(contextRaw, &evidenceContext) != nil || digest(inputRaw) != evidenceContext.InputSHA256 {
		t.Fatal("retained input/context mismatch")
	}
	type ref struct{ path, hash string }
	refs := make(map[string]ref)
	for _, s := range input.Sources {
		if _, exists := refs[s.ID]; exists {
			t.Fatal("duplicate source")
		}
		refs[s.ID] = ref{s.Path, s.SHA256}
	}
	metadata := make(map[int64]map[int64]event)
	for _, a := range evidenceContext.Accounts {
		if _, exists := metadata[a.UserID]; exists {
			t.Fatal("duplicate account")
		}
		metadata[a.UserID] = make(map[int64]event)
		for _, e := range a.Events {
			if _, exists := metadata[a.UserID][e.ID]; exists {
				t.Fatal("duplicate expected event")
			}
			metadata[a.UserID][e.ID] = e
		}
	}
	cache := make(map[string]map[string]json.RawMessage)
	count, rc26, legacy, negative := 0, 0, 0, 0
	seen := make(map[int64]bool)
	for _, a := range input.Accounts {
		for _, m := range a.Movements {
			if seen[m.ID] {
				t.Fatal("source ID reused across accounts")
			}
			seen[m.ID] = true
			sourceID, fragment, ok := strings.Cut(m.Source, "#")
			section, idText, hasID := strings.Cut(fragment, "/id=")
			id, err := strconv.ParseInt(idText, 10, 64)
			if !ok || !hasID || err != nil || id != m.ID {
				t.Fatal("invalid movement source reference")
			}
			source, exists := refs[sourceID]
			if !exists {
				t.Fatal("missing movement source")
			}
			if cache[sourceID] == nil {
				raw := read(source.path)
				if digest(raw) != source.hash {
					t.Fatal("retained source changed")
				}
				var sections map[string]json.RawMessage
				if json.Unmarshal(raw, &sections) != nil {
					t.Fatal("invalid source document")
				}
				cache[sourceID] = sections
			}
			var rows []FundingLog
			if json.Unmarshal(cache[sourceID][section], &rows) != nil {
				t.Fatal("source section missing")
			}
			var selected *FundingLog
			for i := range rows {
				if rows[i].ID == m.ID {
					if selected != nil {
						t.Fatal("duplicate source row")
					}
					selected = &rows[i]
				}
			}
			if selected == nil {
				t.Fatal("movement absent from retained source")
			}
			got := NormalizeFundingLog(*selected, fundingTestRules())
			if got.Status != FundingMovement || got.TargetUserID != a.UserID || got.LogID != m.ID || got.At != m.At || got.DeltaQuota == nil || *got.DeltaQuota != m.Delta {
				t.Fatalf("retained movement mismatch: log=%d status=%s reason=%s", m.ID, got.Status, got.Reason)
			}
			e, exists := metadata[a.UserID][m.ID]
			if !exists || e.At != m.At || e.Delta != m.Delta || e.Source != m.Source || e.Kind != got.Kind || e.Method != got.Method {
				t.Fatalf("retained classification mismatch: log=%d", m.ID)
			}
			count++
			if m.Delta < 0 {
				negative++
			}
			if selected.Type == 3 {
				if strings.Contains(selected.Other, `"op"`) {
					rc26++
				} else {
					legacy++
				}
			}
		}
	}
	if count != 58 || rc26 == 0 || legacy == 0 || negative == 0 {
		t.Fatal("retained acceptance coverage changed")
	}
	for sourceID := range cache {
		s := refs[sourceID]
		if digest(read(s.path)) != s.hash {
			t.Fatal("retained source modified")
		}
	}
	if digest(read(inputPath)) != digest(inputRaw) || digest(read(contextPath)) != digest(contextRaw) {
		t.Fatal("retained control inputs modified")
	}
	t.Logf("exact comparison passed: %d movements, %d RC26 management, %d legacy management, %d negative; %d source files read only", count, rc26, legacy, negative, len(cache))
}
